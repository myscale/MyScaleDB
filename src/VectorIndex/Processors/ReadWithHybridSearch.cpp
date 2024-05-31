#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/scope_guard_safe.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeWithVectorScanSource.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <VectorIndex/Storages/MergeTreeHybridSearchManager.h>
#include <VectorIndex/Storages/MergeTreeTextSearchManager.h>
#include <VectorIndex/Storages/MergeTreeVSManager.h>
#include <VectorIndex/Storages/MergeTreeSelectWithHybridSearchProcessor.h>
#include <VectorIndex/Processors/ReadWithHybridSearch.h>
#include <VectorIndex/Processors/VSRecomputeTransform.h>
#include <VectorIndex/Processors/VSSplitTransform.h>

#if USE_TANTIVY_SEARCH
#include <VectorIndex/Common/BM25InfoInDataParts.h>
#include <Storages/MergeTree/TantivyIndexStore.h>
#include <Interpreters/TantivyFilter.h>
#endif

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedPartsTotal;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
    extern const Event SelectedMarksTotal;
}

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectBM25CollectThreads;
    extern const Metric MergeTreeDataSelectBM25CollectThreadsActive;
}

namespace DB
{

static MergeTreeReaderSettings getMergeTreeReaderSettings(
    const ContextPtr & context, const SelectQueryInfo & query_info)
{
    const auto & settings = context->getSettingsRef();
    return
    {
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = settings.checksum_on_read,
        .read_in_order = query_info.input_order_info != nullptr,
        .apply_deleted_mask = settings.apply_deleted_mask,
        .use_asynchronous_read_from_pool = settings.allow_asynchronous_read_from_io_pool_for_merge_tree
            && (settings.max_streams_to_max_threads_ratio > 1 || settings.max_streams_for_merge_tree_reading > 1),
        .enable_multiple_prewhere_read_steps = settings.enable_multiple_prewhere_read_steps,
    };
}

static const PrewhereInfoPtr & getPrewhereInfo(const SelectQueryInfo & query_info)
{
    return query_info.projection ? query_info.projection->prewhere_info
                                 : query_info.prewhere_info;
}
#if USE_TANTIVY_SEARCH
void ReadWithHybridSearch::getStatisticForTextSearch()
{
    BM25InfoInDataParts parts_with_bm25_info;

    /// Find inverted index desc on the search column from metadata
    TextSearchInfoPtr text_search_info = nullptr;
    if (query_info.text_search_info)
        text_search_info = query_info.text_search_info;
    else
        text_search_info = query_info.hybrid_search_info->text_search_info;

    if (!text_search_info)
    {
        LOG_INFO(log, "Failed to get text search info, unable to collect statistics");
        return;
    }

    String tantivy_index_file_name;
    String search_column_name = text_search_info->text_column_name;
    String query_text = text_search_info->query_text;

    for (const auto & index_desc : metadata_for_reading->getSecondaryIndices())
    {
        if (index_desc.type == TANTIVY_INDEX_NAME && index_desc.column_names.size() == 1 &&
            index_desc.column_names[0] == search_column_name)
        {
            tantivy_index_file_name = INDEX_FILE_PREFIX + index_desc.name;
            break;
        }
    }

    if (tantivy_index_file_name.empty())
    {
        LOG_INFO(log, "Failed to get index name, unable to collect statistics");
        return;
    }

    parts_with_bm25_info.resize(prepared_parts.size());

    /// Get info from tantivy index in this part
    auto process_part = [&](size_t part_index)
    {
        auto & part = prepared_parts[part_index];

        /// Check if index files exist in part
        if (!part->getDataPartStorage().exists(tantivy_index_file_name + ".idx"))
        {
            LOG_DEBUG(log, "File ({}.idx) for fts index does not exists in part {}. Skipping it",
                tantivy_index_file_name, part->name);
            return;
        }

        auto tantivy_store = TantivyIndexStoreFactory::instance().get(tantivy_index_file_name, part->getDataPartStoragePtr());

        if (tantivy_store)
        {
            auto total_docs = tantivy_store->getTotalNumDocs();
            auto total_num_tokens = tantivy_store->getTotalNumTokens();
            auto term_with_doc_nums = tantivy_store->getDocFreq(query_text);

            BM25InfoInDataPart bm_info(total_docs, total_num_tokens, term_with_doc_nums);
            parts_with_bm25_info[part_index] = std::move(bm_info);
        }
    };

    size_t num_threads = std::min<size_t>(requested_num_streams, prepared_parts.size());

    if (num_threads <= 1)
    {
        for (size_t part_index = 0; part_index < prepared_parts.size(); ++part_index)
            process_part(part_index);
    }
    else
    {
        /// Parallel loading of data parts.
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectBM25CollectThreads,
            CurrentMetrics::MergeTreeDataSelectBM25CollectThreadsActive,
            num_threads);

        for (size_t part_index = 0; part_index < prepared_parts.size(); ++part_index)
            pool.scheduleOrThrowOnError([&, part_index, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );
                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);

                process_part(part_index);
            });

        pool.wait();
    }

    /// Sum the bm25 info from all parts
    bm25_stats_in_table.total_num_docs = parts_with_bm25_info.getTotalDocsCountAllParts();
    bm25_stats_in_table.total_num_tokens = parts_with_bm25_info.getTotalNumTokensAllParts();
    bm25_stats_in_table.docs_freq = parts_with_bm25_info.getTermWithDocNumsAllParts();

    return;
}
#endif

ReadWithHybridSearch::ReadWithHybridSearch(
    MergeTreeData::DataPartsVector parts_,
    std::vector<AlterConversionsPtr> alter_conversions_,
    Names all_column_names_,
    const MergeTreeData & data_,
    const SelectQueryInfo & query_info_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
    LoggerPtr log_,
    MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_,
    bool enable_parallel_reading)
    : ReadFromMergeTree(
        parts_,
        alter_conversions_,
        all_column_names_,
        data_,
        query_info_,
        storage_snapshot_,
        context_,
        max_block_size_,
        num_streams_,
        max_block_numbers_to_read_,
        log_,
        analyzed_result_ptr_,
        enable_parallel_reading)
{
    /// Support multiple vector indices
    auto vector_scan_info_ptr = query_info.vector_scan_info;

    /// Determine if we can use two stage search
    /// Only Float32 vector can create MSTG index, and use two stage search
    /// Currently two stage search doesn't support batch distance
    if (context->getSettingsRef().two_stage_search_option > 0 && vector_scan_info_ptr && !vector_scan_info_ptr->is_batch && 
        vector_scan_info_ptr->vector_scan_descs[0].vector_search_type == Search::DataType::FloatVector &&
        metadata_for_reading->hasVectorIndexOnColumn(vector_scan_info_ptr->vector_scan_descs[0].search_column_name))
    {
        /// TODO: Currently support one distance function
        auto vector_scan_desc = vector_scan_info_ptr->vector_scan_descs[0];

        /// Get index type and disk_mode from vector index defined on the search column
        VIType type = VIType::IVFFLAT;
        int disk_mode = data.getSettings()->default_mstg_disk_mode;

        for (auto & vec_index_desc : metadata_for_reading->getVectorIndices())
        {
            if (vec_index_desc.column == vector_scan_desc.search_column_name)
            {
                Search::findEnumByName(vec_index_desc.type, type);

                const auto index_parameter = VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters);
                if (index_parameter.contains(DISK_MODE_PARAM))
                    disk_mode = index_parameter.getParam<int>(DISK_MODE_PARAM, disk_mode);

                break;
            }
        }

        if (disk_mode && (type == VIType::MSTG))
        {
            /// Prepare for number of cadidates (num_reorder) for first stage search
            VIParameter search_params = VectorIndex::convertPocoJsonToMap(vector_scan_desc.vector_parameters);

            UInt64 total_rows = 0;
            for (auto part : prepared_parts)
                total_rows += part->rows_count;

            /// Use total rows of all parts to get num_reorder for first search stage
            num_reorder = VectorIndex::FloatVI::computeFirstStageNumCandidates(type, disk_mode, total_rows, vector_scan_desc.search_column_dim, vector_scan_desc.topk, search_params);

            LOG_DEBUG(log, "num_reorder for first stage = {}", num_reorder);

            bool adaptive_two_stage = context->getSettingsRef().two_stage_search_option == 1;

            /// In adaptive two stage search option, enable only when disk_mode > 0 and saved IO count is larger than 1000
            if (adaptive_two_stage)
            {
                UInt32 total_num_reorder = 0;
                for (auto part : prepared_parts)
                {
                    /// get num_reorder for every part
                    total_num_reorder += VectorIndex::FloatVI::computeFirstStageNumCandidates(
                                            type, disk_mode, part->rows_count, vector_scan_desc.search_column_dim, vector_scan_desc.topk, search_params);
                }

                LOG_DEBUG(log, "num_reorder for first stage = {}, total_num_reorder for all parts = {}", num_reorder, total_num_reorder);

                if (total_num_reorder - num_reorder > 1000)
                    support_two_stage_search = true;
            }
            else /// Always enable
                support_two_stage_search = true;

            /// Add virtual columns which are needed for two stage seach
            if (support_two_stage_search)
            {
                for (auto & name : all_column_names)
                {
                    if (name == "_part")
                    {
                        need_remove_part_virual_column = false;
                        continue;
                    }
                    else if (name == "_part_offset")
                    {
                        need_remove_part_offset_column = false;
                        continue;
                    }
                }

                if (need_remove_part_virual_column)
                    all_column_names.emplace_back("_part");

                if (need_remove_part_offset_column)
                    all_column_names.emplace_back("_part_offset");
            }
        }
    }
}

void ReadWithHybridSearch::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
#if USE_TANTIVY_SEARCH
    /// Collect additional stastics info for bm25 when parts > 1
    if (prepared_parts.size() > 1 && (query_info.text_search_info || query_info.hybrid_search_info))
        getStatisticForTextSearch();
#endif

    /// Referenced from ReadFromMergeTree::initializePipeline(). Add logic for mark range optimization based on where conditions.
    auto result = getAnalysisResult();
    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedPartsTotal, result.total_parts);
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);
    ProfileEvents::increment(ProfileEvents::SelectedMarksTotal, result.total_marks_pk);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result, context);

    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    selected_marks = result.selected_marks;
    selected_rows = result.selected_rows;
    selected_parts = result.selected_parts;

    /// Reference spreadMarkRange()
    Names column_names_to_read = result.column_names_to_read;

    /// If there are only virtual columns in the query, should be wrong, just return.
    if (column_names_to_read.empty())
    {
        LOG_DEBUG(log, "column_names_to_read is empty");
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }


    if(isFinal(query_info))
    {
        std::set<String> add_columns;
        add_columns.insert(column_names_to_read.begin(),column_names_to_read.end());

        for(auto temp_name : metadata_for_reading->getColumnsRequiredForSortingKey())
        {
            if(!add_columns.contains(temp_name))
            {
                column_names_to_read.push_back(temp_name);
                add_columns.insert(temp_name);
            }
        }

        if (!data.merging_params.is_deleted_column.empty())
        {
            if(!add_columns.contains(data.merging_params.is_deleted_column))
            {
                column_names_to_read.push_back(data.merging_params.is_deleted_column);
                add_columns.insert(data.merging_params.is_deleted_column);
            }

            LOG_DEBUG(log, "merging_params.is_deleted_column is : {}", data.merging_params.is_deleted_column);
        }
        if (!data.merging_params.sign_column.empty())
        {
            if(!add_columns.contains(data.merging_params.sign_column))
            {
                column_names_to_read.push_back(data.merging_params.sign_column);
                add_columns.insert(data.merging_params.sign_column);
            }

            LOG_DEBUG(log, "merging_params.sign_column is : {}", data.merging_params.sign_column);
        }
        if (!data.merging_params.version_column.empty())
        {
            if(!add_columns.contains(data.merging_params.version_column))
            {
                column_names_to_read.push_back(data.merging_params.version_column);
                add_columns.insert(data.merging_params.version_column);
            }
            LOG_DEBUG(log, "merging_params.version_column is : {}", data.merging_params.version_column);
        }

    }


    /// Reference spreadMarkRangesAmongStreams()
    Pipe pipe = createReadProcessorsAmongParts(
        std::move(result.parts_with_ranges),
        requested_num_streams,
        column_names_to_read);

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    for (const auto & processor : pipe.getProcessors())
    {
        LOG_DEBUG(log, "[initializePipeline] add processor: {}", processor->getName());
        processors.emplace_back(processor);
    }

    pipeline.init(std::move(pipe));
    pipeline.addContext(context);
    // Attach QueryIdHolder if needed
    if (query_id_holder)
        pipeline.setQueryIdHolder(std::move(query_id_holder));
}


/// Reference from ReadFromMergeTree::spreadMarkRangesAmongStreams()
///
Pipe ReadWithHybridSearch::createReadProcessorsAmongParts(
    RangesInDataParts && parts_with_ranges,
    size_t num_streams,
    const Names & column_names)
{
    if (parts_with_ranges.size() == 0)
        return {};

    const auto & settings = context->getSettingsRef();

    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (parts_with_ranges.size() < num_streams)
            num_streams = parts_with_ranges.size();
    }

    auto pipe = readFromParts(std::move(parts_with_ranges), column_names, settings.use_uncompressed_cache);


    /// Add transforms for two search stage
    if (support_two_stage_search)
    {
        /// Set sort description based on vector scan column
        SortDescription sort_description;

        auto vector_scan_info_ptr = query_info.vector_scan_info;
        auto vector_scan_desc = vector_scan_info_ptr->vector_scan_descs[0];
        /// TODO: batch_distance
        sort_description.emplace_back(vector_scan_desc.column_name, vector_scan_desc.direction);

        /// First sort and merge rows (vector scan search returned unsorted result) read from a data part.
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<PartialSortingTransform>(header, sort_description);
        });

        /// MegeSorting Transform will just return if input only has one chunk.
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<MergeSortingTransform>(
                header, sort_description, block_size.max_block_size_rows, 0, num_reorder, false, 0, 0, 0, nullptr, 0);
        });

        /// Second sort rows from different pipes
        if (pipe.numOutputPorts() > 1)
        {
            auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getHeader(),
                    pipe.numOutputPorts(),
                    sort_description,
                    block_size.max_block_size_rows,
                    0,
                    SortingQueueStrategy::Batch,
                    num_reorder /// limit
                    );

            pipe.addTransform(std::move(transform));
        }

        /// Split num_reorder candidate rows based on data part and put them to different output ports for parallel
        auto split_transform = std::make_shared<VSSplitTransform>(
        pipe.getHeader(),
        num_streams,
        num_reorder
        );
        pipe.addTransform(std::move(split_transform));

        auto output_header = pipe.getHeader().cloneEmpty();

        /// Remove _part / _part_offset virtual columns if not needed for select results
        if (need_remove_part_virual_column)
            output_header.erase("_part");
        if (need_remove_part_offset_column)
            output_header.erase("_part_offset");

        auto input_header = pipe.getHeader();

        /// Add multiple VectorScanRecomputeTransforms for two stage to get accurate distance for given cadidates.
        pipe.transform([&](OutputPortRawPtrs ports)
        {
            Processors reorders;
            reorders.reserve(ports.size());

            for (auto * port : ports)
            {
                auto vector_scan_manager =
                    std::make_shared<MergeTreeVSManager>(storage_snapshot->metadata, vector_scan_info_ptr, context, support_two_stage_search);
                auto reorder = std::make_shared<VSRecomputeTransform>(
                        input_header,
                        output_header,
                        vector_scan_manager,
                        data
                        );
                connect(*port, reorder->getInputPort());
                reorders.push_back(reorder);
            }

            return reorders;
        });
    }


    if(isFinal(query_info))
    {
        /// Add generating sorting key processor
        auto sorting_expr = std::make_shared<ExpressionActions>(
            metadata_for_reading->getSortingKey().expression->getActionsDAG().clone());
        pipe.addSimpleTransform([sorting_expr](const Block & header)
                                { return std::make_shared<ExpressionTransform>(header, sorting_expr); });

        /// Add partial sort processor
        Names sort_columns = metadata_for_reading->getSortingKeyColumns();
        SortDescription sort_description;
        sort_description.compile_sort_description = settings.compile_sort_description;
        sort_description.min_count_to_compile_sort_description = settings.min_count_to_compile_sort_description;
        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);
        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(sort_columns[i], 1, 1);

        pipe.addSimpleTransform([sort_description](const Block & header)
                                {
                                    return std::make_shared<PartialSortingTransform>(header, sort_description);
                                });

        Names partition_key_columns = metadata_for_reading->getPartitionKey().column_names;

        /// Add merging final processor
        ReadFromMergeTree::addMergingFinal(
            pipe,
            sort_description,
            data.merging_params,
            partition_key_columns,
            max_block_size);

    }


    return pipe;
}

Pipe ReadWithHybridSearch::readFromParts(
    RangesInDataParts parts_with_ranges,
    Names required_columns,
    bool use_uncompressed_cache)
{
    Pipes pipes;

    if (!query_info.has_hybrid_search)
        return {};

    for (const auto & part_with_ranges : parts_with_ranges)
    {
        MergeTreeBaseSearchManagerPtr search_manager = nullptr;
        if (query_info.vector_scan_info)
        {
            search_manager = std::make_shared<MergeTreeVSManager>(
                storage_snapshot->metadata, query_info.vector_scan_info, context, support_two_stage_search);
        }
        else if (query_info.text_search_info)
        {
            auto text_search_manager = std::make_shared<MergeTreeTextSearchManager>(storage_snapshot->metadata, query_info.text_search_info, context);
#if USE_TANTIVY_SEARCH
            text_search_manager->setBM25Stats(bm25_stats_in_table);
#endif
            search_manager = text_search_manager;
        }
        else if (query_info.hybrid_search_info)
        {
            auto hybrid_search_manager = std::make_shared<MergeTreeHybridSearchManager>(storage_snapshot->metadata, query_info.hybrid_search_info, context);
#if USE_TANTIVY_SEARCH
            hybrid_search_manager->setBM25Stats(bm25_stats_in_table);
#endif
            search_manager = hybrid_search_manager;
        }

        auto algorithm = std::make_unique<MergeTreeSelectWithHybridSearchProcessor>(
            data,
            storage_snapshot,
            part_with_ranges,
            shared_virtual_fields,
            required_columns,
            use_uncompressed_cache,
            prewhere_info,
            actions_settings,
            block_size,
            reader_settings,
            search_manager,
            context,
            requested_num_streams);

        auto source = std::make_shared<MergeTreeWithVectorScanSource>(std::move(algorithm), data.getLogName());

        pipes.emplace_back(Pipe(std::move(source)));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    /// Use ConcatProcessor to concat sources together.
    /// It is needed to read in parts order (and so in PK order) if single thread is used.
    if (pipe.numOutputPorts() > 1)
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

    return pipe;

}

}
