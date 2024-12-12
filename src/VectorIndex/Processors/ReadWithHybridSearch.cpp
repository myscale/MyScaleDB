#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/scope_guard_safe.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <VectorIndex/Storages/MergeTreeWithVectorScanSource.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <VectorIndex/Storages/MergeTreeHybridSearchManager.h>
#include <VectorIndex/Storages/MergeTreeTextSearchManager.h>
#include <VectorIndex/Storages/MergeTreeVSManager.h>
#include <VectorIndex/Storages/MergeTreeSelectWithHybridSearchProcessor.h>
#include <VectorIndex/Processors/ReadWithHybridSearch.h>

#if USE_TANTIVY_SEARCH
#    include <Interpreters/TantivyFilter.h>
#    include <Storages/MergeTree/TantivyIndexStore.h>
#    include <Storages/MergeTree/TantivyIndexStoreFactory.h>
#    include <VectorIndex/Common/BM25InfoInDataParts.h>
#    include <VectorIndex/Utils/CommonUtils.h>
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
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled; /// TODO: use proper scheduled
    extern const Metric ReadWithHybridSearchSecondStageThreads;
    extern const Metric ReadWithHybridSearchSecondStageThreadsActive;
    extern const Metric MergeTreeDataSelectHybridSearchThreads;
    extern const Metric MergeTreeDataSelectHybridSearchThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

[[maybe_unused]] static MergeTreeReaderSettings getMergeTreeReaderSettings(
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
    String index_name;
    String db_table_name;

    /// Support multiple text columns in table function
    bool from_table_function = text_search_info->from_table_func;

    if (!data.getStorageID().database_name.empty())
        db_table_name = data.getStorageID().database_name + ".";
    db_table_name += data.getStorageID().table_name;

    for (const auto & index_desc : getStorageMetadata()->getSecondaryIndices())
    {
        if (index_desc.type == TANTIVY_INDEX_NAME && ((from_table_function && index_desc.name == text_search_info->index_name)
            || (!from_table_function && index_desc.column_names.size() == 1 && index_desc.column_names[0] == search_column_name)))
        {
            index_name = index_desc.name;
            tantivy_index_file_name = INDEX_FILE_PREFIX + index_desc.name;
            break;
        }
    }

    if (tantivy_index_file_name.empty())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                        "The query was canceled because the table {} lacks a full-text search (FTS) index. "
                        "Please create a FTS index before running TextSearch() or HybridSearch() or full_text_search()", db_table_name);

    parts_with_bm25_info.resize(prepared_parts.size());

    /// Get info from tantivy index in this part
    auto process_part = [&](size_t part_index)
    {
        auto & part = prepared_parts[part_index];

        /// Check if index files exist in part
        if (!part->getDataPartStorage().exists(tantivy_index_file_name + ".idx"))
        {
            String suggestion_index_log = "ALTER TABLE " + db_table_name + " MATERIALIZE INDEX " + index_name;

            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                            "The query was canceled because the FTS index {} has not been built for part {} on table {}. "
                            "Please run the MATERIALIZE INDEX command {} to build the FTS index for existing data. "
                            "If you have already run this command, please wait for it to finish.",
                            index_name, part->name, db_table_name, suggestion_index_log);
        }
        LOG_DEBUG(
            this->log,
            "[getStatisticForTextSearch] part_rel_path: {}, part_status: {}",
            part->getDataPartStoragePtr()->getRelativePath(),
            part->getNameWithState());
        auto tantivy_store
            = TantivyIndexStoreFactory::instance().getOrLoadForSearch(tantivy_index_file_name, part->getDataPartStoragePtr());

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
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
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
    bm25_stats_in_table.total_num_tokens = parts_with_bm25_info.getTextColsTotalNumTokensAllParts();
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
    AnalysisResultPtr analyzed_result_ptr_,
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
    VectorScanInfoPtr vector_scan_info = nullptr;
    if (query_info.vector_scan_info)
        vector_scan_info = query_info.vector_scan_info;
    else if (query_info.hybrid_search_info)
        vector_scan_info = query_info.hybrid_search_info->vector_scan_info;

    if (vector_scan_info)
    {
        /// Mark for each vector scan description
        size_t vector_scan_descs_size = vector_scan_info->vector_scan_descs.size();
        vec_support_two_stage_searches.resize(vector_scan_descs_size, false);
        vec_num_reorders.resize(vector_scan_descs_size, 0);
        /// MYSCALE_INTERNAL_CODE_BEGIN
        supportTwoStageSearch(prepared_parts, vector_scan_info, context->getSettingsRef(),
                            getStorageMetadata(), data.getSettings()->default_mstg_disk_mode, query_info, log);
        /// MYSCALE_INTERNAL_CODE_END
    }
}

/// MYSCALE_INTERNAL_CODE_BEGIN
void ReadWithHybridSearch::supportTwoStageSearch(
    const MergeTreeData::DataPartsVector & prepared_parts_,
    const VectorScanInfoPtr & vector_scan_info_ptr,
    const Settings & settings,
    const StorageMetadataPtr & metadata_for_reading,
    const int default_mstg_disk_mode,
    const SelectQueryInfo & query_info_,
    LoggerPtr log)
{
    if (!vector_scan_info_ptr)
        return;

    /// Support multiple distance functions
    size_t vector_scan_descs_size = vector_scan_info_ptr->vector_scan_descs.size();

    /// Two stage search is disabled
    if (settings.two_stage_search_option == 0)
        return;

    /// Currently two stage search doesn't support batch distance
    if (vector_scan_info_ptr->is_batch)
        return;

    /// TODO: In adaptive two stage search option, disable two stage search for FINAL
    if (query_info_.isFinal() && settings.two_stage_search_option == 1)
        return;

    /// It is allowed that some vector scans with two-stage, but others not
    for (size_t i = 0; i < vector_scan_descs_size; ++i)
    {
        const auto & vec_scan_desc = vector_scan_info_ptr->vector_scan_descs[i];

        /// Only Float32 vector can create MSTG index, and possible use two stage search
        if (vec_scan_desc.vector_search_type != Search::DataType::FloatVector)
            continue;

        /// Get index type and disk_mode from vector index defined on the search column
        VectorIndex::VIType type = VectorIndex::VIType::IVFFLAT;
        int disk_mode = default_mstg_disk_mode;

        for (auto & vec_index_desc : metadata_for_reading->getVectorIndices())
        {
            if (vec_index_desc.column == vec_scan_desc.search_column_name)
            {
                Search::findEnumByName(vec_index_desc.type, type);

                const auto index_parameter = VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters);
                if (index_parameter.contains(DISK_MODE_PARAM))
                    disk_mode = index_parameter.getParam<int>(DISK_MODE_PARAM, disk_mode);

                break;
            }
        }

        bool support_two_stage = false;
        UInt64 num_reorder = 0;

        if (disk_mode && (type == VectorIndex::VIType::MSTG))
        {
            /// Prepare for number of cadidates (num_reorder) for first stage search
            VectorIndex::VIParameter search_params = VectorIndex::convertPocoJsonToMap(vec_scan_desc.vector_parameters);

            UInt64 total_rows = 0;
            for (auto part : prepared_parts_)
                total_rows += part->rows_count;

            /// Use total rows of all parts to get num_reorder for first search stage
            num_reorder = VectorIndex::FloatInnerSegment::computeFirstStageNumCandidates(type, disk_mode, total_rows, vec_scan_desc.search_column_dim, vec_scan_desc.topk, search_params);

            LOG_DEBUG(log, "search column {}'s num_reorder for first stage = {}", vec_scan_desc.search_column_name, num_reorder);

            bool adaptive_two_stage = settings.two_stage_search_option == 1;

            /// In adaptive two stage search option, enable only when disk_mode > 0 and saved IO count is larger than 1000
            if (adaptive_two_stage)
            {
                UInt32 total_num_reorder = 0;
                for (auto part : prepared_parts_)
                {
                    /// get num_reorder for every part
                    total_num_reorder += VectorIndex::FloatInnerSegment::computeFirstStageNumCandidates(
                                            type, disk_mode, part->rows_count, vec_scan_desc.search_column_dim, vec_scan_desc.topk, search_params);
                }

                LOG_DEBUG(log, "num_reorder for first stage = {}, total_num_reorder for all parts = {}", num_reorder, total_num_reorder);

                if (total_num_reorder - num_reorder > 1000)
                    support_two_stage = true;
            }
            else /// Always enable
                support_two_stage = true;

            vec_support_two_stage_searches[i] = support_two_stage;
            vec_num_reorders[i] = num_reorder;
        }
    }

    return;
}
/// MYSCALE_INTERNAL_CODE_END

void ReadWithHybridSearch::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    OpenTelemetry::SpanHolder span("ReadWithHybridSearch::initializePipeline()");
#if USE_TANTIVY_SEARCH
    OpenTelemetry::SpanHolder span_text_stats("ReadWithHybridSearch getStatisticForTextSearch()");

    /// As for Distributd table, the statistic info is already collected in the sclalar block
    if (getContext()->hasScalar("_fts_statistic_info"))
    {
        Block block = getContext()->getScalar("_fts_statistic_info");
        if (block.rows() != 1)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Got the wrong Fts statistics info for Distributed BM25 calculation");

        UInt64 total_docs = 0;
        std::map<UInt32, UInt64> total_tokens_map;
        std::map<std::pair<UInt32, String>, UInt64> terms_freq_map;

        parseBM25StaisiticsInfo(block, 0, total_docs, total_tokens_map, terms_freq_map);

        bm25_stats_in_table.total_num_docs = total_docs;

        bm25_stats_in_table.total_num_tokens.reserve(total_tokens_map.size());
        for (const auto & [field_id, total_tokens] : total_tokens_map)
        {
            bm25_stats_in_table.total_num_tokens.push_back({field_id, total_tokens});
        }

        bm25_stats_in_table.docs_freq.reserve(terms_freq_map.size());
        for (const auto & [field_and_term, doc_freq] : terms_freq_map)
        {
            bm25_stats_in_table.docs_freq.push_back({field_and_term.second, field_and_term.first, doc_freq});
        }
    }
    /// As for non-Distributd table, collect additional stastics info for bm25 when parts > 1
    else if (prepared_parts.size() > 1 && (query_info.text_search_info || query_info.hybrid_search_info))
        getStatisticForTextSearch();
#endif

    OpenTelemetry::SpanHolder span_skip_index("ReadWithHybridSearch getAnalysisResult()");
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

    Pipe pipe;
    /// TODO: batch_distance refactor
    if (query_info.vector_scan_info && query_info.vector_scan_info->is_batch)
    {
        if (query_info.isFinal())
        {
            std::set<String> add_columns;
            add_columns.insert(column_names_to_read.begin(),column_names_to_read.end());

            for(auto temp_name : getStorageMetadata()->getColumnsRequiredForSortingKey())
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
        pipe = createReadProcessorsAmongParts(
            std::move(result.parts_with_ranges),
            requested_num_streams,
            column_names_to_read);
    }
    else if (query_info.has_hybrid_search)
    {
        /// The result of hybrid search on multiple parts are identical or similar as one part cases.
        /// Hence, for hybrid search, first we need to get top k result for vector search and text search from all selected parts,
        /// Second do the fusion on table level, final read other columns on parts to merge result.
        HybridAnalysisResult hybrid_result = getHybridSearchResult(std::move(result.parts_with_ranges));

        pipe = createReadProcessorsAmongParts(
            std::move(hybrid_result.parts_with_hybrid_and_ranges),
            column_names_to_read);
    }

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

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

    if(query_info.isFinal())
    {
        /// Add generating sorting key processor
        auto sorting_expr = std::make_shared<ExpressionActions>(
            getStorageMetadata()->getSortingKey().expression->getActionsDAG().clone());
        pipe.addSimpleTransform([sorting_expr](const Block & header)
                                { return std::make_shared<ExpressionTransform>(header, sorting_expr); });

        /// Add partial sort processor
        Names sort_columns = getStorageMetadata()->getSortingKeyColumns();
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

        Names partition_key_columns = getStorageMetadata()->getPartitionKey().column_names;

        /// Add merging final processor
        ReadFromMergeTree::addMergingFinal(
            pipe,
            sort_description,
            data.merging_params,
            partition_key_columns,
            getMaxBlockSize(),
            enable_vertical_final);

    }

    return pipe;
}

Pipe ReadWithHybridSearch::readFromParts(
    RangesInDataParts parts_with_ranges,
    Names required_columns,
    bool use_uncompressed_cache)
{
    Pipes pipes;

    /// Handle batch_distance only
    if (!query_info.vector_scan_info || !query_info.vector_scan_info->is_batch)
        return {};

    for (const auto & part_with_ranges : parts_with_ranges)
    {
        MergeTreeBaseSearchManagerPtr search_manager = nullptr;
        search_manager = std::make_shared<MergeTreeVSManager>(getStorageMetadata(), query_info.vector_scan_info, context, false);

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
    return pipe;
}

ReadWithHybridSearch::HybridAnalysisResult ReadWithHybridSearch::getHybridSearchResult(const RangesInDataParts & parts_with_ranges) const
{
    return selectTotalHybridResult(parts_with_ranges, getStorageMetadata(), requested_num_streams);
}

ReadWithHybridSearch::HybridAnalysisResult ReadWithHybridSearch::selectTotalHybridResult(
    const RangesInDataParts & parts_with_ranges,
    const StorageMetadataPtr & metadata_snapshot,
    size_t num_streams) const
{
    OpenTelemetry::SpanHolder span("ReadWithHybridSearch::selectTotalHybridResult()");
    HybridAnalysisResult hybrid_result;
    MergeTreeReadTask::BlockSizeParams block_size_params;
    block_size_params.max_block_size_rows = getMaxBlockSize();

    /// Get vector scan and full-text search result for all selected parts
    auto parts_with_vector_text_result = MergeTreeSelectWithHybridSearchProcessor::selectPartsByVectorAndTextIndexes(
        parts_with_ranges,
        metadata_snapshot,
        query_info,
        vec_support_two_stage_searches,
#if USE_TANTIVY_SEARCH
        bm25_stats_in_table,
#endif
        prewhere_info,
        storage_snapshot,
        context,
        block_size_params,
        num_streams,
        data,
        reader_settings);

    /// Mark if vector scan or full-text search is needed.
    VectorScanInfoPtr vector_scan_info = query_info.vector_scan_info;
    TextSearchInfoPtr text_search_info = query_info.text_search_info;
    bool hybrid = false;
    String log_name;

    if (query_info.hybrid_search_info)
    {
        hybrid = true;
        log_name = "selectTotalHybridResult";
        vector_scan_info = query_info.hybrid_search_info->vector_scan_info;
        text_search_info = query_info.hybrid_search_info->text_search_info;
    }
    else
        log_name = vector_scan_info ? "selectTotalVectorScanResult" : "selectTotalTextResult";

    /// If final is used, remove duplicate results from vector scan and/or full-text search results of all selected parts
    if(query_info.isFinal() && parts_with_vector_text_result.size() > 1)
    {
        LOG_DEBUG(log, "Perform final on search results from parts");
        performFinal(parts_with_ranges, parts_with_vector_text_result, num_streams);
    }

    LoggerPtr hybrid_log = getLogger(log_name);

    std::unordered_map<String, ScoreWithPartIndexAndLabels> multiple_distances_topk_results_map;
    if (vector_scan_info)
    {
        /// Check for each vector scan desc
        size_t descs_size = vector_scan_info->vector_scan_descs.size();

        std::vector<ScoreWithPartIndexAndLabels> distances_topk_results_vector;
        distances_topk_results_vector.resize(descs_size);

        auto get_total_topk_on_single_col = [&](size_t desc_index)
        {
            /// Combine vector scan results from selected parts to get top-k result for vector scan.
            ScoreWithPartIndexAndLabels vec_scan_topk_results;

            const auto & vector_scan_desc = vector_scan_info->vector_scan_descs[desc_index];

            /// MYSCALE_INTERNAL_CODE_BEGIN
            bool support_two_stage_search = vec_support_two_stage_searches[desc_index];
            if (support_two_stage_search)
            {
                OpenTelemetry::SpanHolder span2("ReadWithHybridSearch::selectTotalHybridResult(): vector two stage search");
                /// Second stage: get actual top k result

                UInt64 num_reorder = vec_num_reorders[desc_index];
                /// Get top num_reorder candidates: score + part_index + label_id
                auto first_stage_top_candidates = MergeTreeBaseSearchManager::getTotalCandidateVSResult(
                            parts_with_vector_text_result, desc_index, vector_scan_desc, num_reorder, hybrid_log);

                /// Split num_reorder candidates based on part index: part + vector scan results
                auto parts_with_first_stage_top_results = MergeTreeVSManager::splitFirstStageVSResult(
                    parts_with_vector_text_result, first_stage_top_candidates, vector_scan_desc, hybrid_log);

                /// Get accurate distance for candidates from all selected part
                auto parts_with_second_stage_vector_result = selectPartsBySecondStageVectorIndex(
                    parts_with_first_stage_top_results,
                    vector_scan_desc,
                    num_streams);

                /// Get final top k result
                vec_scan_topk_results = MergeTreeBaseSearchManager::getTotalTopKVSResult(
                            parts_with_second_stage_vector_result, 0, vector_scan_desc, hybrid_log);
            }
            else   /// MYSCALE_INTERNAL_CODE_END
                vec_scan_topk_results = MergeTreeBaseSearchManager::getTotalTopKVSResult(
                    parts_with_vector_text_result, desc_index, vector_scan_desc, hybrid_log);

            /// Save the topk results for this vector scan
            distances_topk_results_vector[desc_index] = vec_scan_topk_results;
        };

        size_t num_threads = std::min<size_t>(num_streams, descs_size);
        if (num_threads <= 1)
        {
            for (size_t desc_index = 0; desc_index < descs_size; ++desc_index)
                get_total_topk_on_single_col(desc_index);
        }
        else
        {
            /// Parallel executing get total topk and possible two search stage
            ThreadPool pool(
                CurrentMetrics::MergeTreeDataSelectHybridSearchThreads,
                CurrentMetrics::MergeTreeDataSelectHybridSearchThreadsActive,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
                num_threads);

            for (size_t desc_index = 0; desc_index < descs_size; ++desc_index)
                pool.scheduleOrThrowOnError([&, desc_index]()
                {
                    get_total_topk_on_single_col(desc_index);
                });

            pool.wait();
        }

        /// Save vector scan results in to a map with result column name as key.
        for (size_t i = 0; i < descs_size; ++i)
        {
            const auto & vector_scan_desc = vector_scan_info->vector_scan_descs[i];
            const String & result_column_name = vector_scan_desc.column_name;
            multiple_distances_topk_results_map[result_column_name] = distances_topk_results_vector[i];
        }
    }

    /// Combine text search results from selected parts to get top-k result for text search.
    ScoreWithPartIndexAndLabels text_search_topk_results;
    if (text_search_info)
        text_search_topk_results = MergeTreeBaseSearchManager::getTotalTopKTextResult(parts_with_vector_text_result, text_search_info, hybrid_log);

    /// hybrid search or text search
    if (text_search_info)
    {
        ScoreWithPartIndexAndLabels hybrid_topk_results;

        if (hybrid)
        {
            /// Only has one vector scan for hybrid search
            ScoreWithPartIndexAndLabels vec_scan_topk_results;
            if (multiple_distances_topk_results_map.size() == 1)
                vec_scan_topk_results = multiple_distances_topk_results_map.begin()->second;

            /// Do the fusion on the total top-k result of vector scan and text search from all selected parts, based on (part_index, label_id).
            hybrid_topk_results = MergeTreeHybridSearchManager::hybridSearch(vec_scan_topk_results, text_search_topk_results, query_info.hybrid_search_info, hybrid_log);
        }
        else
        {
            /// Only simple text search, save result to hybrid_topk_results
            hybrid_topk_results = text_search_topk_results;
        }

        /// Filter parts with final top-k hybrid result, and save hybrid result with belonged part
        hybrid_result.parts_with_hybrid_and_ranges = MergeTreeHybridSearchManager::FilterPartsWithHybridResults(
                                        parts_with_vector_text_result, parts_with_ranges, hybrid_topk_results, context->getSettingsRef(), hybrid_log);
    }
    else
    {
        /// Support multiple distance functions
        /// Filter parts with final top-k vector scan results from multiple distance funcs
        hybrid_result.parts_with_hybrid_and_ranges = MergeTreeVSManager::FilterPartsWithManyVSResults(
                                        parts_with_vector_text_result, parts_with_ranges, multiple_distances_topk_results_map, context->getSettingsRef(), hybrid_log);
    }

    return hybrid_result;
}

void ReadWithHybridSearch::performFinal(
    const RangesInDataParts & parts_with_ranges,
    VectorAndTextResultInDataParts & parts_with_vector_text_result,
    size_t num_streams) const
{
    OpenTelemetry::SpanHolder span("ReadWithHybridSearch::performFinal()");
    const auto & settings = context->getSettingsRef();

    /// Construct a local RangesInDataParts based on top k search results
    RangesInDataParts parts_for_final_ranges;
    parts_for_final_ranges.resize(parts_with_vector_text_result.size());

    /// Save all labels in top-k results of one part
    std::vector<std::pair<String, std::set<UInt64>>> vec_original_labels_in_parts;
    vec_original_labels_in_parts.resize(parts_with_vector_text_result.size());

    auto process_part = [&](size_t part_index)
    {
        auto & part_with_mix_results = parts_with_vector_text_result[part_index];
        String part_name = part_with_mix_results.data_part->name;

        /// Get all labels from vector scan result and/or text result
        auto labels_set = MergeTreeBaseSearchManager::getLabelsInSearchResults(part_with_mix_results, log);
        if (labels_set.empty())
            return;

        /// Use labels in search result to filter mark ranges of part
        const auto & part_with_ranges = parts_with_ranges[part_with_mix_results.part_index];
        RangesInDataPart result_ranges(part_with_ranges);
        filterMarkRangesByLabels(part_with_ranges.data_part, settings, labels_set, result_ranges.ranges);

        if (!result_ranges.ranges.empty())
        {
            parts_for_final_ranges[part_index] = std::move(result_ranges);
            vec_original_labels_in_parts[part_index] = std::make_pair(part_name, std::move(labels_set));
        }
    };

    size_t num_threads = std::min<size_t>(num_streams, parts_with_vector_text_result.size());

    if (num_threads <= 1)
    {
        for (size_t part_index = 0; part_index < parts_with_vector_text_result.size(); ++part_index)
            process_part(part_index);
    }
    else
    {
        /// Parallel loading of data parts.
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreads,
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreadsActive,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
            num_threads);

        for (size_t part_index = 0; part_index < parts_with_vector_text_result.size(); ++part_index)
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

    /// Erase part with empty labels
    for (auto it = parts_for_final_ranges.begin(); it != parts_for_final_ranges.end();)
    {
        if (it->ranges.empty())
            it = parts_for_final_ranges.erase(it);
        else
            ++it;
    }

    /// search result inside parts_with_vector_text_result may be empty (computed = false).
    if (parts_for_final_ranges.empty())
    {
        LOG_DEBUG(log, "parts_for_final_ranges is empty");
        return;
    }
    else if (parts_for_final_ranges.size() == 1)
    {
        LOG_DEBUG(log, "parts_for_final_ranges size is 1, no need to performFinal()");
        return;
    }

    /// Reference spreadMarkRangesAmongStreamsFinal() for read with final
    Names column_names_to_read;

    /// Add columns needed to calculate the sorting expression and the sign.
    std::vector<String> add_columns = getStorageMetadata()->getColumnsRequiredForSortingKey();
    column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

    if (!data.merging_params.is_deleted_column.empty())
        column_names_to_read.push_back(data.merging_params.is_deleted_column);
    if (!data.merging_params.sign_column.empty())
        column_names_to_read.push_back(data.merging_params.sign_column);
    if (!data.merging_params.version_column.empty())
        column_names_to_read.push_back(data.merging_params.version_column);

    ::sort(column_names_to_read.begin(), column_names_to_read.end());
    column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

    /// Add virtual columns
    column_names_to_read.push_back("_part");
    column_names_to_read.push_back("_part_offset");

    ExpressionActionsSettings local_actions_settings;

    auto pipe = createReadInOrderFromPartsSource(
        parts_for_final_ranges,
        column_names_to_read,
        storage_snapshot,
        data.getLogName(),
        /*prewhere_info*/ nullptr,
        local_actions_settings,
        reader_settings,
        block_size,
        context,
        /*max_streams*/ 1,
        /*min_marks_for_concurrent_read*/ 0,
        settings.use_uncompressed_cache);

    auto sorting_expr = std::make_shared<ExpressionActions>(
        getStorageMetadata()->getSortingKey().expression->getActionsDAG().clone());

    pipe.addSimpleTransform([sorting_expr](const Block & header)
                            { return std::make_shared<ExpressionTransform>(header, sorting_expr); });

    Names sort_columns = getStorageMetadata()->getSortingKeyColumns();
    SortDescription sort_description;
    sort_description.compile_sort_description = settings.compile_sort_description;
    sort_description.min_count_to_compile_sort_description = settings.min_count_to_compile_sort_description;

    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = getStorageMetadata()->getPartitionKey().column_names;

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

    addMergingFinal(
        pipe,
        sort_description,
        data.merging_params,
        partition_key_columns,
        getMaxBlockSize(),
        enable_vertical_final);

    QueryPipeline final_pipeline(std::move(pipe));
    PullingPipelineExecutor final_executor(final_pipeline);

    /// Save final results to another part and labels map, only include results in top-k labels
    std::map<String, std::set<UInt64>> final_part_labels_map;

    OpenTelemetry::SpanHolder span2("ReadWithHybridSearch::performFinal(): execute final and iterate results");
    Block block;
    while (final_executor.pull(block))
    {
        const PaddedPODArray<UInt64> & col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column).getData();

        /// Handle LowCardinality cases
        auto part_column = block.getByName("_part").column->convertToFullColumnIfLowCardinality();
        const ColumnString & part_col = typeid_cast<const ColumnString &>(*part_column);

        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            String part_name = part_col[i].safeGet<String>();
            UInt64 label = col_data[i];

            /// Check if the label is from original top-k results in this part
            for (const auto & [orig_part_name, orig_labels] : vec_original_labels_in_parts)
            {
                if (orig_part_name == part_name && orig_labels.contains(label))
                {
                    final_part_labels_map[part_name].emplace(label);
                    break;
                }
            }
        }
    }

    OpenTelemetry::SpanHolder span3("ReadWithHybridSearch::performFinal(): filterSearchResultsByFinalLabels");
    /// Filter top-k results in parts_with_vector_text_result
    /// For a result in a part, remove it if not exists in final results of this part
    for (auto & part_with_mix_results : parts_with_vector_text_result)
    {
        String part_name = part_with_mix_results.data_part->name;

        if (final_part_labels_map.contains(part_name) && !final_part_labels_map[part_name].empty())
        {
            auto & part_labels = final_part_labels_map[part_name];
            MergeTreeBaseSearchManager::filterSearchResultsByFinalLabels(part_with_mix_results, part_labels, log);
        }
        else
        {
            /// part not exists in final results
            part_with_mix_results.text_search_result = nullptr;
            part_with_mix_results.vector_scan_results.clear();
        }
    }
}

VectorAndTextResultInDataParts ReadWithHybridSearch::selectPartsBySecondStageVectorIndex(
    const VectorAndTextResultInDataParts & parts_with_candidates,
    const VSDescription & vector_scan_desc,
    size_t num_streams) const
{
    OpenTelemetry::SpanHolder span3("ReadWithHybridSearch::selectPartsBySecondStageVectorIndex()");
    VectorAndTextResultInDataParts parts_with_vector_result;
    parts_with_vector_result.resize(parts_with_candidates.size());

    /// Execute second stage vector scan in this part.
    auto process_part = [&](size_t part_index)
    {
        auto & part_with_candidates = parts_with_candidates[part_index];
        auto & data_part = part_with_candidates.data_part;

        VectorAndTextResultInDataPart vector_result(part_with_candidates.part_index, data_part);

        auto two_stage_vector_scan_result = MergeTreeVSManager::executeSecondStageVectorScan(data_part, vector_scan_desc, part_with_candidates.vector_scan_results[0]);
        vector_result.vector_scan_results.emplace_back(two_stage_vector_scan_result);

        parts_with_vector_result[part_index] = std::move(vector_result);
    };

    size_t num_threads = std::min<size_t>(num_streams, parts_with_candidates.size());

    if (num_threads <= 1)
    {
        for (size_t part_index = 0; part_index < parts_with_candidates.size(); ++part_index)
            process_part(part_index);
    }
    else
    {
        /// Parallel loading of data parts.
        ThreadPool pool(
            CurrentMetrics::ReadWithHybridSearchSecondStageThreads,
            CurrentMetrics::ReadWithHybridSearchSecondStageThreadsActive,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
            num_threads);

        for (size_t part_index = 0; part_index < parts_with_candidates.size(); ++part_index)
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

    return parts_with_vector_result;
}

/// Reference from ReadFromMergeTree::spreadMarkRangesAmongStreams()
Pipe ReadWithHybridSearch::createReadProcessorsAmongParts(
    SearchResultAndRangesInDataParts parts_with_hybrid_ranges,
    const Names & column_names)
{
    if (parts_with_hybrid_ranges.size() == 0)
        return {};

    const auto & settings = context->getSettingsRef();

    size_t num_streams = requested_num_streams;
    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (parts_with_hybrid_ranges.size() < num_streams)
            num_streams = parts_with_hybrid_ranges.size();
    }

    /// Don't consider parallel reading scenario
    auto pipe = readFromParts(std::move(parts_with_hybrid_ranges), column_names, settings.use_uncompressed_cache);

    return pipe;
}

Pipe ReadWithHybridSearch::readFromParts(
    const SearchResultAndRangesInDataParts & parts_with_hybrid_ranges,
    Names required_columns,
    bool use_uncompressed_cache)
{
    Pipes pipes;

    if (!query_info.has_hybrid_search)
        return {};

    for (const auto & part_with_hybrid : parts_with_hybrid_ranges)
    {
        /// Already have search result for data part, save it to search_manager.
        /// Support multiple distance functions, check multiple_vector_scan_results
        if (query_info.vector_scan_info)
        {
            if (part_with_hybrid.multiple_vector_scan_results.empty())
                continue;
        }
        else
        {
            /// Check search_result for text and hybrid search
            if (!part_with_hybrid.search_result || !part_with_hybrid.search_result->computed)
                continue;
        }

        MergeTreeBaseSearchManagerPtr search_manager = nullptr;
        auto & part_with_ranges = part_with_hybrid.part_with_ranges;

        if (query_info.hybrid_search_info)
            search_manager = std::make_shared<MergeTreeHybridSearchManager>(part_with_hybrid.search_result, query_info.hybrid_search_info);
        else if (query_info.vector_scan_info)
            search_manager = std::make_shared<MergeTreeVSManager>(part_with_hybrid.multiple_vector_scan_results, query_info.vector_scan_info);
        else if (query_info.text_search_info)
            search_manager = std::make_shared<MergeTreeTextSearchManager>(part_with_hybrid.search_result, query_info.text_search_info);

        if (!search_manager)
        {
            /// Should not happen
            LOG_WARNING(log, "Failed to initialize search manager for part {}", part_with_ranges.data_part->name);
            continue;
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
            requested_num_streams,
            part_with_hybrid.can_skip_perform_prefilter);

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
