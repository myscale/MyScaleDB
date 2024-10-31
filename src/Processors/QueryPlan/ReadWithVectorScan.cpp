#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadWithVectorScan.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/VectorScanRecomputeTransform.h>
#include <Processors/Transforms/VectorScanSplitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeVectorScanManager.h>
#include <Storages/MergeTree/MergeTreeSelectWithVectorScanProcessor.h>
#include <Storages/MergeTree/MergeTreeWithVectorScanSource.h>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedPartsTotal;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
    extern const Event SelectedMarksTotal;
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

ReadWithVectorScan::ReadWithVectorScan(
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
    LoggerPtr log_)
    : SourceStepWithFilter(DataStream{.header = MergeTreeSelectProcessor::transformHeader(
        storage_snapshot_->getSampleBlockForColumns(all_column_names_),
        query_info_.prewhere_info)}, all_column_names_, query_info_, storage_snapshot_, context_)
    , reader_settings(getMergeTreeReaderSettings(context_, query_info_))
    , prepared_parts(std::move(parts_))
    , alter_conversions_for_parts(std::move(alter_conversions_))
    , all_column_names(std::move(all_column_names_))
    , data(data_)
    , actions_settings(ExpressionActionsSettings::fromContext(context_))
    , block_size{
        .max_block_size_rows = max_block_size_,
        .preferred_block_size_bytes = context->getSettingsRef().preferred_block_size_bytes,
        .preferred_max_column_in_block_size_bytes = context->getSettingsRef().preferred_max_column_in_block_size_bytes}
    , requested_num_streams(num_streams_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , log(std::move(log_))
{
    /// Determine if we can use two stage search
    if (context->getSettingsRef().two_stage_search_option > 0 && !storage_snapshot->metadata->vec_indices.empty())
    {
        /// Currently support one vector index
        auto vector_index = storage_snapshot->metadata->vec_indices[0];

        /// Check vector index type
        Search::IndexType type;
        Search::findEnumByName(vector_index.type, type);

        int disk_mode = data.getSettings()->default_mstg_disk_mode;

        const auto index_parameter = VectorIndex::convertPocoJsonToMap(vector_index.parameters);
        if (index_parameter.contains("disk_mode"))
            disk_mode = index_parameter.getParam<int>("disk_mode", disk_mode);

        auto vector_scan_info_ptr = query_info.vector_scan_info;

        bool adaptive_two_stage = context->getSettingsRef().two_stage_search_option == 1;

        /// Currently two stage search doesn't support batch distance
        if (disk_mode && (type == Search::IndexType::MSTG) && vector_scan_info_ptr && !vector_scan_info_ptr->is_batch)
        {
            /// Prepare for number of cadidates (num_reorder) for first stage search
            auto vector_scan_desc = vector_scan_info_ptr->vector_scan_descs[0];
            Search::Parameters search_params = VectorIndex::convertPocoJsonToMap(vector_scan_desc.vector_parameters);

            UInt64 total_rows = 0;
            for (auto part : prepared_parts)
                total_rows += part->rows_count;

            /// Use total rows of all parts to get num_reorder for first search stage
            num_reorder = VectorIndex::SearchVectorIndex::computeFirstStageNumCandidates(type, disk_mode, total_rows, vector_scan_desc.search_column_dim, vector_scan_desc.topk, search_params);

            LOG_DEBUG(log, "num_reorder for first stage = {}", num_reorder);

            /// In adaptive two stage search option, enable only when disk_mode > 0 and saved IO count is larger than 1000
            if (adaptive_two_stage)
            {
                UInt32 total_num_reorder = 0;
                for (auto part : prepared_parts)
                {
                    /// get num_reorder for every part
                    total_num_reorder += VectorIndex::SearchVectorIndex::computeFirstStageNumCandidates(
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

ReadFromMergeTree::AnalysisResultPtr ReadWithVectorScan::selectRangesToRead(bool find_exact_ranges) const
{
    std::optional<ReadFromMergeTree::Indexes> emptyOptional = std::nullopt;
    return ReadFromMergeTree::selectRangesToRead(
        std::move(prepared_parts),
        std::move(alter_conversions_for_parts),
        storage_snapshot->metadata,
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        all_column_names,
        log,
        /*indexes*/ emptyOptional,
        find_exact_ranges);
}

ReadFromMergeTree::AnalysisResult ReadWithVectorScan::getAnalysisResult() const
{
    if (!analyzed_result_ptr)
        analyzed_result_ptr = selectRangesToRead();

    return *analyzed_result_ptr;
}

void ReadWithVectorScan::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
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

Pipe ReadWithVectorScan::createReadProcessorsAmongParts(
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
        auto split_transform = std::make_shared<VectorScanSplitTransform>(
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
                    std::make_shared<MergeTreeVectorScanManager>(storage_snapshot->metadata, vector_scan_info_ptr, context, support_two_stage_search);
                auto reorder = std::make_shared<VectorScanRecomputeTransform>(
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

    return pipe;
}

Pipe ReadWithVectorScan::readFromParts(
    RangesInDataParts parts_with_ranges,
    Names required_columns,
    bool use_uncompressed_cache)
{
    Pipes pipes;
    auto vector_scan_info_ptr = query_info.vector_scan_info;
    if (!vector_scan_info_ptr)
        return {};

    /// Prewhere info should not be changed, because it is shared by parts.
    if (prewhere_info)
    {
        /// need_filter is false when both prewhere and where exist, prewhere will be delayed, all read rows with a prehwere_column returned.
        /// In this case, we need only rows statisfied prewhere conditions.
        prewhere_info->need_filter = true;
    }

    for (const auto & part_with_ranges : parts_with_ranges)
    {
        auto vector_scan_manager =
            std::make_shared<MergeTreeVectorScanManager>(storage_snapshot->metadata, vector_scan_info_ptr, context, support_two_stage_search);

        auto algorithm = std::make_unique<MergeTreeSelectWithVectorScanProcessor>(
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
            vector_scan_manager);

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
