#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
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
#include <Storages/MergeTree/MergeTreeSource.h>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
}

namespace DB
{

static MergeTreeReaderSettings getMergeTreeReaderSettings(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    return
    {
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = settings.checksum_on_read,
    };
}

static const PrewhereInfoPtr & getPrewhereInfo(const SelectQueryInfo & query_info)
{
    return query_info.projection ? query_info.projection->prewhere_info
                                 : query_info.prewhere_info;
}

ReadWithVectorScan::ReadWithVectorScan(
    MergeTreeData::DataPartsVector parts_,
    Names real_column_names_,
    Names virt_column_names_,
    const MergeTreeData & data_,
    const SelectQueryInfo & query_info_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_,
    bool sample_factor_column_queried_,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
    Poco::Logger * log_,
    bool enable_parallel_reading)
    : SourceStepWithFilter(DataStream{.header = IMergeTreeSelectAlgorithm::transformHeader(
        storage_snapshot_->getSampleBlockForColumns(real_column_names_),
        getPrewhereInfo(query_info_),
        data_.getPartitionValueType(),
        virt_column_names_)})
    , reader_settings(getMergeTreeReaderSettings(context_))
    , prepared_parts(std::move(parts_))
    , real_column_names(std::move(real_column_names_))
    , virt_column_names(std::move(virt_column_names_))
    , data(data_)
    , query_info(query_info_)
    , prewhere_info(getPrewhereInfo(query_info))
    , actions_settings(ExpressionActionsSettings::fromContext(context_))
    , storage_snapshot(std::move(storage_snapshot_))
    , metadata_for_reading(storage_snapshot->getMetadataForQuery())
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , requested_num_streams(num_streams_)
    , preferred_block_size_bytes(context->getSettingsRef().preferred_block_size_bytes)
    , preferred_max_column_in_block_size_bytes(context->getSettingsRef().preferred_max_column_in_block_size_bytes)
    , sample_factor_column_queried(sample_factor_column_queried_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , log(log_)
{
    if (sample_factor_column_queried)
    {
        /// Only _sample_factor virtual column is added by ReadFromMergeTree
        /// Other virtual columns are added by MergeTreeBaseSelectProcessor.
        auto type = std::make_shared<DataTypeFloat64>();
        output_stream->header.insert({type->createColumn(), type, "_sample_factor"});
    }

    if (enable_parallel_reading)
        read_task_callback = context->getMergeTreeReadTaskCallback();

    /// Determine if we can use two stage search
    if (context->getSettingsRef().two_stage_search_option > 0 && !metadata_for_reading->vec_indices.empty())
    {
        /// Currently support one vector index
        auto vector_index = metadata_for_reading->vec_indices[0];

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
            num_reorder = VectorIndex::SearchVectorIndex::computeFirstStageNumCandidates(type, disk_mode, total_rows, vector_scan_desc.topk, search_params);

            LOG_DEBUG(log, "num_reorder for first stage = {}", num_reorder);

            /// In adaptive two stage search option, enable only when disk_mode > 0 and saved IO count is larger than 1000
            if (adaptive_two_stage)
            {
                UInt32 total_num_reorder = 0;
                for (auto part : prepared_parts)
                {
                    /// get num_reorder for every part
                    total_num_reorder += VectorIndex::SearchVectorIndex::computeFirstStageNumCandidates(
                                            type, disk_mode, part->rows_count, vector_scan_desc.topk, search_params);
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
                for (auto & name : virt_column_names)
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
                    virt_column_names.emplace_back("_part");

                if (need_remove_part_offset_column)
                    virt_column_names.emplace_back("_part_offset");
            }
        }
    }
}

MergeTreeDataSelectAnalysisResultPtr ReadWithVectorScan::selectRangesToRead(MergeTreeData::DataPartsVector parts) const
{
    return ReadFromMergeTree::selectRangesToRead(
        std::move(parts),
        prewhere_info,
        filter_nodes,
        storage_snapshot->metadata,
        storage_snapshot->getMetadataForQuery(),
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        real_column_names,
        sample_factor_column_queried,
        log);
}

ReadFromMergeTree::AnalysisResult ReadWithVectorScan::getAnalysisResult() const
{
    auto result_ptr = analyzed_result_ptr ? analyzed_result_ptr : selectRangesToRead(prepared_parts);
    if (std::holds_alternative<std::exception_ptr>(result_ptr->result))
        std::rethrow_exception(std::get<std::exception_ptr>(result_ptr->result));

    return std::get<ReadFromMergeTree::AnalysisResult>(result_ptr->result);
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
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result, context);

    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    selected_marks = result.selected_marks;
    selected_rows = result.selected_rows;
    selected_parts = result.selected_parts;

    Pipe pipe;

    Names column_names_to_read = std::move(result.column_names_to_read);

    /// If there are only virtual columns in the query, should be wrong, just return.
    if (column_names_to_read.empty())
    {
        LOG_DEBUG(log, "column_names_to_read is empty");
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    /// Reference spreadMarkRangesAmongStreams()
    pipe = createReadProcessorsAmongParts(
        std::move(result.parts_with_ranges),
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

    // Attach QueryIdHolder if needed
    if (query_id_holder)
        pipeline.setQueryIdHolder(std::move(query_id_holder));
}


/// Reference from ReadFromMergeTree::spreadMarkRangesAmongStreams()
/// 
Pipe ReadWithVectorScan::createReadProcessorsAmongParts(
    RangesInDataParts parts_with_range,
    const Names & column_names)
{
    if (parts_with_range.size() == 0)
        return {};

    const auto & settings = context->getSettingsRef();

    size_t num_streams = requested_num_streams;
    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (parts_with_range.size() < num_streams)
            num_streams = parts_with_range.size();
    }

/*
   /// Comment following code, since in two stage search we fail to parallel reading with additional sort transform
    Pipes res;
    const size_t min_parts_per_stream = (parts_with_range.size() - 1) / num_streams + 1;
    for (size_t i = 0; i < num_streams && !parts_with_range.empty(); ++i)
    {
        RangesInDataParts new_parts;
        for (size_t need_parts = min_parts_per_stream; need_parts > 0 && !parts_with_range.empty(); need_parts--)
        {
            new_parts.push_back(parts_with_range.back());
            parts_with_range.pop_back();
        }

        res.emplace_back(readFromParts(std::move(new_parts), column_names, settings.use_uncompressed_cache));
    }


    auto pipe = Pipe::unitePipes(std::move(res));
    */

    auto pipe = readFromParts(std::move(parts_with_range), column_names, settings.use_uncompressed_cache);

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
                header, sort_description, max_block_size, num_reorder, false, 0, 0, 0, nullptr, 0);
        });

        /// Second sort rows from different pipes
        if (pipe.numOutputPorts() > 1)
        {
            auto transform = std::make_shared<MergingSortedTransform>(
                    pipe.getHeader(),
                    pipe.numOutputPorts(),
                    sort_description,
                    max_block_size,
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
                    std::make_shared<MergeTreeVectorScanManager>(metadata_for_reading, vector_scan_info_ptr, context, support_two_stage_search);
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
    const RangesInDataParts & parts,
    Names required_columns,
    bool use_uncompressed_cache)
{
    Pipes pipes;
    auto vector_scan_info_ptr = query_info.vector_scan_info;
    if (!vector_scan_info_ptr)
        return {};

    const auto & client_info = context->getClientInfo();

    /// Prewhere info should not be changed, because it is shared by parts.
    if (prewhere_info)
    {
        /// need_filter is false when both prewhere and where exist, prewhere will be delayed, all read rows with a prehwere_column returned.
        /// In this case, we need only rows statisfied prewhere conditions.
        prewhere_info->need_filter = true;
    }

    for (const auto & part : parts)
    {
        auto vector_scan_manager =
            std::make_shared<MergeTreeVectorScanManager>(metadata_for_reading, vector_scan_info_ptr, context, support_two_stage_search);

        /// ToConfirm
        std::optional<ParallelReadingExtension> extension;
        if (read_task_callback)
        {
            extension = ParallelReadingExtension
            {
                .callback = read_task_callback.value(),
                .count_participating_replicas = client_info.count_participating_replicas,
                .number_of_current_replica = client_info.number_of_current_replica,
                .colums_to_read = required_columns
            };
        }

        auto algorithm = std::make_unique<MergeTreeSelectWithVectorScanProcessor>(
            data,
            storage_snapshot,
            part.data_part,
            max_block_size,
            preferred_block_size_bytes,
            preferred_max_column_in_block_size_bytes,
            required_columns,
            part.ranges,
            use_uncompressed_cache,
            prewhere_info,
            actions_settings,
            reader_settings,
            nullptr,
            virt_column_names,
            0UL,
            false,
            vector_scan_manager);

        auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

        pipes.emplace_back(Pipe(std::move(source)));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}

}
