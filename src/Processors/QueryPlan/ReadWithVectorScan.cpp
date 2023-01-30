#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadWithVectorScan.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeVectorScanManager.h>
#include <Storages/MergeTree/MergeTreeSelectWithVectorScanProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>

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
    : ISourceStep(DataStream{.header = IMergeTreeSelectAlgorithm::transformHeader(
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
}

void ReadWithVectorScan::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipe pipe;

    Names column_names_to_read = real_column_names;

    /// If there are only virtual columns in the query, should be wrong, just return.
    if (column_names_to_read.empty())
    {
        LOG_DEBUG(log, "column_names_to_read is empty");
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    pipe = createReadProcessorsAmongParts(
        prepared_parts,
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
}


/// needs to handle:
/// 
Pipe ReadWithVectorScan::createReadProcessorsAmongParts(
    MergeTreeData::DataPartsVector & parts,
    const Names & column_names)
{
    if (parts.size() == 0)
        return {};

    const auto & settings = context->getSettingsRef();

    Pipes res;

    size_t num_streams = requested_num_streams;
    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (parts.size() < num_streams)
            num_streams = parts.size();
    }

    const size_t min_parts_per_stream = (parts.size() - 1) / num_streams + 1;
    for (size_t i = 0; i < num_streams && !parts.empty(); ++i)
    {
        MergeTreeData::DataPartsVector new_parts;
        for (size_t need_parts = min_parts_per_stream; need_parts > 0 && !parts.empty(); need_parts--)
        {
            new_parts.push_back(parts.back());
            parts.pop_back();
        }

        res.emplace_back(readFromParts(std::move(new_parts), column_names, settings.use_uncompressed_cache));
    }

    auto pipe = Pipe::unitePipes(std::move(res));

    return pipe;
}

Pipe ReadWithVectorScan::readFromParts(
    const MergeTreeData::DataPartsVector & parts,
    Names required_columns,
    bool use_uncompressed_cache)
{
    Pipes pipes;
    auto vector_scan_info_ptr = query_info.vector_scan_info;
    if (!vector_scan_info_ptr)
        return {};

    const auto & client_info = context->getClientInfo();
    
    
    for (const auto & part : parts)
    {
        auto vector_scan_manager = std::make_shared<MergeTreeVectorScanManager>(metadata_for_reading, vector_scan_info_ptr, context);

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

        MarkRanges ranges;
        if (part->index_granularity.getMarksCount())
            ranges.emplace_back(0, part->index_granularity.getMarksCount());

        auto algorithm = std::make_unique<MergeTreeSelectWithVectorScanProcessor>(
            data,
            storage_snapshot,
            part,
            max_block_size,
            preferred_block_size_bytes,
            preferred_max_column_in_block_size_bytes,
            required_columns,
            ranges,
            use_uncompressed_cache,
            prewhere_info,
            actions_settings,
            reader_settings,
            nullptr,
            virt_column_names,
            (size_t)0,
            false,
            vector_scan_manager);

        auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

        pipes.emplace_back(Pipe(std::move(source)));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}

}
