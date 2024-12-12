#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <VectorIndex/Storages/MergeTreeThreadSelectWithFilterAlgorithm.h>
#include <VectorIndex/Storages/MergeTreeWithVectorScanSource.h>
#include <VectorIndex/Storages/MergeTreeSelectWithHybridSearchProcessor.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/SharedThreadPools.h>
#include <Common/EventFD.h>

namespace DB
{

MergeTreeWithVectorScanSource::MergeTreeWithVectorScanSource(MergeTreeSelectWithHybridSearchProcessorPtr processor_, const std::string & log_name_)
    : ISource(processor_->getHeader()), processor(std::move(processor_)), log_name(log_name_)
{
}

MergeTreeWithVectorScanSource::~MergeTreeWithVectorScanSource() = default;

std::string MergeTreeWithVectorScanSource::getName() const
{
    return processor->getName();
}

void MergeTreeWithVectorScanSource::onCancel() noexcept
{
    processor->cancel();
}

ISource::Status MergeTreeWithVectorScanSource::prepare()
{
    return ISource::prepare();
}


Chunk MergeTreeWithVectorScanSource::processReadResult(ChunkAndProgress chunk)
{
    if (chunk.num_read_rows || chunk.num_read_bytes)
        progress(chunk.num_read_rows, chunk.num_read_bytes);

    finished = chunk.is_finished;

    /// We can return a chunk with no rows even if are not finished.
    /// This allows to report progress when all the rows are filtered out inside MergeTreeSelectProcessor by PREWHERE logic.
    return std::move(chunk.chunk);
}


std::optional<Chunk> MergeTreeWithVectorScanSource::tryGenerate()
{
    OpenTelemetry::SpanHolder span{fmt::format("MergeTreeWithVectorScanSource({})::tryGenerate", log_name)};
    return processReadResult(processor->read());
}

Pipe createReadInOrderFromPartsSource(
    const RangesInDataParts & parts_with_ranges,
    Names columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    String log_name,
    const PrewhereInfoPtr & prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    const MergeTreeReadTask::BlockSizeParams & block_size_params,
    ContextPtr context,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache)
{
    MergeTreeReadPool::PoolSettings pool_settings
    {
        .threads = max_streams,
        .sum_marks = parts_with_ranges.getMarksCountAllParts(),
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = block_size_params.preferred_block_size_bytes,
        .use_uncompressed_cache = use_uncompressed_cache,
        .use_const_size_tasks_for_remote_reading = context->getSettingsRef().merge_tree_use_const_size_tasks_for_remote_reading,
    };

    MergeTreeReadPoolPtr pool;
    pool = std::make_shared<MergeTreeReadPoolInOrder>(
        /*has_limit_below_one_block*/ false,
        MergeTreeReadType::Default,
        parts_with_ranges,
        VirtualFields{},
        storage_snapshot,
        prewhere_info,
        actions_settings,
        reader_settings,
        columns_to_read,
        pool_settings,
        context);

    Pipes pipes;
    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        auto algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(i);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool, std::move(algorithm), prewhere_info,
            actions_settings, block_size_params, reader_settings);

        auto part_source = std::make_shared<MergeTreeSource>(std::move(processor), log_name);
        pipes.emplace_back(std::move(part_source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}

Pipe createReadFromPoolWithFilterFromPartSource(
    RangesInDataParts parts_with_ranges,
    Names columns_to_read,
    VectorIndex::VIBitmapPtr filter,
    const StorageSnapshotPtr & storage_snapshot,
    String log_name,
    const PrewhereInfoPtr & prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    const MergeTreeReadTask::BlockSizeParams & block_size_params,
    ContextPtr context,
    size_t max_streams,
    size_t min_marks_for_concurrent_read)
{
    const auto & settings = context->getSettingsRef();
    size_t total_rows = parts_with_ranges.getRowsCountAllParts();

    MergeTreeReadPool::PoolSettings pool_settings
    {
        .threads = max_streams,
        .sum_marks = parts_with_ranges.getMarksCountAllParts(),
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = settings.preferred_block_size_bytes,
        .use_uncompressed_cache = settings.use_uncompressed_cache,
        .use_const_size_tasks_for_remote_reading = settings.merge_tree_use_const_size_tasks_for_remote_reading,
    };

    MergeTreeReadPoolPtr pool;
    pool = std::make_shared<MergeTreeReadPool>(
        std::move(parts_with_ranges),
        VirtualFields{},
        storage_snapshot,
        prewhere_info,
        actions_settings,
        reader_settings,
        columns_to_read,
        pool_settings,
        context);

    /// The reason why we change this setting is because MergeTreeReadPool takes the full task
    /// ignoring min_marks_to_read setting in case of remote disk (see MergeTreeReadPool::getTask).
    /// In this case, we won't limit the number of rows to read based on adaptive granularity settings.
    auto block_size_copy = block_size_params;
    block_size_copy.min_marks_to_read = pool_settings.min_marks_for_concurrent_read;

    Pipes pipes;
    for (size_t i = 0; i < pool_settings.threads; ++i)
    {
        auto algorithm = std::make_unique<MergeTreeThreadSelectWithFilterAlgorithm>(i, filter);

        auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool, std::move(algorithm), prewhere_info,
            actions_settings, block_size_copy, reader_settings);

        auto source = std::make_shared<MergeTreeSource>(std::move(processor), log_name);

        if (i == 0)
            source->addTotalRowsApprox(total_rows);

        pipes.emplace_back(std::move(source));
    }

    Pipe pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}

}
