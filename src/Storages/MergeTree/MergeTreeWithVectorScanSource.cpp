#include <Storages/MergeTree/MergeTreeWithVectorScanSource.h>
#include <Storages/MergeTree/MergeTreeSelectWithHybridSearchProcessor.h>
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

}
