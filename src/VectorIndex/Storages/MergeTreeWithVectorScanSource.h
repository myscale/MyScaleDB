#pragma once
#include <Processors/ISource.h>

namespace DB
{

class  MergeTreeSelectWithHybridSearchProcessor;
using MergeTreeSelectWithHybridSearchProcessorPtr = std::unique_ptr< MergeTreeSelectWithHybridSearchProcessor>;

struct ChunkAndProgress;

/// Reference from MergeTreeSource, without async read in linux
class MergeTreeWithVectorScanSource final : public ISource
{
public:
    explicit MergeTreeWithVectorScanSource(MergeTreeSelectWithHybridSearchProcessorPtr processor_, const std::string & log_name_);
    ~MergeTreeWithVectorScanSource() override;

    std::string getName() const override;

    Status prepare() override;

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() noexcept override;

private:
     MergeTreeSelectWithHybridSearchProcessorPtr processor;
    const std::string log_name;

    Chunk processReadResult(ChunkAndProgress chunk);
};

/// Create stream for reading parts from MergeTree.
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
    bool use_uncompressed_cache);

/// Create stream for parallel reading single part from MergeTree.
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
    size_t min_marks_for_concurrent_read);
}
