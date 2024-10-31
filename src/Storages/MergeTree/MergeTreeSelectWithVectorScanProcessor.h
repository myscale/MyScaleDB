#pragma once
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/SelectQueryInfo.h>

#include <Common/logger_useful.h>

#include <SearchIndex/Common/DenseBitmap.h>

namespace DB
{
/// https://github.com/ClickHouse/ClickHouse/pull/53931 Refactoring of reading from MergeTree tables
/// IMergeTreeSelectAlgorithm is refactored to MergeTreeSelectProcessor

/// Merged from old MergeTreeSelectAlgorithm and IMergeTreeSelectAlgorithm
class MergeTreeSelectWithVectorScanProcessor
{
public:
    using ReadRange = MergeTreeRangeReader::ReadResult::ReadRangeInfo;
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;
    using BlockSizeParams = MergeTreeReadTask::BlockSizeParams;
    using BlockAndProgress = MergeTreeReadTask::BlockAndProgress;

    explicit MergeTreeSelectWithVectorScanProcessor(
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const RangesInDataPart & part_with_ranges_,
        VirtualFields shared_virtual_fields_,
        Names required_columns_,
        bool use_uncompressed_cache_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReadTask::BlockSizeParams & block_size_params_,
        const MergeTreeReaderSettings & reader_settings_,
        MergeTreeVectorScanManagerPtr vector_scan_mamanger_ = nullptr);

    static Block transformHeader(
        Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns);

    static std::unique_ptr<MergeTreeBlockSizePredictor> getSizePredictor(
        const MergeTreeData::DataPartPtr & data_part,
        const MergeTreeReadTaskColumns & task_columns,
        const Block & sample_block);

    Block getHeader() const { return result_header; }

    ChunkAndProgress read();

    void cancel() { is_cancelled = true; }

    String getName() const { return "MergeTreeReadWithVectorScan"; }

protected:
    BlockAndProgress readFromPart();

    bool readPrimaryKeyBin(Columns & out_columns);

    /// Sets up range readers corresponding to data readers
    void initializeRangeReaders();

    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;

    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;
    const PrewhereExprInfo prewhere_actions;

    MergeTreeReaderSettings reader_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;

    /// Current task to read from.
    MergeTreeReadTaskPtr task;
    /// This step is added when the part has lightweight delete mask
    PrewhereExprStepPtr lightweight_delete_filter_step;
    /// A result of getHeader(). A chunk which this header is returned from read().
    Block result_header;

    bool use_uncompressed_cache;

    DataTypePtr partition_value_type;

    UncompressedCachePtr owned_uncompressed_cache;
    MarkCachePtr owned_mark_cache;

    /// This setting is used in base algorithm only to additionally limit the number of granules to read.
    /// It is changed in ctor of MergeTreeThreadSelectAlgorithm.
    ///
    /// The reason why we have it here is because MergeTreeReadPool takes the full task
    /// ignoring min_marks_to_read setting in case of remote disk (see MergeTreeReadPool::getTask).
    /// In this case, we won't limit the number of rows to read based on adaptive granularity settings.
    ///
    /// Big reading tasks are better for remote disk and prefetches.
    /// So, for now it's easier to limit max_rows_to_read.
    /// Somebody need to refactor this later.
    size_t min_marks_to_read = 0;

    /// Defer initialization from constructor, because it may be heavy
    /// and it's better to do it lazily in `getNewTaskImpl`, which is executing in parallel.
    void finish();

    MergeTreeReadTaskPtr createTask(MarkRanges ranges) const;

    MergeTreeReadTask::Extras getExtras() const;

    /// Reference from MergeTreeReadPoolBase::fillPerPartInfos()
    MergeTreeReadTaskInfoPtr initializeReadTaskInfo() const;

    /// Used by Task
    Names required_columns;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
    NameSet column_name_set;

    const VirtualFields shared_virtual_fields;

    RangesInDataPart part_with_ranges;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;

    /// Cache getSampleBlock call, which might be heavy.
    Block sample_block;

    /// Mark ranges we should read (in ascending order)
    MarkRanges all_mark_ranges;

    size_t total_rows = 0;

    MergeTreeVectorScanManagerPtr vector_scan_manager = nullptr;

private:
    bool getNewTaskImpl();

    BlockAndProgress readFromPartWithVectorScan();

    Search::DenseBitmapPtr performPrefilter(MarkRanges & mark_ranges);

    LoggerPtr log = getLogger("MergeTreeSelectWithVectorScanProcessor");

    std::atomic<bool> is_cancelled{false};

    bool getNewTask();

    /// True if _part_offset column is added for vector scan, but should not exist in select result.
    bool need_remove_part_offset = false;

    /// Logic row id for rows, used for vector index scan.
    const ColumnUInt64 * part_offset = nullptr;

    /// True if the query can use primary key cache.
    bool use_primary_key_cache = false;

    /// Used for vector scan to handle cases when both prewhere and where exist
    /// remove_prewhere_column is set to true when vector scan try to get _part_offset for rows satisfying prewhere conds.
    bool original_remove_prewhere_column = false;
};

}
