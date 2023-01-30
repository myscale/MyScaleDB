#pragma once
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/SelectQueryInfo.h>

#include <Common/logger_useful.h>

namespace DB
{
class MergeTreeSelectWithVectorScanProcessor final : public MergeTreeSelectAlgorithm
{
public:
    using ReadRange = MergeTreeRangeReader::ReadResult::ReadRangeInfo;
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;

    template <typename... Args>
    explicit MergeTreeSelectWithVectorScanProcessor(Args &&... args)
        : MergeTreeSelectAlgorithm{std::forward<Args>(args)...}
    {
        LOG_TRACE(
            log,
            "Reading {} ranges in order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(),
            data_part->name,
            total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
    }

    String getName() const override { return "MergeTreeReadWithVectorScan"; }
protected:
    BlockAndProgress readFromPart() override;
    void initializeReadersWithVectorScan();

    bool readPrimaryKeyBin(Columns & out_columns);

private:
    bool getNewTaskImpl() override;
    void finalizeNewTask() override {}

    BlockAndProgress readFromPartWithVectorScan();

    ColumnPtr performPrefilter(MarkRanges & mark_ranges);

    Poco::Logger * log = &Poco::Logger::get("MergeTreeSelectWithVectorScanProcessor");

    /// True if _part_offset column is added for vector scan, but should not exist in select result.
    bool need_remove_part_offset = false;

    /// Logic row id for rows, used for vector index scan.
    const ColumnUInt64 * part_offset = nullptr;

    /// True if the query can use primary key cache.
    bool use_primary_key_cache = false;
};

}
