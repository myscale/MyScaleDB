#pragma once
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeVectorScanUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Transforms/ExpressionTransform.h>

namespace DB
{

class ReadWithVectorScan final : public ReadFromMergeTree
{
public:
    ReadWithVectorScan(
        MergeTreeData::DataPartsVector parts_,
        std::vector<AlterConversionsPtr> alter_conversions_,
        Names all_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        LoggerPtr log_,
        MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading
    );

    String getName() const override { return "ReadWithVectorScan"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    const MergeTreeReaderSettings reader_settings;

    MergeTreeData::DataPartsVector prepared_parts;
    std::vector<AlterConversionsPtr> alter_conversions_for_parts;

    Names all_column_names;

    const MergeTreeData & data;
    ExpressionActionsSettings actions_settings;

    const MergeTreeReadTask::BlockSizeParams block_size;

    const size_t requested_num_streams;

    bool support_two_stage_search = false;      /// True if two stage search is used.
    UInt64 num_reorder = 0;   /// number of candidates for first stage search
    bool need_remove_part_virual_column = true; /// _part virtual column is needed only for two stage search
    bool need_remove_part_offset_column = true; /// _part_offset virtual column

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    LoggerPtr log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    Pipe readFromParts(RangesInDataParts parts_with_ranges, Names required_columns, bool use_uncompressed_cache);

    /// Reference spreadMarkRangesAmongStreams()
    Pipe createReadProcessorsAmongParts(
        RangesInDataParts && parts_with_ranges,
        size_t num_streams,
        const Names & column_names);

    ReadFromMergeTree::AnalysisResult getAnalysisResult() const;
    mutable ReadFromMergeTree::AnalysisResultPtr analyzed_result_ptr;
    VirtualFields shared_virtual_fields;
};

}
