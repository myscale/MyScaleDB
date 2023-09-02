#pragma once
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeVectorScanUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{

class ReadWithVectorScan final : public SourceStepWithFilter
{
public:
    ReadWithVectorScan(
        MergeTreeData::DataPartsVector parts_,
        Names real_column_names_,
        Names virt_column_names_,
        const MergeTreeData & data_,
        const SelectQueryInfo & query_info_,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        bool sample_factor_column_queried_,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
        Poco::Logger * log_,
        bool enable_parallel_reading
    );

    String getName() const override { return "ReadWithVectorScan"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    Pipe createReadProcessorsAmongParts(RangesInDataParts parts_with_range,
    const Names & column_names);

private:
    std::optional<MergeTreeReadTaskCallback> read_task_callback;

    const MergeTreeReaderSettings reader_settings;

    MergeTreeData::DataPartsVector prepared_parts;
    Names real_column_names;
    Names virt_column_names;

    const MergeTreeData & data;
    SelectQueryInfo query_info;
    PrewhereInfoPtr prewhere_info;
    ExpressionActionsSettings actions_settings;

    StorageSnapshotPtr storage_snapshot;
    StorageMetadataPtr metadata_for_reading;

    ContextPtr context;

    const size_t max_block_size;
    const size_t requested_num_streams;
    const size_t preferred_block_size_bytes;
    const size_t preferred_max_column_in_block_size_bytes;
    const bool sample_factor_column_queried;

    bool support_two_stage_search = false;      /// True if two stage search is used.
    UInt64 num_reorder = 0;   /// number of candidates for first stage search
    bool need_remove_part_virual_column = true; /// _part virtual column is needed only for two stage search
    bool need_remove_part_offset_column = true; /// _part_offset virtual column

    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read;

    Poco::Logger * log;
    UInt64 selected_parts = 0;
    UInt64 selected_rows = 0;
    UInt64 selected_marks = 0;

    Pipe readFromParts(
        const RangesInDataParts & parts,
        Names required_columns,
        bool use_uncompressed_cache);

    MergeTreeDataSelectAnalysisResultPtr selectRangesToRead(MergeTreeData::DataPartsVector parts) const;
    ReadFromMergeTree::AnalysisResult getAnalysisResult() const;
    MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr;
};

}
