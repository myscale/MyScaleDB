#pragma once
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <VectorIndex/Utils/VSUtils.h>

namespace DB
{

class ReadWithHybridSearch final : public ReadFromMergeTree
{
public:
    ReadWithHybridSearch(
        MergeTreeData::DataPartsVector parts_,
        std::vector<AlterConversionsPtr> alter_conversions_,
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
        MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading
    );

    String getName() const override { return "ReadWithHybridSearch"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    Pipe createReadProcessorsAmongParts(RangesInDataParts parts_with_range,
    const Names & column_names);

private:

    bool support_two_stage_search = false;      /// True if two stage search is used.
    UInt64 num_reorder = 0;   /// number of candidates for first stage search
    bool need_remove_part_virual_column = true; /// _part virtual column is needed only for two stage search
    bool need_remove_part_offset_column = true; /// _part_offset virtual column

    Pipe readFromParts(
        const RangesInDataParts & parts,
        Names required_columns,
        bool use_uncompressed_cache);
};

}
