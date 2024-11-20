#pragma once
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <VectorIndex/Utils/VSUtils.h>

#if USE_TANTIVY_SEARCH
#include <tantivy_search.h>
#endif

namespace DB
{

class ReadWithHybridSearch final : public ReadFromMergeTree
{
public:

    struct HybridAnalysisResult
    {
        /// result ranges on parts after top-k hybrid/vector scan/full-text search on all parts
        SearchResultAndRangesInDataParts parts_with_hybrid_and_ranges;
    };

    ReadWithHybridSearch(
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
        AnalysisResultPtr analyzed_result_ptr_,
        bool enable_parallel_reading
    );

    String getName() const override { return "ReadWithHybridSearch"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:

    /// Support multiple distance functions
    /// The size of following two vectors are equal to the size of vector scan descriptions
    std::vector<bool> vec_support_two_stage_searches;          /// True if two stage search is supported
    [[maybe_unused]] std::vector<UInt64> vec_num_reorders; /// number of candidates for first stage search

    ReadWithHybridSearch::HybridAnalysisResult getHybridSearchResult(const RangesInDataParts & parts) const;

    /// Get total top-k hybrid result and save them in belonged part
    ReadWithHybridSearch::HybridAnalysisResult selectTotalHybridResult(
        const RangesInDataParts & parts_with_ranges,
        const StorageMetadataPtr & metadata_snapshot,
        size_t num_streams) const;

    /// Get accurate distance value for candidates by second stage vector index in belonged part
    VectorAndTextResultInDataParts selectPartsBySecondStageVectorIndex(
        const VectorAndTextResultInDataParts & parts_with_candidates,
        const VSDescription & vector_scan_desc,
        size_t num_streams) const;

    Pipe readFromParts(RangesInDataParts parts_with_ranges, Names required_columns, bool use_uncompressed_cache);

    /// Reference spreadMarkRangesAmongStreams()
    Pipe createReadProcessorsAmongParts(
        RangesInDataParts && parts_with_ranges,
        size_t num_streams,
        const Names & column_names);

    Pipe createReadProcessorsAmongParts(SearchResultAndRangesInDataParts parts_with_hybrid_ranges, const Names & column_names);

    Pipe readFromParts(
        const SearchResultAndRangesInDataParts & parts_with_hybrid_ranges,
        Names required_columns,
        bool use_uncompressed_cache);

#if USE_TANTIVY_SEARCH
    void getStatisticForTextSearch();

    TANTIVY::Statistics bm25_stats_in_table; /// total bm25 info from all parts in a table
#endif
    /// MYSCALE_INTERNAL_CODE_BEGIN
    /// Determine if we can use two stage search, initialize num_reorder
    void supportTwoStageSearch(
        const MergeTreeData::DataPartsVector & prepared_parts_,
        const VectorScanInfoPtr & vector_scan_info_ptr,
        const Settings & settings,
        const StorageMetadataPtr & metadata_for_reading,
        const int default_mstg_disk_mode,
        const SelectQueryInfo & query_info_,
        LoggerPtr log);
    /// MYSCALE_INTERNAL_CODE_END

    void performFinal(
        const RangesInDataParts & parts_with_ranges,
        VectorAndTextResultInDataParts & parts_with_vector_text_result,
        size_t num_streams) const;
};

}
