/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    Pipe createReadProcessorsAmongParts(SearchResultAndRangesInDataParts parts_with_hybrid_ranges, const Names & column_names);

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

    Pipe readFromParts(
        const RangesInDataParts & parts,
        Names required_columns,
        bool use_uncompressed_cache);

    Pipe readFromParts(
        const SearchResultAndRangesInDataParts & parts_with_hybrid_ranges,
        Names required_columns,
        bool use_uncompressed_cache);

#if USE_TANTIVY_SEARCH
    void getStatisticForTextSearch();

    TANTIVY::Statistics bm25_stats_in_table; /// total bm25 info from all parts in a table
#endif

    void performFinal(
        const RangesInDataParts & parts_with_ranges,
        VectorAndTextResultInDataParts & parts_with_vector_text_result,
        size_t num_streams) const;
};

}
