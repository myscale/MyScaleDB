#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <VectorIndex/Storages/HybridSearchResult.h>
#include <VectorIndex/Storages/MergeTreeTextSearchManager.h>
#include <VectorIndex/Storages/MergeTreeVSManager.h>

#include <Common/logger_useful.h>

#if USE_TANTIVY_SEARCH
#include <tantivy_search.h>
#endif

namespace DB
{

/// Hybrid search manager, responsible for hybrid search result precompute
class MergeTreeHybridSearchManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeHybridSearchManager(
        StorageMetadataPtr metadata_, HybridSearchInfoPtr hybrid_search_info_, ContextPtr context_)
        : MergeTreeBaseSearchManager{metadata_, context_}
        , hybrid_search_info(hybrid_search_info_)
    {
        /// Initialize vector scan and text search manager
        vector_scan_manager = make_shared<MergeTreeVSManager>(
                metadata_, hybrid_search_info->vector_scan_info, context_);
        text_search_manager = make_shared<MergeTreeTextSearchManager>(
                metadata_, hybrid_search_info->text_search_info, context_);
    }

    ~MergeTreeHybridSearchManager() override = default;

    /// In cases with no where clause, do vector scan search and text search and combine two results.
    void executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part) override;

    /// In cases with prewhere clause, do filtered vector scan search and filtered text search and combine two results. 
    void executeSearchWithFilter(
        const MergeTreeData::DataPartPtr & data_part,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter) override;

    void mergeResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter = nullptr,
        const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        return hybrid_search_result && hybrid_search_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return hybrid_search_result; }

#if USE_TANTIVY_SEARCH
    void setBM25Stats(const Statistics & bm25_stats_in_table_)
    {
        if (text_search_manager)
            text_search_manager->setBM25Stats(bm25_stats_in_table_);
    }
#endif

private:

    HybridSearchInfoPtr hybrid_search_info;

    /// lock hybrid search result
    std::mutex mutex;
    HybridSearchResultPtr hybrid_search_result = nullptr;

    MergeTreeVectorScanManagerPtr vector_scan_manager = nullptr;
    MergeTreeTextSearchManagerPtr text_search_manager = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeHybridSearchManager");

    /// Combine results from vector scan and text search based on fusion type and fusion paramters.
    HybridSearchResultPtr hybridSearch();

    void RelativeScoreFusion(
        std::map<UInt32, Float32> & labels_with_convex_score,
        const VectorScanResultPtr vec_scan_result,
        const TextSearchResultPtr text_search_result,
        const float weight_of_text,
        const Search::Metric vector_index_metric);

    void computeMinMaxNormScore(
        const ColumnFloat32 * score_col,
        MutableColumnPtr & norm_score_col);

    void RankFusion(
        std::map<UInt32, Float32> & labels_with_rank_score,
        const VectorScanResultPtr vec_scan_result,
        const TextSearchResultPtr text_search_result,
        const int k);

    /// Compute reciprocal rank score for a label id
    /// The map labels_with_ranked_score stores the sum of rank score for a label id
    void computeRankFusionScore(
        std::map<UInt32, Float32> & labels_with_ranked_score,
        const ColumnUInt32 * label_col,
        int k);
};

using MergeTreeHybridSearchManagerPtr = std::shared_ptr<MergeTreeHybridSearchManager>;

}
