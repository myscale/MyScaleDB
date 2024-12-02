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
/// After refactor, the work of hybrid manager changed to:
/// 1. Combine vector scan and full-text search manager to get top-k or first stage vector results for provided data part.
/// 2. Provide static hybridSearch() and fusion functions to do fusion on total top-k results from different parts.
/// 3. Merge hyrbid results on provided data part with other required columns.
class MergeTreeHybridSearchManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeHybridSearchManager(
        StorageMetadataPtr metadata_, HybridSearchInfoPtr hybrid_search_info_, ContextPtr context_, bool support_two_stage_search_ = false)
        : MergeTreeBaseSearchManager{metadata_, context_, hybrid_search_info_ ? hybrid_search_info_->function_column_name : ""}
        , hybrid_search_info(hybrid_search_info_)
    {
        /// Initialize vector scan and text search manager
        vector_scan_manager = make_shared<MergeTreeVSManager>(
                metadata_, hybrid_search_info->vector_scan_info, context_, support_two_stage_search_);
        text_search_manager = make_shared<MergeTreeTextSearchManager>(
                metadata_, hybrid_search_info->text_search_info, context_);
    }

    /// Hybrid search has done on all parts, no need to do search and fusion.
    /// Only need to merge result for hybrid and other columns in part
    MergeTreeHybridSearchManager(HybridSearchResultPtr hybrid_search_result_, HybridSearchInfoPtr hybrid_search_info_)
        : MergeTreeBaseSearchManager{nullptr, nullptr, hybrid_search_info_ ? hybrid_search_info_->function_column_name : ""}
        , hybrid_search_result(std::move(hybrid_search_result_))
    {
        if (hybrid_search_result && hybrid_search_result->computed)
        {
            LOG_DEBUG(log, "Already have precomputed hybrid result, no need to execute search and do fusion");
        }
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
        const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        return hybrid_search_result && hybrid_search_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return hybrid_search_result; }

    /// Return vector scan result if exists for hybrid search
    VectorScanResultPtr getVectorScanResult()
    {
        VectorScanResultPtr result = nullptr;
        if (vector_scan_manager && vector_scan_manager->preComputed())
            result = vector_scan_manager->getSearchResult();

        return result;
    }

    /// Return full-text search result if exists for hybrid search
    TextSearchResultPtr getTextSearchResult()
    {
        TextSearchResultPtr result = nullptr;
        if (text_search_manager && text_search_manager->preComputed())
            result = text_search_manager->getSearchResult();

        return result;
    }

#if USE_TANTIVY_SEARCH
    void setBM25Stats(const TANTIVY::Statistics & bm25_stats_in_table_)
    {
        if (text_search_manager)
            text_search_manager->setBM25Stats(bm25_stats_in_table_);
    }
#endif

    /// Fusion vector scan and full-text search results from all selected parts
    static ScoreWithPartIndexAndLabels hybridSearch(
        const ScoreWithPartIndexAndLabels & vec_scan_result_with_part_index,
        const ScoreWithPartIndexAndLabels & text_search_result_with_part_index,
        const HybridSearchInfoPtr & hybrid_info,
        LoggerPtr log);

    /// Filter parts using total top-k hybrid search result
    /// For every part, select mark ranges to read, also save hybrid result
    static SearchResultAndRangesInDataParts FilterPartsWithHybridResults(
        const VectorAndTextResultInDataParts & parts_with_vector_text_result,
        const RangesInDataParts & parts_with_ranges,
        const ScoreWithPartIndexAndLabels & hybrid_result_with_part_index,
        const Settings & settings,
        LoggerPtr log);

private:

    HybridSearchInfoPtr hybrid_search_info;

    HybridSearchResultPtr hybrid_search_result = nullptr;

    MergeTreeVectorScanManagerPtr vector_scan_manager = nullptr;
    MergeTreeTextSearchManagerPtr text_search_manager = nullptr;

    LoggerPtr log = getLogger("MergeTreeHybridSearchManager");
};

using MergeTreeHybridSearchManagerPtr = std::shared_ptr<MergeTreeHybridSearchManager>;

}
