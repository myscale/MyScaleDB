#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <VectorIndex/Storages/HybridSearchResult.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <SearchIndex/VectorIndex.h>

namespace DB
{

/// Base class for MergeTreeVSManager, MergeTreeTextSearchManager, MergeTreeHybridSearchManager and MergeTreeSparseSearchManager
class MergeTreeBaseSearchManager
{
public:
    using ReadRange = MergeTreeRangeReader::ReadResult::ReadRangeInfo;
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;

    MergeTreeBaseSearchManager(
        StorageMetadataPtr metadata_, ContextPtr context_, const String & function_col_name_ = "")
        : metadata(metadata_)
        , context(context_)
    {
        if (!function_col_name_.empty())
            search_func_cols_names.emplace_back(function_col_name_);
    }

    virtual ~MergeTreeBaseSearchManager() = default;

    /// In cases with no where clause, do search on the corresponding index in the data part to find top-k result.
    virtual void executeSearchBeforeRead(const MergeTreeData::DataPartPtr & /* data_part */) {}

    /// In cases with prewhere clause, do filter search on the corresponding index in the data part to find top-k result.
    virtual void executeSearchWithFilter(
        const MergeTreeData::DataPartPtr & /* data_part */,
        const ReadRanges & /* read_ranges */,
        const Search::DenseBitmapPtr /* filter */) {}

    /// Merge top-k search result with other columns read from part
    /// Join the score/distance column with other columns based on row id.
    virtual void mergeResult(
        Columns & /* pre_result */,
        size_t & /* read_rows */,
        const ReadRanges & /* read_ranges */,
        const ColumnUInt64 * /* part_offset */) {}

    /// True if search result is present and computed flag is set to true.
    virtual bool preComputed() { return false; }

    virtual CommonSearchResultPtr getSearchResult() { return nullptr; }

    /// Support multiple distance functions
    Names getSearchFuncColumnNames() { return search_func_cols_names; }

    const Settings & getSettings() { return context->getSettingsRef(); }

    /// Get top-k vector scan result among all selected parts
    static ScoreWithPartIndexAndLabels getTotalTopKVSResult(
        const SpecialSearchResultInDataParts & vector_results,
        const size_t vec_res_index,
        const VSDescription & vector_scan_desc,
        Poco::Logger * log);

    static ScoreWithPartIndexAndLabels getTotalTopKTextResult(
        const SpecialSearchResultInDataParts & text_results,
        const TextSearchInfoPtr & text_info,
        Poco::Logger * log);

    static ScoreWithPartIndexAndLabels getTotalTopKSparseResult(
        const SpecialSearchResultInDataParts & sparse_results,
        const SparseSearchInfoPtr & sparse_search_info,
        Poco::Logger * log);

    /// Get num_reorder candidate vector result among all selected parts for two stage search
    static ScoreWithPartIndexAndLabels getTotalCandidateVSResult(
        const SpecialSearchResultInDataParts & parts_with_special_result,
        const size_t vec_res_index,
        const VSDescription & vector_scan_desc,
        const UInt64 & num_reorder,
        Poco::Logger * log);

    static std::set<UInt64> getLabelsInSearchResults(
        const SpecialSearchResultInDataPart & mix_results,
        Poco::Logger * log);

    static void filterSearchResultsByFinalLabels(
        SpecialSearchResultInDataPart & mix_results,
        std::set<UInt64> & label_ids,
        Poco::Logger * log);

protected:

    StorageMetadataPtr metadata;
    ContextPtr context;
    Names search_func_cols_names; /// names of search function columns

    std::vector<bool> was_result_processed;  /// Mark if the result was processed or not.

    /// Merge search result with pre_result from read part
    /// Add score/distance of the row id to the corresponding row
    void mergeSearchResultImpl(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        CommonSearchResultPtr tmp_result = nullptr,
        const ColumnUInt64 * part_offset = nullptr);

    /// Get top-k vector/text/sparse search result among all selected parts
    /// Return top-k result based on the special search type
    static ScoreWithPartIndexAndLabels getTotalTopSearchResultImpl(
        const SpecialSearchResultInDataParts & special_search_results,
        const UInt64 & top_k,
        const bool & desc_direction,
        Poco::Logger * log,
        const SpecialSearchFuncType & special_search_type,
        const size_t vec_res_index = 0);

    /// Get label_ids in search result and save in label_ids set.
    static void getLabelsInSearchResult(
        std::set<UInt64> & label_ids,
        const CommonSearchResultPtr & search_result,
        Poco::Logger * log);

    /// Construct a new search result based on original search result and final label ids.
    static CommonSearchResultPtr filterSearchResultByFinalLabels(
        const CommonSearchResultPtr & pre_search_result,
        std::set<UInt64> & label_ids,
        Poco::Logger * log);
};

using MergeTreeBaseSearchManagerPtr = std::shared_ptr<MergeTreeBaseSearchManager>;

}
