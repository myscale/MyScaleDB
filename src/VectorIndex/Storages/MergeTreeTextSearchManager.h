#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <VectorIndex/Storages/HybridSearchResult.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/logger_useful.h>

#if USE_TANTIVY_SEARCH
#include <tantivy_search.h>
#endif

namespace DB
{

/// Text search manager, responsible for full-text search result precompute
class MergeTreeTextSearchManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeTextSearchManager(
        StorageMetadataPtr metadata_, TextSearchInfoPtr text_search_info_, ContextPtr context_)
        : MergeTreeBaseSearchManager{metadata_, context_, text_search_info_ ? text_search_info_->function_column_name : ""}
        , text_search_info(text_search_info_)
    {
    }

    MergeTreeTextSearchManager(TextSearchResultPtr text_search_result_, TextSearchInfoPtr text_search_info_)
        : MergeTreeBaseSearchManager{nullptr, nullptr, text_search_info_ ? text_search_info_->function_column_name : ""}
        ,text_search_result(text_search_result_)
    {
        if (text_search_result && text_search_result->computed)
        {
            LOG_DEBUG(log, "Already have precomputed text result, no need to execute search");
        }
    }

    ~MergeTreeTextSearchManager() override = default;

    void executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part) override;

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
        return text_search_result && text_search_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return text_search_result; }

#if USE_TANTIVY_SEARCH
    void setBM25Stats(const TANTIVY::Statistics & bm25_stats_in_table_)
    {
        bm25_stats_in_table = bm25_stats_in_table_;
    }
#endif

private:

    TextSearchInfoPtr text_search_info;

#if USE_TANTIVY_SEARCH
    TANTIVY::Statistics bm25_stats_in_table; /// total bm25 info from all parts in a table
#endif

    /// lock text search result
    std::mutex mutex;
    TextSearchResultPtr text_search_result = nullptr;

    LoggerPtr log = getLogger("MergeTreeTextSearchManager");

    TextSearchResultPtr textSearch(
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const Search::DenseBitmapPtr filter = nullptr);
};

using MergeTreeTextSearchManagerPtr = std::shared_ptr<MergeTreeTextSearchManager>;

}
