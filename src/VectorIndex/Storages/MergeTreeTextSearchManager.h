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

namespace DB
{

/// Text search manager, responsible for full-text search result precompute
class MergeTreeTextSearchManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeTextSearchManager(
        StorageMetadataPtr metadata_, TextSearchInfoPtr text_search_info_, ContextPtr context_)
        : MergeTreeBaseSearchManager{metadata_, context_}
        , text_search_info(text_search_info_)
    {
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
        const Search::DenseBitmapPtr filter = nullptr,
        const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        return text_search_result && text_search_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return text_search_result; }

private:

    TextSearchInfoPtr text_search_info;

    /// lock text search result
    std::mutex mutex;
    TextSearchResultPtr text_search_result = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeTextSearchManager");

    TextSearchResultPtr textSearch(
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const Search::DenseBitmapPtr filter = nullptr);
};

using MergeTreeTextSearchManagerPtr = std::shared_ptr<MergeTreeTextSearchManager>;

}
