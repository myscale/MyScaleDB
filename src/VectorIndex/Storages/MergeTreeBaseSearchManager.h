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

/// Base class for MergeTreeVectorScanManager, MergeTreeTextSearchManager and MergeTreeHybridSearchManager
class MergeTreeBaseSearchManager
{
public:
    using ReadRange = MergeTreeRangeReader::ReadResult::ReadRangeInfo;
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;

    MergeTreeBaseSearchManager(
        StorageMetadataPtr metadata_, ContextPtr context_)
        : metadata(metadata_)
        , context(context_)
    {
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
        const Search::DenseBitmapPtr /* filter */,
        const ColumnUInt64 * /* part_offset */) {}

    /// True if search result is present and computed flag is set to true.
    virtual bool preComputed() { return false; }

    virtual CommonSearchResultPtr getSearchResult() { return nullptr; }

    Settings getSettings() { return context->getSettingsRef(); }

protected:

    StorageMetadataPtr metadata;
    ContextPtr context;

    /// Merge search result with pre_result from read part
    /// Add score/distance of the row id to the corresponding row
    void mergeSearchResultImpl(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        CommonSearchResultPtr tmp_result = nullptr,
        const Search::DenseBitmapPtr filter = nullptr,
        const ColumnUInt64 * part_offset = nullptr);
};

using MergeTreeBaseSearchManagerPtr = std::shared_ptr<MergeTreeBaseSearchManager>;

}
