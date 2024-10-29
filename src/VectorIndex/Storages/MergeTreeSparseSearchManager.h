#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <VectorIndex/Storages/HybridSearchResult.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>

#include <Common/logger_useful.h>

#if USE_CUSTOM_SKIP_INDEX
#    include <sparse_index.h>
#endif

namespace DB
{

/// Sparse search manager, responsible for sparse search result precompute
class MergeTreeSparseSearchManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeSparseSearchManager(StorageMetadataPtr metadata_, SparseSearchInfoPtr sparse_search_info_, ContextPtr context_)
        : MergeTreeBaseSearchManager{metadata_, context_, sparse_search_info_ ? sparse_search_info_->function_column_name : ""}
        , sparse_search_info(sparse_search_info_)
    {
    }

    MergeTreeSparseSearchManager(SparseSearchResultPtr sparse_search_result_, SparseSearchInfoPtr sparse_search_info_)
        : MergeTreeBaseSearchManager{nullptr, nullptr, sparse_search_info_ ? sparse_search_info_->function_column_name : ""}
        , sparse_search_result(sparse_search_result_)
    {
        if (sparse_search_result && sparse_search_result->computed)
        {
            LOG_DEBUG(log, "Already have precomputed sparse result, no need to execute search");
        }
    }

    ~MergeTreeSparseSearchManager() override = default;

    void executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part) override;

    void executeSearchWithFilter(
        const MergeTreeData::DataPartPtr & data_part, const ReadRanges & read_ranges, const Search::DenseBitmapPtr filter) override;

    void mergeResult(
        Columns & pre_result, size_t & read_rows, const ReadRanges & read_ranges, const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        return sparse_search_result && sparse_search_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return sparse_search_result; }

private:
    SparseSearchInfoPtr sparse_search_info;

    /// Lock sparse search result
    std::mutex mutex;
    SparseSearchResultPtr sparse_search_result = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeSparseSearchManager");

    SparseSearchResultPtr sparseSearch(
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const Search::DenseBitmapPtr filter = nullptr);
};

using MergeTreeSparseSearchManagerPtr = std::shared_ptr<MergeTreeSparseSearchManager>;

}
