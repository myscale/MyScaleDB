#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <Core/Settings.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/logger_useful.h>

#include <VectorIndex/Common/VectorDataset.h>
#include <VectorIndex/Storages/HybridSearchResult.h>

#include <SearchIndex/VectorIndex.h>

namespace DB
{

/// vector scan manager, responsible for vector scan result precompute, and vector scan after read
class MergeTreeVSManager : public MergeTreeBaseSearchManager
{
public:
    MergeTreeVSManager(
        StorageMetadataPtr metadata_, VectorScanInfoPtr vector_scan_info_, ContextPtr context_, std::vector<bool> vec_support_two_stage_searches_ = {})
        : MergeTreeBaseSearchManager{metadata_, context_}
        , vector_scan_info(vector_scan_info_)
        , vec_support_two_stage_searches(vec_support_two_stage_searches_)
        , enable_brute_force_search(context_->getSettingsRef().enable_brute_force_vector_search)
        , max_threads(context_->getSettingsRef().max_threads)
    {
        for (const auto & desc : vector_scan_info_->vector_scan_descs)
            search_func_cols_names.emplace_back(desc.column_name);
    }

    /// From hybrid search or batch_distance
    MergeTreeVSManager(
        StorageMetadataPtr metadata_, VectorScanInfoPtr vector_scan_info_, ContextPtr context_, bool support_two_stage_search_ = false)
        : MergeTreeVSManager(metadata_, vector_scan_info_, context_, std::vector<bool>{support_two_stage_search_})
    {}

    /// Multiple vector scan functions
    MergeTreeVSManager(const ManyVectorScanResults & vec_scan_results_, VectorScanInfoPtr vector_scan_info_)
    : MergeTreeBaseSearchManager{nullptr, nullptr}
    , vector_scan_info(nullptr)
    , vector_scan_results(vec_scan_results_)
    {
        if (preComputed())
        {
            LOG_DEBUG(log, "Already have precomputed vector scan result, no need to execute search");
        }

        for (const auto & desc : vector_scan_info_->vector_scan_descs)
            search_func_cols_names.emplace_back(desc.column_name);
    }

    ~MergeTreeVSManager() override = default;

    void executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part) override;

    void executeSearchWithFilter(
        const MergeTreeData::DataPartPtr & data_part,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter) override;

    /// Two search search: execute vector scan to get accurate distance values
    /// If part doesn't have vector index or real index type doesn't support, just use passed in values.
    static VectorScanResultPtr executeSecondStageVectorScan(
        const MergeTreeData::DataPartPtr & data_part,
        const VSDescription & vector_scan_desc,
        const VectorScanResultPtr & first_stage_vec_result);

    /// Split num_reorder candidates based on part index: part + vector scan results from first stage
    static VectorAndTextResultInDataParts splitFirstStageVSResult(
        const VectorAndTextResultInDataParts & parts_with_mix_results,
        const ScoreWithPartIndexAndLabels & first_stage_top_results,
        const VSDescription & vector_scan_desc,
        LoggerPtr log);

    /// Filter parts using total top-k vector scan results from multiple distance functions
    /// For every part, select mark ranges to read, and save multiple vector scan results
    static SearchResultAndRangesInDataParts FilterPartsWithManyVSResults(
        const VectorAndTextResultInDataParts & parts_with_vector_text_result,
        const RangesInDataParts & parts_with_ranges,
        const std::unordered_map<String, ScoreWithPartIndexAndLabels> & vector_scan_results_with_part_index,
        const Settings & settings,
        LoggerPtr log);

    void mergeResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        for (const auto & result : vector_scan_results)
        {
            if (result && result->computed)
                return true;
        }

        return false;
    }

    /// Return first vector scan result if exists, used for hybrid search
    CommonSearchResultPtr getSearchResult() override
    {
        if (vector_scan_results.size() > 0)
            return vector_scan_results[0];
        else
            return nullptr;
    }

    /// Return all vector scan results
    ManyVectorScanResults getVectorScanResults() { return vector_scan_results; }

private:

    VectorScanInfoPtr vector_scan_info;
    std::vector<bool> vec_support_two_stage_searches;  /// True if vector index in metadata support two stage search

    /// Whether brute force search is enabled based on query setting
    bool enable_brute_force_search;

    /// Support multiple distance functions
    ManyVectorScanResults vector_scan_results;
    std::map<UInt64, std::vector<Float32>> map_labels_distances; /// sorted map with label ids and multiple distances
    size_t max_threads;

    LoggerPtr log = getLogger("MergeTreeVSManager");

    template <Search::DataType T>
    static VectorIndex::VectorDatasetPtr<T> generateVectorDataset(bool is_batch, const VSDescription & desc);

    template <>
    VectorIndex::VectorDatasetPtr<Search::DataType::FloatVector> generateVectorDataset(bool is_batch, const VSDescription & desc);

    template <>
    VectorIndex::VectorDatasetPtr<Search::DataType::BinaryVector> generateVectorDataset(bool is_batch, const VSDescription & desc);

    ManyVectorScanResults vectorScan(
        bool is_batch,
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const ReadRanges & read_ranges = ReadRanges(),
        const Search::DenseBitmapPtr filter = nullptr);

    /// brute force vector search
    template <Search::DataType T>
    VectorScanResultPtr vectorScanWithoutIndex(
        const MergeTreeData::DataPartPtr part,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter,
        VectorIndex::VectorDatasetPtr<T> & query_vector,
        const String & search_column,
        int dim,
        int k,
        bool is_batch,
        const VectorIndex::VIMetric & metric);

    void mergeBatchVectorScanResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        VectorScanResultPtr tmp_result = nullptr,
        const ColumnUInt64 * part_offset = nullptr);

    /// Support multiple distance functions
    /// vector search on a vector column
    VectorScanResultPtr vectorScanOnSingleColumn(
    bool is_batch,
    const MergeTreeData::DataPartPtr & data_part,
    const VSDescription vector_scan_desc,
    const ReadRanges & read_ranges,
    const bool support_two_stage_search,
    const Search::DenseBitmapPtr filter = nullptr);

    /// Merge multiple vector scan results with other columns
    void mergeMultipleVectorScanResults(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        const ManyVectorScanResults multiple_vector_scan_results = {},
        const ColumnUInt64 * part_offset = nullptr);

    template <Search::DataType T>
    void searchWrapper(
        bool prewhere,
        VectorIndex::VectorDatasetPtr<T> & query_vector,
        VectorIndex::VectorDatasetPtr<T> & base_data,
        int k,
        int dim,
        int nq,
        int num_rows_read,
        std::vector<int64_t> & final_id,
        std::vector<float> & final_distance,
        std::vector<size_t> & actual_id_in_range,
        const VectorIndex::VIMetric & metric,
        Search::DenseBitmapPtr & row_exists,
        int delete_id_nums);

    bool bruteForceSearchEnabled(const MergeTreeData::DataPartPtr & data_part);
};

using MergeTreeVectorScanManagerPtr = std::shared_ptr<MergeTreeVSManager>;

}
