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

#include <mutex>

#include <Columns/ColumnsNumber.h>

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
        StorageMetadataPtr metadata_, VectorScanInfoPtr vector_scan_info_, ContextPtr context_, bool support_two_stage_search_ = false)
        : MergeTreeBaseSearchManager{metadata_, context_}
        , vector_scan_info(vector_scan_info_)
        , support_two_stage_search(support_two_stage_search_)
        , enable_brute_force_search(context_->getSettingsRef().enable_brute_force_vector_search)
    {
    }

    MergeTreeVSManager(VectorScanResultPtr vec_scan_result_ = nullptr)
    : MergeTreeBaseSearchManager{nullptr, nullptr}
    , vector_scan_info(nullptr)
    , vector_scan_result(vec_scan_result_)
    {
        if (vector_scan_result && vector_scan_result->computed)
        {
            LOG_DEBUG(log, "Already have precomputed vector scan result, no need to execute search");
        }
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
        const VectorScanInfoPtr vector_scan_info_,
        const VectorScanResultPtr & first_stage_vec_result);

    /// Split num_reorder candidates based on part index: part + vector scan results from first stage
    static VectorAndTextResultInDataParts splitFirstStageVSResult(
        const VectorAndTextResultInDataParts & parts_with_mix_results,
        const ScoreWithPartIndexAndLabels & first_stage_top_results,
        Poco::Logger * log);

    void mergeResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter = nullptr,
        const ColumnUInt64 * part_offset = nullptr) override;

    bool preComputed() override
    {
        return vector_scan_result && vector_scan_result->computed;
    }

    CommonSearchResultPtr getSearchResult() override { return vector_scan_result; }

private:

    VectorScanInfoPtr vector_scan_info;
    bool support_two_stage_search;  /// True if vector index in metadata support two stage search

    /// Whether brute force search is enabled based on query setting
    bool enable_brute_force_search;

    /// lock vector scan result
    std::mutex mutex;
    VectorScanResultPtr vector_scan_result = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeVSManager");

    template <Search::DataType T>
    static VectorIndex::VectorDatasetPtr<T> generateVectorDataset(bool is_batch, const VSDescription & desc);

    template <>
    VectorIndex::VectorDatasetPtr<Search::DataType::FloatVector> generateVectorDataset(bool is_batch, const VSDescription & desc);

    template <>
    VectorIndex::VectorDatasetPtr<Search::DataType::BinaryVector> generateVectorDataset(bool is_batch, const VSDescription & desc);

    VectorScanResultPtr vectorScan(
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
        const VIMetric & metric);

    void mergeBatchVectorScanResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        VectorScanResultPtr tmp_result = nullptr,
        const Search::DenseBitmapPtr = nullptr,
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
        const VIMetric & metric,
        Search::DenseBitmapPtr & row_exists,
        int delete_id_nums);

    bool bruteForceSearchEnabled(const MergeTreeData::DataPartPtr & data_part);
};

using MergeTreeVectorScanManagerPtr = std::shared_ptr<MergeTreeVSManager>;

}
