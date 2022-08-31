#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/VectorScanResult.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <VectorIndex/VectorSegmentExecutor.h>

#include <Common/logger_useful.h>

namespace DB
{

/// vector scan manager, responsible for vector scan result precompute, and vector scan after read
class MergeTreeVectorScanManager
{
public:
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;

    MergeTreeVectorScanManager(
        StorageMetadataPtr metadata_,
        VectorScanInfoPtr vector_scan_info_) : metadata(metadata_), vector_scan_info(vector_scan_info_) {}

    void executeBeforeRead(const String& data_path, const MergeTreeData::DataPartPtr & data_part);

    void executeAfterRead(
        const String& data_path,
        const MergeTreeData::DataPartPtr & data_part,
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        bool has_prewhere = false,
        const FilterWithCachedCount & filter = FilterWithCachedCount{});

    void mergeResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        const FilterWithCachedCount & filter,
        const ColumnUInt64 * part_offset);

    bool preComputed() { return vector_scan_result != nullptr; }

    VectorScanResultPtr getVectorScanResult() { return vector_scan_result; }

    void eraseResult();

private:

    StorageMetadataPtr metadata;
    VectorScanInfoPtr vector_scan_info;

    /// lock vector scan result
    std::mutex mutex;

    VectorScanResultPtr vector_scan_result = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeVectorScanManager");

    VectorIndex::VectorDatasetPtr generateVectorDataset(bool is_batch, const VectorScanDescription& vector_scan_desc);

    VectorScanResultPtr vectorScan(
        bool is_batch,
        const String& data_path,
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const ReadRanges & read_ranges = ReadRanges(),
        const FilterWithCachedCount & filter = FilterWithCachedCount{});

    /// brute force vector search
    VectorScanResultPtr vectorScanWithoutIndex(
        const MergeTreeData::DataPartPtr part,
        const ReadRanges & read_ranges,
        const FilterWithCachedCount & filter,
        VectorIndex::VectorDatasetPtr & query_vector,
        const String & search_column,
        int dim,
        int k,
        bool is_batch,
        const VectorIndex::Metrics& metrics);

    void mergeBatchVectorScanResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        VectorScanResultPtr vector_scan_result = nullptr,
        const FilterWithCachedCount & filter = FilterWithCachedCount{},
        const ColumnUInt64 * part_offset = nullptr);

    void mergeVectorScanResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        VectorScanResultPtr vector_scan_result = nullptr,
        const FilterWithCachedCount & filter = FilterWithCachedCount{},
        const ColumnUInt64 * part_offset = nullptr);

    void searchWrapper(
        bool prewhere,
        VectorIndex::VectorDatasetPtr & query_vector,
        VectorIndex::VectorDatasetPtr & base_data,
        int k,
        int dim,
        int nq,
        int num_rows_read,
        std::vector<int64_t> & final_id,
        std::vector<float> & final_distance,
        std::vector<size_t> & actual_id_in_range,
        const VectorIndex::Metrics& metrics,
        VectorIndex::GeneralBitMapPtr& row_exists,
        int delete_id_nums);
};

using MergeTreeVectorScanManagerPtr = std::shared_ptr<MergeTreeVectorScanManager>;

}
