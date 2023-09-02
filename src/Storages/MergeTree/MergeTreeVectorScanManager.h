#pragma once

#include <mutex>

#include <Columns/ColumnsNumber.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/VectorScanResult.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/logger_useful.h>

#include <VectorIndex/Dataset.h>

#include <SearchIndex/VectorIndex.h>

namespace VectorIndex
{
    class VectorSegmentExecutor;
    using VectorSegmentExecutorPtr = std::shared_ptr<VectorSegmentExecutor>;
}

namespace DB
{

/// vector scan manager, responsible for vector scan result precompute, and vector scan after read
class MergeTreeVectorScanManager
{
public:
    using ReadRange = MergeTreeRangeReader::ReadResult::ReadRangeInfo;
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;

    MergeTreeVectorScanManager(
        StorageMetadataPtr metadata_,
        VectorScanInfoPtr vector_scan_info_,
        ContextPtr context_,
        bool support_two_stage_search_ = false)
        : metadata(metadata_)
        , vector_scan_info(vector_scan_info_)
        , context(context_)
        , support_two_stage_search(support_two_stage_search_) {}

    void executeBeforeRead(const String& data_path, const MergeTreeData::DataPartPtr & data_part);

    void executeAfterRead(
        const String& data_path,
        const MergeTreeData::DataPartPtr & data_part,
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        bool has_prewhere = false,
        const Search::DenseBitmapPtr filter = nullptr);

    void executeVectorScanWithFilter(
        const String& data_path,
        const MergeTreeData::DataPartPtr & data_part,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter);

    /// Two search search: execute vector scan to get accurate distance values
    /// If part doesn't have vector index or real index type doesn't support, just use passed in values.
    VectorScanResultPtr executeSecondStageVectorScan(
        const MergeTreeData::DataPartPtr & data_part,
        const std::vector<UInt64> & row_ids,
        const std::vector<Float32> & distances);

    void mergeResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter = nullptr,
        const ColumnUInt64 * part_offset = nullptr);

    bool preComputed() { return vector_scan_result != nullptr; }

    VectorScanResultPtr getVectorScanResult() { return vector_scan_result; }

    void eraseResult();

    Settings getSettings() { return context->getSettingsRef(); }

private:

    StorageMetadataPtr metadata;
    VectorScanInfoPtr vector_scan_info;
    ContextPtr context;
    bool support_two_stage_search;  /// True if vector index in metadata support two stage search

    /// lock vector scan result
    std::mutex mutex;

    VectorScanResultPtr vector_scan_result = nullptr;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeVectorScanManager");

    VectorIndex::VectorDatasetPtr generateVectorDataset(bool is_batch, const VectorScanDescription& vector_scan_desc);

    VectorScanResultPtr vectorScan(
        bool is_batch,
        const String & data_path,
        const MergeTreeData::DataPartPtr & data_part = nullptr,
        const ReadRanges & read_ranges = ReadRanges(),
        const Search::DenseBitmapPtr filter = nullptr);

    /// Do preparition of finding index for vectorScan() and executeSecondStageVectorScan()
    std::vector<VectorIndex::VectorSegmentExecutorPtr> prepareForVectorScan(
        String & metric_str,
        const String & data_path,
        const MergeTreeData::DataPartPtr & data_part = nullptr
    );

    /// brute force vector search
    VectorScanResultPtr vectorScanWithoutIndex(
        const MergeTreeData::DataPartPtr part,
        const ReadRanges & read_ranges,
        const Search::DenseBitmapPtr filter,
        VectorIndex::VectorDatasetPtr & query_vector,
        const String & search_column,
        int dim,
        int k,
        bool is_batch,
        const Search::Metric & metric);

    void mergeBatchVectorScanResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        VectorScanResultPtr tmp_result = nullptr,
        const Search::DenseBitmapPtr = nullptr,
        const ColumnUInt64 * part_offset = nullptr);

    void mergeVectorScanResult(
        Columns & pre_result,
        size_t & read_rows,
        const ReadRanges & read_ranges = ReadRanges(),
        VectorScanResultPtr tmp_result = nullptr,
        const Search::DenseBitmapPtr = nullptr,
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
        const Search::Metric & metric,
        Search::DenseBitmapPtr & row_exists,
        int delete_id_nums);
};

using MergeTreeVectorScanManagerPtr = std::shared_ptr<MergeTreeVectorScanManager>;

}
