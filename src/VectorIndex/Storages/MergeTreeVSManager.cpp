#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Common/CurrentMetrics.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <VectorIndex/Common/BruteForceSearch.h>
#include <VectorIndex/Common/ScanThreadLimiter.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Storages/MergeTreeVSManager.h>
#include <VectorIndex/Storages/VSDescription.h>
#include <VectorIndex/Utils/VIUtils.h>
#include <VectorIndex/Utils/VSUtils.h>

#include <VectorIndex/Common/SegmentsMgr.h>
#include <VectorIndex/Common/Segment.h>

#include <memory>

/// #define profile

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectHybridSearchThreads;
    extern const Metric MergeTreeDataSelectHybridSearchThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int ILLEGAL_COLUMN;
    extern const int INVALID_VECTOR_INDEX;
}

template <typename FloatType>
std::vector<float> getQueryVector(const IColumn * query_vector_column, size_t dim, bool is_batch)
{
    const auto * query_data_concrete = checkAndGetColumn<ColumnVector<FloatType>>(query_vector_column);

    if (!query_data_concrete)
    {
        if (is_batch)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Float32 or Float64 inside Array(Array()) in batch distance function");
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Float32 or Float64 inside Array() in distance function");
    }

    const auto & query_vec = query_data_concrete->getData();

    size_t dim_of_query = query_vec.size();

    /// in batch distance case, dim_of_query = dim * offsets. dim in query is already checked in getFloatQueryVectorInBatch().
    if (!is_batch && (dim_of_query != dim))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Dimension is not equal: query: {} vs search column: {}",
            std::to_string(dim_of_query),
            std::to_string(dim));

    /// TODO: effectively transform float64 array to float32 array
    std::vector<float> query_new_data(dim_of_query);

    for (size_t i = 0; i < dim_of_query; ++i)
    {
        query_new_data[i] = static_cast<float>(query_vec[i]);
    }

    return query_new_data;
}

std::vector<float> getFloatQueryVectorInBatch(const IColumn * query_vectors_column, const size_t dim, int & query_vector_num)
{
    const ColumnArray * query_vectors_col = checkAndGetColumn<ColumnArray>(query_vectors_column);

    if (!query_vectors_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(Array()) in batch distance function");

    const IColumn & query_vectors = query_vectors_col->getData();
    auto & offsets = query_vectors_col->getOffsets();


    if (offsets.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of offsets.size() exceeds the limit of int data type");
    query_vector_num = static_cast<int>(offsets.size());

    for (size_t row = 0; row < offsets.size(); ++row)
    {
        size_t vec_start_offset = row != 0 ? offsets[row - 1] : 0;
        size_t vec_end_offset = offsets[row];
        size_t vec_size = vec_end_offset - vec_start_offset;
        if (vec_size != dim)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Having query vector with wrong dimension: {} vs search column dimension: {}",
                std::to_string(vec_size),
                std::to_string(dim));
    }

    std::vector<float> query_new_data;
    if (checkColumn<ColumnFloat32>(&query_vectors))
        query_new_data = getQueryVector<Float32>(&query_vectors, dim, true);
    else if (checkColumn<ColumnFloat64>(&query_vectors))
        query_new_data = getQueryVector<Float64>(&query_vectors, dim, true);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Float64 or Float32 inside Array(Array()) in batch distance function");

    return query_new_data;
}

template <>
VectorIndex::Float32VectorDatasetPtr MergeTreeVSManager::generateVectorDataset(bool is_batch, const VSDescription & desc)
{
    auto & query_column = desc.query_column;
    auto dim = desc.search_column_dim;

    if (!query_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type");

    ColumnPtr holder = query_column->convertToFullColumnIfConst();
    const ColumnArray * query_col = checkAndGetColumn<ColumnArray>(holder.get());

    if (is_batch)
    {
        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array in batch distance function");

        const IColumn & query_data = query_col->getData();

        int query_vector_num = 0;
        std::vector<float> query_new_data = getFloatQueryVectorInBatch(&query_data, dim, query_vector_num);

        return std::make_shared<VectorIndex::VectorDataset<Search::DataType::FloatVector>>(
                   query_vector_num,
                   static_cast<int32_t>(dim),
                   const_cast<float *>(query_new_data.data()));
    }
    else
    {
        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array in distance function");

        const IColumn & query_data = query_col->getData();

        std::vector<float> query_new_data;
        if (checkColumn<ColumnFloat32>(&query_data))
            query_new_data = getQueryVector<Float32>(&query_data, dim, false);
        else if (checkColumn<ColumnFloat64>(&query_data))
            query_new_data = getQueryVector<Float64>(&query_data, dim, false);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Float32 or Float64 inside Array() in distance function");

        return std::make_shared<VectorIndex::VectorDataset<Search::DataType::FloatVector>>(
            1,
            static_cast<int32_t>(dim),
            const_cast<float *>(query_new_data.data()));
    }
}

template <>
VectorIndex::BinaryVectorDatasetPtr MergeTreeVSManager::generateVectorDataset(bool is_batch, const VSDescription & desc)
{
    auto & query_column = desc.query_column;
    if (!query_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type");

    auto dim = desc.search_column_dim;
    if (dim % 8 != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Dimension of Binary vector must be a multiple of 8");

    ColumnPtr holder = query_column->convertToFullColumnIfConst();

    if (is_batch)
    {
        const ColumnArray * query_col = checkAndGetColumn<ColumnArray>(holder.get());
        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array in batch distance function");

        const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&(query_col->getData()));
        if (!src_data_concrete)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong query column type, expect fixed String inside Array() in batch distance function");

        auto &offsets = src_data_concrete->getOffsets();
        size_t query_vector_num = offsets.size();

        std::vector<uint8_t> query_data(query_vector_num * dim / 8);

        for (size_t i = 0; i < query_vector_num; i++)
        {
            size_t vec_start_offset = i != 0 ? offsets[i - 1] : 0;
            size_t vec_end_offset = offsets[i];

            // every string ends with terminating zero byte.
            size_t str_len = vec_end_offset - vec_start_offset - 1;
            if (str_len * 8 != dim)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong dimension in batch distance: {}, search column dimension: {}", std::to_string(str_len * 8), std::to_string(dim));
            }

            const char *str = src_data_concrete->getDataAt(i).data;
            std::memcpy(query_data.data() + i * str_len, str, str_len);
        }

        return std::make_shared<VectorIndex::VectorDataset<Search::DataType::BinaryVector>>(
                query_vector_num,
                static_cast<int32_t>(dim),
                const_cast<uint8_t *>(query_data.data()));
    }
    else
    {
        const ColumnString * query_col = checkAndGetColumn<ColumnString>(holder.get());

        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect fixed String in distance function");

        // every String column ends with terminating zero byte.
        auto bytes_of_query = query_col->getChars().size() - 1;
        if (bytes_of_query * 8 != dim)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Dimension for Binary vector search is not equal: query: {} vs search column: {}",
                bytes_of_query * 8,
                dim);
        const char * str_binary = query_col->getDataAt(0).data;

        std::vector<uint8_t> query_new_data(bytes_of_query);
        std::memcpy(query_new_data.data(), str_binary, bytes_of_query);

        return std::make_shared<VectorIndex::VectorDataset<Search::DataType::BinaryVector>>(
            1,
            static_cast<int32_t>(dim),
            const_cast<uint8_t *>(query_new_data.data()));
    }
}

void MergeTreeVSManager::executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeVSManager::executeSearchBeforeRead");

    /// Skip to execute vector scan if already computed
    if (!preComputed() && vector_scan_info)
        vector_scan_results = vectorScan(vector_scan_info->is_batch, data_part);
}

void MergeTreeVSManager::executeSearchWithFilter(
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const VectorIndex::VIBitmapPtr filter)
{
    /// Skip to execute vector scan if already computed
    if (!preComputed() && vector_scan_info)
        vector_scan_results = vectorScan(vector_scan_info->is_batch, data_part, read_ranges, filter);
}

ManyVectorScanResults MergeTreeVSManager::vectorScan(
    bool is_batch,
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const VectorIndex::VIBitmapPtr filter)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::vectorScan()");

    /// Support multiple distance functions
    ManyVectorScanResults multiple_vector_scan_results;

    size_t descs_size = vector_scan_info->vector_scan_descs.size();
    multiple_vector_scan_results.resize(descs_size);

    /// Do vector scan on a single column
    auto process_vector_scan_on_single_col = [&](size_t desc_index)
    {
        auto & vector_scan_desc = vector_scan_info->vector_scan_descs[desc_index];
        bool support_two_stage_search = vec_support_two_stage_searches[desc_index];

        /// Do vector scan on a vector column
        VectorScanResultPtr vec_scan_result = vectorScanOnSingleColumn(is_batch, data_part, vector_scan_desc, read_ranges, support_two_stage_search, filter);

        /// vectorScanOnSingleColumn() always return a non-null shared_ptr
        if (vec_scan_result)
        {
            /// Add name of distance function column
            vec_scan_result->name = vector_scan_desc.column_name;

            /// Add vector scan result on a vector column to results for multiple distances
            multiple_vector_scan_results[desc_index] = std::move(vec_scan_result);
        }
    };

    size_t num_threads = std::min<size_t>(max_threads, descs_size);
    if (num_threads <= 1)
    {
        for (size_t desc_index = 0; desc_index < descs_size; ++desc_index)
            process_vector_scan_on_single_col(desc_index);
    }
    else
    {
        /// Parallel executing vector scan
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreads,
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreadsActive,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
            num_threads);

        for (size_t desc_index = 0; desc_index < descs_size; ++desc_index)
            pool.scheduleOrThrowOnError([&, desc_index]()
            {
                process_vector_scan_on_single_col(desc_index);
            });

        pool.wait();
    }

    return multiple_vector_scan_results;
}

VectorScanResultPtr MergeTreeVSManager::vectorScanOnSingleColumn(
    bool is_batch,
    const MergeTreeData::DataPartPtr & data_part,
    const VSDescription vector_scan_desc,
    const ReadRanges & read_ranges,
    const bool support_two_stage_search,
    const Search::DenseBitmapPtr filter)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::vectorScanOnSingleColumn()");
    const String search_column_name = vector_scan_desc.search_column_name;

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<CommonSearchResult>();

    VectorIndex::VectorDatasetVariantPtr vec_data;
    switch (vector_scan_desc.vector_search_type)
    {
        case Search::DataType::FloatVector:
            vec_data = generateVectorDataset<Search::DataType::FloatVector>(is_batch, vector_scan_desc);
            break;
        case Search::DataType::BinaryVector:
            vec_data = generateVectorDataset<Search::DataType::BinaryVector>(is_batch, vector_scan_desc);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported vector search type for column {}", search_column_name);
    }

    VectorIndex::VIParameter search_params = VectorIndex::convertPocoJsonToMap(vector_scan_desc.vector_parameters);
    if (!search_params.empty())
    {
        LOG_DEBUG(log, "Search parameters: {} for vector column {}", search_params.toString(), search_column_name);
        search_params.erase("metric_type");
    }

    UInt64 dim = vector_scan_desc.search_column_dim;
    int k = vector_scan_desc.topk > 0 ? vector_scan_desc.topk : VectorIndex::DEFAULT_TOPK;
    LOG_DEBUG(log, "Set k to {}, dim to {} for vector column {}", k, dim, search_column_name);

    /// vector index search
    tmp_vector_scan_result->result_columns.resize(is_batch ? 3 : 2);
    auto vector_id_column = DataTypeUInt32().createColumn();
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();
    try
    {   
        auto vi_executor = data_part->segments_mgr->getSegmentByColumn(search_column_name);
        if (!vi_executor)
            throw Exception(ErrorCodes::VECTOR_INDEX_CACHE_LOAD_ERROR, "Vector index not found in part {}", data_part->name);

        int64_t query_vector_num = 0;
        std::visit([&query_vector_num](auto &&vec_data_ptr)
                   {
                       query_vector_num = vec_data_ptr->getVectorNum();
                   }, vec_data);

        /// find index
        OpenTelemetry::SpanHolder span3("MergeTreeVSManager::vectorScan()::find_index::search");
        LOG_DEBUG(log, "Start search: vector num: {}, two_stage_enable: {}", query_vector_num, support_two_stage_search);

        /// Although the vector index type support two stage search, the actual built index may fallback to flat.
        bool first_stage_only = support_two_stage_search;

        auto search_results = vi_executor->searchVI(vec_data, k, filter, search_params, first_stage_only);
        auto per_id = search_results->getResultIndices();
        auto per_distance = search_results->getResultDistances();

        k = search_results->getNumCandidates();

        if (is_batch)
        {
            OpenTelemetry::SpanHolder span4("MergeTreeVSManager::vectorScan()::find_index::segment_batch_generate_results");
            for (int64_t label = 0; label < k * query_vector_num; ++label)
            {
                UInt32 vector_id = static_cast<uint32_t>(label / k);
                if (per_id[label] > -1)
                {
                    label_column->insert(per_id[label]);
                    vector_id_column->insert(vector_id);
                    distance_column->insert(per_distance[label]);
                }
            }
            tmp_vector_scan_result->result_columns[1] = std::move(vector_id_column);
            tmp_vector_scan_result->result_columns[2] = std::move(distance_column);
        }
        else
        {
            OpenTelemetry::SpanHolder span4("MergeTreeVSManager::vectorScan()::find_index::segment_generate_results");
            for (int64_t label = 0; label < k; ++label)
            {
                if (per_id[label] > -1)
                {
                    LOG_TRACE(log, "Vector column: {}, label: {}, distance: {}", search_column_name, per_id[label], per_distance[label]);
                    label_column->insert(per_id[label]);
                    distance_column->insert(per_distance[label]);
                }
            }
            tmp_vector_scan_result->result_columns[1] = std::move(distance_column);
        }

        tmp_vector_scan_result->computed = true;
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);
        return tmp_vector_scan_result;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::VECTOR_INDEX_CACHE_LOAD_ERROR)
            throw;
    }

    if (bruteForceSearchEnabled(data_part))
    {
        VectorIndex::VIMetric metric = getVSMetric(data_part, vector_scan_desc);
        VectorScanResultPtr res_without_index;
        std::visit([&](auto &&vec_data_ptr)
        {
            res_without_index = vectorScanWithoutIndex(data_part, read_ranges, filter, vec_data_ptr, search_column_name, static_cast<int>(dim), k, is_batch, metric);
        }, vec_data);
        return res_without_index;
    }
    /// No vector index available and brute force search disabled
    tmp_vector_scan_result->computed = false;
    return tmp_vector_scan_result;
}

VectorScanResultPtr MergeTreeVSManager::executeSecondStageVectorScan(
    const MergeTreeData::DataPartPtr & data_part,
    const VSDescription & vector_scan_desc,
    const VectorScanResultPtr & first_stage_vec_result)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::executeSecondStageVectorScan()");
    LoggerPtr log_ = getLogger("executeSecondStageVectorScan");

    /// Currently, only FloatVector can create MSTG index, and use two stage search(MYSCALE_OSS_DELETE_LINE)
    if (vector_scan_desc.vector_search_type != Search::DataType::FloatVector)
        return first_stage_vec_result;

    const String search_column_name = vector_scan_desc.search_column_name;

    /// Prepare for two stage search
    const ColumnUInt32 * first_label_col = checkAndGetColumn<ColumnUInt32>(first_stage_vec_result->result_columns[0].get());
    const ColumnFloat32 * first_dist_col = checkAndGetColumn<ColumnFloat32>(first_stage_vec_result->result_columns[1].get());

    if (!first_label_col)
    {
        LOG_DEBUG(log_, "Label column is null");
        return first_stage_vec_result;
    }

    /// Determine the top k value, no more than passed in result size.
    int k = vector_scan_desc.topk > 0 ? vector_scan_desc.topk : VectorIndex::DEFAULT_TOPK;

    size_t num_reorder = first_label_col->size();
    if (k > static_cast<int>(num_reorder))
        k = static_cast<int>(num_reorder);

    LOG_DEBUG(log_, "[executeSecondStageVectorScan] topk = {}, result size from first stage = {}", vector_scan_desc.topk, num_reorder);

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<CommonSearchResult>();
    tmp_vector_scan_result->result_columns.resize(2);
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    /// Prepare parameters for computeTopDistanceSubset() if index supports two stage search
    VectorIndex::VectorDatasetVariantPtr vec_data = generateVectorDataset<Search::DataType::FloatVector>(false, vector_scan_desc);
    auto first_stage_result = Search::SearchResult::createTopKHolder(1, num_reorder);
    auto sr_indices = first_stage_result->getResultIndices();
    auto sr_distances = first_stage_result->getResultDistances();

    for (size_t i = 0; i < first_label_col->size(); i++)
    {
        sr_indices[i] = first_label_col->getUInt(i);
        sr_distances[i] = first_dist_col->getFloat32(i);
    }

    OpenTelemetry::SpanHolder span2("MergeTreeVSManager::executeSecondStageVectorScan()::before calling computeTopDistanceSubset");

    auto vi_seg = data_part->segments_mgr->getSegmentByColumn(search_column_name);
    std::shared_ptr<Search::SearchResult> search_results;
    if (vi_seg)
        search_results = vi_seg->computeTopDistanceSubset(vec_data, first_stage_result, k);
    else
    {
        LOG_DEBUG(log_, "Index {} does not support two stage search in part {}, use first stage result directly", search_column_name, data_part->name);
        search_results = first_stage_result;
    }

    /// Cut first stage result count (num_reorder) to top k for cases where index not support two stage search
    auto real_result_size = search_results->getNumCandidates() > k ? k : search_results->getNumCandidates();
    auto per_id = search_results->getResultIndices();
    auto per_distance = search_results->getResultDistances();

    for (int64_t label = 0; label < real_result_size; ++label)
    {
        if (per_id[label] > -1)
        {
            LOG_TRACE(log_, "Label: {}, distance: {}", per_id[label], per_distance[label]);
            label_column->insert(per_id[label]);
            distance_column->insert(per_distance[label]);
        }
    }

    if (label_column->size() > 0)
    {
        tmp_vector_scan_result->computed = true;
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);
        tmp_vector_scan_result->result_columns[1] = std::move(distance_column);
    }

    return tmp_vector_scan_result;
}

VectorAndTextResultInDataParts MergeTreeVSManager::splitFirstStageVSResult(
    const VectorAndTextResultInDataParts & parts_with_mix_results,
    const ScoreWithPartIndexAndLabels & first_stage_top_results,
    const VSDescription & vector_scan_desc,
    LoggerPtr log)
{
    /// Merge candidate vector results from the same part index into a vector
    std::map<size_t, std::vector<ScoreWithPartIndexAndLabel>> part_index_merged_map;

    for (const auto & score_with_part_index_label : first_stage_top_results)
    {
        const auto & part_index = score_with_part_index_label.part_index;
        part_index_merged_map[part_index].emplace_back(score_with_part_index_label);
    }

    VectorAndTextResultInDataParts parts_with_vector_result;

    /// Construct new vector scan result for data part existing in top candidates
    for (const auto & mix_results_in_part : parts_with_mix_results)
    {
        size_t part_index = mix_results_in_part.part_index;

        /// Found data part in first stage top results
        if (part_index_merged_map.contains(part_index))
        {
            auto & score_with_part_index_labels = part_index_merged_map[part_index];

            /// Construct new vector scan result for second stage
            VectorScanResultPtr tmp_vector_scan_result = std::make_shared<CommonSearchResult>();

            tmp_vector_scan_result->result_columns.resize(2);
            auto score_column = DataTypeFloat32().createColumn();
            auto label_column = DataTypeUInt32().createColumn();

            LOG_TEST(log, "First stage vector scan result for part {}:", mix_results_in_part.data_part->name);
            for (const auto & score_with_part_index_label : score_with_part_index_labels)
            {
                const auto & label_id = score_with_part_index_label.label_id;
                const auto & score = score_with_part_index_label.score;

                LOG_TEST(log, "Label: {}, score: {}", label_id, score);
                label_column->insert(label_id);
                score_column->insert(score);
            }

            if (label_column->size() > 0)
            {
                tmp_vector_scan_result->computed = true;
                tmp_vector_scan_result->result_columns[0] = std::move(label_column);
                tmp_vector_scan_result->result_columns[1] = std::move(score_column);

                /// Add result column name
                tmp_vector_scan_result->name = vector_scan_desc.column_name;

                VectorAndTextResultInDataPart part_with_vector(part_index, mix_results_in_part.data_part);
                part_with_vector.vector_scan_results.emplace_back(tmp_vector_scan_result);

                parts_with_vector_result.emplace_back(std::move(part_with_vector));
            }
        }
    }

    return parts_with_vector_result;
}

SearchResultAndRangesInDataParts MergeTreeVSManager::FilterPartsWithManyVSResults(
    const VectorAndTextResultInDataParts & parts_with_vector_text_result,
    const RangesInDataParts & parts_with_ranges,
    const std::unordered_map<String, ScoreWithPartIndexAndLabels> & vector_scan_results_with_part_index,
    const Settings & settings,
    LoggerPtr log)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::FilterPartsWithManyVSResults()");

    std::unordered_map<size_t, ManyVectorScanResults> part_index_merged_results_map;
    std::unordered_map<size_t, std::set<UInt64>> part_index_merged_labels_map;

    for (const auto & [col_name, score_with_part_index_labels] : vector_scan_results_with_part_index)
    {
        /// For each vector scan, merge results from the same part index into a VectorScanResult
        std::unordered_map<size_t, VectorScanResultPtr> part_index_single_vector_scan_result_map;
        for (const auto & score_with_label : score_with_part_index_labels)
        {
            const auto & part_index = score_with_label.part_index;
            const auto & label_id = score_with_label.label_id;
            const auto & score = score_with_label.score;

            /// Save all labels from multiple vector scan results
            part_index_merged_labels_map[part_index].emplace(label_id);

            /// Construct single vector scan result for each part
            if (part_index_single_vector_scan_result_map.contains(part_index))
            {
                auto & single_vector_scan_result = part_index_single_vector_scan_result_map[part_index];
                single_vector_scan_result->result_columns[0]->insert(label_id);
                single_vector_scan_result->result_columns[1]->insert(score);
            }
            else
            {
                VectorScanResultPtr single_vector_scan_result = std::make_shared<CommonSearchResult>();
                single_vector_scan_result->result_columns.resize(2);

                auto label_column = DataTypeUInt32().createColumn();
                auto score_column = DataTypeFloat32().createColumn();

                label_column->insert(label_id);
                score_column->insert(score);

                single_vector_scan_result->computed = true;
                single_vector_scan_result->name = col_name;
                single_vector_scan_result->result_columns[0] = std::move(label_column);
                single_vector_scan_result->result_columns[1] = std::move(score_column);

                /// Save in map
                part_index_single_vector_scan_result_map[part_index] = single_vector_scan_result;
            }
        }

        /// For each data part, put single vector scan result into ManyVectorScanResults
        for (const auto & [part_index, single_vector_scan_result] : part_index_single_vector_scan_result_map)
            part_index_merged_results_map[part_index].emplace_back(single_vector_scan_result);
    }

    size_t parts_with_ranges_size = parts_with_ranges.size();
    SearchResultAndRangesInDataParts parts_with_ranges_vector_scan_result;
    parts_with_ranges_vector_scan_result.resize(parts_with_ranges_size);

    /// Filter data part with part index in vector scan results, filter mark ranges with label ids
    auto filter_part_with_results = [&](size_t part_index)
    {
        const auto & part_with_ranges = parts_with_ranges[part_index];

        /// Check if part_index exists in result map
        if (part_index_merged_results_map.contains(part_index))
        {
            /// Filter mark ranges with label ids in multiple vector scan results
            MarkRanges mark_ranges_for_part = part_with_ranges.ranges;
            auto & labels_set = part_index_merged_labels_map[part_index];
            filterMarkRangesByLabels(part_with_ranges.data_part, settings, labels_set, mark_ranges_for_part);

            if (!mark_ranges_for_part.empty())
            {
                /// Save can_skip_perform_prefilter
                bool can_skip_perform_prefilter = false;
                for (const auto & part_with_mix_results : parts_with_vector_text_result)
                {
                    if (part_with_mix_results.part_index == part_index)
                    {
                        can_skip_perform_prefilter = part_with_mix_results.can_skip_perform_prefilter;
                        break;
                    }
                }

                RangesInDataPart ranges(part_with_ranges.data_part,
                        part_with_ranges.alter_conversions,
                        part_with_ranges.part_index_in_query,
                        std::move(mark_ranges_for_part));
                SearchResultAndRangesInDataPart results_with_ranges(std::move(ranges), can_skip_perform_prefilter, part_index_merged_results_map[part_index]);
                parts_with_ranges_vector_scan_result[part_index] = std::move(results_with_ranges);
            }
        }
    };

    size_t num_threads = std::min<size_t>(settings.max_threads, parts_with_ranges_size);
    if (num_threads <= 1)
    {
        for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
            filter_part_with_results(part_index);
    }
    else
    {
        /// Parallel executing filter parts_in_ranges with total top-k results
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreads,
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreadsActive,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
            num_threads);

        for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
            pool.scheduleOrThrowOnError([&, part_index]()
                {
                    filter_part_with_results(part_index);
                });

        pool.wait();
    }

    /// Skip empty search result
    size_t next_part = 0;
    for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
    {
        auto & part_with_results = parts_with_ranges_vector_scan_result[part_index];
        if (part_with_results.multiple_vector_scan_results.empty())
            continue;

        if (next_part != part_index)
            std::swap(parts_with_ranges_vector_scan_result[next_part], part_with_results);
        ++next_part;
    }

    LOG_TRACE(log, "[FilterPartsWithManyVSResults] The size of parts with vector scan results: {}", next_part);
    parts_with_ranges_vector_scan_result.resize(next_part);

    return parts_with_ranges_vector_scan_result;
}

void MergeTreeVSManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const ColumnUInt64 * part_offset)
{
    if (vector_scan_info && vector_scan_info->is_batch)
    {
        mergeBatchVectorScanResult(pre_result, read_rows, read_ranges, vector_scan_results[0], part_offset);
    }
    else if (search_func_cols_names.size() == 1)
    {
        mergeSearchResultImpl(pre_result, read_rows, read_ranges, vector_scan_results[0], part_offset);
    }
    else
    {
        mergeMultipleVectorScanResults(pre_result, read_rows, read_ranges, vector_scan_results, part_offset);
    }
}

void MergeTreeVSManager::mergeBatchVectorScanResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    VectorScanResultPtr tmp_result,
    const ColumnUInt64 * part_offset)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::mergeBatchVectorScanResult()");
    const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(tmp_result->result_columns[0].get());
    const ColumnUInt32 * vector_id_column = checkAndGetColumn<ColumnUInt32>(tmp_result->result_columns[1].get());
    const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(tmp_result->result_columns[2].get());

    auto final_vector_id_column = DataTypeUInt32().createColumn();
    auto final_distance_column = DataTypeFloat32().createColumn();

    /// create new column vector to save final results
    MutableColumns final_result;
    for (auto & col : pre_result)
    {
        final_result.emplace_back(col->cloneEmpty());
    }

    if (part_offset == nullptr)
    {
        size_t start_pos = 0;
        size_t end_pos = 0;
        size_t prev_row_num = 0;

        /// when no filter, the prev read result should be continuous, so we just need to scan all result rows and
        /// keep results of which the row id is contained in label_column
        for (auto & read_range : read_ranges)
        {
            start_pos = read_range.start_row;
            end_pos = read_range.start_row + read_range.row_num;
            for (size_t ind = 0; ind < label_column->size(); ++ind)
            {
                if (label_column->getUInt(ind) >= start_pos && label_column->getUInt(ind) < end_pos)
                {
                    for (size_t i = 0; i < final_result.size(); ++i)
                    {
                        Field field;
                        pre_result[i]->get(label_column->getUInt(ind) - start_pos + prev_row_num, field);
                        final_result[i]->insert(field);
                    }

                    final_vector_id_column->insert(vector_id_column->getUInt(ind));
                    final_distance_column->insert(distance_column->getFloat32(ind));
                }
            }
            prev_row_num += read_range.row_num;
        }
    }
    else // part_offset != nullptr
    {
        /// when no filter, the prev read result should be continuous, so we just need to scan all result rows and
        /// keep results of which the row id is contained in label_column
        for (auto & read_range : read_ranges)
        {
            const size_t start_pos = read_range.start_row;
            const size_t end_pos = read_range.start_row + read_range.row_num;
            for (size_t ind = 0; ind < label_column->size(); ++ind)
            {
                const UInt64 physical_pos = label_column->getUInt(ind);

                if (physical_pos >= start_pos && physical_pos < end_pos)
                {
                    const ColumnUInt64::Container & offset_raw_value = part_offset->getData();
                    const size_t part_offset_column_size = part_offset->size();
                    size_t logic_pos = 0;
                    bool logic_pos_found = false;
                    for (size_t j = 0; j < part_offset_column_size; ++j)
                    {
                        if (offset_raw_value[j] == physical_pos)
                        {
                            logic_pos_found = true;
                            logic_pos = j;
                        }
                    }

                    if (!logic_pos_found)
                    {
                        continue;
                    }

                    for (size_t i = 0; i < final_result.size(); ++i)
                    {
                        Field field;
                        pre_result[i]->get(logic_pos, field);
                        final_result[i]->insert(field);
                    }

                    final_vector_id_column->insert(vector_id_column->getUInt(ind));
                    final_distance_column->insert(distance_column->getFloat32(ind));
                }
            }
        }
    }

    for (size_t i = 0; i < pre_result.size(); ++i)
    {
        pre_result[i] = std::move(final_result[i]);
    }

    read_rows = final_distance_column->size();

    Columns cols(2);
    cols[0] = std::move(final_vector_id_column);
    cols[1] = std::move(final_distance_column);
    auto distance_tuple_column = ColumnTuple::create(cols);


    pre_result.emplace_back(std::move(distance_tuple_column));
}

/// 1. read raw vector data block by block
/// 2. for each block, compute topk targets
/// 3. get the first topk targets
template <Search::DataType T>
VectorScanResultPtr MergeTreeVSManager::vectorScanWithoutIndex(
    const MergeTreeData::DataPartPtr part,
    const ReadRanges & read_ranges,
    const VectorIndex::VIBitmapPtr filter,
    VectorIndex::VectorDatasetPtr<T> & query_vector,
    const String & search_column,
    int dim,
    int k,
    bool is_batch,
    const VectorIndex::VIMetric & metric)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::vectorScanWithoutIndex()");
    /// Limit the number of vector index search threads to 2 * number of physical cores
    static VectorIndex::LimiterSharedContext brute_force_context(getNumberOfPhysicalCPUCores() * 2);
    VectorIndex::ScanThreadLimiter limiter(brute_force_context, log);

    NamesAndTypesList cols;
    /// get search vector column info from part's metadata instead of table's
    /// Avoid issues when a new vector column has been added and existing old parts don't have it.
    auto col_and_type = part->tryGetColumn(search_column);
    if (col_and_type)
    {
        cols.emplace_back(*col_and_type);
    }
    else
    {
        /// wrong column
        throw Exception(ErrorCodes::LOGICAL_ERROR, "vector column {} doesn't exists in part {}", search_column, part->name);
    }

    /// only consider no prewhere case
    if (part->storage.hasLightweightDeletedMask())
    {
        cols.emplace_back(RowExistsColumn::name, RowExistsColumn::type);
    }

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<CommonSearchResult>();
    tmp_vector_scan_result->result_columns.resize(is_batch ? 3 : 2);

    size_t nq = query_vector->getVectorNum();
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();
    auto vector_id_column = DataTypeUInt32().createColumn();

    auto alter_conversions = part->storage.getAlterConversionsForPart(part);
    MergeTreeReaderSettings reader_settings;
    reader_settings.save_marks_in_cache = true;

    StorageSnapshotPtr storage_snapshot_ptr = std::make_shared<StorageSnapshot>(part->storage, metadata);

    /// create part reader to read vector column
    auto reader = part->getReader(
        cols,
        storage_snapshot_ptr,
        MarkRanges{MarkRange(0, part->getMarksCount())},
        /*vitual_fields = */ {},
        /* uncompressed_cache = */ nullptr,
        part->storage.getContext()->getMarkCache().get(),
        alter_conversions,
        reader_settings,
        /*ValueSizeMap*/ {},
        ReadBufferFromFileBase::ProfileCallback{});

    size_t current_mark = 0;
    size_t total_rows_in_part = part->rows_count;
    const auto & index_granularity = part->index_granularity;

    size_t num_rows_read = 0;

    size_t default_read_num = 0;

    bool continue_read = false;

    std::vector<float> final_distance;
    if (metric == VectorIndex::VIMetric::IP)
    {
        final_distance = std::vector<float>(k * nq, std::numeric_limits<float>().min());
    }
    else
    {
        final_distance = std::vector<float>(k * nq, std::numeric_limits<float>().max());
    }

    std::vector<int64_t> final_id(k * nq, -1);
    /// has filter, should filter vector data passed to brute force search function, filter already includes LWD _row_exists, won't consider
    /// LWD either in this case
    if (filter)
    {
        size_t filter_parsed = 0;
        [[maybe_unused]] size_t range_num = 0;

        /// for debugging filter
        [[maybe_unused]] size_t part_left_rows = 0;
        size_t range_left_rows = 0;
        size_t mark_left_rows = 0;

        size_t current_rows_in_mark = 0;
        size_t current_rows_in_range = 0;

        /// used to test filter passed in is correct
        LOG_TRACE(
            log,
            "VectorScanManager with filter, Part: {}, Filter Size: {}, Filter Byte Size: {}, Count in Filter is:{}",
            part->name,
            filter->get_size(),
            filter->byte_size(),
            filter->count()
            );

        for (const auto & single_range : read_ranges)
        {
            LOG_TRACE(
                log,
                "VectorScanManager Part: {}, Range: {}, Row Numbers in Range: {}, Start Mark: {}, End Mark: {}, start_row: {}, ",
                part->name,
                range_num,
                read_ranges[range_num].row_num,
                read_ranges[range_num].start_mark,
                read_ranges[range_num].end_mark,
                read_ranges[range_num].start_row);

            /// for each single_range, will only fetch data of one mark each time
            current_mark = single_range.start_mark;
            current_rows_in_range = 0;
            range_left_rows = 0;
            mark_left_rows = 0;

            while (current_mark < single_range.end_mark)
            {

                Columns result;
                result.resize(cols.size());

                default_read_num = index_granularity.getMarkRows(current_mark);
                /// read all rows in one part once, continue_read should be false to only read data of one mark in by one time
                size_t num_rows = reader->readRows(current_mark, 0, continue_read, default_read_num, result);
                current_mark ++;
                current_rows_in_mark = 0;

                if (num_rows == 0)
                {
                    /// num_rows equals 0 means vector column is empty, filter should skip those rows
                    filter_parsed += default_read_num;
                    continue;
                }

                size_t total_rows = 0;
                std::vector<size_t> actual_id_in_range;
                VectorIndex::VectorDatasetPtr<T> base_data;
                std::vector<typename VectorIndex::SearchIndexDataTypeMap<T>::VectorDatasetType, AllocatorWithMemoryTracking<typename VectorIndex::SearchIndexDataTypeMap<T>::VectorDatasetType>> vector_raw_data;

                /// prepare continuous data
                /// data of search column stored in one_column, commonly is vector data
                const auto & one_column = result[0];
                if (!one_column)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "vector column {} doesn't exists in part {}", search_column, part->name);

                if constexpr (T == Search::DataType::FloatVector)
                {
                    const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
                    const IColumn & src_data = array->getData();
                    const ColumnArray::Offsets & __restrict offsets = array->getOffsets();
                    const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
                    const PaddedPODArray<Float32> & __restrict src_vec = src_data_concrete->getData();

                    if (src_vec.empty())
                        continue;

                    total_rows = offsets.size();
                    actual_id_in_range.reserve(total_rows);
                    vector_raw_data.reserve(dim * total_rows);

                    /// filter out the data we want to do ANN on using the filter
                    size_t start_pos = filter_parsed;

                    LOG_TRACE(
                        get_logger(),
                        "filter_parsed:{}",
                        filter_parsed);

                    /// this outer for loop's i is the position of data relative to the filter
                    /// imagine filter as a array of array:[0,1,0,0,0,1,...][0,1,0...][...]
                    /// where actually all these arrays are concatenated into a single array and
                    /// each array represents all the rows in a single range
                    mark_left_rows = 0;
                    for (size_t i = start_pos; i < start_pos + default_read_num; ++i)
                    {
                        ///filter and num_rows could be larger than real row size in this mark
                        if (i == filter->get_size())
                            break;

                        if (filter->unsafe_test(i))
                        {
                            size_t vec_start_offset = current_rows_in_mark != 0 ? offsets[current_rows_in_mark - 1] : 0;
                            size_t vec_end_offset = offsets[current_rows_in_mark];
                            if(vec_start_offset != vec_end_offset)
                            {
                                for (size_t offset = vec_start_offset; offset < vec_end_offset; ++offset)
                                    vector_raw_data.emplace_back(src_vec[offset]);

                                LOG_TRACE(
                                    get_logger(),
                                    "current_rows_in_range:{}, i:{}, src_vec[vec_start_offset]:{}",
                                    current_rows_in_range,
                                    i,
                                    src_vec[vec_start_offset]);

                                actual_id_in_range.emplace_back(current_rows_in_range);
                                mark_left_rows ++;
                            }
                        }

                        current_rows_in_mark++;
                        current_rows_in_range++;
                    }

                    filter_parsed += default_read_num;

                    if (vector_raw_data.empty())
                    {
                        assert(mark_left_rows == 0);
                        continue;
                    }

                    base_data = std::make_shared<VectorIndex::VectorDataset<T>>(
                            static_cast<int32_t>(mark_left_rows),
                            static_cast<int32_t>(dim),
                            const_cast<float *>(vector_raw_data.data()));

                    assert(vector_raw_data.size() == mark_left_rows * dim);
                }
                else if constexpr (T == Search::DataType::BinaryVector)
                {
                    mark_left_rows = 0;
                    size_t start_pos = filter_parsed;

                    using BinaryVectorDatasetType = typename VectorIndex::SearchIndexDataTypeMap<Search::DataType::BinaryVector>::VectorDatasetType;
                    if (const ColumnFixedString *fixed_string = checkAndGetColumn<ColumnFixedString>(one_column.get()))
                    {
                        auto fixed_N = fixed_string->getN();
                        total_rows = fixed_string->size();
                        if (total_rows == 0)
                            return nullptr;

                        actual_id_in_range.reserve(total_rows);
                        vector_raw_data.reserve(total_rows * fixed_N);

                        /// this outer for loop's i is the position of data relative to the filter
                        /// imagine filter as a array of FixedString(N):[0001..., 0010...., ...]
                        /// where actually all these arrays are concatenated into a single array and
                        /// each array represents all the rows in a single range
                        for (size_t i = start_pos; i < start_pos + default_read_num; ++i)
                        {
                            ///filter and num_rows could be larger than real row size in this mark
                            if (i == filter->get_size())
                                break;

                            if (filter->unsafe_test(i))
                            {
                                auto binary_vector_data = reinterpret_cast<const BinaryVectorDatasetType *>(fixed_string->getDataAt(current_rows_in_mark).data);
                                vector_raw_data.insert(vector_raw_data.end(), binary_vector_data, binary_vector_data + fixed_N);
                                actual_id_in_range.emplace_back(current_rows_in_range);
                                mark_left_rows++;
                            }
                            current_rows_in_mark++;
                            current_rows_in_range++;
                        }
                    }
                    /// BinaryVector is represented as FixedString(N), sometimes it maybe Sparse(FixedString(N))
                    else if (const ColumnSparse *sparse_column = checkAndGetColumn<ColumnSparse>(one_column.get()))
                    {
                        LOG_INFO(get_logger(), "test DB::VectorSearchType::BinaryVector: Sparse(FixedString(N))");  /// MYSCALE_OSS_DELETE_LINE
                        const ColumnFixedString * sparse_fixed_string = checkAndGetColumn<ColumnFixedString>(&(sparse_column->getValuesColumn()));
                        if (!sparse_fixed_string)
                            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Vector column type for BinaryVector is not FixString(N) in column {}", search_column);

                        auto fixed_N = sparse_fixed_string->getN();
                        total_rows = sparse_fixed_string->size();
                        if (total_rows == 0)
                            return nullptr;

                        actual_id_in_range.reserve(total_rows);
                        vector_raw_data.reserve(total_rows * fixed_N);

                        for (size_t i = start_pos; i < start_pos + default_read_num; ++i)
                        {
                            if (i == filter->get_size())
                                break;

                            if (filter->unsafe_test(i))
                            {
                                auto binary_vector_data = reinterpret_cast<const BinaryVectorDatasetType *>(sparse_fixed_string->getDataAt(current_rows_in_mark).data);
                                vector_raw_data.insert(vector_raw_data.end(), binary_vector_data, binary_vector_data + fixed_N);
                                actual_id_in_range.emplace_back(current_rows_in_range);
                                mark_left_rows++;
                            }
                            current_rows_in_mark++;
                            current_rows_in_range++;
                        }
                    }
                    else
                        throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Vector column type for BinaryVector is not FixString(N) in column {}", search_column);

                    filter_parsed += default_read_num;

                    if (vector_raw_data.empty())
                    {
                        assert(mark_left_rows == 0);
                        continue;
                    }

                    base_data = std::make_shared<VectorIndex::VectorDataset<T>>(
                            static_cast<int32_t>(mark_left_rows),
                            static_cast<int32_t>(dim),
                            const_cast<uint8_t *>(vector_raw_data.data()));

                    assert(vector_raw_data.size() == mark_left_rows * dim / 8);
                }

                VectorIndex::VIBitmapPtr row_exists = std::make_shared<VectorIndex::VIBitmap>(mark_left_rows, true);

                /// invokes searchWrapper each time reading one mark, use actual_id_in_range to record the real row id in each range,
                /// so row_exists bitmap won't be used, and the num_read_rows will be zero for that we use real row id already.
                searchWrapper(
                        true,
                        query_vector,
                        base_data,
                        k,
                        dim,
                        static_cast<int>(nq),
                        0,
                        final_id,
                        final_distance,
                        actual_id_in_range,
                        metric,
                        row_exists,
                        0);

                if(mark_left_rows)
                {
                    LOG_TRACE(
                        log,
                        "Part: {}, Range: {}, Mark: {}, Rows In Mark: {}, rows left in mark: {}, raw_data size: {}, vector index name: {}, path: {}",
                        part->name,
                        range_num,
                        current_mark,
                        current_rows_in_mark,
                        mark_left_rows,
                        vector_raw_data.size(),
                        "brute force",
                        part->getDataPartStorage().getFullPath());
                }
                range_left_rows += mark_left_rows;
            }

            if(range_left_rows)
            {
                LOG_TRACE(
                    log,
                    "Part: {}, Range:{}, Total Marks:{}, Rows In Range: {}, filter read:{}, rows left in range:{}",
                    part->name,
                    range_num,
                    single_range.end_mark - single_range.start_mark,
                    current_rows_in_range,
                    filter_parsed,
                    range_left_rows);
            }
            range_num ++;
            part_left_rows += range_left_rows;
        }

        if(part_left_rows)
        {
            LOG_TRACE(log, "Part:{}, rows left in part:{}", part->name, part_left_rows);
        }
    }
    else
    {
        default_read_num = index_granularity.getMarkRows(current_mark);
        /// has no filter, will pass the vector data and the dense bitmap for deleted rows to search function
        while (num_rows_read < total_rows_in_part)
        {
            LOG_TRACE(
                log,
                "VectorScanManager Part: {}, Row numbers in mark: {}, ",
                part->name,
                total_rows_in_part);

            size_t max_read_row = std::min((total_rows_in_part - num_rows_read), default_read_num);
            Columns result;
            result.resize(cols.size());
            /// will read rows of a part, continue_read will be set true after read first time
            size_t num_rows = reader->readRows(current_mark, 0, continue_read, max_read_row, result);
            current_mark ++;

            continue_read = true;

            if (num_rows == 0)
                break;

            VectorIndex::VectorDatasetPtr<T> base_data;
            std::vector<typename VectorIndex::SearchIndexDataTypeMap<T>::VectorDatasetType, AllocatorWithMemoryTracking<typename VectorIndex::SearchIndexDataTypeMap<T>::VectorDatasetType>> vector_raw_data;
            const auto & one_column = result[0];
            if (!one_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "vector column {} doesn't exists in part {}", search_column, part->name);
            size_t total_rows = 0;

            if constexpr (T == Search::DataType::FloatVector)
            {
                const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
                const IColumn & src_data = array->getData();
                const ColumnArray::Offsets & offsets = array->getOffsets();
                total_rows = offsets.size();

                const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
                if (!src_data_concrete)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Bad type of column {}", cols.back().name);
                const PaddedPODArray<Float32> & src_vec = src_data_concrete->getData();

                if (src_vec.empty())
                {
                    num_rows_read += num_rows;
                    continue;
                }

                vector_raw_data.assign(total_rows * dim, std::numeric_limits<typename VectorIndex::SearchIndexDataTypeMap<T>::VectorDatasetType>().max());
                for (size_t row = 0; row < total_rows; row++)
                {
                    size_t vec_start_offset = row != 0 ? offsets[row - 1] : 0;
                    size_t vec_end_offset = offsets[row];
                    if(vec_start_offset != vec_end_offset)
                    {
                        for (size_t offset = vec_start_offset; offset < vec_end_offset && offset < vec_start_offset + dim; ++offset)
                        {
                            vector_raw_data[row * dim + offset - vec_start_offset] = src_vec[offset];
                        }
                    }
                }
            }
            else if constexpr (T == Search::DataType::BinaryVector)
            {
                if (const ColumnFixedString *fixed_string = checkAndGetColumn<ColumnFixedString>(one_column.get()))
                {
                    total_rows = fixed_string->size();
                    auto fixed_N = fixed_string->getN();

                    vector_raw_data.reserve(total_rows * fixed_N);
                    auto &chars = fixed_string->getChars();
                    std::memcpy(vector_raw_data.data(), chars.data(), chars.size());
                }
                /// BinaryVector is represented as FixedString(N), sometimes it maybe Sparse(FixedString(N))
                else if (const ColumnSparse *sparse_column = checkAndGetColumn<ColumnSparse>(one_column.get()))
                {
                    const ColumnFixedString * sparse_fixed_string = checkAndGetColumn<ColumnFixedString>(&(sparse_column->getValuesColumn()));
                    if (!sparse_fixed_string)
                        throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Vector column type for BinaryVector is not FixString(N) in column {}", search_column);

                    total_rows = sparse_column->size();
                    auto fixed_N = sparse_fixed_string->getN();
                    vector_raw_data.reserve(total_rows * fixed_N);

                    using BinaryVectorDatasetType = typename VectorIndex::SearchIndexDataTypeMap<Search::DataType::BinaryVector>::VectorDatasetType;
                    for (size_t i = 0; i < sparse_column->size(); i++)
                    {
                        std::memcpy(vector_raw_data.data() + i * fixed_N, reinterpret_cast<const BinaryVectorDatasetType *>(sparse_column->getDataAt(i).data), sparse_column->getDataAt(i).size);
                    }
                }
                else
                    throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Vector column type for BinaryVector is not FixString(N) in column {}", search_column);
            }

            LOG_TRACE(
                log,
                "Part: {}, "
                "num_rows: {}, "
                "raw_data size: {}",
                part->name,
                num_rows,
                vector_raw_data.size());

            int deleted_row_num = 0;
            VectorIndex::VIBitmapPtr row_exists = std::make_shared<VectorIndex::VIBitmap>(total_rows, true);

            //make sure result contain lwd row_exists column
            if (result.size() == 2 && part->storage.hasLightweightDeletedMask())
            {
                LOG_TRACE(
                    log,
                    "Try to get row exists col, result size: {}",
                    result.size());
                const auto& row_exists_col = result[1];
                if (row_exists_col)
                {
                    const ColumnUInt8 * col = checkAndGetColumn<ColumnUInt8>(row_exists_col.get());
                    const auto & col_data = col->getData();
                    LOG_TRACE(log, "Col data size: {}", col_data.size()); /// MYSCALE_OSS_DELETE_LINE
                    for (size_t i = 0; i < col_data.size(); i++)
                    {
                        if (!col_data[i])
                        {
                            LOG_TRACE(log, "Unset: {}", i);
                            ++deleted_row_num;
                            row_exists->unset(i);
                        }
                    }
                }
            }

            base_data = std::make_shared<VectorIndex::VectorDataset<T>>(
                    static_cast<int32_t>(total_rows),
                    static_cast<int32_t>(dim),
                    const_cast<typename VectorIndex::SearchIndexDataTypeMap<T>::VectorDatasetType *>(vector_raw_data.data()));

            std::vector<size_t> place_holder;

            /// invoke searchWrapper each time after reading rows, if a vector is empty, the vector will not be wrote into the base_data.
            /// while the size of base_data equals with size of reading rows including the rows with empty vector, row_exists bitmap has
            /// inverted values with _row_exits column of the LWD
            searchWrapper(
                false,
                query_vector,
                base_data,
                k,
                dim,
                static_cast<int>(nq),
                static_cast<int>(num_rows_read),
                final_id,
                final_distance,
                place_holder,
                metric,
                row_exists,
                deleted_row_num);

            num_rows_read += num_rows;

            LOG_TRACE(
                log,
                "Part: {}, num_rows: {}, vector index name: {}, path: {}",
                part->name,
                num_rows,
                "brute force",
                part->getDataPartStorage().getFullPath());
        }
    }

    LOG_DEBUG(log, "Part: {}, total num rows read: {}", part->name, num_rows_read);

    /// batch search case
    if (is_batch)
    {
        for (size_t label = 0; label < k * nq; ++label)
        {
            UInt32 vector_id = static_cast<uint32_t>(label / k);
            if (final_id[label] > -1)
            {
                label_column->insert(final_id[label]);
                vector_id_column->insert(vector_id);
                distance_column->insert(final_distance[label]);
            }
        }
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);
        tmp_vector_scan_result->result_columns[1] = std::move(vector_id_column);
        tmp_vector_scan_result->result_columns[2] = std::move(distance_column);
    }
    /// single vector case
    else
    {
        for (size_t label = 0; label < k * nq; ++label)
        {
            if (final_id[label] > -1)
            {
                LOG_TRACE(log, "Label: {}, distance: {}", final_id[label], final_distance[label]);  /// MYSCALE_OSS_DELETE_LINE
                label_column->insert(final_id[label]);
                distance_column->insert(final_distance[label]);
            }
        }
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);
        tmp_vector_scan_result->result_columns[1] = std::move(distance_column);
    }

    tmp_vector_scan_result->computed = true;
    return tmp_vector_scan_result;
}

template <Search::DataType T>
void MergeTreeVSManager::searchWrapper(
    bool prewhere,
    VectorIndex::VectorDatasetPtr<T> & query_vector,
    VectorIndex::VectorDatasetPtr<T> & base_data,
    int k,
    int /* dim */,
    int nq,
    int num_rows_read,
    std::vector<int64_t> & final_id,
    std::vector<float> & final_distance,
    std::vector<size_t> & actual_id_in_range,
    const VectorIndex::VIMetric & metric,
    VectorIndex::VIBitmapPtr & row_exists,
    int delete_id_num)
{
    std::vector<float> per_distance;
    std::vector<float> tmp_per_distance;

    switch (T)
    {
        case Search::DataType::FloatVector:
        {
            if (metric == VectorIndex::VIMetric::IP)
            {
                per_distance = std::vector<float>(k * nq, std::numeric_limits<float>().min());
                if (delete_id_num > 0)
                    tmp_per_distance = std::vector<float>((k + delete_id_num) * nq, std::numeric_limits<float>().min());
            }
            else if (metric == VectorIndex::VIMetric::Cosine || metric == VectorIndex::VIMetric::L2)
            {
                per_distance = std::vector<float>(k * nq, std::numeric_limits<float>().max());
                if (delete_id_num > 0)
                    tmp_per_distance = std::vector<float>((k + delete_id_num) * nq, std::numeric_limits<float>().max());
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported metric type for Float32 vector");
            break;
        }
        case Search::DataType::BinaryVector:
        {
            if (metric == VectorIndex::VIMetric::Hamming || metric == VectorIndex::VIMetric::Jaccard)
            {
                per_distance = std::vector<float>(k * nq, std::numeric_limits<float>().max());
                if (delete_id_num > 0)
                    tmp_per_distance = std::vector<float>((k + delete_id_num) * nq, std::numeric_limits<float>().max());
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported metric type for Binary vector");
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type");
    }

    std::vector<int64_t> per_id(k * nq, -1);
    std::vector<int64_t> tmp_per_id;
    float * distance_data;
    int64_t * id_data;

    if (delete_id_num > 0)
    {
        tmp_per_id = std::vector<int64_t>((k + delete_id_num) * nq);
        distance_data = tmp_per_distance.data();
        id_data = tmp_per_id.data();
    }
    else
    {
        distance_data = per_distance.data();
        id_data = per_id.data();
    }


    LOG_TRACE(log, "the base data length:{}", base_data->getVectorNum());

    VectorIndex::SimpleSegment<T>::searchWithoutIndex(
        query_vector, base_data, k + delete_id_num, distance_data, id_data, metric);

    if (delete_id_num > 0)
    {
        for (int i = 0; i < nq; i++)
        {
            /// pos for this single query vector
            int curr_pos = 0;
            int tmp_curr_pos = 0;
            while (curr_pos < k && tmp_curr_pos < k + delete_id_num)
            {
                auto & tmp_id = tmp_per_id[i * (k + delete_id_num) + tmp_curr_pos];
                if (tmp_id >= 0 && row_exists->is_member(tmp_id))
                {
                    per_id[i * k + curr_pos] = tmp_per_id[i * (k + delete_id_num) + tmp_curr_pos];
                    per_distance[i * k + curr_pos] = tmp_per_distance[i * (k + delete_id_num) + tmp_curr_pos];
                    ++curr_pos;
                }
                ++tmp_curr_pos;
            }
        }
    }

    std::vector<float> intermediate_distance;
    intermediate_distance.reserve(k * nq);
    std::vector<int64_t> intermediate_ids;
    intermediate_ids.reserve(k * nq);

    /// in case of prewhere, the data needed to scan is sparse, so we compress sparse data into concrete array and record their original id
    /// in actual_id_in_range.
    if (prewhere)
    {
        for (int i = 0; i < k * nq; i++)
        {
            /// security check
            if (per_id[i] > -1)
                per_id[i] = actual_id_in_range[per_id[i]];
        }
    }

    for (int q = 0; q < nq; q++)
    {
        size_t current_result_offset = q * k;

        size_t j = current_result_offset;
        size_t z = current_result_offset;
        for (int i = 0; i < k; i++)
        {
            if ((metric != VectorIndex::VIMetric::IP && final_distance[j] > per_distance[z])
                || (metric == VectorIndex::VIMetric::IP && final_distance[j] < per_distance[z]))
            {
                intermediate_distance.emplace_back(per_distance[z]);
                intermediate_ids.emplace_back(per_id[z] + num_rows_read);
                z++;
            }
            else
            {
                intermediate_distance.emplace_back(final_distance[j]);
                intermediate_ids.emplace_back(final_id[j]);
                j++;
            }
        }
    }
    intermediate_distance.resize(k * nq);
    intermediate_ids.resize(k * nq);
    final_distance = std::move(intermediate_distance);
    final_id = std::move(intermediate_ids);
}

bool MergeTreeVSManager::bruteForceSearchEnabled(const MergeTreeData::DataPartPtr & data_part)
{
    /// Always enable for small part
    if (data_part->isSmallPart())
        return true;
    else
        return enable_brute_force_search;
}

void MergeTreeVSManager::mergeMultipleVectorScanResults(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const ManyVectorScanResults multiple_vector_scan_results,
    const ColumnUInt64 * part_offset)
{
    OpenTelemetry::SpanHolder span("MergeTreeVSManager::mergeMultipleVectorScanResults()");

    /// Use search_func_cols_names in base manager
    size_t distances_size = search_func_cols_names.size();

    /// Initialize the sorted map by the result of multiple distances
    if (map_labels_distances.empty())
    {
        /// Loop vector scan result for multiple distances
        for (const auto & tmp_result : multiple_vector_scan_results)
        {
            if (!tmp_result)
               continue;

            const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(tmp_result->result_columns[0].get());
            const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(tmp_result->result_columns[1].get());
            const String distance_col_name = tmp_result->name;

            size_t dist_index = 0;
            bool found = false;
            for (size_t i = 0; i < distances_size; ++i)
            {
                if (search_func_cols_names[i] == distance_col_name)
                {
                    dist_index = i;
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                LOG_DEBUG(log, "Unknown distance column name '{}'", distance_col_name);
                continue;
            }

            if (!label_column)
            {
                LOG_DEBUG(log, "Label colum is null for distance column name '{}'", distance_col_name);
                continue;
            }

            for (size_t ind = 0; ind < label_column->size(); ++ind)
            {
                /// Get label_id and distance value
                auto label_id = label_column->getUInt(ind);
                auto distance_score = distance_column->getFloat32(ind);

                if (!map_labels_distances.contains(label_id))
                {
                    /// Default value for distance is NaN
                    map_labels_distances[label_id].resize(distances_size, NAN);
                }

                map_labels_distances[label_id][dist_index] = distance_score;
            }
        }

        if (map_labels_distances.size() > 0)
            was_result_processed.assign(map_labels_distances.size(), false);

        LOG_TEST(log, "[mergeMultipleVectorScanResults] results size: {}", map_labels_distances.size());
        for (const auto & [label_id, dists_vec]: map_labels_distances)
        {
            String distances;
            for (size_t i = 0; i < dists_vec.size(); ++i)
                distances += (" " + toString(dists_vec[i]));

            LOG_TEST(log, "label: {}, distances:{}", label_id, distances);
        }
    }

    /// create new column vector to save final results
    MutableColumns final_result;
    LOG_DEBUG(log, "Create final result");
    for (auto & col : pre_result)
    {
        final_result.emplace_back(col->cloneEmpty());
    }

    /// Create column vector to save multiple distances result
    MutableColumns distances_result;
    for (size_t i = 0; i < distances_size; ++i)
        distances_result.emplace_back(ColumnFloat32::create());

    if (part_offset == nullptr)
    {
        LOG_DEBUG(log, "No part offset");
        size_t start_pos = 0;
        size_t end_pos = 0;
        size_t prev_row_num = 0;

        for (auto & read_range : read_ranges)
        {
            start_pos = read_range.start_row;
            end_pos = read_range.start_row + read_range.row_num;

            /// loop map of labels and distances
            size_t map_ind = 0;
            for (const auto & [label_value, distance_score_vec] : map_labels_distances)
            {
                if (was_result_processed[map_ind])
                {
                    map_ind++;
                    continue;
                }

                if (label_value >= start_pos && label_value < end_pos)
                {
                    const size_t index_of_arr = label_value - start_pos + prev_row_num;
                    for (size_t i = 0; i < final_result.size(); ++i)
                    {
                        Field field;
                        pre_result[i]->get(index_of_arr, field);
                        final_result[i]->insert(field);
                    }

                    /// for each distance column
                    auto distances_score_vec = map_labels_distances[label_value];
                    for (size_t col = 0; col < distances_size; ++col)
                        distances_result[col]->insert(distances_score_vec[col]);

                    was_result_processed[map_ind] = true;
                    map_ind++;
                }
            }
            prev_row_num += read_range.row_num;
        }
    }
    else if (part_offset->size() > 0)
    {
        LOG_DEBUG(log, "Get part offset");

        /// When lightweight delete applied, the rowid in the label column cannot be used as index of pre_result.
        /// Match the rowid in the value of label col and the value of part_offset to find the correct index.
        const ColumnUInt64::Container & offset_raw_value = part_offset->getData();
        size_t part_offset_size = part_offset->size();

        /// loop map of labels and distances
        size_t map_ind = 0;
        for (const auto & [label_value, distance_score_vec] : map_labels_distances)
        {
            if (was_result_processed[map_ind])
            {
                map_ind++;
                continue;
            }

            /// label_value (row id) = part_offset.
            /// We can use binary search to quickly locate part_offset for current label.
            size_t low = 0;
            size_t high = part_offset_size;
            while (low < high)
            {
                const size_t mid = low + (high - low) / 2;

                if (offset_raw_value[mid] == label_value)
                {
                    /// Use the index of part_offset to locate other columns in pre_result and fill final_result.
                    for (size_t i = 0; i < final_result.size(); ++i)
                    {
                        Field field;
                        pre_result[i]->get(mid, field);
                        final_result[i]->insert(field);
                    }

                    /// for each distance column
                    auto distances_score_vec = map_labels_distances[label_value];
                    for (size_t col = 0; col < distances_size; ++col)
                        distances_result[col]->insert(distances_score_vec[col]);

                    was_result_processed[map_ind] = true;
                    map_ind++;

                    /// break from binary search loop
                    break;
                }
                else if (offset_raw_value[mid] < label_value)
                    low = mid + 1;
                else
                    high = mid;
            }
        }
    }

    for (size_t i = 0; i < pre_result.size(); ++i)
        pre_result[i] = std::move(final_result[i]);

    read_rows = distances_result[0]->size();
    for (size_t i = 0; i < distances_size; ++i)
        pre_result.emplace_back(std::move(distances_result[i]));
}

}
