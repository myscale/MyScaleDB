#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Common/FieldVisitorConvertToNumber.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <Storages/MergeTree/MergeTreeVectorScanManager.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <VectorIndex/BruteForceSearch.h>
#include <VectorIndex/MergeUtils.h>
#include <VectorIndex/Status.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/VectorSegmentExecutor.h>

#include <memory>

/// #define profile

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

    /// in batch distance case, dim_of_query = dim * offsets. dim in query is already checked in getQueryVectorInBatch().
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

std::vector<float> getQueryVectorInBatch(const IColumn * query_vectors_column, const size_t dim, int & query_vector_num)
{
    const ColumnArray * query_vectors_col = checkAndGetColumn<ColumnArray>(query_vectors_column);

    if (!query_vectors_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(Array)) in batch distance function");

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

void MergeTreeVectorScanManager::eraseResult()
{
    if (vector_scan_result->is_batch)
    {
        vector_scan_result->result_columns[0] = DataTypeUInt32().createColumn();
        vector_scan_result->result_columns[1] = DataTypeUInt32().createColumn();
        vector_scan_result->result_columns[2] = DataTypeFloat32().createColumn();
    }
    else
    {
        vector_scan_result->result_columns[0] = DataTypeUInt32().createColumn();
        vector_scan_result->result_columns[1] = DataTypeFloat32().createColumn();
    }
}

VectorIndex::VectorDatasetPtr MergeTreeVectorScanManager::generateVectorDataset(bool is_batch, const VectorScanDescription& desc)
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
        std::vector<float> query_new_data = getQueryVectorInBatch(&query_data, dim, query_vector_num);

        // default value
        Search::Parameters search_params = VectorIndex::convertPocoJsonToMap(desc.vector_parameters);
        int k = desc.topk > 0 ? desc.topk : VectorIndex::DEFAULT_TOPK;
        LOG_DEBUG(log, "Set k to {}, dim to {}", k, dim);

        return std::make_shared<VectorIndex::VectorDataset>(
            query_vector_num, static_cast<int32_t>(dim), const_cast<float *>(query_new_data.data()));
    }
    else
    {
        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array in distance function");

        const IColumn & query_data = query_col->getData();

        LOG_DEBUG(log, "dim to {}", dim);

        std::vector<float> query_new_data;
        if (checkColumn<ColumnFloat32>(&query_data))
            query_new_data = getQueryVector<Float32>(&query_data, dim, false);
        else if (checkColumn<ColumnFloat64>(&query_data))
            query_new_data = getQueryVector<Float64>(&query_data, dim, false);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Float32 or Float64 inside Array() in distance function");

        return std::make_shared<VectorIndex::VectorDataset>(1, static_cast<int32_t>(dim), const_cast<float *>(query_new_data.data()));
    }
}

void MergeTreeVectorScanManager::executeBeforeRead(const String& data_path, const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::executeBeforeRead");
    this->vector_scan_result = vectorScan(vector_scan_info->is_batch, data_path, data_part);
}

void MergeTreeVectorScanManager::executeAfterRead(
    const String& data_path,
    const MergeTreeData::DataPartPtr & data_part,
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    bool has_prewhere,
    const Search::DenseBitmapPtr filter)
{
    if (vector_scan_info->is_batch)
    {
        if (has_prewhere)
        {
            VectorScanResultPtr tmp_result = vectorScan(true, data_path, data_part, read_ranges, filter);
            mergeBatchVectorScanResult(pre_result, read_rows, read_ranges, tmp_result, filter);
        }
        else
        {
            // no prewhere case
            mergeBatchVectorScanResult(pre_result, read_rows, read_ranges, this->vector_scan_result, filter);
        }
    }
    else
    {
        if (has_prewhere)
        {
            VectorScanResultPtr tmp_result = vectorScan(false, data_path, data_part, read_ranges, filter);
            mergeVectorScanResult(pre_result, read_rows, read_ranges, tmp_result, filter);
        }
        else
        {
            // no prewhere case
            mergeVectorScanResult(pre_result, read_rows, read_ranges, this->vector_scan_result, filter);
        }
    }
}

void MergeTreeVectorScanManager::executeVectorScanWithFilter(
    const String& data_path,
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const Search::DenseBitmapPtr filter)
{
    this->vector_scan_result = vectorScan(vector_scan_info->is_batch, data_path, data_part, read_ranges, filter);
}

VectorScanResultPtr MergeTreeVectorScanManager::vectorScan(
    bool is_batch,
    const String & data_path,
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const Search::DenseBitmapPtr filter)
{
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScan()");
    bool find_index = false;
    const VectorScanDescriptions & descs = vector_scan_info->vector_scan_descs;

    const VectorScanDescription & desc = descs[0];
    const String search_column_name = desc.search_column_name;

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<VectorScanResult>();

    tmp_vector_scan_result->result_columns.resize(3);
    auto vector_id_column = DataTypeUInt32().createColumn();
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    auto vec_data = generateVectorDataset(is_batch, desc);

    UInt64 dim = desc.search_column_dim;
    Search::Parameters search_params = VectorIndex::convertPocoJsonToMap(desc.vector_parameters);
    LOG_DEBUG(log, "Search parameters: {}", search_params.toString());

    int k = desc.topk > 0 ? desc.topk : VectorIndex::DEFAULT_TOPK;
    search_params.erase("metric_type");

    LOG_DEBUG(log, "Set k to {}, dim to {}", k, dim);

    String metric_str;
    std::vector<VectorIndex::VectorSegmentExecutorPtr> vec_executors = prepareForVectorScan(metric_str, data_path, data_part);

    find_index = vec_executors.size() > 0;

    Search::Metric metric = VectorIndex::getMetric(metric_str);

    if (find_index)
    {
        /// find index
        for (VectorIndex::VectorSegmentExecutorPtr & vec_executor : vec_executors)
        {
            OpenTelemetry::SpanHolder span3("MergeTreeVectorScanManager::vectorScan()::find_index::search");

            Search::DenseBitmapPtr real_filter = nullptr;
            if (filter != nullptr)
            {
                real_filter = vec_executor->getRealBitmap(filter);
            }

            if (real_filter != nullptr && !real_filter->any())
            {
                /// don't perform vector search if the segment is completely filtered out
                continue;
            }

            LOG_DEBUG(log, "Start search: vector num: {}", vec_data->getVectorNum());

            /// Although the vector index type support two stage search, the actual built index may fallback to flat.
            bool first_stage_only = false;
            if (support_two_stage_search && vec_executor->supportTwoStageSearch())
                first_stage_only = true;

            LOG_DEBUG(log, "first stage only = {}", first_stage_only);

            auto search_results = vec_executor->search(vec_data, k, real_filter, search_params, first_stage_only);
            auto per_id = search_results->getResultIndices();
            auto per_distance = search_results->getResultDistances();

            /// Update k value to num_reorder in two search stage.
            if (first_stage_only)
                k = search_results->getNumCandidates();

            if (is_batch)
            {
                OpenTelemetry::SpanHolder span4("MergeTreeVectorScanManager::vectorScan()::find_index::segment_batch_generate_results");
                for (int64_t label = 0; label < k * vec_data->getVectorNum(); ++label)
                {
                    UInt32 vector_id = static_cast<uint32_t>(label / k);
                    if (per_id[label] > -1)
                    {
                        label_column->insert(per_id[label]);
                        vector_id_column->insert(vector_id);
                        distance_column->insert(per_distance[label]);
                    }
                }
            }
            else
            {
                OpenTelemetry::SpanHolder span4("MergeTreeVectorScanManager::vectorScan()::find_index::segment_generate_results");
                for (int64_t label = 0; label < k; ++label)
                {
                    if (per_id[label] > -1)
                    {
                        label_column->insert(per_id[label]);
                        distance_column->insert(per_distance[label]);
                    }
                }
            }
        }

        if (is_batch)
        {
            OpenTelemetry::SpanHolder span3("MergeTreeVectorScanManager::vectorScan()::find_index::data_part_batch_generate_results");
            tmp_vector_scan_result->query_vector_num = static_cast<int>(vec_data->getVectorNum());
            tmp_vector_scan_result->result_columns[1] = std::move(vector_id_column);
            tmp_vector_scan_result->result_columns[2] = std::move(distance_column);
        }
        else
        {
            OpenTelemetry::SpanHolder span3("MergeTreeVectorScanManager::vectorScan()::find_index::data_part_generate_results");
            tmp_vector_scan_result->query_vector_num = 1;
            tmp_vector_scan_result->result_columns[1] = std::move(distance_column);
        }

        tmp_vector_scan_result->is_batch = is_batch;
        tmp_vector_scan_result->top_k = k;
        tmp_vector_scan_result->computed = true;
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);

        return tmp_vector_scan_result;
    }
    else
    {
        return vectorScanWithoutIndex(data_part, read_ranges, filter, vec_data, search_column_name, static_cast<int>(dim), k, is_batch, metric);
    }
}

VectorScanResultPtr MergeTreeVectorScanManager::executeSecondStageVectorScan(
    const MergeTreeData::DataPartPtr & data_part,
    const std::vector<UInt64> & row_ids,
    const std::vector<Float32> & distances)
{
    /// Reference vectorScan() for non-batch vector search
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::executeSecondStageVectorScan()");
    span.addAttribute("secondsearchstage.num_reorder", row_ids.size());
    const VectorScanDescriptions & descs = vector_scan_info->vector_scan_descs;

    const VectorScanDescription & desc = descs[0];

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<VectorScanResult>();

    tmp_vector_scan_result->result_columns.resize(2);
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    /// Determine the top k value, no more than passed in row_ids size.
    int k = desc.topk > 0 ? desc.topk : VectorIndex::DEFAULT_TOPK;
    span.addAttribute("secondsearchstage.origin_topk", k);
    size_t num_reorder = row_ids.size();
    if (k > static_cast<int>(num_reorder))
        k = static_cast<int>(num_reorder);

    /// Get segment ids for vector index
    const String data_path = data_part->getDataPartStorage().getFullPath();

    [[maybe_unused]] String metric_str;
    std::vector<VectorIndex::VectorSegmentExecutorPtr> vec_executors = prepareForVectorScan(metric_str, data_path, data_part);

    bool brute_force = vec_executors.size() == 0;
    if (brute_force)
    {
        /// Data part has no vector index, no need to do two stage search.
        for (int64_t label = 0; label < k; label++)
        {
            label_column->insert(row_ids[label]);
            distance_column->insert(distances[label]);
        }
    }
    else
    {
        /// Prepare paramters for computeTopDistanceSubset() if index supports two stage search
        auto vec_data = generateVectorDataset(false, desc);
        auto first_stage_result = Search::SearchResult::createTopKHolder(1, num_reorder);
        auto sr_indices = first_stage_result->getResultIndices();
        auto sr_distances = first_stage_result->getResultDistances();

        for (size_t i = 0; i < row_ids.size(); i++)
        {
            sr_indices[i] = row_ids[i];
            sr_distances[i] = distances[i];
        }

        OpenTelemetry::SpanHolder span2("MergeTreeVectorScanManager::executeSecondStageVectorScan()::before calling computeTopDistanceSubset");

        for (VectorIndex::VectorSegmentExecutorPtr & vec_executor : vec_executors)
        {
            std::shared_ptr<Search::SearchResult> real_first_stage_result = nullptr;

            {
                OpenTelemetry::SpanHolder span3("MergeTreeVectorScanManager::executeSecondStageVectorScan()::TransferToOldRowIds()");
                /// Try to transfer to old part's row ids for decouple part. And skip if no need.
                real_first_stage_result = vec_executor->TransferToOldRowIds(first_stage_result);
            }

            /// No rows needed from this old data part
            if (!real_first_stage_result)
                continue;

            std::shared_ptr<Search::SearchResult> search_results;
            {
                OpenTelemetry::SpanHolder span4("MergeTreeVectorScanManager::executeSecondStageVectorScan()::computeTopDistanceSubset()");
                if (vec_executor->supportTwoStageSearch())
                {
                    search_results = vec_executor->computeTopDistanceSubset(vec_data, real_first_stage_result, k);
                }
                else
                    search_results = real_first_stage_result;
            }

            /// Cut first stage result count (num_reorder) to top k for cases where index not support two stage search
            auto real_result_size = search_results->getNumCandidates();
            if (real_result_size > k)
                real_result_size = k;

            auto per_id = search_results->getResultIndices();
            auto per_distance = search_results->getResultDistances();

            for (int64_t label = 0; label < real_result_size; ++label)
            {
                if (per_id[label] > -1)
                {
                    label_column->insert(per_id[label]);
                    distance_column->insert(per_distance[label]);
                }
            }
        }
    }

    if (label_column->size() > 0)
    {
        tmp_vector_scan_result->top_k = k;
        tmp_vector_scan_result->computed = true;
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);
        tmp_vector_scan_result->result_columns[1] = std::move(distance_column);
    }
    else /// no result
    {
        tmp_vector_scan_result->computed = false;
        LOG_DEBUG(log, "[executeSecondStageVectorScan] Failed to get second stage result for part {}", data_part->name);
    }

    return tmp_vector_scan_result;
}

std::vector<VectorIndex::VectorSegmentExecutorPtr> MergeTreeVectorScanManager::prepareForVectorScan(
    String & metric_str,
    const String & data_path,
    const MergeTreeData::DataPartPtr & data_part)
{
    std::vector<VectorIndex::VectorSegmentExecutorPtr> vec_executors;

    VectorIndexDescription index;
    bool find_index = false;
    const VectorIndicesDescription & vector_indices = metadata->vec_indices;
    const VectorScanDescriptions & descs = vector_scan_info->vector_scan_descs;

    const VectorScanDescription & desc = descs[0];
    const String search_column_name = desc.search_column_name;

    UInt64 dim = desc.search_column_dim;

    metric_str = data_part->storage.getSettings()->vector_search_metric_type;

    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(data_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
    }

    std::vector<VectorIndex::SegmentId> segment_ids;
    for (auto & v_index : vector_indices)
    {
        if (v_index.column == search_column_name)
        {
            if (v_index.parameters && v_index.parameters->has("metric_type"))
            {
                metric_str = v_index.parameters->getValue<String>("metric_type");
            }

            segment_ids = VectorIndex::getAllSegmentIds(data_path, data_part, v_index.name, v_index.column);
            if (segment_ids.size() >= 1)
            {
                find_index = true;
                index = v_index;
                LOG_DEBUG(log, "Index found, because index segment_ids is not empty");
                String cache_key = segment_ids[0].getCacheKey().toString();
                LOG_DEBUG(log, "Cache key = {}", cache_key);
                break;
            }
        }
    }

    /// Will use brute force search.
    if (!find_index)
        return vec_executors;

    Search::Metric metric = VectorIndex::getMetric(metric_str);

    LOG_DEBUG(log, "Find index, segment_ids size: {}", segment_ids.size());
    Search::IndexType index_type = VectorIndex::getIndexType(index.type);
    Search::Parameters index_params = VectorIndex::convertPocoJsonToMap(index.parameters);
    index_params.erase("metric_type");
    DB::OpenTelemetry::SpanHolder span2("MergeTreeVectorScanManager::vectorScan()::find_index");
    span2.addAttribute("vectorScan.segment_ids", segment_ids.size());

    bool retry = false;
    bool brute_force = false;
    bool is_shutdown = false;

    size_t min_bytes_to_build_vector_index = data_part->storage.getSettings()->min_bytes_to_build_vector_index;
    int default_mstg_disk_mode = data_part->storage.getSettings()->default_mstg_disk_mode;
    for (VectorIndex::SegmentId & segment_id : segment_ids)
    {
        LOG_DEBUG(log, "Create vector segment executor for : {}", segment_id.getFullPath());
        // FIXME (qliu): rows_count is wrong for decoupled parts
        VectorIndex::VectorSegmentExecutorPtr vec_executor = std::make_shared<VectorIndex::VectorSegmentExecutor>(
            segment_id,
            index_type,
            metric,
            dim,
            data_part->rows_count,
            index_params,
            min_bytes_to_build_vector_index,
            default_mstg_disk_mode);

        is_shutdown = data_part->storage.isShutdown();
        if (!is_shutdown)
        {
            VectorIndex::Status status = vec_executor->load(data_part->getState() == MergeTreeDataPartState::Active);
            LOG_DEBUG(log, "Vector number in index: {}", vec_executor->getRawDataSize());
            LOG_DEBUG(log, "Load vector index: {}", status.getCode());

            if (status.getCode() == ErrorCodes::INVALID_VECTOR_INDEX)
            {
                /// inactive part reload vector index cache, behavior is prohibited
                LOG_WARNING(log, "Query using vector index was canceled due to a concurrent inactive part reload vector index");
                context->getQueryContext()->killCurrentQuery();
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled.");
            }
            else if (!status.fine())
            {
                /// case of merged vector indices had been removed, we need to use new vector index files
                LOG_ERROR(log, "Fail to load vector index: {}", segment_id.getFullPath());
                retry = true;
                brute_force = true;
                break;
            }
        }
        else
        {
            break;
        }
        vec_executors.emplace_back(std::move(vec_executor));
    }

    if (!is_shutdown && data_part->storage.isShutdown())
    {
        retry = false;
        for (VectorIndex::SegmentId & segment_id : segment_ids)
        {
            VectorIndex::VectorSegmentExecutor::removeFromCache(segment_id.getCacheKey());
        }
    }

    if (retry)
    {
        vec_executors.clear();
        segment_ids.clear();
        if (data_part->containVectorIndex(index.name, index.column))
        {
            VectorIndex::SegmentId segment_id(part_storage->volume, data_path, data_part->name, index.name, index.column);
            segment_ids.emplace_back(std::move(segment_id));
        }

        if (segment_ids.size() == 1)
        {
            LOG_DEBUG(log, "Create vector segment executor for : {}", segment_ids[0].getFullPath());
            VectorIndex::VectorSegmentExecutorPtr vec_executor = std::make_shared<VectorIndex::VectorSegmentExecutor>(
                segment_ids[0],
                index_type,
                metric,
                dim,
                data_part->rows_count,
                index_params,
                min_bytes_to_build_vector_index,
                default_mstg_disk_mode);

            is_shutdown = data_part->storage.isShutdown();
            if (!is_shutdown)
            {
                VectorIndex::Status status = vec_executor->load(data_part->getState() == MergeTreeDataPartState::Active);
                LOG_DEBUG(log, "Vector number in index: {}", vec_executor->getRawDataSize());
                LOG_DEBUG(log, "Load vector index: {}", status.getCode());

                if (status.getCode() == ErrorCodes::INVALID_VECTOR_INDEX)
                {
                    /// inactive part reload vector index cache, behavior is prohibited
                    LOG_WARNING(log, "Query using vector index was canceled due to a concurrent inactive part reload vector index");
                    context->getQueryContext()->killCurrentQuery();
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled.");
                }
                else if (!status.fine())
                {
                    LOG_ERROR(log, "Fail to load vector index: {}", segment_ids[0].getFullPath());
                }
                else
                {
                    vec_executors.emplace_back(std::move(vec_executor));
                    brute_force = false;
                }
            }
        }
    }

    if (is_shutdown || data_part->storage.isShutdown())
    {
        if (retry && !is_shutdown && segment_ids.size() == 1)
        {
            VectorIndex::VectorSegmentExecutor::removeFromCache(segment_ids[0].getCacheKey());
        }
        LOG_WARNING(log, "Query using vector index was canceled due to a concurrent detach or drop table or database query.");
        context->getQueryContext()->killCurrentQuery();
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled.");
    }

    if (brute_force)
        vec_executors.clear();

    return vec_executors;
}

void MergeTreeVectorScanManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const Search::DenseBitmapPtr filter,
    const ColumnUInt64 * part_offset)
{
    if (vector_scan_info->is_batch)
    {
        mergeBatchVectorScanResult(pre_result, read_rows, read_ranges, vector_scan_result, filter, part_offset);
    }
    else
    {
        mergeVectorScanResult(pre_result, read_rows, read_ranges, vector_scan_result, filter, part_offset);
    }
}

void MergeTreeVectorScanManager::mergeBatchVectorScanResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    VectorScanResultPtr tmp_result,
    const Search::DenseBitmapPtr filter,
    const ColumnUInt64 * part_offset)
{
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::mergeBatchVectorScanResult()");
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

    if (filter)
    {
        /// merge label and distance result into result columns
        size_t current_column_pos = 0;
        int range_index = 0;
        size_t start_pos = read_ranges[range_index].start_row;
        size_t offset = 0;
        for (size_t i = 0; i < filter->get_size(); ++i)
        {
            if (offset >= read_ranges[range_index].row_num)
            {
                ++range_index;
                start_pos = read_ranges[range_index].start_row;
                offset = 0;
            }

            if (filter->unsafe_test(i))
            {
                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
                    /// start_pos + offset equals to the real row id
                    if (label_column->getUInt(ind) == start_pos + offset)
                    {
                        /// for each result column
                        for (size_t j = 0; j < final_result.size(); ++j)
                        {
                            Field field;
                            pre_result[j]->get(current_column_pos, field);
                            final_result[j]->insert(field);
                        }
                        final_vector_id_column->insert(vector_id_column->getUInt(ind));
                        final_distance_column->insert(distance_column->getFloat32(ind));
                    }
                }
                ++current_column_pos;
            }
            ++offset;
        }
    }
    else
    {
        if (part_offset == nullptr)
        {
            size_t start_pos = 0;
            size_t end_pos = 0;
            size_t prev_row_num = 0;

            /// auto merge_start_time = std::chrono::system_clock::now();

            /// when no filter, the prev read result should be continuous, so we just need to scan all result rows and
            /// keep results of which the row id is contained in label_column
            for (auto & read_range : read_ranges)
            {
                start_pos = read_range.start_row;
                end_pos = read_range.start_row + read_range.row_num;
                /// LOG_DEBUG(log, "start_pos: {}, end_pos: {}, prev_row_num: {}", start_pos, end_pos, prev_row_num);
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

/// TODO: remove duplicated code in
void MergeTreeVectorScanManager::mergeVectorScanResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    VectorScanResultPtr tmp_result,
    const Search::DenseBitmapPtr filter,
    const ColumnUInt64 * part_offset)
{
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::mergeVectorScanResult()");
    const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(tmp_result->result_columns[0].get());
    const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(tmp_result->result_columns[1].get());

    if (!label_column)
    {
        LOG_DEBUG(log, "Label colum is null");
    }

    /// Initialize was_result_processed
    if (tmp_result->was_result_processed.size() == 0)
        tmp_result->was_result_processed.assign(label_column->size(), false);

    auto final_distance_column = DataTypeFloat32().createColumn();

    /// create new column vector to save final results
    MutableColumns final_result;
    LOG_DEBUG(log, "Create final result");
    for (auto & col : pre_result)
    {
        final_result.emplace_back(col->cloneEmpty());
    }

    if (filter)
    {
        size_t current_column_pos = 0;
        int range_index = 0;
        size_t start_pos = read_ranges[range_index].start_row;
        size_t offset = 0;
        for (size_t i = 0; i < filter->get_size(); ++i)
        {
            if (offset >= read_ranges[range_index].row_num)
            {
                ++range_index;
                start_pos = read_ranges[range_index].start_row;
                offset = 0;
            }
            if (filter->unsafe_test(i))
            {
                /// for each vector search result, try to find if there is one with label equals to row id.
                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
                    /// Skip if this label has already processed
                    if (tmp_result->was_result_processed[ind])
                        continue;

                    if (label_column->getUInt(ind) == start_pos + offset)
                    {
                        /// LOG_DEBUG(log, "merge result: ind: {}, current_column_pos: {}, filter_id: {}", ind, current_column_pos, i + start_offset);
                        /// for each result column
                        for (size_t col = 0; col < final_result.size(); ++col)
                        {
                            Field field;
                            pre_result[col]->get(current_column_pos, field);
                            final_result[col]->insert(field);
                        }
                        final_distance_column->insert(distance_column->getFloat32(ind));

                        tmp_result->was_result_processed[ind] = true;
                    }
                }
                ++current_column_pos;
            }
            ++offset;
        }
    }
    else
    {
        LOG_DEBUG(log, "No filter statement");
        if (part_offset == nullptr)
        {
            size_t start_pos = 0;
            size_t end_pos = 0;
            size_t prev_row_num = 0;

            for (auto & read_range : read_ranges)
            {
                start_pos = read_range.start_row;
                end_pos = read_range.start_row + read_range.row_num;
                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
                    if (tmp_result->was_result_processed[ind])
                        continue;

                    const UInt64 label_value = label_column->getUInt(ind);
                    if (label_value >= start_pos && label_value < end_pos)
                    {
                        const size_t index_of_arr = label_value - start_pos + prev_row_num;
                        for (size_t i = 0; i < final_result.size(); ++i)
                        {
                            Field field;
                            pre_result[i]->get(index_of_arr, field);
                            final_result[i]->insert(field);
                        }

                        final_distance_column->insert(distance_column->getFloat32(ind));

                        tmp_result->was_result_processed[ind] = true;
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

            size_t start_pos = 0;
            size_t end_pos = 0;

            for (auto & read_range : read_ranges)
            {
                start_pos = read_range.start_row;
                end_pos = read_range.start_row + read_range.row_num;

                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
                    if (tmp_result->was_result_processed[ind])
                        continue;

                    const UInt64 label_value = label_column->getUInt(ind);

                    /// Check if label_value inside this read range
                    if (label_value < start_pos || (label_value >= end_pos))
                        continue;

                    /// read range doesn't consider LWD, hence start_row and row_num in read range cannot be used in this case.
                    int low = 0;
                    int mid;
                    if (part_offset_size - 1 > static_cast<size_t>(std::numeric_limits<int>::max()))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of part_offset_size exceeds the limit of int data type");
                    int high = static_cast<int>(part_offset_size - 1);

                    /// label_value (row id) = part_offset.
                    /// We can use binary search to quickly locate part_offset for current label.
                    while (low <= high)
                    {
                        mid = low + (high - low) / 2;

                        if (label_value == offset_raw_value[mid])
                        {
                            /// Use the index of part_offset to locate other columns in pre_result and fill final_result.
                            for (size_t i = 0; i < final_result.size(); ++i)
                            {
                                Field field;
                                pre_result[i]->get(mid, field);
                                final_result[i]->insert(field);
                            }

                            final_distance_column->insert(distance_column->getFloat32(ind));

                            tmp_result->was_result_processed[ind] = true;

                            /// break from binary search loop
                            break;
                        }
                        else if (label_value > offset_raw_value[mid])
                            low = mid + 1;
                        else
                            high = mid - 1;
                    }
                }
            }
        }
    }

    for (size_t i = 0; i < pre_result.size(); ++i)
    {
        pre_result[i] = std::move(final_result[i]);
    }
    read_rows = final_distance_column->size();

    pre_result.emplace_back(std::move(final_distance_column));
}


/// 1. read raw vector data block by block
/// 2. for each block, compute topk targets
/// 3. get the first topk targets
VectorScanResultPtr MergeTreeVectorScanManager::vectorScanWithoutIndex(
    const MergeTreeData::DataPartPtr part,
    const ReadRanges & read_ranges,
    const Search::DenseBitmapPtr filter,
    VectorIndex::VectorDatasetPtr & query_vector,
    const String & search_column,
    int dim,
    int k,
    bool is_batch,
    const Search::Metric & metric)
{
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScanWithoutIndex()");
    NamesAndTypesList cols;
    /// get search vector column info
    auto col_and_type = this->metadata->getColumns().getAllPhysical().tryGetByName(search_column);
    if (col_and_type)
    {
        cols.emplace_back(*col_and_type);
    }
    else
    {
        /// wrong column
        throw Exception(ErrorCodes::LOGICAL_ERROR, "not valid column");
    }

    /// only consider no prewhere case
    if (part->storage.hasLightweightDeletedMask())
    {
        cols.emplace_back(LightweightDeleteDescription::FILTER_COLUMN);
    }

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<VectorScanResult>();
    tmp_vector_scan_result->result_columns.resize(is_batch ? 3 : 2);

    size_t nq = query_vector->getVectorNum();
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();
    auto vector_id_column = DataTypeUInt32().createColumn();

    tmp_vector_scan_result->is_batch = is_batch;
    tmp_vector_scan_result->top_k = k;
    tmp_vector_scan_result->query_vector_num = static_cast<int>(nq);

    MergeTreeReaderSettings reader_settings = {.save_marks_in_cache = true};

    /// create part reader to read vector column
    auto reader = part->getReader(
        cols,
        this->metadata,
        MarkRanges{MarkRange(0, part->getMarksCount())},
        /* uncompressed_cache = */ nullptr,
        part->storage.getContext()->getMarkCache().get(),
        reader_settings,
        {},
        {});

    size_t current_mark = 0;
    size_t total_rows_in_part = part->rows_count;
    const auto & index_granularity = part->index_granularity;

    size_t num_rows_read = 0;

    size_t default_read_num = 0;

    bool continue_read = false;

    std::vector<float> final_distance;
    if (metric == Search::Metric::IP)
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
        size_t range_num = 0;

        /// for debugging filter
        size_t part_left_rows = 0;
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
            /// for debug
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

                /// prepare continuous data
                /// data of search column stored in one_column, commonly is vector data
                const auto & one_column = result[0];
                const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
                const IColumn & src_data = array->getData();
                const ColumnArray::Offsets & __restrict offsets = array->getOffsets();
                const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
                const PaddedPODArray<Float32> & __restrict src_vec = src_data_concrete->getData();

                if (src_vec.empty())
                    continue;

                std::vector<float> vector_raw_data;
                vector_raw_data.reserve(dim * offsets.size());

                std::vector<size_t> actual_id_in_range;
                actual_id_in_range.reserve(offsets.size());
                /// filter out the data we want to do ANN on using the filter
                size_t start_pos = filter_parsed;

                /// only for debug
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
                            /// only for debug
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
                    ASSERT(mark_left_rows == 0)
                    continue;
                }

                auto base_data = std::make_shared<VectorIndex::VectorDataset>(
                    static_cast<int32_t>(mark_left_rows),
                    static_cast<int32_t>(dim),
                    const_cast<float *>(vector_raw_data.data()));

                ASSERT(vector_raw_data.size() == mark_left_rows * dim)

                Search::DenseBitmapPtr row_exists = std::make_shared<Search::DenseBitmap>(mark_left_rows, true);

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

                /// for debug
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

            /// for debug
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

        /// for debug
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
            /// for debug
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

            const auto & one_column = result[0];
            const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
            const IColumn & src_data = array->getData();
            const ColumnArray::Offsets & offsets = array->getOffsets();
            const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
            if (!src_data_concrete)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Bad type of column {}", cols.back().name);
            const PaddedPODArray<Float32> & src_vec = src_data_concrete->getData();

            if (src_vec.empty())
            {
                num_rows_read += num_rows;
                continue;
            }

            std::vector<float> vector_raw_data(dim * offsets.size(), std::numeric_limits<float>().max());

            for (size_t row = 0; row < offsets.size(); ++row)
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

            /// for debug
            LOG_TRACE(
                log,
                "Part: {}, "
                "raw_data size: {}",
                part->name,
                vector_raw_data.size());

            int deleted_row_num = 0;
            Search::DenseBitmapPtr row_exists = std::make_shared<Search::DenseBitmap>(offsets.size(), true);

            //make sure result contain lwd row_exists column
            if (result.size() == 2 && part->storage.hasLightweightDeletedMask())
            {
                /// for debug
                LOG_TRACE(
                    log,
                    "Try to get row exists col, result size: {}",
                    result.size());
                const auto& row_exists_col = result[1];
                if (row_exists_col)
                {
                    const ColumnUInt8 * col = checkAndGetColumn<ColumnUInt8>(row_exists_col.get());
                    const auto & col_data = col->getData();
                    LOG_TRACE(
                        log,
                        "Col data size: {}",
                        col_data.size());
                    for (size_t i = 0; i < col_data.size(); i++)
                    {
                        if (!col_data[i])
                        {
                            LOG_DEBUG(log, "Unset: {}", i);
                            ++deleted_row_num;
                            row_exists->unset(i);
                        }
                    }
                }
            }

            auto base_data = std::make_shared<VectorIndex::VectorDataset>(
                static_cast<int32_t>(offsets.size()), static_cast<int32_t>(dim), const_cast<float *>(vector_raw_data.data()));

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
                LOG_TRACE(log, "Label: {}, distance: {}", final_id[label], final_distance[label]);
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


void MergeTreeVectorScanManager::searchWrapper(
    bool prewhere,
    VectorIndex::VectorDatasetPtr & query_vector,
    VectorIndex::VectorDatasetPtr & base_data,
    int k,
    int /* dim */,
    int nq,
    int num_rows_read,
    std::vector<int64_t> & final_id,
    std::vector<float> & final_distance,
    std::vector<size_t> & actual_id_in_range,
    const Search::Metric & metric,
    Search::DenseBitmapPtr & row_exists,
    int delete_id_num)
{
    std::vector<float> per_distance;
    std::vector<float> tmp_per_distance;
    if (metric == Search::Metric::IP)
    {
        per_distance = std::vector<float>(k * nq, std::numeric_limits<float>().min());
        if (delete_id_num > 0)
            tmp_per_distance = std::vector<float>((k + delete_id_num) * nq, std::numeric_limits<float>().min());
    }
    else
    {
        per_distance = std::vector<float>(k * nq, std::numeric_limits<float>().max());
        if (delete_id_num > 0)
            tmp_per_distance = std::vector<float>((k + delete_id_num) * nq, std::numeric_limits<float>().max());
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

    auto s = VectorIndex::VectorSegmentExecutor::searchWithoutIndex(
        query_vector, base_data, k + delete_id_num, distance_data, id_data, metric);
    if (!s.fine())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "brute force search failed");
    }

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
            if ((metric != Search::Metric::IP && final_distance[j] > per_distance[z])
                || (metric == Search::Metric::IP && final_distance[j] < per_distance[z]))
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
}
