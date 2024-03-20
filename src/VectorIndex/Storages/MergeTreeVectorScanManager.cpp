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

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <Storages/MergeTree/MergeTreeDataPartState.h>

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <VectorIndex/Common/BruteForceSearch.h>
#include <VectorIndex/Common/SearchThreadLimiter.h>
#include <VectorIndex/Common/VectorIndexCommon.h>
#include <VectorIndex/Storages/MergeTreeVectorScanManager.h>
#include <VectorIndex/Storages/VectorScanDescription.h>
#include <VectorIndex/Utils/VectorIndexUtils.h>

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

template <>
VectorIndex::Float32VectorDatasetPtr MergeTreeVectorScanManager::generateVectorDataset(bool is_batch, const VectorScanDescription& desc)
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

        LOG_DEBUG(log, "dim to {}", dim);

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
VectorIndex::BinaryVectorDatasetPtr MergeTreeVectorScanManager::generateVectorDataset(bool is_batch, const VectorScanDescription& desc)
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

        const ColumnString *src_data_concrete = checkAndGetColumn<ColumnString>(query_col->getData());
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

void MergeTreeVectorScanManager::executeBeforeRead(const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::executeBeforeRead");
    this->vector_scan_result = vectorScan(vector_scan_info->is_batch, data_part);
}

void MergeTreeVectorScanManager::executeAfterRead(
    const MergeTreeData::DataPartPtr & data_part,
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    bool has_prewhere,
    const VectorIndexBitmapPtr filter)
{
    if (vector_scan_info->is_batch)
    {
        if (has_prewhere)
        {
            VectorScanResultPtr tmp_result = vectorScan(true, data_part, read_ranges, filter);
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
            VectorScanResultPtr tmp_result = vectorScan(false, data_part, read_ranges, filter);
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
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const VectorIndexBitmapPtr filter)
{
    this->vector_scan_result = vectorScan(vector_scan_info->is_batch, data_part, read_ranges, filter);
}

VectorScanResultPtr MergeTreeVectorScanManager::vectorScan(
    bool is_batch,
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const VectorIndexBitmapPtr filter)
{
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScan()");
    const VectorScanDescriptions & descs = vector_scan_info->vector_scan_descs;

    const VectorScanDescription & desc = descs[0];
    const String search_column_name = desc.search_column_name;

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<VectorScanResult>();

    tmp_vector_scan_result->result_columns.resize(3);
    auto vector_id_column = DataTypeUInt32().createColumn();
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    VectorIndex::VectorDatasetVariantPtr vec_data;
    switch (desc.vector_search_type)
    {
        case Search::DataType::FloatVector:
            vec_data = generateVectorDataset<Search::DataType::FloatVector>(is_batch, desc);
            break;
        case Search::DataType::BinaryVector:
            vec_data = generateVectorDataset<Search::DataType::BinaryVector>(is_batch, desc);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported vector search type");
    }

    UInt64 dim = desc.search_column_dim;
    VectorIndexParameter search_params = VectorIndex::convertPocoJsonToMap(desc.vector_parameters);
    LOG_DEBUG(log, "Search parameters: {}", search_params.toString());

    int k = desc.topk > 0 ? desc.topk : VectorIndex::DEFAULT_TOPK;
    search_params.erase("metric_type");

    LOG_DEBUG(log, "Set k to {}, dim to {}", k, dim);

    String metric_str;
    bool enable_brute_force_for_part = bruteForceSearchEnabled(data_part);

    MergeTreeDataPartColumnIndexPtr column_index;
    if (data_part->vector_index.getColumnIndexByColumnName(search_column_name).has_value())
        column_index = data_part->vector_index.getColumnIndexByColumnName(search_column_name).value();

    std::vector<VectorIndex::IndexWithMetaHolderPtr> index_holders;
    VectorIndexMetric metric;

    if (column_index)
    {
        index_holders = column_index->getIndexHolders(data_part->getState() != MergeTreeDataPartState::Outdated);
        metric = column_index->getMetric();
    }
    else
    {
        if (desc.vector_search_type == Search::DataType::FloatVector)
            metric_str = data_part->storage.getSettings()->float_vector_search_metric_type;
        else if (desc.vector_search_type == Search::DataType::BinaryVector)
            metric_str = data_part->storage.getSettings()->binary_vector_search_metric_type;
        metric = Search::getMetricType(
            static_cast<String>(metric_str), desc.vector_search_type);
    }

    if (!index_holders.empty())
    {
        int64_t query_vector_num = 0;
        std::visit([&query_vector_num](auto &&vec_data_ptr)
                   {
                       query_vector_num = vec_data_ptr->getVectorNum();
                   }, vec_data);

        /// find index
        for (size_t i = 0; i < index_holders.size(); ++i)
        {
            auto & index_with_meta = index_holders[i]->value();
            OpenTelemetry::SpanHolder span3("MergeTreeVectorScanManager::vectorScan()::find_index::search");

            VectorIndexBitmapPtr real_filter = nullptr;
            if (filter != nullptr)
            {
                real_filter = getRealBitmap(filter, index_with_meta);
            }

            if (real_filter != nullptr && !real_filter->any())
            {
                /// don't perform vector search if the segment is completely filtered out
                continue;
            }

            LOG_DEBUG(log, "Start search: vector num: {}", query_vector_num);

            /// Although the vector index type support two stage search, the actual built index may fallback to flat.
            bool first_stage_only = false;
            if (support_two_stage_search && MergeTreeDataPartColumnIndex::supportTwoStageSearch(index_with_meta))
                first_stage_only = true;

            LOG_DEBUG(log, "first stage only = {}", first_stage_only);

            auto search_results = column_index->search(index_with_meta, vec_data, k, real_filter, search_params, first_stage_only);
            auto per_id = search_results->getResultIndices();
            auto per_distance = search_results->getResultDistances();

            /// Update k value to num_reorder in two search stage.
            if (first_stage_only)
                k = search_results->getNumCandidates();

            if (is_batch)
            {
                OpenTelemetry::SpanHolder span4("MergeTreeVectorScanManager::vectorScan()::find_index::segment_batch_generate_results");
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
            tmp_vector_scan_result->query_vector_num = static_cast<int>(query_vector_num);
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
    else if (enable_brute_force_for_part)
    {
        VectorScanResultPtr res_without_index;
        std::visit([&](auto &&vec_data_ptr)
                   {
                       res_without_index = vectorScanWithoutIndex(data_part, read_ranges, filter, vec_data_ptr, search_column_name, static_cast<int>(dim), k, is_batch, metric);
                   }, vec_data);
        return res_without_index;
    }
    else
    {
        /// No vector index available and brute force search disabled
        tmp_vector_scan_result->computed = false;
        return tmp_vector_scan_result;
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

    if (desc.vector_search_type != Search::DataType::FloatVector)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only Float32 Vector support two stage search");
    }
    const String search_column_name = desc.search_column_name;

    bool brute_force = false;

    MergeTreeDataPartColumnIndexPtr column_index;
    if (data_part->vector_index.getColumnIndexByColumnName(search_column_name).has_value())
        column_index = data_part->vector_index.getColumnIndexByColumnName(search_column_name).value();

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

    std::vector<IndexWithMetaHolderPtr> index_holders;
    if (column_index)
        index_holders = column_index->getIndexHolders(data_part->getState() != MergeTreeDataPartState::Outdated);

    brute_force = index_holders.size() == 0;
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
        VectorIndex::VectorDatasetVariantPtr vec_data = generateVectorDataset<Search::DataType::FloatVector>(false, desc);
        auto first_stage_result = Search::SearchResult::createTopKHolder(1, num_reorder);
        auto sr_indices = first_stage_result->getResultIndices();
        auto sr_distances = first_stage_result->getResultDistances();

        for (size_t i = 0; i < row_ids.size(); i++)
        {
            sr_indices[i] = row_ids[i];
            sr_distances[i] = distances[i];
        }

        OpenTelemetry::SpanHolder span2("MergeTreeVectorScanManager::executeSecondStageVectorScan()::before calling computeTopDistanceSubset");

        for (size_t i = 0; i < index_holders.size(); ++i)
        {
            auto & index_with_meta = index_holders[i]->value();
            std::shared_ptr<Search::SearchResult> real_first_stage_result = nullptr;

            {
                OpenTelemetry::SpanHolder span3("MergeTreeVectorScanManager::executeSecondStageVectorScan()::TransferToOldRowIds()");
                /// Try to transfer to old part's row ids for decouple part. And skip if no need.
                real_first_stage_result = MergeTreeDataPartColumnIndex::TransferToOldRowIds(index_with_meta, first_stage_result);
            }

            /// No rows needed from this old data part
            if (!real_first_stage_result)
                continue;

            std::shared_ptr<Search::SearchResult> search_results;
            {
                OpenTelemetry::SpanHolder span4("MergeTreeVectorScanManager::executeSecondStageVectorScan()::computeTopDistanceSubset()");
                if (MergeTreeDataPartColumnIndex::supportTwoStageSearch(index_with_meta))
                {
                    search_results = column_index->computeTopDistanceSubset(index_with_meta, vec_data, real_first_stage_result, k);
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

void MergeTreeVectorScanManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const VectorIndexBitmapPtr filter,
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
    const VectorIndexBitmapPtr filter,
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
        else
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
    const VectorIndexBitmapPtr filter,
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
template <Search::DataType T>
VectorScanResultPtr MergeTreeVectorScanManager::vectorScanWithoutIndex(
    const MergeTreeData::DataPartPtr part,
    const ReadRanges & read_ranges,
    const VectorIndexBitmapPtr filter,
    VectorIndex::VectorDatasetPtr<T> & query_vector,
    const String & search_column,
    int dim,
    int k,
    bool is_batch,
    const VectorIndexMetric & metric)
{
    OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScanWithoutIndex()");
    /// Limit the number of vector index search threads to 2 * number of physical cores
    static VectorIndex::LimiterSharedContext vector_index_context(getNumberOfPhysicalCPUCores() * 2);
    VectorIndex::SearchThreadLimiter limiter(vector_index_context, log);


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

    auto alter_conversions = part->storage.getAlterConversionsForPart(part);

    MergeTreeReaderSettings reader_settings = {.save_marks_in_cache = true};

    /// create part reader to read vector column
    auto reader = part->getReader(
        cols,
        this->metadata,
        MarkRanges{MarkRange(0, part->getMarksCount())},
        /* uncompressed_cache = */ nullptr,
        part->storage.getContext()->getMarkCache().get(),
        alter_conversions,
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
    if (metric == VectorIndexMetric::IP)
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

        for (const auto & single_range : read_ranges)
        {
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

                    base_data = std::make_shared<VectorIndex::VectorDataset<T>>(
                            static_cast<int32_t>(mark_left_rows),
                            static_cast<int32_t>(dim),
                            const_cast<float *>(vector_raw_data.data()));

                    ASSERT(vector_raw_data.size() == mark_left_rows * dim)
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
                        const ColumnFixedString *sparse_fixed_string = checkAndGetColumn<ColumnFixedString>(sparse_column->getValuesColumn());
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
                        ASSERT(mark_left_rows == 0)
                        continue;
                    }

                    base_data = std::make_shared<VectorIndex::VectorDataset<T>>(
                            static_cast<int32_t>(mark_left_rows),
                            static_cast<int32_t>(dim),
                            const_cast<uint8_t *>(vector_raw_data.data()));

                    ASSERT(vector_raw_data.size() == mark_left_rows * dim / 8)
                }

                VectorIndexBitmapPtr row_exists = std::make_shared<VectorIndexBitmap>(mark_left_rows, true);

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

                range_left_rows += mark_left_rows;
            }

            range_num ++;
            part_left_rows += range_left_rows;
        }
    }
    else
    {
        default_read_num = index_granularity.getMarkRows(current_mark);
        /// has no filter, will pass the vector data and the dense bitmap for deleted rows to search function
        while (num_rows_read < total_rows_in_part)
        {
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
                    const ColumnFixedString *sparse_fixed_string = checkAndGetColumn<ColumnFixedString>(sparse_column->getValuesColumn());
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

            int deleted_row_num = 0;
            VectorIndexBitmapPtr row_exists = std::make_shared<VectorIndexBitmap>(total_rows, true);

            //make sure result contain lwd row_exists column
            if (result.size() == 2 && part->storage.hasLightweightDeletedMask())
            {
                const auto& row_exists_col = result[1];
                if (row_exists_col)
                {
                    const ColumnUInt8 * col = checkAndGetColumn<ColumnUInt8>(row_exists_col.get());
                    const auto & col_data = col->getData();

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
void MergeTreeVectorScanManager::searchWrapper(
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
    const VectorIndexMetric & metric,
    VectorIndexBitmapPtr & row_exists,
    int delete_id_num)
{
    std::vector<float> per_distance;
    std::vector<float> tmp_per_distance;

    switch (T)
    {
        case Search::DataType::FloatVector:
        {
            if (metric == VectorIndexMetric::IP)
            {
                per_distance = std::vector<float>(k * nq, std::numeric_limits<float>().min());
                if (delete_id_num > 0)
                    tmp_per_distance = std::vector<float>((k + delete_id_num) * nq, std::numeric_limits<float>().min());
            }
            else if (metric == VectorIndexMetric::Cosine || metric == VectorIndexMetric::L2)
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
            if (metric == VectorIndexMetric::Hamming || metric == VectorIndexMetric::Jaccard)
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

    MergeTreeDataPartColumnIndex::searchWithoutIndex<T>(query_vector, base_data, k + delete_id_num, distance_data, id_data, metric);

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
            if ((metric != VectorIndexMetric::IP && final_distance[j] > per_distance[z])
                || (metric == VectorIndexMetric::IP && final_distance[j] < per_distance[z]))
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

bool MergeTreeVectorScanManager::bruteForceSearchEnabled(const MergeTreeData::DataPartPtr & data_part)
{
    /// Always enable for small part
    if (data_part->isSmallPart())
        return true;
    else
        return enable_brute_force_search;
}
}
