#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Common/FieldVisitorConvertToNumber.h>

#include <Storages/MergeTree/MergeTreeVectorScanManager.h>

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <VectorIndex/BruteForceSearch.h>
#include <VectorIndex/VectorSegmentExecutor.h>
#include <VectorIndex/Status.h>
#include <VectorIndex/VectorIndexFactory.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/MergeUtils.h>
#include <VectorIndex/VectorIndex.h>

#include <memory>

/// #define profile

namespace DB
{
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
    int dim = desc.search_column_dim;

    /// LOG_DEBUG(log, "[vectorScanImpl] column: {}", node.column->dumpStructure());
    ColumnPtr holder = query_column->convertToFullColumnIfConst();
    const ColumnArray * query_col = checkAndGetColumn<ColumnArray>(holder.get());

    if (is_batch)
    {
        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(Array(float64))");

        const IColumn & query_data = query_col->getData();

        const ColumnArray * query_vectors_col = checkAndGetColumn<ColumnArray>(&query_data);

        if (!query_vectors_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(Array(float64))");

        const IColumn & query_vectors = query_vectors_col->getData();

        const ColumnFloat64 * query_data_concrete = checkAndGetColumn<ColumnFloat64>(&query_vectors);

        if (!query_data_concrete)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(Array(float64))");

        const PaddedPODArray<Float64> & query_vec = query_data_concrete->getData();
        size_t elem_num = query_vec.size();
        auto & offsets = query_vectors_col->getOffsets();

        int query_vector_num = offsets.size();

        LOG_DEBUG(log, "[batchVectorScan] query_num: {}", query_vector_num);

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

        std::vector<float> query_new_data(elem_num);
        for (size_t i = 0; i < elem_num; ++i)
        {
            query_new_data[i] = static_cast<float>(query_vec[i]);
        }

        // default value
        VectorIndex::Parameters vec_parameters = VectorIndex::convertPocoJsonToMap(desc.vector_parameters);
        int k = 50;
        if (vec_parameters.contains("topK"))
        {
            k = VectorIndex::StoI(vec_parameters.at("topK"));
            vec_parameters.erase("topK");
        }

        LOG_DEBUG(log, "[batchVectorScan] set k to {}, dim to {}", k, dim);
        return std::make_shared<VectorIndex::VectorDataset>(
            query_vector_num, static_cast<int32_t>(dim), const_cast<float *>(query_new_data.data()));
    }
    else
    {
        if (!query_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(float64)");

        const IColumn & query_data = query_col->getData();
        // const ColumnArray::Offsets & offsets = src_col->getOffsets();
        const ColumnFloat64 * query_data_concrete = checkAndGetColumn<ColumnFloat64>(&query_data);

        if (!query_data_concrete)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query column type, expect Array(float64)");

        const PaddedPODArray<Float64> & query_vec = query_data_concrete->getData();

        size_t dim_of_query = query_vec.size();

        if (dim_of_query != dim)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Dimension is not equal: query: {} vs search column: {}",
                std::to_string(dim_of_query),
                std::to_string(dim));

        /// TODO: effectively transform float64 array to float32 array
        std::vector<float> query_new_data(dim);

        for (size_t i = 0; i < dim; ++i)
        {
            query_new_data[i] = static_cast<float>(query_vec[i]);
        }

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
    const FilterWithCachedCount & filter)
{
    LOG_DEBUG(log, "executeAfterRead");
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


VectorScanResultPtr MergeTreeVectorScanManager::vectorScan(
    bool is_batch,
    const String & data_path,
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const FilterWithCachedCount & filter)
{
    VectorIndexDescription index;
    bool find_index = false;
    const VectorIndicesDescription & vector_indices = metadata->vec_indices;
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
    VectorIndex::Parameters vec_parameters = VectorIndex::convertPocoJsonToMap(desc.vector_parameters);

    LOG_DEBUG(log, "[vectorScan] data_path = {}", data_path);
    LOG_DEBUG(log, "[vectorScan] data_part name = {}", data_part->name);

    int k = 50;
    if (vec_parameters.contains("topK"))
    {
        k = VectorIndex::StoI(vec_parameters.at("topK"));
        vec_parameters.erase("topK");
    }

    String metrics_str = data_part->storage.getSettings()->vector_search_metric_type;
    LOG_DEBUG(log, "[vectorscan] metric: {}", metrics_str);

    std::vector<VectorIndex::SegmentId> segment_ids;
    for (auto & v_index : vector_indices)
    {
        if (v_index.column == search_column_name)
        {
            if (v_index.parameters && v_index.parameters->has("metric_type"))
            {
                metrics_str = v_index.parameters->getValue<String>("metric_type");
            }
            
            LOG_DEBUG(log, "[vectorscan] metric: {}", metrics_str);
            if (data_part->containVectorIndex(v_index.name, v_index.column))
            {
                find_index = true;
                index = v_index;

                /// For decouple part, background index build will mark the data part's metadata before put it in cache.
                /// Hence use the old segments first to avoid load vector index.
                if (data_part->containRowIdsMaps())
                {
                    segment_ids = VectorIndex::getAllSegmentIds(data_path, data_part, v_index.name, v_index.column);
                    LOG_DEBUG(log, "[vectorScan] index found for decouple part, use old parts' index first when both exist in metadata.");
                }
                else
                {
                    VectorIndex::SegmentId segment_id(data_path, data_part->name, data_part->name, index.name, index.column, 0);
                    segment_ids.emplace_back(std::move(segment_id));

                    LOG_DEBUG(log, "[vectorScan] index found, because current data part contains it");
                }

                break;
            }
            else
            {
                segment_ids = VectorIndex::getAllSegmentIds(data_path, data_part, v_index.name, v_index.column);
                if (segment_ids.size() > 1)
                {
                    find_index = true;
                    index = v_index;
                    LOG_DEBUG(log, "[vectorScan] index found, because index segment_ids is not empty");
                    String cache_key = segment_ids[0].getCacheKey().toString();
                    LOG_DEBUG(log, "[vectorScan] the cache key = {}", cache_key);
                    break;
                }
            }
        }
    }

    VectorIndex::Metrics metrics = VectorIndex::VectorIndexFactory::createIndexMetrics(metrics_str);

    if (find_index)
    {
        LOG_DEBUG(log, "[vectorScan] find index, segment_ids size: {}", segment_ids.size());
        DB::OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScan::find_index");
        span.addAttribute("vectorScan.segment_ids", segment_ids.size());

        std::vector<uint64_t> selected_row_ids;
        if (filter.present())
        {
            auto & filter_data = filter.getData();
            int range_index = 0;
            size_t start_pos = read_ranges[range_index].start_row;
            size_t offset = 0;
            size_t filter_data_size = 0;
            for (size_t i = 0; i < filter_data.size(); ++i)
            {
                /// to another read range
                if (offset >= read_ranges[range_index].row_num)
                {
                    ++range_index;
                    start_pos = read_ranges[range_index].start_row;
                    offset = 0;
                }
                if (filter_data[i])
                {
                    ++filter_data_size;
                    /// LOG_DEBUG(log, "set filter: i: {}, start_pos: {}, offset: {}", i, start_pos, offset);
                    selected_row_ids.emplace_back(start_pos + offset);
                }
                ++offset;
            }
            LOG_DEBUG(log, "[vectorScan] filter size: {}, read_range size: {}", filter_data_size, read_ranges.size());
            span.addAttribute("vectorScan.filter_sizes", filter_data_size);
            span.addAttribute("vectorScan.read_ranges", read_ranges.size());
        }
        else if (!read_ranges.empty()) /// having prewhere, but this read round does not generate a filter
        {
            size_t start_pos = read_ranges[0].start_row;
            size_t read_row_num = read_ranges[0].row_num;
            for (size_t i = start_pos; i < start_pos + read_row_num; ++i)
            {
                selected_row_ids.emplace_back(i);
            }
        }

        std::vector<VectorIndex::VectorSegmentExecutorPtr> vec_executors;
        bool retry = false;
        bool brute_force = false;

        for (VectorIndex::SegmentId & segment_id : segment_ids)
        {
            LOG_DEBUG(log, "[vectorScan] create vector segment executor for : {}", segment_id.getFullPath());
            VectorIndex::VectorSegmentExecutorPtr vec_executor = std::make_shared<VectorIndex::VectorSegmentExecutor>(
                VectorIndex::VectorIndexFactory::createIndexType(index.type),
                segment_id,
                vec_parameters,
                dim);
            VectorIndex::Status status = vec_executor->load();
            LOG_DEBUG(log, "[vectorScan] vector number in index: {}", vec_executor->getRawDataSize());
            LOG_DEBUG(log, "[vectorScan] load vector index: {}", status.getCode());

            if (!status.fine())
            {
                /// case of merged vector indices had been removed, we need to use new vector index files
                LOG_INFO(log, "[vectorScan] fail to load vector index: {}", segment_id.getFullPath());
                retry = true;
                brute_force = true;
                break;
            }
            vec_executors.emplace_back(vec_executor);
        }

        if (retry)
        {
            vec_executors.clear();
            segment_ids.clear();
            if (data_part->containVectorIndex(index.name, index.column))
            {
                VectorIndex::SegmentId segment_id(data_path, data_part->name, data_part->name, index.name, index.column, 0);
                segment_ids.emplace_back(std::move(segment_id));
            }

            if (segment_ids.size() == 1)
            {
                LOG_DEBUG(log, "[vectorScan] create vector segment executor for : {}", segment_ids[0].getFullPath());
                VectorIndex::VectorSegmentExecutorPtr vec_executor = std::make_shared<VectorIndex::VectorSegmentExecutor>(
                    VectorIndex::VectorIndexFactory::createIndexType(index.type),
                    segment_ids[0],
                    vec_parameters,
                    dim);
                VectorIndex::Status status = vec_executor->load();
                LOG_DEBUG(log, "[vectorScan] vector number in index: {}", vec_executor->getRawDataSize());
                LOG_DEBUG(log, "[vectorScan] load vector index: {}", status.getCode());

                if (!status.fine())
                {
                    LOG_INFO(log, "[vectorScan] fail to load vector index: {}", segment_ids[0].getFullPath());
                }
                else
                {
                    vec_executors.emplace_back(vec_executor);
                    brute_force = false;
                }
            }
        }

        if (brute_force)
            return vectorScanWithoutIndex(data_part, read_ranges, filter, vec_data, search_column_name, dim, k, is_batch, metrics);

        for (VectorIndex::VectorSegmentExecutorPtr & vec_executor : vec_executors)
        {
            OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScan::build_bitmap_search_segment");
            // remove deleted vector
            /// TODO: to be optimized, not support for decouple case right now
            /// data_part->onLightweightDelete();
            int64_t base_vector_size = vec_executor->getRawDataSize();

            VectorIndex::GeneralBitMapPtr bits;

            /// have no filter
            if (selected_row_ids.empty() && read_ranges.empty())
            {
                bits = std::make_shared<VectorIndex::GeneralBitMap>(base_vector_size);
                memset(bits->bitmap, 255, (base_vector_size / 8) + 1);
            }
            else 
            {
                /// handle filter case
                bits = vec_executor->getRealBitMap(selected_row_ids);
            }

            LOG_DEBUG(log, "[vectorScan] start search: vector num: {}", vec_data->getVectorNum());

            std::vector<float> per_distance(k * vec_data->getVectorNum(), 0.0);
            std::vector<int64_t> per_id(k * vec_data->getVectorNum(), -1);
            LOG_DEBUG(log, "[vectorScan] per_id size: {}, per_distance size: {}", per_id.size(), per_distance.size());
            float * distance_data = per_distance.data();
            int64_t * id_data = per_id.data();

            auto search_status = vec_executor->search(vec_data, k, distance_data, id_data, bits, vec_parameters);
            if (search_status.getCode() == 10)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong dimension parameter");
            }
            else if (search_status.getCode() != 0)
            {
                LOG_WARNING(log, "[batchVectorScan] fail to search with vector index. code {}", search_status.getCode());
                /// TODO: default vector search without vector index
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "fail to search with vector index. code: {} message: {}",
                    std::to_string(search_status.getCode()),
                    search_status.getMessage());
            }

            LOG_DEBUG(log, "[vectorScan] after search");

            if (is_batch)
            {
                for (size_t label = 0; label < k * vec_data->getVectorNum(); ++label)
                {
                    UInt32 vector_id = label / k;
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
                for (size_t label = 0; label < k; ++label)
                {
                    if (per_id[label] > -1)
                    {
                        LOG_DEBUG(log, "[vectorScan] label: {}, distance: {}", per_id[label], per_distance[label]);
                        label_column->insert(per_id[label]);
                        distance_column->insert(per_distance[label]);
                    }
                }
            }
        }

        if (is_batch)
        {
            tmp_vector_scan_result->query_vector_num = vec_data->getVectorNum();
            tmp_vector_scan_result->result_columns[1] = std::move(vector_id_column);
            tmp_vector_scan_result->result_columns[2] = std::move(distance_column);
        }
        else
        {
            tmp_vector_scan_result->query_vector_num = 1;
            tmp_vector_scan_result->result_columns[1] = std::move(distance_column);
        }

        tmp_vector_scan_result->is_batch = is_batch;
        tmp_vector_scan_result->top_k = k;
        tmp_vector_scan_result->computed = true;
        tmp_vector_scan_result->result_columns[0] = std::move(label_column);

        LOG_DEBUG(log, "[vectorScan] after generate results");

        return tmp_vector_scan_result;
    }
    else
    {
        return vectorScanWithoutIndex(data_part, read_ranges, filter, vec_data, search_column_name, dim, k, is_batch, metrics);
    }
}

void MergeTreeVectorScanManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const FilterWithCachedCount & filter,
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
    VectorScanResultPtr vector_scan_result,
    const FilterWithCachedCount & filter,
    const ColumnUInt64 * part_offset)
{
    const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(vector_scan_result->result_columns[0].get());
    const ColumnUInt32 * vector_id_column = checkAndGetColumn<ColumnUInt32>(vector_scan_result->result_columns[1].get());
    const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(vector_scan_result->result_columns[2].get());

    auto final_vector_id_column = DataTypeUInt32().createColumn();
    auto final_distance_column = DataTypeFloat32().createColumn();

    /// create new column vector to save final results
    MutableColumns final_result;
    for (auto & col : pre_result)
    {
        final_result.emplace_back(col->cloneEmpty());
    }

    if (filter.present())
    {
        /// merge label and distance result into result columns
        auto & filter_data = filter.getData();
        size_t current_column_pos = 0;
        int range_index = 0;
        size_t start_pos = read_ranges[range_index].start_row;
        size_t offset = 0;
        for (size_t i = 0; i < filter_data.size(); ++i)
        {
            if (offset >= read_ranges[range_index].row_num)
            {
                ++range_index;
                start_pos = read_ranges[range_index].start_row;
                offset = 0;
            }

            if (filter_data[i])
            {
                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
                    /// start_pos + offset equals to the real row id
                    if (label_column->getUInt(ind) == start_pos + offset)
                    {
                        /// LOG_DEBUG(log, "merge result: ind: {}, current_column_pos: {}, filter_id: {}", ind, current_column_pos, i + start_offset);
                        /// for each result column
                        for (size_t i = 0; i < final_result.size(); ++i)
                        {
                            Field field;
                            pre_result[i]->get(current_column_pos, field);
                            final_result[i]->insert(field);
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
    VectorScanResultPtr vector_scan_result,
    const FilterWithCachedCount & filter,
    const ColumnUInt64 * part_offset)
{
    const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(vector_scan_result->result_columns[0].get());
    const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(vector_scan_result->result_columns[1].get());

    if (!label_column)
    {
        LOG_DEBUG(log, "[mergeVectorScanResult] label colum is null");
    }

    const size_t vector_scan_result_size = label_column->size();
    LOG_DEBUG(log, "[mergeVectorScanResult] label colum size: {}, distance column size: {}", label_column->size(), distance_column->size());
    const ColumnUInt32::Container & label_column_ctr = label_column->getData();
    const ColumnFloat32::Container & distance_column_ctr = distance_column->getData();
    for (size_t i = 0; i < 5 && i < vector_scan_result_size; ++i)
    {
        LOG_DEBUG(log, "[mergeVectorScanResult] label[{}] = {}, distance[{}] = {}", i, label_column_ctr[i], i, distance_column_ctr[i]);
    }

    auto final_distance_column = DataTypeFloat32().createColumn();

    /// create new column vector to save final results
    MutableColumns final_result;
    LOG_DEBUG(log, "[mergeVectorScanResult] create final result");
    for (auto & col : pre_result)
    {
        final_result.emplace_back(col->cloneEmpty());
    }

    if (filter.present())
    {
        LOG_DEBUG(log, "[mergeVectorScanResult] filter data size: {}, read_ranges size: {}", filter.getData().size(), read_ranges.size());
        auto & filter_data = filter.getData();
        size_t current_column_pos = 0;
        int range_index = 0;
        size_t start_pos = read_ranges[range_index].start_row;
        size_t offset = 0;
        for (size_t i = 0; i < filter_data.size(); ++i)
        {
            if (offset >= read_ranges[range_index].row_num)
            {
                ++range_index;
                start_pos = read_ranges[range_index].start_row;
                offset = 0;
            }
            if (filter_data[i])
            {
                /// LOG_DEBUG(log, "range_index: {}, start_pos: {}, offset: {}, i: {}, filter_data row id: {}", range_index, start_pos, offset, i, start_pos + offset);
                /// for each vector search result, try to find if there is one with label equals to row id.
                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
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
                    }
                }
                ++current_column_pos;
            }
            ++offset;
        }
    }
    else
    {
        LOG_DEBUG(log, "[mergeVectorScanResult] no filter statement");
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
                    }
                }
                prev_row_num += read_range.row_num;
            }
        }
        else
        {
            for (auto & read_range : read_ranges)
            {
                const size_t start_pos = read_range.start_row;
                const size_t end_pos = read_range.start_row + read_range.row_num;
                for (size_t ind = 0; ind < label_column->size(); ++ind)
                {
                    const UInt64 label_value = label_column->getUInt(ind);
                    if (label_value >= start_pos && label_value < end_pos)
                    {
                        const ColumnUInt64::Container & offset_raw_value = part_offset->getData();

                        /// When lightweight delete applied, the rowid in the label column cannot be used as index of pre_result.
                        /// Match the rowid in the value of label col and the value of part_offset to find the correct index.
                        /// TODO: the value in part_offset is sorted, use binary search?
                        for (size_t j = 0; j < part_offset->size(); ++j)
                        {
                            if (offset_raw_value[j] == label_value)
                            {
                                /// Use the index of part_offset to locate other columns in pre_result and fill final_result.
                                for (size_t i = 0; i < final_result.size(); ++i)
                                {
                                    Field field;
                                    pre_result[i]->get(j, field);
                                    final_result[i]->insert(field);
                                }

                                final_distance_column->insert(distance_column->getFloat32(ind));

                                break;
                            }
                        }
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

    LOG_DEBUG(log, "[mergeVectorScanResult] distance column size: {}, merge result size: {}", label_column->size(), read_rows);
}


/// 1. read raw vector data block by block
/// 2. for each block, compute topk targets
/// 3. get the first topk targets
VectorScanResultPtr MergeTreeVectorScanManager::vectorScanWithoutIndex(
    const MergeTreeData::DataPartPtr part,
    const ReadRanges & read_ranges,
    const FilterWithCachedCount & filter,
    VectorIndex::VectorDatasetPtr & query_vector,
    const String & search_column,
    int dim,
    int k,
    bool is_batch,
    const VectorIndex::Metrics& metrics)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeVectorScanManager::vectorScanWithoutIndex");

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
    if (part->storage.hasLightweightDeletedMask() && !filter.present())
    {
        cols.emplace_back(LightweightDeleteDescription::FILTER_COLUMN);
    }
    
    VectorIndex::GeneralBitMapPtr row_exists = std::make_shared<VectorIndex::GeneralBitMap>(part->rows_count);
    memset(row_exists->bitmap, 255, (part->rows_count / 8) + 1);

    VectorScanResultPtr tmp_vector_scan_result = std::make_shared<VectorScanResult>();
    tmp_vector_scan_result->result_columns.resize(is_batch ? 3 : 2);

    size_t nq = query_vector->getVectorNum();
    auto distance_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();
    auto vector_id_column = DataTypeUInt32().createColumn();

    tmp_vector_scan_result->is_batch = is_batch;
    tmp_vector_scan_result->top_k = k;
    tmp_vector_scan_result->query_vector_num = nq;

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
    size_t total_mask = part->getMarksCount();
    size_t total_rows_to_read = part->rows_count;
    const auto & index_granularity = part->index_granularity;

    /// compute how many rows to read in one round
    size_t max_search_block_size_bytes = part->storage.getContext()->getSettingsRef().preferred_block_size_bytes;

    size_t num_rows_read = 0;

    size_t default_read_num = std::max(index_granularity.getMarkRows(current_mark), max_search_block_size_bytes / 4 / dim);

    LOG_DEBUG(log, "default_read_num: {}, mark row: {}", default_read_num, index_granularity.getMarkRows(current_mark));
    bool continue_read = false;

    std::vector<float> final_distance;
    if (metrics == VectorIndex::Metrics::IP)
    {
        final_distance = std::vector<float>(k * nq, std::numeric_limits<float>().min());
    }
    else
    {
        final_distance = std::vector<float>(k * nq, std::numeric_limits<float>().max());
    }

    std::vector<int64_t> final_id(k * nq, -1);

    if (filter.present())
    {
        LOG_TRACE(log, "with filter");
        /// for debug
        for (int i = 0; i < read_ranges.size(); i++)
        {
            LOG_TRACE(
                log,
                "read_range row_num: {}, start_mark: {}, end_mark: {}, start_row: {}, ",
                read_ranges[i].row_num,
                read_ranges[i].start_mark,
                read_ranges[i].end_mark,
                read_ranges[i].start_row);
        }
        LOG_TRACE(log, "filter size: {}", filter.getData().size());

        size_t filter_parsed = 0;
        for (const auto & single_range : read_ranges)
        {
            Columns result;
            result.resize(cols.size());
            size_t num_rows = reader->readRows(single_range.start_mark, 0, false, single_range.row_num, result);
            if (num_rows == 0)
            {
                LOG_WARNING(log, "[vectorScanWithoutIndex] part: {}, no data read for column {}", part->name, cols.back().name);
                break;
            }
            else if (num_rows != single_range.row_num)
            {
                LOG_WARNING(
                    log,
                    "[vectorScanWithoutIndex] part: {}, read row num doesn't match range row_num: {} - {}",
                    part->name,
                    num_rows,
                    single_range.row_num);
                throw Exception(ErrorCodes::INCORRECT_DATA, "read row num doesn't match range row_num");
            }

            ///prepare continuous data
            const auto & one_column = result.back();
            const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
            const IColumn & src_data = array->getData();
            const ColumnArray::Offsets & __restrict offsets = array->getOffsets();
            const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
            const PaddedPODArray<Float32> & __restrict src_vec = src_data_concrete->getData();
            // size_t size = offsets.size();
            if (src_vec.empty())
            {
                num_rows_read += num_rows;
                continue;
            }

            size_t i = 0;
            size_t start_row = 0;
            /// skip empty arrays
            while (i < offsets.size() && offsets[i] == start_row)
            {
                ++i;
            }

            //std::vector<float> vector_raw_data(dim * offsets.size(), std::numeric_limits<float>().max());
            std::vector<float> vector_raw_data;
            vector_raw_data.reserve(single_range.row_num);
            std::vector<size_t> actual_id_in_range;
            actual_id_in_range.reserve(single_range.row_num);

            /// filter out the data we want to do ANN on using the filter
            const auto & filter_data = filter.getData();
            size_t start_pos = filter_parsed;
            size_t row_in_range = 0;

            /// this outer for loop's i is the position of data relative to the filter
            /// imagine filter as a array of array:[0,1,0,0,0,1,...][0,1,0...][...]
            /// where actually all these arrays are concatenated into a single array and
            /// each array represents all the rows in a single range
            for (size_t i = start_pos; i < start_pos + single_range.row_num; ++i)
            {
                if (filter_data[i])
                {
                    size_t vec_start_offset = row_in_range != 0 ? offsets[row_in_range - 1] : 0;
                    size_t vec_end_offset = offsets[row_in_range];
                    if (vec_start_offset != vec_end_offset)
                    {
                        for (size_t offset = vec_start_offset; offset < vec_end_offset; ++offset)
                        {
                            //vector_raw_data[row_in_range * dim + offset - vec_start_offset] = src_vec[offset];
                            vector_raw_data.emplace_back(src_vec[offset]);
                        }
                        actual_id_in_range.emplace_back(row_in_range);
                    }
                }
                row_in_range++;
            }
            filter_parsed += single_range.row_num;
            num_rows_read += num_rows;

            if (vector_raw_data.empty())
            {
                LOG_DEBUG(log, "range:{} - {} has data but all data are empty array", single_range.start_mark, single_range.end_mark);
                continue;
            }

            LOG_DEBUG(log, "part:{}, raw_data size: {}", part->name, vector_raw_data.size());

            auto base_data = std::make_shared<VectorIndex::VectorDataset>(
                static_cast<int32_t>(actual_id_in_range.size()), static_cast<int32_t>(dim), const_cast<float *>(vector_raw_data.data()));

            searchWrapper(true, query_vector, base_data, k, dim, nq, single_range.start_row, final_id, final_distance, actual_id_in_range, metrics, row_exists, 0);

            LOG_DEBUG(
                log,
                "part_name: {}, num_rows: {}, vector index name: {}, path: {}",
                part->name,
                num_rows,
                "brute force",
                part->getDataPartStorage().getFullPath());
        }
        LOG_DEBUG(log, "part:{}, num_rows_read: {}, filter read:{}", part->name, num_rows_read, filter_parsed);
    }
    else
    {
        while (num_rows_read < total_rows_to_read)
        {
            size_t remaining_size = total_rows_to_read - num_rows_read;
            size_t max_read_row = std::min(remaining_size, default_read_num);
            Columns result;
            result.resize(cols.size());
            size_t num_rows = reader->readRows(current_mark, 0, continue_read, max_read_row, result);

            continue_read = true;

            for (size_t mask = 0; mask < total_mask - 1; ++mask)
            {
                if (index_granularity.getMarkStartingRow(mask) >= num_rows_read
                    && index_granularity.getMarkStartingRow(mask + 1) < num_rows_read)
                {
                    current_mark = mask;
                }
            }

            LOG_DEBUG(log, "[vectorScanWithoutIndex] part: {}, read num_rows: {}, col size: {}", part->name, num_rows, cols.size());

            if (num_rows == 0)
            {
                LOG_WARNING(log, "[vectorScanWithoutIndex] part: {}, no data read for column {}", part->name, cols.back().name);
                break;
            }

            const auto & one_column = result[0];
            const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
            const IColumn & src_data = array->getData();
            const ColumnArray::Offsets & offsets = array->getOffsets();
            const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
            const PaddedPODArray<Float32> & src_vec = src_data_concrete->getData();
            // size_t size = offsets.size();
            if (src_vec.empty())
            {
                num_rows_read += num_rows;
                continue;
            }

            size_t i = 0;
            size_t start_row = 0;
            /// skip empty arrays
            while (i < offsets.size() && offsets[i] == start_row)
            {
                ++i;
            }

            std::vector<float> vector_raw_data(dim * offsets.size(), std::numeric_limits<float>().max());

            for (size_t row = 0; row < offsets.size(); ++row)
            {
                size_t vec_start_offset = row != 0 ? offsets[row - 1] : 0;
                size_t vec_end_offset = offsets[row];
                if (vec_start_offset != vec_end_offset)
                {
                    for (size_t offset = vec_start_offset; offset < vec_end_offset; ++offset)
                    {
                        vector_raw_data[row * dim + offset - vec_start_offset] = src_vec[offset];
                    }
                }
            }

            LOG_DEBUG(log, "part: {}, raw_data size: {}", part->name, vector_raw_data.size());

            int deleted_row_num = 0;
            if (part->storage.hasLightweightDeletedMask())
            {
                LOG_DEBUG(log, "[vectorScanWithoutIndex] try to get row exists col, result size: {}", result.size());
                const auto& row_exists_col = result[1];
                if (row_exists_col)
                {
                    const ColumnUInt8 * col = checkAndGetColumn<ColumnUInt8>(row_exists_col.get());
                    const auto & col_data = col->getData();
                    LOG_DEBUG(log, "[vectorScanWithoutIndex] col data size: {}", col_data.size());
                    for (int i = 0; i < col_data.size(); i++)
                    {
                        if (!col_data[i])
                        {
                            LOG_DEBUG(log, "[vectorScanWithoutIndex] unset: {}", num_rows_read + i);
                            ++deleted_row_num;
                            row_exists->unset(num_rows_read + i);
                        }
                    }
                }
            }

            auto base_data = std::make_shared<VectorIndex::VectorDataset>(
                static_cast<int32_t>(offsets.size()), static_cast<int32_t>(dim), const_cast<float *>(vector_raw_data.data()));

            std::vector<size_t> place_holder;
            searchWrapper(false, query_vector, base_data, k, dim, nq, num_rows_read, final_id, final_distance, place_holder, metrics, row_exists, deleted_row_num);

            
            num_rows_read += num_rows;

            LOG_DEBUG(
                log,
                "part_name: {}, num_rows: {}, vector index name: {}, path: {}",
                part->name,
                num_rows,
                "brute force",
                part->getDataPartStorage().getFullPath());
        }
    }

    LOG_DEBUG(log, "part_name: {}, total num rows read: {}", part->name, num_rows_read);

    /// batch search case
    if (is_batch)
    {
        for (size_t label = 0; label < k * nq; ++label)
        {
            UInt32 vector_id = label / k;
            if (final_id[label] > -1 && row_exists->test(final_id[label]))
            {
                /// LOG_DEBUG(log, "[vectorScan] label: {}, distance: {}", final_id[label], final_distance[label]);
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
            if (final_id[label] > -1 && row_exists->test(final_id[label]))
            {
                /// LOG_DEBUG(log, "[vectorScan] label: {}, distance: {}", final_id[label], final_distance[label]);
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
    int dim,
    int nq,
    int num_rows_read,
    std::vector<int64_t> & final_id,
    std::vector<float> & final_distance,
    std::vector<size_t> & actual_id_in_range,
    const VectorIndex::Metrics& metrics,
    VectorIndex::GeneralBitMapPtr& row_exists,
    int delete_id_num)
{
    std::vector<float> per_distance;
    std::vector<float> tmp_per_distance;
    if (metrics == VectorIndex::Metrics::IP)
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

    auto s = VectorIndex::VectorSegmentExecutor::searchWithoutIndex(query_vector, base_data, k + delete_id_num, distance_data, id_data, metrics);
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
                if (tmp_id < 0)
                {
                    LOG_ERROR(log, "tmp_id: {}, num_rows_read: {}", tmp_id, num_rows_read);
                }
                else if (tmp_id >= 0 && row_exists->test(tmp_id + num_rows_read))
                {
                    per_id[i * k + curr_pos] = tmp_per_id[i * (k + delete_id_num) + tmp_curr_pos];
                    per_distance[i * k + curr_pos] = tmp_per_distance[i * (k + delete_id_num) + tmp_curr_pos];
                    ++curr_pos;
                }
                ++tmp_curr_pos;
            }
        }
    }

    for (int i = 0; i < k * nq; i++)
    {
        LOG_TRACE(log, "per_id: {}, per_distance:{}", id_data[i], distance_data[i]);
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
            if (per_id[i] > -1 && per_id[i] < actual_id_in_range.size())
                per_id[i] = actual_id_in_range[per_id[i]];
        }
    }

    for (size_t q = 0; q < nq; q++)
    {
        size_t current_result_offset = q * k;

        size_t j = current_result_offset;
        size_t z = current_result_offset;
        for (size_t i = 0; i < k; i++)
        {
            if ((metrics != VectorIndex::Metrics::IP && final_distance[j] > per_distance[z]) 
                || (metrics == VectorIndex::Metrics::IP && final_distance[j] < per_distance[z]))
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
