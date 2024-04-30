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

#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>

namespace DB
{

/// TODO: remove duplicated code in
void MergeTreeBaseSearchManager::mergeSearchResultImpl(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    CommonSearchResultPtr tmp_result,
    const Search::DenseBitmapPtr filter,
    const ColumnUInt64 * part_offset)
{
    Poco::Logger * log = &Poco::Logger::get("MergeTreeBaseSearchManager");

    OpenTelemetry::SpanHolder span("MergeTreeBaseSearchManager::mergeSearchResultImpl()");
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
}
