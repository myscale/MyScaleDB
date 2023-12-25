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

#include <VectorIndex/Processors/VectorScanRecomputeTransform.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Interpreters/OpenTelemetrySpanLog.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VectorScanRecomputeTransform::VectorScanRecomputeTransform(
        const Block & input_header_,
        const Block & output_header_,
        MergeTreeVectorScanManagerPtr vector_scan_manager_,
        const MergeTreeData & data_
        )
    : IProcessor({input_header_}, {output_header_})
    , input(inputs.front())
    , output(outputs.front())
    , input_header(input_header_)
    , vector_scan_manager(std::move(vector_scan_manager_))
    , data(data_)
{
    if (!output_header_.has("_part"))
        remove_columns_pos.emplace(input_header.getPositionByName("_part"));
    if (!output_header_.has("_part_offset"))
        remove_columns_pos.emplace(input_header.getPositionByName("_part_offset"));

    if (remove_columns_pos.size() == 0)
        same_header = true;

    distance_column_pos = input_header.getPositionByName("distance_func");
}

IProcessor::Status VectorScanRecomputeTransform::prepare()
{
    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.pushData(std::move(output_data));
        has_output = false;

        return Status::PortFull;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_data = input.pullData(true);

        /// If we got an excption from input, just return it and mark that we're finished.
        if (input_data.exception)
        {
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();

            /// Skip transform in case of exception.
            if (same_header)
                output_data = std::move(input_data);
            else
            {
                /// Remove not needed columns from input
                const auto & current_columns = input_data.chunk.getColumns();
                for (size_t i = 0; i < current_columns.size(); i++)
                {
                    if (remove_columns_pos.find(i) != remove_columns_pos.end())
                        continue;

                    output_data.chunk.addColumn(std::move(current_columns[i]));
                }

                output_data.exception = input_data.exception;
            }

            output.pushData(std::move(output_data));
            output.finish();

            return Status::PortFull;
        }
    }

    has_input = true;
    processed_parts_name.clear();

    /// Now we have new input and can try to generate more result in work().
    return Status::Ready;
}

void VectorScanRecomputeTransform::work()
{
    // Exceptions should be skipped in prepare().
    assert(!input_data.exception);

    try
    {
        transform(input_data.chunk, output_data.chunk);
        has_input = false;
    }
    catch (Exception &)
    {
        output_data.exception = std::current_exception();
        has_output = true;
        has_input = false;
        return;
    }

    if (output_data.chunk)
       has_output = true;
}

void VectorScanRecomputeTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    DB::OpenTelemetry::SpanHolder span("VectorScanRecomputeTransform::transform()");

    /// Send rows to corresponding data parts and get accurate distance value.
    const auto & current_columns = input_chunk.getColumns();
    UInt64 num_rows = input_chunk.getNumRows();

    auto part_column = current_columns[input_header.getPositionByName("_part")]->convertToFullColumnIfLowCardinality();
    const ColumnString * part_col = typeid_cast<const ColumnString *>(part_column.get());

    if (!part_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data type of _part virtual column should be String");

    const auto & row_id_column = current_columns[input_header.getPositionByName("_part_offset")];
    const ColumnUInt64 * row_id_col = typeid_cast<const ColumnUInt64 *>(row_id_column.get());

    if (!row_id_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data type of _part_offset virtual column should be UInt64");

    const auto & distance_column = current_columns[input_header.getPositionByName("distance_func")];
    const ColumnFloat32 * dist_col = typeid_cast<const ColumnFloat32 *>(distance_column.get());

    if (!dist_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data type of distance_func column should be Float32");

    /// Save output result columns with final top-k rows and accurate distance value
    MutableColumns result_columns = output.getHeader().cloneEmptyColumns();
    size_t num_result_rows = 0;

    UInt64 next_start_pos = 0;
    bool first = true;
    String current_part_name;
    std::vector<UInt64> row_ids;  /// row id (_part_offset) in data part
    std::vector<Float32> distances;
    std::vector<size_t> pos_indexes;   // position index in the input block

    while (next_start_pos < num_rows)
    {
        row_ids.clear();
        distances.clear();
        pos_indexes.clear();
        first = true;
        current_part_name.clear();

        for (UInt64 i = next_start_pos; i < num_rows; i++)
        {
            String part_name = part_col->getDataAt(i).toString();

            if (processed_parts_name.find(part_name) != processed_parts_name.end())
                continue;

            /// Saved current part name to find all the same rows from this part
            if (current_part_name.empty())
                current_part_name = part_name;
            else if (part_name != current_part_name)
            {
                if (first)
                {
                    first = false;
                    next_start_pos = i;
                }
                continue;
            }

            /// Get _part_offset and distance_func
            pos_indexes.emplace_back(i);
            row_ids.emplace_back(row_id_col->get64(i));
            distances.emplace_back(dist_col->getFloat32(i));
        }

        /// No more data part needs to be processed.
        if (current_part_name.empty())
            break;

        processed_parts_name.emplace(current_part_name);

        const MergeTreeData::DataPartPtr data_part = data.getActiveContainingPart(current_part_name);

        /// Two stage search: try to get accurage distance values.
        /// No need for num_reorder, which can be derived from length of row_ids
        auto vector_scan_result = vector_scan_manager->executeSecondStageVectorScan(data_part, row_ids, distances);

        /// Do something similar as mergeResult, pick up only row ids in returned result.
        if (vector_scan_result && vector_scan_result->computed)
            mergeResultForTwoStage(current_columns, pos_indexes, row_ids, vector_scan_result, result_columns, num_result_rows);

        /// There is only one part in the chunk
        if (first)
           break;
    }

    output_chunk.setColumns(std::move(result_columns), num_result_rows);
    LOG_DEBUG(&Poco::Logger::get("VectorScanRecomputeTransform"), "Vector Scan Recompute {} rows", output_chunk.getNumRows());
}

void VectorScanRecomputeTransform::mergeResultForTwoStage(
    const Columns & input_columns,
    std::vector<size_t> & position_index,
    std::vector<UInt64> & row_ids,
    VectorScanResultPtr tmp_result,
    MutableColumns & final_result,
    size_t & num_result_rows)
{
    DB::OpenTelemetry::SpanHolder span("VectorScanRecomputeTransform::mergeResultForTwoStage()");
    auto * log = &Poco::Logger::get("mergeResultForTwoStage");

    /// Reference from mergeVectorScanResult
    const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(tmp_result->result_columns[0].get());
    const auto & distance_column = tmp_result->result_columns[1];

    if (!label_column)
        LOG_DEBUG(log, "Label colum is null");

    /// For each vector search result, try to find if there is one with label equals to row id.
    for (size_t ind = 0; ind < label_column->size(); ++ind)
    {
        const UInt64 label_value = label_column->getUInt(ind);
        for (size_t i = 0; i < row_ids.size(); i++)
        {
            if (row_ids[i] == label_value)
            {
                auto pos = position_index[i];
                size_t k = 0;

                /// Copy a row to result and remove not needed columns from input
                for (size_t j = 0; j < input_columns.size(); j++)
                {
                    if (remove_columns_pos.find(j) != remove_columns_pos.end())
                        continue;

                    /// Update distance value
                    if (j == distance_column_pos)
                        final_result[k++]->insertFrom(*distance_column, ind);
                    else
                        final_result[k++]->insertFrom(*input_columns[j], pos);
                }

                /// continue to process next label
                break;
            }
        }
    }

    if (final_result.size() > 0)
        num_result_rows = final_result[0]->size();
    else
        num_result_rows = 0;
}

}
