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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <VectorIndex/Processors/VSSplitTransform.h>
#include <Common/typeid_cast.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VSSplitTransform::VSSplitTransform(
        const Block & header,
        size_t num_outputs,
        UInt64 limit_)
    : IProcessor({header}, OutputPorts(num_outputs, header))
    , num_streams(num_outputs)
    , limit(limit_)
{
    part_column_pos = header.getPositionByName("_part");
}

IProcessor::Status VSSplitTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        status = !has_data ? prepareConsume()
                           : prepareGenerate();
    }

    return status;
}

IProcessor::Status VSSplitTransform::prepareConsume()
{
    auto & input = inputs.front();

    /// Check all outputs are finished or ready to get data.
    bool all_finished = true;
    for (auto & output : outputs)
    {
        if (output.isFinished())
            continue;

        all_finished = false;
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    /// If enough rows read or input is finished, call splitChunk()
    if ((limit && rows_read >= limit) || input.isFinished())
    {
        if (chunks.size())
        {
            has_data = true;
            was_output_processed.assign(outputs.size(), false);

            out_ports_data.clear();

            /// Put rows from the same data part to a output port, and loop data parts among all output ports.
            splitChunk(std::move(chunks));

            /// Mark as finish when rows are enough.
            if (!input.isFinished())
                input.close();

            return Status::Ready;
        }

        if (!input.isFinished())
            input.close();
    }

    if (input.isFinished())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// Try get chunk from input.
    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(limit != 0);
    rows_read += current_chunk.getNumRows();

    chunks.push_back(std::move(current_chunk));

    return Status::Ready;
}

IProcessor::Status VSSplitTransform::prepareGenerate()
{
    bool all_outputs_processed = true;
    size_t num_outputs_with_rows = out_ports_data.size();

    size_t chunk_number = 0;
    for (auto & output : outputs)
    {
        auto & was_processed = was_output_processed[chunk_number];
        ++chunk_number;

        if (was_processed)
            continue;

        /// split chunk doesn't put any row to this output chunk.
        if (chunk_number - 1 >= num_outputs_with_rows)
        {
            was_processed = true;
            continue;
        }

        if (output.isFinished())
            continue;

        if (!output.canPush())
        {
            all_outputs_processed = false;
            continue;
        }
        auto & out_port_data = out_ports_data[chunk_number-1];

        output.push(Chunk(std::move(out_port_data.split_columns), out_port_data.num_rows));
        was_processed = true;
    }

    if (all_outputs_processed)
    {
        has_data = false;
        return Status::Ready;
    }

    return Status::PortFull;
}

void VSSplitTransform::splitChunk(Chunks result_chunks)
{
    DB::OpenTelemetry::SpanHolder span("VSSplitTransform::splitChunk()");
    span.addAttribute("splittranform.num_reorder", limit);

    /// Split rows based on part name and put rows from the same data part to the same output port.
    /// Rows from different data parts are put to different output ports via round roubin.
    std::map<String, size_t> part_output_map;
    size_t next_output_pos = 0;
    size_t prev_split_rows = 0;

    for (Chunk & chunk : result_chunks)
    {
        const auto & current_columns = chunk.getColumns();

        UInt64 num_rows = chunk.getNumRows();

        /// Handle LowCardinality cases
        auto part_column = current_columns[part_column_pos]->convertToFullColumnIfLowCardinality();

        /// total rows split should be no more than limit
        UInt64 rows_need_split = 0;
        if (prev_split_rows + num_rows <= limit)
            rows_need_split = num_rows;
        else
            rows_need_split = limit - prev_split_rows;

        prev_split_rows += rows_need_split;

        if (const ColumnString * col = typeid_cast<const ColumnString *>(part_column.get()))
        {
            for (UInt64 i = 0; i < rows_need_split; i++)
            {
                String part_name = col->getDataAt(i).toString();
                size_t insert_pos = 0;

                /// Find a output port for the row from this date part.
                if (auto search = part_output_map.find(part_name); search != part_output_map.end())
                    insert_pos = search->second;
                else
                {
                    /// First row from this data part
                    insert_pos = next_output_pos % num_streams;
                    part_output_map.insert({part_name,insert_pos});

                    next_output_pos ++;

                    /// Initialize a new output port. If next_output_pos > num_streams, use an exists output port for this data part.
                    if (next_output_pos <= num_streams)
                    {
                        MutableColumns new_output_columns = outputs.front().getHeader().cloneEmptyColumns();

                        OutputPortData output_port_data;
                        output_port_data.split_columns = std::move(new_output_columns);
                        output_port_data.num_rows = 0;

                        out_ports_data.emplace_back(std::move(output_port_data));
                    }
                }

                /// Put the row into proper output port
                auto & out_port_data = out_ports_data[insert_pos];

                for (size_t j = 0; j < current_columns.size(); j++)
                    out_port_data.split_columns[j]->insertFrom(*current_columns[j], i);

                out_port_data.num_rows++;
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Data type of _part virtual column should be String");
        }
    }

    span.addAttribute("splittranform.split_rows", prev_split_rows);
    LOG_DEBUG(&Poco::Logger::get("VSSplitTransform"), "Vector Scan Split {} rows to {} output ports", prev_split_rows, out_ports_data.size());
}

}
