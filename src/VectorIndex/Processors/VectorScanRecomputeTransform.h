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

#include <Processors/IProcessor.h>
#include <Core/SortDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <VectorIndex/Storages/MergeTreeVectorScanManager.h>

namespace DB
{

/// Implementation for vector scan reorder in two stage search, get accurate distance value.
/// This processor support one input and one output.
/// The header of output doesn't contain _part and _part_offset virtual column if not exists in input header.
class VectorScanRecomputeTransform : public IProcessor
{
public:
    VectorScanRecomputeTransform(
        const Block & input_header_,
        const Block & output_header_,
        MergeTreeVectorScanManagerPtr vector_scan_manager_,
        const MergeTreeData & data_
        );

    String getName() const override { return "VectorScanRecompute"; }

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    /* Implementation of ISimpleTransform.
    */
    void transform(Chunk & input_chunk, Chunk & output_chunk);

    /* Implementation of IProcessor;
     */
    Status prepare() override;
    void work() override;

private:
    /* Data (formerly) inherited from ISimpleTransform, needed for the
     * implementation of the IProcessor interface.
     */
    InputPort & input;
    OutputPort & output;

    bool has_input = false;
    Port::Data input_data;
    bool has_output = false;
    Port::Data output_data;

    /* Data for vector scan recompute transform itself.
     */
    Block input_header;

    MergeTreeVectorScanManagerPtr vector_scan_manager;

    const MergeTreeData & data;

    std::unordered_set<size_t> remove_columns_pos;
    size_t distance_column_pos;
    bool same_header = false; /// The output and input header are different, input with _part, output not.

    std::unordered_set<std::string> processed_parts_name; /// Candidates may come from multiple parts

    void mergeResultForTwoStage(
        const Columns & input_columns,
        std::vector<size_t> & position_index,
        std::vector<UInt64> & row_ids,
        VectorScanResultPtr tmp_result,
        MutableColumns & result_columns,
        size_t & num_result_rows);
};

}
