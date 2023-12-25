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

#include <Columns/IColumn.h>

#include <memory>

namespace DB
{

struct VectorScanResult
{
    bool is_batch;
    bool computed;
    int top_k;   /// Will be reused to store num_reorder in two stage search
    int query_vector_num;
    MutableColumns result_columns;
    std::vector<bool> was_result_processed;  /// Mark if the result was processed or not.
    UInt64 vector_scan_duration_ms;
    UInt64 read_duration_ms;

    VectorScanResult(): is_batch(false), computed(false) {}
};

using VectorScanResultPtr = std::shared_ptr<VectorScanResult>;

}
