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

#include <memory>
#include <vector>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>


namespace CurrentMetrics
{
extern const Metric AllVectorIndexMemorySize;
}

namespace VectorIndex
{
struct SegmentId;
}

namespace DB
{
class VIWithColumnInPart;

using Float64 = double;

enum VIState
{
    SMALL_PART,             // rows_count = 0
    PENDING,                // part without index, wait build index
    BUILDING,               // part without index, is building index 
    BUILT,                  // part with ready index(not decouple index), rename with ready
    LOADED,                 // fake status, only displayed in vector_index_segments system table
    ERROR                   // part build index error, and will not build index more than
};

const String MEMORY_USAGE_BYTES = "memory_usage_bytes";
const String DISK_USAGE_BYTES = "disk_usage_bytes";

class VIInfo
{
public:
    VIInfo(
        const String & database_,
        const String & table_,
        const String & index_name,
        VectorIndex::SegmentId & segment_id,
        const VIState & status_ = BUILT,
        const String & err_msg_ = "");

    VIInfo(
        const String & database_,
        const String & table_,
        const VIWithColumnInPart & column_index,
        const VIState & status_ = BUILT,
        const String & err_msg_ = "");

    String database;
    String table;
    String part;

    /// For vector index from merged old part
    String owner_part;
    Int32 owner_part_id;

    /// Index name
    String name;
    /// Index type
    String type;
    /// Index dimension
    UInt64 dimension;
    /// Total number of vectors (including deleted ones)
    UInt64 total_vec;
    // size of vector index in memory
    UInt64 memory_usage_bytes = 0;
    // Size of vector index on disk
    UInt64 disk_usage_bytes = 0;

    VIState state;
    String err_msg;

    double elapsed = 0;

    /// Index building progress
    UInt8 progress() const { return state == BUILT || state == LOADED ? 100 : 0; }

    String statusString() const;

private:

    CurrentMetrics::Increment vector_memory_size_metric{CurrentMetrics::AllVectorIndexMemorySize, 0};
};

using VIInfoPtr = std::shared_ptr<VIInfo>;
using VIInfoPtrList = std::vector<VIInfoPtr>;

}
