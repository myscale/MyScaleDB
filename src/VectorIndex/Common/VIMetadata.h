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

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <VectorIndex/Common/SegmentId.h>
#include <VectorIndex/Common/VICommon.h>

namespace VectorIndex
{
class VIMetadata
{
public:
    VIMetadata(const SegmentId & segment_id_) : segment_id(segment_id_) { }

    VIMetadata(
        const SegmentId & segment_id_,
        DB::String version_,
        VIType type_,
        VIMetric metric_,
        size_t dimension_,
        size_t total_vec_,
        bool fallback_to_flat_,
        VIParameter build_params_,
        std::unordered_map<std::string, std::string> infos_)
        : segment_id(segment_id_)
        , version(version_)
        , type(type_)
        , metric(metric_)
        , dimension(dimension_)
        , total_vec(total_vec_)
        , fallback_to_flat(fallback_to_flat_)
        , build_params(build_params_)
        , infos(infos_)
    {
    }

    void readText(DB::ReadBuffer & buf);
    void writeText(DB::WriteBuffer & buf) const;

    const SegmentId & segment_id;

    /// Current part name is not provided and is read from the description file. 
    /// This value is incorrect in some scenarios.
    DB::String version;
    VIType type;
    VIMetric metric;
    size_t dimension;
    size_t total_vec;
    bool fallback_to_flat;
    VIParameter build_params;
    std::unordered_map<std::string, std::string> infos;
};
}
