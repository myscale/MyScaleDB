#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/SegmentId.h>

namespace VectorIndex
{
class Metadata
{
public:
    Metadata(const SegmentId & segment_id_) : segment_id(segment_id_) { }

    Metadata(
        const SegmentId & segment_id_,
        DB::String version_,
        Search::IndexType type_,
        Search::Metric metric_,
        size_t dimension_,
        size_t total_vec_,
        bool fallback_to_flat_,
        Search::Parameters build_params_,
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
    DB::String version;
    Search::IndexType type;
    Search::Metric metric;
    size_t dimension;
    size_t total_vec;
    bool fallback_to_flat;
    Search::Parameters build_params;
    std::unordered_map<std::string, std::string> infos;
};
}
