#pragma once

#include <memory>
#include <vector>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VectorIndicesDescription.h>
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

using Float64 = double;

enum VectorIndexStatus
{
    SMALL_PART,
    PENDING,
    BUILDING,
    BUILT,
    LOADED,
    ERROR
};

const String MEMORY_USAGE_BYTES = "memory_usage_bytes";
const String DISK_USAGE_BYTES = "disk_usage_bytes";

class VectorIndexInfo
{
public:
    VectorIndexInfo(
        const String & database_,
        const String & table_,
        const UInt64 & total_vec_,
        const VectorIndexDescription & desc,
        const VectorIndex::SegmentId & segment_id,
        const MergeTreeSettingsPtr & settings,
        const StorageInMemoryMetadata & metadata,
        const VectorIndexStatus & status_ = BUILT,
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

    VectorIndexStatus status;
    String err_msg;

    void onBuildStart()
    {
        watch = std::make_unique<Stopwatch>();
        status = BUILDING;
    }

    void onBuildFinish(bool success, const String & err = "")
    {
        if (watch != nullptr)
            watch->stop();

        if (success)
            status = BUILT;
        else
            onError(err);
    }

    void onError(const String & err = "")
    {
        status = ERROR;
        err_msg = err;
    }

    void onIndexLoad() { status = LOADED; }

    void onIndexUnload() { status = BUILT; }

    /// Index building progress
    UInt8 progress() const { return status == BUILT || status == LOADED ? 100 : 0; }
    /// Elapsed time of index building
    double elapsed() { return watch == nullptr ? 0 : watch->elapsedSeconds(); }

    String statusString() const;

private:
    /// Stopwatch for index build
    StopwatchUniquePtr watch{nullptr};

    CurrentMetrics::Increment vector_memory_size_metric{CurrentMetrics::AllVectorIndexMemorySize, 0};
};

using VectorIndexInfoPtr = std::shared_ptr<VectorIndexInfo>;
using VectorIndexInfoPtrList = std::vector<VectorIndexInfoPtr>;
}
