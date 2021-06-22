#pragma once

#include <memory>
#include <vector>
#include <base/types.h>
#include <Common/Stopwatch.h>

namespace VectorIndex
{
class Metadata;
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
    /// Newly created vector index
    VectorIndexInfo(
        const String & database_,
        const String & table_,
        const String & part_,
        const String & name_,
        const String & type_,
        UInt64 total_vec_,
        VectorIndexStatus status_)
        : database(database_)
        , table(table_)
        , part(part_)
        , owner_part(part_)
        , owner_part_id(0)
        , name(name_)
        , type(type_)
        , status(status_)
        , total_vec(total_vec_)
        , memory_usage_bytes(0)
        , disk_usage_bytes(0)
    {
    }

    VectorIndexInfo(
        const String & database_,
        const String & table_,
        const String & part_,
        const String & name_,
        const String & type_,
        UInt64 total_vec_,
        UInt64 memory_usage_bytes_,
        UInt64 disk_usage_bytes_,
        const String & owner_part_ = "",
        Int32 owner_part_id_ = 0,
        VectorIndexStatus status_ = BUILT,
        const String & err_msg_ = "")
        : database(database_)
        , table(table_)
        , part(part_)
        , owner_part(owner_part_.empty() ? part_ : owner_part_)
        , owner_part_id(owner_part_id_)
        , name(name_)
        , type(type_)
        , status(status_)
        , err_msg(err_msg_)
        , total_vec(total_vec_)
        , memory_usage_bytes(memory_usage_bytes_)
        , disk_usage_bytes(disk_usage_bytes_)
    {
    }

    VectorIndexInfo(
        const String & database_,
        const String & table_,
        const VectorIndex::Metadata & metadata,
        VectorIndexStatus status_ = BUILT,
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

    /// Total number of vectors (including deleted ones)
    UInt64 total_vec;

    /// Index building progress
    UInt8 progress() const { return status == BUILT || status == LOADED ? 100 : 0; }
    /// Elapsed time of index building
    double elapsed() { return watch == nullptr ? 0 : watch->elapsedSeconds(); }

    // size of vector index in memory
    UInt64 memory_usage_bytes;
    // Size of vector index on disk
    UInt64 disk_usage_bytes;

    void setIndexSize(const VectorIndex::Metadata & metadata)
    {
        memory_usage_bytes = getMetadataInfoInt64(MEMORY_USAGE_BYTES, metadata);
        disk_usage_bytes = getMetadataInfoInt64(DISK_USAGE_BYTES, metadata);
    }

    String statusString() const;

private:
    /// Stopwatch for index build
    StopwatchUniquePtr watch{nullptr};

    static String getMetadataInfoString(const String & info_name, const VectorIndex::Metadata & metadata);

    static Int64 getMetadataInfoInt64(const String & info_name, const VectorIndex::Metadata & metadata)
    {
        String value = getMetadataInfoString(info_name, metadata);
        return value.empty() ? 0 : std::stol(value);
    }
};

using VectorIndexInfoPtr = std::shared_ptr<VectorIndexInfo>;
using VectorIndexInfoPtrList = std::vector<VectorIndexInfoPtr>;
}
