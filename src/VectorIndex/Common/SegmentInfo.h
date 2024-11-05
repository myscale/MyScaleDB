#pragma once

#include <memory>
#include <vector>
#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <VectorIndex/Storages/VIDescriptions.h>
#include <VectorIndex/Common/SegmentStatus.h>
#include <VectorIndex/Common/VICommon.h>

namespace DB
{

class SegmentInfo
{
public:
    SegmentInfo(
        const VIDescription & vec_desc,
        const IMergeTreeDataPart & data_part,
        const String & owner_part_name,
        const UInt8 own_part_id,
        const VectorIndex::SegmentStatusPtr & status_,
        const VectorIndex::VIType & index_type,
        const UInt64 index_dimension,
        const UInt64 total_vectors,
        const UInt64 memory_usage,
        const UInt64 disk_usage)
        : database(data_part.storage.getStorageID().database_name),
          table(data_part.storage.getStorageID().table_name),
          part(data_part.name),
          owner_part(owner_part_name),
          owner_part_id(own_part_id),
          name(vec_desc.name),
          type(Search::enumToString(index_type)),
          dimension(index_dimension),
          total_vec(total_vectors),
          memory_usage_bytes(memory_usage),
          disk_usage_bytes(disk_usage),
          status(VectorIndex::SegmentStatus(status_->getStatus(), status_->getErrMsg())),
          elapsed_time(status_->getElapsedTime())
    {}

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

    VectorIndex::SegmentStatus status;

    UInt64 elapsed_time{0};

    /// Index building progress
    UInt8 progress() const { return status.getStatus() == VectorIndex::SegmentStatus::BUILT || status.getStatus() == VectorIndex::SegmentStatus::LOADED ? 100 : 0; }

    String statusString() const
    {
        return status.statusToString();
    }
};

using SegmentInfoPtr = std::shared_ptr<SegmentInfo>;
using SegmentInfoPtrList = std::vector<SegmentInfoPtr>;

}
