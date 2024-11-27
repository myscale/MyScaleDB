#include <VectorIndex/Storages/VITask.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int VECTOR_INDEX_ALREADY_EXISTS;
}

VectorIndex::SegmentBuiltStatus VITask::prepare()
{
    try
    {
        ctx = builder.prepareBuildVIContext(
            metadata_snapshot, vector_index_entry->part_name, vector_index_entry->vector_index_name, slow_mode);

        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::SUCCESS};
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            getLogger("VITask"),
            "Prepare build vector index {} error {}: {}",
            vector_index_entry->part_name,
            e.code(),
            e.message());
        if (e.code() == ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART)
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::NO_DATA_PART, e.code(), e.message()};
        else if (e.code() == ErrorCodes::VECTOR_INDEX_ALREADY_EXISTS)
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};
        else
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }
}

VITask::~VITask()
{
    LOG_DEBUG(log, "Destroy vector index job with vector index entry: {}", vector_index_entry->part_name);
}

}
