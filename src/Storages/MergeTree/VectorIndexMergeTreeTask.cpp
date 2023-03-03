#include <Storages/MergeTree/VectorIndexMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID VectorIndexMergeTreeTask::getStorageID()
{
    return storage.getStorageID();
}

bool VectorIndexMergeTreeTask::executeStep()
{
    if (vector_index_entry != nullptr)
    {
        LOG_DEBUG(log, "execute vector index build for : {} slow_mode: {}", vector_index_entry->part_name, slow_mode);
        try
        {
            builder.buildVectorIndex(metadata_snapshot, vector_index_entry->part_name, false, slow_mode);
            storage.updateVectorIndexBuildStatus(vector_index_entry->part_name, true, "");
        }
        catch (...)
        {
            String exception_message = getCurrentExceptionMessage(false);
            LOG_ERROR(log, "something went wrong during index building: {}", exception_message);
            storage.updateVectorIndexBuildStatus(vector_index_entry->part_name, false, exception_message);

            auto part = storage.getActiveContainingPart(vector_index_entry->part_name);
            if (part)
                part->setBuildError();
        }
    }
    return false;
}

UInt64 VectorIndexMergeTreeTask::getPriority()
{
    return 0;
}

void VectorIndexMergeTreeTask::onCompleted()
{
    if (vector_index_entry)
        LOG_DEBUG(log, "on complete: {}", vector_index_entry->part_name);

    task_result_callback(true);
}

VectorIndexMergeTreeTask::~VectorIndexMergeTreeTask()
{
    LOG_TRACE(log, "destroy vector index job with vector index entry: {}", vector_index_entry->part_name);
}

}
