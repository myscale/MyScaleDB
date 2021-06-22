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
    if (vector_index_entry != nullptr && !vector_index_entry->data_part_names.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("(VectorIndexMergeTreeTask)"), "execute vector index build for : {} slow_mode: {}", vector_index_entry->data_part_names[0], slow_mode);
        try
        {
            builder.buildVectorIndex(metadata_snapshot, vector_index_entry->data_part_names, false, slow_mode);
            storage.updateVectorIndexBuildStatus(vector_index_entry->data_part_names[0], true, "");
        }
        catch (...)
        {
            String exception_message = getCurrentExceptionMessage(false);
            LOG_ERROR(&Poco::Logger::get("(VectorIndexMergeTreeTask)"), "something went wrong during index building: {}", exception_message);
            storage.updateVectorIndexBuildStatus(vector_index_entry->data_part_names[0], false, exception_message);

            for (const String & part_name : vector_index_entry->data_part_names)
            {
                auto part = storage.getActiveContainingPart(part_name);
                if (part)
                {
                    part->setBuildError();
                }
            }
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
    for (const auto & part : vector_index_entry->data_part_names)
        LOG_DEBUG(&Poco::Logger::get("vectorIndexTask"), "on complete: {}", part);

    /// storage.finishVectorIndexJob(std::move(vector_index_entry->data_part_names));
    task_result_callback(true);
}

VectorIndexMergeTreeTask::~VectorIndexMergeTreeTask()
{
    LOG_TRACE(&Poco::Logger::get("vectorIndexTask"), "destroy vector index job with vector index entry:");
    for (auto & data : vector_index_entry->data_part_names)
    {
        LOG_TRACE(&Poco::Logger::get("vectorIndexTask"), "{}", data);
    }
    /// storage.finishVectorIndexJob(std::move(vector_index_entry->data_part_names));
}

}
