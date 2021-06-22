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
    LOG_DEBUG(&Poco::Logger::get("(VectorIndexMergeTreeTask)"), "enter execute step");
    if (vector_index_entry != nullptr && !vector_index_entry->data_parts.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("(VectorIndexMergeTreeTask)"), "actually execute step");
        try
        {
            builder.buildVectorIndex(metadata_snapshot, vector_index_entry->data_parts, false);
        }
        catch (std::exception & e)
        {
            LOG_DEBUG(&Poco::Logger::get("(VectorIndexMergeTreeTask)"), "something went wrong during index building: {}", e.what());
            for (auto & part : vector_index_entry->data_parts)
            {
                part->setBuildError();
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
    storage.finishVectorIndexJob(std::move(vector_index_entry->data_parts));
    task_result_callback(true);
}

VectorIndexMergeTreeTask::~VectorIndexMergeTreeTask()
{
    LOG_TRACE(&Poco::Logger::get("vectorIndexTask"), "destroy vector index job with vector index entry:");
    for (auto & data : vector_index_entry->data_parts)
    {
        LOG_TRACE(&Poco::Logger::get("vectorIndexTask"), "{}", data->name);
    }
    storage.finishVectorIndexJob(std::move(vector_index_entry->data_parts));
}

}
