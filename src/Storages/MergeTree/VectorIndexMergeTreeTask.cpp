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
        LOG_DEBUG(log, "Execute vector index build for {}, slow_mode: {}", vector_index_entry->part_name, slow_mode);
        try
        {
            builder.buildVectorIndex(metadata_snapshot, vector_index_entry->part_name, vector_index_entry->vector_index_name, slow_mode);
            storage.updateVectorIndexBuildStatus(vector_index_entry->part_name, vector_index_entry->vector_index_name, true, "");
        }
        catch (...)
        {
            /// Record the failed build status to vector_indices, only when the index exists.
            for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
            {
                /// Find the vector index description from metadata snapshot when build starts.
                if (vec_index_desc.name == vector_index_entry->vector_index_name)
                {
                    /// Check vector index exists in table's latest metadata
                    auto & latest_vec_indices = storage.getInMemoryMetadataPtr()->getVectorIndices();
                    if (latest_vec_indices.has(vec_index_desc))
                    {
                        String exception_message = getCurrentExceptionMessage(false);
                        storage.updateVectorIndexBuildStatus(vector_index_entry->part_name, vector_index_entry->vector_index_name, false, exception_message);
                    }

                    break;
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
    if (vector_index_entry)
        LOG_DEBUG(log, "On complete: {}", vector_index_entry->part_name);

    task_result_callback(true);
}

VectorIndexMergeTreeTask::~VectorIndexMergeTreeTask()
{
    LOG_TRACE(log, "Destroy vector index job with vector index entry: {}", vector_index_entry->part_name);
}

}
