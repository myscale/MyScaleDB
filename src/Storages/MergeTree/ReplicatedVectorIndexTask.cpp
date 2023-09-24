#include <Storages/MergeTree/ReplicatedVectorIndexTask.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID ReplicatedVectorIndexTask::getStorageID()
{
    return storage.getStorageID();
}

bool ReplicatedVectorIndexTask::executeStep()
{
    auto remove_processed_entry = [&] (bool need_create_status = true) -> bool
    {
        try
        {
            storage.queue.removeProcessedEntry(storage.getZooKeeper(), selected_entry->log_entry);
            state = State::SUCCESS;

            std::lock_guard lock(storage.currently_vector_indexing_parts_mutex);
            storage.currently_vector_indexing_parts.erase(entry.source_parts.at(0));
            LOG_DEBUG(log, "currently_vector_indexing_parts remove: {}", entry.source_parts.at(0));

            String status_str;
            if (build_status == BuildVectorIndexStatus::SUCCESS)
                status_str = "success";
            else if (build_status == BuildVectorIndexStatus::BUILD_FAIL)
                status_str = "build_fail";
            else if (build_status == BuildVectorIndexStatus::NO_DATA_PART)
            {
                need_create_status = false;
                status_str = "no_data_part";
            }
            else if (build_status == BuildVectorIndexStatus::BUILD_SKIPPED)
            {
                need_create_status = false;
                status_str = "skipped";
            }
            else
                status_str = "meta_error";

            /// Some cases like build vector index for part is skipped, no need to create status in zookeeper.
            if (need_create_status)
                storage.createVectorIndexBuildStatusForPart(entry.source_parts.at(0), entry.index_name, status_str);
            else
                LOG_DEBUG(log, "No need to create build status '{}' in zookeeper for part {}", status_str, entry.source_parts.at(0));

            /// write latest cached vector index info to zookeeper
            storage.vidx_info_updating_task->schedule();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        return false;
    };

    auto execute_fetch = [&] () -> bool
    {
        if (storage.executeFetchVectorIndex(entry, replica_to_fetch))
            return remove_processed_entry();

        /// Try again in cases when replica cannot send vector index.
        return true;
    };

    switch (state)
    {
        case State::NEED_PREPARE :
        {
            bool res = false;
            bool need_fetch = false;
            std::tie(res, need_fetch) = prepare();

            /// Avoid resheduling, execute fetch here, in the same thread.
            if (!res)
            {
                if (need_fetch)
                    return execute_fetch();
                else
                {
                    /// There is no need to build vector index for some reasons.
                    return remove_processed_entry(false);
                }
            }

            state = State::NEED_EXECUTE_BUILD_VECTOR_INDEX;
            return true;
        }
        case State::NEED_EXECUTE_BUILD_VECTOR_INDEX :
        {
            try
            {
                build_status = builder.buildVectorIndex(metadata_snapshot, source_part->name, entry.slow_mode);
                storage.updateVectorIndexBuildStatus(entry.source_parts[0], true, "");

                /// For memory limit failure, try to run build 3 times.
                if (build_status == BuildVectorIndexStatus::BUILD_FAIL && !source_part->vector_index_build_error)
                    return true;

                /// Try to build again when unable to move vector index files due to concurrent mutation
                if (build_status == BuildVectorIndexStatus::BUILD_RETRY)
                    return true;
            }
            catch (...)
            {
                String exception_message = getCurrentExceptionMessage(false);
                storage.updateVectorIndexBuildStatus(entry.source_parts[0], false, exception_message);

                /// Set build error for part, avoid to build it again.
                auto part = storage.getActiveContainingPart(entry.source_parts[0]);
                if (part)
                    part->setBuildError();

                /// Mark build status as fail
                build_status = BuildVectorIndexStatus::BUILD_FAIL;

                /// Remove build index log entry to let other tables continue to create entry and build vector index.
                remove_processed_entry();

                throw;
            }

            /// Need to call remove_processed_entry()
            state = State::NEED_FINALIZE;
            return true;
        }
        case State::NEED_FINALIZE :
        {
            /// For failure build, execute_fetch should return some error status of the vector index building.
            return remove_processed_entry();
        }
        case State::SUCCESS :
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Do not call execute on previously succeeded task");
        }
    }
    return false;
}

void ReplicatedVectorIndexTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

ReplicatedVectorIndexTask::~ReplicatedVectorIndexTask()
{
    LOG_TRACE(log, "destroy vector index job with vector index entry: {}", entry.source_parts.at(0));
}

std::pair<bool, bool> ReplicatedVectorIndexTask::prepare()
{
    const String & source_part_name = entry.source_parts.at(0);

    if (metadata_snapshot->vec_indices.empty())
    {
        LOG_DEBUG(log, "Metadata of source part {} doesn't have vector index, will skip to build it", source_part_name);
        return {false, false};
    }

    /// Get active part containing current part to build vector index
    source_part = storage.getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part {} for vector index building is not ready, will skip to build vector index", source_part_name);
        return {false, false};
    }
    /// No need to check part name, mutations are not blocked by build vector index.
    auto info = MergeTreePartInfo::fromPartName(source_part_name, storage.format_version);
    if (!source_part->info.isFromSamePart(info))
    {
        LOG_DEBUG(log, "Source part {} for vector index building is covered by part {}, will skip to build vector index", source_part_name, source_part->name);
        return {false, false};
    }

    /// If we already have this vector index in this part, we do not need to do anything.

    /// TODO - Now we support only ONE vector index for a table.
    /// If multiples are supported, the log entry should contain the name and column of the vector index.
    for (const auto & vec_index : metadata_snapshot->vec_indices)
    {
        if (source_part->containVectorIndex(vec_index.name, vec_index.column))
        {
            LOG_DEBUG(log, "No need to build vector index for part {} because the covered part {} already has it built",
                      source_part_name, source_part->name);
            return {false, false};
        }
    }

    /// TODO - add estimation on needed space for vector index build

    /// building vector index is more expensive than fetching and it may be better to do build index tasks on single replica
    /// instead of building the same vector index on all replicas.
    if (storage.build_vindex_strategy_picker.shouldBuildVIndexOnSingleReplica(entry))
    {
        std::optional<String> replica_to_execute_build = storage.build_vindex_strategy_picker.pickReplicaToExecuteBuildVectorIndex(entry);
        if (replica_to_execute_build)
        {
            LOG_DEBUG(log,
                "Prefer fetching vector index in part {} from replica {} due to build_vector_index_on_random_single_replica",
                source_part_name, replica_to_execute_build.value());

            replica_to_fetch = replica_to_execute_build.value();

            return {false, true};
        }
    }

    return {true, false};
}

}
