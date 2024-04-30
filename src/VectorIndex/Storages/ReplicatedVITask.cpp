/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Storages/MergeTree/MergeTreeData.h>
#include <VectorIndex/Storages/ReplicatedVITask.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int VECTOR_INDEX_ALREADY_EXISTS;
}

VIBuiltStatus ReplicatedVITask::prepare()
{
    const String & source_part_name = entry.source_parts.at(0);

    try
    {
        ctx.reset();
        ctx = builder.prepareBuildVIContext(metadata_snapshot, entry.source_parts.at(0), entry.index_name, slow_mode, true);
    }
    catch (Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("VITask"), "Prepare build vector index {} error {}: {}", part_name, e.code(), e.message());
        if (e.code() == ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART)
            return VIBuiltStatus{VIBuiltStatus::NO_DATA_PART, e.code(), e.message()};
        else if (e.code() == ErrorCodes::VECTOR_INDEX_ALREADY_EXISTS)
            return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
        else
            return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }

    auto & replicated_storage = dynamic_cast<StorageReplicatedMergeTree &>(storage);

    /// TODO - add estimation on needed space for vector index build

    /// building vector index is more expensive than fetching and it may be better to do build index tasks on single replica
    /// instead of building the same vector index on all replicas.
    if (replicated_storage.build_vindex_strategy_picker.shouldBuildVIndexOnSingleReplica(entry))
    {
        std::optional<String> replica_to_execute_build
            = replicated_storage.build_vindex_strategy_picker.pickReplicaToExecuteBuildVectorIndex(entry);
        if (replica_to_execute_build)
        {
            LOG_DEBUG(
                log,
                "Prefer fetching vector index {} in part {} from replica {} due to build_vector_index_on_random_single_replica",
                entry.index_name,
                source_part_name,
                replica_to_execute_build.value());

            String temp_fetch_vector_index_path;

            if (replicated_storage.executeFetchVectorIndex(entry, replica_to_execute_build.value(), temp_fetch_vector_index_path))
            {
                LOG_DEBUG(log, "Fetch Vector Index finish for part: {}", source_part_name);
                const DataPartStorageOnDiskBase * part_storage
                    = dynamic_cast<const DataPartStorageOnDiskBase *>(ctx->source_part->getDataPartStoragePtr().get());
                if (!part_storage)
                    return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL};
                ctx->temporary_directory_lock = replicated_storage.getTemporaryPartDirectoryHolder(temp_fetch_vector_index_path);
                ctx->vector_tmp_full_path
                    = fs::path(part_storage->getFullPath()).parent_path().parent_path() / temp_fetch_vector_index_path / "";
                ctx->vector_tmp_relative_path = replicated_storage.getRelativeDataPath() + temp_fetch_vector_index_path + "/";
            }
            else
            {
                /// Is there an infinite loop?
                LOG_WARNING(log, "Fetch vector index {} to part {} error, will retry.", entry.index_name, source_part_name);
                return VIBuiltStatus{VIBuiltStatus::BUILD_RETRY};
            }
        }
    }

    return VIBuiltStatus{VIBuiltStatus::SUCCESS};
}

void ReplicatedVITask::remove_processed_entry()
{
    try
    {
        bool need_create_status = true;
        auto & replicated_storage = dynamic_cast<StorageReplicatedMergeTree &>(storage);
        replicated_storage.queue.removeProcessedEntry(replicated_storage.getZooKeeper(), selected_entry->log_entry);
        state = State::SUCCESS;

        std::lock_guard lock(replicated_storage.currently_vector_indexing_parts_mutex);
        replicated_storage.currently_vector_indexing_parts.erase(entry.source_parts.at(0));
        LOG_DEBUG(log, "currently_vector_indexing_parts remove: {}", entry.source_parts.at(0));

        /// Create vector index build status only when index node feature is enabled.
        if (replicated_storage.getSettings()->build_vector_index_on_random_single_replica)
        {
            String status_str = build_status.statusToString();
            if (build_status.getStatus() == VIBuiltStatus::NO_DATA_PART
                || build_status.getStatus() == VIBuiltStatus::BUILD_SKIPPED)
                need_create_status = false;

            /// Some cases like build vector index for part is skipped, no need to create status in zookeeper.
            if (need_create_status)
                replicated_storage.createVectorIndexBuildStatusForPart(entry.source_parts.at(0), entry.index_name, status_str);
            else
                LOG_DEBUG(
                    log,
                    "No need to create build status '{}' in zookeeper for vector index {} in part {}",
                    status_str,
                    entry.index_name,
                    entry.source_parts.at(0));
        }

        /// write latest cached vector index info to zookeeper
        replicated_storage.vidx_info_updating_task->schedule();
    }
    catch (zkutil::KeeperException & e)
    {
        LOG_WARNING(
            log,
            "Remove build vector log entry for index {} in part {} failed: code={}, message={}",
            entry.index_name,
            entry.source_parts.at(0),
            e.code,
            e.message());
    }
}

ReplicatedVITask::~ReplicatedVITask()
{
    LOG_TRACE(log, "destroy vector index job with vector index entry: {}", entry.source_parts.at(0));
}

}
