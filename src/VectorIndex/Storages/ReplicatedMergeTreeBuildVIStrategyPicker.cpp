#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <VectorIndex/Storages/ReplicatedMergeTreeBuildVIStrategyPicker.h>
#include <VectorIndex/Common/StorageVectorIndicesMgr.h>

#include <base/types.h>
#include <base/sort.h>
#include <optional>
#include <mutex>
#include <city.h>
#include <algorithm>
#include <atomic>


namespace DB
{

/// minimum interval (seconds) between checks if chosen replica finished the vector index build.
static const auto RECHECK_BUILD_READYNESS_INTERVAL_SECONDS = 1;

/// don't refresh state too often (to limit number of zookeeper ops)
static const auto REFRESH_STATE_MINIMUM_INTERVAL_SECONDS = 3;

/// refresh the state automatically if it was not refreshed for a longer time
static const auto REFRESH_STATE_MAXIMUM_INTERVAL_SECONDS = 30;


ReplicatedMergeTreeBuildVIStrategyPicker::ReplicatedMergeTreeBuildVIStrategyPicker(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{}


bool ReplicatedMergeTreeBuildVIStrategyPicker::isBuildVectorIndexFinishedByReplica(const String & replica, const ReplicatedMergeTreeLogEntryData & entry)
{
    /// those have only seconds resolution, so recheck period is quite rough
    auto reference_timestamp = entry.last_postpone_time;
    if (reference_timestamp == 0)
        reference_timestamp = entry.create_time;

    /// we don't want to check zookeeper too frequent
    if (time(nullptr) - reference_timestamp >= RECHECK_BUILD_READYNESS_INTERVAL_SECONDS)
    {
        return storage.vi_manager->checkReplicaHaveVIndexInPart(replica, entry.source_parts.at(0), entry.index_name);
    }

    return false;
}


bool ReplicatedMergeTreeBuildVIStrategyPicker::shouldBuildVIndexOnSingleReplica(const ReplicatedMergeTreeLogEntryData & entry) const
{
    return (
        can_build_on_single_replica /// true if feature is enabled and there are two or more active replicas
        && entry.type == ReplicatedMergeTreeLogEntry::BUILD_VECTOR_INDEX /// it is a build vector index log entry
    );
}


/// that will return the same replica name for ReplicatedMergeTreeLogEntry on all the replicas (if the replica set is the same).
/// that way each replica knows who is responsible for doing a certain vector index building.

/// in some corner cases (added / removed / deactivated replica)
/// nodes can pick different replicas to execute build vector index and wait for it (or to execute the same build vector index together)
/// but that doesn't have a significant impact (just 2 replicas will do the build vector index)
std::optional<String>
ReplicatedMergeTreeBuildVIStrategyPicker::pickReplicaToExecuteBuildVectorIndex(const ReplicatedMergeTreeLogEntryData & entry)
{
    /// last state refresh was too long ago, need to sync up the replicas list
    if (time(nullptr) - last_refresh_time > REFRESH_STATE_MAXIMUM_INTERVAL_SECONDS)
        refreshState();

    std::lock_guard lock(mutex);

    auto num_replicas = active_replicas.size();

    if (num_replicas == 0)
        return std::nullopt;

    int replica_index;

    if (storage.getSettings()->build_vector_index_on_random_single_replica == 1)
    {
        /// Choose a replica based on the hash of part name
        auto hash = getEntryHash(entry);
        replica_index = static_cast<int>(hash % num_replicas);
    }
    else /// Always choose the last sorted active replica.
        replica_index = static_cast<int>(num_replicas - 1);

    if (replica_index == current_replica_index)
        return std::nullopt;

    return active_replicas.at(replica_index);
}


void ReplicatedMergeTreeBuildVIStrategyPicker::refreshState()
{
    /// build vector index on single replica is disabled.
    if (storage.getSettings()->build_vector_index_on_random_single_replica == 0)
    {
        if (can_build_on_single_replica)
            can_build_on_single_replica = false;

        return;
    }

    /// Always to refresh state of active replicas. Enable single replica build vector index when there are 2 or more active replicas.
    auto now = time(nullptr);

    /// the setting was already enabled, and last state refresh was done recently
    if (last_refresh_time != 0 
        && now - last_refresh_time < REFRESH_STATE_MINIMUM_INTERVAL_SECONDS)
        return;

    auto zookeeper = storage.getZooKeeper();
    auto all_replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");

    ::sort(all_replicas.begin(), all_replicas.end());

    std::vector<String> active_replicas_tmp;
    int current_replica_index_tmp = -1;

    for (const String & replica : all_replicas)
    {
        if (zookeeper->exists(storage.zookeeper_path + "/replicas/" + replica + "/is_active"))
        {
            active_replicas_tmp.push_back(replica);
            if (replica == storage.replica_name)
            {
                current_replica_index_tmp = static_cast<int>(active_replicas_tmp.size() - 1);
            }
        }
    }

    if (current_replica_index_tmp < 0 || active_replicas_tmp.size() < 2)
    {
        if (can_build_on_single_replica)
        {
            LOG_WARNING(storage.log, "Can't find current replica in the active replicas list, or too few active replicas {} to enable build vector index on single replica", active_replicas_tmp.size());
            /// we can reset the settings w/o lock (it's atomic)
            can_build_on_single_replica = false;
        }

        return;
    }

    std::lock_guard lock(mutex);
    can_build_on_single_replica = true;
    last_refresh_time = now;
    current_replica_index = current_replica_index_tmp;
    active_replicas = active_replicas_tmp;
}


uint64_t ReplicatedMergeTreeBuildVIStrategyPicker::getEntryHash(const ReplicatedMergeTreeLogEntryData & entry) const
{
    auto hash_data = storage.zookeeper_path + entry.source_parts.at(0);
    return CityHash_v1_0_2::CityHash64(hash_data.c_str(), hash_data.length());
}


}
