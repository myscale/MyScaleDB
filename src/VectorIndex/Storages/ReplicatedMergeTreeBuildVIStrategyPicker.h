#pragma once

#include <base/types.h>
#include <optional>
#include <mutex>
#include <vector>
#include <atomic>
#include <boost/noncopyable.hpp>

namespace DB
{

class StorageReplicatedMergeTree;
struct ReplicatedMergeTreeLogEntryData;

/// NOTE: This class is referenced from ReplicatedMergeTreeMergeStrategyPicker.
/// Do similar as merge for vector index build on single replica.

/// Building vector index is more expensive than fetching
/// (so instead of doing exactly the same build cluster-wise you can do build once and fetch ready vector index)
/// Fetches may be desirable for other operational reasons (backup replica without lot of CPU resources).
///
/// That class allow to take a decisions about preferred strategy for a concreate vector index build.
///
/// Since that code is used in shouldExecuteLogEntry we need to be able to:
/// 1) make decision fast
/// 2) avoid excessive zookeeper operations
///
/// Because of that we need to cache some important things,
/// like list of active replicas (to limit the number of zookeeper operations)
///
/// That means we need to refresh the state of that object regularly
///
class ReplicatedMergeTreeBuildVIStrategyPicker : public boost::noncopyable
{
public:
    explicit ReplicatedMergeTreeBuildVIStrategyPicker(StorageReplicatedMergeTree & storage_);

    /// triggers refreshing the cached state (list of replicas etc.)
    /// used when we get new build vector index event from the zookeeper queue ( see queueUpdatingTask() etc )
    void refreshState();

    /// return true if there are two or more active replicas
    /// and we may need to do a fetch (or postpone) instead of build vector index
    bool shouldBuildVIndexOnSingleReplica(const ReplicatedMergeTreeLogEntryData & entry) const;

    /// returns the replica name
    /// and it's not current replica should do the vector index building
    /// If current replica is picked, the return value is nullpointer.
    std::optional<String> pickReplicaToExecuteBuildVectorIndex(const ReplicatedMergeTreeLogEntryData & entry);

    /// checks (in zookeeper) if the picked replica finished the vector index building
    bool isBuildVectorIndexFinishedByReplica(const String & replica, const ReplicatedMergeTreeLogEntryData & entry);

private:
    StorageReplicatedMergeTree & storage;

    /// calculate entry hash based on zookeeper path and new part name
    /// ATTENTION: it's not a general-purpose hash, it just allows to select replicas consistently
    uint64_t getEntryHash(const ReplicatedMergeTreeLogEntryData & entry) const;

    /// True if feature is enabled and there are two or more active replicas.
    std::atomic<bool> can_build_on_single_replica = false;
    std::atomic<time_t> last_refresh_time = 0;

    std::mutex mutex;

    /// those 2 members accessed under the mutex, only when
    /// can_build_on_single_replica enabled
    int current_replica_index = -1;
    std::vector<String> active_replicas;

};

}
