#pragma once

#include <VectorIndex/Common/VectorIndicesMgr.h>

namespace DB
{
class StorageMergeTree;
class StorageReplicatedMergeTree;

struct ReplicatedMergeTreeLogEntry;
using LogEntry = ReplicatedMergeTreeLogEntry;

class StorageVectorIndicesMgr : public VectorIndicesMgr
{
public:
    StorageVectorIndicesMgr(StorageMergeTree & data_);
    /// remove all vector index cached files in nvme disk
    void startup() override
    {
        clearVectorNvmeCache();
    }
    void shutdown() override 
    {
        builds_blocker.cancelForever();
        clearCachedVectorIndex(data.getDataPartsVectorForInternalUsage());
    }

private:
    StorageMergeTree & getStorage();
};


class StorageReplicatedVectorIndicesMgr : public VectorIndicesMgr
{
public:
    StorageReplicatedVectorIndicesMgr(StorageReplicatedMergeTree & data_);
    void startup() override
    {
        clearVectorNvmeCache(getPreloadVectorIndicesFromZK());
    }

    void shutdown() override
    {
        /// first dump vector index info to zookeeper
        if (vidx_init_loaded.load())
            writeVectorIndexInfoToZookeeper();

        builds_blocker.cancelForever();
        clearCachedVectorIndex(data.getDataPartsVectorForInternalUsage());
    }

    bool isReplicaMgr() const override { return true; }

    void createZKNodeIfNotExists(Coordination::Requests & ops);

    scope_guard getFetchIndexHolder(const String & part_name)
    {
        addPartToFetchIndex(part_name);
        return scope_guard([this, part_name] { removePartFromFetchIndex(part_name); });
    }
    bool addPartToFetchIndex(const String & part_name)
    {
        std::lock_guard lock(currently_fetching_vector_index_parts_mutex);
        return currently_fetching_vector_index_parts.insert(part_name).second;
    }
    bool removePartFromFetchIndex(const String & part_name)
    {
        std::lock_guard lock(currently_fetching_vector_index_parts_mutex);
        return currently_fetching_vector_index_parts.erase(part_name) > 0;
    }
    bool containsPartInFetchIndex(const String & part_name) const
    {
        std::lock_guard lock(currently_fetching_vector_index_parts_mutex);
        return currently_fetching_vector_index_parts.count(part_name) > 0;
    }
    scope_guard getSendIndexHolder(const String & part_name)
    {
        addPartToSendIndex(part_name);
        return scope_guard([this, part_name] { removePartFromSendIndex(part_name); });
    }
    bool addPartToSendIndex(const String & part_name)
    {
        std::lock_guard lock(currently_sending_vector_index_parts_mutex);
        return currently_sending_vector_index_parts.insert(part_name).second;
    }
    bool removePartFromSendIndex(const String & part_name)
    {
        std::lock_guard lock(currently_sending_vector_index_parts_mutex);
        return currently_sending_vector_index_parts.erase(part_name) > 0;
    }
    bool containsPartInSendIndex(const String & part_name) const
    {
        std::lock_guard lock(currently_sending_vector_index_parts_mutex);
        return currently_sending_vector_index_parts.count(part_name) > 0;
    }

    /// Fetch built vector index in part from other replica.
    /// NOTE: First of all tries to find the part on other replica.
    /// After that tries to fetch the vector index.
    bool executeFetchVectorIndex(LogEntry & entry, String & replica, String & fetch_vector_index_path);

    /// Download the vector index files of the specified part from the specified replica.
    /// Returns false if vector index files are aready fetching right now.
    bool fetchVectorIndex(
        DataPartPtr future_part,
        const String & part_name,
        const String & vec_index_name,
        const StorageMetadataPtr & metadata_snapshot,
        const String & source_zookeeper_name,
        const String & source_replica_path,
        zkutil::ZooKeeper::Ptr zookeeper_ = nullptr,
        bool try_fetch_shared = true);

    /// Create a finished flag node 'vidx_build_parts/part_name_in_log_entry' on zookeeper for a part
    /// when vector index build has done, no matter success or not.
    void createVectorIndexBuildStatusForPart(const String & part_name, const String & vec_index_name, const String & status);

    /// Remove parts with vector index build status from ZooKeeper
    void removeVecIndexBuildStatusForPartsFromZK(zkutil::ZooKeeperPtr & zookeeper, const DataPartsVector & parts_to_delete);

    /// Update cached vector index info to zookeeper periodically.
    void scheduleUpdateVectorIndexInfoZookeeperJob();

    bool isVectorIndexInfoLoaded() const { return vidx_init_loaded.load(); }

    /// Check if part is being merged right now via future_parts in queue.
    bool canSendVectorIndexForPart(const String & part_name);
    /// Check if the part in replica has finished vector index building job.
    bool checkReplicaHaveVIndexInPart(const String & replica, const String & part_name, const String & vec_index_name);
    /// Get the last loaded cache vector index info from Zookeeper.
    std::unordered_map<String, std::unordered_set<String>> getPreloadVectorIndicesFromZK();
    /// Write vector index info to zookeeper.
    void writeVectorIndexInfoToZookeeper(bool force = false);
    /// Get cached vector index info from zookeeper and load into cache.
    void loadVectorIndexFromZookeeper();
private:
    StorageReplicatedMergeTree & getStorage();
    zkutil::ZooKeeperPtr getZooKeeper();

    /// Required only to avoid races between mutate and fetchVectorIndex
    std::unordered_set<String> currently_fetching_vector_index_parts;
    mutable std::mutex currently_fetching_vector_index_parts_mutex;

    /// Required only to avoid races between merge and sendVectorIndex
    std::unordered_set<String> currently_sending_vector_index_parts;
    mutable std::mutex currently_sending_vector_index_parts_mutex;

    /// Whether vector indices were initially loaded on table start-up
    std::atomic<bool> vidx_init_loaded{false};

    /// It is acquired when writing vector index info to zookeeper
    std::mutex vidx_info_mutex;
};

} // namespace DB
