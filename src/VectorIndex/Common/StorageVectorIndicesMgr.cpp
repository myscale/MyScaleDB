#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsScope.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>

#include <Interpreters/InterserverCredentials.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <VectorIndex/Common/StorageVectorIndicesMgr.h>

#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Common/ErrorCodes.h>


namespace ProfileEvents
{
extern const Event ReplicatedPartVectorIndexFetches;
}


namespace DB
{
using namespace VectorIndex;
namespace ErrorCodes
{
    extern const int DEADLOCK_AVOIDED;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int INTERSERVER_SCHEME_DOESNT_MATCH;
}

StorageVectorIndicesMgr::StorageVectorIndicesMgr(StorageMergeTree & data_) : VectorIndicesMgr(dynamic_cast<MergeTreeData &>(data_))
{
    initVectorIndexObject();
}

StorageMergeTree & StorageVectorIndicesMgr::getStorage()
{
    return dynamic_cast<StorageMergeTree &>(data);
}

StorageReplicatedVectorIndicesMgr::StorageReplicatedVectorIndicesMgr(StorageReplicatedMergeTree & data_)
    : VectorIndicesMgr(dynamic_cast<MergeTreeData &>(data_))
{
    initVectorIndexObject();
}

StorageReplicatedMergeTree & StorageReplicatedVectorIndicesMgr::getStorage()
{
    return dynamic_cast<StorageReplicatedMergeTree &>(data);
}

zkutil::ZooKeeperPtr StorageReplicatedVectorIndicesMgr::getZooKeeper()
{
    return getStorage().getZooKeeper();
}


void StorageReplicatedVectorIndicesMgr::createZKNodeIfNotExists(Coordination::Requests & ops)
{
    /// Vector index build status
    auto & storage = getStorage();
    if (data.getInMemoryMetadataPtr()->hasVectorIndices() && data.getSettings()->build_vector_index_on_random_single_replica)
        ops.emplace_back(zkutil::makeCreateRequest(storage.replica_path + "/vidx_build_parts", "", zkutil::CreateMode::Persistent));
}

bool StorageReplicatedVectorIndicesMgr::executeFetchVectorIndex(LogEntry & entry, String & replica, String & fetch_vector_index_path)
{
    String part_name = entry.source_parts.at(0);
    String vec_index_name = entry.index_name;
    auto & storage = getStorage();
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    /// If mutilple vector indices are supported, we need to check the index name and column in entry.
    if (!metadata_snapshot->hasVectorIndices() || !metadata_snapshot->getVectorIndices().has(vec_index_name))
    {
        LOG_INFO(
            log,
            "No vector index {} in table {}, hence no need to fetch it for part {}",
            vec_index_name,
            storage.zookeeper_path,
            part_name);
        return false;
    }

    /// Get active part containg current part to build vector index
    DataPartPtr future_part = storage.getActiveContainingPart(part_name);
    if (!future_part)
    {
        LOG_DEBUG(log, "Source part {} is not active, cannot fetch vector index {} for it", part_name, vec_index_name);
        return false;
    }

    if (future_part->name != part_name)
    {
        const auto part_info = MergeTreePartInfo::fromPartName(part_name, storage.format_version);

        /// Check part name without mutation version is same.
        if (future_part->info.isFromSamePart(part_info))
        {
            LOG_DEBUG(
                log, "Will put fetched vector index {} in future part {} instead of part {}", vec_index_name, future_part->name, part_name);
        }
        else
        {
            LOG_WARNING(
                log,
                "Part {} is covered by {} but should fetch vector index {}. "
                "Possibly the vector index fetching of this part is not needed and will be skipped. "
                "This shouldn't happen often.",
                part_name,
                future_part->name,
                vec_index_name);
            return false;
        }
    }

    /// We can't fetch vector index when none replicas have the built vector index in the part
    if (replica.empty())
    {
        LOG_INFO(
            log,
            "No active replica has vector index {} in part {} (cannot execute {}: {})",
            vec_index_name,
            part_name,
            entry.znode_name,
            entry.getDescriptionForLogs(storage.format_version));
        return false;
    }

    /// Fetch vector index in part from replica
    try
    {
        try
        {
            /// Temp fetch vector index acording Fetcher::fetchVectorIndex()
            static const String TMP_PREFIX = "tmp-fetch_vector_index_" + vec_index_name + "_";
            fetch_vector_index_path = TMP_PREFIX + future_part->name;

            String source_replica_path = fs::path(storage.zookeeper_path) / "replicas" / replica;
            if (!fetchVectorIndex(
                    future_part,
                    part_name,
                    vec_index_name,
                    metadata_snapshot,
                    storage.zookeeper_name,
                    source_replica_path,
                    /* zookeeper_ */ nullptr,
                    /* try_fetch_shared= */ true))
            {
                return false;
            }
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
            {
                LOG_WARNING(
                    log,
                    "Failed to lock future part {} for fetching vector index {} in part {}, will try again later",
                    vec_index_name,
                    future_part->name,
                    part_name);
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                return false;
            }

            /// No stacktrace, just log message
            if (e.code() == ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS)
                e.addMessage("Too busy replica. Will try later.");
            throw;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        throw;
    }

    return true;
}

/// Reference from StorageReplicatedMergeTree::fetchPart()
bool StorageReplicatedVectorIndicesMgr::fetchVectorIndex(
    DataPartPtr future_part,
    const String & part_name,
    const String & vec_index_name,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const String & source_zookeeper_name,
    const String & source_replica_path,
    zkutil::ZooKeeper::Ptr zookeeper_,
    bool try_fetch_shared)
{
    auto & storage = getStorage();
    auto zookeeper = zookeeper_ ? zookeeper_ : getZooKeeper();

    /// Check if the replica is active or not
    if (!zookeeper->exists(fs::path(source_replica_path) / "is_active"))
    {
        LOG_DEBUG(log, "The replica {} with vector index {} for part {} is not active", source_replica_path, vec_index_name, part_name);
        return false;
    }

    /// Use future part name in log
    String future_part_name = future_part->name;

    /// Use currently_fetching_vector_index_parts for vector index fetch.
    if (containsPartInFetchIndex(future_part_name))
    {
        LOG_DEBUG(log, "Part {} is already fetching vector index {} right now", future_part_name, vec_index_name);
        return false;
    }

    auto vi_fetch_holder = getFetchIndexHolder(future_part_name);

    if (future_part_name != part_name)
        LOG_DEBUG(
            log,
            "Fetching vector index {} in part {} from {}:{} and put in future part {}",
            vec_index_name,
            part_name,
            source_zookeeper_name,
            source_replica_path,
            future_part_name);
    else
        LOG_DEBUG(log, "Fetching vector index {} in part {} from {}:{}", vec_index_name, part_name, source_zookeeper_name, source_replica_path);

    TableLockHolder table_lock_holder;
    table_lock_holder = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

    /// Logging
    Stopwatch stopwatch;
    String tmp_fetch_vector_index_path; /// temp directory for fetched vector index files
    ProfileEventsScope profile_events_scope;

    auto write_part_log = [&](const ExecutionStatus & execution_status)
    {
        storage.writePartLog(
            PartLogElement::DOWNLOAD_VECTOR_INDEX,
            execution_status,
            stopwatch.elapsed(),
            part_name,
            future_part,
            {},
            nullptr,
            profile_events_scope.getSnapshot());
    };

    /// Not consider part_to_clone cases.

    ReplicatedMergeTreeAddress address;
    ConnectionTimeouts timeouts;
    String interserver_scheme;
    InterserverCredentialsPtr credentials;
    std::optional<CurrentlySubmergingEmergingTagger> tagger_ptr;
    std::function<String()> get_vector_index;

    /// Get part's disk
    auto disk_name = future_part->getDataPartStorage().getDiskName();
    auto future_part_disk = future_part->storage.getStoragePolicy()->getDiskByName(disk_name);

    {
        address.fromString(zookeeper->get(fs::path(source_replica_path) / "host"));
        timeouts = ConnectionTimeouts::getFetchPartHTTPTimeouts(storage.getContext()->getServerSettings(), storage.getContext()->getSettingsRef());

        credentials = storage.getContext()->getInterserverCredentials();
        interserver_scheme = storage.getContext()->getInterserverScheme();

        get_vector_index = [&, address, timeouts, credentials, interserver_scheme]()
        {
            if (interserver_scheme != address.scheme)
                throw Exception(
                    ErrorCodes::INTERSERVER_SCHEME_DOESNT_MATCH,
                    "Interserver schemes are different: "
                    "'{}' != '{}', can't fetch vector index {} from {}",
                    interserver_scheme,
                    address.scheme,
                    vec_index_name,
                    address.host);

            return storage.fetcher.fetchVectorIndex(
                future_part,
                storage.getContext(),
                part_name,
                vec_index_name,
                source_zookeeper_name,
                source_replica_path,
                address.host,
                address.replication_port,
                timeouts,
                credentials->getUser(),
                credentials->getPassword(),
                interserver_scheme,
                storage.replicated_fetches_throttler,
                "",
                try_fetch_shared,
                future_part_disk);
        };
    }

    try
    {
        /// Download vector index files and store them to the temporary directory with name tmp_fetch_vector_index_<part_name> under table data's path
        tmp_fetch_vector_index_path = get_vector_index();

        if (tmp_fetch_vector_index_path.empty())
        {
            /// Will try again if fetch failed
            LOG_DEBUG(
                log,
                "Fail to fetch vector index {} in part {} from {}, will try again later",
                vec_index_name,
                future_part_name,
                source_replica_path);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            return false;
        }

        write_part_log({});
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException("", true));
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ReplicatedPartVectorIndexFetches);

    LOG_DEBUG(
        log,
        "Fetched vector index {} in part {} from {} into {}.",
        vec_index_name,
        future_part_name,
        source_replica_path,
        tmp_fetch_vector_index_path);

    return true;
}

void StorageReplicatedVectorIndicesMgr::createVectorIndexBuildStatusForPart(
    const String & part_name, const String & vec_index_name, const String & status)
{
    auto zookeeper = getZooKeeper();
    auto & storage = getStorage();
    /// vidx_build_parts/vec_index_name/part_name, content is status
    auto vector_index_zk_path = fs::path(storage.replica_path) / "vidx_build_parts" / vec_index_name;

    /// In cases when path is not created.
    if (!zookeeper->exists(vector_index_zk_path))
    {
        zookeeper->createAncestors(vector_index_zk_path);
        zookeeper->createIfNotExists(vector_index_zk_path, "");

        LOG_DEBUG(log, "Created zookeeper path {} for vector index {}", vector_index_zk_path, vec_index_name);
    }

    zookeeper->createOrUpdate(
        fs::path(storage.replica_path) / "vidx_build_parts" / vec_index_name / part_name, status, zkutil::CreateMode::Persistent);
}

/// Remove parts with vector index build status from ZooKeeper
void StorageReplicatedVectorIndicesMgr::removeVecIndexBuildStatusForPartsFromZK(
    zkutil::ZooKeeperPtr & zookeeper, const DataPartsVector & parts_to_delete)
{
    for (const auto & part : parts_to_delete)
    {
        auto & storage = getStorage();
        String part_name = part->name;

        /// Support multiple vector indices
        for (const auto & vec_index_desc : storage.getInMemoryMetadataPtr()->getVectorIndices())
        {
            String vec_index_name = vec_index_desc.name;

            auto vi_status = part->segments_mgr->getSegmentStatus(vec_index_name);
            if (!(vi_status == SegmentStatus::BUILT) && !(vi_status == SegmentStatus::ERROR))
                continue;

            String part_status_path = fs::path(storage.replica_path) / "vidx_build_parts" / vec_index_name / part_name;

            try
            {
                if (!zookeeper->exists(part_status_path))
                    continue;

                auto rm_res = zookeeper->tryRemove(part_status_path);
                if (rm_res != Coordination::Error::ZOK && rm_res != Coordination::Error::ZNONODE)
                {
                    LOG_WARNING(
                        log, "Unexpected status code {} on attempt to remove vidx_build_parts/{}/{}", rm_res, vec_index_name, part_name);
                    continue;
                }

                LOG_DEBUG(log, "Deleted vector index {} build status for part {}", vec_index_name, part_name);
            }
            catch (...)
            {
                LOG_INFO(
                    log,
                    "An error occurred while checking and cleaning vector index {} build status for part {}: {}",
                    vec_index_name,
                    part_name,
                    getCurrentExceptionMessage(false));
            }
        }
    }
}

void StorageReplicatedVectorIndicesMgr::scheduleUpdateVectorIndexInfoZookeeperJob()
{
    auto & storage = getStorage();

    if (!hasAnyVectorIndex())
    {
        storage.vidx_info_updating_task->scheduleAfter(storage.getSettings()->vidx_zk_update_period.totalMilliseconds());
        return;
    }

    if (!vidx_init_loaded.load())
    {
        LOG_INFO(log, "Start loading vector indices from zookeeper");
        bool synced = false;
        Stopwatch watch;

        try
        {
            watch.start();
            synced = storage.waitForProcessingQueue(storage.getContext()->getSettingsRef().receive_timeout.totalMilliseconds(), SyncReplicaMode::DEFAULT, {});
            watch.stop();
        }
        catch (Exception & e)
        {
            LOG_WARNING(log, "Failed to wait for replica syncing: {}", e.displayText());
            return;
        }

        if (synced)
            LOG_INFO(log, "Replica synced in {} seconds", watch.elapsedSeconds());
        else
            LOG_WARNING(log, "Failed to shrink queue size in {} seconds", watch.elapsedSeconds());

        LOG_INFO(log, "Start loading vector indices from zookeeper");

        watch.restart();
        loadVectorIndexFromZookeeper();
        watch.stop();

        LOG_INFO(log, "Loading vector indices from zookeeper done in {} seconds", watch.elapsedSeconds());

        vidx_init_loaded.store(true);
    }

    writeVectorIndexInfoToZookeeper();

    storage.vidx_info_updating_task->scheduleAfter(storage.getSettings()->vidx_zk_update_period.totalMilliseconds());
}

/// Get the last loaded cache vector index info from Zookeeper.
std::unordered_map<String, std::unordered_set<String>> StorageReplicatedVectorIndicesMgr::getPreloadVectorIndicesFromZK()
{
    StorageReplicatedMergeTree & storage = getStorage();
    auto zookeeper = storage.getZooKeeper();

    String vector_index_info;
    bool success = zookeeper->tryGet(fs::path(storage.replica_path) / "vidx_info", vector_index_info);

    if (!success || vector_index_info.empty())
    {
        /// try other replicas
        Strings replicas = zookeeper->getChildren(fs::path(storage.zookeeper_path) / "replicas");

        /// Select replicas in uniformly random order.
        std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);

        for (const String & replica : replicas)
        {
            if (replica == storage.replica_name)
                continue;

            if (!zookeeper->exists(fs::path(storage.zookeeper_path) / "replicas" / replica / "is_active"))
                continue;

            String replica_vidx_info;
            success = zookeeper->tryGet(fs::path(storage.zookeeper_path) / "replicas" / replica / "vidx_info", replica_vidx_info);

            if (success && !replica_vidx_info.empty())
                vector_index_info += replica_vidx_info;
        }
    }

    if (vector_index_info.empty())
    {
        LOG_INFO(log, "No vector index info found on zookeeper for table {}", storage.getStorageID().getFullTableName());
        return {};
    }

    ReadBufferFromString in(vector_index_info);
    std::unordered_map<String, std::unordered_set<String>> vector_indices;

    while (!in.eof())
    {
        String part_name, vector_index_name;
        in >> part_name >> "\t" >> vector_index_name >> "\n";

        if (vector_indices.contains(part_name))
        {
            vector_indices.at(part_name).emplace(vector_index_name);
        }
        else
        {
            std::unordered_set<String> set{vector_index_name};
            vector_indices.try_emplace(part_name, set);
        }
    }

    return vector_indices;
}

/// Write vector index info to zookeeper.
void StorageReplicatedVectorIndicesMgr::writeVectorIndexInfoToZookeeper(bool force)
{
    std::lock_guard lock{vidx_info_mutex};
    auto & storage = getStorage();
    if (force || storage.getInMemoryMetadata().hasVectorIndices())
    {
        /// get cached vector index info
        auto cache_list = VICacheManager::getAllItems();
        auto table_id = toString(storage.getStorageID().uuid);

        std::unordered_map<String, std::unordered_set<String>> cached_index_parts;
        std::unordered_map<String, String> index_column_map;

        /// get cached vector index & parts for current table
        for (const auto & cache_item : cache_list)
        {
            auto cache_key = cache_item.first;
            LOG_DEBUG(log, "write_cache: cache_key: {}", cache_key.toString());
            if (cache_key.getTableUUID() == table_id)
            {
                if (cached_index_parts.contains(cache_key.vector_index_name))
                {
                    cached_index_parts.at(cache_key.vector_index_name).emplace(cache_key.getPartName());
                }
                else
                {
                    std::unordered_set<String> part_set{cache_key.getPartName()};
                    cached_index_parts.try_emplace(cache_key.vector_index_name, part_set);
                }

                index_column_map.try_emplace(cache_key.vector_index_name, cache_key.column_name);
            }
        }

        WriteBufferFromOwnString out;
        int count = 0;

        /// get active part name (without mutation) for cached vector index
        for (const auto & index_parts : cached_index_parts)
        {
            auto index_name = index_parts.first;
            auto cached_parts = index_parts.second;
            auto column_name = index_column_map.at(index_name);

            for (const auto & part : storage.getDataPartsVectorForInternalUsage())
            {
                auto part_name = part->info.getPartNameWithoutMutation();

                if (cached_parts.contains(part_name))
                {
                    out << part_name << "\t" << index_name << "\n";
                    count++;
                    continue;
                }

                /// maybe part with decouple index, record it sub index
                auto vi_seg = part->segments_mgr->getSegment(index_name);
                if (!vi_seg)
                    continue;

                for (const auto & cache_key : vi_seg->getCachedSegmentKeys())
                {
                    if (cached_parts.contains(cache_key.getPartName()))
                    {
                        out << part_name << "\t" << index_name << "\n";
                        count++;
                        break;
                    }
                }
            }
        }

        try
        {
            LOG_DEBUG(log, "Writing {} vector index info to zookeeper", count);
            getZooKeeper()->createOrUpdate(fs::path(storage.replica_path) / "vidx_info", out.str(), zkutil::CreateMode::Persistent);
            LOG_DEBUG(log, "Wrote {} vector index info to zookeeper", count);
        }
        catch (zkutil::KeeperException & e)
        {
            LOG_ERROR(log, "Failed to write vector index info to zookeeper: {}", e.what());
        }
    }
}

void StorageReplicatedVectorIndicesMgr::loadVectorIndexFromZookeeper()
{
    std::unordered_map<String, std::unordered_set<String>> vector_indices = getPreloadVectorIndicesFromZK();

    LOG_INFO(log, "Load {} vector indices from keeper", vector_indices.size());

    Stopwatch watch;
    loadVectorIndices(vector_indices);

    LOG_INFO(log, "Loaded vector indices from keeper in {} seconds", watch.elapsedSeconds());
}

bool StorageReplicatedVectorIndicesMgr::canSendVectorIndexForPart(const String & part_name)
{
    const auto & storage = getStorage();
    return storage.queue.canSendVectorIndexForPart(part_name);
}

bool StorageReplicatedVectorIndicesMgr::checkReplicaHaveVIndexInPart(const String & replica, const String & part_name, const String & vec_index_name)
{
    auto zookeeper = getZooKeeper();
    auto & storage = getStorage();
    return zookeeper->exists(fs::path(storage.zookeeper_path) / "replicas" / replica / "vidx_build_parts" / vec_index_name / part_name);
}

} // namespace DB
