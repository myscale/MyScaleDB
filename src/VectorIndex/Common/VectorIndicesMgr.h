#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Common/logger_useful.h>
#include <Common/ActionBlocker.h>

#include <VectorIndex/Common/VICommands.h>
#include <VectorIndex/Common/VectorDataset.h>
#include <VectorIndex/Common/SegmentsMgr.h>
#include <VectorIndex/Common/Segment.h>

#include <VectorIndex/Common/VectorIndexObject.h>

namespace DB
{
struct VIEntry;
using VIEntryPtr = std::shared_ptr<VIEntry>;
struct VIContext;
using VIContextPtr = std::shared_ptr<VIContext>;
class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

/// VectorIndicesMgr class is designed to manage vector index data and cache at the table level.
/// This class provides functionalities for starting and shutting down vector indices, handling the loading and clearing of vector indices, 
/// and managing tasks related to vector indices.
class VectorIndicesMgr
{
public:
    virtual ~VectorIndicesMgr() = default;

    virtual void startup() = 0;
    virtual void shutdown() = 0;

    virtual bool isReplicaMgr() const { return false; }

    void initVectorIndexObject();

    bool canMergeForVectorIndex(
        const StorageMetadataPtr & metadata_snapshot, const DataPartPtr & left, const DataPartPtr & right, PreformattedMessage & out_reason);

    /// record the part without mutation level is indexing vector index, part in this set will not be merged
    scope_guard getPartIndexLock(const String & part_name, const String & /*vec_index_name*/)
    {
        addPartToIndexing(part_name);
        return [this, part_name] { removePartFromIndexing(part_name); };
    }
    void addPartToIndexing(const String & part_name)
    {
        String part_name_no_mutation = VectorIndex::cutMutVer(part_name);
        std::lock_guard lock(currently_vector_indexing_parts_mutex);
        if (currently_vector_indexing_parts_without_mutation.insert(part_name_no_mutation).second)
            LOG_DEBUG(log, "Part {} is added to currently_vector_indexing_parts_without_mutation", part_name);
    }
    void removePartFromIndexing(const String & part_name)
    {
        String part_name_no_mutation = VectorIndex::cutMutVer(part_name);
        std::lock_guard lock(currently_vector_indexing_parts_mutex);
        currently_vector_indexing_parts_without_mutation.erase(part_name_no_mutation);
    }
    bool containsPartInIndexing(const String & part_name) const
    {
        String part_name_no_mutation = VectorIndex::cutMutVer(part_name);
        std::lock_guard lock(currently_vector_indexing_parts_mutex);
        return currently_vector_indexing_parts_without_mutation.count(part_name_no_mutation) > 0;
    }
    /// wait for all parts indexing vector index to finish
    void waitForBuildingVectorIndices();

    /// According old_vec_indices and new_vec_indices, start a job to add or remove vector index
    void startVectorIndexJob(const VIDescriptions & old_vec_indices, const VIDescriptions & new_vec_indices);

    void addVectorIndex(const VIDescription & vec_desc, bool need_init = true)
    {
        auto vector_index_data = std::make_shared<VectorIndexObject>(data, vec_desc, isReplicaMgr());
        if (need_init)
            vector_index_data->init();
        std::lock_guard lock(vector_index_object_map_mutex);
        vector_index_object_map.emplace(vec_desc.name, vector_index_data);
    }

    void dropVectorIndex(const String & vec_index_name)
    {
        std::lock_guard lock(vector_index_object_map_mutex);
        if (auto it = vector_index_object_map.find(vec_index_name); it != vector_index_object_map.end())
        {
            auto old_vector_index_data = it->second;
            vector_index_object_map.erase(it);
            old_vector_index_data->drop();
        }
    }

    /// Has at least one vector index
    bool hasAnyVectorIndex() const
    {
        std::lock_guard lock(vector_index_object_map_mutex);
        return vector_index_object_map.size() > 0;
    }

    const VectorIndexObjectPtr getVectorIndexObject(const String & vec_index_name) const;

    /// Check whether the cache need to be deleted according to the part to which the cache belongs.
    bool needClearVectorIndexCache(
        const DataPartPtr & part,
        const StorageMetadataPtr & metadata_snapshot,
        const VectorIndex::CachedSegmentKey & cache_key,
        const VIDescription & vec_desc) const;
    
    /// Clear the vector index cache and file according to the part to which the cache belongs.
    void clearCachedVectorIndex(const DataPartsVector & parts, bool force = true);

    /// Load vector indices to memory, map key is part_name, value are vector indices in this part to load
    void loadVectorIndices(std::unordered_map<String, std::unordered_set<String>> & vector_indices);

    /// Remove loaded vector indices from memory
    static void abortLoadVectorIndex(std::vector<VectorIndex::CachedSegmentKey> & loaded_keys);

    /// Schedule a job to remove vector index cache and file
    void scheduleRemoveVectorIndexCacheJob(const StorageMetadataPtr & metadata_snapshot);

    /// Check backgroud pool size for vector index if new log entry is allowed.
    /// True if allowed to select part for build vector index.
    bool allowToBuildVI(const bool slow_mode, const size_t builds_count_in_queue) const;

    /// Select a part without vector index to build vector index
    /// In multiple vectors case, choose a part without a vector index in metadata.
    VIEntryPtr selectPartToBuildVI(
        const StorageMetadataPtr & metadata_snapshot,
        VectorIndicesMgr & vi_manager,
        bool select_slow_mode_part);

    /// throw exception when check can build failed
    void checkPartCanBuildVI(MergeTreeDataPartPtr data_part, const VIDescription & vec_desc);

    /// handle build index task
    VectorIndex::SegmentBuiltStatus buildVI(const VIContextPtr ctx);

    /// Prepare the context for building vector index
    VIContextPtr prepareBuildVIContext(
        const StorageMetadataPtr & metadata_snapshot,
        const String & part_name,
        const String & vector_index_name,
        bool slow_mode);
    
    /// Find furture part and do some checks before move vector index files.
    /// Used for first build or retry to move if first build failed to move.
    VectorIndex::SegmentBuiltStatus TryMoveVIFiles(const VIContextPtr ctx);

    /** Is used to cancel all index builds. On cancel() call all currently running actions will throw exception soon.
      * All new attempts to start a vector index build will throw an exception until all 'LockHolder' objects will be destroyed.
      */
    ActionBlocker builds_blocker;
protected:
    VectorIndicesMgr(MergeTreeData & data_)
    : data(data_)
    , log(getLogger(data.getLogName() + " (VectorIndicesMgr)"))
    {
    }

    /// MYSCALE_INTERNAL_CODE_BEGIN
    /// Clear the vector index cache and file on the Nvme disk when storage startup.
    void clearVectorNvmeCache(std::unordered_map<String, std::unordered_set<String>> preload_indices = {}) const;
    /// MYSCALE_INTERNAL_CODE_END

    VectorIndex::SegmentBuiltStatus buildVIForOnePart(VIContextPtr ctx);

    /// Move build vector index files from temporary directory to data part directory, and apply lightweight delete if needed.
    /// And finally write vector index checksums file.
    void moveVIFilesToFuturePartAndCache(
        const MergeTreeDataPartPtr & dest_part,
        DiskPtr disk,
        VectorIndex::CachedSegmentPtr build_index,
        const String & vector_tmp_relative_path, 
        const VIDescription & vec_index_desc,
        const std::shared_ptr<MergeTreeDataPartChecksums> & checksums);

    bool isSlowModePart(const MergeTreeDataPartPtr & part, const String & index_name)
    {
        /// Smaller part built with single vector index is also treated as slow mode.
        auto vi_seg = part->segments_mgr->getSegment(index_name);
        
        return vi_seg->isDecoupled()|| part->rows_count < data.getSettings()->max_rows_for_slow_mode_single_vector_index_build;
    }

    MergeTreeData & data;
    time_t last_cache_check_time = 0;
    mutable std::mutex vector_index_object_map_mutex;
    VectorIndexObjectMap vector_index_object_map;

    /// Mutex for currently_vector_indexing_parts_without_mutation
    mutable std::mutex currently_vector_indexing_parts_mutex;
    std::set<String> currently_vector_indexing_parts_without_mutation;

    LoggerPtr log;

};

}
