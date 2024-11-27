#include <stdexcept>

#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <IO/HashingReadBuffer.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Common/ActionBlocker.h>
#include <Common/ErrorCodes.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils.h>

#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/VIException.h>
#include <VectorIndex/Common/VIPartReader.h>
#include <VectorIndex/Common/SegmentStatus.h>
#include <VectorIndex/Common/VectorIndicesMgr.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <VectorIndex/Storages/VIEntry.h>
#include <VectorIndex/Utils/VIUtils.h>

namespace ProfileEvents
{
extern const Event VectorIndexBuildFailEvents;
}

namespace CurrentMetrics
{
extern const Metric BackgroundVectorIndexPoolTask;
extern const Metric BackgroundSlowModeVectorIndexPoolTask;
}

namespace DB
{
using namespace VectorIndex;

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int INCORRECT_DATA;
    extern const int VECTOR_INDEX_BUILD_MEMORY_TOO_LARGE;
    extern const int VECTOR_INDEX_BUILD_MEMORY_INSUFFICIENT;
    extern const int DEADLOCK_AVOIDED;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int NO_SUCH_DATA_PART;
    extern const int VECTOR_INDEX_ALREADY_EXISTS;
}

void VectorIndicesMgr::initVectorIndexObject()
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    auto vector_indices = metadata_snapshot->getVectorIndices();
    for (const auto & vec_index : vector_indices)
        addVectorIndex(vec_index, false);
}


void VectorIndicesMgr::clearCachedVectorIndex(const DataPartsVector & parts, bool force)
{
    StorageMetadataPtr meta_snapshot = data.getInMemoryMetadataPtr();
    if (meta_snapshot->getVectorIndices().empty())
        return;

    for (const auto & part : parts)
    {
        /// check if table is shutdown
        if (part->storage.isShutdown())
            force = true;
        for (const auto & vec_index_desc : meta_snapshot->getVectorIndices())
        {
            auto vi_seg = part->segments_mgr->getSegment(vec_index_desc.name);
            if (vi_seg)
            {
                if (force)
                {
                    vi_seg->removeCache();
                }
                else
                {
                    /// check if has active part contain this part
                    auto active_contain_part = data.getActiveContainingPart(part->name);
                    if (!active_contain_part)
                    {
                        vi_seg->removeCache();
                        continue;
                    }
                    ///  check if the active part contain the part in cache
                    for (const auto & cache_key : vi_seg->getCachedSegmentKeys())
                    {
                        bool clear_cache
                            = needClearVectorIndexCache(active_contain_part, meta_snapshot, cache_key, vec_index_desc);
                        if (clear_cache)
                            VectorIndex::VICacheManager::removeFromCache(cache_key);
                    }
                }
            }
        }
    }
}

bool VectorIndicesMgr::canMergeForVectorIndex(
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartPtr & left,
    const DataPartPtr & right,
    PreformattedMessage & out_reason)
{
    /// No need to check if there is no vector index on the table.
    if (!metadata_snapshot->hasVectorIndices())
        return true;
    LOG_DEBUG(left->storage.log, "Check if parts {} and {} can be merged for vector index", left->name, right->name);
    /// Check if part is building vector index
    if (containsPartInIndexing(left->name) || containsPartInIndexing(right->name))
    {
        out_reason = PreformattedMessage::create("source part {} or {} is currently building vector index", left->name, right->name);
        return false;
    }

    /// Check if part contains merged vector index
    for (const auto & vec_desc : metadata_snapshot->getVectorIndices())
    {
        auto left_vi_seg = left->segments_mgr->getSegment(vec_desc.name);
        auto right_vi_seg = right->segments_mgr->getSegment(vec_desc.name);
        if (!BaseSegment::canMergeForSegs(left_vi_seg, right_vi_seg))
        {
            out_reason = PreformattedMessage::create("source part {} or {} doesn't contain the same built vector index", left->name, right->name);
            return false;
        }
    }
    return true;
}

void VectorIndicesMgr::waitForBuildingVectorIndices()
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    for (const auto & vec_desc : metadata_snapshot->getVectorIndices())
    {
        auto vi_obj = getVectorIndexObject(vec_desc.name);
        vi_obj->waitBuildFinish();
    }
}

void VectorIndicesMgr::startVectorIndexJob(const VIDescriptions & old_vec_indices, const VIDescriptions & new_vec_indices)
{
    auto table_context = data.getContext();
    auto storage_id = data.getStorageID();
    /// Compare old and new vector_indices to determine add or drop vector index
    /// Support multiple vector indices
    /// Check drop vector index
    for (const auto & old_vec_index : old_vec_indices)
    {
        if (new_vec_indices.has(old_vec_index))
            continue;

        dropVectorIndex(old_vec_index.name);
        VIEventLog::addEventLog(
            table_context,
            storage_id.database_name,
            storage_id.table_name,
            old_vec_index.name,
            "",
            "",
            VIEventLogElement::DEFINITION_DROPPED);
    }

    /// Check create vector index
    for (const auto & new_vec_index : new_vec_indices)
    {
        if (old_vec_indices.has(new_vec_index))
            continue;

        addVectorIndex(new_vec_index);
        VIEventLog::addEventLog(
            table_context,
            storage_id.database_name,
            storage_id.table_name,
            new_vec_index.name,
            "",
            "",
            VIEventLogElement::DEFINITION_CREATED);
    }
}

const VectorIndexObjectPtr VectorIndicesMgr::getVectorIndexObject(const String & vec_index_name) const
{
    std::lock_guard lock(vector_index_object_map_mutex);
    if (auto it = vector_index_object_map.find(vec_index_name); it != vector_index_object_map.end())
        return it->second;
    
    throw Exception(ErrorCodes::INVALID_VECTOR_INDEX, "Vector index {} not found in table {}", vec_index_name, data.getStorageID().table_name);
}

bool VectorIndicesMgr::needClearVectorIndexCache(
    const DataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const CachedSegmentKey & cache_key,
    const VIDescription & vec_desc) const
{
    if (!part)
        return true;

    /// Support multiple vector indices
    /// The active part contains the part in the cache_key.
    /// First make sure the vector index info in cache key is same with defined in metadata. If not exists or not same, remove cache and file.
    String index_name = cache_key.vector_index_name;

    /// Check if vector index has been dropped.
    if (!metadata_snapshot->getVectorIndices().has(index_name))
        return true;

    bool existed = false;

    /// Check vector index in cache is same as metadata
    for (const auto & vec_index_desc : metadata_snapshot->getVectorIndices())
    {
        /// Find description for this vector index.
        if (index_name != vec_index_desc.name)
            continue;

        /// Check if the description is same with the cache key.
        /// Check column name
        if (vec_index_desc != vec_desc)
        {
            LOG_WARNING(
                log,
                "Vector index {} description in metadata is different with cache key, will remove cache and file. it's may be a bug.",
                vec_index_desc.name);
            return true;
        }

        /// Check if the part has the vector index.
        auto vi_seg = part->segments_mgr->getSegment(index_name);
        if (vi_seg && vi_seg->containCachedSegmentKey(cache_key))
            existed = true;

        /// Already found the vector index in metadata with the same name as cache key.
        break;
    }

    return !existed;
}

/// MYSCALE_INTERNAL_CODE_BEGIN
void VectorIndicesMgr::clearVectorNvmeCache(std::unordered_map<String, std::unordered_set<String>> preload_indices) const
{
    auto vector_nvme_cache_folder
        = fs::path(data.getContext()->getVectorIndexCachePath()) / getPartRelativePath(data.getRelativeDataPath());
    std::vector<String> will_remove_cache_folder;
    if (fs::exists(vector_nvme_cache_folder))
    {
        try
        {
            for (const auto & entry : fs::directory_iterator(vector_nvme_cache_folder))
            {
                if (entry.is_directory())
                {
                    String cache_folder = entry.path().filename();
                    std::vector<String> tokens;
                    boost::split(tokens, cache_folder, boost::is_any_of("-"));
                    if (tokens.size() != 2 && tokens.size() != 3)
                    {
                        LOG_DEBUG(log, "Remove Illegal nvme cache folder: {}", cache_folder);
                        will_remove_cache_folder.emplace_back(cache_folder);
                        continue;
                    }

                    String part_name = tokens[0];
                    String index_name = tokens[1];
                    auto part = data.getActiveContainingPart(part_name);
                    if (!part)
                    {
                        LOG_DEBUG(
                            log,
                            "Does not has active part contain old part {}, Remove Illegal nvme cache folder: {}",
                            part_name,
                            cache_folder);
                        will_remove_cache_folder.emplace_back(cache_folder);
                        continue;
                    }

                    String active_part_name = part->info.getPartNameWithoutMutation();
                    if (!preload_indices.contains(active_part_name) || !preload_indices[active_part_name].contains(index_name))
                    {
                        /// This cache not in proload indices set, will remove
                        LOG_DEBUG(log, "Cache folder {} isn't in proload indices set, will remove", cache_folder);
                        will_remove_cache_folder.emplace_back(cache_folder);
                        continue;
                    }

                    if (active_part_name != part_name)
                    {
                        auto vi_state = part->segments_mgr->getSegmentStatus(index_name);
                        if (vi_state == SegmentStatus::BUILT)
                        {
                            LOG_DEBUG(
                                log,
                                "Active contain part {} already has single vector index, will remove cache folder {}",
                                part->name,
                                cache_folder);
                            will_remove_cache_folder.emplace_back(cache_folder);
                            continue;
                        }
                    }
                }
                else
                {
                    LOG_DEBUG(log, "Remove Illegal nvme cache folder: {}", entry.path().filename());
                    will_remove_cache_folder.emplace_back(entry.path().filename());
                    continue;
                }
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Clear Nvme Cache error, Will Clear all cache.");
            fs::remove_all(vector_nvme_cache_folder);
            return;
        }
    }

    for (const auto & remove_file : will_remove_cache_folder)
    {
        auto path = fs::path(vector_nvme_cache_folder) / remove_file;
        LOG_DEBUG(log, "Remove Index Cache Path: {}", path);
        fs::remove_all(path);
    }
}
/// MYSCALE_INTERNAL_CODE_END

void VectorIndicesMgr::loadVectorIndices(std::unordered_map<String, std::unordered_set<String>> & vector_indices)
{
    auto metadata = data.getInMemoryMetadata();

    std::unordered_map<String, VIDescription> v_index_map;
    for (const auto & v_index : metadata.getVectorIndices())
    {
        v_index_map.try_emplace(v_index.name, v_index);
    }

    std::unordered_set<String> valid_vidx;
    std::unordered_set<String> invalid_vidx;
    std::unordered_set<String> reuse_path;
    std::vector<CachedSegmentKey> loaded_keys;

    size_t dim = 0;

    for (const auto & data_part : data.getDataPartsVectorForInternalUsage())
    {
        String part_name = data_part->info.getPartNameWithoutMutation();

        if (!vector_indices.contains(part_name))
            continue;

        for (const auto & vidx_name : vector_indices[part_name])
        {
            if (invalid_vidx.contains(vidx_name) || !v_index_map.contains(vidx_name))
                continue;

            auto v_index = v_index_map[vidx_name];

            if (!valid_vidx.contains(vidx_name))
            {
                /// check vector index metadata
                auto col_and_type = metadata.getColumns().getAllPhysical().tryGetByName(v_index.column);
                if (!col_and_type)
                {
                    invalid_vidx.insert(vidx_name);
                    LOG_ERROR(log, "Vector index column {} not found in metadata", v_index.column);
                    continue;
                }

                if (v_index.vector_search_type == Search::DataType::FloatVector)
                {
                    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(col_and_type->getTypeInStorage().get());
                    if (!array_type)
                    {
                        invalid_vidx.insert(vidx_name);
                        LOG_ERROR(log, "Float32 Vector index column {} type is not array", v_index.column);
                        continue;
                    }
                    dim = metadata.getConstraints().getArrayLengthByColumnName(v_index.column).first;
                }
                else if (v_index.vector_search_type == Search::DataType::BinaryVector)
                {
                    const DataTypeFixedString * fixed_string_type
                        = typeid_cast<const DataTypeFixedString *>(col_and_type->getTypeInStorage().get());
                    if (!fixed_string_type)
                    {
                        invalid_vidx.insert(vidx_name);
                        LOG_ERROR(log, "Binary Vector index column {} type is not FixedString(N)", v_index.column);
                        continue;
                    }
                    dim = fixed_string_type->getN() * 8;
                }

                if (dim == 0)
                {
                    invalid_vidx.insert(vidx_name);
                    LOG_ERROR(log, "Wrong dimension: 0 for column {}.", v_index.column);
                    continue;
                }

                valid_vidx.insert(vidx_name);
            }

            if (data.isShutdown())
                abortLoadVectorIndex(loaded_keys);

            auto vi_seg = data_part->segments_mgr->getSegment(v_index.name);
            if (vi_seg && vi_seg->loadVI())
            {
                for (const auto & cache_key : vi_seg->getCachedSegmentKeys())
                {
                    LOG_DEBUG(log, "Loaded vector index {} in {}", v_index.name, data_part->name);
                    loaded_keys.emplace_back(cache_key);
                }
            }
            else
            {
                LOG_ERROR(log, "Failed to load vector index {} in part {}.", v_index.name, data_part->name);
            }
        }
    }

    if (data.isShutdown())
        abortLoadVectorIndex(loaded_keys);
}

void VectorIndicesMgr::abortLoadVectorIndex(std::vector<CachedSegmentKey> & loaded_keys)
{
    for (const auto & key : loaded_keys)
        VICacheManager::removeFromCache(key);
}

void VectorIndicesMgr::scheduleRemoveVectorIndexCacheJob(const StorageMetadataPtr & metadata_snapshot)
{
    auto now = time(nullptr);
    if (last_cache_check_time == 0)
        last_cache_check_time = now;

    /// we don't want to check vector index too frequent.
    if (now - last_cache_check_time < data.getSettings()->vector_index_cache_recheck_interval_seconds.totalSeconds())
        return;

    /// Update last_cache_check
    last_cache_check_time = now;

    ///check existing parts to see if any cached vector index need cleaning
    auto cached_item_list = VICacheManager::getAllItems();

    /// getRelativeDataPath() contains '/' in the tail, but table_path in cache key doesn't have.
    std::string relative_data_path = fs::path(data.getRelativeDataPath()).parent_path().string();
    for (const auto & cache_item : cached_item_list)
    {
        const auto & cache_key = cache_item.first;
        const auto vec_desc = *cache_item.second;
        /// not this table
        if (cache_key.table_path.find(relative_data_path) == std::string::npos)
            continue;

        /// skip restored decouple owner parts cache key
        if (startsWith(cache_key.part_name_no_mutation, DECOUPLE_OWNER_PARTS_RESTORE_PREFIX))
            continue;

        /// Need to check part no matter exists or not exists.
        MergeTreeDataPartPtr part = data.getActiveContainingPart(cache_key.part_name_no_mutation);
        bool clear_cache = needClearVectorIndexCache(part, metadata_snapshot, cache_key, vec_desc);

        /// don't need to check vi params, already checked in needClearVectorIndexCache
        if (clear_cache)
        {
            LOG_DEBUG(log, "Find not existed cache, remove it: {}", cache_key.toString());
            VICacheManager::removeFromCache(cache_key);
        }
    }
}


bool VectorIndicesMgr::allowToBuildVI(const bool slow_mode, const size_t builds_count_in_queue) const
{
    ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(data.getContext()->getConfigRef());
    size_t occupied = 0;

    /// Allow build vector index only if there are enough threads.
    if (slow_mode)
    {
        /// Check slow mode build vector index log entry in queue
        if (builds_count_in_queue >= server_settings.background_slow_mode_vector_pool_size)
        {
            LOG_DEBUG(
                log,
                "[allowToBuildVI] Number of queued build vector index enties ({})"
                " is greater than background_slow_mode_vector_pool_size ({}), so won't select new parts to build vector index",
                builds_count_in_queue,
                server_settings.background_slow_mode_vector_pool_size);
            return false;
        }

        occupied = CurrentMetrics::values[CurrentMetrics::BackgroundSlowModeVectorIndexPoolTask].load(std::memory_order_relaxed);

        if (occupied < server_settings.background_slow_mode_vector_pool_size)
            return true;
    }
    else
    {
        /// Check build vector index log entry in queue
        if (builds_count_in_queue >= server_settings.background_vector_pool_size)
        {
            LOG_DEBUG(
                log,
                "[allowToBuildVI] Number of queued build vector index enties ({})"
                " is greater than background_vector_pool_size ({}), so won't select new parts to build vector index",
                builds_count_in_queue,
                server_settings.background_vector_pool_size);
            return false;
        }

        occupied = CurrentMetrics::values[CurrentMetrics::BackgroundVectorIndexPoolTask].load(std::memory_order_relaxed);

        if (occupied < server_settings.background_vector_pool_size)
            return true;
    }

    return false;
}

VIEntryPtr VectorIndicesMgr::selectPartToBuildVI(
    const StorageMetadataPtr & metadata_snapshot,
    VectorIndicesMgr & vi_manager,
    bool select_slow_mode_part)
{
    if (!metadata_snapshot->hasVectorIndices())
        return {};

    for (const auto & part : data.getDataPartsForInternalUsage())
    {
        /// TODO: Support atomic insert, avoid to select active data parts in an uncommited transaction.

        /// Skip empty part
        if (part->isEmpty())
            continue;

        /// Since building vector index doesn't block mutation on the part, the new part need to check if any covered part is building vindex.
        /// The new part already blocked merge to select it, hence it's safe here. all_1_1_0 can avoid index build selection for future parts all_1_1_0_*
        if (vi_manager.containsPartInIndexing(part->name))
        {
            LOG_DEBUG(log, "Skip to select part {} build vector index due to part is building vector index", part->name);
            continue;
        }

        /// ReplicatedMergeTree depends on virtual_parts for merge, MergeTree depends on currently_merging_mutating_parts
        if (data.partIsAssignedToBackgroundOperation(part))
        {
            LOG_DEBUG(log, "Skip to select part {} build vector index due to part is assigned to background operation", part->name);
            continue;
        }

        /// Support multiple vector indices
        for (const auto & vec_index : metadata_snapshot->getVectorIndices())
        {
            auto vi_seg = part->segments_mgr->getSegment(vec_index.name);

            if (!vi_seg || !vi_seg->canBuildIndex())
                continue;

            /// Part doesn't contain this vector index, need to build.
            if (select_slow_mode_part)
            {
                if (!isSlowModePart(part, vec_index.name))
                    continue;

                LOG_DEBUG(log, "Select slow mode part name: {} for vector index name: {}", part->name, vec_index.name);
                return std::make_shared<VIEntry>(
                    part->name,
                    vec_index.name,
                    data,
                    isReplicaMgr() ? scope_guard({}) : vi_manager.getPartIndexLock(part->name, vec_index.name));
            }
            else /// normal fast mode
            {
                if (isSlowModePart(part, vec_index.name))
                    continue;

                LOG_DEBUG(log, "Select part name: {} for vector index name: {}", part->name, vec_index.name);
                return std::make_shared<VIEntry>(
                    part->name,
                    vec_index.name,
                    data,
                    isReplicaMgr() ? scope_guard({}) : vi_manager.getPartIndexLock(part->name, vec_index.name));
            }
        }
    }

    return {};
}

void VectorIndicesMgr::checkPartCanBuildVI(MergeTreeDataPartPtr data_part, const VIDescription & vec_desc)
{
    if (!data_part)
        throw Exception(ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART, "No active part contain to build index, skip build index.");

    if (data_part->isEmpty())
        throw Exception(ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART, "Empty part, vector index will not be created.");

    auto metadata_snapshot = data_part->storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot || !metadata_snapshot->getVectorIndices().has(vec_desc.name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "There is no definition of {} in the table definition, skip build index.", vec_desc.name);

    if (!data_part->getColumns().contains(vec_desc.column))
    {
        LOG_WARNING(
            log,
            "No column {} in part {} to build index, If the column is newly added and the Materialize operation has not been performed "
            "yet, please perform the Materialize operation as soon as possible.",
            vec_desc.column,
            data_part->name);
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "No column {} in part {} to build index, maybe need Materialize operation.",
            vec_desc.column,
            data_part->name);
    }

    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(data_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");

    if (data_part->segments_mgr->getSegmentStatus(vec_desc.name) == VectorIndex::SegmentStatus::CANCELLED)
        throw Exception(ErrorCodes::ABORTED, "Cancel build vector index {} for part {}.", vec_desc.name, data_part->name);

    if (!data_part->segments_mgr->getSegment(vec_desc.name))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column index {} for part {} does not exits, it's a bug.", vec_desc.name, data_part->name);

    /// If we already have this vector index in this part, we do not need to do anything.
    if (data_part->segments_mgr->getSegmentStatus(vec_desc.name) == VectorIndex::SegmentStatus::BUILT)
        throw Exception(
            ErrorCodes::VECTOR_INDEX_ALREADY_EXISTS,
            "No need to build vector index {} for part {}, because the part already has ready vector index.",
            vec_desc.name,
            data_part->name);
}

VIContextPtr VectorIndicesMgr::prepareBuildVIContext(
    const StorageMetadataPtr & metadata_snapshot,
    const String & part_name,
    const String & vector_index_name,
    bool slow_mode)
{
    auto ctx = std::make_shared<VIContext>();
    if (part_name.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Part name in index task is empty, build error.");

    /// Get active part containing current part to build vector index
    /// No need to check part name, mutations are not blocked by build vector index.
    MergeTreeDataPartPtr part = data.getActiveContainingPart(part_name);
    if (isReplicaMgr())
    {
        auto target_vector_index_part_info = MergeTreePartInfo::fromPartName(part_name, data.format_version);
        if (!part || !part->info.isFromSamePart(target_vector_index_part_info))
            throw Exception(
                ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART,
                "No Active DataPart or Active part is inconsistent with the part in vector task entry part name {}.",
                part->name);
    }

    bool has_vector_index_desc = false;
    for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
    {
        /// Support multiple vector indices
        if (vec_index_desc.name != vector_index_name)
            continue;

        has_vector_index_desc = true;
        checkPartCanBuildVI(part, vec_index_desc);
        ctx->vec_index_desc = vec_index_desc;
        break;
    }

    if (!has_vector_index_desc)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "There is no definition of {} in the table definition, skip build index.", vector_index_name);

    const DataPartStorageOnDiskBase * part_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
    auto disk = part_storage->getDisk();

    ctx->source_part = part;
    ctx->metadata_snapshot = metadata_snapshot;
    ctx->part_name = part_name;
    ctx->vector_index_name = vector_index_name;
    ctx->slow_mode = slow_mode;
    ctx->vi_status = part->segments_mgr->getSegment(vector_index_name)->getSegmentStatus();
    ctx->builds_blocker = &builds_blocker;

    /// Create temporary directory to store built vector index files.
    /// The director name starts with prefix "vector_tmp_" + vector_index_name + part_name w/o_mutation
    /// e.g. part all_1_1_0_5 vector index v1: vector_tmp_v1_all_1_1_0
    String part_name_prefix = part->info.getPartNameWithoutMutation();
    String tmp_vector_index_dir = "vector_tmp_" + vector_index_name + "_" + part_name_prefix;
    String vector_tmp_relative_path = data.getRelativeDataPath() + tmp_vector_index_dir + "/";
    String vector_tmp_full_path = data.getFullPathOnDisk(disk) + tmp_vector_index_dir + "/";

    ctx->log = getLogger("VITask[" + part_name_prefix + "]");
    ctx->temporary_directory_lock = data.getTemporaryPartDirectoryHolder(tmp_vector_index_dir);
    ctx->vector_tmp_full_path = vector_tmp_full_path;
    ctx->vector_tmp_relative_path = vector_tmp_relative_path;
    ctx->index_build_memory_lock = std::make_unique<VectorIndex::VIBuildMemoryUsageHelper>();

    /// will init in index serialize
    ctx->vector_index_checksum = nullptr;

    ctx->build_cancel_callback = [source_part = part,
                                  vi_status = ctx->vi_status,
                                  builds_blocker_ = ctx->builds_blocker,
                                  vec_index_desc = ctx->vec_index_desc]() -> bool
    {
        if (source_part->storage.isShutdown() || builds_blocker_->isCancelled())
            return true;

        if (vi_status->getStatus() == VectorIndex::SegmentStatus::CANCELLED)
            return true;

        /// Check vector index exists in table's metadata
        auto & latest_vec_indices = source_part->storage.getInMemoryMetadataPtr()->getVectorIndices();
        if (latest_vec_indices.empty() || !latest_vec_indices.has(vec_index_desc))
            return true;

        MergeTreeDataPartPtr last_active_contain_part = source_part->storage.getActiveContainingPart(source_part->name);
        auto target_vector_index_part_info = MergeTreePartInfo::fromPartName(source_part->name, source_part->storage.format_version);
        if (!last_active_contain_part || !last_active_contain_part->info.isFromSamePart(target_vector_index_part_info))
            return true;

        return false;
    };

    ctx->clean_tmp_folder = [log_ = ctx->log, disk](const String & vector_tmp_path) mutable
    {
        /// Remove temporay directory
        if (disk->exists(vector_tmp_path))
            disk->removeRecursive(vector_tmp_path);
        else
            LOG_DEBUG(log_, "Remove vector_tmp_relative_path doesn't exist {}", vector_tmp_path);
    };

    ctx->write_event_log = [part_ = ctx->source_part, vector_index_name_ = ctx->vector_index_name](
                               VIEventLogElement::Type event_type, int error_code, const String & error_message) mutable
    {
        VIEventLog::addEventLog(
            Context::getGlobalContextInstance(), part_, vector_index_name_, event_type, "", ExecutionStatus(error_code, error_message));
    };

    return ctx;
}

/// only build index and serialize vector index
VectorIndex::SegmentBuiltStatus VectorIndicesMgr::buildVI(const VIContextPtr ctx)
{
    try
    {
        LOG_INFO(log, "Build vector index {} for part {} start.", ctx->vector_index_name, ctx->source_part->name);
        if (ctx->source_part->segments_mgr->getSegmentStatus(ctx->vector_index_name) == VectorIndex::SegmentStatus::BUILT)
        {
            LOG_WARNING(log, "Vector index has been created successfully and does not need to be built again. it's a bug.");
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};
        }

        MergeTreeDataPartPtr future_part = data.getActiveContainingPart(ctx->source_part->name);
        if (future_part)
        {
            if (future_part->segments_mgr->getSegmentStatus(ctx->vector_index_name) == VectorIndex::SegmentStatus::BUILT)
            {
                LOG_WARNING(
                    log,
                    "Vector index has been created successfully in part {} and does not need to be built again for part {}.",
                    future_part->name,
                    ctx->source_part->name);
                return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};
            }
        }
        ctx->vi_status->setStatus(VectorIndex::SegmentStatus::BUILDING);
        ctx->watch.restart();

        VectorIndex::SegmentBuiltStatus status = buildVIForOnePart(ctx);

        if (status.getStatus() == VectorIndex::SegmentBuiltStatus::SUCCESS)
            LOG_INFO(
                log,
                "Build vector index and serialize vector index for part {} with the index name {} completed in {} sec, slow_mode: {}, will "
                "move index later",
                ctx->part_name,
                ctx->vector_index_name,
                ctx->watch.elapsedSeconds(),
                ctx->slow_mode);

        return status;
    }
    catch (Exception & e)
    {
        LOG_WARNING(
            log,
            "Vector index build task for part {} index name {} error {}: {}",
            ctx->source_part->name,
            ctx->vector_index_name,
            e.code(),
            e.message());

        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }
}

VectorIndex::SegmentBuiltStatus VectorIndicesMgr::buildVIForOnePart(VIContextPtr ctx)
{
    if (ctx->build_cancel_callback())
        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_CANCEL};


    /// dimension is checked in constructor of Segment
    /// TODO: Build all the vector indices for this part?
    /// only one column to build vector index, using a large dimension as default value.
    /// below is a horror to test whether a moved part need to rebuild its index. basially is reads from vector_index_ready if there is one,
    /// creates a pesudo vector index using parameters recorded in vector_index_ready and compare with the new index to see if they are the same.
    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(ctx->source_part->getDataPartStoragePtr().get());

    if (part_storage == nullptr)
        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_FAIL};

    auto disk = part_storage->getDisk();

    /// Since the vector index is stored in a temporary directory, add check for it too.
    /// Loop through the relative_data_path to check if any directory with vector_tmp_<vector_index_name>_<part_name_without_mutation> exists
    if (disk->exists(ctx->vector_tmp_relative_path))
    {
        auto index_checksum_file = getVectorIndexChecksumsFileName(ctx->vector_index_name);
        if (disk->exists(ctx->vector_tmp_relative_path + "/" + index_checksum_file))
        {
            ctx->vector_index_checksum = std::make_shared<MergeTreeDataPartChecksums>(
                getVectorIndexChecksums(disk, ctx->vector_index_name, ctx->vector_tmp_relative_path));
            /// [WIP] check if the checksums file is valid?
            LOG_INFO(
                log,
                "The Vector index for part {} in temp path {} is already prepared, skip rebuild vector index.",
                ctx->source_part->name,
                ctx->vector_tmp_relative_path);
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::SUCCESS};
        }
        ctx->clean_tmp_folder(ctx->vector_tmp_relative_path);
    }

    /// Create temp directory before serialize.
    disk->createDirectories(ctx->vector_tmp_relative_path);

    try
    {
        ctx->write_event_log(VIEventLogElement::BUILD_START, 0, "");
        auto vi_seg = ctx->source_part->segments_mgr->getSegment(ctx->vector_index_name);

        /// variant index_ptr
        CachedSegmentPtr search_index;

        VECTOR_INDEX_EXCEPTION_ADAPT(
            search_index = vi_seg->buildVI(ctx->slow_mode, *(ctx->index_build_memory_lock), ctx->build_cancel_callback), "Build Index")


        if (!ctx->build_cancel_callback())
        {
            VECTOR_INDEX_EXCEPTION_ADAPT(
                vi_seg->serialize(disk, search_index, ctx->vector_tmp_full_path, ctx->vector_index_checksum, *ctx->index_build_memory_lock),
                "Serialize Index")

            LOG_DEBUG(ctx->log, "Build index and Serialize Vector Index finish, in {} sec.", ctx->watch.elapsedSeconds());

            ctx->build_index = search_index;
        }
        else
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_CANCEL};
    }
    catch (Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);

        /// check memory limit touch max retry count or instance memory limit can't build index or other error
        if ((e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED || ++ctx->failed_count >= ctx->maxBuildRetryCount)
            && (e.code() != ErrorCodes::VECTOR_INDEX_BUILD_MEMORY_INSUFFICIENT))
        {
            LOG_ERROR(
                log,
                "Vector Index build task for part {} index name {} failed: {}: {}",
                ctx->source_part->name,
                ctx->vector_index_name,
                e.code(),
                e.message());

            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_FAIL, e.code(), e.message()};
        }

        /// retry build for MEMORY_LIMIT_EXCEEDED case
        LOG_WARNING(
            log,
            "Cannot build vector index {} for part {} for now due to {}.",
            ctx->vector_index_name,
            ctx->source_part->name,
            ErrorCodes::getName(e.code()));

        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_RETRY};
    }

    return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::SUCCESS};
}

/// move vector index to latest active part
VectorIndex::SegmentBuiltStatus VectorIndicesMgr::TryMoveVIFiles(const VIContextPtr ctx)
{
    LOG_INFO(log, "Try to move vector index {} file to the latest active part {}.", ctx->vector_index_name, ctx->source_part->name);
    if (ctx->build_cancel_callback())
    {
        LOG_WARNING(ctx->log, "Cancel move vector index {}, because cancel build index.", ctx->vector_index_name);
        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_CANCEL};
    }
    /// Find an active same future part to store the vector index files.
    MergeTreeDataPartPtr future_part = nullptr;
    if (ctx->source_part->getState() == DB::MergeTreeDataPartState::Active)
    {
        future_part = ctx->source_part;
    }
    else
    {
        /// Find future active part
        future_part = data.getActiveContainingPart(ctx->source_part->name);
        if (!future_part)
        {
            LOG_WARNING(log, "Failed to find future part for part {}, build cancel", ctx->part_name);
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};
        }
    }

    /// In replicated case, slow replica may build vector index for merged data part.
    /// Here check the future part has the same prefix name as build part.
    if (!future_part->info.isFromSamePart(ctx->source_part->info))
    {
        LOG_WARNING(
            log,
            "future part '{}' is a merged part not mutated part from part '{}' build vector index, no need to move.",
            future_part->name,
            ctx->source_part->name);
        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};
    }

    /// Fetched part already contains vector vector index
    if (future_part->segments_mgr->getSegmentStatus(ctx->vector_index_name) == SegmentStatus::BUILT)
    {
        LOG_WARNING(log, "Future Part {} already has built vector index. not move vector index.", future_part->name);
        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};
    }

    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(ctx->source_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_FAIL};

    auto disk = part_storage->getDisk();

    try
    {
        LOG_INFO(log, "Will Move Index File to Part {}", future_part->name);
        /// lock part for move build vector index, avoid concurrently mutation
        auto move_index_lock = future_part->segments_mgr->tryLockSegmentsTimed(RWLockImpl::Type::Write, std::chrono::milliseconds(1000));
        /// check part is active
        if (future_part->getState() != MergeTreeDataPartState::Active)
        {
            LOG_WARNING(ctx->log, "Part {} is not in active state when moving index, will retry.", future_part->name);
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_RETRY};
        }

        if (future_part->rows_count == 0)
        {
            LOG_WARNING(ctx->log, "Part {} is empty, skip build vector index.", future_part->name);
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_CANCEL};
        }

        /// Finally, move index files to part and apply lightweight delete.
        moveVIFilesToFuturePartAndCache(
            future_part, disk, ctx->build_index, ctx->vector_tmp_relative_path, ctx->vec_index_desc, ctx->vector_index_checksum);
        if (auto vi_seg = future_part->segments_mgr->getSegment(ctx->vector_index_name))
            vi_seg->getSegmentStatus()->setElapsedTime(ctx->watch.elapsedSeconds());
    }
    catch (Exception & e)
    {
        LOG_WARNING(
            ctx->log,
            "Move index file in {} to part {} error {}: {}",
            ctx->vector_tmp_relative_path,
            future_part->name,
            e.code(),
            e.message());

        if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
        {
            LOG_WARNING(log, "Will move vector index files later since future part `{}` is under mutating", future_part->name);
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_RETRY};
        }

        if (ctx->source_part->segments_mgr->getSegmentStatus(ctx->vector_index_name) == SegmentStatus::BUILT)
            return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED};

        return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }

    LOG_INFO(log, "Move Vector Index {} file to Part {} success.", ctx->vector_index_name, future_part->name);
    return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::SUCCESS};
}

void VectorIndicesMgr::moveVIFilesToFuturePartAndCache(
    const MergeTreeDataPartPtr & dest_part,
    DiskPtr disk,
    CachedSegmentPtr build_index,
    const String & vector_tmp_relative_path,
    const VIDescription & vec_index_desc,
    const std::shared_ptr<MergeTreeDataPartChecksums> & checksums)
{
    String vector_index_name = vec_index_desc.name;
    LOG_DEBUG(
        log,
        "Current active part {} with state {} is selected to store vector index {}",
        dest_part->name,
        dest_part->getState(),
        vector_index_name);

    String dest_relative_path = dest_part->getDataPartStorage().getRelativePath();

    auto old_index_files = getVectorIndexFileNamesInChecksums(dest_part->getDataPartStoragePtr(), vec_index_desc.name, false);
    /// Move to current part which is active.
    bool found_vector_file = false;
    Names moved_files;
    try
    {
        /// Put here to avoid concurently drop vector index
        auto lock_part = data.lockParts();

        String index_checksums_file_name = getVectorIndexChecksumsFileName(vector_index_name);
        if (!disk->exists(vector_tmp_relative_path + index_checksums_file_name))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Vector Index {} checksums file does not exists.",
                vector_tmp_relative_path + index_checksums_file_name);

        for (auto it = disk->iterateDirectory(vector_tmp_relative_path); it->isValid(); it->next())
        {
            if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX) || endsWith(it->name(), index_checksums_file_name))
                continue;

            disk->createHardLink(vector_tmp_relative_path + it->name(), dest_relative_path + it->name());

            moved_files.emplace_back(it->name());

            if (!found_vector_file)
                found_vector_file = true;
        }

        /// Finally write the new built vector index checksums file
        LOG_DEBUG(
            log,
            "Move vector index finish, will move checksum files from {} to {}.",
            vector_tmp_relative_path + index_checksums_file_name,
            dest_relative_path + index_checksums_file_name);

        disk->replaceFile(vector_tmp_relative_path + index_checksums_file_name, dest_relative_path + index_checksums_file_name);
    }
    catch (Exception & e)
    {
        LOG_WARNING(
            log,
            "Failed to move built vector index {} files to part {}, will do some clean-up work: {}",
            vector_index_name,
            dest_part->name,
            e.message());

        if (disk->exists(vector_tmp_relative_path))
            disk->removeRecursive(vector_tmp_relative_path);

        if (!moved_files.empty())
            removeVectorIndexFilesFromFileLists(dest_part->getDataPartStoragePtr(), moved_files);

        throw;
    }

    if (!found_vector_file)
    {
        LOG_WARNING(log, "Failed to find any vector index files in directory {}, will remove it", vector_tmp_relative_path);
        if (disk->exists(vector_tmp_relative_path))
            disk->removeRecursive(vector_tmp_relative_path);
        else
            LOG_WARNING(log, "[moveVIFilesToFuturePartAndCache] vector_tmp_relative_path doesn't exist {}", vector_tmp_relative_path);

        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector index file does not exists when move vector index.");
    }

    auto new_vi_seg = createSegment(vec_index_desc, dest_part, checksums);

    /// FIXME: Release move index lock in advance to reduce blocking time
    if (!new_vi_seg->cache(build_index))
        LOG_DEBUG(log, "Load vector index from index ptr error, index may be obtained by fetch index.");

    dest_part->segments_mgr->updateSegment(vector_index_name, new_vi_seg);

    /// Apply lightweight delete bitmap to index's bitmap, no need update lwd bitmap, because cache index already update bitmap
    if (dest_part->hasLightweightDelete())
    {
        LOG_DEBUG(log, "Apply lightweight delete to vector index {} in part {}", vector_index_name, dest_part->name);
        dest_part->onLightweightDelete(vector_index_name);
    }
}

}
