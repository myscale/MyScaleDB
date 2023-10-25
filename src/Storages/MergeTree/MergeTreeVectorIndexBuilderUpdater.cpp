#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/HashingReadBuffer.h>
#include <Interpreters/VectorIndexEventLog.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVectorIndexBuilderUpdater.h>
#include <VectorIndex/Metadata.h>
#include <VectorIndex/PartReader.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/VectorSegmentExecutor.h>
#include <Common/ActionBlocker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>

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

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int VECTOR_INDEX_BUILD_MEMORY_INSUFFICIENT;
}


namespace BuildIndexHelpers
{

    static bool checkOperationIsNotCanceled(ActionBlocker & builds_blocker)
    {
        if (builds_blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Cancelled building vector index");

        return true;
    }

/// Will not set build error in part when vector index is dropped.
static void setPartVectorIndexBuildStatus(const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part, const String & vec_index_name)
{
    /// Avoid to set build error in cases when a new index with the same name is added.
    for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
    {
        /// Find the vector index description from metadata snapshot when build starts.
        if (vec_index_desc.name == vec_index_name)
        {
            /// Check vector index exists in table's latest metadata
            auto & latest_vec_indices = part->storage.getInMemoryMetadataPtr()->getVectorIndices();
            if (latest_vec_indices.has(vec_index_desc))
            {
                /// Set build error to avoid multiple attempts to build vector index for a part
                part->setBuildError(vec_index_desc.name);
            }

            break;
        }
    }
}

}

MergeTreeVectorIndexBuilderUpdater::MergeTreeVectorIndexBuilderUpdater(MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (VectorIndexUpdater)"))
{
    if (startsWith(data.getName(), "Replicated"))
        is_replicated = true;
}

void MergeTreeVectorIndexBuilderUpdater::removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot)
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
    std::list<std::pair<VectorIndex::CacheKey, Search::Parameters>> cached_item_list
        = VectorIndex::VectorSegmentExecutor::getAllCacheNames();

    /// getRelativeDataPath() contains '/' in the tail, but table_path in cache key doesn't have.
    std::string relative_data_path = fs::path(data.getRelativeDataPath()).parent_path().string();
    for (const auto & cache_item : cached_item_list)
    {
        /// not this table
        if (cache_item.first.table_path.find(relative_data_path) == std::string::npos)
            continue;

        const auto cache_key = cache_item.first;
        /// skip restored decouple owner parts cache key
        if (startsWith(cache_key.part_name_no_mutation, DECOUPLE_OWNER_PARTS_RESTORE_PREFIX))
            continue;

        /// Need to check part no matter exists or not exists.
        MergeTreeDataPartPtr part = data.getActiveContainingPart(cache_key.part_name_no_mutation);
        auto [clear_cache, clear_file] = data.needClearVectorIndexCacheAndFile(part, metadata_snapshot, cache_key);

        if (!clear_cache)
        {
            LOG_DEBUG(log, "Find Vector Index in metadata");
            Search::Parameters params = cache_item.second;

            /// Support multiple vector indices
            /// TODO: Further check whether the paramters are the same.
            for (const auto & vec_index_desc : metadata_snapshot->getVectorIndices())
            {
                if (cache_key.vector_index_name != vec_index_desc.name)
                    continue;

                LOG_DEBUG(
                    log,
                    "Params: {}, desc params: {}",
                    VectorIndex::ParametersToString(params),
                    VectorIndex::ParametersToString(VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters)));

                break;
            }
        }

        if (clear_cache)
        {
            LOG_DEBUG(log, "Find not existed cache, remove it: {}", cache_key.toString());
            VectorIndex::VectorSegmentExecutor::removeFromCache(cache_key);

            /// Clear vector files in active part
            if (part && clear_file)
            {
                if (part->containVectorIndex(cache_key.vector_index_name))
                {
                    LOG_DEBUG(log, "Remove files of dropped vector index {} for part {}", cache_key.vector_index_name, part->name);
                    part->removeVectorIndex(cache_key.vector_index_name);
                }
                else if (part->containRowIdsMaps(cache_key.vector_index_name)) /// Decouple part
                {
                    LOG_DEBUG(log, "Remove old parts' vector index files {} for decouple part {}", cache_key.vector_index_name, part->name);
                    part->removeRowIdsMaps(cache_key.vector_index_name, false);
                }
            }
        }
    }
}

bool MergeTreeVectorIndexBuilderUpdater::allowToBuildVectorIndex(const bool slow_mode, const size_t builds_count_in_queue) const
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
            LOG_DEBUG(log, "[allowToBuildVectorIndex] Number of queued build vector index enties ({})"
                    " is greater than background_slow_mode_vector_pool_size ({}), so won't select new parts to build vector index",
                    builds_count_in_queue, server_settings.background_slow_mode_vector_pool_size);
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
            LOG_DEBUG(log, "[allowToBuildVectorIndex] Number of queued build vector index enties ({})"
                    " is greater than background_vector_pool_size ({}), so won't select new parts to build vector index",
                    builds_count_in_queue, server_settings.background_vector_pool_size);
            return false;
        }

        occupied = CurrentMetrics::values[CurrentMetrics::BackgroundVectorIndexPoolTask].load(std::memory_order_relaxed);

        if (occupied < server_settings.background_vector_pool_size)
            return true;
    }

    return false;
}

VectorIndexEntryPtr MergeTreeVectorIndexBuilderUpdater::selectPartToBuildVectorIndex(
    const StorageMetadataPtr & metadata_snapshot,
    bool select_slow_mode_part,
    const MergeTreeData::DataParts & currently_merging_mutating_parts)
{
    if (!metadata_snapshot->hasVectorIndices())
        return {};

    size_t min_rows_to_build_vector_index = data.getSettings()->min_rows_to_build_vector_index;
    for (const auto & part : data.getDataPartsForInternalUsage())
    {
        /// TODO: Support atomic insert, avoid to select active data parts in an uncommited transaction.

        /// Skip empty part
        if (part->isEmpty())
            continue;

        /// Since building vector index doesn't block mutation on the part, the new part need to check if any covered part is building vindex.
        /// The new part already blocked merge to select it, hence it's safe here. all_1_1_0 can avoid index build selection for future parts all_1_1_0_*
        {
            std::lock_guard lock(data.currently_vector_indexing_parts_mutex);
            if (data.currently_vector_indexing_parts.count(part->name) > 0)
                continue;

            bool skip_build_index = false;
            std::vector<String> need_remove_parts;
            for (const auto & part_name : data.currently_vector_indexing_parts)
            {
                auto info = MergeTreePartInfo::fromPartName(part_name, data.format_version);
                if (part->info.contains(info))
                {
                    if (part->info.isFromSamePart(info))
                    {
                        LOG_DEBUG(log, "Skip to select future mutation part {} build vector index due to the same origin part {}", part->name, part_name);
                        skip_build_index = true;
                        break;
                    }
                    else
                    {
                        LOG_DEBUG(log, "Remove possible stale or not needed part {} due to covered part {} exists", part_name, part->name);
                        need_remove_parts.emplace_back(part_name);
                    }
                }
            }

            for (const auto & part_name : need_remove_parts)
                data.currently_vector_indexing_parts.erase(part_name);

            if (skip_build_index)
                continue;
        }

        if (currently_merging_mutating_parts.count(part) > 0)
            continue;

        /// ReplicatedMergeTree depends on virtual_parts for merge, MergeTree depends on currently_merging_mutating_parts
        if (is_replicated && data.partIsAssignedToBackgroundOperation(part))
        {
            LOG_DEBUG(log, "Skip to select part {} build vector index due to part is assigned to background operation", part->name);
            continue;
        }

        /// Support multiple vector indices
        for (const auto & vec_index : metadata_snapshot->getVectorIndices())
        {
            if (part->hasBuildError(vec_index.name))
               continue;

            /// Part already has vector index or no need to build.
            if (part->containVectorIndex(vec_index.name) || part->isSmallPart(min_rows_to_build_vector_index))
                continue;

            if (part->containRowIdsMaps(vec_index.name) && data.getSettings()->disable_rebuild_for_decouple)
                continue;

            /// Part doesn't contain this vector index, need to build.
            if (select_slow_mode_part)
            {
                if (!isSlowModePart(part, vec_index.name))
                    continue;

                LOG_DEBUG(log, "Select slow mode part name: {} for vector index name: {}", part->name, vec_index.name);
                return std::make_shared<VectorIndexEntry>(part->name, vec_index.name, data, is_replicated);
            }
            else /// normal fast mode
            {
                if (isSlowModePart(part, vec_index.name))
                    continue;

                LOG_DEBUG(log, "Select part name: {} for vector index name: {}", part->name, vec_index.name);
                return std::make_shared<VectorIndexEntry>(part->name, vec_index.name, data, is_replicated);
            }
        }
    }

    return {};
}

BuildVectorIndexStatus
MergeTreeVectorIndexBuilderUpdater::buildVectorIndex(
    const StorageMetadataPtr & metadata_snapshot,
    const String & part_name,
    const String & vector_index_name,
    bool slow_mode)
{
    if (part_name.empty())
    {
        LOG_INFO(log, "No data");
        return BuildVectorIndexStatus::NO_DATA_PART;
    }

    if (!metadata_snapshot->hasVectorIndices())
    {
        LOG_INFO(log, "No vector index declared");
        return BuildVectorIndexStatus::BUILD_SKIPPED;
    }

    Stopwatch watch;
    /// build vector index part by part
    /// we may consider building vector index in parallel in the future.
    LOG_INFO(log, "Start vector index build task for part {}, index name: {}, slow_mode: {}", part_name, vector_index_name, slow_mode);

    /// One part is selected to build index.
    MergeTreeDataPartPtr part = data.getActiveContainingPart(part_name);
    if (!part)
    {
        LOG_INFO(log, "Part {} is not active, no need to build index", part_name);
        return BuildVectorIndexStatus::NO_DATA_PART;
    }

    if (part->isBuildCancelled())
    {
        LOG_INFO(log, "The index build job for Part {} has been cancelled", part_name);
        VectorIndexEventLog::addEventLog(data.getContext(), part, vector_index_name, VectorIndexEventLogElement::BUILD_CANCELD);
        return BuildVectorIndexStatus::BUILD_FAIL;
    }

    /// Check latest metadata
    if (!part->storage.getInMemoryMetadataPtr()->getVectorIndices().has(vector_index_name))
    {
        LOG_INFO(log, "Skip build for dropped vector index {}.", vector_index_name);
        return BuildVectorIndexStatus::BUILD_SKIPPED;
    }

    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
    }

    BuildVectorIndexStatus status = BuildVectorIndexStatus::SUCCESS;
    part->onVectorIndexBuildStart(vector_index_name);

    constexpr int maxBuildRetryCount = 3;
    int failed_count = 0;
    while (true)
    {
        try
        {
            if (BuildIndexHelpers::checkOperationIsNotCanceled(builds_blocker))
            {
                LOG_INFO(log, "Build vector index {} for part {}", vector_index_name, part->name);
                status = buildVectorIndexForOnePart(metadata_snapshot, part, vector_index_name, slow_mode);
            }
            break;
        }
        catch (Exception & e)
        {
            /// retry build for MEMORY_LIMIT_EXCEEDED case
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED && ++failed_count < maxBuildRetryCount)
                continue;

            if (e.code() == ErrorCodes::VECTOR_INDEX_BUILD_MEMORY_INSUFFICIENT)
            {
                LOG_WARNING(log, "Cannot build vector index {} for part {} for now due to build memory limitation.", vector_index_name, part->name);
                part->onVectorIndexBuildError(
                    vector_index_name, "Cannot build vector index for now due to build memory limitation");
                return BuildVectorIndexStatus::BUILD_RETRY;
            }

            LOG_WARNING(log,"Vector Index build task for part {} index name {} failed: {}", part->name, vector_index_name, e.message());
            /// Set part's build error if exists for build vector index with exception cases.
            BuildIndexHelpers::setPartVectorIndexBuildStatus(metadata_snapshot, part, vector_index_name);
            VectorIndexEventLog::addEventLog(
                data.getContext(), part, vector_index_name, VectorIndexEventLogElement::BUILD_ERROR, ExecutionStatus::fromCurrentException());
            part->onVectorIndexBuildError(vector_index_name, e.message());

            throw;
        }
    }

    if (status != BuildVectorIndexStatus::SUCCESS)
    {
        if (status == BuildVectorIndexStatus::BUILD_FAIL)
        {
            BuildIndexHelpers::setPartVectorIndexBuildStatus(metadata_snapshot, part, vector_index_name);
            ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);
        }
        part->onVectorIndexBuildError(vector_index_name, Search::enumToString(status));
    }
    else
    {
        LOG_INFO(log, "Vector index build task finished for part {} index name {}", part->name, vector_index_name);
        part->onVectorIndexBuildFinish(vector_index_name);
    }

    watch.stop();
    LOG_INFO(log, "Vector index build task for part {} index name {} finished in {} sec, slow_mode: {}", part_name, vector_index_name, watch.elapsedSeconds(), slow_mode);

    // TODO: handle fail case
    return status;
}

BuildVectorIndexStatus MergeTreeVectorIndexBuilderUpdater::buildVectorIndexForOnePart(
    const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part, const String & vector_index_name, bool slow_mode)
{
    LOG_TRACE(log, "Start checking for build index {} for part {}", vector_index_name, part->name);

    bool enforce_fixed_array = data.getSettings()->enforce_fixed_vector_length_constraint;

    /// TODO: Build all the vector indices for this part?
    /// Currently build one vector index for this part.
    for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
    {
        /// Support multiple vector indices
        if (vec_index_desc.name != vector_index_name)
            continue;

        if (part->containVectorIndex(vec_index_desc.name))
        {
            LOG_DEBUG(log, "Part {} already has vector index {} built", part->name, vector_index_name);
            VectorIndexEventLog::addEventLog(data.getContext(), part, vector_index_name, VectorIndexEventLogElement::BUILD_SUCCEED);
            return BuildVectorIndexStatus::SUCCESS;
        }

        if (!part->getColumns().contains(vec_index_desc.column))
        {
            LOG_WARNING(log, "Part {} has no column {} can match vec_index_desc {}", part->name, vec_index_desc.column, vec_index_desc.name);
            part->addVectorIndex(vec_index_desc.name);
            VectorIndexEventLog::addEventLog(data.getContext(), part, vector_index_name, VectorIndexEventLogElement::BUILD_SUCCEED);
            return BuildVectorIndexStatus::SUCCESS;
        }

        /// only one column to build vector index, using a large dimension as default value.
        uint64_t dim = 0;
        NamesAndTypesList cols;

        auto col_and_type = metadata_snapshot->getColumns().getAllPhysical().tryGetByName(vec_index_desc.column);
        if (col_and_type)
        {
            cols.emplace_back(*col_and_type);
            const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(col_and_type->getTypeInStorage().get());
            if (array_type)
            {
                dim = metadata_snapshot->getConstraints().getArrayLengthByColumnName(vec_index_desc.column).first;
                if (dim == 0)
                {
                    LOG_ERROR(log, "Wrong dimension: 0, please check length constraint on the column.");
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong dimension: 0, please check length constraint on the column.");
                }
            }
        }
        else
        {
            LOG_WARNING(log, "Found column {} in part and VectorIndexDescription, but not in metadata snapshot.", vec_index_desc.column);
            VectorIndexEventLog::addEventLog(
                data.getContext(),
                part,
                vector_index_name,
                VectorIndexEventLogElement::BUILD_ERROR,
                ExecutionStatus(
                    ErrorCodes::ABORTED,
                    "Found column " + vec_index_desc.column + " in part and VectorIndexDescription, but not in metadata snapshot"));
            return BuildVectorIndexStatus::META_ERROR;
        }

        /// below is a horror to test whether a moved part need to rebuild its index. basially is reads from vector_index_ready if there is one,
        /// creates a pesudo vector index using parameters recorded in vector_index_ready and compare with the new index to see if they are the same.
        const DataPartStorageOnDiskBase * part_storage
            = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
        if (part_storage == nullptr)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
        }
        auto disk = part_storage->getDisk();
        String vector_index_description_file_name = VectorIndex::getVectorIndexDescriptionFileName(vector_index_name);

        /// Create temporary directory to store built vector index files.
        /// The director name starts with prefix "vector_tmp_" + vector_index_name + part_name w/o_mutation
        /// e.g. part all_1_1_0_5 vector index v1: vector_tmp_v1_all_1_1_0
        String part_name_prefix = part->info.getPartNameWithoutMutation();
        String tmp_vector_index_dir = "vector_tmp_" + vector_index_name + "_" + part_name_prefix + "/";
        String vector_tmp_relative_path = data.getRelativeDataPath() + tmp_vector_index_dir + "/";
        String vector_tmp_full_path = data.getFullPathOnDisk(disk) + tmp_vector_index_dir + "/";

        /// Add lock to avoid wronly remove of temporary directory
        auto temporary_directory_lock = data.getTemporaryPartDirectoryHolder(tmp_vector_index_dir);

        /// Since the vector index is stored in a temporary directory, add check for it too.
        /// Loop through the relative_data_path to check if any directory with vector_tmp_<vector_index_name>_<part_name_without_mutation> exists
        bool retry_move_only = false;
        if (disk->exists(vector_tmp_relative_path))
        {
            if (disk->exists(vector_tmp_relative_path + "/" + vector_index_description_file_name))
            {
                retry_move_only = true;
            }
            else
            {
                LOG_DEBUG(log, "Remove incomplete temporary directory {}", vector_tmp_relative_path);
                disk->removeRecursive(vector_tmp_relative_path);
            }
        }

        /// Need to move built vector index files to the part.
        if (retry_move_only)
        {
            auto status = TryMoveVectorIndexFiles(part, vec_index_desc, disk, vector_tmp_relative_path, dim, nullptr);
            if (status != BuildVectorIndexStatus::SUCCESS)
                return status;

            // VectorIndexEventLog::addEventLog(data.getContext(), part, VectorIndexEventLogElement::BUILD_SUCCEED);

            return BuildVectorIndexStatus::SUCCESS;
        }

        /// Create temp directory before serialize.
        if (disk->exists(vector_tmp_relative_path))
        {
            LOG_DEBUG(
                log, "The temporary directory to store vector index files already exists, will be removed {}", vector_tmp_relative_path);
            disk->removeRecursive(vector_tmp_relative_path);
        }

        disk->createDirectories(vector_tmp_relative_path);

        /// Used for cancel build vector index
        auto check_build_canceled = [this, &part, &vec_index_desc]() -> bool
        {
            if (builds_blocker.isCancelled() || part->isBuildCancelled())
                return true;

            /// Check vector index exists in table's metadata
            auto & latest_vec_indices = part->storage.getInMemoryMetadataPtr()->getVectorIndices();
            if (latest_vec_indices.empty() || !latest_vec_indices.has(vec_index_desc))
                return true;

            return false;
        };

        VectorIndex::SegmentId segment_id(
            part->getDataPartStoragePtr(), vector_tmp_full_path, part->name, vec_index_desc.name, vec_index_desc.column);
        VectorIndex::PartReader part_reader(
            part, cols, metadata_snapshot, data.getContext()->getMarkCache().get(), check_build_canceled, dim, enforce_fixed_array);

        Search::Parameters parameters = VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters);
        Search::IndexType index_type = VectorIndex::getIndexType(vec_index_desc.type);
        Search::Metric metric = VectorIndex::getMetric(parameters.extractParam("metric_type", std::string(data.getSettings()->vector_search_metric_type)));
        VectorIndex::VectorSegmentExecutorPtr vec_index_builder = std::make_shared<VectorIndex::VectorSegmentExecutor>(
            segment_id,
            index_type,
            metric,
            dim,
            part->rows_count,
            parameters,
            data.getSettings()->min_bytes_to_build_vector_index,
            data.getSettings()->default_mstg_disk_mode);
        size_t max_build_index_add_block_size = data.getContext()->getSettingsRef().max_build_index_add_block_size;
        size_t max_build_index_train_block_size = data.getContext()->getSettingsRef().max_build_index_train_block_size;

        VectorIndexEventLog::addEventLog(data.getContext(), part, vector_index_name, VectorIndexEventLogElement::BUILD_START);
        vec_index_builder->buildIndex(&part_reader, check_build_canceled, slow_mode, max_build_index_train_block_size, max_build_index_add_block_size);

        const auto empty_ids = part_reader.emptyIds();
        if (!empty_ids.empty())
            vec_index_builder->removeByIds(empty_ids.size(), empty_ids.data());

        if (!part->isBuildCancelled() && BuildIndexHelpers::checkOperationIsNotCanceled(builds_blocker))
        {
            /// remove empty vectors
            LOG_DEBUG(log, "Serialize vector index");
            VectorIndex::Status seri_status = vec_index_builder->serialize();
            LOG_DEBUG(log, "Serialization status: {}", seri_status.getCode());

            if (!seri_status.fine())
            {
                /// Remove temporay directory
                if (disk->exists(vector_tmp_relative_path))
                {
                    LOG_DEBUG(log, "seri_status is not fine, will remove vector_tmp_relative_path {}", vector_tmp_relative_path);
                    disk->removeRecursive(vector_tmp_relative_path);
                }
                else
                    LOG_DEBUG(log, "seri_status is not fine, vector_tmp_relative_path doesn't exist {}", vector_tmp_relative_path);

                throw Exception(seri_status.getCode(), seri_status.getMessage().data());
            }

            /// Done with writing vector index files to temporary directory.
            /// Decide to move index files to which part direcory.
            auto status = TryMoveVectorIndexFiles(part, vec_index_desc, disk, vector_tmp_relative_path, dim, vec_index_builder);
            if (status != BuildVectorIndexStatus::SUCCESS)
                return status;
        }
        else
        {
            return BuildVectorIndexStatus::BUILD_SKIPPED;
        }
    }

    LOG_DEBUG(log, "Vector index build complete");

    VectorIndexEventLog::addEventLog(data.getContext(), part, vector_index_name, VectorIndexEventLogElement::BUILD_SUCCEED);
    return BuildVectorIndexStatus::SUCCESS;
}

BuildVectorIndexStatus MergeTreeVectorIndexBuilderUpdater::TryMoveVectorIndexFiles(
    const MergeTreeDataPartPtr & build_part,
    const VectorIndexDescription & vec_index_desc,
    DiskPtr disk,
    const String & vector_tmp_relative_path,
    const UInt64 & dim,
    VectorIndex::VectorSegmentExecutorPtr vec_index_builder)
{
    if (!vec_index_builder)
        LOG_DEBUG(log, "Vector index is built for part: {} and stored in temporary directory {}", build_part->name, vector_tmp_relative_path);

    /// Find an active same future part to store the vector index files.
    MergeTreeDataPartPtr future_part = nullptr;
    if (build_part->getState() == DB::MergeTreeDataPartState::Active)
    {
        future_part = build_part;
    }
    else
    {
        /// Find future active part
        future_part = data.getActiveContainingPart(build_part->name);
        if (!future_part)
        {
            LOG_WARNING(log, "Failed to find future part for part {}, leave the temporary directory", build_part->name);
            VectorIndexEventLog::addEventLog(data.getContext(), build_part, vec_index_desc.name, VectorIndexEventLogElement::BUILD_CANCELD);
            return BuildVectorIndexStatus::BUILD_SKIPPED;
        }
    }

    /// Check the latest metadata before move files, in case drop index submitted during index building.
    auto & latest_vec_indices = future_part->storage.getInMemoryMetadataPtr()->getVectorIndices();
    if (latest_vec_indices.empty() || !latest_vec_indices.has(vec_index_desc))
    {
        LOG_INFO(log, "Vector index {} has been dropped, no need to build it.", vec_index_desc.name);
        if (disk->exists(vector_tmp_relative_path))
            disk->removeRecursive(vector_tmp_relative_path);
        else
            LOG_DEBUG(log, "[Dropped] vector_tmp_relative_path doesn't exist {}", vector_tmp_relative_path);
        build_part->removeVectorIndexInfo(vec_index_desc.name);
        VectorIndexEventLog::addEventLog(data.getContext(), build_part, vec_index_desc.name, VectorIndexEventLogElement::BUILD_CANCELD);
        return BuildVectorIndexStatus::BUILD_SKIPPED;
    }

    /// In replicated case, slow replica may build vector index for merged data part.
    /// Here check the future part has the same prefix name as build part.
    if (is_replicated)
    {
        if (!future_part->info.isFromSamePart(build_part->info))
        {
            LOG_DEBUG(
                log,
                "future part '{}' is a merged part not mutated part from part '{}' build vector index, no need to move.",
                future_part->name, build_part->name);
            if (disk->exists(vector_tmp_relative_path))
            {
                LOG_DEBUG(log, "Will remove unneeded vector index");
                disk->removeRecursive(vector_tmp_relative_path);
            }
            else
                LOG_DEBUG(log, "[Unneeded] vector_tmp_relative_path doesn't exist {}", vector_tmp_relative_path);

            VectorIndexEventLog::addEventLog(data.getContext(), build_part, vec_index_desc.name, VectorIndexEventLogElement::BUILD_CANCELD);
            return BuildVectorIndexStatus::BUILD_SKIPPED;
        }

        /// Fetched part already contains vector vector index
        if (future_part->containVectorIndex(vec_index_desc.name))
        {
            LOG_DEBUG(log, "future part '{}' already contain vector index {}, no need to move", future_part->name, vec_index_desc.name);
            if (disk->exists(vector_tmp_relative_path))
                disk->removeRecursive(vector_tmp_relative_path);

            VectorIndexEventLog::addEventLog(data.getContext(), build_part, vec_index_desc.name, VectorIndexEventLogElement::BUILD_CANCELD);
            return BuildVectorIndexStatus::BUILD_SKIPPED;
        }
    }

    /// lock part for move build vector index, avoid concurrently mutation
    auto move_mutate_lock = future_part->tryLockPartForIndexMoveAndMutate();
    if (!move_mutate_lock.owns_lock())
    {
        LOG_INFO(log, "Will move vector index files later since future part `{}` is under mutating", future_part->name);
        VectorIndexEventLog::addEventLog(data.getContext(), build_part, vec_index_desc.name, VectorIndexEventLogElement::BUILD_SUCCEED);
        return BuildVectorIndexStatus::BUILD_RETRY;
    }

    /// Second, initialize or update VectorSegmentExecutorPtr depending on parameter vec_index_builder.
    VectorIndex::SegmentId future_segment(
        future_part->getDataPartStoragePtr(),
        future_part->name,
        vec_index_desc.name,
        vec_index_desc.column);

    if (vec_index_builder)
    {
        /// First build case with already initialized builder.
        /// update delete bitmap in memory in currently builder, which will be put in cache.
        /// Update segment id with correct part name and path.
        vec_index_builder->updateSegmentId(future_segment);
    }
    else
    {
        /// Retry to move case, where vector index has already built.
        /// Initialize builder
        Search::Parameters parameters = VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters);
        Search::IndexType index_type = VectorIndex::getIndexType(vec_index_desc.type);
        Search::Metric metric = VectorIndex::getMetric(parameters.extractParam("metric_type", std::string(data.getSettings()->vector_search_metric_type)));

        vec_index_builder = std::make_shared<VectorIndex::VectorSegmentExecutor>(
            future_segment,
            index_type,
            metric,
            dim,
            future_part->rows_count,
            parameters,
            data.getSettings()->min_bytes_to_build_vector_index,
            data.getSettings()->default_mstg_disk_mode);
    }

    /// Finally, move index files to part and apply lightweight delete.
    moveVectorIndexFilesToFuturePartAndCache(disk, vector_tmp_relative_path, future_part, vec_index_desc, vec_index_builder);

    return BuildVectorIndexStatus::SUCCESS;
}

bool MergeTreeVectorIndexBuilderUpdater::moveVectorIndexFilesToFuturePartAndCache(
    DiskPtr disk,
    const String & vector_tmp_relative_path, 
    const MergeTreeDataPartPtr & dest_part,
    const VectorIndexDescription & vec_index_desc,
    const VectorIndex::VectorSegmentExecutorPtr vec_executor)
{
    if (!dest_part)
        return false;

    String vector_index_name = vec_index_desc.name;
    LOG_DEBUG(log, "Current active part {} with state {} is selected to store vector index {}", dest_part->name, dest_part->getState(), vector_index_name);

    String dest_relative_path = dest_part->getDataPartStorage().getRelativePath();

    /// Calculate vector index checksums
    auto vector_index_checksums = dest_part->calculateVectorIndexChecksums(vector_tmp_relative_path);
    MergeTreeDataPartChecksums decouple_checksums;

    /// Move to current part which is active.
    bool found_vector_file = false;
    Names moved_files;
    try
    {
        /// Put here to avoid concurently drop vector index
        auto lock_part = data.lockParts();

        for (auto it = disk->iterateDirectory(vector_tmp_relative_path); it->isValid(); it->next())
        {
            if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX))
                continue;
            disk->moveFile(vector_tmp_relative_path + it->name(), dest_relative_path + it->name());

            moved_files.emplace_back(it->name());

            if (!found_vector_file)
                found_vector_file = true;
        }
    }
    catch (Exception & e)
    {
        LOG_WARNING(log,"Failed to move built vector index {} files to part {}, will do some clean-up work: {}", vector_index_name, dest_part->name, e.message());

        if (!moved_files.empty())
        {
            if (disk->exists(vector_tmp_relative_path))
                disk->removeRecursive(vector_tmp_relative_path);
            dest_part->removeIncompleteMovedVectorIndexFiles(moved_files);
        }

        throw;
    }

    if (!found_vector_file)
    {
        LOG_WARNING(log, "Failed to find any vector index files in directory {}, will remove it", vector_tmp_relative_path);
        if (disk->exists(vector_tmp_relative_path))
            disk->removeRecursive(vector_tmp_relative_path);
        else
            LOG_WARNING(log, "[moveVectorIndexFilesToFuturePartAndCache] vector_tmp_relative_path doesn't exist {}", vector_tmp_relative_path);

        return false;
    }

    LOG_INFO(log, "Move vector index files for index {} to part {}", vec_index_desc.name, dest_part->name);
    disk->removeRecursive(vector_tmp_relative_path);

    /// Write the new built vector index checksums file
    String vector_index_checksums_file_path = dest_relative_path + VectorIndex::getVectorIndexChecksumsFileName(vector_index_name);
    auto out_checksums = disk->writeFile(vector_index_checksums_file_path, 4096);
    vector_index_checksums.write(*out_checksums);
    out_checksums->finalize();

    /// Need to remove row ids maps files
    if (dest_part->containRowIdsMaps(vec_index_desc.name))
    {
        /// remove Decouple merge source part vector index description file
        dest_part->forceAllDecoupledVectorIndexExpire(vector_index_name, vec_index_desc.column);

        /// Try cancel Decouple Index Cache Load
        dest_part->CancelLoadingVIOfInactivePart(vector_index_name, vec_index_desc.column);

        VectorIndex::removeRowIdsMapsFromPartAndCache(dest_part, vec_index_desc.name, vec_index_desc.column, /* skip checksum */ true, log);
    }

    /// Finally, Update metadata of checksum in dest part
    if (!const_pointer_cast<IMergeTreeDataPart>(dest_part)->loadBuiltVectorIndexChecksums(vector_index_name))
    {
        LOG_INFO(log, "Check consistency for vector index {} in part {} failed, will remove it.", vector_index_name, dest_part->name);
        dest_part->removeVectorIndex(vector_index_name);
        return false;
    }

    LOG_DEBUG(log, "load new index to LRU cache");
    if (!vec_executor->cache().fine())
    {
        LOG_DEBUG(log, "Cannot cache item, will load from index file");
        if (!vec_executor->load().fine())
            LOG_ERROR(log, "Unable to load index from file in ready state, which is highly likely a bug");
    }

    /// new index online
    dest_part->addVectorIndex(vector_index_name);
    dest_part->addBuiltVectorIndex(vec_index_desc);

    /// Apply lightweight delete bitmap to index's bitmap
    if (dest_part->hasLightweightDelete())
    {
        LOG_DEBUG(log, "Apply lightweight delete to vector index {} in part {}", vector_index_name, dest_part->name);
        dest_part->onLightweightDelete(vector_index_name);
    }

    return true;
}

}
