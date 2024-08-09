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

#include <stdexcept>

#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Common/ErrorCodes.h>
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
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>

#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/VIMetadata.h>
#include <VectorIndex/Common/VIPartReader.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <VectorIndex/Storages/VIBuilderUpdater.h>
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


namespace BuildIndexHelpers
{

    static bool checkOperationIsNotCanceled(ActionBlocker & builds_blocker)
    {
        if (builds_blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Cancelled building vector index");

        return true;
    }

}

VIBuilderUpdater::VIBuilderUpdater(MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (VectorIndexUpdater)"))
{
    if (startsWith(data.getName(), "Replicated"))
        is_replicated = true;
}

void VIBuilderUpdater::removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot)
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
    std::list<std::pair<VectorIndex::CacheKey, VIParameter>> cached_item_list = VectorIndex::VICacheManager::getAllCacheNames();

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
        auto [clear_cache, _] = data.needClearVectorIndexCacheAndFile(part, metadata_snapshot, cache_key);

        if (!clear_cache)
        {
            LOG_DEBUG(log, "Find Vector Index in metadata");
            VIParameter params = cache_item.second;

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
            VectorIndex::VICacheManager::removeFromCache(cache_key);
        }
    }
}

bool VIBuilderUpdater::allowToBuildVI(const bool slow_mode, const size_t builds_count_in_queue) const
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

VIEntryPtr VIBuilderUpdater::selectPartToBuildVI(const StorageMetadataPtr & metadata_snapshot, bool select_slow_mode_part)
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
                        LOG_DEBUG(
                            log,
                            "Skip to select future mutation part {} build vector index due to the same origin part {}",
                            part->name,
                            part_name);
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

        /// ReplicatedMergeTree depends on virtual_parts for merge, MergeTree depends on currently_merging_mutating_parts
        if (data.partIsAssignedToBackgroundOperation(part))
        {
            LOG_DEBUG(log, "Skip to select part {} build vector index due to part is assigned to background operation", part->name);
            continue;
        }

        /// Support multiple vector indices
        for (const auto & vec_index : metadata_snapshot->getVectorIndices())
        {
            auto column_index_opt = part->vector_index.getColumnIndex(vec_index.name);
            if (!column_index_opt.has_value())
            {
                LOG_DEBUG(log, "Part: {} does not have column index {}, will be initialized.", part->name, vec_index.name);
                part->vector_index.addVectorIndex(vec_index);
            }

            const auto column_index = part->vector_index.getColumnIndex(vec_index.name).value();

            auto status = column_index->getVectorIndexState();

            if ((status != VIState::PENDING)
                || (column_index->isDecoupleIndexFile() && !data.getSettings()->enable_rebuild_for_decouple))
                continue;

            /// Part doesn't contain this vector index, need to build.
            if (select_slow_mode_part)
            {
                if (!isSlowModePart(part, vec_index.name))
                    continue;

                LOG_DEBUG(log, "Select slow mode part name: {} for vector index name: {}", part->name, vec_index.name);
                return std::make_shared<VIEntry>(part->name, vec_index.name, data, is_replicated);
            }
            else /// normal fast mode
            {
                if (isSlowModePart(part, vec_index.name))
                    continue;

                LOG_DEBUG(log, "Select part name: {} for vector index name: {}", part->name, vec_index.name);
                return std::make_shared<VIEntry>(part->name, vec_index.name, data, is_replicated);
            }
        }
    }

    return {};
}

void VIBuilderUpdater::checkPartCanBuildVI(
    MergeTreeDataPartPtr data_part, const VIDescription & vec_desc)
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
            "No column {} in part {} to build index, If the column is newly added and the Materialize operation has not been performed yet, please perform the Materialize operation as soon as possible.",
            vec_desc.column,
            data_part->name);
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "No column {} in part {} to build index, maybe need Materialize operation.", vec_desc.column, data_part->name);
    }

    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(data_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");

    if (data_part->vector_index.isBuildCancelled(vec_desc.name))
        throw Exception(ErrorCodes::ABORTED, "Cancel build vector index {} for part {}.", vec_desc.name, data_part->name);

    if (!data_part->vector_index.getColumnIndex(vec_desc.name).has_value())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column index {} for part {} does not exits, it's a bug.", vec_desc.name, data_part->name);

    /// If we already have this vector index in this part, we do not need to do anything.
    /// Support multiple vector indices
    if (data_part->vector_index.alreadyWithVIndexSegment(vec_desc.name))
        throw Exception(ErrorCodes::VECTOR_INDEX_ALREADY_EXISTS, "No need to build vector index {} for part {}, because the part already has ready vector index.", vec_desc.name, data_part->name);
}

VIContextPtr VIBuilderUpdater::prepareBuildVIContext(
    const StorageMetadataPtr & metadata_snapshot, const String & part_name, const String & vector_index_name, bool slow_mode, bool is_replicated_vector_task)
{
    auto ctx = std::make_shared<VIContext>();
    if (part_name.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Part name in index task is empty, build error.");

    /// Get active part containing current part to build vector index
    /// No need to check part name, mutations are not blocked by build vector index.
    MergeTreeDataPartPtr part = data.getActiveContainingPart(part_name);
    if (is_replicated_vector_task)
    {
        auto target_vector_index_part_info = MergeTreePartInfo::fromPartName(part_name, data.format_version);
        if (!part || !part->info.isFromSamePart(target_vector_index_part_info))
            throw Exception(ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART, "No Active DataPart or Active part is inconsistent with the part in vector task entry part name {}.", part->name);
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no definition of {} in the table definition, skip build index.", vector_index_name);

    const DataPartStorageOnDiskBase * part_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
    auto disk = part_storage->getDisk();

    ctx->source_part = part;
    ctx->metadata_snapshot = metadata_snapshot;
    ctx->part_name = part_name;
    ctx->vector_index_name = vector_index_name;
    ctx->slow_mode = slow_mode;
    ctx->source_column_index = part->vector_index.getColumnIndex(vector_index_name).value();
    ctx->builds_blocker = &builds_blocker;

    /// Create temporary directory to store built vector index files.
    /// The director name starts with prefix "vector_tmp_" + vector_index_name + part_name w/o_mutation
    /// e.g. part all_1_1_0_5 vector index v1: vector_tmp_v1_all_1_1_0
    String part_name_prefix = part->info.getPartNameWithoutMutation();
    String tmp_vector_index_dir = "vector_tmp_" + vector_index_name + "_" + part_name_prefix;
    String vector_tmp_relative_path = data.getRelativeDataPath() + tmp_vector_index_dir + "/";
    String vector_tmp_full_path = data.getFullPathOnDisk(disk) + tmp_vector_index_dir + "/";

    ctx->log = &Poco::Logger::get("VITask[" + part_name_prefix + "]");
    ctx->temporary_directory_lock = data.getTemporaryPartDirectoryHolder(tmp_vector_index_dir);
    ctx->vector_tmp_full_path = vector_tmp_full_path;
    ctx->vector_tmp_relative_path = vector_tmp_relative_path;
    ctx->index_build_memory_lock = std::make_unique<VIBuildMemoryUsageHelper>();

    /// will init in index serialize
    ctx->vector_index_checksum = nullptr;

    ctx->build_cancel_callback = [source_part = part, builds_blocker = ctx->builds_blocker, vec_index_desc = ctx->vec_index_desc]() -> bool
    {
        if (source_part->storage.isShutdown() || builds_blocker->isCancelled()
            || source_part->vector_index.isBuildCancelled(vec_index_desc.name))
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

    ctx->clean_tmp_folder_callback = [log = ctx->log, disk](const String & vector_tmp_path) mutable
    {
        /// Remove temporay directory
        if (disk->exists(vector_tmp_path))
            disk->removeRecursive(vector_tmp_path);
        else
            LOG_DEBUG(log, "Remove vector_tmp_relative_path doesn't hello exist {}", vector_tmp_path);
    };

    ctx->write_event_log = [part = ctx->source_part, vector_index_name = ctx->vector_index_name](
                               VIEventLogElement::Type event_type, int error_code, const String & error_message) mutable
    {
        VIEventLog::addEventLog(
            Context::getGlobalContextInstance(), part, vector_index_name, event_type, ExecutionStatus(error_code, error_message));
    };

    return ctx;
}

/// only build index and serialize vector index
VIBuiltStatus VIBuilderUpdater::buildVI(const VIContextPtr ctx)
{
    try
    {
        LOG_INFO(log, "Build vector index {} for part {} start.", ctx->vector_index_name, ctx->source_part->name);
        if (ctx->source_column_index->getVectorIndexState() == VIState::BUILT)
        {
            LOG_WARNING(log, "Vector index has been created successfully and does not need to be built again. it's a bug.");
            return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
        }

        MergeTreeDataPartPtr future_part = data.getActiveContainingPart(ctx->source_part->name);
        if (future_part)
        {
            auto future_column_index_opt = future_part->vector_index.getColumnIndex(ctx->vector_index_name);
            if (future_column_index_opt.has_value() && future_column_index_opt.value()->getVectorIndexState() == VIState::BUILT)
            {
                LOG_WARNING(log, "Vector index has been created successfully in part {} and does not need to be built again for part {}.", future_part->name, ctx->source_part->name);
                return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
            }
        }

        ctx->watch.restart();
        ctx->source_column_index->onBuildStart();

        VIBuiltStatus status = buildVIForOnePart(ctx);

        if (status.getStatus() == VIBuiltStatus::SUCCESS)
            LOG_INFO(
                log,
                "Build vector index and serialize vector index for part {} with the index name {} completed in {} sec, slow_mode: {}, will move index later",
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

        return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }
}

VIBuiltStatus VIBuilderUpdater::buildVIForOnePart(VIContextPtr ctx)
{
    if (ctx->build_cancel_callback())
        return VIBuiltStatus{VIBuiltStatus::BUILD_CANCEL};

    bool enforce_fixed_array = data.getSettings()->enforce_fixed_vector_length_constraint;

    /// TODO: Build all the vector indices for this part?
    /// only one column to build vector index, using a large dimension as default value.
    uint64_t dim = 0;
    NamesAndTypesList cols;

    Search::DataType search_type = ctx->vec_index_desc.vector_search_type;

    auto col_and_type = ctx->metadata_snapshot->getColumns().getAllPhysical().tryGetByName(ctx->vec_index_desc.column);
    if (col_and_type)
    {
        cols.emplace_back(*col_and_type);
        dim = getVectorDimension(search_type, *ctx->metadata_snapshot, ctx->vec_index_desc.column);
        checkVectorDimension(search_type, dim);
    }
    else
    {
        LOG_WARNING(log, "Found column {} in part and VIDescription, but not in metadata snapshot.", ctx->vec_index_desc.column);
        return VIBuiltStatus{VIBuiltStatus::META_ERROR};
    }

    /// below is a horror to test whether a moved part need to rebuild its index. basially is reads from vector_index_ready if there is one,
    /// creates a pesudo vector index using parameters recorded in vector_index_ready and compare with the new index to see if they are the same.
    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(ctx->source_part->getDataPartStoragePtr().get());

    if (part_storage == nullptr)
        return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL};

    auto disk = part_storage->getDisk();

    /// Since the vector index is stored in a temporary directory, add check for it too.
    /// Loop through the relative_data_path to check if any directory with vector_tmp_<vector_index_name>_<part_name_without_mutation> exists
    if (disk->exists(ctx->vector_tmp_relative_path))
    {
        auto index_checksum_file = getVectorIndexChecksumsFileName(ctx->vector_index_name);
        if (disk->exists(ctx->vector_tmp_relative_path + "/" + index_checksum_file))
        {

            LOG_INFO(log, "The Vector index for part {} in temp path {} is already prepared, skip rebuild vector index.", ctx->source_part->name, ctx->vector_tmp_relative_path);
            return VIBuiltStatus{VIBuiltStatus::SUCCESS};
        }
        ctx->clean_tmp_folder_callback(ctx->vector_tmp_relative_path);
    }

    /// Create temp directory before serialize.
    disk->createDirectories(ctx->vector_tmp_relative_path);

    try
    {
        ctx->write_event_log(VIEventLogElement::BUILD_START, 0, "");

        /// variant index_ptr
        VIVariantPtr search_index;

        size_t add_block_size = data.getContext()->getSettingsRef().max_build_index_add_block_size;
        switch (search_type)
        {
            case Search::DataType::FloatVector:
            {
                size_t train_block_size = data.getContext()->getSettingsRef().max_build_index_train_block_size;
                VectorIndex::VIPartReader<Search::DataType::FloatVector> part_reader(
                    ctx->source_part,
                    cols,
                    ctx->metadata_snapshot,
                    data.getContext()->getMarkCache().get(),
                    ctx->build_cancel_callback,
                    dim,
                    enforce_fixed_array);

                VECTOR_INDEX_EXCEPTION_ADAPT(
                    ctx->source_column_index->buildIndex<Search::DataType::FloatVector>(
                        search_index,
                        &part_reader,
                        ctx->slow_mode,
                        train_block_size,
                        add_block_size,
                        *(ctx->index_build_memory_lock),
                        ctx->build_cancel_callback),
                    "Build Index")

                break;
            }
            case Search::DataType::BinaryVector:
            {
                size_t train_block_size = data.getContext()->getSettingsRef().max_build_binary_vector_index_train_block_size;

                VectorIndex::VIPartReader<Search::DataType::BinaryVector> part_reader(
                    ctx->source_part,
                    cols,
                    ctx->metadata_snapshot,
                    data.getContext()->getMarkCache().get(),
                    ctx->build_cancel_callback,
                    dim,
                    enforce_fixed_array);

                VECTOR_INDEX_EXCEPTION_ADAPT(
                    ctx->source_column_index->buildIndex<Search::DataType::BinaryVector>(
                        search_index,
                        &part_reader,
                        ctx->slow_mode,
                        train_block_size,
                        add_block_size,
                        *(ctx->index_build_memory_lock),
                        ctx->build_cancel_callback),
                    "Build Index")

                break;
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported vector search type");
        }


        if (!ctx->build_cancel_callback())
        {
            VECTOR_INDEX_EXCEPTION_ADAPT(
                ctx->source_column_index->serialize(search_index, disk, ctx->vector_tmp_full_path, ctx->vector_index_checksum, *ctx->index_build_memory_lock),
                "Serialize Index")

            LOG_DEBUG(ctx->log, "Build index and Serialize Vector Index finish, in {} sec.", ctx->watch.elapsedSeconds());

            ctx->build_index = search_index;
        }
        else
            return VIBuiltStatus{VIBuiltStatus::BUILD_CANCEL};
    }
    catch (Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);

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

            return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL, e.code(), e.message()};
        }

        /// retry build for MEMORY_LIMIT_EXCEEDED case
        LOG_WARNING(
            log, "Cannot build vector index {} for part {} for now due to {}.", ctx->vector_index_name, ctx->source_part->name, ErrorCodes::getName(e.code()));

        return VIBuiltStatus{VIBuiltStatus::BUILD_RETRY};
    }

    return VIBuiltStatus{VIBuiltStatus::SUCCESS};
}

/// move vector index to latest active part
VIBuiltStatus VIBuilderUpdater::TryMoveVIFiles(const VIContextPtr ctx)
{
    LOG_INFO(log, "Try to move vector index {} file to the latest active part {}.", ctx->vector_index_name, ctx->source_part->name);
    if (ctx->build_cancel_callback())
    {
        LOG_WARNING(ctx->log, "Cancel move vector index {}, because cancel build index.", ctx->vector_index_name);
        return VIBuiltStatus{VIBuiltStatus::BUILD_CANCEL};
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
            return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
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
        return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
    }

    /// Fetched part already contains vector vector index
    if (future_part->vector_index.alreadyWithVIndexSegment(ctx->vector_index_name))
    {
        LOG_WARNING(log, "Future Part {} already has built vector index. not move vector index.", future_part->name);
        return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
    }

    const DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(ctx->source_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
        return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL};

    auto disk = part_storage->getDisk();

    try
    {
        LOG_INFO(log, "Will Move Index File to Part {}", future_part->name);
        /// lock part for move build vector index, avoid concurrently mutation
        auto move_index_lock = future_part->vector_index.tryLockTimed(RWLockImpl::Type::Write, std::chrono::milliseconds(1000));
        /// check part is active
        if (future_part->getState() != MergeTreeDataPartState::Active)
        {
            LOG_WARNING(ctx->log, "Part {} is not in active state when moving index, will retry.", future_part->name);
            return VIBuiltStatus{VIBuiltStatus::BUILD_RETRY};
        }

        if (future_part->rows_count == 0)
        {
            LOG_WARNING(ctx->log, "Part {} is empty, skip build vector index.", future_part->name);
            return VIBuiltStatus{VIBuiltStatus::BUILD_CANCEL};
        }

        /// Finally, move index files to part and apply lightweight delete.
        moveVIFilesToFuturePartAndCache(
            future_part, disk, ctx->build_index, ctx->vector_tmp_relative_path, ctx->vec_index_desc, ctx->vector_index_checksum);
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
            return VIBuiltStatus{VIBuiltStatus::BUILD_RETRY};
        }

        if (ctx->source_column_index->getVectorIndexState() == VIState::BUILT)
            return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};

        return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }

    LOG_INFO(log, "Move Vector Index {} file to Part {} success.", ctx->vector_index_name, future_part->name);
    return VIBuiltStatus{VIBuiltStatus::SUCCESS};
}

void VIBuilderUpdater::moveVIFilesToFuturePartAndCache(
    const MergeTreeDataPartPtr & dest_part,
    DiskPtr disk,
    VIVariantPtr build_index,
    const String & vector_tmp_relative_path,
    const VIDescription & vec_index_desc,
    const std::shared_ptr<MergeTreeDataPartChecksums> & checksums [[maybe_unused]]/*checksums*/)
{
    String vector_index_name = vec_index_desc.name;
    LOG_DEBUG(
        log,
        "Current active part {} with state {} is selected to store vector index {}",
        dest_part->name,
        dest_part->getState(),
        vector_index_name);

    String dest_relative_path = dest_part->getDataPartStorage().getRelativePath();

    auto old_index_files = VectorIndex::getVectorIndexFileNamesInChecksums(dest_part->getDataPartStoragePtr(), vec_index_desc.name, false);
    /// Move to current part which is active.
    bool found_vector_file = false;
    Names moved_files;
    try
    {
        /// Put here to avoid concurently drop vector index
        auto lock_part = data.lockParts();

        String index_checksums_file_name = VectorIndex::getVectorIndexChecksumsFileName(vector_index_name);
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
            VectorIndex::removeVectorIndexFilesFromFileLists(dest_part->getDataPartStoragePtr(), moved_files);

        throw;
    }

    if (!found_vector_file)
    {
        LOG_WARNING(log, "Failed to find any vector index files in directory {}, will remove it", vector_tmp_relative_path);
        if (disk->exists(vector_tmp_relative_path))
            disk->removeRecursive(vector_tmp_relative_path);
        else
            LOG_WARNING(
                log, "[moveVIFilesToFuturePartAndCache] vector_tmp_relative_path doesn't exist {}", vector_tmp_relative_path);

        throw Exception(ErrorCodes::INCORRECT_DATA, "Vector index file does not exists when move vector index.");
    }

    VISegmentMetadata vector_index_segment_metadata
        = VISegmentMetadata(true, false, vec_index_desc.name, vec_index_desc.column, {});

    auto column_index = dest_part->vector_index.getColumnIndex(vec_index_desc.name).value();
    auto old_index_segment_metadata = column_index->getIndexSegmentMetadata();

    /// FIXME: Release move index lock in advance to reduce blocking time
    if (!column_index->cache(build_index))
        LOG_DEBUG(log, "Load vector index from index ptr error, index may be obtained by fetch index.");

    column_index->setIndexSegmentMetadata(vector_index_segment_metadata);

    /// Apply lightweight delete bitmap to index's bitmap, no need update lwd bitmap, because cache index already update bitmap
    if (dest_part->hasLightweightDelete())
    {
        LOG_DEBUG(log, "Apply lightweight delete to vector index {} in part {}", vector_index_name, dest_part->name);
        dest_part->onLightweightDelete(vector_index_name);
    }

    if (old_index_segment_metadata->is_ready.load() && old_index_segment_metadata->is_decouple_index)
    {
        dest_part->vector_index.decoupleIndexOffline(old_index_segment_metadata, old_index_files);
        column_index->removeDecoupleIndexInfos();
    }
}

}
