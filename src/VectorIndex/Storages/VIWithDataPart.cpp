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

#include <shared_mutex>
#include <stdexcept>
#include <omp.h>

#include <Disks/IDisk.h>
#include <IO/HashingReadBuffer.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <VectorIndex/Common/BruteForceSearch.h>
#include <VectorIndex/Common/ScanThreadLimiter.h>
#include <VectorIndex/Common/SegmentId.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/VIMetadata.h>
#include <VectorIndex/Common/VIPartReader.h>
#include <VectorIndex/Common/VectorDataset.h>
#include <VectorIndex/Common/VectorIndexIO.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <VectorIndex/Storages/VIWithDataPart.h>
#include <VectorIndex/Utils/VIUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_VECTOR_INDEX;
    extern const int CORRUPTED_DATA;
    extern const int STD_EXCEPTION;
    extern const int DEADLOCK_AVOIDED;
}

std::once_flag MergeTreeDataPartColumnIndex::once;
int MergeTreeDataPartColumnIndex::max_threads = getNumberOfPhysicalCPUCores() * 2;

void MergeTreeDataPartColumnIndex::transferToNewRowIds(const VIWithMeta & index_with_meta, SearchResultPtr result)
{
    if (!index_with_meta.row_ids_map || index_with_meta.row_ids_map->empty())
        return;

    for (size_t k = 0; k < result->numQueries(); k++)
    {
        for (auto & label : result->getResultIndices(k))
            if (label != -1)
                label = (*index_with_meta.row_ids_map)[label];
    }
}

SearchResultPtr MergeTreeDataPartColumnIndex::TransferToOldRowIds(const VIWithMeta & index_with_meta, const SearchResultPtr result)
{
    if (!index_with_meta.inverted_row_ids_map || index_with_meta.inverted_row_ids_map->empty())
        return result;

    if (!result)
    {
        LOG_DEBUG(&Poco::Logger::get("TransferToOldRowIds"), "SearchResult is NIL");
        return result;
    }

    if (index_with_meta.inverted_row_sources_map->empty())
    {
        LOG_DEBUG(&Poco::Logger::get("TransferToOldRowIds"), "Skip to transfter to old row ids");
        return nullptr;
    }

    long inverted_size = static_cast<long>(index_with_meta.inverted_row_sources_map->size());

    /// Transfer row IDs in the decoupled data part to real row IDs of the old data part.
    /// TODO: Not handle batch distance cases.
    auto new_distances = result->getResultDistances();
    auto new_ids = result->getResultIndices();

    std::vector<UInt64> real_row_ids;
    std::vector<Float32> distances;
    for (int i = 0; i < result->getNumCandidates(); i++)
    {
        auto new_row_id = new_ids[i];

        if (new_row_id == -1 || new_row_id >= inverted_size)
            continue;

        if (index_with_meta.own_id == (*(index_with_meta.inverted_row_sources_map))[new_row_id])
        {
            real_row_ids.emplace_back((*(index_with_meta.inverted_row_ids_map))[new_row_id]);
            distances.emplace_back(new_distances[i]);
        }
    }

    if (real_row_ids.size() == 0)
        return nullptr;

    /// Prepare search result for this old part
    size_t real_num_reorder = real_row_ids.size();
    SearchResultPtr real_search_result = SearchResult::createTopKHolder(result->numQueries(), real_num_reorder);

    auto per_ids = real_search_result->getResultIndices();
    auto per_distances = real_search_result->getResultDistances();

    for (size_t i = 0; i < real_num_reorder; i++)
    {
        per_ids[i] = real_row_ids[i];
        per_distances[i] = distances[i];
    }

    return real_search_result;
}

bool MergeTreeDataPartColumnIndex::supportTwoStageSearch(const VIWithMeta & index_with_meta)
{
    bool isSupport = false;
    std::visit([&](auto && index) { isSupport = index->supportTwoStageSearch(); }, index_with_meta.index);
    return isSupport;
}

MergeTreeDataPartColumnIndex::MergeTreeDataPartColumnIndex(const IMergeTreeDataPart & part, const VIDescription & vec_desc)
    : MergeTreeDataPartColumnIndex(
        part.storage.getStorageID(),
        part.name,
        vec_desc,
        part.rows_count,
        part.storage.getInMemoryMetadataPtr(),
        part.getDataPartStoragePtr(),
        part.storage.getSettings())
{
}

MergeTreeDataPartColumnIndex::MergeTreeDataPartColumnIndex(
    const StorageID & storage_id_,
    const String & current_part_name_,
    const VIDescription & vec_desc,
    const UInt64 rows_count_,
    StorageMetadataPtr metadata_snapshot,
    DataPartStoragePtr part_storage_,
    MergeTreeSettingsPtr merge_tree_setting)
    : storage_id(storage_id_)
    , current_part_name(current_part_name_)
    , index_name(vec_desc.name)
    , column_name(vec_desc.column)
    , total_vec(rows_count_)
    , vector_search_type(vec_desc.vector_search_type)
    , part_storage(part_storage_)
    , build_index_info(std::make_shared<VectorIndexBuildInfo>())
{
    dimension = getVectorDimension(vec_desc.vector_search_type, *metadata_snapshot, vec_desc.column);

    if (total_vec * getEachVectorBytes(vector_search_type, dimension) < merge_tree_setting->min_bytes_to_build_vector_index)
    {
        index_type = fallbackToFlat(vec_desc.vector_search_type);
        fallback_to_flat = true;
    }
    else
    {
        index_type = Search::getVectorIndexType(vec_desc.type, vector_search_type);
        fallback_to_flat = false;
    }

    vector_index_segment_metadata.set(
        std::make_unique<VectorIndexSegmentMetadata>(false, false, vec_desc.name, vec_desc.column, std::vector<MergedPartNameAndId>()));

    if (vec_desc.parameters && vec_desc.parameters->has("metric_type"))
        metric_str = vec_desc.parameters->getValue<String>("metric_type");
    else
    {
        if (vec_desc.vector_search_type == Search::DataType::FloatVector)
            metric_str = merge_tree_setting->float_vector_search_metric_type;
        else
            metric_str = merge_tree_setting->binary_vector_search_metric_type;
    }

    des = convertPocoJsonToMap(vec_desc.parameters);

    des.erase("metric_type");
}

void MergeTreeDataPartColumnIndex::onBuildStart()
{
    std::lock_guard lock(build_index_info->build_info_mutex);
    build_index_info->state = VectorIndexState::BUILDING;
    build_index_info->watch = std::make_unique<Stopwatch>();
}

void MergeTreeDataPartColumnIndex::onBuildFinish(bool is_success, const String & err_msg_)
{
    std::lock_guard lock(build_index_info->build_info_mutex);
    if (build_index_info->watch != nullptr)
        build_index_info->watch->stop();

    if (is_success)
    {
        build_index_info->state = VectorIndexState::BUILT;
        build_index_info->err_msg = "";
    }
    else
    {
        build_index_info->state = VectorIndexState::ERROR;
        build_index_info->err_msg = err_msg_;
    }
}

double MergeTreeDataPartColumnIndex::getBuildElapsed()
{
    std::lock_guard lock(build_index_info->build_info_mutex);
    return build_index_info->watch == nullptr ? 0 : build_index_info->watch->elapsedSeconds();
}

VectorIndexState MergeTreeDataPartColumnIndex::getVectorIndexState()
{
    std::lock_guard lock(build_index_info->build_info_mutex);
    return build_index_info->state;
}

const String MergeTreeDataPartColumnIndex::getLastBuildErrMsg()
{
    std::lock_guard lock(build_index_info->build_info_mutex);
    return build_index_info->err_msg;
}

void MergeTreeDataPartColumnIndex::shutdown()
{
    if (is_shutdown.load())
        return;
    is_shutdown.store(true);

    cancelBuild();

    auto * vector_index_segment = const_cast<VectorIndexSegmentMetadata *>(getIndexSegmentMetadata().get());
    auto segment_ids = getAllSegmentIds(part_storage, current_part_name, *vector_index_segment);
    vector_index_segment->is_ready.store(false);

    /// force cache expire
    for (auto & segment_id : segment_ids)
    {
        VICacheManager::removeFromCache(segment_id.getOriginalCacheKey());
        VICacheManager::removeFromCache(segment_id.getCacheKey());
    }
}

VectorIndexInfoPtrList MergeTreeDataPartColumnIndex::getVectorIndexInfos()
{
    VectorIndexInfoPtrList res;
    {
        std::lock_guard lock(vector_info_mutex);
        if (vector_infos.empty())
            return res;
        vector_infos.at(0)->state = getVectorIndexState();
        vector_infos.at(0)->err_msg = getLastBuildErrMsg();
        vector_infos.at(0)->elapsed = getBuildElapsed();
        for (auto info : vector_infos)
            res.emplace_back(info);
    }
    return res;
}

void MergeTreeDataPartColumnIndex::removeDecoupleIndexInfos()
{
    std::lock_guard lock(vector_info_mutex);
    if (!vector_infos.empty())
        vector_infos.erase(vector_infos.begin() + 1, vector_infos.end());
}

void MergeTreeDataPartColumnIndex::removeAllIndexInfos()
{
    std::lock_guard lock(vector_info_mutex);
    vector_infos.clear();
}

void MergeTreeDataPartColumnIndex::updateIndexBuildInfo(VectorIndexBuildInfoPtr build_index_info_)
{
    std::atomic_store(&build_index_info, build_index_info_);
}

void MergeTreeDataPartColumnIndex::initVectorIndexInfo(bool is_small_part)
{
    auto segment_ids = getAllSegmentIds(part_storage, current_part_name, *getIndexSegmentMetadata());
    if (!segment_ids.empty() && !segment_ids[0].fromMergedParts())
    {
        /// vpart initialize index info
        std::lock_guard lock(vector_info_mutex);
        build_index_info->state = VectorIndexState::BUILT;
        vector_infos.emplace_back(std::make_shared<VIInfo>(
            storage_id.database_name, storage_id.table_name, index_name, segment_ids[0], build_index_info->state));
    }
    else
    {
        /// Both "dpart" and "part," when lacking a single index, require the addition of pending state index information,
        /// as they both necessitate the rebuilding of vector indices. 
        /// However, "dpart" additionally requires the initialization of other vector index information (merged source parts index).
        build_index_info->state = is_small_part ? VectorIndexState::SMALL_PART : VectorIndexState::PENDING;
        vector_infos.emplace_back(
            std::make_shared<VIInfo>(storage_id.database_name, storage_id.table_name, *this, build_index_info->state));
        for (auto & segment_id : segment_ids)
        {
            std::lock_guard lock(vector_info_mutex);
            auto vector_index_info = std::make_shared<VIInfo>(
                storage_id.database_name, storage_id.table_name, index_name, segment_id, VectorIndexState::BUILT);
            vector_infos.emplace_back(vector_index_info);
        }
    }
}

bool MergeTreeDataPartColumnIndex::canMergeForColumnIndex(const MergeTreeDataPartPtr & left, const MergeTreeDataPartPtr & right, const String & vec_index_name)
{
    auto left_column_index_opt = left->vector_index.getColumnIndex(vec_index_name);
    auto right_column_index_opt = right->vector_index.getColumnIndex(vec_index_name);
    if (!left_column_index_opt.has_value() || !right_column_index_opt.has_value())
        return false;
    
    auto left_column_index = left_column_index_opt.value();
    auto right_column_index = right_column_index_opt.value();

    if ((left_column_index->isDecoupleIndexFile() && left->rows_count != 0) || (right_column_index->isDecoupleIndexFile() && right->rows_count != 0))
            return false;

    auto left_index_status = left_column_index->getVectorIndexState();
    auto right_index_status = right_column_index->getVectorIndexState();

    if (left_index_status <= VectorIndexState::PENDING && right_index_status <= VectorIndexState::PENDING)
        return true;
    else if (
        (left_index_status <= VectorIndexState::PENDING && right_index_status == VectorIndexState::ERROR)
        || (right_index_status <= VectorIndexState::PENDING && left_index_status == VectorIndexState::ERROR))
        return true;
    else if (left_index_status == VectorIndexState::BUILT && right_index_status == VectorIndexState::BUILT)
        return true;
    else if (left_index_status == VectorIndexState::ERROR && right_index_status == VectorIndexState::ERROR)
        return true;
    else if (
        (left_index_status == VectorIndexState::SMALL_PART && right_index_status == VectorIndexState::BUILT)
        || (right_index_status == VectorIndexState::SMALL_PART && left_index_status == VectorIndexState::BUILT))
        return true;

    return false;
}

size_t MergeTreeDataPartColumnIndex::getMemoryUsage() const
{
    VIVariantPtr index_variant;
    try
    {
        VECTOR_INDEX_EXCEPTION_ADAPT(index_variant = createIndex(), "getMemoryUsage")
    }
    catch (Exception & e)
    {
        LOG_DEBUG(log, "Failed to build dummy index while getting resource usage, error {}: {}", e.code(), e.message());
        return 0;
    }
    size_t memory_usage = 0;
    std::visit([&](auto && index_ptr) { memory_usage = index_ptr->getResourceUsage().memory_usage_bytes; }, index_variant);
    return memory_usage;
}

size_t MergeTreeDataPartColumnIndex::getDiskUsage() const
{
    VIVariantPtr index_variant;
    try
    {
        VECTOR_INDEX_EXCEPTION_ADAPT(index_variant = createIndex(), "getDiskUsage")
    }
    catch (Exception & e)
    {
        LOG_DEBUG(log, "Failed to build dummy index while getting resource usage, error {}: {}", e.code(), e.message());
        return 0;
    }
    size_t disk_usage = 0;
    std::visit([&](auto && index_ptr) { disk_usage = index_ptr->getResourceUsage().disk_usage_bytes; }, index_variant);
    return disk_usage;
}

void MergeTreeDataPartColumnIndex::cancelBuild()
{
    if (!build_index_info->cancel_build.load())
        build_index_info->cancel_build.store(true);
}

VIVariantPtr MergeTreeDataPartColumnIndex::createIndex() const
{
    auto metric = Search::getMetricType(metric_str, vector_search_type);
    VIVariantPtr index_variant;
    VIParameter index_des = des;
    if (fallback_to_flat)
        std::erase_if(
            index_des,
            [](const auto & item)
            {
                auto const & [key, value] = item;
                return key != "metric_type";
            });

    const String vector_index_cache_prefix
        = getUniqueVectorIndexCachePrefix(part_storage->getRelativePath(), current_part_name, index_name);

    if (vector_search_type == Search::DataType::FloatVector)
        index_variant = Search::createVectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::FloatVector>(
            index_name,
            index_type,
            metric,
            dimension,
            total_vec,
            index_des,
            vector_index_cache_prefix,
            true /* use_file_checksum */,
            true /* manage_cache_folder */);
    else if (vector_search_type == Search::DataType::BinaryVector)
        index_variant = Search::createVectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::BinaryVector>(
            index_name,
            index_type,
            metric,
            dimension,
            total_vec,
            index_des,
            vector_index_cache_prefix,
            true /* use_file_checksum */,
            true /* manage_cache_folder */);

    return index_variant;
}

#ifdef ENABLE_SCANN
std::shared_ptr<DiskIOManager> MergeTreeDataPartColumnIndex::getDiskIOManager() const
{
    return nullptr;

}
#endif

void MergeTreeDataPartColumnIndex::serialize(
    VIVariantPtr & index,
    DiskPtr disk,
    const String & serialize_local_folder,
    std::shared_ptr<MergeTreeDataPartChecksums> & vector_index_checksum,
    VIBuildMemoryUsageHelper & /*build_memory_lock*/)
{
    auto index_serialize_folder = fs::path(serialize_local_folder) / std::string(index_name + "-");
    auto index_checksums = std::make_shared<MergeTreeDataPartChecksums>();
    LOG_INFO(log, "index serialize path: {}", index_serialize_folder);
    auto file_writer = Search::IndexDataFileWriter<VectorIndexOStream>(
        index_serialize_folder,
        [&](const std::string & name, std::ios::openmode /*mode*/)
        { return std::make_shared<VectorIndexWriter>(disk, name, index_checksums); });

    String version;
    String memory_usage;
    String disk_usage;
    std::visit(
        [&](auto && index_ptr)
        {
            index_ptr->serialize(&file_writer);
            index_ptr->saveDataID(&file_writer);

            printMemoryInfo(log, "After serialization");

            version = index_ptr->getVersion().toString();
            auto usage = index_ptr->getResourceUsage();
            memory_usage = std::to_string(usage.memory_usage_bytes);
            disk_usage = std::to_string(usage.disk_usage_bytes);
        },
        index);

    LOG_INFO(
        log,
        "Index type: {}, version: {}, memory_usage_bytes: {}, disk_usage_bytes: {}",
        Search::enumToString(index_type),
        version,
        memory_usage,
        disk_usage);
    std::unordered_map<std::string, std::string> infos;

    infos["memory_usage_bytes"] = memory_usage;
    infos["disk_usage_bytes"] = disk_usage;

    VIParameter index_des = des;

    if (index_type == VIType::FLAT)
        std::erase_if(
            index_des,
            [](const auto & item)
            {
                auto const & [key, value] = item;
                return key != "metric_type";
            });
    auto segment_id = getCurrentFakeSegmentId();
    auto metric = Search::getMetricType(metric_str, vector_search_type);
    VIMetadata metadata(segment_id, version, index_type, metric, dimension, total_vec, fallback_to_flat, index_des, infos);

    auto buf = disk->writeFile(index_serialize_folder.string() + VECTOR_INDEX_DESCRIPTION + VECTOR_INDEX_FILE_SUFFIX, 4096);

    metadata.writeText(*buf);
    buf->finalize();

    /// Calculate vector index checksums
    vector_index_checksum = std::make_shared<MergeTreeDataPartChecksums>(
        calculateVectorIndexChecksums(part_storage, serialize_local_folder, index_checksums));

    String checksum_file_path = index_serialize_folder.string() + VECTOR_INDEX_CHECKSUMS + VECTOR_INDEX_FILE_SUFFIX;
    LOG_INFO(log, "Write {} checksum file {}", index_name, checksum_file_path);

    auto out_checksums = disk->writeFile(checksum_file_path, 4096);
    vector_index_checksum->write(*out_checksums);
    out_checksums->finalize();
}

IndexWithMetaHolderPtr MergeTreeDataPartColumnIndex::loadDecoupleCache(SegmentId & segment_id)
{
    if (!segment_id.fromMergedParts())
        return nullptr;

    CacheKey ori_cache_key = segment_id.getOriginalCacheKey();
    VICacheManager * mgr = VICacheManager::getInstance();

    IndexWithMetaHolderPtr index_holder = mgr->get(segment_id.getCacheKey());
    if (index_holder)
        return index_holder;

    IndexWithMetaHolderPtr ori_index_holder = mgr->get(ori_cache_key);
    if (ori_index_holder)
    {
        auto new_cache_item = ori_index_holder->value().clone();
        ori_index_holder.reset();
        auto load_func = [new_cache_item = new_cache_item, segment_id = segment_id]() mutable -> VectorIndexWithMetaPtr
        {
            auto [row_ids_map, inverted_row_ids_map, inverted_row_sources_map] = segment_id.getMergedMaps();
            new_cache_item->row_ids_map = row_ids_map;
            new_cache_item->inverted_row_ids_map = inverted_row_ids_map;
            new_cache_item->inverted_row_sources_map = inverted_row_sources_map;
            new_cache_item->own_id = segment_id.getOwnPartId();

            /// update delete bitmap
            auto del_row_ids = readDeleteBitmapAccordingSegmentId(segment_id);

            auto delete_bitmap = std::make_shared<VIBitmap>(new_cache_item->total_vec, true);
            auto cache_delete_bitmap = new_cache_item->getDeleteBitmap();

            convertBitmap(segment_id, del_row_ids, delete_bitmap, row_ids_map, inverted_row_ids_map, inverted_row_sources_map);

            auto final_delete_bitmap = Search::intersectDenseBitmaps(delete_bitmap, cache_delete_bitmap);
            new_cache_item->setDeleteBitmap(final_delete_bitmap);

            return new_cache_item;
        };

        index_holder = mgr->get(segment_id.getCacheKey());
        if (index_holder)
            return index_holder;

        index_holder = mgr->load(segment_id.getCacheKey(), load_func);
        return index_holder;
    }

    return nullptr;
}

/// cancel load vector index implement
IndexWithMetaHolderPtr MergeTreeDataPartColumnIndex::load(SegmentId & segment_id, bool is_active, const String & nvme_cache_path_uuid)
{
    OpenTelemetry::SpanHolder span("MergeTreeDataPartColumnIndex::load");
    VICacheManager * mgr = VICacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();
    const String cache_key_str = cache_key.toString();

    LOG_DEBUG(log, "segment_id.getPathPrefix() = {}", segment_id.getPathPrefix());
    LOG_DEBUG(log, "cache_key_str = {}", cache_key_str);

    auto write_event_log = [&](VIEventLogElement::Type event, int code = 0, String msg = "")
    {
        VIEventLog::addEventLog(
            Context::getGlobalContextInstance(),
            cache_key.getTableUUID(),
            cache_key.getIndexName(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            event,
            cache_key.getCurPartName(),
            ExecutionStatus(code, msg));
    };

    IndexWithMetaHolderPtr index_holder = mgr->get(cache_key);
    if (!index_holder)
    {
        auto decoupel_index_holder = loadDecoupleCache(segment_id);
        if (decoupel_index_holder)
            return decoupel_index_holder;

        write_event_log(VIEventLogElement::LOAD_START);
        auto load_func = [&]() -> VectorIndexWithMetaPtr
        {
            LOG_INFO(log, "loading vector index from {}", segment_id.getFullPath());
            if (!is_active)
                /// InActive part, Cancel Load Vector Index
                throw Exception(ErrorCodes::INVALID_VECTOR_INDEX, "Part is inactive, will not reload index!");

            VIMetadata metadata(segment_id);
            auto buf = segment_id.getDisk()->readFile(segment_id.getVectorDescriptionFilePath());
            metadata.readText(*buf);

            if (!segment_id.getDisk()->exists(segment_id.getVectorDescriptionFilePath()))
                throw VIException(
                    ErrorCodes::CORRUPTED_DATA,
                    "Does not exists {}, Index is not in the ready state and cannot be loaded",
                    segment_id.getVectorDescriptionFilePath());

            auto check_index_expired = [segment_id = segment_id]() mutable -> bool
            {
                return !segment_id.getDisk()->exists(segment_id.getVectorDescriptionFilePath());
            };

            bool index_fallback_to_flat = metadata.fallback_to_flat;
            VIType load_index_type = index_type;
            VIParameter index_params = des;
            if (index_fallback_to_flat)
            {
                load_index_type = fallbackToFlat(vector_search_type);
                std::erase_if(
                    index_params,
                    [](const auto & item)
                    {
                        auto const & [key, value] = item;
                        return key != "metric_type";
                    });
            }
            index_params.setParam("load_index_version", metadata.version);

            String vector_index_cache_prefix = getUniqueVectorIndexCachePrefix(
                part_storage->getRelativePath(), segment_id.getCacheKey().part_name_no_mutation, index_name, nvme_cache_path_uuid);
            VIVariantPtr index_variant;

            if (vector_search_type == Search::DataType::FloatVector)
                index_variant = Search::createVectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::FloatVector>(
                    index_name,
                    load_index_type,
                    metadata.metric,
                    metadata.dimension,
                    metadata.total_vec,
                    index_params,
                    vector_index_cache_prefix,
                    true /* use_file_checksum */,
                    true /* manage_cache_folder */);
            else if (vector_search_type == Search::DataType::BinaryVector)
                index_variant = Search::createVectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::BinaryVector>(
                    index_name,
                    load_index_type,
                    metadata.metric,
                    metadata.dimension,
                    metadata.total_vec,
                    index_params,
                    vector_index_cache_prefix,
                    true /* use_file_checksum */,
                    true /* manage_cache_folder */);

            auto file_reader = Search::IndexDataFileReader<VectorIndexIStream>(
                segment_id.getFullPath(),
                [disk = segment_id.getDisk()](const std::string & name, std::ios::openmode /*mode*/)
                { return std::make_shared<VectorIndexReader>(disk, name); });

            UInt64 index_total_vec = 0;
            printMemoryInfo(log, "Before load");
            std::visit(
                [&](auto && index_ptr)
                {
                    index_ptr->load(&file_reader, check_index_expired);
                    index_ptr->loadDataID(&file_reader);
                    index_total_vec = index_ptr->numData();
                },
                index_variant);
            printMemoryInfo(log, "After load");
            assert(index_total_vec = metadata.total_vec);
            LOG_INFO(log, "load total_vec={}", index_total_vec);

            auto [row_ids_map, inverted_row_ids_map, inverted_row_sources_map] = segment_id.getMergedMaps();

            auto del_row_ids = readDeleteBitmapAccordingSegmentId(segment_id);

            auto delete_bitmap = std::make_shared<VIBitmap>(index_total_vec, true);

            convertBitmap(segment_id, del_row_ids, delete_bitmap, row_ids_map, inverted_row_ids_map, inverted_row_sources_map);

            /// rechack index valid
            if (!segment_id.getDisk()->exists(segment_id.getVectorDescriptionFilePath()))
                throw VIException(
                    ErrorCodes::CORRUPTED_DATA,
                    "Does not exists {}, Index is not in the ready state and cannot be loaded",
                    segment_id.getVectorDescriptionFilePath());

            return std::make_shared<VIWithMeta>(
                index_variant,
                index_total_vec,
                delete_bitmap,
                index_params,
                row_ids_map,
                inverted_row_ids_map,
                inverted_row_sources_map,
                segment_id.getOwnPartId(),
                disk_mode,
                index_fallback_to_flat,
                vector_index_cache_prefix);
        };

        try
        {
            VECTOR_INDEX_EXCEPTION_ADAPT(index_holder = mgr->load(cache_key, load_func), "Load Index")

            if (!index_holder)
            {
                LOG_WARNING(
                    log,
                    "Fail to load vector index {}, cache key: {}, it might be that the cache size is not enough.",
                    index_name,
                    cache_key.toString());
                write_event_log(VIEventLogElement::LOAD_FAILED);
            }
            else
            {
                write_event_log(VIEventLogElement::LOAD_SUCCEED);
            }
        }
        catch (const Exception & e)
        {
            LOG_WARNING(
                log, "Fail to load vector index {}, cache key: {}, error {}: {}", index_name, cache_key.toString(), e.code(), e.message());
            write_event_log(VIEventLogElement::LOAD_ERROR, e.code(), e.message());
            return nullptr;
        }
    }

    return index_holder;
}

bool MergeTreeDataPartColumnIndex::cache(VIVariantPtr index)
{
    auto segment_id = getCurrentFakeSegmentId();
    bool isNullptr = true;
    std::visit(
        [&](auto && index_ptr)
        {
            if (index_ptr)
                isNullptr = false;
        },
        index);

    if (isNullptr)
    {
        LOG_WARNING(log, "{} index is null, not caching", segment_id.getCacheKey().toString());
        return false;
    }

    VICacheManager * mgr = VICacheManager::getInstance();

    auto part_deleted_row_ids = readDeleteBitmapAccordingSegmentId(segment_id);
    auto delete_bitmap = std::make_shared<VIBitmap>(total_vec, true);
    convertBitmap(segment_id, part_deleted_row_ids, delete_bitmap, nullptr, nullptr, nullptr);

    String vector_index_cache_prefix = getUniqueVectorIndexCachePrefix(part_storage->getRelativePath(), current_part_name, index_name);
    /// when cacheIndexAndMeta() is called, related files should have already been loaded.
    VectorIndexWithMetaPtr cache_item = std::make_shared<VIWithMeta>(
        index, total_vec, delete_bitmap, des, nullptr, nullptr, nullptr, 0, disk_mode, fallback_to_flat, vector_index_cache_prefix);

    mgr->put(segment_id.getCacheKey(), cache_item);

    return true;
}


std::vector<IndexWithMetaHolderPtr> MergeTreeDataPartColumnIndex::getIndexHolders(bool is_active)
{
    std::vector<IndexWithMetaHolderPtr> ret;
    std::vector<SegmentId> segment_ids;

    segment_ids = getAllSegmentIds(part_storage, current_part_name, *getIndexSegmentMetadata());

    for (auto & segment_id : segment_ids)
    {
        auto index_holder = load(segment_id, is_active);
        if (!index_holder)
        {
            ret.clear();
            break;
        }
        ret.emplace_back(std::move(index_holder));
    }

    if (!ret.empty())
        return ret;

    /// Re-acquire segment_id. Previously, the acquisition of index holer may have failed due to the conversion of dpart to vpart.
    segment_ids = getAllSegmentIds(part_storage, current_part_name, *getIndexSegmentMetadata());

    /// It is still a decouple part or no ready index, so there is no need to reload it.
    if (segment_ids.size() != 1)
        return ret;

    auto index_holder = load(segment_ids[0], is_active);
    if (!index_holder)
        return ret;

    ret.emplace_back(std::move(index_holder));

    return ret;
}

SearchResultPtr MergeTreeDataPartColumnIndex::computeTopDistanceSubset(
    const VIWithMeta & index_with_meta, VectorDatasetVariantPtr queries, SearchResultPtr first_stage_result, int32_t top_k)
{
    auto index_variant = index_with_meta.index;
    if (vector_search_type != Search::DataType::FloatVector)
        throw VIException(ErrorCodes::LOGICAL_ERROR, "Only Float32 Vector support two stage vector search");

    if (!std::holds_alternative<FloatVIPtr>(index_variant) || !std::holds_alternative<Float32VectorDatasetPtr>(queries))
        throw VIException(ErrorCodes::LOGICAL_ERROR, "Vector index type and dataset type do not match search type");

    FloatVIPtr float_index = std::get<FloatVIPtr>(index_variant);
    Float32VectorDatasetPtr float32_dataset = std::get<Float32VectorDatasetPtr>(queries);

    auto search_queries = std::make_shared<Search::DataSet<float>>(
        float32_dataset->getData(), float32_dataset->getVectorNum(), float32_dataset->getDimension());
    auto ret = float_index->computeTopDistanceSubset(search_queries, first_stage_result, top_k);
    transferToNewRowIds(index_with_meta, ret);
    return ret;
}

SearchResultPtr MergeTreeDataPartColumnIndex::search(
    const VIWithMeta & index_with_meta,
    VectorDatasetVariantPtr queries,
    int32_t k,
    VIBitmapPtr filter,
    VIParameter & parameters,
    bool first_stage_only)
{
    // Check if the index is initialized and ready for searching
    auto & index_variant = index_with_meta.index;
    if ((vector_search_type == Search::DataType::FloatVector && std::holds_alternative<FloatVIPtr>(index_variant)
         && std::holds_alternative<Float32VectorDatasetPtr>(queries))
        || (vector_search_type == Search::DataType::BinaryVector && std::holds_alternative<BinaryVIPtr>(index_variant)
            && std::holds_alternative<BinaryVectorDatasetPtr>(queries)))
    {
        std::visit(
            [&](auto && index_ptr)
            {
                if (index_ptr == nullptr)
                    throw VIException(ErrorCodes::LOGICAL_ERROR, "Index not initialized before searching!");
                if (!index_ptr->ready())
                    throw VIException(ErrorCodes::LOGICAL_ERROR, "Index not ready before searching!");
            },
            index_variant);

        std::visit(
            [&](auto && query_dataset)
            {
                // Check if the dimensions of the searched index and input match
                if (query_dataset->getDimension() != static_cast<int64_t>(dimension))
                    throw VIException(ErrorCodes::LOGICAL_ERROR, "The dimension of searched index and input doesn't match.");
            },
            queries);
    }
    else
    {
        throw VIException(ErrorCodes::LOGICAL_ERROR, "Vector index type and dataset type do not match search type");
    }

    LOG_DEBUG(log, "Index {} has {} vectors", index_name, this->total_vec);

    /// Limit the number of bruteforce search threads to 2 * number of physical cores
    static LimiterSharedContext brute_force_context(getNumberOfPhysicalCPUCores() * 2);
    ScanThreadLimiter limiter(brute_force_context, log);

    auto merged_filter = filter;
    // Merge filter and delete_bitmap
    auto delete_bitmap = index_with_meta.getDeleteBitmap();

    if (!delete_bitmap->all())
        merged_filter = Search::intersectDenseBitmaps(filter, delete_bitmap);

    if (index_with_meta.fallback_to_flat)
        parameters.clear();

    try
    {
        SearchResultPtr ret;

        {
            OpenTelemetry::SpanHolder span_search("MergeTreeDataPartColumnIndex::performSearch()::search");
            if (vector_search_type == Search::DataType::FloatVector)
            {
                Float32VectorDatasetPtr & float_query_dataset = std::get<Float32VectorDatasetPtr>(queries);
                auto search_queries
                    = std::make_shared<Search::DataSet<SearchIndexDataTypeMap<Search::DataType::FloatVector>::IndexDatasetType>>(
                        float_query_dataset->getData(), float_query_dataset->getVectorNum(), float_query_dataset->getDimension());
                const FloatVIPtr & float_index = std::get<FloatVIPtr>(index_variant);
                ret = float_index->search(search_queries, k, parameters, first_stage_only, merged_filter.get());
            }
            else if (vector_search_type == Search::DataType::BinaryVector)
            {
                BinaryVectorDatasetPtr & binary_query_dataset = std::get<BinaryVectorDatasetPtr>(queries);
                auto search_queries
                    = std::make_shared<Search::DataSet<SearchIndexDataTypeMap<Search::DataType::BinaryVector>::IndexDatasetType>>(
                        binary_query_dataset->getBoolData(), binary_query_dataset->getVectorNum(), binary_query_dataset->getDimension());
                const BinaryVIPtr & binary_index = std::get<BinaryVIPtr>(index_variant);
                ret = binary_index->search(search_queries, k, parameters, first_stage_only, merged_filter.get());
            }
        }

        // Transfer the results to newRowIds
        /// if (!first_stage_only) Use new row ids for decouple part
        {
            OpenTelemetry::SpanHolder span_transfer_id("MergeTreeDataPartColumnIndex::performSearch()::transferToNewRowIds");
            transferToNewRowIds(index_with_meta, ret);
        }

        return ret;
    }
    catch (const SearchIndexException & e)
    {
        LOG_ERROR(log, "SearchIndexException: {}", e.what());
        throw VIException(e.getCode(), e.what());
    }
    catch (const std::exception & e)
    {
        throw VIException(ErrorCodes::STD_EXCEPTION, e.what());
    }
}

bool MergetreeDataPartVectorIndex::isBuildCancelled(const String & index_name)
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    if (auto it = vector_indices.find(index_name); it != vector_indices.end())
        return it->second->isBuildCancelled();
    return true;
}

void MergetreeDataPartVectorIndex::cancelIndexBuild(const String & index_name)
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    if (auto it = vector_indices.find(index_name); it != vector_indices.end())
        it->second->cancelBuild();
}

void MergetreeDataPartVectorIndex::cancelAllIndexBuild()
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    for (auto it : vector_indices)
        it.second->cancelBuild();
}

/// revert according std::optional
std::optional<MergeTreeDataPartColumnIndexPtr> MergetreeDataPartVectorIndex::getColumnIndex(const String & index_name)
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    if (auto it = vector_indices.find(index_name); it != vector_indices.end())
        return it->second;
    return std::nullopt;
}

std::optional<MergeTreeDataPartColumnIndexPtr> MergetreeDataPartVectorIndex::getColumnIndexByColumnName(const String & column_name)
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    for (auto it : vector_indices)
    {
        if (it.second->column_name == column_name)
            return it.second;
    }
    return std::nullopt;
}


void MergetreeDataPartVectorIndex::addVectorIndex(const VIDescription & vec_desc)
{
    MergeTreeDataPartColumnIndexPtr column_index = std::make_shared<MergeTreeDataPartColumnIndex>(part, vec_desc);
    {
        std::unique_lock<std::shared_mutex> lock(vector_indices_mutex);
        if (auto it = vector_indices.find(vec_desc.name); it != vector_indices.end())
            return;
        vector_indices[vec_desc.name] = column_index;
        vector_indices[vec_desc.name]->initVectorIndexInfo(part.isSmallPart());
    }
}

/// [TODO] Behavior definition for adding the same index multiple times
void MergetreeDataPartVectorIndex::addVectorIndex(const String & index_name, MergeTreeDataPartColumnIndexPtr column_index)
{
    std::unique_lock<std::shared_mutex> lock(vector_indices_mutex);
    if (!vector_indices.try_emplace(index_name, column_index).second)
    {
        LOG_DEBUG(log, "already add index: {} in part: {}, will replace", index_name, part.name);
        auto it = vector_indices.find(index_name);
        it->second->shutdown();
        it->second = column_index;
    }
    vector_indices[index_name]->initVectorIndexInfo(part.isSmallPart());
}

void MergetreeDataPartVectorIndex::removeVectorIndex(const String & index_name)
{
    std::unique_lock<std::shared_mutex> lock(vector_indices_mutex);
    if (auto it = vector_indices.find(index_name); it != vector_indices.end())
    {
        it->second->shutdown();
        /// remove index file
        removeIndexFile(it->second->part_storage, index_name, true);
        VIEventLog::addEventLog(
            Context::getGlobalContextInstance(),
            part.storage.getStorageID().database_name,
            part.storage.getStorageID().table_name,
            index_name,
            part.name,
            part.info.partition_id,
            VIEventLogElement::CLEARED,
            part.name);
    }
    vector_indices.erase(index_name);
}

void MergetreeDataPartVectorIndex::convertIndexFileForUpgrade()
{
    IDataPartStorage & part_storage = const_cast<IDataPartStorage &>(part.getDataPartStorage());

    /// If checksums file needs to be generated.
    bool has_intact_old_version_vector_index = false;

    /// Only supports either all index versions are in V1, or all versions are in V2
    String old_vector_index_ready_v1 = toString("vector_index_ready") + VECTOR_INDEX_FILE_OLD_SUFFIX;
    String old_vector_index_ready_v2 = toString("vector_index_ready_v2") + VECTOR_INDEX_FILE_OLD_SUFFIX;
    String old_index_description_v2 = toString(VECTOR_INDEX_DESCRIPTION) + VECTOR_INDEX_FILE_OLD_SUFFIX;

    /// Support multiple vector indices
    auto metadata_snapshot = part.storage.getInMemoryMetadataPtr();
    auto vec_indices = metadata_snapshot->getVectorIndices();

    /// Only one vector index is supported before upgrade to support multiple.
    /// Use the first vector index description.
    auto vector_index_desc = vec_indices[0];
    String current_index_description_name = toString(VECTOR_INDEX_DESCRIPTION) + VECTOR_INDEX_FILE_SUFFIX;
    String current_checksums_file_name = getVectorIndexChecksumsFileName(vector_index_desc.name);
    String new_description_file_name = getVectorIndexDescriptionFileName(vector_index_desc.name);

    /// Quick check the existence of new description file with index name.
    bool from_checksums = false;
    if (part_storage.exists(current_checksums_file_name))
    {
        from_checksums = true;
        if (part_storage.exists(new_description_file_name))
        {
            /// The current version file already exists locally, no need to convert
            LOG_DEBUG(log, "The current version file already exists locally, does not need to convert");
            return;
        }
    }

    /// Used for upgrade from checksums version, need update checksums with new file names.
    std::unordered_map<String, String> converted_files_map;
    for (auto it = part_storage.iterate(); it->isValid(); it->next())
    {
        String file_name = it->name();

        /// v1, v2 or checksums
        if (from_checksums)
        {
            if (!endsWith(file_name, VECTOR_INDEX_FILE_SUFFIX))
                continue;
        }
        else if (!(endsWith(file_name, VECTOR_INDEX_FILE_OLD_SUFFIX)))
            continue;

        /// vector index description file need to update name.
        bool is_description = false;

        /// Check for checksums first
        if (from_checksums)
        {
            if (endsWith(file_name, current_index_description_name))
            {
                /// Lastest desciption file name with index name
                if (endsWith(file_name, new_description_file_name))
                {
                    LOG_DEBUG(log, "The current version file already exists locally, does not need to convert");
                    return;
                }

                has_intact_old_version_vector_index = true;
                is_description = true;
            }
            else if (file_name == current_checksums_file_name)
                continue;
        }
        else if (endsWith(file_name, old_vector_index_ready_v2)) /// v2 ready file
        {
            has_intact_old_version_vector_index = true;

            LOG_DEBUG(log, "Delete ready file {}", file_name);
            part_storage.removeFile(file_name);

            continue;
        }
        else if (endsWith(file_name, old_index_description_v2))
        {
            is_description = true;
        }
        else if (endsWith(file_name, old_vector_index_ready_v1)) /// v1 ready file
        {
            has_intact_old_version_vector_index = true;
            is_description = true; /// ready will be updated to description file.
        }

        /// There are some common codes to get new description file name.
        if (is_description)
        {
            String new_file_name = file_name;
            if (endsWith(file_name, VECTOR_INDEX_FILE_OLD_SUFFIX))
                new_file_name = fs::path(file_name).replace_extension(VECTOR_INDEX_FILE_SUFFIX).string();

            /// Replace vector_index_description to <index_name>-vector_index_description
            /// Replace merged-<part_id>-<part_name>-vector_index_description to merged-<part_id>-<part_name>-<index_name>-vector_index_description
            new_file_name = std::regex_replace(new_file_name, std::regex("vector"), vector_index_desc.name + "-vector");
            converted_files_map[file_name] = new_file_name;
        }
        else
        {
            /// For other vector index files (exclude ready, checksum, or description files), update vector index file extension to latest.
            /// Support multiple vector indices feature changes the vector index file name by removing column name.
            /// e.g. <index_name>-<column_name>-id_list will be updated to <index_name>-id_list

            /// old vector index name
            String old_vector_index_column_name = vector_index_desc.name + "-" + vector_index_desc.column;
            if (file_name.find(old_vector_index_column_name) == std::string::npos)
            {
                /// Just check suffix
                if (!from_checksums && endsWith(file_name, VECTOR_INDEX_FILE_OLD_SUFFIX))
                {
                    String new_file_name = fs::path(file_name).replace_extension(VECTOR_INDEX_FILE_SUFFIX).string();
                    converted_files_map[file_name] = new_file_name;
                }
                continue;
            }

            /// Replace "<index_name>-<column_name>" to "<index_name>"
            String new_file_name = std::regex_replace(file_name, std::regex(old_vector_index_column_name), vector_index_desc.name);

            if (!from_checksums && endsWith(new_file_name, VECTOR_INDEX_FILE_OLD_SUFFIX))
                new_file_name = fs::path(new_file_name).replace_extension(VECTOR_INDEX_FILE_SUFFIX).string();

            /// for faiss index file, contain "<index_name>-<index_name>" file, replcace "<index_name>-<index_name>" to "<index_name>-data_bin"
            std::vector<String> tokens;
            boost::split(tokens, new_file_name, boost::is_any_of("-"));
            if (tokens.size() == 2 && tokens[0] + VECTOR_INDEX_FILE_SUFFIX == tokens[1])
                new_file_name = tokens[0] + "-data_bin" + VECTOR_INDEX_FILE_SUFFIX;

            converted_files_map[file_name] = new_file_name;
        }
    }

    /// Support multiple vector indices
    /// No vector index files in part or incomplete vector index files
    if (!has_intact_old_version_vector_index)
        return;

    /// Here we collect converted files and upgrade is needed.
    for (auto const & [old_name_, new_name_] : converted_files_map)
    {
        part_storage.moveFile(old_name_, new_name_);
        LOG_DEBUG(log, "Convert vector index file {} to {}", old_name_, new_name_);
    }

    /// Old version vector index files have ready file, will generate checksums file.
    MergeTreeDataPartChecksums vector_index_checksums;
    if (!from_checksums)
        vector_index_checksums = calculateVectorIndexChecksums(part.getDataPartStoragePtr(), part_storage.getRelativePath());
    else if (!converted_files_map.empty())
    {
        if (!part_storage.exists(current_checksums_file_name))
        {
            LOG_WARNING(log, "checksums file '{}' doesn't exist in part {}, will not update it", current_checksums_file_name, part.name);
            return;
        }

        /// Multiple vector indices feature changes vector file names
        MergeTreeDataPartChecksums old_checksums;
        auto buf = part_storage.readFile(current_checksums_file_name, {}, std::nullopt, std::nullopt);
        if (old_checksums.read(*buf))
            assertEOF(*buf);

        for (auto const & [name_, checksum_] : old_checksums.files)
        {
            String new_name_;
            if (converted_files_map.contains(name_))
            {
                new_name_ = converted_files_map[name_];
            }
            else
            {
                new_name_ = name_;
            }
            vector_index_checksums.addFile(new_name_, checksum_.file_size, checksum_.file_hash);
        }

        part_storage.removeFile(current_checksums_file_name);
    }

    LOG_DEBUG(log, "write vector index {} checksums file for part {}", vector_index_desc.name, part.name);
    auto out_checksums = part_storage.writeFile(current_checksums_file_name, 4096, {});
    vector_index_checksums.write(*out_checksums);
    out_checksums->finalize();
    /// Incomplete vector index files will be removed when loading checksums file.

}

/// Incomplete files should be deleted at this stage, [TODO] behavioral confirmation
void MergetreeDataPartVectorIndex::loadVectorIndexFromLocalFile(bool need_convert_index_file)
{
    auto metadata_snapshot = part.storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasVectorIndices())
        return;
    auto & vector_indices_des = metadata_snapshot->getVectorIndices();
    /// [TODO]for upgrade, Need to consider the situation where convert fails
    if (need_convert_index_file)
        convertIndexFileForUpgrade();

    for (auto vec_index_desc : vector_indices_des)
    {
        const auto & vector_index_name = vec_index_desc.name;
        String checksums_filename = getVectorIndexChecksumsFileName(vector_index_name);
        const IDataPartStorage & part_storage = part.getDataPartStorage();
        addVectorIndex(vec_index_desc);
        if (part_storage.exists(checksums_filename))
        {
            MergeTreeDataPartChecksums vector_index_checksums;

            /// Avoid to detach part due to failure on vector index check consistency.
            try
            {
                auto buf = part_storage.readFile(checksums_filename, {}, std::nullopt, std::nullopt);
                if (vector_index_checksums.read(*buf))
                    assertEOF(*buf);
            }
            catch (...)
            {
                LOG_WARNING(
                    log,
                    "An error occurred while checking vector index {} files consistency for part {}: {}",
                    vector_index_name,
                    part.name,
                    getCurrentExceptionMessage(false));
                continue;
            }

            if (vector_index_checksums.empty())
                continue;

            bool is_decouple
                = vector_index_checksums.has(toString("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX);

            if (is_decouple && !part.storage.getSettings()->enable_decouple_vector_index)
                continue;

            /// Check consistency for this vector index
            if (checkConsistencyForVectorIndex(part.getDataPartStoragePtr(), vector_index_checksums))
            {
                if (is_decouple && merged_source_parts.empty())
                    merged_source_parts = getMergedSourcePartsFromFileName(vector_index_name, vector_index_checksums);

                auto storage_id = part.storage.getStorageID();

                MergeTreeDataPartColumnIndexPtr column_index = std::make_shared<MergeTreeDataPartColumnIndex>(part, vec_index_desc);

                std::vector<MergedPartNameAndId> decouple_merge_source_parts = is_decouple ? merged_source_parts : std::vector<MergedPartNameAndId>{};

                /// Update part metadata, will generate
                auto vector_index_segment = VectorIndexSegmentMetadata(
                    /*ready*/ true, is_decouple, vector_index_name, vec_index_desc.column, decouple_merge_source_parts);

                column_index->setIndexSegmentMetadata(vector_index_segment);
                addVectorIndex(vector_index_name, column_index);
            }
            else
            {
                LOG_ERROR(log, "Check constistency for vector index {} in part {} failed.", vector_index_name, part.name);
                removeIndexFile(part.getDataPartStoragePtr(), vector_index_name, true);
            }
        }
    }

    /// Try to remove incomplete vector index files if exists, do not need remove check fail
    removeIncompleteMovedVectorIndexFiles(part);
}

void MergetreeDataPartVectorIndex::inheritVectorIndexStatus(
    MergetreeDataPartVectorIndex & other_vector_index, const StorageMetadataPtr metadata_snapshot, const NameSet & need_rebuild_index)
{
    if (!metadata_snapshot->hasVectorIndices())
        return;
    for (const auto & vec_desc : metadata_snapshot->getVectorIndices())
    {
        if (need_rebuild_index.contains(vec_desc.column) || !getColumnIndex(vec_desc).has_value()
            || !other_vector_index.getColumnIndex(vec_desc).has_value())
            continue;
        auto column_index = getColumnIndex(vec_desc).value();
        auto index_info = other_vector_index.getColumnIndex(vec_desc).value()->getIndexBuildInfo();
        column_index->updateIndexBuildInfo(index_info);
    }
}

std::vector<MergeTreeDataPartColumnIndexPtr> MergetreeDataPartVectorIndex::getInvalidVectorIndexHolder(StorageMetadataPtr metadata_snapshot)
{
    if (!metadata_snapshot)
        LOG_ERROR(log, "Get invalid column index error, metadata snapshot is nullptr.");
    std::vector<MergeTreeDataPartColumnIndexPtr> expire_index_holder;
    {
        std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
        for (auto it : vector_indices)
            if (!metadata_snapshot->getVectorIndices().has(it.first))
                expire_index_holder.emplace_back(it.second);
    }
    return expire_index_holder;
}

void MergetreeDataPartVectorIndex::decoupleIndexOffline(VectorIndexSegmentMetadataPtr index_segment_metadata, NameSet old_files_set)
{
    /// force index file expire
    auto & part_storage = const_cast<IDataPartStorage &>(part.getDataPartStorage());

    auto & segment_metadata = const_cast<VectorIndexSegmentMetadata &>(*index_segment_metadata);

    auto segment_ids = VectorIndex::getAllSegmentIds(part, segment_metadata);

    index_segment_metadata->is_ready.store(false);

    if (segment_ids.empty())
        return;

    for (auto & segment_id : segment_ids)
    {
        part_storage.removeFileIfExists(segment_id.getVectorDescriptionFilePath());
        VICacheManager::removeFromCache(segment_id.getOriginalCacheKey());
        VICacheManager::removeFromCache(segment_id.getCacheKey());
    }

    const String inverted_row_ids_map_file_name = fs::path(segment_ids[0].getInvertedRowIdsMapFilePath()).filename();
    const String inverted_row_source_map_file_name = fs::path(segment_ids[0].getInvertedRowSourcesMapFilePath()).filename();

    for (const auto & file_name : old_files_set)
    {
        if (endsWith(file_name, inverted_row_ids_map_file_name) || endsWith(file_name, inverted_row_source_map_file_name) || endsWith(file_name, SegmentId::getRowIdsMapFileSuffix()))
            continue;
        LOG_DEBUG(log, "Remove Decouple Index File: {}", file_name);
        part_storage.removeFileIfExists(file_name);
    }

    if (!containDecoupleIndex())
    {
        LOG_DEBUG(log, "Remove Decouple Index row ids map, inverted row ids map, inverted row source map.");
        for (auto & segment_id : segment_ids)
            part_storage.removeFileIfExists(fs::path(segment_id.getRowIdsMapFilePath()).filename());
        part_storage.removeFileIfExists(inverted_row_ids_map_file_name);
        part_storage.removeFileIfExists(inverted_row_source_map_file_name);
    }
}

bool MergetreeDataPartVectorIndex::containDecoupleIndex()
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    for (auto it : vector_indices)
        if (it.second->isDecoupleIndexFile())
            return true;
    return false;
}

RWLockImpl::LockHolder
MergetreeDataPartVectorIndex::tryLockTimed(RWLockImpl::Type type, const std::chrono::milliseconds & acquire_timeout) const
{
    auto lock_holder = move_lock->getLock(type, "", acquire_timeout);
    if (!lock_holder)
    {
        const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
        throw Exception(
            ErrorCodes::DEADLOCK_AVOIDED,
            "{} locking attempt on {}'s vector index has timed out! ({}ms) Possible deadlock avoided.",
            type_str,
            part.name,
            acquire_timeout.count());
    }
    return lock_holder;
}

void MergetreeDataPartVectorIndex::removeAllVectorIndexInfo()
{
    std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
    for (auto it : vector_indices)
        it.second->removeAllIndexInfos();
}

}
