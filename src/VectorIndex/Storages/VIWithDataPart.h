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

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <Core/Names.h>
#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Common/BruteForceSearch.h>
#include <VectorIndex/Common/SegmentId.h>
#include <VectorIndex/Common/VIBuildMemoryUsageHelper.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/VectorDataset.h>
#include <VectorIndex/Storages/VIInfo.h>
#include <VectorIndex/Utils/VIUtils.h>
#include <base/types.h>
#include <Poco/Logger.h>
#include <Common/MultiVersion.h>
#include <Common/RWLock.h>


namespace VectorIndex
{

template <Search::DataType T>
struct VectorDataset;

template <Search::DataType T>
using VectorDatasetPtr = std::shared_ptr<VectorDataset<T>>;

enum class BuildMemoryCheckResult;

}

namespace DB
{
using String = std::string;
using namespace VectorIndex;

class IMergeTreeDataPart;
class IDataPartStorage;
struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;
class MergeTreeDataPartColumnIndex;
using MergeTreeDataPartColumnIndexPtr = std::shared_ptr<MergeTreeDataPartColumnIndex>;

struct MergedPartNameAndId
{
    String name;
    int id;
    bool with_index{true};

    MergedPartNameAndId(const String & name_, const int & id_, bool with_index_ = true) : name(name_), id(id_), with_index(with_index_) { }
};


using VectorIndexDescriptionPtr = std::shared_ptr<const VIDescription>;

/// Record meta information related to vpart/dpart segment files
struct VectorIndexSegmentMetadata
{
    VectorIndexSegmentMetadata(
        bool is_ready_,
        bool is_decouple_index_,
        const String & index_name_,
        const String & column_name_,
        const std::vector<MergedPartNameAndId> & merge_source_parts_)
        : is_ready(is_ready_)
        , is_decouple_index(is_decouple_index_)
        , index_name(index_name_)
        , column_name(column_name_)
        , merge_source_parts(merge_source_parts_)
    {
    }

    VectorIndexSegmentMetadata(const VectorIndexSegmentMetadata & other)
        : is_ready(other.is_ready.load())
        , is_decouple_index(other.is_decouple_index)
        , index_name(other.index_name)
        , column_name(other.column_name)
        , merge_source_parts(other.merge_source_parts)
    {
    }

    mutable std::atomic<bool> is_ready{false};
    bool is_decouple_index = false;
    const String index_name;
    const String column_name;
    const std::vector<MergedPartNameAndId> merge_source_parts;
};

using VectorIndexSegmentMetadataPtr = MultiVersion<VectorIndexSegmentMetadata>::Version;
using MultiVersionIndexSegment = MultiVersion<VectorIndexSegmentMetadata>;

class MergeTreeDataPartColumnIndex
{
public:
    MergeTreeDataPartColumnIndex(
        const StorageID & storage_id_,
        const String & current_part_name_,
        const VIDescription & vec_desc_,
        const UInt64 rows_count_,
        StorageMetadataPtr metadata_snapshot,
        DataPartStoragePtr part_storage_,
        MergeTreeSettingsPtr merge_tree_setting);

    MergeTreeDataPartColumnIndex(const IMergeTreeDataPart & part, const VIDescription & vec_desc_);

    ~MergeTreeDataPartColumnIndex() = default;

    struct VectorIndexBuildInfo
    {
        mutable std::mutex build_info_mutex;
        VectorIndexState state{VectorIndexState::PENDING};

        StopwatchUniquePtr watch{nullptr};

        std::atomic<bool> cancel_build{false};

        String err_msg;
    };

    using VectorIndexBuildInfoPtr = std::shared_ptr<VectorIndexBuildInfo>;

    friend class VIInfo;
    friend class MergetreeDataPartVectorIndex;

    void onBuildStart();

    void onBuildFinish(bool is_success = true, const String & err_msg = "");

    double getBuildElapsed();

    VectorIndexState getVectorIndexState();

    const String getLastBuildErrMsg();

    VectorIndexInfoPtrList getVectorIndexInfos();

    void removeDecoupleIndexInfos();

    void removeAllIndexInfos();

    VectorIndexBuildInfoPtr getIndexBuildInfo() { return std::atomic_load(&build_index_info); }

    /// only for mutate task, new part needs to inherit the index status of the old part.
    /// If you need to call this function when part in active state, you need to modify
    /// other operations involving vector_infos[0] into atomic operations
    void updateIndexBuildInfo(VectorIndexBuildInfoPtr vec_info);

    /// When loading part or adding a new vector index to part,
    /// vector index info will be initialized for use by the vector index segments system table.
    void initVectorIndexInfo(bool is_small_part = false);

    VectorIndexSegmentMetadataPtr getIndexSegmentMetadata() const { return vector_index_segment_metadata.get(); }

    void setIndexSegmentMetadata(VectorIndexSegmentMetadata vector_index_segment_metadata_)
    {
        vector_index_segment_metadata.set(std::make_unique<VectorIndexSegmentMetadata>(vector_index_segment_metadata_));
    }

    const String getIndexName() const { return index_name; }

    void cancelBuild();

    bool isBuildCancelled() const { return build_index_info->cancel_build.load(std::memory_order_relaxed); }

    bool isDecoupleIndexFile() const { return vector_index_segment_metadata.get()->is_decouple_index; }

    size_t getMemoryUsage() const;

    size_t getDiskUsage() const;

    VIMetric getMetric() const { return Search::getMetricType(metric_str, vector_search_type); }

    SegmentId getCurrentFakeSegmentId() { return SegmentId(part_storage, current_part_name, index_name, column_name); }

    /// Called when vector index drops
    /// remove vector index file, cancle vector index build, release cache
    void shutdown();

    bool isShutdown() const { return is_shutdown.load(); }

    template <Search::DataType T>
    void buildIndex(
        VIVariantPtr & index_variant,
        VISourcePartReader<T> * part_reader,
        bool slow_mode,
        size_t max_build_index_train_block_size,
        size_t max_build_index_add_block_size,
        VIBuildMemoryUsageHelper & build_memory_lock,
        std::function<bool()> cancel_build_callback = {});

    void serialize(
        VIVariantPtr & index_variant,
        DiskPtr disk,
        const String & serialize_local_folder,
        std::shared_ptr<MergeTreeDataPartChecksums> & vector_index_checksum,
        VIBuildMemoryUsageHelper & build_memory_lock);

    IndexWithMetaHolderPtr load(SegmentId & segment_id, bool is_active = true, const String & nvme_cache_path_uuid = "");

    IndexWithMetaHolderPtr loadDecoupleCache(SegmentId & segment_id);

    bool cache(VIVariantPtr index);

    std::vector<IndexWithMetaHolderPtr> getIndexHolders(bool is_active = true);

    SearchResultPtr computeTopDistanceSubset(
        const VIWithMeta & index_with_meta, VectorDatasetVariantPtr queries, SearchResultPtr first_stage_result, int32_t top_k);

    SearchResultPtr search(
        const VIWithMeta & index_with_meta,
        VectorDatasetVariantPtr queries,
        int32_t k,
        VIBitmapPtr filter,
        VIParameter & parameters,
        bool first_stage_only);

    bool hasUsableVectorIndex();

    static void transferToNewRowIds(const VIWithMeta & index_with_meta, SearchResultPtr result);

    static SearchResultPtr TransferToOldRowIds(const VIWithMeta & index_with_meta, const SearchResultPtr result);

    template <Search::DataType T>
    static void searchWithoutIndex(
        VectorDatasetPtr<T> query_data,
        VectorDatasetPtr<T> bash_data,
        int32_t k,
        float *& distances,
        int64_t *& labels,
        const VIMetric & metric);

    static bool supportTwoStageSearch(const VIWithMeta & index_with_meta);

    static bool canMergeForColumnIndex(const MergeTreeDataPartPtr & left, const MergeTreeDataPartPtr & right, const String & vec_index_name);

private:
    VIVariantPtr createIndex() const;

#ifdef ENABLE_SCANN
    std::shared_ptr<DiskIOManager> getDiskIOManager() const;
#endif

    static std::once_flag once;
    static int max_threads;

private:
    const StorageID storage_id;
    const String current_part_name;
    const String index_name;
    const String column_name;
    const UInt64 total_vec;
    const Search::DataType vector_search_type;

    UInt64 dimension;
    VIType index_type;
    String metric_str;
    VIParameter des;
    UInt32 disk_mode;
    bool fallback_to_flat;

    const DataPartStoragePtr part_storage;

    MultiVersionIndexSegment vector_index_segment_metadata;

    mutable std::mutex vector_info_mutex;
    VectorIndexInfoPtrList vector_infos;

    VectorIndexBuildInfoPtr build_index_info;

    std::atomic<bool> is_shutdown{false};

    Poco::Logger * log{&Poco::Logger::get("MergeTreeDataPartColumnIndex")};
};

template <Search::DataType T>
void MergeTreeDataPartColumnIndex::buildIndex(
    VIVariantPtr & index_variant,
    VISourcePartReader<T> * part_reader,
    bool slow_mode,
    size_t max_build_index_train_block_size,
    size_t max_build_index_add_block_size,
    VIBuildMemoryUsageHelper & build_memory_lock,
    std::function<bool()> cancel_build_callback)
{
    OpenTelemetry::SpanHolder span("MergeTreeDataPartColumnIndex::buildIndex");

    auto num_threads = max_threads;
    if (slow_mode)
        num_threads = num_threads / 2;
    if (num_threads == 0)
        num_threads = 1;

    index_variant = createIndex();
    typename SearchIndexDataTypeMap<T>::VectorIndexPtr index_ptr;
    if constexpr (T == Search::DataType::FloatVector)
    {
        if (!std::holds_alternative<FloatVIPtr>(index_variant))
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Index Type error, except parameter type to be FloatVector");

        index_ptr = std::get<FloatVIPtr>(index_variant);
    }
    else if constexpr (T == Search::DataType::BinaryVector)
    {
        if (!std::holds_alternative<BinaryVIPtr>(index_variant))
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Index Type error, except parameter type to be BinaryVector");

        index_ptr = std::get<BinaryVIPtr>(index_variant);
    }
    else
        throw VIException(ErrorCodes::LOGICAL_ERROR, "Unknown Serach Type!");

    index_ptr->setTrainDataChunkSize(max_build_index_train_block_size);
    index_ptr->setAddDataChunkSize(max_build_index_add_block_size);
    build_memory_lock.checkBuildMemory(index_ptr->getResourceUsage().build_memory_usage_bytes);

    printMemoryInfo(log, "Before build");
    index_ptr->build(part_reader, num_threads, cancel_build_callback);
    printMemoryInfo(log, "After build");
}

template <Search::DataType T>
void MergeTreeDataPartColumnIndex::searchWithoutIndex(
    VectorDatasetPtr<T> query_data,
    VectorDatasetPtr<T> base_data,
    int32_t k,
    float *& distances,
    int64_t *& labels,
    const VIMetric & metric)
{
    omp_set_num_threads(1);
    auto new_metric = metric;
    if constexpr (T == Search::DataType::FloatVector)
    {
        if (metric == VIMetric::Cosine)
        {
            LOG_DEBUG(&Poco::Logger::get("MergeTreeDataPartColumnIndex"), "Normalize vectors for cosine similarity brute force search");
            new_metric = VIMetric::IP;
            query_data->normalize();
            base_data->normalize();
        }
    }
    tryBruteForceSearch<T>(
        query_data->getData(),
        base_data->getData(),
        query_data->getDimension(),
        k,
        query_data->getVectorNum(),
        base_data->getVectorNum(),
        labels,
        distances,
        new_metric);
    if constexpr (T == Search::DataType::FloatVector)
    {
        if (metric == VIMetric::Cosine)
        {
            for (int64_t i = 0; i < k * query_data->getVectorNum(); i++)
            {
                distances[i] = 1 - distances[i];
            }
        }
    }
}

class MergetreeDataPartVectorIndex
{
public:
    explicit MergetreeDataPartVectorIndex(const IMergeTreeDataPart & part_)
        : part(part_), log(&Poco::Logger::get("MergetreeDataPartVectorIndex"))
    {
    }

    ~MergetreeDataPartVectorIndex() = default;

    MergetreeDataPartVectorIndex operator=(const MergetreeDataPartVectorIndex &) = delete;
    MergetreeDataPartVectorIndex(const MergetreeDataPartVectorIndex &) = delete;

    using VectorIndexChecksums = std::unordered_map<String, MergeTreeDataPartChecksums>;

    void onVectorIndexBuildError(const String & index_name, const String & err_msg)
    {
        std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
        if (auto it = vector_indices.find(index_name); it != vector_indices.end())
            it->second->onBuildFinish(false, err_msg);
    }

    bool containAnyVectorIndexInReady()
    {
        std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
        for (auto & it : vector_indices)
        {
            if (it.second->getIndexSegmentMetadata()->is_ready.load())
                return true;
        }
        return false;
    }

    bool containDecoupleOrVPartIndexInReady(const String & index_name)
    {
        std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
        if (auto it = vector_indices.find(index_name); it != vector_indices.end())
            return it->second->getIndexSegmentMetadata()->is_ready.load();
        return false;
    }

    bool alreadyWithVIndexSegment(const String & index_name)
    {
        std::shared_lock<std::shared_mutex> lock(vector_indices_mutex);
        if (auto it = vector_indices.find(index_name); it != vector_indices.end())
            return it->second->getVectorIndexState() == VectorIndexState::BUILT;
        return false;
    }

    void addVectorIndex(const String & index_name, MergeTreeDataPartColumnIndexPtr column_index);

    void addVectorIndex(const VIDescription & vec_desc);

    void removeVectorIndex(const String & index_name);

    std::optional<MergeTreeDataPartColumnIndexPtr> getColumnIndex(const VIDescription & vec_desc)
    {
        return getColumnIndex(vec_desc.name);
    }

    std::optional<MergeTreeDataPartColumnIndexPtr> getColumnIndex(const String & index_name);

    std::optional<MergeTreeDataPartColumnIndexPtr> getColumnIndexByColumnName(const String & column_name);

    bool isBuildCancelled(const String & index_name);

    void cancelIndexBuild(const String & index_name);

    void cancelAllIndexBuild();

    void convertIndexFileForUpgrade();

    void loadVectorIndexFromLocalFile(bool need_convert_index_file = false);

    void setMergedSourceParts(std::vector<MergedPartNameAndId> source_parts) { merged_source_parts = source_parts; }
    const std::vector<MergedPartNameAndId> getMergedSourceParts() { return merged_source_parts; }

    void inheritVectorIndexStatus(
        MergetreeDataPartVectorIndex & other_vector_index,
        const StorageMetadataPtr metadata_snapshot,
        const NameSet & need_rebuild_index = {});

    std::vector<MergeTreeDataPartColumnIndexPtr> getInvalidVectorIndexHolder(StorageMetadataPtr metadata_snapshot);

    void decoupleIndexOffline(VectorIndexSegmentMetadataPtr index_segment_metadata, NameSet old_files_set);

    bool containDecoupleIndex();

    RWLockImpl::LockHolder tryLockTimed(RWLockImpl::Type type, const std::chrono::milliseconds & acquire_timeout) const;

    void removeAllVectorIndexInfo();

private:
    mutable std::shared_mutex vector_indices_mutex;
    std::unordered_map<String, MergeTreeDataPartColumnIndexPtr> vector_indices;

    /// Source part names which were merged to this decouple part, used to locate their vector index files.
    std::vector<MergedPartNameAndId> merged_source_parts = {};

    const IMergeTreeDataPart & part;

    std::atomic<Poco::Logger *> log;

    mutable RWLock move_lock = RWLockImpl::create();
};

}
