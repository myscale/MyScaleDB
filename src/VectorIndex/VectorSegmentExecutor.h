#pragma once
#include <string>
#include <Common/logger_useful.h>
#include <Compression/CompressionInfo.h>
#include <Storages/VectorIndicesDescription.h>
#include <VectorIndex/CacheManager.h>
#include <VectorIndex/Dataset.h>
#include <VectorIndex/IndexException.h>
#include <VectorIndex/PartReader.h>
#include <VectorIndex/SegmentId.h>
#include <VectorIndex/Status.h>
#include <SearchIndex/VectorSearch.h>

namespace VectorIndex
{

enum class BuildMemoryCheckResult
{
    OK, /// ok to build
    LATER, /// currently unable to build, maybe try again later
    NEVER, /// size required greater than limit
};

struct IndexWithMeta
{
    IndexWithMeta() = delete;

    IndexWithMeta(
        VectorIndexPtr & index_,
        uint64_t total_vec_,
        Search::DenseBitmapPtr delete_bitmap_,
        Search::Parameters des_,
        std::shared_ptr<std::vector<UInt64>> row_ids_map_,
        std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map_,
        std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map_,
        int disk_mode_,
        bool fallback_to_flat_,
        String & vector_index_cache_prefix_)
        : index(index_)
        , total_vec(total_vec_)
        , delete_bitmap(delete_bitmap_)
        , des(des_)
        , row_ids_map(row_ids_map_)
        , inverted_row_ids_map(inverted_row_ids_map_)
        , inverted_row_sources_map(inverted_row_sources_map_)
        , disk_mode(disk_mode_)
        , fallback_to_flat(fallback_to_flat_)
        , vector_index_cache_prefix(vector_index_cache_prefix_)
    {
    }

    VectorIndexPtr index;
    size_t total_vec;

private:
    Search::DenseBitmapPtr delete_bitmap;
    mutable std::mutex mutex_of_delete_bitmap;
    mutable std::mutex mutex_of_row_id_maps;
public:
    Search::Parameters des;
    mutable std::shared_mutex rwLock_of_row_id_maps;
    std::shared_ptr<std::vector<UInt64>> row_ids_map;
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map;
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map;
    int disk_mode;
    bool fallback_to_flat;
    String vector_index_cache_prefix;

    void setDeleteBitmap(Search::DenseBitmapPtr delete_bitmap_)
    {
        std::lock_guard<std::mutex> lg(mutex_of_delete_bitmap);
        delete_bitmap = std::move(delete_bitmap_);
    }

    Search::DenseBitmapPtr getDeleteBitmap() const
    {
        std::lock_guard<std::mutex> lg(mutex_of_delete_bitmap);
        return delete_bitmap;
    }

    std::unique_lock<std::mutex> tryLockIndexForUpdateRowIdsMaps() const
    {
        return std::unique_lock<std::mutex>(mutex_of_row_id_maps, std::try_to_lock);
    }
};
using IndexWithMetaPtr = std::shared_ptr<IndexWithMeta>;



class VectorSegmentExecutor
{
    /// The exposed api set which should be called by users trying to use vector index;
    /// the user should not visit any index directly.
public:
    /// Create the index but not inserting any data
    VectorSegmentExecutor(
        const SegmentId & segment_id_,
        Search::IndexType type_,
        Search::Metric metric_,
        size_t dimension_,
        size_t total_vec_,
        Search::Parameters des_,
        size_t min_bytes_to_build_vector_index_,
        int DEFAULT_DISK_MODE_);

    explicit VectorSegmentExecutor(const SegmentId & segment_id_);

    ~VectorSegmentExecutor()
    {
        if (!build_memory_size_recorded)
            return;

        /// decrease build memory size reserved before build. see checkBuildMemory()
        std::lock_guard lock(build_memory_mutex);
        current_build_memory_size -= build_memory_size_recorded;

        LOG_DEBUG(
            &Poco::Logger::get("VectorSegmentExecutor"),
            "after build: size = {}, current_total = {}",
            build_memory_size_recorded,
            current_build_memory_size);
    }

    /// Serialize and store index at segment_id
    Status serialize();

    /// Load index from segment_id,
    /// If hit in cache then simply redirect pointer.
    Status load(bool isActivePart = true);

    /// A method that wraps VectorIndex::search() and does some check and post-process.
    std::shared_ptr<Search::SearchResult> search(
        VectorDatasetPtr queries,
        int32_t k,
        const Search::DenseBitmapPtr & filter,
        Search::Parameters & parameters,
        bool first_stage_only = false);

    std::shared_ptr<Search::SearchResult>
    computeTopDistanceSubset(VectorDatasetPtr queries, std::shared_ptr<Search::SearchResult> first_stage_result, int32_t top_k);

    void buildIndex(PartReader * reader, const std::function<bool()> & check_build_canceled_callbak, bool slow_mode, size_t train_block_size, size_t add_block_size);

    /// Put the index stored in VectorSegmentExecutor into cache.
    Status cache();

    Status removeByIds(size_t n, const size_t * ids);

    /// Return total number of vectors.
    int64_t getRawDataSize();

    /// cancel building the current vector index, free associated resources.
    Status cancelBuild();

    void updateCacheValueWithRowIdsMaps(const IndexWithMetaHolderPtr index_holder);

    static void setCacheManagerSizeInBytes(size_t size);

    static void setBuildMemorySizeInBytes(size_t size);

    static std::list<std::pair<CacheKey, Search::Parameters>> getAllCacheNames();

    static Status searchWithoutIndex(
        VectorDatasetPtr query_data,
        VectorDatasetPtr bash_data,
        int32_t k,
        float *& distances,
        int64_t *& labels,
        const Search::Metric & metric);

    /// expire the related index from cache.
    static Status removeFromCache(const CacheKey & cache_key);
    /// According to the cache key to cancel load vector index
    static void cancelVectorIndexLoading(const CacheKey & cache_key);

    Search::DenseBitmapPtr getRealBitmap(const Search::DenseBitmapPtr & filter)
    {
        if (!segment_id.fromMergedParts())
            return filter;

        if (inverted_row_ids_map->empty() && !filter->to_vector().empty())
            LOG_ERROR(log, "Inverted row ids maps empty, This mast be a bug! cache key: {}",
                segment_id.getCacheKey().toString());

        Search::DenseBitmapPtr real_filter = std::make_shared<Search::DenseBitmap>(total_vec);
        /// Transfer row IDs in the decoupled data part to real row IDs of the old data part.
        for (auto & new_row_id : filter->to_vector())
        {
            if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
            {
                real_filter->set((*inverted_row_ids_map)[new_row_id]);
            }
        }
        return real_filter;
    }

    std::shared_ptr<Search::SearchResult> TransferToOldRowIds(const std::shared_ptr<Search::SearchResult> & result)
    {
        if (!segment_id.fromMergedParts())
            return result;

        if (!result)
        {
            LOG_DEBUG(&Poco::Logger::get("TransferToOldRowIds"), "SearchResult is NIL");
            return result;
        }

        if (inverted_row_sources_map->empty())
        {
            LOG_DEBUG(log, "Skip to transfter to old row ids");
            return nullptr;
        }

        long inverted_size = static_cast<long>(inverted_row_sources_map->size());

        /// Transfer row IDs in the decoupled data part to real row IDs of the old data part.
        /// TODO: Not handle batch distance cases.
        auto new_distances = result->getResultDistances();
        auto new_ids = result->getResultIndices();

        std::vector<UInt64> real_row_ids;
        std::vector<DB::Float32> distances;
        for (int i = 0; i < result->getNumCandidates(); i++)
        {
            auto new_row_id = new_ids[i];

            if (new_row_id == -1 || new_row_id >= inverted_size)
                continue;

            if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
            {
                real_row_ids.emplace_back((*inverted_row_ids_map)[new_row_id]);
                distances.emplace_back(new_distances[i]);
            }
        }

        if (real_row_ids.size() == 0)
            return nullptr;

        /// Prepare search result for this old part
        size_t real_num_reorder = real_row_ids.size();
        std::shared_ptr<Search::SearchResult> real_search_result =
                Search::SearchResult::createTopKHolder(result->numQueries(), real_num_reorder);

        auto per_ids = real_search_result->getResultIndices();
        auto per_distances = real_search_result->getResultDistances();

        for (size_t i = 0; i < real_num_reorder; i++)
        {
            per_ids[i] = real_row_ids[i];
            per_distances[i] = distances[i];
        }

        return real_search_result;
    }

    const SegmentId getSegmentId() const {return segment_id;}

    String generateUUIDv4() const { return DB::toString(DB::UUIDHelpers::generateV4()); }

    /// Update SegmentId
    void updateSegmentId(const SegmentId & new_segment_id) { segment_id = new_segment_id; }

    /// Update part's single delete bitmap after lightweight delete on disk and cache if exists.
    void updateBitMap(const std::vector<UInt64> & deleted_row_ids);

    /// Update merged old part's delete bitmap after lightweight delete on disk and cache if exists.
    void updateMergedBitMap(const std::vector<UInt64> & deleted_row_ids);

    bool supportTwoStageSearch() const { return index->supportTwoStageSearch(); }

    const std::vector<UInt64> readDeleteBitmapAccordingSegmentId() const;

    void convertBitmap(const std::vector<UInt64> & deleted_row_ids);

    Search::IndexResourceUsage getIndexResourceUsage();

    Search::IndexType getIndexType() { return type; }

    /// True if the vector index is stored in cache. Used for lightweight delete.
    bool storedInCache();

private:
    void init();

    String getUniqueVectorIndexCachePrefix() const;

    void handleMergedMaps();

    void transferToNewRowIds(std::shared_ptr<Search::SearchResult> & result)
    {
        if (row_ids_map->empty() && !segment_id.fromMergedParts())
        {
            return;
        }

        for (size_t k = 0; k < result->numQueries(); k++)
        {
            for (auto & label : result->getResultIndices(k))
                if (label != -1)
                    label = (*row_ids_map)[label];
        }
    }

#ifdef ENABLE_SCANN
    std::shared_ptr<Search::DiskIOManager> getDiskIOManager();
#endif
    void configureDiskMode();

    static std::once_flag once;
    static int max_threads;

    static std::mutex build_memory_mutex;
    /// global memory size limit for index building
    static size_t build_memory_size_limit;
    /// current total memory size reserved for index building globally
    static size_t current_build_memory_size;

    /// check if index to build will exceed build memory size limit
    static BuildMemoryCheckResult checkBuildMemorySize(size_t size);

    void checkBuildMemory(size_t size);

    const Poco::Logger * log = &Poco::Logger::get("VectorSegmentExecutor");
    const int DEFAULT_DISK_MODE;

    SegmentId segment_id; // this index's related segment_id and file write position.
    Search::IndexType type;
    Search::Metric metric;
    size_t dimension;
    size_t total_vec = 0;
    Search::Parameters des;
    size_t min_bytes_to_build_vector_index;
    VectorIndexPtr index = nullptr; // index related to this VectorSegmentExecutor
    Search::DenseBitmapPtr delete_bitmap = nullptr; // manage deletion from database
    std::shared_ptr<std::vector<UInt64>> row_ids_map = std::make_shared<std::vector<UInt64>>();
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map = std::make_shared<std::vector<UInt64>>();
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map = std::make_shared<std::vector<uint8_t>>();

    bool fallback_to_flat = false;
    int disk_mode = false;
    std::string vector_index_cache_prefix;

    /// build memory reserved before build
    size_t build_memory_size_recorded = 0;
};

using VectorSegmentExecutorPtr = std::shared_ptr<VectorSegmentExecutor>;
}
