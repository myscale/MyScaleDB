#pragma once
#include <string>
#include <Compression/CompressionInfo.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/VectorIndicesDescription.h>
#include <VectorIndex/CacheManager.h>
#include <VectorIndex/Dataset.h>
#include <VectorIndex/IndexException.h>
#include <VectorIndex/PartReader.h>
#include <VectorIndex/SegmentId.h>
#include <VectorIndex/Status.h>
#include <Common/logger_useful.h>
#include <Common/MemoryStatisticsOS.h>
#include <sys/resource.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wfloat-conversion"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#include <SearchIndex/VectorSearch.h>
#pragma clang diagnostic pop
#endif

namespace VectorIndex
{
inline void printMemoryInfo(const Poco::Logger *log, std::string msg)
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    DB::MemoryStatisticsOS memory_stat;
    DB::MemoryStatisticsOS::Data data = memory_stat.get();
    LOG_INFO(
            log,
            "{}: peak resident memory {} MB, resident memory {} MB, virtual memory {} MB",
            msg,
            usage.ru_maxrss / 1024,
            data.resident / 1024 / 1024,
            data.virt / 1024 / 1024);
#endif
}

inline Search::IndexType fallbackToFlat(DB::VectorSearchType &search_type)
{
    switch (search_type)
    {
        case DB::VectorSearchType::Float32Vector:
            return Search::IndexType::FLAT;
        case DB::VectorSearchType::BinaryVector:
            return Search::IndexType::BinaryFLAT;
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type");
    }
}

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
        VectorIndexVariantPtr & index_variant_,
        uint64_t total_vec_,
        Search::DenseBitmapPtr delete_bitmap_,
        Search::Parameters des_,
        std::shared_ptr<std::vector<UInt64>> row_ids_map_,
        std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map_,
        std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map_,
        int disk_mode_,
        bool fallback_to_flat_,
        String & vector_index_cache_prefix_)
        : index_variant(index_variant_)
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

    VectorIndexVariantPtr index_variant;
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
        DB::VectorSearchType vector_search_type_,
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
    Status serialize(std::shared_ptr<DB::MergeTreeDataPartChecksums> & checksums);

    /// Load index from segment_id,
    /// If hit in cache then simply redirect pointer.
    Status load(bool isActivePart = true);

    /// A method that wraps VectorIndex::search() and does some check and post-process.
    std::shared_ptr<Search::SearchResult> search(
        VectorDatasetVariantPtr queries,
        int32_t k,
        const Search::DenseBitmapPtr & filter,
        Search::Parameters & parameters,
        bool first_stage_only = false);

    std::shared_ptr<Search::SearchResult>
    computeTopDistanceSubset(VectorDatasetVariantPtr queries, std::shared_ptr<Search::SearchResult> first_stage_result, int32_t top_k);

    template<DB::VectorSearchType T>
    void buildIndex(PartReader<T> * reader, const std::function<bool()> & check_build_canceled_callbak, bool slow_mode, size_t train_block_size, size_t add_block_size);

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

    template <DB::VectorSearchType T>
    static Status searchWithoutIndex(
            VectorDatasetPtr<T> query_data,
            VectorDatasetPtr<T> bash_data,
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

    bool supportTwoStageSearch() const
    {
        bool isSupported = false;
        std::visit([&isSupported](auto && index_ptr)
                   {
                       isSupported = index_ptr->supportTwoStageSearch();
                   }, index_variant);
        return isSupported;
    }

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
    DB::VectorSearchType vector_search_type = DB::VectorSearchType::InvalidVector;
    Search::IndexType type;
    Search::Metric metric;
    size_t dimension;
    size_t total_vec = 0;
    Search::Parameters des;
    size_t min_bytes_to_build_vector_index;
    size_t each_vector_bytes;

    VectorIndexVariantPtr index_variant; // index related to this VectorSegmentExecutor

    Search::DenseBitmapPtr delete_bitmap = nullptr; // manage deletion from database
    std::shared_ptr<std::vector<UInt64>> row_ids_map = std::make_shared<std::vector<UInt64>>();
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map = std::make_shared<std::vector<UInt64>>();
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map = std::make_shared<std::vector<uint8_t>>();

    std::shared_ptr<DB::MergeTreeDataPartChecksums> vector_index_checksums;

    bool fallback_to_flat = false;
    int disk_mode = false;
    std::string vector_index_cache_prefix;

    /// build memory reserved before build
    size_t build_memory_size_recorded = 0;
};

template<DB::VectorSearchType T>
void VectorSegmentExecutor::buildIndex(PartReader<T> *reader, const std::function<bool()> &check_build_canceled_callbak, bool slow_mode, size_t train_block_size, size_t add_block_size)
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::buildIndex");

    auto num_threads = max_threads;
    if (slow_mode)
        num_threads = num_threads / 2;
    if (num_threads == 0)
        num_threads = 1;

    if (T != vector_search_type)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Wrong vector search type for build vector index");

    if (total_vec * each_vector_bytes < min_bytes_to_build_vector_index)
    {
        fallback_to_flat = true;
        type = fallbackToFlat(vector_search_type);
        std::erase_if(
            des,
            [](const auto & item)
            {
                auto const & [key, value] = item;
                return key != "metric_type";
            });
    }

    try
    {
        configureDiskMode();

        typename VectorSearchTypeMap<T>::VectorIndexPtr index;
        if constexpr (T == DB::VectorSearchType::Float32Vector)
        {
            index = Search::createVectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::FloatVector>(
                    segment_id.getIndexName(),
                    type,
                    metric,
                    dimension,
                    total_vec,
                    des,
                    false /* load_diskann_after_build */,
                    vector_index_cache_prefix,
#ifdef ENABLE_SCANN
                    getDiskIOManager(),
#endif
                    true /* use_file_checksum */,
                    true /* manage_cache_folder */);
        }
        else if constexpr (T == DB::VectorSearchType::BinaryVector)
        {
            index = Search::createVectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::BinaryVector>(
                    segment_id.getIndexName(),
                    type,
                    metric,
                    dimension,
                    total_vec,
                    des,
                    false /* load_diskann_after_build */,
                    vector_index_cache_prefix,
#ifdef ENABLE_SCANN
                    getDiskIOManager(),
#endif
                    true /* use_file_checksum */,
                    true /* manage_cache_folder */);
        }

        index->setTrainDataChunkSize(train_block_size);
        index->setAddDataChunkSize(add_block_size);

        checkBuildMemory(index->getResourceUsage().build_memory_usage_bytes);

        printMemoryInfo(log, "Before build");
        index->build(reader, num_threads, check_build_canceled_callbak);
        printMemoryInfo(log, "After build");

        index_variant = index;
    }
    catch (const SearchIndexException &e)
    {
        LOG_WARNING(log, "Failed to build index for {}: {}", segment_id.current_part_name, e.what());
        throw IndexException(e.getCode(), e.what());
    }
    catch (const DB::Exception &e)
    {
        throw e;
    }

    delete_bitmap = std::make_shared<Search::DenseBitmap>(total_vec, true);
}

template <DB::VectorSearchType T>
Status VectorSegmentExecutor::searchWithoutIndex(
        VectorDatasetPtr<T> query_data,
        VectorDatasetPtr<T> base_data,
        int32_t k,
        float *& distances,
        int64_t *& labels,
        const Search::Metric & metric)
{
    omp_set_num_threads(1);
    auto new_metric = metric;
    if constexpr (T == DB::VectorSearchType::Float32Vector)
    {
        if (metric == Search::Metric::Cosine)
        {
            LOG_DEBUG(&Poco::Logger::get("VectorSegmentExecutor"), "Normalize vectors for cosine similarity brute force search");
            new_metric = Search::Metric::IP;
            query_data->normalize();
            base_data->normalize();
        }
    }
    auto status = tryBruteForceSearch<T>(
            query_data->getData(),
            base_data->getData(),
            query_data->getDimension(),
            k,
            query_data->getVectorNum(),
            base_data->getVectorNum(),
            labels,
            distances,
            new_metric);
    if constexpr (T == DB::VectorSearchType::Float32Vector)
    {
        if (metric == Search::Metric::Cosine)
        {
            for (int64_t i = 0; i < k * query_data->getVectorNum(); i++)
            {
                distances[i] = 1 - distances[i];
            }
        }
    }
    return status;
}

using VectorSegmentExecutorPtr = std::shared_ptr<VectorSegmentExecutor>;
}
