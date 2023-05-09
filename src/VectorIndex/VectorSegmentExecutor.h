#pragma once
#pragma GCC diagnostic ignored "-Wunused-function"
#include <string>
#include <Common/logger_useful.h>
#include <Compression/CompressionInfo.h>
#include <Storages/VectorIndicesDescription.h>
#include <VectorIndex/Dataset.h>
#include <VectorIndex/IndexException.h>
#include <VectorIndex/PartReader.h>
#include <VectorIndex/SegmentId.h>
#include <VectorIndex/Status.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wextra-semi-stmt"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wc++20-compat"
#pragma GCC diagnostic ignored "-Walloca"
#pragma GCC diagnostic ignored "-Wmissing-noreturn"
#pragma GCC diagnostic ignored "-Wgcc-compat"
#pragma GCC diagnostic ignored "-Wcovered-switch-default"
#pragma GCC diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#pragma GCC diagnostic ignored "-Wextra-semi"
#pragma GCC diagnostic ignored "-Wfinal-dtor-non-final-class"
#pragma GCC diagnostic ignored "-Wundef"
#pragma GCC diagnostic ignored "-Wsuggest-override"
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#pragma GCC diagnostic ignored "-Wdeprecated-copy-with-user-provided-dtor"
#pragma GCC diagnostic ignored "-Wmismatched-tags"
#pragma GCC diagnostic ignored "-Wundefined-reinterpret-cast"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wshadow-uncaptured-local"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-private-field"
#pragma GCC diagnostic ignored "-Wshadow-field"
#pragma GCC diagnostic ignored "-Wdelete-non-abstract-non-virtual-dtor"
#pragma GCC diagnostic ignored "-Wshadow"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma GCC diagnostic ignored "-Wrange-loop-bind-reference"
#pragma GCC diagnostic ignored "-Wenum-compare-switch"
#include <SearchIndex/VectorSearch.h>
#pragma GCC diagnostic pop

namespace VectorIndex
{

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
        bool disk_mode_,
        bool fallback_to_flat_,
        std::string vector_index_cache_prefix_)
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

    ~IndexWithMeta()
    {
        if (disk_mode)
        {
            fs::remove_all(vector_index_cache_prefix);
        }
    }

    VectorIndexPtr index;
    size_t total_vec;

private:
    Search::DenseBitmapPtr delete_bitmap;
    mutable std::mutex mutex_of_delete_bitmap;
public:
    Search::Parameters des;
    std::shared_ptr<std::vector<UInt64>> row_ids_map;
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map;
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map;
    bool disk_mode;
    bool fallback_to_flat;
    std::string vector_index_cache_prefix;

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
        bool DEFAULT_DISK_MODE_);

    explicit VectorSegmentExecutor(const SegmentId & segment_id_);

    /// Serialize and store index at segment_id
    Status serialize();

    /// Load index from segment_id,
    /// If hit in cache then simply redirect pointer.
    Status load();

    /// A method that wraps VectorIndex::search() and does some check and post-process.
    std::shared_ptr<Search::SearchResult> search(
        VectorDatasetPtr dataset,
        int32_t k,
        const Search::DenseBitmapPtr & filter,
        Search::Parameters & parameters);

    void buildIndex(PartReader * reader, bool slow_mode, size_t train_block_size, size_t add_block_size);

    /// Put the index stored in VectorSegmentExecutor into cache.
    Status cache();

    Status removeByIds(size_t n, const size_t * ids);

    Search::DenseBitmapPtr getDeleteBitMap() { return this->delete_bitmap; }

    /// Return total number of vectors.
    int64_t getRawDataSize();

    /// cancel building the current vector index, free associated resources.
    Status cancelBuild();

    void updateCacheValueWithRowIdsMaps();

    static void setCacheManagerSizeInBytes(size_t size);

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

    Search::DenseBitmapPtr getRealBitmap(const Search::DenseBitmapPtr & filter)
    {
        if (!segment_id.fromMergedParts())
            return filter;

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

    /// Update SegmentId
    void updateSegmentId(const SegmentId & new_segment_id) { segment_id = new_segment_id; }

    /// Reload delete bitmap from disk.
    bool reloadDeleteBitMap() { return readBitMap(); }

    /// Update part's single delete bitmap after lightweight delete on disk and cache if exists.
    void updateBitMap(const std::vector<UInt64> & deleted_row_ids);

    /// Update merged old part's delete bitmap after lightweight delete on disk and cache if exists.
    void updateMergedBitMap(const std::vector<UInt64> & deleted_row_ids);

private:
    void init();

    bool writeBitMap();

    bool readBitMap();

    void handleMergedMaps();

    std::shared_ptr<Search::SearchResult> performSearch(
        VectorDatasetPtr dataset,
        int32_t k,
        const Search::DenseBitmapPtr & filter,
        Search::Parameters & parameters);

    void transferToNewRowIds(int64_t *& labels, int size)
    {
        if (row_ids_map->empty())
        {
            return;
        }

        for (int i = 0; i < size; i++)
        {
            if (labels[i] != -1)
            {
                labels[i] = (*row_ids_map)[labels[i]];
            }
        }
    }

    std::shared_ptr<Search::DiskIOManager> getDiskIOManager();
    void configureDiskMode();

    static std::once_flag once;
    static int max_threads;

    const Poco::Logger * log = &Poco::Logger::get("VectorSegmentExecutor");
    const bool DEFAULT_DISK_MODE;

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
    bool disk_mode = false;
    std::string vector_index_cache_prefix;
};

using VectorSegmentExecutorPtr = std::shared_ptr<VectorSegmentExecutor>;
}
