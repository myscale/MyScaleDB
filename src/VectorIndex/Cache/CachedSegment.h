#pragma once

#include <base/types.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric LoadedVectorIndexMemorySize;
}

namespace VectorIndex
{
using VIDescriptionPtr = std::shared_ptr<DB::VIDescription>;
/// Calculate the memory usage of the inner segment, including the index and delete_bitmap
inline Int64 calculateInnerSegmentMemoryUsage(const InnerSegmentVariantPtr & index, const VIBitmapPtr & delete_bitmap)
{
    Int64 memory_usage = 0;
    std::visit([&memory_usage](auto &&index_ptr)
    {
        memory_usage += index_ptr->getResourceUsage().memory_usage_bytes;
    }, index);
    /// calculate delete_bitmap memory usage
    if (delete_bitmap)
        memory_usage += delete_bitmap->byte_size();
    return memory_usage;
}

struct CachedSegment
{
    using CachedSegmentPtr = std::shared_ptr<CachedSegment>;
    CachedSegment() = delete;

    explicit CachedSegment(
        InnerSegmentVariantPtr index_, VIBitmapPtr delete_bitmap_, bool fallback_to_flat_, const VIDescriptionPtr & vec_desc_, scope_guard & vi_cache_path_holder_)
        : index(std::move(index_))
        , delete_bitmap(std::move(delete_bitmap_))
        , fallback_to_flat(fallback_to_flat_)
        , vec_desc(std::move(vec_desc_))
        , vi_cache_path_holder(std::move(vi_cache_path_holder_))
        , index_load_memory_size_metric(CurrentMetrics::LoadedVectorIndexMemorySize, calculateInnerSegmentMemoryUsage(index, delete_bitmap))
    {
    }

    inline void setDeleteBitmap(VIBitmapPtr delete_bitmap_)
    {
        /// atomic store delete_bitmap
        std::atomic_store(&delete_bitmap, delete_bitmap_);
    }

    inline VIBitmapPtr getDeleteBitmap() const
    {
        /// atomic load delete_bitmap
        return std::atomic_load(&delete_bitmap);
    }

    const InnerSegmentVariantPtr index;
private:
    // mutable std::mutex bitmap_mutex;
    VIBitmapPtr delete_bitmap;

public:
    const bool fallback_to_flat;
    /// used for check index is same type with same name
    const VIDescriptionPtr vec_desc;
    /// lock the cache path, when the cache is removed, the cache path will be removed
    scope_guard vi_cache_path_holder;
    CurrentMetrics::Increment index_load_memory_size_metric;
};

using CachedSegmentPtr = std::shared_ptr<CachedSegment>;

} // namespace VectorIndex
