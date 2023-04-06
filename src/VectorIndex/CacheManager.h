#pragma once

#include <memory>
#include <string>
#include <list>
#include <functional>

#include <Common/LRUResourceCache.h>

#include <VectorIndex/VectorIndex.h>
#include <VectorIndex/VectorSegmentExecutor.h>

namespace std
{
template<>
struct hash<VectorIndex::CacheKey>
{
    std::size_t operator()(VectorIndex::CacheKey const& key) const noexcept
    {
        return std::hash<std::string>{}(key.toString());
    }
};
}

namespace VectorIndex
{

static bool m = false;
static size_t cache_size_in_bytes = 0;

class IndexWithMetaWeightFunc
{
public:
    size_t operator()(const IndexWithMeta & index_meta) const
    {
        return index_meta.index->sizeInBytes();
    }
};

class IndexWithMetaReleaseFunction
{
public:
    void operator()(std::shared_ptr<IndexWithMeta> index_meta_ptr)
    {
        if (index_meta_ptr)
            index_meta_ptr.reset();
    }
};

using VectorIndexCacheType
    = DB::LRUResourceCache<CacheKey, IndexWithMeta, IndexWithMetaWeightFunc, IndexWithMetaReleaseFunction, std::hash<CacheKey>>;
using IndexWithMetaHolderPtr = VectorIndexCacheType::MappedHolderPtr;

class VectorIndexCache
    : public DB::LRUResourceCache<CacheKey, IndexWithMeta, IndexWithMetaWeightFunc, IndexWithMetaReleaseFunction, std::hash<CacheKey>>
{
public:
    explicit VectorIndexCache(size_t max_size) : VectorIndexCacheType(max_size) { }

    std::list<std::pair<CacheKey, IndexWithMetaPtr>> getCacheList()
    {
        std::lock_guard lock(mutex);

        std::list<std::pair<CacheKey, IndexWithMetaPtr>> res;

        for (auto it = cells.begin(); it != cells.cend(); ++it)
        {
            res.push_back(std::make_pair(it->first, it->second.value));
        }

        return res;
    }
};

class CacheManager
{
    // cache manager manages a series of cache instance.
    // these caches could either be cache in memory or cache on GPU device.
    // it privides a getInstance() method which returns a consistent view
    // of all caches to all classes trying to access cache.

private:
    explicit CacheManager(int);

public:
    void put(const CacheKey & cache_key, IndexWithMetaPtr index);
    IndexWithMetaHolderPtr get(const CacheKey & cache_key);
    size_t countItem() const;
    void forceExpire(const CacheKey & cache_key);
    IndexWithMetaHolderPtr load(const CacheKey & cache_key, std::function<IndexWithMetaPtr()> load_func);
    std::list<std::pair<CacheKey, Parameters>> getAllItems();

    static CacheManager * getInstance();
    static void setCacheSize(size_t size_in_bytes);

protected:
    mutable std::unique_ptr<VectorIndexCache> cache;
    Poco::Logger * log;
};

}
