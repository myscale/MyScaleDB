#pragma once

#include <memory>
#include <string>
#include <list>
#include <functional>

#include <Common/CacheBase.h>

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

struct IndexAndMutex
{
    IndexWithMetaPtr index_ptr;
    std::shared_ptr<std::mutex> mu_ptr;

    IndexAndMutex(IndexWithMetaPtr index_ptr_, std::shared_ptr<std::mutex> mu_ptr_) : index_ptr(index_ptr_), mu_ptr(mu_ptr_) { }
};
using IndexAndMutexPtr = std::shared_ptr<IndexAndMutex>;

class IndexAndMutexWeightFunc
{
public:
    size_t operator()(const IndexAndMutex & iam) const
    {
        if (iam.index_ptr == nullptr)
        {
            return 0;
        }

        return iam.index_ptr->index->sizeInBytes();
    }
};

class VectorIndexCache : public DB::CacheBase<CacheKey, IndexAndMutex, std::hash<CacheKey>, IndexAndMutexWeightFunc>
{
public:
    using Base = DB::CacheBase<CacheKey, IndexAndMutex, std::hash<CacheKey>, IndexAndMutexWeightFunc>;

    explicit VectorIndexCache(size_t max_size) : Base("LRU", max_size) { }

    std::list<std::pair<CacheKey, IndexAndMutexPtr>> getCacheList()
    {
        std::lock_guard lock(mutex);

        std::list<std::pair<CacheKey, IndexAndMutexPtr>> l;

        auto cache = dynamic_cast<LRUPolicy *>(cache_policy.get());
        if (cache)
        {
            for (auto it = cache->cells.begin(); it != cache->cells.cend(); ++it)
            {
                l.push_back(std::make_pair(it->first, it->second.value));
            }
        }

        return l;
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
    void put(const CacheKey& cache_key, IndexWithMetaPtr index);
    IndexWithMetaPtr get(const CacheKey& cache_key);
    size_t countItem() const;
    void forceExpire(const CacheKey& cache_key);
    void startLoading(const CacheKey& cache_key);
    std::shared_ptr<std::mutex> getMutex(const CacheKey& cache_key);
    std::list<std::pair<CacheKey, Parameters>> getAllItems();
    void updateKey(const CacheKey& old_key, const CacheKey& new_key);

    static CacheManager * getInstance();
    static void setCacheSize(size_t size_in_bytes);

protected:
    mutable std::unique_ptr<VectorIndexCache> cache_;
    Poco::Logger *log;

};

}
