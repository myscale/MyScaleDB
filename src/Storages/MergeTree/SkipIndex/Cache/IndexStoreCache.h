#pragma once

#include <Storages/MergeTree/SkipIndex/Store/IndexStore.h>
#include <Common/CacheBase.h>

namespace DB
{

/// @brief Even if the memory limit is set here, Key-Value expulsion will never occur during runtime.
static constexpr UInt32 INDEX_STORE_CACHE_SIZE = 4294967295U; // 4GB
using StoreKey = std::pair<String, String>;

template <typename T>
struct KeyHash;

/// @brief template specialization
template <>
struct KeyHash<StoreKey>
{
public:
    size_t operator()(const StoreKey & p) const
    {
        auto hash_first = std::hash<std::string>{}(p.first);
        auto hash_second = std::hash<std::string>{}(p.second);
        return hash_first ^ hash_second; // XOR
    }
};

template <typename StoreType, typename WeightFunc, typename StoreTypePtr>
class IndexStoreCache
{
public:
    IndexStoreCache(size_t max_size, String log_name) : cache(max_size), log(&Poco::Logger::get(log_name + " Index Store Cache"))
    {
        LOG_INFO(log, "{} Index Store Cache initialized", log_name);
    }

    virtual ~IndexStoreCache() = default;

    /// avoid destory singleton.
    IndexStoreCache(const IndexStoreCache &) = delete;
    IndexStoreCache & operator=(const IndexStoreCache &) = delete;

    virtual void insertStore(const StoreKey & key, const StoreTypePtr value)
    {
        this->cache.set(key, value);
        LOG_TRACE(log, "Inserted key-value pair into index store cache. Key: ({}, {})", key.first, key.second);
    }

    virtual StoreTypePtr getStore(const StoreKey & key)
    {
        StoreTypePtr value = this->cache.get(key);
        if (value)
        {
            LOG_TRACE(log, "Retrieved value from index store cache. Key: ({}, {})", key.first, key.second);
        }
        else
        {
            LOG_TRACE(log, "Key not found in index store cache. Key: ({}, {})", key.first, key.second);
        }
        return value;
    }

    virtual void removeStore(const StoreKey & key)
    {
        this->cache.remove(key);
        LOG_TRACE(log, "Removed key-value pair from index store cache. Key: ({}, {})", key.first, key.second);
    }

    virtual size_t count() const { return cache.count(); }


protected:
    CacheBase<StoreKey, StoreType, KeyHash<StoreKey>, WeightFunc> cache;
    Poco::Logger * log;
};

template <typename StoreType, typename WeightFunc, typename StoreTypePtr>
using IndexStoreCachePtr = std::shared_ptr<IndexStoreCache<StoreType, WeightFunc, StoreTypePtr>>;

}
