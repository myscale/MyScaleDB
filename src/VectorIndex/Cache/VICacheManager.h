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

#include <functional>
#include <list>
#include <memory>
#include <string>

#include <Common/LRUResourceCache.h>
#include <VectorIndex/Cache/VICacheObject.h>

namespace std
{
template <>
struct hash<VectorIndex::CacheKey>
{
    std::size_t operator()(VectorIndex::CacheKey const & key) const noexcept { return std::hash<std::string>{}(key.toString()); }
};
}

namespace VectorIndex
{

struct VIWithMeta;
using VectorIndexWithMetaPtr = std::shared_ptr<VIWithMeta>;

static bool m = false;
static size_t cache_size_in_bytes = 0;

class IndexWithMetaWeightFunc
{
public:
    size_t operator()(const VIWithMeta & index_meta) const;
};

class IndexWithMetaReleaseFunction
{
public:
    void operator()(std::shared_ptr<VIWithMeta> index_meta_ptr);
};

using VectorIndexCacheType
    = DB::LRUResourceCache<CacheKey, VIWithMeta, IndexWithMetaWeightFunc, IndexWithMetaReleaseFunction, std::hash<CacheKey>>;
using IndexWithMetaHolderPtr = VectorIndexCacheType::MappedHolderPtr;

class VectorIndexCache
    : public DB::LRUResourceCache<CacheKey, VIWithMeta, IndexWithMetaWeightFunc, IndexWithMetaReleaseFunction, std::hash<CacheKey>>
{
public:
    explicit VectorIndexCache(size_t max_size) : VectorIndexCacheType(max_size) { }

    std::list<std::pair<CacheKey, VectorIndexWithMetaPtr>> getCacheList()
    {
        std::list<std::pair<CacheKey, VectorIndexWithMetaPtr>> res;
        {
            std::lock_guard lock(mutex);

            for (auto it = cells.begin(); it != cells.cend(); ++it)
            {
                res.push_back(std::make_pair(it->first, it->second.value));
            }
        }

        return res;
    }
};

class VICacheManager
{
    // cache manager manages a series of cache instance.
    // these caches could either be cache in memory or cache on GPU device.
    // it privides a getInstance() method which returns a consistent view
    // of all caches to all classes trying to access cache.

private:
    explicit VICacheManager(int);

public:
    void put(const CacheKey & cache_key, VectorIndexWithMetaPtr index);
    IndexWithMetaHolderPtr get(const CacheKey & cache_key);
    size_t countItem() const;
    void forceExpire(const CacheKey & cache_key);
    IndexWithMetaHolderPtr load(const CacheKey & cache_key,
                                std::function<VectorIndexWithMetaPtr()> load_func);
    std::list<std::pair<CacheKey, VIParameter>> getAllItems();

    static VICacheManager * getInstance();
    static void setCacheSize(size_t size_in_bytes);

    static std::list<std::pair<CacheKey, VIParameter>> getAllCacheNames();
    static bool storedInCache(const CacheKey & cache_key);

    static void removeFromCache(const CacheKey & cache_key);

protected:
    static std::unique_ptr<VectorIndexCache> cache;
    Poco::Logger * log;
};

}
