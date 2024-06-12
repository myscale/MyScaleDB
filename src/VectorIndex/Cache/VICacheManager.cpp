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

#include <memory>

#include <Interpreters/Context.h>
#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Common/VIException.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <Common/CurrentMetrics.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace CurrentMetrics
{
extern const Metric VectorIndexCacheManagerSize;
}

namespace VectorIndex
{

std::unique_ptr<VectorIndexCache> VICacheManager::cache;

size_t IndexWithMetaWeightFunc::operator()(const VIWithMeta & index_meta) const
{
    size_t res;
    std::visit([&res](auto &&index_ptr)
               {
                   res = index_ptr->getResourceUsage().memory_usage_bytes;
               }, index_meta.index);
    return res;
}

void IndexWithMetaReleaseFunction::operator()(std::shared_ptr<VIWithMeta> index_meta_ptr)
{
    if (index_meta_ptr)
        index_meta_ptr.reset();
}

VICacheManager::VICacheManager(int) : log(&Poco::Logger::get("VICacheManager"))
{
    while (!m)
    {
        sleep(100);
    }

    cache = std::make_unique<VectorIndexCache>(cache_size_in_bytes);
}

VICacheManager * VICacheManager::getInstance()
{
    constexpr int unused = 0;
    static VICacheManager cache_mgr(unused);
    return &cache_mgr;
}

IndexWithMetaHolderPtr VICacheManager::get(const CacheKey & cache_key)
{
    if (!cache)
    {
        throw VIException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }

    auto value = cache->get(cache_key);

    return value;
}

void VICacheManager::put(const CacheKey & cache_key, VectorIndexWithMetaPtr index)
{
    if (!cache)
    {
        throw VIException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }
    LOG_INFO(log, "Put into cache: cache_key = {}", cache_key.toString());

    DB::VIEventLog::addEventLog(
        DB::Context::getGlobalContextInstance(),
        cache_key.getTableUUID(),
        cache_key.getIndexName(),
        cache_key.getPartName(),
        cache_key.getPartitionID(),
        DB::VIEventLogElement::LOAD_START);

    if (!cache->getOrSet(
            cache_key, [&]() { return index; }))
    {
        LOG_DEBUG(log, "Put into cache: {} failed", cache_key.toString());
        DB::VIEventLog::addEventLog(
            DB::Context::getGlobalContextInstance(),
            cache_key.getTableUUID(),
            cache_key.getIndexName(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VIEventLogElement::LOAD_FAILED);
    }
    else
    {
        DB::VIEventLog::addEventLog(
            DB::Context::getGlobalContextInstance(),
            cache_key.getTableUUID(),
            cache_key.getIndexName(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VIEventLogElement::LOAD_SUCCEED);
    }
}

size_t VICacheManager::countItem() const
{
    return cache->size();
}

void VICacheManager::forceExpire(const CacheKey & cache_key)
{
    LOG_INFO(log, "Force expire cache: cache_key = {}", cache_key.toString());
    auto global_context = DB::Context::getGlobalContextInstance();
    if (global_context)
        DB::VIEventLog::addEventLog(
            global_context,
            cache_key.getTableUUID(),
            cache_key.getIndexName(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VIEventLogElement::CACHE_EXPIRE,
            cache_key.getCurPartName());
    cache->tryRemove(cache_key);
}

IndexWithMetaHolderPtr VICacheManager::load(const CacheKey & cache_key,
                                          std::function<VectorIndexWithMetaPtr()> load_func)
{
    if (!cache)
    {
        throw VIException(DB::ErrorCodes::LOGICAL_ERROR, "load: cache not allocated");
    }
    LOG_INFO(log, "Start loading cache: cache_key = {}", cache_key.toString());

    auto value = cache->getOrSet(cache_key, load_func);

    return value;
}

void VICacheManager::setCacheSize(size_t size_in_bytes)
{
    cache_size_in_bytes = size_in_bytes;

    if (m && cache)
        cache->updateMaxWeight(size_in_bytes);

    m = true;

    CurrentMetrics::set(CurrentMetrics::VectorIndexCacheManagerSize, size_in_bytes);
}

std::list<std::pair<CacheKey, VIParameter>> VICacheManager::getAllItems()
{
    std::list<std::pair<CacheKey, VIParameter>> result;

    std::list<std::pair<CacheKey, std::shared_ptr<VIWithMeta>>> cache_list = cache->getCacheList();

    for (auto cache_item : cache_list)
    {
        // key   --- string
        // value --- std::shared_ptr<VIWithMeta>
        result.emplace_back(std::make_pair(cache_item.first, cache_item.second->des));
    }
    return result;
}

std::list<std::pair<CacheKey, VIParameter>> VICacheManager::getAllCacheNames()
{
    return getInstance()->getAllItems();
}

bool VICacheManager::storedInCache(const CacheKey & cache_key)
{
    VICacheManager * mgr = getInstance();

    IndexWithMetaHolderPtr column_index = mgr->get(cache_key);

    if (column_index)
        return true;
    else
        return false;
}

void VICacheManager::removeFromCache(const CacheKey & cache_key)
{
    Poco::Logger * log = &Poco::Logger::get("VICacheManager");
    VICacheManager * mgr = getInstance();
    LOG_DEBUG(log, "Num of cache items before forceExpire {} ", mgr->countItem());
    mgr->forceExpire(cache_key);
    LOG_DEBUG(log, "Num of cache items after forceExpire {} ", mgr->countItem());
}

}
