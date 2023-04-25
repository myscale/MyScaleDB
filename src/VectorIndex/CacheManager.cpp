#include <memory>
#include <VectorIndex/CacheManager.h>

#include <VectorIndex/IndexException.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace VectorIndex
{

CacheManager::CacheManager(int) : log(&Poco::Logger::get("CacheManager"))
{
    while (!m)
    {
        sleep(100);
    }

    cache = std::make_unique<VectorIndexCache>(cache_size_in_bytes);
}

CacheManager * CacheManager::getInstance()
{
    constexpr int unused = 0;
    static CacheManager cache_mgr(unused);
    return &cache_mgr;
}

IndexWithMetaHolderPtr CacheManager::get(const CacheKey & cache_key)
{
    if (!cache)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }

    return cache->get(cache_key);
}

void CacheManager::put(const CacheKey & cache_key, IndexWithMetaPtr index)
{
    if (!cache)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }
    LOG_INFO(log, "Put into cache: cache_key = {}", cache_key.toString());

    cache->getOrSet(cache_key, [&]() { return index; });
}

size_t CacheManager::countItem() const
{
    return cache->size();
}

void CacheManager::forceExpire(const CacheKey & cache_key)
{
    LOG_INFO(log, "Force expire cache: cache_key = {}", cache_key.toString());
    return cache->tryRemove(cache_key);
}

IndexWithMetaHolderPtr CacheManager::load(const CacheKey & cache_key, std::function<IndexWithMetaPtr()> load_func)
{
    if (!cache)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "load: cache not allocated");
    }
    LOG_INFO(log, "Start loading cache: cache_key = {}", cache_key.toString());

    return cache->getOrSet(cache_key, load_func);
}

void CacheManager::setCacheSize(size_t size_in_bytes)
{
    cache_size_in_bytes = size_in_bytes;
    m = true;
}

std::list<std::pair<CacheKey, Search::Parameters>> CacheManager::getAllItems()
{
    std::list<std::pair<CacheKey, Search::Parameters>> result;

    std::list<std::pair<CacheKey, std::shared_ptr<IndexWithMeta>>> cache_list = cache->getCacheList();

    for (auto cache_item : cache_list)
    {
        // key   --- string
        // value --- std::shared_ptr<IndexWithMeta>
        result.emplace_back(std::make_pair(cache_item.first, cache_item.second->des));
    }
    return result;
}

}
