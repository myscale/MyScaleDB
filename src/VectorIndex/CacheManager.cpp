#include <memory>
#include <VectorIndex/CacheManager.h>

#include <VectorIndex/IndexException.h>
#include <VectorIndex/VectorSegmentExecutor.h>
#include <Interpreters/VectorIndexEventLog.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace VectorIndex
{

size_t IndexWithMetaWeightFunc::operator()(const IndexWithMeta & index_meta) const
{
    return index_meta.index->getResourceUsage().memory_usage_bytes;
}

void IndexWithMetaReleaseFunction::operator()(std::shared_ptr<IndexWithMeta> index_meta_ptr)
{
    if (index_meta_ptr)
        index_meta_ptr.reset();
}

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

    DB::VectorIndexEventLog::addEventLog(
        DB::Context::getGlobalContextInstance(),
        cache_key.getTableUUID(),
        cache_key.getPartName(),
        cache_key.getPartitionID(),
        DB::VectorIndexEventLogElement::LOAD_START);

    if (!cache->getOrSet(
            cache_key, [&]() { return index; }))
    {
        LOG_DEBUG(log, "Put into cache: {} failed", cache_key.toString());
        DB::VectorIndexEventLog::addEventLog(
            DB::Context::getGlobalContextInstance(),
            cache_key.getTableUUID(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VectorIndexEventLogElement::LOAD_FAILED);
    }
    else
    {
        DB::VectorIndexEventLog::addEventLog(
            DB::Context::getGlobalContextInstance(),
            cache_key.getTableUUID(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VectorIndexEventLogElement::LOAD_SUCCEED);
    }
}

void CacheManager::flushWillUnloadLog()
{
    auto cache_mgr = getInstance();
    auto cache_items = cache_mgr->cache->getCacheList();
    for (auto item : cache_items)
    {
        auto context_ = DB::Context::getGlobalContextInstance();
        auto cache_key = item.first;
        if (context_)
            DB::VectorIndexEventLog::addEventLog(
                context_,
                cache_key.getTableUUID(),
                cache_key.getPartName(),
                cache_key.getPartitionID(),
                DB::VectorIndexEventLogElement::WILLUNLOAD);
    }
}

size_t CacheManager::countItem() const
{
    return cache->size();
}

void CacheManager::forceExpire(const CacheKey & cache_key)
{
    LOG_INFO(log, "Force expire cache: cache_key = {}", cache_key.toString());
    auto global_context = DB::Context::getGlobalContextInstance();
    if (global_context)
        DB::VectorIndexEventLog::addEventLog(
            global_context,
            cache_key.getTableUUID(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VectorIndexEventLogElement::CACHE_EXPIRE);
    return cache->tryRemove(cache_key);
}

IndexWithMetaHolderPtr CacheManager::load(const CacheKey & cache_key, 
                                          std::function<IndexWithMetaPtr()> load_func)
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
