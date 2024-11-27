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

size_t CachedSegmentWeightFunc::operator()(const CachedSegment & cached_segment) const
{
    return calculateInnerSegmentMemoryUsage(cached_segment.index, cached_segment.getDeleteBitmap());
}

void CachedSegmentReleaseFunction::operator()(std::shared_ptr<CachedSegment> cached_segment_ptr)
{
    if (cached_segment_ptr)
    {
        cached_segment_ptr->index_load_memory_size_metric.changeTo(0);
    }
}

VICacheManager::VICacheManager(int) : log(getLogger("VICacheManager"))
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

CachedSegmentHolderPtr VICacheManager::get(const CachedSegmentKey & cache_key)
{
    if (!cache)
    {
        throw VIException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }

    auto value = cache->get(cache_key);

    return value;
}

void VICacheManager::put(const CachedSegmentKey & cache_key, CachedSegmentPtr index)
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

void VICacheManager::forceExpire(const CachedSegmentKey & cache_key)
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

CachedSegmentHolderPtr VICacheManager::load(const CachedSegmentKey & cache_key,
                                          std::function<CachedSegmentPtr()> load_func)
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

std::list<std::pair<CachedSegmentKey, std::shared_ptr<DB::VIDescription>>> VICacheManager::getAllItems(bool exclude_expired)
{
    std::list<std::pair<CachedSegmentKey, std::shared_ptr<DB::VIDescription>>> result;

    std::list<std::pair<CachedSegmentKey, std::shared_ptr<CachedSegment>>> cache_list = getInstance()->cache->getCachedList(exclude_expired);

    for (auto cache_item : cache_list)
    {
        // key   --- string
        // value --- std::shared_ptr<CachedSegment>
        result.emplace_back(std::make_pair(cache_item.first, cache_item.second->vec_desc));
    }
    return result;
}

bool VICacheManager::storedInCache(const CachedSegmentKey & cache_key)
{
    VICacheManager * mgr = getInstance();

    CachedSegmentHolderPtr cached_segment = mgr->get(cache_key);
    return cached_segment != nullptr;
}

void VICacheManager::removeFromCache(const CachedSegmentKey & cache_key)
{
    LoggerPtr log = getLogger("VICacheManager");
    VICacheManager * mgr = getInstance();
    LOG_DEBUG(log, "Num of cache items before forceExpire {} ", mgr->countItem());
    mgr->forceExpire(cache_key);
    LOG_DEBUG(log, "Num of cache items after forceExpire {} ", mgr->countItem());
}

}
