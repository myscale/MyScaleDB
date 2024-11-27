#include <memory>
#include <optional>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Cache/PKCacheManager.h>

namespace DB
{


PKCacheManager::PKCacheManager(size_t max_size)
    : cache_ex(max_size), log(getLogger("PKCacheManager"))
{
    LOG_INFO(log, "PrimaryKeyCache size limit is: {}", max_size);
}


void PKCacheManager::setPartPkCache(String cache_key, Columns columns)
{
    LOG_INFO(log, "PrimaryKeyCache put cache_key={}", cache_key);

    /// type of clickhouse LRUCache's value must be std::shard_ptr
    std::shared_ptr<Columns> cols_ptr = std::make_shared<Columns>(columns);

    cache_ex.set(cache_key, cols_ptr);
}


std::optional<Columns> PKCacheManager::getPartPkCache(String cache_key)
{
    std::shared_ptr<Columns> pk_cache = cache_ex.get(cache_key);
    if (!pk_cache)
        return std::nullopt;

    return *pk_cache;
}

void PKCacheManager::removeFromPKCache(const String & cache_key)
{
    return cache_ex.remove(cache_key);
}


bool PKCacheManager::isSupportedPrimaryKey(const KeyDescription & primary_key)
{
    if (primary_key.data_types.size() != 1)
        return false;

    return primary_key.data_types[0]->isValueRepresentedByNumber();
}


PKCacheManager & PKCacheManager::getMgr()
{
    static PKCacheManager mgr(Context::getGlobalContextInstance()->getPKCacheSize());
    return mgr;
}

}


