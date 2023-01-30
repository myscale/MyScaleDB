#include <memory>
#include <optional>

#include <Storages/MergeTree/PrimaryKeyCacheManager.h>

namespace DB
{


PrimaryKeyCacheManager::PrimaryKeyCacheManager(size_t max_size)
: cache_ex("LRU", max_size)
{
}


void PrimaryKeyCacheManager::setPartPkCache(String part_name, Columns columns)
{
    /// type of clickhouse LRUCache's value must be std::shard_ptr
    std::shared_ptr<Columns> cols_ptr = std::make_shared<Columns>(columns);

    cache_ex.set(part_name, cols_ptr);
}


std::optional<Columns> PrimaryKeyCacheManager::getPartPkCache(String part_name)
{
    std::shared_ptr<Columns> pk_cache = cache_ex.get(part_name);
    if (!pk_cache)
        return std::nullopt;

    return *pk_cache;
}


bool PrimaryKeyCacheManager::isSupportedPrimaryKey(const KeyDescription & primary_key)
{
    if (primary_key.data_types.size() != 1)
        return false;

    String type_name = primary_key.data_types[0]->getName();
    return type_name == "UInt32" || type_name == "UInt64";
}


PrimaryKeyCacheManager & PrimaryKeyCacheManager::getMgr()
{
    constexpr size_t MaxSize = static_cast<size_t>(1) << 30;
    static PrimaryKeyCacheManager mgr(MaxSize);
    return mgr;
}

}


