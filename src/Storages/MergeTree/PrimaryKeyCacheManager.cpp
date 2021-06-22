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
    /// too rigid

    Columns *cols = new Columns(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        (*cols)[i] = columns[i];
    }

    std::shared_ptr<Columns> cols_ptr;
    cols_ptr.reset(cols);

    cache_ex.set(part_name, cols_ptr);
}


std::optional<Columns> PrimaryKeyCacheManager::getPartPkCache(String part_name)
{
    std::shared_ptr<Columns> v = cache_ex.get(part_name);
    if (v == nullptr)
    {
        return std::nullopt;
    }
    else
    {
        return {*v};
    }
}


bool PrimaryKeyCacheManager::isSupportedPrimaryKey(const KeyDescription & kd)
{
    size_t n = kd.data_types.size();
    if (n != 1)
    {
        return false;
    }
    String type_name = kd.data_types[0]->getName();
    return type_name == "UInt32" || type_name == "UInt64";
}


PrimaryKeyCacheManager & PrimaryKeyCacheManager::getMgr()
{
    constexpr size_t MaxSize = static_cast<size_t>(1) << 30;
    static PrimaryKeyCacheManager mgr(MaxSize);
    return mgr;
}

}


