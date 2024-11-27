#pragma once

#include <map>
#include <mutex>
#include <functional>
#include <optional>

#include <Columns/IColumn.h>
#include <Common/CacheBase.h>
#include <Storages/KeyDescription.h>

namespace DB
{


class ColumnsWeightFunc
{
public:
    size_t operator()(const Columns & cols) const
    {
        size_t total_size = 0;
        for (auto & column : cols)
        {
            total_size += column->byteSize();
        }
        return total_size;
    }
};


class PKCacheManager
{
public:
    void setPartPkCache(String cache_key, Columns columns);
    std::optional<Columns> getPartPkCache(String cache_key);
    void removeFromPKCache(const String & cache_key);

    /// tools
    static bool isSupportedPrimaryKey(const KeyDescription & kd);


private:
    CacheBase<String, Columns, std::hash<String>, ColumnsWeightFunc> cache_ex;
    LoggerPtr log;

    explicit PKCacheManager(size_t max_size);
    ~PKCacheManager() = default;

public:
    static PKCacheManager & getMgr();

/// no copy
public:
    PKCacheManager(const PKCacheManager &) = delete;
    PKCacheManager & operator=(const PKCacheManager &) = delete;
};

}
