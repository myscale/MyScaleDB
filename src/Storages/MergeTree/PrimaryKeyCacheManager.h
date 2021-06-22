#pragma once

#include <map>
#include <mutex>
#include <functional>
#include <optional>

#include <Common/CacheBase.h>
#include <Columns/IColumn.h>
#include <Storages/KeyDescription.h>

namespace DB
{


class ColumnsWeightFunc
{
public:
    size_t operator()(const Columns & cols) const
    {
        size_t n = 0;
        for (auto it = cols.begin(); it != cols.cend(); ++it)
        {
            n += (*it)->size();
        }
        return n;
    }
};


class PrimaryKeyCacheManager
{
public:
    void setPartPkCache(String part_name, Columns columns);
    std::optional<Columns> getPartPkCache(String part_name);

    /// tools

    static bool isSupportedPrimaryKey(const KeyDescription & kd);


private:
    CacheBase<String, Columns, std::hash<String>, ColumnsWeightFunc> cache_ex;

    explicit PrimaryKeyCacheManager(size_t max_size);
    ~PrimaryKeyCacheManager() = default;

public:
    static PrimaryKeyCacheManager & getMgr();

/// no copy
public:
    PrimaryKeyCacheManager(const PrimaryKeyCacheManager &) = delete;
    PrimaryKeyCacheManager & operator=(const PrimaryKeyCacheManager &) = delete;
};

}
