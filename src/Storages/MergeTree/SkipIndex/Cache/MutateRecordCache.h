#pragma once

#include <Common/CacheBase.h>

namespace DB
{

/// @brief Even if the memory limit is set here, Key-Value expulsion will never occur during runtime.
static constexpr UInt32 MUTATE_RECORD_CACHE_SIZE = 4294967295U; // 4GB


class MutateRecordWeightFunc
{
public:
    size_t operator()(const String & /* value */) const { return 1; }
};


class MutateRecordCache
{
public:
    MutateRecordCache(const MutateRecordCache &) = delete;
    MutateRecordCache & operator=(const MutateRecordCache &) = delete;

    explicit MutateRecordCache(size_t max_size) : cache(max_size) { }

    ~MutateRecordCache() = default;

    /// @brief key: part rel_path after mutate, value: part rel_path before mutate.
    void insert(const String & key, const String & value) { this->cache.set(key, std::make_shared<String>(value)); }

    /// @brief get part rel_path before mutate, return empty string if not exists.
    String get(const String & key)
    {
        auto res = this->cache.get(key);
        if (res == nullptr)
        {
            const static String empty_string = "";
            return empty_string;
        }
        else
        {
            return *res;
        }
    }

    /// @brief remove mutate record.
    void remove(const String & key) { this->cache.remove(key); }

    size_t count() { return this->cache.count(); }


private:
    CacheBase<String, String, std::hash<String>, MutateRecordWeightFunc> cache;
};

using MutateRecordCachePtr = std::shared_ptr<MutateRecordCache>;


}
