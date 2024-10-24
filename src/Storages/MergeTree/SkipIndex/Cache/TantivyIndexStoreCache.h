#pragma once

#include <Storages/MergeTree/SkipIndex/Cache/IndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Store/TantivyIndexStore.h>
#include <Common/CacheBase.h>

namespace DB
{

class TantivyIndexStoreCache : public IndexStoreCache<TantivyIndexStore, TantivyIndexStoreWeightFunc, TantivyIndexStorePtr>
{
public:
    TantivyIndexStoreCache(size_t max_size, String log_name) : IndexStoreCache(max_size, log_name) { }

    ~TantivyIndexStoreCache() override = default;

    TantivyIndexStoreCache & getInstance()
    {
        static TantivyIndexStoreCache tantivy_cache(INDEX_STORE_CACHE_SIZE, "Tantivy");
        return tantivy_cache;
    }
};

using TantivyIndexStoreCachePtr = std::shared_ptr<TantivyIndexStoreCache>;

}
