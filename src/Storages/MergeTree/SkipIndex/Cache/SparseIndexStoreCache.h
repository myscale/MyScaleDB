#pragma once

#include <Storages/MergeTree/SkipIndex/Cache/IndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Store/SparseIndexStore.h>
#include <Common/CacheBase.h>

namespace DB
{

class SparseIndexStoreCache : public IndexStoreCache<SparseIndexStore, SparseIndexStoreWeightFunc, SparseIndexStorePtr>
{
public:
    SparseIndexStoreCache(size_t max_size, String log_name) : IndexStoreCache(max_size, log_name) { }

    ~SparseIndexStoreCache() override = default;

    SparseIndexStoreCache & getInstance()
    {
        static SparseIndexStoreCache sparse_cache(INDEX_STORE_CACHE_SIZE, "Sparse");
        return sparse_cache;
    }
};

using SparseIndexStoreCachePtr = std::shared_ptr<SparseIndexStoreCache>;


}
