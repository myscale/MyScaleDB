#pragma once

#include <Storages/MergeTree/SkipIndex/Cache/SparseIndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Factory/IndexStoreFactory.h>
#include <Storages/MergeTree/SkipIndex/Factory/IndexStoreFactoryImpl.h>
#include <Storages/MergeTree/SkipIndex/Store/SparseIndexStore.h>

namespace DB
{

class SparseIndexFactory : public IndexStoreFactory<SparseIndexStoreCache, SparseIndexStore, SparseIndexStorePtr>
{
public:
    SparseIndexFactory(const String & log_name, SparseIndexStoreCachePtr cache, MutateRecordCachePtr mutate_to_from)
        : IndexStoreFactory<SparseIndexStoreCache, SparseIndexStore, SparseIndexStorePtr>(log_name, cache, mutate_to_from)
    {
    }

    static SparseIndexFactory & instance()
    {
        static SparseIndexFactory instance(
            "Sparse",
            std::make_shared<SparseIndexStoreCache>(INDEX_STORE_CACHE_SIZE, "Sparse"),
            std::make_shared<MutateRecordCache>(INDEX_STORE_CACHE_SIZE));
        return instance;
    }
};


}
