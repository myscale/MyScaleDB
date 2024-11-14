#pragma once

#include <Storages/MergeTree/SkipIndex/Cache/TantivyIndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Factory/IndexStoreFactory.h>
#include <Storages/MergeTree/SkipIndex/Factory/IndexStoreFactoryImpl.h>
#include <Storages/MergeTree/SkipIndex/Store/TantivyIndexStore.h>

namespace DB
{

class TantivyIndexFactory : public IndexStoreFactory<TantivyIndexStoreCache, TantivyIndexStore, TantivyIndexStorePtr>
{
public:
    TantivyIndexFactory(const String & log_name, TantivyIndexStoreCachePtr cache, MutateRecordCachePtr mutate_to_from)
        : IndexStoreFactory<TantivyIndexStoreCache, TantivyIndexStore, TantivyIndexStorePtr>(log_name, cache, mutate_to_from)
    {
    }

    static TantivyIndexFactory & instance()
    {
        static TantivyIndexFactory instance(
            "Tantivy",
            std::make_shared<TantivyIndexStoreCache>(INDEX_STORE_CACHE_SIZE, "Tantivy"),
            std::make_shared<MutateRecordCache>(INDEX_STORE_CACHE_SIZE));
        return instance;
    }
};


}
