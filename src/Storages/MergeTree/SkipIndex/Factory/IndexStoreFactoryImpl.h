#pragma once

#include <Storages/MergeTree/SkipIndex/Factory/IndexStoreFactory.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event SI_RemoveCache_Milliseconds;
    extern const Event SI_RenamePart_Milliseconds;
    extern const Event SI_DropIndex_Milliseconds;
    extern const Event SI_GetForBuild_Milliseconds;
    extern const Event SI_GetInitForBuild_Milliseconds;
    extern const Event SI_Mutate_Milliseconds;
    extern const Event SI_GetLoadForSearch_Milliseconds;

    extern const Event SI_RemoveCache_Counts;
    extern const Event SI_RenamePart_Counts;
    extern const Event SI_DropIndex_Counts;
    extern const Event SI_GetForBuild_Counts;
    extern const Event SI_GetInitForBuild_Counts;
    extern const Event SI_Mutate_Counts;
    extern const Event SI_GetLoadForSearch_Counts;
}

namespace DB
{


template <typename CacheType, typename StoreType, typename StoreTypePtr>
IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::IndexStoreFactory(
    const String & log_name, std::shared_ptr<CacheType> cache_, MutateRecordCachePtr mutate_to_from_)
    : log(&Poco::Logger::get(log_name + "IndexFactory")), cache(cache_), mutate_to_from(mutate_to_from_)
{
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::get(const StoreKey & store_key)
{
    return this->cache->getStore(store_key);
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr
IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::getForBuild(const String & skp_index_name, const DataPartStoragePtr storage)
{
    Stopwatch watch;

    auto res = this->cache->getStore(std::make_pair(storage->getRelativePath(), skp_index_name));

    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_GetForBuild_Milliseconds, watch.elapsedMilliseconds());
    ProfileEvents::increment(ProfileEvents::SI_GetForBuild_Counts);
    return res;
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr
IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::getOrLoadForSearch(const String & skp_index_name, const DataPartStoragePtr storage)
{
    DB::OpenTelemetry::SpanHolder span("index_store_factory::get_or_load_for_search");
    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::SI_GetLoadForSearch_Counts);

    auto store_key = std::make_pair(storage->getRelativePath(), skp_index_name);
    // First check
    StoreTypePtr res = this->cache->getStore(store_key);
    if (res != nullptr)
    {
        LOG_DEBUG(
            this->log,
            "[getOrLoadForSearch] store_key: [{}, {}], full_index_path: {}",
            store_key.first,
            store_key.second,
            res->getFullIndexPathInCache());
        watch.stop();
        ProfileEvents::increment(ProfileEvents::SI_GetLoadForSearch_Milliseconds, watch.elapsedMilliseconds());
        return res;
    }

    // Avoid multi-thread entry multi times.
    std::unique_lock<std::shared_mutex> lock(this->mutex_for_search);

    // Second check
    res = this->cache->getStore(store_key);
    if (res != nullptr)
    {
        watch.stop();
        ProfileEvents::increment(ProfileEvents::SI_GetLoadForSearch_Milliseconds, watch.elapsedMilliseconds());
        return res;
    }

    // Ensure only one thread can generate store object, avoid index files corrupt.
    StoreTypePtr new_store = std::make_shared<StoreType>(skp_index_name, storage);
    this->cache->insertStore(store_key, new_store);
    LOG_INFO(
        this->log,
        "[getOrLoadForSearch] store_key: [{}, {}], ref count : {}, `store` size: {}, "
        "`mutate_to_from` size: {}",
        store_key.first,
        store_key.second,
        new_store.use_count(),
        this->cache->count(),
        this->mutate_to_from->count());
    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_GetLoadForSearch_Milliseconds, watch.elapsedMilliseconds());
    return new_store;
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::getOrInitForBuild(
    const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder)
{
    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::SI_GetInitForBuild_Counts);

    auto store_key = std::make_pair(storage->getRelativePath(), skp_index_name);

    // First check
    StoreTypePtr res = this->cache->getStore(store_key);
    if (res != nullptr)
    {
        LOG_DEBUG(
            this->log,
            "[getOrInitForBuild] store_key: [{}, {}], full_index_path: {}",
            store_key.first,
            store_key.second,
            res->getFullIndexPathInCache());
        watch.stop();
        ProfileEvents::increment(ProfileEvents::SI_GetInitForBuild_Milliseconds, watch.elapsedMilliseconds());
        return res;
    }

    // Avoid multi-thread entry multi times.
    std::unique_lock<std::shared_mutex> lock(this->mutex_for_build);

    // Second check
    res = this->cache->getStore(store_key);
    if (res != nullptr)
    {
        watch.stop();
        ProfileEvents::increment(ProfileEvents::SI_GetInitForBuild_Milliseconds, watch.elapsedMilliseconds());
        return res;
    }

    // Ensure only one thread can generate store object, avoid index files corrupt.
    StoreTypePtr new_store = std::make_shared<StoreType>(skp_index_name, storage, storage_builder);
    this->cache->insertStore(store_key, new_store);
    LOG_INFO(
        this->log,
        "[getOrInitForBuild] store_key: [{}, {}], ref_count: {}, `store` size: {}, "
        "`mutate_to_from` size: {}",
        store_key.first,
        store_key.second,
        new_store.use_count(),
        this->cache->count(),
        this->mutate_to_from->count());
    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_GetInitForBuild_Milliseconds, watch.elapsedMilliseconds());
    return new_store;
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
size_t IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::remove(const String & data_part_relative_path, const DB::Names & index_names)
{
    Stopwatch watch;
    size_t hitted = 0;
    ProfileEvents::increment(ProfileEvents::SI_RemoveCache_Counts);

    for (size_t i = 0; i < index_names.size(); i++)
    {
        auto store_key = std::make_pair(data_part_relative_path, SKP_PREFIX + index_names[i]);
        StoreTypePtr res = this->cache->getStore(store_key);
        if (res)
        {
            hitted++;
            this->cache->removeStore(store_key);
        }
    }

    LOG_INFO(
        this->log,
        "[remove] part_rel_path: {}, removed: {}, `stores` size: {}, `mutate_to_from` size: {}",
        data_part_relative_path,
        hitted,
        this->cache->count(),
        this->mutate_to_from->count());

    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_RemoveCache_Milliseconds, watch.elapsedMilliseconds());
    return hitted;
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::mutate(
    const String & source_part_relative_path, const String & target_part_relative_path)
{
    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::SI_Mutate_Counts);

    this->mutate_to_from->insert(target_part_relative_path, source_part_relative_path);
    LOG_INFO(
        this->log,
        "[mutate] from `{} to `{}`, `mutate_to_from` size: {}",
        source_part_relative_path,
        target_part_relative_path,
        this->mutate_to_from->count());
    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_Mutate_Milliseconds, watch.elapsedMilliseconds());
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::renamePart(
    const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage, const DB::Names & index_names)
{
    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::SI_RenamePart_Counts);

    auto context = Context::getGlobalContextInstance();
    auto data_part_relative_path_after_rename = storage->getRelativePath();

    String data_part_relative_path_before_mutate = this->mutate_to_from->get(data_part_relative_path_before_rename);

    if (!data_part_relative_path_before_mutate.empty())
    {
        // We were able to find the mutate record from `mutate_to_from` for that data part.
        // Indicates that LWD and materialize index may have occurred.
        // To make the `IndexStore` reusable, append a new key to the `IndexStore` and point to the old `IndexStore` shared pointer.
        bool is_lwd = updateStoresForMutate(data_part_relative_path_before_mutate, data_part_relative_path_after_rename, index_names);

        if (!is_lwd)
        {
            // After rename operation is complete, the `IndexStore` should update the index cache directory (starts with `tmp`)
            auto target_part_full_path_in_cache = this->innerCreateNewPathInCache(storage->getRelativePath());

            // The `data_part_relative_path_before_mutate` record before mutate occurred cannot be found in the stores keys,
            // indicates that the mutate operation may execute `MATERIALIZE INDEX`.
            updateStoresForBuild(
                data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_full_path_in_cache, index_names);
        }
        // Remove mutate operation from `mutate_to_from` record.
        this->mutate_to_from->remove(data_part_relative_path_before_rename);

        LOG_INFO(
            this->log,
            "[renamePart] after mutate(lwd:{}), before_rename {}, after_rename {}, `stores` size: {}, "
            "`mutate_to_from` size: {}",
            is_lwd,
            is_lwd ? data_part_relative_path_before_mutate : data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            this->cache->count(),
            this->mutate_to_from->count());
    }
    else
    {
        // After rename operation is complete, the `IndexStore` should update the index cache directory (starts with `tmp`)
        auto target_part_full_path_in_cache = this->innerCreateNewPathInCache(storage->getRelativePath());


        // We can't find the mutate record from `mutate_to_from` for that data part.
        // Indicates that `tmp_insert`, `tmp_merge` and `tmp_clone` may have occurred.
        // These operations will generate a new `IndexStore`, we need to update the index cache directory in this tmp `IndexStore`.
        updateStoresForBuild(
            data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_full_path_in_cache, index_names);

        LOG_INFO(
            this->log,
            "[renamePart] after insert/merge/xxx, before_rename {}, after_rename {}, `stores` size: {}, "
            "`mutate_to_from` size: {}",
            data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            this->cache->count(),
            this->mutate_to_from->count());
    }
    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_RenamePart_Milliseconds, watch.elapsedMilliseconds());
}


// TODO dropIndex 后面需要加上额外的 disk 清理代码
template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::dropIndex(const String & skp_index_name, const DataPartStoragePtr storage)
{
    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::SI_DropIndex_Counts);

    try
    {
        // storage->getRelativePath(): store/069/069cd2be-0a8f-4091-ad82-38015b19bdef/all_1_1_0
        StoreKey store_key = std::make_pair(storage->getRelativePath(), skp_index_name);
        this->cache->removeStore(store_key);


        // TODO dropIndex 之后，这里的逻辑怎么写比较合适？
        // fs::path data_part_relative_path = fs::path(storage->getRelativePath());
        // TantivyIndexFilesManager::removeTantivyIndexInCache(data_part_relative_path, skp_index_name);
        // TantivyIndexFilesManager::removeEmptyTableUUIDInCache(data_part_relative_path);
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            this->log,
            "[dropIndex] Error happened when dropIndex, stores(build/search) may not be cleaned correctly, data part relative path {}, "
            "exception is {}",
            storage->getRelativePath(),
            e.what());
    }
    watch.stop();
    ProfileEvents::increment(ProfileEvents::SI_DropIndex_Milliseconds, watch.elapsedMilliseconds());
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::updateStoresForBuild(
    const String & data_part_relative_path_before_rename, // tmp_mut_all_20930_20950_1_x
    const String & data_part_relative_path_after_rename, // all_30010_30010_1_x
    const String & target_part_full_path_in_cache, // /xxx/tantivy_index_cache/store/6a5/xxx/all_47761_47823_1_47829
    const DB::Names & index_names)
{
    std::unordered_map<StoreKey, StoreTypePtr, KeyHash<StoreKey>> stores_need_append;
    std::vector<StoreKey> old_keys_to_remove;

    for (size_t i = 0; i < index_names.size(); i++)
    {
        StoreKey store_key = std::make_pair(data_part_relative_path_before_rename, SKP_PREFIX + index_names[i]);
        StoreTypePtr store_ptr = this->cache->getStore(store_key);
        if (store_ptr)
        {
            // TODO 这里传递的目录应该是 cache 的全路径才对，这样可以避免潜在的并发问题
            store_ptr->updateFullIndexPathInCache(target_part_full_path_in_cache);
            // Update store_key in this->stores.
            StoreKey new_key = std::make_pair(data_part_relative_path_after_rename, SKP_PREFIX + index_names[i]);
            stores_need_append[new_key] = store_ptr;
            old_keys_to_remove.push_back(store_key);
        }
    }

    // add new keys with updated stores.
    for (auto & [key, store] : stores_need_append)
    {
        this->cache->insertStore(key, store);
        LOG_INFO(
            this->log,
            "[updateStoresForBuild] insert updated store into `stores`, store_key: [{}, {}], store_inner_index_path(new): "
            "{}, store_size:{}",
            key.first,
            key.second,
            store->getFullIndexPathInCache(),
            this->cache->count());
    }

    // erase old keys

    if (old_keys_to_remove.empty())
    {
        return;
    }

    std::ostringstream oss;

    // oss << "[";
    for (auto & old_key : old_keys_to_remove)
    {
        this->cache->removeStore(old_key);
        // oss << "(" << old_key.first << "," << old_key.second << "),";
    }
    // oss << "]";

    // LOG_INFO(this->log, "[updateStoresForBuild] remove old store keys: {}, store_size:{}", oss.str(), this->cache->count());
    LOG_INFO(this->log, "[updateStoresForBuild] removed keys: {}, store_size:{}", old_keys_to_remove.size(), this->cache->count());
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
bool IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::updateStoresForMutate(
    const String & data_part_relative_path_before_mutate, // [all_20930_20950_1_20960] -> mutate -> [tmp_mut_all_20930_20950_1_x]
    const String & data_part_relative_path_after_rename, // [tmp_mut_all_20930_20950_1_x] -> rename -> [all_20930_20950_1_x]
    const DB::Names & index_names)
{
    bool is_lwd = false;
    for (size_t i = 0; i < index_names.size(); i++)
    {
        StoreKey old_key = std::make_pair(data_part_relative_path_before_mutate, SKP_PREFIX + index_names[i]);
        StoreKey new_key = std::make_pair(data_part_relative_path_after_rename, SKP_PREFIX + index_names[i]);

        StoreTypePtr old_store_ptr = this->cache->getStore(old_key);
        if (old_store_ptr)
        {
            is_lwd = true;
            this->cache->insertStore(new_key, old_store_ptr);
        }
        LOG_INFO(
            this->log,
            "[updateStoresForMutate] skp_idx_name: {}, is_lwd: {}, old_key: [{},{}], new_key: [{},{}], full_index_path(old): {}, "
            "mutate_size:{}",
            index_names[i],
            is_lwd,
            old_key.first,
            old_key.second,
            new_key.first,
            new_key.second,
            old_store_ptr == nullptr ? "" : old_store_ptr->getFullIndexPathInCache(),
            this->mutate_to_from->count());
    }
    return is_lwd;
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::freeIdleStoreReader(
    const DataPartStoragePtr storage, const DB::Names & index_names)
{
    for (size_t i = 0; i < index_names.size(); i++)
    {
        StoreKey store_key = std::make_pair(storage->getRelativePath(), SKP_PREFIX + index_names[i]);

        StoreTypePtr store_ptr = this->cache->getStore(store_key);
        bool all_freed = true;
        if (store_ptr)
        {
            if (store_ptr.use_count() == 2)
            {
                all_freed &= store_ptr->freeIndexReader();
            }
            LOG_INFO(
                this->log,
                "[freeIdleStoreReader] skp_idx_name: {}, key: [{},{}], stores_size: {}, mutate_size: {}",
                index_names[i],
                store_key.first,
                store_key.second,
                this->cache->count(),
                this->mutate_to_from->count());
        }
    }
}

}
