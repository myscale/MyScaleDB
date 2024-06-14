#include <Storages/MergeTree/TantivyIndexStoreFactory.h>


namespace DB
{

TantivyIndexStoreFactory & TantivyIndexStoreFactory::instance()
{
    static TantivyIndexStoreFactory instance;
    return instance;
}

TantivyIndexStorePtr TantivyIndexStoreFactory::get(const String & skp_index_name, const DataPartStoragePtr storage)
{
    String store_key = this->combineKey(skp_index_name, storage->getRelativePath());
    std::lock_guard lock(stores_mutex);

    auto it = stores.find(store_key);
    if (it == stores.end())
    {
        LOG_INFO(
            this->log,
            "[get] store is null, store_key is {}, stores size is {}, get from {}",
            store_key,
            stores.size(),
            storage->getRelativePath());
        return nullptr;
    }
    return it->second;
}


TantivyIndexStorePtr TantivyIndexStoreFactory::getOrLoad(const String & skp_index_name, const DataPartStoragePtr storage)
{
    String store_key = this->combineKey(skp_index_name, storage->getRelativePath());
    std::lock_guard lock(stores_mutex);

    auto it = stores.find(store_key);

    if (it == stores.end())
    {
        stores.emplace(store_key, std::make_shared<TantivyIndexStore>(skp_index_name, storage));
        LOG_INFO(
            this->log,
            "[getOrLoad] load store, store_key is {}, stores size is {}, store ref count is: {}, load from {}",
            store_key,
            stores.size(),
            stores[store_key].use_count(),
            storage->getRelativePath());
        return stores[store_key];
    }
    return it->second;
}


TantivyIndexStorePtr TantivyIndexStoreFactory::getOrInit(
    const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder)
{
    String store_key = this->combineKey(skp_index_name, storage_builder->getRelativePath());
    std::lock_guard lock(stores_mutex);

    auto it = stores.find(store_key);
    if (it == stores.end())
    {
        stores.emplace(store_key, std::make_shared<TantivyIndexStore>(skp_index_name, storage, storage_builder));
        LOG_INFO(
            this->log,
            "[getOrInit] init store, store_key is {}, stores size is {}, store ref count is: {}, init from {}",
            store_key,
            stores.size(),
            stores[store_key].use_count(),
            storage->getRelativePath());
        return stores[store_key];
    }
    return it->second;
}

void TantivyIndexStoreFactory::mutate(const String & source_part_relative_path, const String & target_part_relative_path)
{
    std::lock_guard lock(mutate_mutex);
    size_t mutate_to_from_size = this->mutate_to_from.size();
    this->mutate_to_from[target_part_relative_path] = source_part_relative_path;
    LOG_INFO(
        this->log,
        "[mutate] from `{} to `{}`, `mutate_to_from` size increase from {} to {}",
        source_part_relative_path,
        target_part_relative_path,
        mutate_to_from_size,
        this->mutate_to_from.size());
}


void TantivyIndexStoreFactory::updateStoresForBuild(
    const String & data_part_relative_path_before_rename, // tmp_mut_all_20930_20950_1_x
    const String & data_part_relative_path_after_rename, // all_30010_30010_1_x
    const fs::path & target_part_path_in_tantivy_cache // /xxx/tantivy_index_cache/store/6a5/xxx/all_47761_47823_1_47829
)
{
    std::unordered_map<String, TantivyIndexStorePtr> stores_need_append;
    for (auto store_it = stores.begin(); store_it != stores.end();)
    {
        if (store_it->first.find(data_part_relative_path_before_rename) != String::npos)
        {
            // Update `TantivyIndexStore` holds `tantivy_index_cache_path`
            // In this case, the `TantivyIndexStore` just serialized, and the target data part will be active only after the `TantivyIndexStore` is renamed
            store_it->second->updateCacheDataPartRelativeDirectory(target_part_path_in_tantivy_cache);
            auto [skp_index_name, _] = this->splitKey(store_it->first);
            // Update store_key in this->stores.
            String new_key = combineKey(skp_index_name, data_part_relative_path_after_rename);
            stores_need_append[new_key] = store_it->second;
            store_it = stores.erase(store_it);
            // for multi skp index, not need break.
        }
        else
        {
            ++store_it;
        }
    }
    // Transfer of ownership: from `updated_stores` to `this->stores` cache.
    for (auto & [key, store] : stores_need_append)
    {
        stores.insert_or_assign(std::move(key), std::move(store));
    }
}


bool TantivyIndexStoreFactory::updateStoresForMutate(
    const String & data_part_relative_path_before_mutate, // [all_20930_20950_1_20960] -> mutate -> [tmp_mut_all_20930_20950_1_x]
    const String & data_part_relative_path_after_rename // [tmp_mut_all_20930_20950_1_x] -> rename -> [all_20930_20950_1_x]
)
{
    std::vector<std::pair<String, String>> updated_keys;
    bool is_lwd = false;
    for (const auto & [store_key, store] : stores)
    {
        if (store_key.find(data_part_relative_path_before_mutate) != String::npos)
        {
            is_lwd = true;
            auto [skp_index_name, _] = this->splitKey(store_key);
            String new_key = combineKey(skp_index_name, data_part_relative_path_after_rename);
            updated_keys.emplace_back(store_key, new_key);
        }
    }
    for (const auto & [old_key, new_key] : updated_keys)
    {
        stores[new_key] = stores[old_key];
    }
    return is_lwd;
}


void TantivyIndexStoreFactory::renamePart(const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage)
{
    auto context = Context::getGlobalContextInstance();
    auto data_part_relative_path_after_rename = storage->getRelativePath();


    std::lock_guard lock_store(stores_mutex);
    std::lock_guard lock_mutate(mutate_mutex);

    auto it = this->mutate_to_from.find(data_part_relative_path_before_rename);
    if (it != this->mutate_to_from.end())
    {
        size_t stores_size = stores.size();
        // We were able to find the mutate record from `mutate_to_from` for that data part.
        // Indicates that LWD and materialize index may have occurred.
        // To make the `TantivyIndexStore` reusable, append a new key to the `TantivyIndexStore` and point to the old `TantivyIndexStore` shared pointer.
        String data_part_relative_path_before_mutate = it->second;
        bool is_lwd = updateStoresForMutate(data_part_relative_path_before_mutate, data_part_relative_path_after_rename);

        if (!is_lwd)
        {
            // After rename operation is complete, the `TantivyIndexStore` should update the tantivy index cache directory (starts with `tmp`)
            auto target_part_path_in_tantivy_cache = fs::path(context->getTantivyIndexCachePath()) / storage->getRelativePath();

            // The `data_part_relative_path_before_mutate` record before mutate occurred cannot be found in the stores keys,
            // indicates that the mutate operation may execute `MATERIALIZE INDEX`.
            updateStoresForBuild(
                data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_path_in_tantivy_cache);
        }
        // Remove mutate operation from `mutate_to_from` record.
        this->mutate_to_from.erase(it);

        LOG_INFO(
            this->log,
            "[renamePart] after mutate(lwd:{}), from {} to {}, stores size from {} to {}, current `mutate_to_from` size {}",
            is_lwd,
            is_lwd ? data_part_relative_path_before_mutate : data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            stores_size,
            stores.size(),
            mutate_to_from.size());
    }
    else
    {
        size_t stores_size = stores.size();

        // After rename operation is complete, the `TantivyIndexStore` should update the tantivy index cache directory (starts with `tmp`)
        auto target_part_path_in_tantivy_cache = fs::path(context->getTantivyIndexCachePath()) / storage->getRelativePath();

        // We can't find the mutate record from `mutate_to_from` for that data part.
        // Indicates that `tmp_insert`, `tmp_merge` and `tmp_clone` may have occurred.
        // These operations will generate a new `TantivyIndexStore`, we need to update the tantivy index cache directory in this tmp `TantivyIndexStore`.
        updateStoresForBuild(
            data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_path_in_tantivy_cache);

        LOG_INFO(
            this->log,
            "[renamePart] after insert/merge/xxx, from {} to {}, stores size from {} to {}, current mutate_to_from size {}",
            data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            stores_size,
            stores.size(),
            mutate_to_from.size());
    }
}


void TantivyIndexStoreFactory::dropIndex(const String & skp_index_name, const DataPartStoragePtr storage)
{
    try
    {
        auto context = Context::getGlobalContextInstance();
        auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, context->getPath(), 0);
        size_t stores_size = this->stores.size();

        // store->getRelativePath(): store/069/069cd2be-0a8f-4091-ad82-38015b19bdef/all_1_1_0
        fs::path data_part_relative_path = fs::path(storage->getRelativePath());
        String store_key = combineKey(skp_index_name, data_part_relative_path);

        std::lock_guard lock(stores_mutex);

        auto it = this->stores.find(store_key);
        if (it != this->stores.end())
        {
            TantivyIndexStorePtr store_ptr = it->second;
            size_t removed = 0;
            auto it2 = this->stores.begin();
            while (it2 != this->stores.end())
            {
                if (it2->second == store_ptr)
                {
                    auto [iter_skp_index_name, _] = this->splitKey(it2->first);
                    it2 = this->stores.erase(it2);
                    // Please note that `TantivyIndexStore` destruction can also trigger cache path remove.
                    TantivyIndexFilesManager::removeTantivyIndexInCache(data_part_relative_path, iter_skp_index_name);
                    removed += 1;
                }
                else
                {
                    ++it2;
                }
            }
            LOG_INFO(
                this->log,
                "[dropIndex] remove {} store(s), stores size decreased from {} to {}",
                removed,
                stores_size,
                this->stores.size());
        }
        TantivyIndexFilesManager::removeEmptyTableUUIDInCache(data_part_relative_path);
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            this->log,
            "[remove] Error happend when dropIndex, stores may not be cleaned correctly, data part relative path {}, exception is {}",
            storage->getRelativePath(),
            e.what());
    }
}


size_t TantivyIndexStoreFactory::remove(const String & data_part_relative_path)
{
    /// In cases when restart happened, this->stores may not contain the TantivyIndexStore for this part, we need to clear cache.
    /// But for stores existing in factory, an non-static removeTantivyIndexCache() will be called.
    size_t removed = 0;
    size_t stores_size = stores.size();

    try
    {
        std::lock_guard lock(stores_mutex);

        for (auto it = stores.begin(); it != stores.end();)
        {
            if (it->first.find(data_part_relative_path) != String::npos)
            {
                it = stores.erase(it);
                removed++;
            }
            else
            {
                ++it;
            }
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            log,
            "[remove] Error happend when remove stores cache key, path in key `{}`, exception is {}",
            data_part_relative_path,
            e.what());
    }

    if (removed != 0)
    {
        LOG_INFO(
            this->log,
            "[remove] path in key `{}`, stores size {} -> {}, removed: {}, current mutate_to_from size {}",
            data_part_relative_path,
            stores_size,
            stores.size(),
            removed,
            mutate_to_from.size());
    }
    return removed;
}


std::pair<String, String> TantivyIndexStoreFactory::splitKey(const String & key)
{
    size_t pos = key.find(':');
    if (pos != String::npos)
    {
        return std::make_pair(key.substr(0, pos), key.substr(pos + 1));
    }
    return std::make_pair(key, "");
}

String TantivyIndexStoreFactory::combineKey(const String & skp_index_name, const String & relative_path)
{
    return skp_index_name + ":" + relative_path;
}

String TantivyIndexStoreFactory::generateKey(const String & skp_index_name, const DataPartStoragePtr storage)
{
    String store_key = this->combineKey(skp_index_name, storage->getRelativePath());
    return store_key;
}

}
