#include <Storages/MergeTree/TantivyIndexStoreFactory.h>


namespace DB
{
TantivyIndexStorePtr FTSSafeCache::find_from_stores(const String & key)
{
    auto result = this->stores.get_optional(key);
    return result.has_value() ? result.value() : nullptr;
}

String FTSSafeCache::find_from_mutate(const String & after_mutate)
{
    auto before_mutate = this->mutate_to_from.get_optional(after_mutate);
    return before_mutate.has_value() ? before_mutate.value() : std::string();
}

void FTSSafeCache::emplace_into_stores(const String & key, TantivyIndexStorePtr new_store)
{
    this->stores.emplace(key, new_store);
}

void FTSSafeCache::emplace_into_mutate(const String & before_mutate, const String & after_mutate)
{
    this->mutate_to_from.emplace(after_mutate, before_mutate);
}

size_t FTSSafeCache::erase_store_keys(const std::vector<String> & keys)
{
    return this->stores.erase_keys(keys);
}

size_t FTSSafeCache::erase_store_key(const String & key)
{
    return this->stores.erase(key);
}

size_t FTSSafeCache::erase_mutate_key(const String & key)
{
    return this->mutate_to_from.erase(key);
}

size_t FTSSafeCache::mutate_size()
{
    return this->mutate_to_from.size();
}

size_t FTSSafeCache::stores_size()
{
    return this->stores.size();
}

std::pair<String, String> FTSSafeCache::splitFTSKey(const String & key)
{
    size_t pos = key.find(':');
    if (pos != String::npos)
    {
        return std::make_pair(key.substr(0, pos), key.substr(pos + 1));
    }
    return std::make_pair(key, "");
}

String FTSSafeCache::combineFTSKey(const String & skp_index_name, const String & relative_path)
{
    return skp_index_name + ":" + relative_path;
}


String concatenateStrings(const std::vector<String> & strings, const String & delimiter)
{
    std::ostringstream oss;
    std::copy(strings.begin(), strings.end(), std::ostream_iterator<std::string>(oss, delimiter.c_str()));

    std::string result = oss.str();
    if (!result.empty() && !delimiter.empty())
    {
        result.erase(result.length() - delimiter.length());
    }

    return result;
}


TantivyIndexStoreFactory & TantivyIndexStoreFactory::instance()
{
    static TantivyIndexStoreFactory instance;
    return instance;
}

TantivyIndexStorePtr TantivyIndexStoreFactory::getForBuidWithKey(const String & key)
{
    return this->cache.find_from_stores(key);
}

TantivyIndexStorePtr TantivyIndexStoreFactory::getForBuild(const String & skp_index_name, const DataPartStoragePtr storage)
{
    String store_key = this->cache.combineFTSKey(skp_index_name, storage->getRelativePath());
    return this->cache.find_from_stores(store_key);
}


TantivyIndexStorePtr TantivyIndexStoreFactory::getOrLoadForSearch(const String & skp_index_name, const DataPartStoragePtr storage)
{
    String store_key = this->cache.combineFTSKey(skp_index_name, storage->getRelativePath());

    // First check
    auto res = this->cache.find_from_stores(store_key);
    if (res != nullptr)
    {
        LOG_DEBUG(
            this->log,
            "[getOrLoadForSearch] store_key: {}, part_rel_path: {}, store_index_path: {}",
            store_key,
            storage->getRelativePath(),
            res->getTantivyIndexCacheDirectory());
        return res;
    }

    std::unique_lock<std::shared_mutex> lock(this->mutex_for_search);

    // Second check
    res = this->cache.find_from_stores(store_key);
    if (res != nullptr)
        return res;

    // Ensure only one thread can generate store object, avoid index files corrupt.
    TantivyIndexStorePtr new_store = std::make_shared<TantivyIndexStore>(skp_index_name, storage);
    this->cache.emplace_into_stores(store_key, new_store);
    LOG_INFO(
        this->log,
        "[getOrLoadForSearch] store_key: {}, ref count : {}, load from {}, `stores` size: {}, "
        "`mutate_to_from` size: {}",
        store_key,
        new_store.use_count(),
        storage->getRelativePath(),
        this->cache.stores_size(),
        this->cache.mutate_size());
    return new_store;
}


TantivyIndexStorePtr TantivyIndexStoreFactory::getOrInitForBuild(
    const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder)
{
    String store_key = this->cache.combineFTSKey(skp_index_name, storage->getRelativePath());

    // First check
    auto res = this->cache.find_from_stores(store_key);
    if (res != nullptr)
    {
        LOG_DEBUG(
            this->log,
            "[getOrInitForBuild] store_key: {}, part_rel_path: {}, store_index_path: {}",
            store_key,
            storage->getRelativePath(),
            res->getTantivyIndexCacheDirectory());
        return res;
    }

    std::unique_lock<std::shared_mutex> lock(this->mutex_for_build);

    // Second check
    res = this->cache.find_from_stores(store_key);
    if (res != nullptr)
        return res;

    // Ensure only one thread can generate store object, avoid index files corrupt.
    TantivyIndexStorePtr new_store = std::make_shared<TantivyIndexStore>(skp_index_name, storage, storage_builder);
    this->cache.emplace_into_stores(store_key, new_store);
    LOG_INFO(
        this->log,
        "[getOrInitForBuild] store_key: {}, part_rel_path: {}, `stores` size: {}, "
        "`mutate_to_from` size: {}",
        store_key,
        storage->getRelativePath(),
        this->cache.stores_size(),
        this->cache.mutate_size());
    return new_store;
}

void TantivyIndexStoreFactory::updateStoresForBuild(
    const String & data_part_relative_path_before_rename, // tmp_mut_all_20930_20950_1_x
    const String & data_part_relative_path_after_rename, // all_30010_30010_1_x
    const fs::path & target_part_path_in_tantivy_cache, // /xxx/tantivy_index_cache/store/6a5/xxx/all_47761_47823_1_47829
    const DB::Names & index_names)
{
    std::unordered_map<String, TantivyIndexStorePtr> stores_need_append;
    std::vector<String> old_keys_to_remove;

    for (size_t i = 0; i < index_names.size(); i++)
    {
        String index_name = "skp_idx_" + index_names[i];
        String store_key = this->cache.combineFTSKey(index_name, data_part_relative_path_before_rename);
        auto store_ptr = this->cache.find_from_stores(store_key);
        if (store_ptr)
        {
            store_ptr->updateCacheDataPartRelativeDirectory(target_part_path_in_tantivy_cache);
            // Update store_key in this->stores.
            String new_key = this->cache.combineFTSKey(index_name, data_part_relative_path_after_rename);
            stores_need_append[new_key] = store_ptr;
            old_keys_to_remove.push_back(store_key);
        }
    }

    // add new keys with updated stores.
    for (auto & [key, store] : stores_need_append)
    {
        this->cache.emplace_into_stores(key, store);
        LOG_INFO(
            this->log,
            "[updateStoresForBuild] insert updated store into `stores`, key: {}, store_inner_index_path(new): {}, store_size:{}",
            key,
            store->getTantivyIndexCacheDirectory(),
            this->cache.stores_size());
    }

    // erase old keys
    this->cache.erase_store_keys(old_keys_to_remove);
    LOG_INFO(
        this->log,
        "[updateStoresForBuild] remove old store keys: {}, store_size:{}",
        concatenateStrings(old_keys_to_remove, ", "),
        this->cache.stores_size());
}


bool TantivyIndexStoreFactory::updateStoresForMutate(
    const String & data_part_relative_path_before_mutate, // [all_20930_20950_1_20960] -> mutate -> [tmp_mut_all_20930_20950_1_x]
    const String & data_part_relative_path_after_rename, // [tmp_mut_all_20930_20950_1_x] -> rename -> [all_20930_20950_1_x]
    const DB::Names & index_names)
{
    std::vector<std::pair<String, String>> updated_keys;
    bool is_lwd = false;
    for (size_t i = 0; i < index_names.size(); i++)
    {
        String index_name = "skp_idx_" + index_names[i];
        String old_key = this->cache.combineFTSKey(index_name, data_part_relative_path_before_mutate);
        String new_key = this->cache.combineFTSKey(index_name, data_part_relative_path_after_rename);
        auto old_store_ptr = this->cache.find_from_stores(old_key);
        if (old_store_ptr)
        {
            is_lwd = true;
            this->cache.emplace_into_stores(new_key, old_store_ptr);
        }
        LOG_INFO(
            this->log,
            "[updateStoresForMutate] idx_name:{}, is_lwd: {}, old_key: {}, new_key: {}, store_inner_index_path(old): {}, mutate_size:{}",
            index_name,
            is_lwd,
            old_key,
            new_key,
            old_store_ptr == nullptr ? "" : old_store_ptr->getTantivyIndexCacheDirectory(),
            this->cache.mutate_size());
    }
    return is_lwd;
}

void TantivyIndexStoreFactory::mutate(const String & source_part_relative_path, const String & target_part_relative_path)
{
    this->cache.emplace_into_mutate(source_part_relative_path, target_part_relative_path);
    LOG_INFO(
        this->log,
        "[mutate] from `{} to `{}`, `mutate_to_from` size: {}",
        source_part_relative_path,
        target_part_relative_path,
        this->cache.mutate_size());
}


void TantivyIndexStoreFactory::renamePart(
    const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage, const DB::Names & index_names)
{
    auto context = Context::getGlobalContextInstance();
    auto data_part_relative_path_after_rename = storage->getRelativePath();

    String data_part_relative_path_before_mutate = this->cache.find_from_mutate(data_part_relative_path_before_rename);

    if (!data_part_relative_path_before_mutate.empty())
    {
        // We were able to find the mutate record from `mutate_to_from` for that data part.
        // Indicates that LWD and materialize index may have occurred.
        // To make the `TantivyIndexStore` reusable, append a new key to the `TantivyIndexStore` and point to the old `TantivyIndexStore` shared pointer.
        bool is_lwd = updateStoresForMutate(data_part_relative_path_before_mutate, data_part_relative_path_after_rename, index_names);

        if (!is_lwd)
        {
            // After rename operation is complete, the `TantivyIndexStore` should update the tantivy index cache directory (starts with `tmp`)
            auto target_part_path_in_tantivy_cache = fs::path(context->getTantivyIndexCachePath()) / storage->getRelativePath();

            // The `data_part_relative_path_before_mutate` record before mutate occurred cannot be found in the stores keys,
            // indicates that the mutate operation may execute `MATERIALIZE INDEX`.
            updateStoresForBuild(
                data_part_relative_path_before_rename,
                data_part_relative_path_after_rename,
                target_part_path_in_tantivy_cache,
                index_names);
        }
        // Remove mutate operation from `mutate_to_from` record.
        this->cache.erase_mutate_key(data_part_relative_path_before_rename);

        LOG_INFO(
            this->log,
            "[renamePart] after mutate(lwd:{}), before_rename {}, after_rename {}, `stores` size: {}, "
            "`mutate_to_from` size: {}",
            is_lwd,
            is_lwd ? data_part_relative_path_before_mutate : data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            this->cache.stores_size(),
            this->cache.mutate_size());
    }
    else
    {
        // After rename operation is complete, the `TantivyIndexStore` should update the tantivy index cache directory (starts with `tmp`)
        auto target_part_path_in_tantivy_cache = fs::path(context->getTantivyIndexCachePath()) / storage->getRelativePath();

        // We can't find the mutate record from `mutate_to_from` for that data part.
        // Indicates that `tmp_insert`, `tmp_merge` and `tmp_clone` may have occurred.
        // These operations will generate a new `TantivyIndexStore`, we need to update the tantivy index cache directory in this tmp `TantivyIndexStore`.
        updateStoresForBuild(
            data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_path_in_tantivy_cache, index_names);

        LOG_INFO(
            this->log,
            "[renamePart] after insert/merge/xxx, before_rename {}, after_rename {}, `stores` size: {}, "
            "`mutate_to_from` size: {}",
            data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            this->cache.stores_size(),
            this->cache.mutate_size());
    }
}

void TantivyIndexStoreFactory::dropIndex(const String & skp_index_name, const DataPartStoragePtr storage)
{
    try
    {
        // storage->getRelativePath(): store/069/069cd2be-0a8f-4091-ad82-38015b19bdef/all_1_1_0
        fs::path data_part_relative_path = fs::path(storage->getRelativePath());
        String store_key = this->cache.combineFTSKey(skp_index_name, data_part_relative_path);

        this->cache.erase_store_key(store_key);
        TantivyIndexFilesManager::removeTantivyIndexInCache(data_part_relative_path, skp_index_name);
        TantivyIndexFilesManager::removeEmptyTableUUIDInCache(data_part_relative_path);
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
}


size_t TantivyIndexStoreFactory::remove(const String & data_part_relative_path, const DB::Names & index_names)
{
    std::vector<String> removed_keys;
    for (size_t i = 0; i < index_names.size(); i++)
    {
        removed_keys.push_back(this->cache.combineFTSKey("skp_idx_" + index_names[i], data_part_relative_path));
    }

    size_t erased_size = this->cache.erase_store_keys(removed_keys);

    LOG_INFO(
        this->log,
        "[remove] part relative path `{}`, `stores` size: {}, `mutate_to_from` size: {}",
        data_part_relative_path,
        this->cache.stores_size(),
        this->cache.mutate_size());
    return erased_size;
}


String TantivyIndexStoreFactory::generateKey(const String & skp_index_name, const DataPartStoragePtr storage)
{
    String store_key = this->cache.combineFTSKey(skp_index_name, storage->getRelativePath());
    return store_key;
}

}
