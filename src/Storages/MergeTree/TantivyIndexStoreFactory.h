#pragma once

#include <array>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <tantivy_search.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/TantivyIndexStore.h>

namespace DB
{

template <typename Key, typename Value>
class safe_unordered_map
{
private:
    std::unordered_map<Key, Value> map_;
    mutable std::shared_mutex mutex_;

public:
    safe_unordered_map() = default;
    ~safe_unordered_map() = default;

    safe_unordered_map(const safe_unordered_map &) = delete;
    safe_unordered_map & operator=(const safe_unordered_map &) = delete;

    std::optional<Value> get_optional(const Key & key)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end())
            return it->second;
        else
            return std::nullopt;
    }

    // access
    Value & at(const Key & key)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.at(key);
    }

    const Value & at(const Key & key) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.at(key);
    }

    Value & operator[](const Key & key)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return map_[key];
    }

    typename std::unordered_map<Key, Value>::iterator find(const Key & key)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.find(key);
    }

    typename std::unordered_map<Key, Value>::const_iterator find(const Key & key) const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.find(key);
    }
    // insert, emplace
    std::pair<typename std::unordered_map<Key, Value>::iterator, bool> insert(const std::pair<Key, Value> & value)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return map_.insert(value);
    }

    template <typename... Args>
    std::pair<typename std::unordered_map<Key, Value>::iterator, bool> emplace(Args &&... args)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return map_.emplace(std::forward<Args>(args)...);
    }

    size_t erase(const Key & key)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return map_.erase(key);
    }

    size_t erase_keys(const std::vector<Key> & keys)
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        size_t count = 0;
        for (const auto & key : keys)
        {
            count += map_.erase(key);
        }
        return count;
    }

    // metrics
    bool empty() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.empty();
    }

    size_t size() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.size();
    }

    // iterator related
    typename std::unordered_map<Key, Value>::iterator begin()
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.begin();
    }

    typename std::unordered_map<Key, Value>::iterator end()
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.end();
    }

    typename std::unordered_map<Key, Value>::const_iterator begin() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.begin();
    }

    typename std::unordered_map<Key, Value>::const_iterator end() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.end();
    }

    std::vector<Key> keys() const
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<Key> keys;
        keys.reserve(map_.size());
        for (const auto & pair : map_)
        {
            keys.push_back(pair.first);
        }
        return keys;
    }
};


/// key: index_name + relative path
using TantivyIndexStores = safe_unordered_map<String, TantivyIndexStorePtr>;
using IndexNames = safe_unordered_map<String, safe_unordered_map<String, size_t>>;

class FTSSafeCache
{
public:
    TantivyIndexStorePtr find_from_stores(const String & key);
    String find_from_mutate(const String & after_mutate);

    void emplace_into_stores(const String & key, TantivyIndexStorePtr new_store);
    void emplace_into_mutate(const String & before_mutate, const String & after_mutate);

    size_t erase_store_keys(const std::vector<String> & keys);
    size_t erase_store_key(const String & key);
    size_t erase_mutate_key(const String & key);

    size_t mutate_size();
    size_t stores_size();

    std::pair<String, String> splitFTSKey(const String & key);
    String combineFTSKey(const String & skp_index_name, const String & relative_path);

private:
    // size_t default zero.
    safe_unordered_map<String, String> mutate_to_from;
    TantivyIndexStores stores;
};


/// A singleton for storing TantivyIndexStores
class TantivyIndexStoreFactory : private boost::noncopyable
{
public:
    static TantivyIndexStoreFactory & instance();

    /// @brief Use `get` will directly find key from this->stores.
    TantivyIndexStorePtr getForBuild(const String & skp_index_name, const DataPartStoragePtr storage);
    TantivyIndexStorePtr getForBuidWithKey(const String & key);

    /// @brief Use `getOrLoad` will get a `TantivyIndexStorePtr` to load Tantivy index, for TextSearch and HybridSearch.
    /// @param skp_index_name index name.
    /// @param storage The data part is used to perform TextSearch and HybridSearch, we use it's storage to deserialize FTS index files.
    TantivyIndexStorePtr getOrLoadForSearch(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief Use `getOrInit` will get a `TantivyIndexStorePtr` to build Tantivy index.
    /// @param skp_index_name index name.
    /// @param storage data part storage.
    /// @param storage_builder data part storage for building Tantivy index.
    TantivyIndexStorePtr
    getOrInitForBuild(const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder);

    /// @brief When a data part is removed by ClickHouse, we should remove it's related key in this->stores, otherwise TantivyIndexStore will not be destroyed.
    /// @param data_part_relative_path data part relative path needs removed.
    size_t remove(const String & data_part_relative_path, const DB::Names & index_names);

    /// @brief When a mutate operation is performed by ClickHouse, we should record source_part and target_part in `mutate_to_from`.
    /// @param source_part_relative_path data part relative path before mutate operation.
    /// @param target_part_relative_path data part relative path after mutate operation.
    void mutate(const String & source_part_relative_path, const String & target_part_relative_path);

    /// @brief When a rename operation is performed by ClickHouse, we should update the keys in the tantivy index cache and this->stores simultaneously.
    /// @param data_part_relative_path_before_rename example: store/xx/xxx/tmp_mut_all_20930_20950_1_x
    /// @param storage represent the `data_part_storage` after the rename is completed.
    void renamePart(const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage, const DB::Names & index_names);

    /// @brief drop index for a single part in tantivy index cache.
    /// @param skp_index_name tantivy index name.
    /// @param storage data part storage ptr.
    void dropIndex(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief Generate a key for this->stores.
    /// @param skp_index_name index name.
    /// @param storage related data part storage.
    String generateKey(const String & skp_index_name, const DataPartStoragePtr storage);

    static String getPartRelativePath(const String & table_path)
    {
        /// get table relative path from data_part_path,
        /// for example: table_path: /var/lib/clickhouse/store/0e3/0e3..../all_1_1_0 or store/0e3/0e3..../,
        /// return path: store/0e3/0e3....
        auto path = fs::path(table_path).parent_path();
        return fs::path(path.parent_path().parent_path().filename()) / path.parent_path().filename() / path.filename();
    }


private:
    LoggerPtr log = getLogger("FTSIndexStoreFactory");
    FTSSafeCache cache;
    mutable std::shared_mutex mutex_for_search;
    mutable std::shared_mutex mutex_for_build;

    /// @brief update this->stores and move tantivy index cache directory.
    /// @param data_part_relative_path_before_rename data part before rename.
    /// @param data_part_relative_path_after_rename data part after rename.
    /// @param target_part_path_in_tantivy_cache target part path in tantivy index cache.
    void updateStoresForBuild(
        const String & data_part_relative_path_before_rename,
        const String & data_part_relative_path_after_rename,
        const fs::path & target_part_path_in_tantivy_cache,
        const DB::Names & index_names);

    /// @brief update this->stores for mutation.
    /// @param data_part_relative_path_before_mutate data part before mutate.
    /// @param data_part_relative_path_after_rename data part after mutate.
    bool updateStoresForMutate(
        const String & data_part_relative_path_before_mutate,
        const String & data_part_relative_path_after_rename,
        const DB::Names & index_names);
};
}
