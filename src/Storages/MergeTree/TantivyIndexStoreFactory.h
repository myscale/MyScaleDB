#pragma once

#include <array>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <tantivy_search.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/TantivyIndexStore.h>

namespace DB
{


/// key: index_name + relative path
using TantivyIndexStores = std::unordered_map<std::string, TantivyIndexStorePtr>;

/// A singleton for storing TantivyIndexStores
class TantivyIndexStoreFactory : private boost::noncopyable
{
public:
    static TantivyIndexStoreFactory & instance();

    /// @brief Use `get` will directly find key from this->stores.
    TantivyIndexStorePtr get(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief Use `getOrLoad` will get a `TantivyIndexStorePtr` to load Tantivy index, for TextSearch and HybridSearch.
    /// @param skp_index_name index name.
    /// @param storage The data part is used to perform TextSearch and HybridSearch, we use it's storage to deserialize FTS index files.
    TantivyIndexStorePtr getOrLoad(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief Use `getOrInit` will get a `TantivyIndexStorePtr` to build Tantivy index.
    /// @param skp_index_name index name.
    /// @param storage data part storage.
    /// @param storage_builder data part storage for building Tantivy index.
    TantivyIndexStorePtr
    getOrInit(const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder);

    /// @brief When a data part is removed by ClickHouse, we should remove it's related key in this->stores, otherwise TantivyIndexStore will not be destroyed.
    /// @param data_part_relative_path data part relative path needs removed.
    size_t remove(const String & data_part_relative_path);

    /// @brief When a mutate operation is performed by ClickHouse, we should record source_part and target_part in `mutate_to_from`.
    /// @param source_part_relative_path data part relative path before mutate operation.
    /// @param target_part_relative_path data part relative path after mutate operation.
    void mutate(const String & source_part_relative_path, const String & target_part_relative_path);

    /// @brief When a rename operation is performed by ClickHouse, we should update the keys in the tantivy index cache and this->stores simultaneously.
    /// @param data_part_relative_path_before_rename example: store/xx/xxx/tmp_mut_all_20930_20950_1_x
    /// @param storage represent the `data_part_storage` after the rename is completed.
    void renamePart(const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage);

    /// @brief drop index for a single part in tantivy index cache.
    /// @param skp_index_name tantivy index name.
    /// @param storage data part storage ptr.
    void dropIndex(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief Generate a key for this->stores.
    /// @param skp_index_name index name.
    /// @param storage related data part storage.
    String generateKey(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief iter current this->stores in factory, and execute callback func.
    void iter(const std::function<void(const std::string &, TantivyIndexStorePtr)> & callback)
    {
        std::lock_guard<std::mutex> lock(stores_mutex);
        for (const auto & [key, value] : stores)
        {
            callback(key, value);
        }
    }

    static String getPartRelativePath(const String & table_path)
    {
        /// get table relative path from data_part_path,
        /// for example: table_path: /var/lib/clickhouse/store/0e3/0e3..../all_1_1_0 or store/0e3/0e3..../,
        /// return path: store/0e3/0e3....
        auto path = fs::path(table_path).parent_path();
        return fs::path(path.parent_path().parent_path().filename()) / path.parent_path().filename() / path.filename();
    }


private:
    TantivyIndexStores stores;
    std::mutex stores_mutex;
    std::mutex mutate_mutex;
    Poco::Logger * log = &Poco::Logger::get("FTSIndexStoreFactory");

    std::unordered_map<String, String> mutate_to_from;

    std::pair<String, String> splitKey(const String & key);
    String combineKey(const String & skp_index_name, const String & relative_path);

    /// @brief update this->stores and move tantivy index cache directory.
    /// @param data_part_relative_path_before_rename data part before rename.
    /// @param data_part_relative_path_after_rename data part after rename.
    /// @param target_part_path_in_tantivy_cache target part path in tantivy index cache.
    void updateStoresForBuild(
        const String & data_part_relative_path_before_rename,
        const String & data_part_relative_path_after_rename,
        const fs::path & target_part_path_in_tantivy_cache);

    /// @brief update this->stores for mutation.
    /// @param data_part_relative_path_before_mutate data part before mutate.
    /// @param data_part_relative_path_after_rename data part after mutate.
    bool updateStoresForMutate(const String & data_part_relative_path_before_mutate, const String & data_part_relative_path_after_rename);
};
}
