#pragma once

#include <array>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
// #include <sparse_index.h>
#include <typeinfo>
#include <Disks/DiskLocal.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/SkipIndex/Cache/IndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Cache/MutateRecordCache.h>
#include <Storages/MergeTree/SkipIndex/Cache/SparseIndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Cache/TantivyIndexStoreCache.h>
#include <Storages/MergeTree/SkipIndex/Store/IndexStore.h>


namespace DB
{
/// A singleton for storing SparseIndexStores
template <typename CacheType, typename StoreType, typename StoreTypePtr>
class IndexStoreFactory
{
public:
    IndexStoreFactory(const String & log_name, std::shared_ptr<CacheType> cache, MutateRecordCachePtr mutate_to_from);

    virtual ~IndexStoreFactory() = default;

    /// @brief Use `getForBuild` won't have ability to build index.
    StoreTypePtr get(const StoreKey & store_key);

    /// @brief Use `getForBuild` won't have ability to build index.
    StoreTypePtr getForBuild(const String & skp_index_name, const DataPartStoragePtr storage);

    StoreTypePtr getOrLoadForSearch(const String & skp_index_name, const DataPartStoragePtr storage);

    /// @brief Use `getOrInit` will get a `StoreTypePtr` to build Skip index.
    /// @param skp_index_name index name.
    /// @param storage data part storage.
    /// @param storage_builder data part storage for building Sparse index.
    StoreTypePtr
    getOrInitForBuild(const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder);

    /// @brief When a data part is removed by ClickHouse, we should remove it's related key in `cahe`, otherwise StoreType won't be destroyed.
    /// @param data_part_relative_path data part relative path needs removed.
    size_t remove(const String & data_part_relative_path, const DB::Names & index_names);

    /// @brief When a mutate operation is performed by ClickHouse, we should record source_part and target_part in `mutate_record`.
    /// @param source_part_relative_path data part relative path before mutate operation.
    /// @param target_part_relative_path data part relative path after mutate operation.
    void mutate(const String & source_part_relative_path, const String & target_part_relative_path);

    /// @brief When a rename operation is performed by ClickHouse, we should update the keys in `cache` and `index files cache directory` simultaneously.
    /// @param data_part_relative_path_before_rename example: store/xx/xxx/tmp_mut_all_20930_20950_1_x
    /// @param storage represent the `data_part_storage` after the rename is completed.
    /// @param index_names related skip index names.
    void renamePart(const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage, const DB::Names & index_names);

    /// @brief drop index for a single part, with given skip index name.
    /// @param skp_index_name which skip index needs dropped.
    /// @param storage data part storage ptr.
    void dropIndex(const String & skp_index_name, const DataPartStoragePtr storage);


    /// @brief The Concept of "Idle".
    /// "Idle" refers to a situation where the `StorePtr` for a data part in the cache has a `ref_count` of 1.
    /// When `partA` and `partB` merge into `partC`, both `partA` and `partB` enter an Idle state.
    ///
    /// @param storage The storage for the Idle data part, which transitions from `Active` to `Outdated` status.
    /// @param index_names The associated index names.
    void freeIdleStoreReader(const DataPartStoragePtr storage, const DB::Names & index_names);

    static String getPartRelativePath(const String & table_path)
    {
        /// get table relative path from data_part_path,
        /// for example: table_path: /var/lib/clickhouse/store/0e3/0e3..../all_1_1_0 or store/0e3/0e3..../,
        /// return path: store/0e3/0e3....
        auto path = fs::path(table_path).parent_path();
        return fs::path(path.parent_path().parent_path().filename()) / path.parent_path().filename() / path.filename();
    }


    /// @brief needs implement by derived class
    // virtual IndexStorePtr innerCreateIndexStore(const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder=nullptr) = 0;

    /// @brief needs implement by derived class
    virtual const String innerCreateNewPathInCache(const String & target_part_relative_path)
    {
        auto context = Context::getGlobalContextInstance();

        if (typeid(CacheType) == typeid(TantivyIndexStoreCache))
        {
            return fs::path(context->getTantivyIndexCachePath()) / target_part_relative_path;
        }
        else if (typeid(CacheType) == typeid(SparseIndexStoreCache))
        {
            return fs::path(context->getSparseIndexCachePath()) / target_part_relative_path;
        }
        else
        {
            // ERROR! Code should not run here.
        }
        // ERROR! Code should not run here.
        return "";
    }

private:
    Poco::Logger * log = nullptr;

    std::shared_ptr<CacheType> cache;
    MutateRecordCachePtr mutate_to_from;

    mutable std::shared_mutex mutex_for_search;
    mutable std::shared_mutex mutex_for_build;

    const String SKP_PREFIX = "skp_idx_";

    /// @brief update this->stores and move sparse index cache directory.
    /// @param data_part_relative_path_before_rename data part before rename.
    /// @param data_part_relative_path_after_rename data part after rename.
    /// @param target_part_full_path_in_cache target part path in sparse index cache.
    void updateStoresForBuild(
        const String & data_part_relative_path_before_rename, // tmp_mut_all_20930_20950_1_x
        const String & data_part_relative_path_after_rename, // all_30010_30010_1_x
        const String & target_part_full_path_in_cache, // /xxx/tantivy_index_cache/store/6a5/xxx/all_47761_47823_1_47829
        const DB::Names & index_names);

    /// @brief update this->stores for mutation.
    /// @param data_part_relative_path_before_mutate data part before mutate.
    /// @param data_part_relative_path_after_rename data part after mutate.
    bool updateStoresForMutate(
        const String & data_part_relative_path_before_mutate, // [all_20930_20950_1_20960] -> mutate -> [tmp_mut_all_20930_20950_1_x]
        const String & data_part_relative_path_after_rename, // [tmp_mut_all_20930_20950_1_x] -> rename -> [all_20930_20950_1_x]
        const DB::Names & index_names);
};
}
