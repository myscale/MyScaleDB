#pragma once
#include <filesystem>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Disks/IDisk.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace fs = std::filesystem;

namespace DB
{
class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class DataPartStorageOnDiskBase;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace VectorIndex
{

String cutMutVer(const String & part_name);
String cutPartitionID(const String & part_name);
String cutTableUUIDFromCacheKey(const String & cache_key);
String cutPartNameFromCacheKey(const String & cache_key);

struct CacheKey
{
    String table_path;
    String part_name_no_mutation; /// part_name doesn't include mutation version
    String vector_index_name;
    String column_name;

    bool operator==(const CacheKey & other) const
    {
        return (table_path == other.table_path) && (part_name_no_mutation == other.part_name_no_mutation)
            && (vector_index_name == other.vector_index_name) && (column_name == other.column_name);
    }

    String toString() const { return table_path + "/" + part_name_no_mutation + "/" + vector_index_name + "-" + column_name; }

    static String getTableUUIDFromCacheKey(const String & cache_key)
    {
        return cutTableUUIDFromCacheKey(cache_key);
    }

    static String getPartNameFromCacheKey(const String & cache_key)
    {
        return cutPartNameFromCacheKey(cache_key);
    }

    static String getPartitionIDFromCacheKey(const String & cache_key)
    {
        String part_name = cutPartNameFromCacheKey(cache_key);
        return cutPartitionID(cache_key);
    }

    String getTableUUID()  const
    {
        fs::path full_path(table_path);
        return full_path.stem().string();
    }

    String getPartName() const { return part_name_no_mutation; }

    String getPartitionID() const 
    {
        return cutPartitionID(part_name_no_mutation);
    }

    String getIndexName() const { return vector_index_name; }
};

struct SegmentId
{
    String data_part_path;
    String current_part_name;
    String owner_part_name;
    String vector_index_name;
    String column_name;
    UInt8 owner_part_id;
    DB::DiskPtr disk;   /// Use disk from data part storage

    SegmentId(
        DB::DataPartStoragePtr data_part_storage_,
        const String & data_part_path_,
        const String & current_part_name_,
        const String & owner_part_name_,
        const String & vector_index_name_,
        const String & column_name_,
        UInt8 owner_part_id_)
        : data_part_path(data_part_path_)
        , current_part_name(current_part_name_)
        , owner_part_name(owner_part_name_)
        , vector_index_name(vector_index_name_)
        , column_name(column_name_)
        , owner_part_id(owner_part_id_)
    {
        initDisk(data_part_storage_);
    }

    /// Used for vector index in decouple part
    SegmentId(
        DB::DataPartStoragePtr data_part_storage_,
        const String & current_part_name_,
        const String & owner_part_name_,
        const String & vector_index_name_,
        const String & column_name_,
        UInt8 owner_part_id_)
        : SegmentId(data_part_storage_, data_part_storage_->getFullPath(), current_part_name_, owner_part_name_, vector_index_name_, column_name_, owner_part_id_)
    {
    }

    /// Used for vector index in VPart
    SegmentId(
        DB::DataPartStoragePtr data_part_storage_,
        const String & current_part_name_,
        const String & vector_index_name_,
        const String & column_name_)
        : SegmentId(data_part_storage_, data_part_storage_->getFullPath(), current_part_name_, current_part_name_, vector_index_name_, column_name_, 0)
    {
    }

    /// Used for vector index build, where data_part_path is different from data_part_storage
    SegmentId(
        DB::DataPartStoragePtr data_part_storage_,
        const String & data_part_path_,
        const String & current_part_name_,
        const String & vector_index_name_,
        const String & column_name_)
        : SegmentId(data_part_storage_, data_part_path_, current_part_name_, current_part_name_, vector_index_name_, column_name_, 0)
    {
    }

    /// Get the disk from data part storage to read/write vector index files.
    void initDisk(DB::DataPartStoragePtr data_part_storage)
    {
        const DB::DataPartStorageOnDiskBase * part_storage = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(data_part_storage.get());
        if (part_storage == nullptr)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
        }

        disk = getVolumeFromPartStorage(*part_storage)->getDisk();
    }

    String getPathPrefix() const
    {
        /// normal vector index
        if (owner_part_name == current_part_name)
        {
            return data_part_path;
        }
        else
        {
            return data_part_path + "merged-" + DB::toString(owner_part_id) + "-" + owner_part_name + "-";
        }
    }

    String getIndexName() const { return vector_index_name; }

    String getFullPath() const { return getPathPrefix() + getIndexName() + "-"; }

    CacheKey getCacheKey() const
    {
        return CacheKey{
            getPartRelativePath(fs::path(data_part_path).parent_path()), cutMutVer(owner_part_name), vector_index_name, column_name};
    }

    String getVectorDescriptionFilePath() const { return getPathPrefix() + getVectorIndexDescriptionFileName(vector_index_name); }

    bool fromMergedParts() { return current_part_name != owner_part_name; }

    String getRowIdsMapFilePath() const { return getPathPrefix() + "row_ids_map" + VECTOR_INDEX_FILE_SUFFIX; }

    String getInvertedRowIdsMapFilePath() const { return data_part_path + "/" + "merged-inverted_row_ids_map" + VECTOR_INDEX_FILE_SUFFIX; }

    String getInvertedRowSourcesMapFilePath() const
    {
        return data_part_path + "/" + "merged-inverted_row_sources_map" + VECTOR_INDEX_FILE_SUFFIX;
    }

    static String getPartRelativePath(const String & table_path)
    {
        /// get table relative path from data_part_path,
        /// for example: table_path: /var/lib/clickhouse/store/0e3/0e3..../all_1_1_0 or store/0e3/0e3..../,
        /// return path: store/0e3/0e3....
        auto path = fs::path(table_path).parent_path();
        return fs::path(path.parent_path().parent_path().filename())
               / path.parent_path().filename()
               / path.filename();
    }

    UInt8 getOwnPartId() const { return owner_part_id; }

    DB::DiskPtr getDisk() { return disk; }
};
}
