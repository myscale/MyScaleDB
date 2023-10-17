#pragma once
#include <filesystem>
#include <VectorIndex/VectorIndexCommon.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace fs = std::filesystem;

namespace DB
{
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
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
};

struct SegmentId
{
    DB::VolumePtr volume;
    String data_part_path;
    String current_part_name;
    String owner_part_name;
    String vector_index_name;
    String column_name;
    UInt8 owner_part_id;

    SegmentId(
        DB::VolumePtr volume_,
        const String & data_part_path_,
        const String & current_part_name_,
        const String & owner_part_name_,
        const String & vector_index_name_,
        const String & column_name_,
        UInt8 owner_part_id_)
        : volume(volume_)
        , data_part_path(data_part_path_)
        , current_part_name(current_part_name_)
        , owner_part_name(owner_part_name_)
        , vector_index_name(vector_index_name_)
        , column_name(column_name_)
        , owner_part_id(owner_part_id_)
    {
    }


    SegmentId(
        DB::VolumePtr volume_,
        const String & data_part_path_,
        const String & current_part_name_,
        const String & vector_index_name_,
        const String & column_name_)
        : volume(volume_)
        , data_part_path(data_part_path_)
        , current_part_name(current_part_name_)
        , owner_part_name(current_part_name_)
        , vector_index_name(vector_index_name_)
        , column_name(column_name_)
        , owner_part_id(0)
    {
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

    String getIndexNameWithColumn() const { return vector_index_name + "-" + column_name; }

    String getFullPath() const { return getPathPrefix() + getIndexNameWithColumn() + "-"; }

    CacheKey getCacheKey() const
    {
        fs::path full_path(data_part_path);
        /// use parent data path, need to call parent_path() twice,
        /// according to https://en.cppreference.com/w/cpp/filesystem/path/parent_path
        return CacheKey{full_path.parent_path().parent_path().string(), cutMutVer(owner_part_name), vector_index_name, column_name};
    }

    String getVectorReadyFilePath() const { return getPathPrefix() + VECTOR_INDEX_READY + VECTOR_INDEX_FILE_SUFFIX; }

    String getVectorDescriptionFilePath() const { return getPathPrefix() + VECTOR_INDEX_DESCRIPTION + VECTOR_INDEX_FILE_SUFFIX; }

    String getBitMapFilePath() const { return getPathPrefix() + VECTOR_INDEX_BITMAP + VECTOR_INDEX_FILE_SUFFIX; }

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
};
}
