#pragma once
#include <filesystem>
#include <base/types.h>
#include <VectorIndex/VectorIndexCommon.h>

#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace VectorIndex
{

String cutMutVer(const String & part_name);

struct CacheKey
{
    String table_path;
    String part_name_no_mutation; /// part_name doesn't include mutation version
    String vector_index_name;
    String column_name;

    bool operator==(const CacheKey& other) const {
        return (table_path == other.table_path)
        && (part_name_no_mutation == other.part_name_no_mutation)
        && (vector_index_name == other.vector_index_name)
        && (column_name == other.column_name);
    }

    String toString() const
    {
        return table_path + "/" + part_name_no_mutation + "/" + vector_index_name + "_" + column_name;
    }
};

struct SegmentId
{
    String data_part_path;
    String current_part_name;
    String owner_part_name;
    String vector_index_name;
    String column_name;
    UInt8 owner_part_id;

    SegmentId(const String& data_part_path_, const String& current_part_name_, const String& owner_part_name_,
              const String& vector_index_name_, const String& column_name_, UInt8 owner_part_id_): 
              data_part_path(data_part_path_), current_part_name(current_part_name_), owner_part_name(owner_part_name_),
              vector_index_name(vector_index_name_), column_name(column_name_), owner_part_id(owner_part_id_) {}


    SegmentId(const String& data_part_path_, const String& current_part_name_, 
              const String& vector_index_name_, const String& column_name_, UInt8 owner_part_id_):
              data_part_path(data_part_path_), current_part_name(current_part_name_), owner_part_name(current_part_name_),
              vector_index_name(vector_index_name_), column_name(column_name_), owner_part_id(owner_part_id_) {}

    String getPathSuffix() const
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

    String getIndexNameWithColumn() const
    {
        return vector_index_name + "_" + column_name;
    }

    String getFullPath() const
    {
        return getPathSuffix() + getIndexNameWithColumn();
    }

    CacheKey getCacheKey() const
    {
        fs::path full_path(data_part_path);
        /// use parent data path, need to call parent_path() twice, 
        /// according to https://en.cppreference.com/w/cpp/filesystem/path/parent_path
        return CacheKey{full_path.parent_path().parent_path().string(), cutMutVer(owner_part_name), vector_index_name, column_name};
    }

    String getVectorReadyFilePath() const
    {
        return getPathSuffix() + VECTOR_INDEX_READY;
    }

    String getBitMapFilePath() const
    {
        return getPathSuffix() + VECTOR_INDEX_BITMAP;
    }

    bool fromMergedParts()
    {
        return current_part_name != owner_part_name;
    }

    String getRowIdsMapFilePath() const
    {
        return getPathSuffix() + "row_ids_map" + VECTOR_INDEX_FILE_SUFFIX;
    }

    String getInvertedRowIdsMapFilePath() const
    {
        return data_part_path + "/" + "merged-inverted_row_ids_map" + VECTOR_INDEX_FILE_SUFFIX;
    }

    String getInvertedRowSourcesMapFilePath() const
    {
        return data_part_path + "/" + "merged-inverted_row_sources_map" + VECTOR_INDEX_FILE_SUFFIX;
    }    

    UInt8 getOwnPartId() const
    {
        return owner_part_id;
    }
};
}
