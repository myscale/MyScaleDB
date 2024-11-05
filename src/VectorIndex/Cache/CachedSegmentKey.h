#pragma once

#include <boost/algorithm/string.hpp>

#include <base/types.h>

#include <VectorIndex/Utils/VIUtils.h>

namespace VectorIndex
{
struct CachedSegmentKey
{
    DB::String table_path;                      /// current cached segment table relative path
    DB::String cur_part_name;                   /// current cached segment part name
    DB::String part_name_no_mutation;           /// cached segment belongs to part without mutation
    DB::String vector_index_name;               /// cached segment vector index name
    DB::String column_name;                     /// cached segment column name

    DB::String toString() const
    {
        return table_path + "/" + part_name_no_mutation + "/" + vector_index_name + "-" + column_name;
    }

    bool operator==(const CachedSegmentKey & other) const
    {
        /// do not compare cur_part_name, because simple segment can be used by decouple segment
        return (table_path == other.table_path)
            && (part_name_no_mutation == other.part_name_no_mutation) && (vector_index_name == other.vector_index_name)
            && (column_name == other.column_name);
    }
    DB::String getTableUUID() const
    {
        fs::path full_path(table_path);
        return full_path.stem().string();
    }

    DB::String getPartName() const { return part_name_no_mutation; }

    DB::String getCurPartName() const { return cur_part_name; }

    DB::String getPartitionID() const { return cutPartitionID(part_name_no_mutation); }

    DB::String getIndexName() const { return vector_index_name; }
};
} // namespace VectorIndex
