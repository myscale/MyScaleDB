/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <filesystem>

#include "../../../base/base/types.h"

#include "../Common/VICommon.h"

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric LoadedVectorIndexMemorySize;
}

namespace VectorIndex
{

String cutMutVer(const String & part_name);
String cutPartitionID(const String & part_name);
String cutTableUUIDFromCacheKey(const String & cache_key);
String cutPartNameFromCacheKey(const String & cache_key);

struct VIWithMeta
{
    using VIWithMetaPtr = std::shared_ptr<VIWithMeta>;
    VIWithMeta() = delete;

    VIWithMeta(
        VIVariantPtr & index_,
        uint64_t total_vec_,
        VIBitmapPtr delete_bitmap_,
        VIParameter des_,
        std::shared_ptr<std::vector<UInt64>> row_ids_map_,
        std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map_,
        std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map_,
        uint32_t own_id_,
        int disk_mode_,
        bool fallback_to_flat_,
        String & vector_index_cache_prefix_)
        : index(index_)
        , total_vec(total_vec_)
        , delete_bitmap(delete_bitmap_)
        , des(des_)
        , row_ids_map(row_ids_map_)
        , inverted_row_ids_map(inverted_row_ids_map_)
        , inverted_row_sources_map(inverted_row_sources_map_)
        , own_id(own_id_)
        , disk_mode(disk_mode_)
        , fallback_to_flat(fallback_to_flat_)
        , vector_index_cache_prefix(vector_index_cache_prefix_)
        , index_load_memory_size_metric(std::visit(
              [&](auto && index_ptr)
              {
                   return CurrentMetrics::Increment{
                      CurrentMetrics::LoadedVectorIndexMemorySize, static_cast<Int64>(index_ptr->getResourceUsage().memory_usage_bytes)};
              },
              index))
    {
    }

    VIVariantPtr index;
    size_t total_vec;

private:
    VIBitmapPtr delete_bitmap;
    mutable std::mutex mutex_of_delete_bitmap;
    mutable std::mutex mutex_of_row_id_maps;

public:
    VIParameter des;
    std::shared_ptr<std::vector<UInt64>> row_ids_map;
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map;
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map;
    UInt32 own_id;
    int disk_mode;
    bool fallback_to_flat;
    String vector_index_cache_prefix;

    CurrentMetrics::Increment index_load_memory_size_metric;

    void setDeleteBitmap(VIBitmapPtr delete_bitmap_) { std::atomic_store(&delete_bitmap, std::move(delete_bitmap_)); }

    VIBitmapPtr getDeleteBitmap() const { return std::atomic_load(&delete_bitmap); }

    VIWithMetaPtr clone()
    {
        return std::make_shared<VIWithMeta>(
            index,
            total_vec,
            delete_bitmap,
            des,
            row_ids_map,
            inverted_row_ids_map,
            inverted_row_sources_map,
            own_id,
            disk_mode,
            fallback_to_flat,
            vector_index_cache_prefix);
    }
};

struct CacheKey
{
    String table_path;
    String cur_part_name;
    String part_name_no_mutation; /// part_name doesn't include mutation version
    String vector_index_name;
    String column_name;

    bool operator==(const CacheKey & other) const
    {
        return (table_path == other.table_path) && (cur_part_name == other.cur_part_name)
            && (part_name_no_mutation == other.part_name_no_mutation) && (vector_index_name == other.vector_index_name)
            && (column_name == other.column_name);
    }

    String toString() const
    {
        return table_path + "/" + cur_part_name + "-" + part_name_no_mutation + "/" + vector_index_name + "-" + column_name;
    }

    static String getTableUUIDFromCacheKey(const String & cache_key) { return cutTableUUIDFromCacheKey(cache_key); }

    static String getPartNameFromCacheKey(const String & cache_key) { return cutPartNameFromCacheKey(cache_key); }

    static String getPartitionIDFromCacheKey(const String & cache_key)
    {
        String part_name = cutPartNameFromCacheKey(cache_key);
        return cutPartitionID(cache_key);
    }

    String getTableUUID() const
    {
        fs::path full_path(table_path);
        return full_path.stem().string();
    }

    String getPartName() const { return part_name_no_mutation; }

    String getCurPartName() const { return cur_part_name; }

    String getPartitionID() const { return cutPartitionID(part_name_no_mutation); }

    String getIndexName() const { return vector_index_name; }
};
}
