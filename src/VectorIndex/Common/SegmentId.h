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
#include <filesystem>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <base/types.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Cache/VectorIndexCacheObject.h>
#include <VectorIndex/Common/VectorIndexCommon.h>

namespace fs = std::filesystem;

namespace VectorIndex
{
String cutMutVer(const String & part_name);
}

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

struct VectorIndexWithMeta;
using VectorIndexWithMetaPtr = std::shared_ptr<VectorIndexWithMeta>;

struct SegmentId
{
    String data_part_path;
    String current_part_name;
    String owner_part_name;
    String vector_index_name;
    String column_name;
    UInt8 owner_part_id;
    DB::DiskPtr disk; /// Use disk from data part storage

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
        : SegmentId(
            data_part_storage_,
            data_part_storage_->getFullPath(),
            current_part_name_,
            owner_part_name_,
            vector_index_name_,
            column_name_,
            owner_part_id_)
    {
    }

    /// Used for vector index in VPart
    SegmentId(
        DB::DataPartStoragePtr data_part_storage_,
        const String & current_part_name_,
        const String & vector_index_name_,
        const String & column_name_)
        : SegmentId(
            data_part_storage_,
            data_part_storage_->getFullPath(),
            current_part_name_,
            current_part_name_,
            vector_index_name_,
            column_name_,
            0)
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
    void initDisk(DB::DataPartStoragePtr data_part_storage);

    String getPathPrefix() const;

    String getIndexName() const { return vector_index_name; }

    String getFullPath() const { return getPathPrefix() + getIndexName() + "-"; }

    VectorIndex::CacheKey getCacheKey() const
    {
        return VectorIndex::CacheKey{
            getPartRelativePath(fs::path(data_part_path).parent_path()),
            VectorIndex::cutMutVer(current_part_name),
            VectorIndex::cutMutVer(owner_part_name),
            vector_index_name,
            column_name};
    }

    VectorIndex::CacheKey getOriginalCacheKey() const
    {
        return VectorIndex::CacheKey{
            getPartRelativePath(fs::path(data_part_path).parent_path()),
            VectorIndex::cutMutVer(owner_part_name),
            VectorIndex::cutMutVer(owner_part_name),
            vector_index_name,
            column_name};
    }

    String getVectorDescriptionFilePath() const { return getPathPrefix() + VectorIndex::getVectorIndexDescriptionFileName(vector_index_name); }

    bool fromMergedParts() const { return current_part_name != owner_part_name; }

    String getRowIdsMapFilePath() const { return getPathPrefix() + getRowIdsMapFileSuffix(); }

    static String getRowIdsMapFileSuffix() { return String("row_ids_map") + VECTOR_INDEX_FILE_SUFFIX; }

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
        return fs::path(path.parent_path().parent_path().filename()) / path.parent_path().filename() / path.filename();
    }

    std::tuple<std::shared_ptr<RowIds>, std::shared_ptr<RowIds>, std::shared_ptr<RowSource>> getMergedMaps();

    UInt8 getOwnPartId() const { return owner_part_id; }

    DB::DiskPtr getDisk() { return disk; }
};
}
