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

#include <Common/logger_useful.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Storages/VIDescriptions.h>

#pragma GCC diagnostic ignored "-Wunused-function"

namespace Search
{
enum class DataType;
}

namespace DB
{
struct MergedPartNameAndId;
struct VISegmentMetadata;
class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

/// used to move vector indices files of one old data part
/// to new data part's path, and move vector index checksums to avoid recalculate
DB::MergeTreeDataPartChecksums moveVectorIndexFiles(
    const bool decouple,
    const String & part_id,
    const String & part_name,
    const String & vec_index_name,
    DB::MergeTreeDataPartPtr old_data_part,
    DB::MergeTreeMutableDataPartPtr new_data_part);

/// Calculate checksum for the vector index file under the specified path
MergeTreeDataPartChecksums calculateVectorIndexChecksums(
    const DataPartStoragePtr & part_storage,
    const String & vector_index_relative_path,
    std::shared_ptr<MergeTreeDataPartChecksums> existing_checksums = nullptr);

}

namespace VectorIndex
{

struct SegmentId;
struct VIWithMeta;

std::vector<SegmentId> getAllSegmentIds(const DB::MergeTreeDataPartPtr & data_part, const String & index_name);

std::vector<SegmentId>
getAllSegmentIds(const DB::MergeTreeDataPartPtr & data_part, const DB::VISegmentMetadata & vector_index_segment_metadata);

std::vector<SegmentId>
getAllSegmentIds(const DB::IMergeTreeDataPart & data_part, const DB::VISegmentMetadata & vector_index_segment_metadata);

std::vector<SegmentId> getAllSegmentIds(
    const DB::DataPartStoragePtr part_storage,
    const String & current_part_name,
    const DB::VISegmentMetadata & vector_index_segment_metadata);

DB::NameSet
getVectorIndexFileNamesInChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool need_checksums_file = false);

DB::MergeTreeDataPartChecksums
getVectorIndexChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name);

void removeVectorIndexFilesFromFileLists(const DB::DataPartStoragePtr & part_storage, const DB::Names & index_files_list);

void removeIndexFile(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool include_checksum_file = true);

DB::NameSet getAllValidVectorIndexFileNames(const DB::IMergeTreeDataPart & data_part);

void removeIncompleteMovedVectorIndexFiles(const DB::IMergeTreeDataPart & data_part);

void dumpCheckSums(const DB::DataPartStoragePtr & part_storage, const String & index_name, const DB::MergeTreeDataPartChecksums & index_checksum);

bool checkConsistencyForVectorIndex(const DB::DataPartStoragePtr & part_storage, const DB::MergeTreeDataPartChecksums & index_checksums);

const std::vector<UInt64> readDeleteBitmapAccordingSegmentId(const SegmentId segment_id);

void convertBitmap(
    const SegmentId & segment_id,
    const std::vector<UInt64> & part_deleted_row_ids,
    VIBitmapPtr index_deleted_row_ids,
    const std::shared_ptr<RowIds> & row_ids_map,
    const std::shared_ptr<RowIds> & inverted_row_ids_map,
    const std::shared_ptr<RowSource> & inverted_row_sources_map);

VIBitmapPtr getRealBitmap(
    const VIBitmapPtr filter,
    const VIWithMeta & index_with_meta);

std::vector<DB::MergedPartNameAndId>
getMergedSourcePartsFromFileName(const String & index_name, const DB::MergeTreeDataPartChecksums vector_index_checksums);

String getVectorIndexCachePrefix(
    const String & table_relative_path,
    const String & part_name,
    const String & index_name);

std::pair<String, String> getPartNameUUIDFromNvmeCachePath(const String & path_with_uuid);

String generateUUIDv4();

void printMemoryInfo(const Poco::Logger * log, std::string msg);

/// Update part's single delete bitmap after lightweight delete on disk and cache if exists.
void updateBitMap(SegmentId & segment_id, const std::vector<UInt64> & deleted_row_ids);

/// Update part's single delete bitmap after lightweight delete on disk and cache if exists.
void updateSingleBitMap(VIWithMeta & index_with_meta, const std::vector<UInt64> & deleted_row_ids);

/// Update merged old part's delete bitmap after lightweight delete on disk and cache if exists.
void updateMergedBitMap(VIWithMeta & index_with_meta, const std::vector<UInt64> & deleted_row_ids);

uint64_t getVectorDimension(const Search::DataType &search_type, const DB::StorageInMemoryMetadata &metadata, const String &column_name);

size_t getEachVectorBytes(const Search::DataType &search_type, const size_t dimension);

}
