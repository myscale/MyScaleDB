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

#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <sys/resource.h>

#include <Common/ErrorCodes.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <IO/copyData.h>
#include <IO/HashingReadBuffer.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

#include <VectorIndex/Common/SegmentId.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/VIWithDataPart.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <VectorIndex/Storages/VSDescription.h>
#include <VectorIndex/Utils/VIUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_VECTOR_INDEX;
    extern const int CORRUPTED_DATA;
}

/// used to move vector indices files of one old data part
/// to new data part's path, and move vector index checksums to avoid recalculate
MergeTreeDataPartChecksums moveVectorIndexFiles(
    const bool decouple,
    const String & part_id,
    const String & part_name,
    const String & vec_index_name,
    DB::MergeTreeDataPartPtr old_data_part,
    DB::MergeTreeMutableDataPartPtr new_data_part)
{
    LOG_DEBUG(&Poco::Logger::get("VIUtils"), "Create hard link for vector index {} file From {} to {}.", vec_index_name, old_data_part->name, new_data_part->name);
    const auto & old_storage = old_data_part->getDataPartStorage();
    auto & new_storage = new_data_part->getDataPartStorage();

    bool both_on_disk = !old_storage.isStoredOnRemoteDisk() && !new_storage.isStoredOnRemoteDisk();
    bool same_disk = old_storage.getDiskName() == new_storage.getDiskName();

    auto old_path = old_storage.getFullPath();
    auto new_path = new_storage.getFullPath();

    DB::MergeTreeDataPartChecksums new_index_checksums;

    /// move and rename vector index files,
    /// combine vector index checksums and fill it to map
    DB::MergeTreeDataPartChecksums old_index_checksums = getVectorIndexChecksums(old_data_part->getDataPartStoragePtr(), vec_index_name);

    /// No need to move when no vector index files
    if (old_index_checksums.empty())
        return new_index_checksums;
    
    const DB::DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(&new_storage);
    
    auto disk = part_storage->getDisk();

    for (const auto & [old_file_name, checksum_] : old_index_checksums.files)
    {
        /// vector index in old part doesn't exists
        if (!old_storage.exists(old_file_name))
            return new_index_checksums;

        String old_file_path = old_path + old_file_name;

        /// Use original name when not decouple (for cases when only one VPart is merged)
        String new_file_name;
        if (decouple)
            new_file_name = "merged-" + part_id + "-" + part_name + "-" + old_file_name;
        else
            new_file_name = old_file_name;

        String new_file_path = new_path + new_file_name;

        if (both_on_disk || same_disk)
        {
            /// if both saved on local disk or on same remote fs, just call fs::rename to move files
            disk->createHardLink(old_file_path, new_file_path);
        }
        else
        {
            /// different disks, we need to read from old part and write to new part
            auto read_buf = old_storage.readFile(old_file_path, /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
            auto size = read_buf->getFileSize();
            auto write_buf
                = new_storage.writeFile(new_file_path, std::min<size_t>(size, DBMS_DEFAULT_BUFFER_SIZE), /* mode */ {}, /* settings */ {});

            DB::copyData(*read_buf, *write_buf, size);
            write_buf->finalize();
        }

        new_index_checksums.addFile(new_file_name, checksum_.file_size, checksum_.file_hash);
    }

    return new_index_checksums;
}

MergeTreeDataPartChecksums calculateVectorIndexChecksums(
    const DataPartStoragePtr & part_storage,
    const String & vector_index_relative_path,
    std::shared_ptr<MergeTreeDataPartChecksums> existing_checksums)
{
    const DataPartStorageOnDiskBase * data_part_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(part_storage.get());
    if (data_part_storage == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");

    MergeTreeDataPartChecksums index_checksums;
    auto disk = data_part_storage->getDisk();
    for (auto it = disk->iterateDirectory(vector_index_relative_path); it->isValid(); it->next())
    {
        if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX))
            continue;
        
        if (existing_checksums && existing_checksums->has(it->name()))
        {
            LOG_DEBUG(&Poco::Logger::get("VIUtils"), "checksum exists: {}", it->name());
            auto checksum = existing_checksums->files.at(it->name());
            index_checksums.addFile(it->name(), checksum.file_size, checksum.file_hash);
        }
        else
        {
            auto file_buf = disk->readFile(it->path());
            HashingReadBuffer hashing_buf(*file_buf);
            hashing_buf.ignoreAll();
            index_checksums.addFile(it->name(), hashing_buf.count(), hashing_buf.getHash());
        }
    }

    return index_checksums;
}

}
namespace VectorIndex
{
String cutMutVer(const String & part_name)
{
    std::vector<String> tokens;
    boost::split(tokens, part_name, boost::is_any_of("_"));
    if (tokens.size() <= 4) /// without mutation version
    {
        return part_name;
    }
    else
        return tokens[0] + "_" + tokens[1] + "_" + tokens[2] + "_" + tokens[3];
}

String cutPartitionID(const String & part_name)
{
    std::vector<String> tokens;
    boost::split(tokens, part_name, boost::is_any_of("_"));
    return tokens[0];
}

String cutTableUUIDFromCacheKey(const String & cache_key)
{
    std::vector<String> tokens;
    boost::split(tokens, cache_key, boost::is_any_of("/"));
    return tokens[tokens.size() - 3];
}

String cutPartNameFromCacheKey(const String & cache_key)
{
    std::vector<String> tokens;
    boost::split(tokens, cache_key, boost::is_any_of("/"));
    return tokens[tokens.size() - 2];
}

/// [TODO] replace with index segment ptr
std::vector<SegmentId>
getAllSegmentIds(const DB::MergeTreeDataPartPtr & data_part, const String & index_name)
{
    auto column_index_opt = data_part->vector_index.getColumnIndex(index_name);
    if (!column_index_opt.has_value())
        return {};
    auto column_index = column_index_opt.value();

    return getAllSegmentIds(data_part->getDataPartStoragePtr(), data_part->name, *column_index->getIndexSegmentMetadata());
}

std::vector<SegmentId>
getAllSegmentIds(const DB::MergeTreeDataPartPtr & data_part, const DB::VISegmentMetadata & vector_index_segment_metadata)
{
    return getAllSegmentIds(data_part->getDataPartStoragePtr(), data_part->name, vector_index_segment_metadata);
}

std::vector<SegmentId>
getAllSegmentIds(const DB::IMergeTreeDataPart & data_part, const DB::VISegmentMetadata & vector_index_segment_metadata)
{
    return getAllSegmentIds(data_part.getDataPartStoragePtr(), data_part.name, vector_index_segment_metadata);
}

std::vector<SegmentId> getAllSegmentIds(
    const DB::DataPartStoragePtr part_storage,
    const String & current_part_name,
    const DB::VISegmentMetadata & vector_index_segment_metadata)
{
    std::vector<SegmentId> segment_ids;

    if (!vector_index_segment_metadata.is_ready.load())
        return segment_ids;

    /// If no merged old parts' index files, decide whether we have simple built vector index.
    if (!vector_index_segment_metadata.is_decouple_index)
    {
        SegmentId segment_id(part_storage, current_part_name, vector_index_segment_metadata.index_name, vector_index_segment_metadata.column_name);
        segment_ids.emplace_back(std::move(segment_id));
    }
    else
    {
        for (const auto & old_part : vector_index_segment_metadata.merge_source_parts)
        {
            /// empty part merge without vector index
            if (!old_part.with_index)
                continue;

            SegmentId segment_id(
                part_storage,
                current_part_name,
                old_part.name,
                vector_index_segment_metadata.index_name,
                vector_index_segment_metadata.column_name,
                old_part.id);
            segment_ids.emplace_back(std::move(segment_id));
        }
    }

    if (!vector_index_segment_metadata.is_ready.load())
        segment_ids.clear();

    return segment_ids;
}

DB::NameSet
getVectorIndexFileNamesInChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool need_checksums_file)
{
    String checksums_filename = VectorIndex::getVectorIndexChecksumsFileName(index_name);
    if (!part_storage->exists(checksums_filename))
        return {};

    try
    {
        DB::MergeTreeDataPartChecksums vector_index_checksums;
        auto buf = part_storage->readFile(checksums_filename, {}, std::nullopt, std::nullopt);
        if (vector_index_checksums.read(*buf))
            assertEOF(*buf);

        if (vector_index_checksums.empty())
            return {};

        DB::NameSet res;
        for (const auto & [file_name, _] : vector_index_checksums.files)
            res.emplace(file_name);

        if (need_checksums_file)
            res.emplace(checksums_filename);

        return res;
    }
    catch (const DB::Exception & e)
    {
        /// read checksum failed, return empty list
        LOG_WARNING(
            &Poco::Logger::get("VIUtils"),
            "getVectorIndexFileNamesInChecksums: read checksum file {} error {}: {}",
            checksums_filename,
            e.code(),
            e.message());
    }

    return {};
}

DB::MergeTreeDataPartChecksums
getVectorIndexChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name)
{
     String checksums_filename = VectorIndex::getVectorIndexChecksumsFileName(index_name);
    if (!part_storage->exists(checksums_filename))
        throw DB::Exception(
            DB::ErrorCodes::INVALID_VECTOR_INDEX,
            "Checksums file {} in part {} does not exists.",
            checksums_filename,
            part_storage->getRelativePath());

    DB::MergeTreeDataPartChecksums vector_index_checksums;
    auto buf = part_storage->readFile(checksums_filename, {}, std::nullopt, std::nullopt);
    if (vector_index_checksums.read(*buf))
        assertEOF(*buf);

    if (vector_index_checksums.empty())
        throw DB::Exception(DB::ErrorCodes::INVALID_VECTOR_INDEX, "Read empty checksums for file {}", checksums_filename);

    return vector_index_checksums;

}

void removeVectorIndexFilesFromFileLists(const DB::DataPartStoragePtr & part_storage, const DB::Names & index_files_list)
{
    DB::IDataPartStorage * storage = const_cast<DB::IDataPartStorage *>(part_storage.get());
    for (const auto & file_name : index_files_list)
        storage->removeFileIfExists(file_name);
}

void removeIndexFile(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool include_checksum_file)
{
    auto * mutable_part_storage = const_cast<DB::IDataPartStorage *>(part_storage.get());
    for (auto & file_name : getVectorIndexFileNamesInChecksums(part_storage, index_name, include_checksum_file))
        mutable_part_storage->removeFileIfExists(file_name);
}

DB::NameSet getAllValidVectorIndexFileNames(const DB::IMergeTreeDataPart & data_part)
{
    if (!data_part.storage.getInMemoryMetadataPtr()->hasVectorIndices())
        return {};

    DB::NameSet valid_indices_file;

    for (const auto & vec_desc : data_part.storage.getInMemoryMetadataPtr()->getVectorIndices())
    {
        auto index_file = getVectorIndexFileNamesInChecksums(data_part.getDataPartStoragePtr(), vec_desc.name, /*need checksum file*/ true);
        valid_indices_file.insert(index_file.begin(), index_file.end());
    }

    return valid_indices_file;
}

void removeIncompleteMovedVectorIndexFiles(const DB::IMergeTreeDataPart & data_part)
{
    auto exclude_index_file = getAllValidVectorIndexFileNames(data_part);
    auto & mutable_part_storage = const_cast<DB::IDataPartStorage &>(data_part.getDataPartStorage());
    auto iter_file = mutable_part_storage.iterate();
    for (auto it = mutable_part_storage.iterate(); it->isValid(); it->next())
    {
        if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX) || exclude_index_file.count(it->name()) != 0)
            continue;

        mutable_part_storage.removeFileIfExists(it->name());
    }
}

void dumpCheckSums(
    const DB::DataPartStoragePtr & part_storage, const String & index_name, const DB::MergeTreeDataPartChecksums & index_checksum)
{
    /// write new part decoupled vector index checksums file
    auto * mutable_part_storage = const_cast<DB::IDataPartStorage *>(part_storage.get());
    auto out_checksums = mutable_part_storage->writeFile(getVectorIndexChecksumsFileName(index_name), 4096, {});

    index_checksum.write(*out_checksums);
    out_checksums->finalize();
}

bool checkConsistencyForVectorIndex(const DB::DataPartStoragePtr & part_storage, const DB::MergeTreeDataPartChecksums & index_checksums)
{
    /// Base consistency check: existence of files recorded in checksums
    try
    {
        index_checksums.checkSizes(*part_storage);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

const std::vector<UInt64> readDeleteBitmapAccordingSegmentId(const SegmentId segment_id)
{
    // Get mergedatapart information according to context
    auto local_context = DB::Context::createCopy(DB::Context::getGlobalContextInstance());
    DB::UUID table_uuid = DB::VIEventLog::parseUUID(segment_id.getCacheKey().getTableUUID());

    // Get database and table name
    if (!DB::DatabaseCatalog::instance().tryGetByUUID(table_uuid).second)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Unable to get table and database by table uuid");
    auto table_id = DB::DatabaseCatalog::instance().tryGetByUUID(table_uuid).second->getStorageID();
    if (!table_id)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Unable to get table and database by table uuid");

    // Get MergeTree Data Storage
    DB::StoragePtr table = DB::DatabaseCatalog::instance().tryGetTable({table_id.database_name, table_id.table_name}, local_context);
    DB::MergeTreeData * merge_tree = dynamic_cast<DB::MergeTreeData *>(table.get());
    if (!merge_tree)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Unable to fetch MergeTree Data Storage");

    std::vector<UInt64> del_row_ids; /// Store deleted row ids

    // Get the part corresponding to the current segment, Whether to allow reading delete bitmap from part in outdated state?
    auto part = merge_tree->getPartIfExists(segment_id.current_part_name, {DB::MergeTreeDataPartState::Active, DB::MergeTreeDataPartState::Outdated});
    if (!part)
        return del_row_ids;

    auto row_exists_column_opt = part->readRowExistsColumn();
    if (!row_exists_column_opt.has_value())
        return del_row_ids;

    const DB::ColumnUInt8 * row_exists_col = typeid_cast<const DB::ColumnUInt8 *>(row_exists_column_opt.value().get());
    if (row_exists_col == nullptr)
        return del_row_ids;

    const DB::ColumnUInt8::Container & vec_res = row_exists_col->getData();
    for (size_t pos = 0; pos < vec_res.size(); pos++)
    {
        if (!vec_res[pos])
            del_row_ids.push_back(static_cast<UInt64>(pos));
    }

    return del_row_ids;
}

void convertBitmap(
    const SegmentId & segment_id,
    const std::vector<UInt64> & part_deleted_row_ids,
    VIBitmapPtr index_deleted_row_ids,
    const std::shared_ptr<RowIds> & /*row_ids_map*/,
    const std::shared_ptr<RowIds> & inverted_row_ids_map,
    const std::shared_ptr<RowSource> & inverted_row_sources_map)
{
    if (segment_id.fromMergedParts())
        if (!inverted_row_sources_map || !inverted_row_ids_map)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Inverted Row Id Data corruption");

    if (segment_id.fromMergedParts())
    {
        for (auto & new_row_id : part_deleted_row_ids)
        {
            if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
            {
                UInt64 old_row_id = (*inverted_row_ids_map)[new_row_id];
                if (index_deleted_row_ids->is_member(old_row_id))
                {
                    index_deleted_row_ids->unset(old_row_id);
                }
            }
        }
    }
    else
    {
        for (auto & del_row_id : part_deleted_row_ids)
        {
            if (index_deleted_row_ids->is_member(del_row_id))
            {
                index_deleted_row_ids->unset(del_row_id);
            }
        }
    }
}

VIBitmapPtr getRealBitmap(
    const VIBitmapPtr filter,
    const VIWithMeta & index_with_meta)
{
    if (!index_with_meta.inverted_row_ids_map || index_with_meta.inverted_row_ids_map->empty())
        return filter;

    VIBitmapPtr real_filter = std::make_shared<VIBitmap>(index_with_meta.total_vec);

    for (auto & new_row_id : filter->to_vector())
        {
            if (index_with_meta.own_id == (*(index_with_meta.inverted_row_sources_map))[new_row_id])
            {
                real_filter->set((*(index_with_meta.inverted_row_ids_map))[new_row_id]);
            }
        }

    return real_filter;
}

std::vector<DB::MergedPartNameAndId>
getMergedSourcePartsFromFileName(const String & index_name, const DB::MergeTreeDataPartChecksums vector_index_checksums)
{
    std::vector<DB::MergedPartNameAndId> old_part_names;
    if (!vector_index_checksums.has(DB::toString("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX))
        return old_part_names;

    String description_file_name = VectorIndex::getVectorIndexDescriptionFileName(index_name);
    for (auto const & [file_name, _] : vector_index_checksums.files)
    {
        if (!endsWith(file_name, description_file_name))
            continue;

        /// Found merged files, merged-<id>-<part name>-<vector_index_name>-vector_index_ready
        DB::Strings tokens;
        boost::algorithm::split(tokens, file_name, boost::is_any_of("-"));
        if (tokens.size() != 5)
        {
            return {};
        }

        old_part_names.emplace_back(tokens[2], std::stoi(tokens[1]));
    }
    return old_part_names;
}

String getVectorIndexCachePrefix(
    const String & table_relative_path,
    const String & part_name,
    const String & index_name)
{
    auto global_context = DB::Context::getGlobalContextInstance();
    if (global_context)
    {
        return fs::path(global_context->getVectorIndexCachePath())
            / fs::path(table_relative_path).parent_path().parent_path()
            / String(cutMutVer(part_name) + "-" + index_name)
            / "";
    }
    else
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot get Vector Index Cache prefix!");
}

String generateUUIDv4() {
    return DB::toString(DB::UUIDHelpers::generateV4());
}

std::pair<String, String> getPartNameUUIDFromNvmeCachePath(const String & nvme_cache_path)
{
    std::vector<String> tokens;
    /// path uuid example: all_2778448_2786755_136-v1-bcab5ab2-0b55-41ed-b42d-c62ba844e434
    boost::split(tokens, nvme_cache_path, boost::is_any_of("-"));
    String part_name, index_name, uuid_path;
    if (tokens.size() == 7)
    {
        part_name = tokens[0];
        index_name = tokens[1];
        uuid_path = tokens[2] + "-" + tokens[3] + "-" + tokens[4] + "-" + tokens[5] + "-" + tokens[6];
    }
    return std::make_pair(String(part_name + "-" + index_name), uuid_path);

}   


void printMemoryInfo(const Poco::Logger * log, std::string msg)
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    DB::MemoryStatisticsOS memory_stat;
    DB::MemoryStatisticsOS::Data data = memory_stat.get();
    LOG_INFO(
        log,
        "{}: peak resident memory {} MB, resident memory {} MB, virtual memory {} MB",
        msg,
        usage.ru_maxrss / 1024,
        data.resident / 1024 / 1024,
        data.virt / 1024 / 1024);
#endif
}

void updateBitMap(SegmentId & segment_id, const std::vector<UInt64> & deleted_row_ids)
{
    VICacheManager * mgr = VICacheManager::getInstance();
    IndexWithMetaHolderPtr index_holder = mgr->get(segment_id.getCacheKey());

    if (!index_holder)
        return;
    if (!segment_id.fromMergedParts())
        updateSingleBitMap(index_holder->value(), deleted_row_ids);
    else
        updateMergedBitMap(index_holder->value(), deleted_row_ids);
}

void updateSingleBitMap(VIWithMeta & index_with_meta, const std::vector<UInt64> & deleted_row_ids)
{
    if (deleted_row_ids.empty() || index_with_meta.row_ids_map)
        return;

    VIBitmapPtr delete_bitmap = std::make_shared<VIBitmap>(*(index_with_meta.getDeleteBitmap().get()));

    /// Map new deleted row ids to row ids in old part and update delete bitmap
    bool need_update = false;
    for (auto & del_row_id : deleted_row_ids)
    {
        if (delete_bitmap->is_member(del_row_id))
        {
            delete_bitmap->unset(del_row_id);

            if (!need_update)
                need_update = true;
        }
    }

    if (!need_update)
        return;

    index_with_meta.setDeleteBitmap(delete_bitmap);
}

void updateMergedBitMap(VIWithMeta & index_with_meta, const std::vector<UInt64> & deleted_row_ids)
{
    if (deleted_row_ids.empty() || !index_with_meta.row_ids_map || index_with_meta.row_ids_map->empty())
        return;

    VIBitmapPtr delete_bitmap = std::make_shared<VIBitmap>(*(index_with_meta.getDeleteBitmap().get()));

    /// Map new deleted row ids to row ids in old part and update delete bitmap
    bool need_update = false;
    for (auto & new_row_id : deleted_row_ids)
    {
        if (index_with_meta.own_id == (*(index_with_meta.inverted_row_sources_map))[new_row_id])
        {
            UInt64 old_row_id = (*(index_with_meta.inverted_row_ids_map))[new_row_id];
            if (delete_bitmap->is_member(old_row_id))
            {
                delete_bitmap->unset(old_row_id);

                if (!need_update)
                    need_update = true;
            }
        }
    }

    if (!need_update)
        return;

    index_with_meta.setDeleteBitmap(delete_bitmap);
}

uint64_t getVectorDimension(const Search::DataType &search_type, const DB::StorageInMemoryMetadata &metadata, const String &column_name)
{
    std::optional<DB::NameAndTypePair> search_column_type = metadata.columns.getAllPhysical().tryGetByName(column_name);
    if (!search_column_type)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "vector search column name: {}, type is not exist", column_name);
    uint64_t result_dim = 0;
    switch (search_type)
    {
        case Search::DataType::FloatVector:
        {
            const DB::DataTypeArray *array_type = typeid_cast<const DB::DataTypeArray *>(search_column_type->type.get());
            if (!array_type)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector search type is FloatVector for column {}, but datatype is not Array()", column_name);
            result_dim = metadata.getConstraints().getArrayLengthByColumnName(column_name).first;
            LOG_DEBUG(&Poco::Logger::get("VIUtils"), "vector search type: FloatVector, search column dim: {}", result_dim);
            break;
        }
        case Search::DataType::BinaryVector:
        {
            const DB::DataTypeFixedString *fixed_string_type = typeid_cast<const DB::DataTypeFixedString *>(search_column_type->type.get());
            if (!fixed_string_type)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector search type is BinaryVector for column {}, but datatype is not FixedString", column_name);
            result_dim = static_cast<uint64_t>(fixed_string_type->getN() * 8);
            LOG_DEBUG(&Poco::Logger::get("VIUtils"), "vector search type: BinaryVector, search column dim: {}", result_dim);
            break;
        }
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported Vector search Type");
    }
    return result_dim;
}
size_t getEachVectorBytes(const Search::DataType &search_type, const size_t dimension)
{
    switch (search_type)
    {
        case Search::DataType::FloatVector:
            return dimension * sizeof(float);
        case Search::DataType::BinaryVector:
            // As for Binary Vector, each dimension is 1 bit
            return dimension / 8;
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type");
    }
}

}
