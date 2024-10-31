#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>

#include <boost/algorithm/string.hpp>

#include <Disks/IDisk.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Common/logger_useful.h>

#include <VectorIndex/SegmentId.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/VectorSegmentExecutor.h>

#pragma GCC diagnostic ignored "-Wunused-function"
namespace VectorIndex
{

/// used to move vector indices files of one old data part
/// to new data part's path, and move vector index checksums to avoid recalculate
static std::unordered_map<String, DB::MergeTreeDataPartChecksums> moveVectorIndexFiles(
    const String & part_id, const String & part_name, DB::MergeTreeDataPartPtr old_data_part, DB::MergeTreeMutableDataPartPtr new_data_part)
{
    const auto & old_storage = old_data_part->getDataPartStorage();
    auto & mutable_old_storage = const_cast<DB::IDataPartStorage &>(old_storage);
    auto & new_storage = new_data_part->getDataPartStorage();
    bool both_on_disk = !old_storage.isStoredOnRemoteDisk() && !new_storage.isStoredOnRemoteDisk();
    bool same_disk = old_storage.getDiskName() == new_storage.getDiskName();
    std::unordered_map<String, DB::MergeTreeDataPartChecksums> vector_index_checksums_map;

    auto old_path = old_storage.getFullPath();
    auto new_path = new_storage.getFullPath();

    /// move and rename vector index files,
    /// combine vector index checksums and fill it to map
    std::lock_guard lock_old(old_data_part->vector_index_checksums_mutex);
    for (const auto & [vector_index_name, checksums_] : old_data_part->vector_index_checksums_map)
    {
        for (const auto & [old_file_name, checksum_] : checksums_.files)
        {
            String old_file_path = old_path + old_file_name;
            String new_file_name = "merged-" + part_id + "-" + part_name + "-" + old_file_name;
            String new_file_path = new_path + new_file_name;

            if (both_on_disk || same_disk)
            {
                /// if both saved on local disk or on same remote fs, just call fs::rename to move files
                fs::rename(old_file_path, new_file_path);
            }
            else
            {
                /// different disks, we need to read from old part and write to new part
                auto read_buf = old_storage.readFile(old_file_path, /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
                auto size = read_buf->getFileSize();
                auto write_buf = new_storage.writeFile(
                    new_file_path, std::min<size_t>(size, DB::DBMS_DEFAULT_BUFFER_SIZE), /* mode */ {}, /* settings */ {});

                DB::copyData(*read_buf, *write_buf, size);
                write_buf->finalize();
            }

            vector_index_checksums_map[vector_index_name].addFile(new_file_name, checksum_.file_size, checksum_.file_hash);
        }

        /// remove old part vector index checksums files
        mutable_old_storage.removeFile(old_path + vector_index_name + "-" + VECTOR_INDEX_CHECKSUMS + VECTOR_INDEX_FILE_SUFFIX);
    }
    old_data_part->vector_index_checksums_map.clear();

    return vector_index_checksums_map;
}

static std::vector<SegmentId> getAllOldSegementIds(
    const String & data_path, const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & index_column)
{
    std::vector<SegmentId> segment_ids;
    if (!data_part)
        return segment_ids;

    const DB::DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(data_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
    {
        return segment_ids;
    }
    auto volume = getVolumeFromPartStorage(*part_storage);
    if (data_part->containRowIdsMaps())
    {
        auto old_parts = data_part->getMergedSourceParts();

        for (const auto & old_part : old_parts)
        {
            SegmentId segment_id(
                volume,
                data_path,
                data_part->name,
                old_part.name,
                index_name,
                index_column,
                old_part.id);
            segment_ids.emplace_back(std::move(segment_id));
        }
    }

    return segment_ids;
}

static std::vector<SegmentId> getAllSegmentIds(
    const String & data_path, const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & index_column)
{
    std::vector<SegmentId> segment_ids;

    if (!data_part)
        return segment_ids;

    const DB::DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(data_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
    {
        return segment_ids;
    }
    auto volume = getVolumeFromPartStorage(*part_storage);
    /// If no merged old parts' index files, decide whether we have simple built vector index.
    if (data_part->containVectorIndex(index_name, index_column))
    {
        SegmentId segment_id(volume, data_path, data_part->name, index_name, index_column);
        segment_ids.emplace_back(std::move(segment_id));
    }

    /// TODO: Should we add a new function getAllOldSegementIds() to get list of old parts, no matter there is built vector index or not.
    /// decide whether we have merged old data parts‘ index files
    if (segment_ids.empty() && data_part->containRowIdsMaps())
    {
        auto old_parts = data_part->getMergedSourceParts();

        for (const auto & old_part : old_parts)
        {
            SegmentId segment_id(
                volume,
                data_path,
                data_part->name,
                old_part.name,
                index_name,
                index_column,
                old_part.id);
            segment_ids.emplace_back(std::move(segment_id));
        }
    }

    return segment_ids;
}

/// Remove old parts' vector index from cache manager and data part.
static void
removeRowIdsMaps(const DB::MergeTreeDataPartPtr & data_part, const String & vector_index_name, bool skip_checksum, const Poco::Logger * log)
{
    if (!data_part || !data_part->isStoredOnDisk() || !data_part->containRowIdsMaps())
        return;

    LOG_DEBUG(log, "Try to remove row ids maps files in {}", data_part->getDataPartStorage().getFullPath());
    /// currently only consider one vector index
    auto metadata_snapshot = data_part->storage.getInMemoryMetadataPtr();
    auto vec_index_desc = metadata_snapshot->vec_indices[0];

    std::vector<SegmentId> old_segments;
    auto old_parts = data_part->getMergedSourceParts();
    const DB::DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(data_part->getDataPartStoragePtr().get());
    if (part_storage == nullptr)
    {
        return;
    }
    auto volume = getVolumeFromPartStorage(*part_storage);
    for (const auto & old_part : old_parts)
    {
        SegmentId segment_id(
            volume,
            data_part->getDataPartStorage().getFullPath(),
            data_part->name,
            old_part.name,
            vec_index_desc.name,
            vec_index_desc.column,
            old_part.id);
        old_segments.emplace_back(std::move(segment_id));
    }

    for (auto & old_segment : old_segments)
    {
        VectorSegmentExecutor::removeFromCache(old_segment.getCacheKey());
    }

    /// Remove files and erase the metadata of row ids maps from data part.
    /// skip remove its checksums file, if skip_checksum is true
    data_part->removeAllRowIdsMaps(vector_index_name, skip_checksum);
}

}
