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
static DB::MergeTreeDataPartChecksums moveVectorIndexFiles(
    const bool decouple,
    const String & part_id,
    const String & part_name,
    const String & vec_index_name,
    DB::MergeTreeDataPartPtr old_data_part,
    DB::MergeTreeMutableDataPartPtr new_data_part)
{
    const auto & old_storage = old_data_part->getDataPartStorage();
    auto & mutable_old_storage = const_cast<DB::IDataPartStorage &>(old_storage);
    auto & new_storage = new_data_part->getDataPartStorage();

    bool both_on_disk = !old_storage.isStoredOnRemoteDisk() && !new_storage.isStoredOnRemoteDisk();
    bool same_disk = old_storage.getDiskName() == new_storage.getDiskName();

    auto old_path = old_storage.getFullPath();
    auto new_path = new_storage.getFullPath();

    DB::MergeTreeDataPartChecksums new_index_checksums;

    /// move and rename vector index files,
    /// combine vector index checksums and fill it to map
    DB::MergeTreeDataPartChecksums old_index_checksums = old_data_part->getVectorIndexChecksums(vec_index_name);

    /// No need to move when no vector index files
    if (old_index_checksums.empty())
        return new_index_checksums;

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
            fs::rename(old_file_path, new_file_path);
        }
        else
        {
            /// different disks, we need to read from old part and write to new part
            auto read_buf = old_storage.readFile(old_file_path, /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
            auto size = read_buf->getFileSize();
            auto write_buf = new_storage.writeFile(
                new_file_path, std::min<size_t>(size, DBMS_DEFAULT_BUFFER_SIZE), /* mode */ {}, /* settings */ {});

            DB::copyData(*read_buf, *write_buf, size);
            write_buf->finalize();
        }

        new_index_checksums.addFile(new_file_name, checksum_.file_size, checksum_.file_hash);
    }

    /// remove old part vector index checksums files
    mutable_old_storage.removeFileIfExists(VectorIndex::getVectorIndexChecksumsFileName(vec_index_name));

    /// remove from old part's metadata
    old_data_part->removeVectorIndexChecksums(vec_index_name);

    return new_index_checksums;
}

static std::vector<SegmentId> getAllOldSegmentIds(
    const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & index_column)
{
    std::vector<SegmentId> segment_ids;
    if (!data_part)
        return segment_ids;

    if (data_part->containRowIdsMaps(index_name))
    {
        auto old_parts = data_part->getMergedSourceParts(index_name);

        for (const auto & old_part : old_parts)
        {
            SegmentId segment_id(
                data_part->getDataPartStoragePtr(),
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
    const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & index_column)
{
    std::vector<SegmentId> segment_ids;

    if (!data_part)
        return segment_ids;

    /// If no merged old parts' index files, decide whether we have simple built vector index.
    if (data_part->containVectorIndex(index_name))
    {
        SegmentId segment_id(data_part->getDataPartStoragePtr(), data_part->name, index_name, index_column);
        segment_ids.emplace_back(std::move(segment_id));
    }

    if (!segment_ids.empty())
        return segment_ids;

    /// TODO: Should we add a new function getAllOldSegmentIds() to get list of old parts, no matter there is built vector index or not.
    /// decide whether we have merged old data parts‘ index files
    return getAllOldSegmentIds(data_part, index_name, index_column);
}

/// Remove old parts' vector index from cache manager and data part.
static void removeRowIdsMapsFromPartAndCache(const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & column_name, bool skip_checksum, const Poco::Logger * log)
{
    if (!data_part || !data_part->isStoredOnDisk() || !data_part->containRowIdsMaps(index_name))
        return;

    LOG_DEBUG(log, "Try to remove row ids maps files for vector index {} in {}", index_name, data_part->getDataPartStorage().getFullPath());

    std::vector<SegmentId> old_segments = getAllOldSegmentIds(data_part, index_name, column_name);
    for (auto & old_segment : old_segments)
    {
        VectorSegmentExecutor::removeFromCache(old_segment.getCacheKey());
    }

    /// Remove files and erase the metadata of row ids maps related to this vector index from data part.
    /// skip remove its checksums file, if skip_checksum is true
    data_part->removeRowIdsMaps(index_name, skip_checksum);
}

/// Remove vector index from cache manager and data part, merged old parts or simple vector index.
static void removeVectorIndexFromPartAndCache(const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & column_name, const Poco::Logger * log)
{
    if (!data_part || !data_part->isStoredOnDisk())
        return;

    /// Clear build error for vector index with the index_name in this part
    data_part->removeBuildError(index_name);

    /// Quick return if no simple vector index or merged vector indices
    if (!data_part->containVectorIndex(index_name) && !data_part->containRowIdsMaps(index_name))
        return;

    /// Clear cache first, now getAllSegmentIds() is based on vector index files
    auto segment_ids = getAllSegmentIds(data_part, index_name, column_name);
    for (auto & segment_id : segment_ids)
        VectorSegmentExecutor::removeFromCache(segment_id.getCacheKey());

    /// Delete files in part directory if exists and metadata
    /// Simple vector index for VPart
    if (segment_ids.size() == 1)
    {
        data_part->removeVectorIndex(index_name);
        LOG_DEBUG(log, "Removed cache and files for vector index {} in part {}", index_name, data_part->name);
    }
    else
    {
        data_part->removeRowIdsMaps(index_name);
        LOG_DEBUG(log, "Removed caches and row ids maps files for vector index {} in decouple part {}", index_name, data_part->name);
    }
}

}
