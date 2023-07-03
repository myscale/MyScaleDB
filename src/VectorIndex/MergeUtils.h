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
#include <Common/logger_useful.h>

#include <VectorIndex/SegmentId.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/VectorSegmentExecutor.h>

#pragma GCC diagnostic ignored "-Wunused-function"
namespace VectorIndex
{

/// used to move vector indices files of one old data part
/// to new data part's path
static inline void moveVectorIndexFiles(
    const String & part_id, const String & part_name, const DB::IDataPartStorage & old_storage, DB::IDataPartStorage & new_storage)
{
    bool both_on_disk = !old_storage.isStoredOnRemoteDisk() && !new_storage.isStoredOnRemoteDisk();
    bool same_disk = old_storage.getDiskName() == new_storage.getDiskName();

    auto old_path = old_storage.getFullPath();
    auto new_path = new_storage.getFullPath();

    /// first get all vector indices related files
    String ext(VECTOR_INDEX_FILE_SUFFIX);
    for (const auto & p : fs::recursive_directory_iterator(old_path))
    {
        if (p.path().extension() != ext)
            continue;

        if (both_on_disk || same_disk)
        {
            /// if both saved on local disk or on same remote fs, just call fs::rename to move files
            String new_file_path = new_path + "merged-" + part_id + "-" + part_name + "-" + DB::fileName(p.path());
            fs::rename(p.path(), new_file_path);
        }
        else
        {
            /// different disks, we need to read from old part and write to new part
            auto read_buf = old_storage.readFile(p.path(), /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
            auto size = read_buf->getFileSize();

            String new_file_path = new_path + "merged-" + part_id + "-" + part_name + "-" + DB::fileName(p.path());
            auto write_buf
                = new_storage.writeFile(new_file_path, std::min<size_t>(size, DBMS_DEFAULT_BUFFER_SIZE), /* mode */ {}, /* settings */ {});

            DB::copyData(*read_buf, *write_buf, size);
            write_buf->finalize();
        }
    }
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
            String vector_index_cache_prefix = fs::path(data_part->storage.getContext()->getVectorIndexCachePath())
                / data_part->storage.getRelativeDataPath()
                / DB::MergeTreePartInfo::fromPartName(old_part.name, DB::MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                      .getPartNameWithoutMutation()
                / "";
            SegmentId segment_id(
                volume,
                data_path,
                data_part->name,
                old_part.name,
                index_name,
                index_column,
                vector_index_cache_prefix,
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
        String vector_index_cache_prefix = fs::path(data_part->storage.getContext()->getVectorIndexCachePath())
            / data_part->storage.getRelativeDataPath() / data_part->info.getPartNameWithoutMutation() / "";
        SegmentId segment_id(volume, data_path, data_part->name, index_name, index_column, vector_index_cache_prefix);
        segment_ids.emplace_back(std::move(segment_id));
    }

    /// TODO: Should we add a new function getAllOldSegementIds() to get list of old parts, no matter there is built vector index or not.
    /// decide whether we have merged old data parts‘ index files
    if (segment_ids.empty() && data_part->containRowIdsMaps())
    {
        auto old_parts = data_part->getMergedSourceParts();

        for (const auto & old_part : old_parts)
        {
            String vector_index_cache_prefix = fs::path(data_part->storage.getContext()->getVectorIndexCachePath())
                / data_part->storage.getRelativeDataPath()
                / DB::MergeTreePartInfo::fromPartName(old_part.name, DB::MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                      .getPartNameWithoutMutation()
                / "";
            SegmentId segment_id(
                volume,
                data_path,
                data_part->name,
                old_part.name,
                index_name,
                index_column,
                vector_index_cache_prefix,
                old_part.id);
            segment_ids.emplace_back(std::move(segment_id));
        }
    }

    return segment_ids;
}

/// Remove old parts' vector index from cache manager and data part.
static void removeRowIdsMaps(const DB::MergeTreeDataPartPtr & data_part, const Poco::Logger * log)
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
        String vector_index_cache_prefix = fs::path(data_part->storage.getContext()->getVectorIndexCachePath())
            / data_part->storage.getRelativeDataPath()
            / DB::MergeTreePartInfo::fromPartName(old_part.name, DB::MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                    .getPartNameWithoutMutation()
            / "";
        SegmentId segment_id(
            volume,
            data_part->getDataPartStorage().getFullPath(),
            data_part->name,
            old_part.name,
            vec_index_desc.name,
            vec_index_desc.column,
            vector_index_cache_prefix,
            old_part.id);
        old_segments.emplace_back(std::move(segment_id));
    }

    for (auto & old_segment : old_segments)
    {
        VectorSegmentExecutor::removeFromCache(old_segment.getCacheKey());
    }

    /// Remove files and erase the metadata of row ids maps from data part.
    data_part->removeAllRowIdsMaps();
}

}
