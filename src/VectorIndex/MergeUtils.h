#pragma once
#pragma GCC diagnostic ignored "-Wunused-function"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/SegmentId.h>

#include <Common/logger_useful.h>

namespace VectorIndex
{

/// used to rename and move vector indices files of one old data part
/// to new data part's path
static inline void renameVectorIndexFiles(const String & part_id, const String & part_name, const String & old_path, const String & new_path)
{
    /// first get all vector indices related files
    String ext(VECTOR_INDEX_FILE_SUFFIX);
    for (auto &p : fs::recursive_directory_iterator(old_path))
    {
        if (p.path().extension() == ext)
        {
            String new_file_path = new_path + "merged-" + part_id + "-" + part_name + "-" + DB::fileName(p.path());
            fs::rename(p.path(), new_file_path);
        }
    }   
}

static std::vector<SegmentId> getAllSegmentIds(const String & data_path, const DB::MergeTreeDataPartPtr & data_part, const String & index_name, const String & index_column)
{
    std::vector<SegmentId> segment_ids;

    if (!data_part)
        return segment_ids;

    /// TODO: Should we add a new function getAllOldSegementIds() to get list of old parts, no matter there is built vector index or not.
    /// decide whether we have merged old data parts‘ index files
    if (data_part->containRowIdsMaps())
    {
        auto log = &Poco::Logger::get("getAllSegmentIds");
        auto old_parts = data_part->getMergedSourceParts();

        for (const auto & old_part : old_parts)
        {
            LOG_DEBUG(log, "segments: merged-{}-{}", old_part.id, old_part.name);
            SegmentId segment_id(data_path, data_part->name, old_part.name, index_name, index_column, old_part.id);
            segment_ids.emplace_back(std::move(segment_id));
        }
    }

    /// If no merged old parts' index files, decide whether we have simple built vector index.
    if (segment_ids.empty() && data_part->containVectorIndex(index_name, index_column))
    {
        SegmentId segment_id(data_path, data_part->name, data_part->name, index_name, index_column, 0);
        segment_ids.emplace_back(std::move(segment_id));
    }
    return segment_ids;
}

static bool containRowIdsMaps(const String & data_path)
{
    fs::path path = fs::path(data_path) / (DB::toString("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX);
    if (fs::exists(path))
        return true;
    else
        return false;
}

/// Remove old parts' vector index from cache manager and data part.
static void removeRowIdsMaps(const DB::MergeTreeDataPartPtr & data_part)
{
    if (!data_part || !data_part->isStoredOnDisk() || !data_part->containRowIdsMaps())
        return;

    LOG_INFO(&Poco::Logger::get("removeRowIdsMaps"), "try to remove row ids maps files in {}", data_part->getDataPartStorage().getFullPath());
    /// currently only consider one vector index
    auto metadata_snapshot = data_part->storage.getInMemoryMetadataPtr();
    auto vec_index_desc = metadata_snapshot->vec_indices[0];

    auto old_segments = getAllSegmentIds(data_part->getDataPartStorage().getFullPath(), data_part, vec_index_desc.name, vec_index_desc.column);
    for (auto & old_segment : old_segments)
    {
        VectorSegmentExecutor::removeFromCache(old_segment.getCacheKey());
    }

    /// Remove files and erase the metadata of row ids maps from data part.
    data_part->removeAllRowIdsMaps();
}

}
