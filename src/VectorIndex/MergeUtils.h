#pragma once
#pragma GCC diagnostic ignored "-Wunused-function"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/SegmentId.h>

#include <Common/logger_useful.h>

namespace VectorIndex
{

/// used to rename and move vector indices files of one old data part
/// to new data part's path
static inline void renameVectorIndexFiles(const String& part_id, const String& part_name, const String& old_path, const String& new_path)
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

    /// decide whether we have merged old data parts‘ index files
    for (auto &p : fs::recursive_directory_iterator(data_path))
    {
        if (p.path().filename().string().find("row_ids_map") != std::string::npos 
            && p.path().filename().string().find("inverted_row_ids_map") == std::string::npos)
        {
            /// found merged files
            std::vector<String> strs;
            boost::algorithm::split(strs, p.path().filename().string(), boost::is_any_of("-"));

            LOG_DEBUG(&Poco::Logger::get("getAllSegmentIds"), "segments: {} {}", strs[0], strs[1]);

            SegmentId segment_id(data_path, data_part->name, strs[2], index_name, index_column, std::stoi(strs[1]));
            segment_ids.emplace_back(std::move(segment_id));
        }
    }
    if (segment_ids.empty())
    {
        SegmentId segment_id(data_path, data_part->name, data_part->name, index_name, index_column, 0);
        segment_ids.emplace_back(std::move(segment_id));
    }
    return segment_ids;
}

static bool containRowIdsMaps(const String& data_path)
{
    if (fs::exists(data_path))
    {
        for (auto & p : fs::recursive_directory_iterator(data_path))
        {
            if (p.path().filename().string().find("inverted_row_ids_map") != std::string::npos)
            {
                return true;
            }
        }
    }
    else
        LOG_INFO(&Poco::Logger::get("containRowIdsMaps"), "Part path: {} does not exist, it may be still in memory.", data_path);
    return false;
}

static bool containRowIdsMaps(const std::shared_ptr<const DB::IMergeTreeDataPart>& part)
{
    /// Skip to check disk for in-memory part
    if (!part->isStoredOnDisk())
        return false;
    else
       return containRowIdsMaps(part->getDataPartStorage().getFullPath());
}

static void removeAllRowIdsMaps(const String& data_path)
{
    for (auto &p : fs::recursive_directory_iterator(data_path))
    {
        if (p.path().filename().string().starts_with("merged-"))
        {
            fs::remove(p.path());
        }
    }
}

}
