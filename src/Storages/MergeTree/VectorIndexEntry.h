#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Common/logger_useful.h>

namespace DB
{

struct VectorIndexEntry
{
    //std::vector<MergeTreeDataPartPtr> data_parts;

    std::vector<String> data_part_names;
    MergeTreeData & data;

    VectorIndexEntry(const std::vector<String> data_part_names_, MergeTreeData & data_) : data_part_names(std::move(data_part_names_)), data(data_) 
    { 
        for (const auto & data_part : data_part_names)
        {
            LOG_DEBUG(&Poco::Logger::get("vectorIndexEntry"), "[constructor] currently_vector_indexing_parts add: {}", data_part);
            data.currently_vector_indexing_parts.insert(data_part);
        }
    }

    ~VectorIndexEntry() 
    {
        std::lock_guard lock(data.currently_processing_in_background_mutex);
        for (const auto & data_part : data_part_names)
        {
            LOG_DEBUG(&Poco::Logger::get("vectorIndexEntry"), "[deconstructor] currently_vector_indexing_parts remove: {}", data_part);
            data.currently_vector_indexing_parts.erase(data_part);
        } 
    }
};

using VectorIndexEntryPtr = std::shared_ptr<VectorIndexEntry>;

}
