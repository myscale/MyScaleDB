#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Common/logger_useful.h>

namespace DB
{

struct VectorIndexEntry
{
    String part_name;
    String vector_index_name;
    MergeTreeData & data;
    bool is_replicated;

    VectorIndexEntry(const String part_name_, const String & index_name_, MergeTreeData & data_, const bool is_replicated_)
     : part_name(std::move(part_name_))
     , vector_index_name(index_name_)
     , data(data_)
     , is_replicated(is_replicated_)
    {
        LOG_DEBUG(&Poco::Logger::get("vectorIndexEntry"), "[constructor] currently_vector_indexing_parts add: {}", part_name);
        /// insert for replicated merge tree to avoid creating log entry multiple times for the same part.
        std::lock_guard lock(data.currently_vector_indexing_parts_mutex);
        data.currently_vector_indexing_parts.insert(part_name);
    }

    ~VectorIndexEntry() 
    {
        /// VectorIndexEntry will be distroyed for replicated merge tree after create log entry.
        if (!is_replicated)
        {
            LOG_DEBUG(&Poco::Logger::get("vectorIndexEntry"), "[deconstructor] currently_vector_indexing_parts remove: {}", part_name);

            std::lock_guard lock(data.currently_vector_indexing_parts_mutex);
            data.currently_vector_indexing_parts.erase(part_name);
        }
    }
};

using VectorIndexEntryPtr = std::shared_ptr<VectorIndexEntry>;

}
