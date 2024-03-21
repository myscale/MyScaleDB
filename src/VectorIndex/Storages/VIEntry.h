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

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Common/logger_useful.h>

#include <VectorIndex/Interpreters/VIEventLog.h>

namespace DB
{

struct VIDescription;
class MergeTreeDataPartColumnIndex;
using MergeTreeDataPartColumnIndexPtr = std::shared_ptr<MergeTreeDataPartColumnIndex>;


struct VIContext
{
    StorageMetadataPtr metadata_snapshot;
    VIDescription vec_index_desc{VIDescription()};
    MergeTreeData::DataPartPtr source_part;
    MergeTreeDataPartColumnIndexPtr source_column_index;
    VIVariantPtr build_index;
    ActionBlocker * builds_blocker;
    String part_name;
    String vector_index_name;
    String vector_tmp_full_path;
    String vector_tmp_relative_path;
    std::shared_ptr<MergeTreeDataPartChecksums> vector_index_checksum;

    Poco::Logger * log{nullptr};

    Stopwatch watch = Stopwatch();

    scope_guard temporary_directory_lock;

    IndexBuildMemoryUsageHelperPtr index_build_memory_lock;

    std::function<void(const String &)> clean_tmp_folder_callback = {};

    std::function<bool()> build_cancel_callback = {};

    std::function<void(VIEventLogElement::Type, int, const String &)> write_event_log = {};

    bool slow_mode{false};

    const int maxBuildRetryCount = 3;

    int failed_count{0};
};

struct VIEntry
{
    String part_name;
    String vector_index_name;
    MergeTreeData & data;
    bool is_replicated;
    Poco::Logger * log = &Poco::Logger::get("VectorIndexEntry");

    VIEntry(const String part_name_, const String & index_name_, MergeTreeData & data_, const bool is_replicated_)
     : part_name(std::move(part_name_))
     , vector_index_name(index_name_)
     , data(data_)
     , is_replicated(is_replicated_)
    {
        /// Replicated merge tree cases will do the add when create log entry is sucessfull or pull log entry.
        if (!is_replicated)
        {
            LOG_DEBUG(log, "currently_vector_indexing_parts add: {}", part_name);
            std::lock_guard lock(data.currently_vector_indexing_parts_mutex);
            data.currently_vector_indexing_parts.insert(part_name);
        }
    }

    ~VIEntry()
    {
        /// VIEntry will be distroyed for replicated merge tree after create log entry.
        if (!is_replicated)
        {
            LOG_DEBUG(log, "currently_vector_indexing_parts remove: {}", part_name);

            std::lock_guard lock(data.currently_vector_indexing_parts_mutex);
            data.currently_vector_indexing_parts.erase(part_name);
        }
    }
};

using VIEntryPtr = std::shared_ptr<VIEntry>;

}
