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

#include <functional>

#include <Core/Names.h>

#include <VectorIndex/Storages/VectorIndexBuilderUpdater.h>
#include <VectorIndex/Storages/VectorIndexTaskBase.h>

#include <Common/logger_useful.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class MergeTreeData;

class VectorIndexTask : public VectorIndexTaskBase
{
public:
    template <class Callback>
    VectorIndexTask(
        MergeTreeData & storage_,
        StorageMetadataPtr /*metadata_snapshot_*/,
        VectorIndexEntryPtr vector_index_entry_,
        VectorIndexBuilderUpdater & builder_,
        Callback && task_result_callback_,
        bool slow_mode_)
        : VectorIndexTaskBase(
            storage_, builder_, task_result_callback_, vector_index_entry_->part_name, vector_index_entry_->vector_index_name, slow_mode_)
        , vector_index_entry(std::move(vector_index_entry_))
    {
        log = &Poco::Logger::get("VectorIndexTask");
        LOG_DEBUG(log, "Create VectorIndexTask for {}, slow mode: {}", vector_index_entry->part_name, slow_mode);
    }

    ~VectorIndexTask() override;

private:
    VectorIndexEntryPtr vector_index_entry;

    void recordBuildStatus();

    BuildVectorIndexStatus prepare() override;
};
}
