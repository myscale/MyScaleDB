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

#include <Storages/MergeTree/IExecutableTask.h>
#include <VectorIndex/Storages/VectorIndexBuilderUpdater.h>
#include <VectorIndex/Storages/VectorIndexEntry.h>

#include <Common/logger_useful.h>


namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct VectorIndexContext;
using VectorIndexContextPtr = std::shared_ptr<VectorIndexContext>;

class VectorIndexTaskBase : public IExecutableTask
{
public:
    template <class Callback>
    VectorIndexTaskBase(
        MergeTreeData & storage_,
        VectorIndexBuilderUpdater & builder_,
        Callback && task_result_callback_,
        const String & part_name_,
        const String & vector_index_name_,
        bool slow_mode_)
        : storage(storage_)
        , metadata_snapshot(storage.getInMemoryMetadataPtr())
        , builder(builder_)
        , task_result_callback(std::forward<Callback>(task_result_callback_))
        , part_name(part_name_)
        , vector_index_name(vector_index_name_)
        , slow_mode(slow_mode_)
    {
    }

    bool executeStep() override;
    StorageID getStorageID() override;
    UInt64 getPriority() override { return priority; }
    void onCompleted() override;

    ~VectorIndexTaskBase() override;

protected:
    void recordBuildStatus();

    virtual BuildVectorIndexStatus prepare() { return BuildVectorIndexStatus{BuildVectorIndexStatus::SUCCESS}; }

    virtual void remove_processed_entry() { }

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE_BUILD_VECTOR_INDEX,
        NEED_MOVE_VECTOR_INDEX,
        NEED_FINALIZE,
        SUCCESS
    };

    State getNextState();

    MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;
    VectorIndexBuilderUpdater & builder;
    std::unique_ptr<Stopwatch> stopwatch;
    BuildVectorIndexStatus build_status{BuildVectorIndexStatus::SUCCESS};
    IExecutableTask::TaskResultCallback task_result_callback;
    const String part_name;
    const String vector_index_name;

    VectorIndexContextPtr ctx;

    ContextMutablePtr fake_query_context;

    bool slow_mode;

    State state{State::NEED_PREPARE};
    UInt64 priority{0};

    Poco::Logger * log;
};
}
