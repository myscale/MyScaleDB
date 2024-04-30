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
#include <VectorIndex/Storages/VIBuilderUpdater.h>
#include <VectorIndex/Storages/VIEntry.h>

#include <Common/logger_useful.h>


namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct VIContext;
using VIContextPtr = std::shared_ptr<VIContext>;

class VITaskBase : public IExecutableTask
{
public:
    template <class Callback>
    VITaskBase(
        MergeTreeData & storage_,
        VIBuilderUpdater & builder_,
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

    ~VITaskBase() override;

protected:
    void recordBuildStatus();

    virtual VIBuiltStatus prepare() { return VIBuiltStatus{VIBuiltStatus::SUCCESS}; }

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
    VIBuilderUpdater & builder;
    std::unique_ptr<Stopwatch> stopwatch;
    VIBuiltStatus build_status{VIBuiltStatus::SUCCESS};
    IExecutableTask::TaskResultCallback task_result_callback;
    const String part_name;
    const String vector_index_name;

    VIContextPtr ctx;

    ContextMutablePtr fake_query_context;

    bool slow_mode;

    State state{State::NEED_PREPARE};
    UInt64 priority{0};

    Poco::Logger * log;
};
}
