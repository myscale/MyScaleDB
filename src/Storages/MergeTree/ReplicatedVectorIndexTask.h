#pragma once

#include <functional>

#include <Core/Names.h>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeVectorIndexBuilderUpdater.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/logger_useful.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class StorageReplicatedMergeTree;

class ReplicatedVectorIndexTask : public IExecutableTask
{
public:
    template <class Callback>
    ReplicatedVectorIndexTask(
        StorageReplicatedMergeTree & storage_,
        ReplicatedMergeTreeQueue::SelectedEntryPtr & selected_entry_,
        MergeTreeVectorIndexBuilderUpdater & builder_,
        Callback && task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(storage.getInMemoryMetadataPtr())
        , selected_entry(selected_entry_)
        , entry(*selected_entry->log_entry)
        , builder(builder_)
        , log(&Poco::Logger::get("ReplicatedVectorIndexTask"))
        , task_result_callback(std::forward<Callback>(task_result_callback_))
    {
    }

    bool executeStep() override;
    StorageID getStorageID() override;
    UInt64 getPriority() override { return priority; }
    void onCompleted() override;

    ~ReplicatedVectorIndexTask() override;

private:
    /// result, need_to_fetch
    std::pair<bool, bool> prepare();

    UInt64 priority{0};

    StorageReplicatedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry;
    ReplicatedMergeTreeLogEntry & entry;
    MergeTreeVectorIndexBuilderUpdater & builder;
    std::unique_ptr<Stopwatch> stopwatch;
    Poco::Logger * log;

    MergeTreeData::DataPartPtr source_part{nullptr};
    BuildVectorIndexStatus build_status;
    String replica_to_fetch;

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE_BUILD_VECTOR_INDEX,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    IExecutableTask::TaskResultCallback task_result_callback;

    ContextMutablePtr fake_query_context;
};

}
