#pragma once

#include <functional>

#include <Core/Names.h>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeVectorIndexBuilderUpdater.h>
#include <Storages/MergeTree/VectorIndexEntry.h>

#include <Common/logger_useful.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class MergeTreeData;

class VectorIndexMergeTreeTask : public IExecutableTask
{
public:
    template <class Callback>
    VectorIndexMergeTreeTask(
        MergeTreeData & storage_,
        StorageMetadataPtr metadata_snapshot_,
        VectorIndexEntryPtr vector_index_entry_,
        MergeTreeVectorIndexBuilderUpdater & builder_,
        Callback && task_result_callback_, 
        bool slow_mode_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , vector_index_entry(std::move(vector_index_entry_))
        , builder(builder_)
        , task_result_callback(std::forward<Callback>(task_result_callback_))
        , slow_mode(slow_mode_)
        , log(&Poco::Logger::get("VectorIndexMergeTreeTask"))
    {
        LOG_DEBUG(log, "Create VectorIndexMergeTreeTask for {}, slow mode: {}", vector_index_entry->part_name, slow_mode);
    }

    bool executeStep() override;
    StorageID getStorageID() override;
    UInt64 getPriority() override;
    void onCompleted() override;

    ~VectorIndexMergeTreeTask() override;

private:
    MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;
    VectorIndexEntryPtr vector_index_entry;
    MergeTreeVectorIndexBuilderUpdater & builder;
    std::unique_ptr<Stopwatch> stopwatch;

    IExecutableTask::TaskResultCallback task_result_callback;

    ContextMutablePtr fake_query_context;

    bool slow_mode;

    Poco::Logger * log;
};

}
