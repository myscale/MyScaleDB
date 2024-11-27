#pragma once

#include <functional>

#include <Core/Names.h>

#include <Storages/MergeTree/IExecutableTask.h>
#include <VectorIndex/Common/VectorIndicesMgr.h>
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
        VectorIndicesMgr & builder_,
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
    StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + part_name + "::" + vector_index_name; }
    void onCompleted() override;

    ~VITaskBase() override;

protected:
    void recordBuildStatus();

    virtual VectorIndex::SegmentBuiltStatus prepare() { return VectorIndex::SegmentBuiltStatus{VectorIndex::SegmentBuiltStatus::SUCCESS}; }

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
    VectorIndicesMgr & builder;
    std::unique_ptr<Stopwatch> stopwatch;
    VectorIndex::SegmentBuiltStatus build_status{VectorIndex::SegmentBuiltStatus::SUCCESS};
    IExecutableTask::TaskResultCallback task_result_callback;
    const String part_name;
    const String vector_index_name;

    VIContextPtr ctx;

    ContextMutablePtr fake_query_context;

    bool slow_mode;

    State state{State::NEED_PREPARE};
    Priority priority{0};

    LoggerPtr log;
};
}
