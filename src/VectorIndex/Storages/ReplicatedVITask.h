#pragma once

#include <functional>

#include <Core/Names.h>

#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Common/VectorIndicesMgr.h>
#include <VectorIndex/Storages/VITaskBase.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class StorageReplicatedMergeTree;

class ReplicatedVITask : public VITaskBase
{
public:
    template <class Callback>
    ReplicatedVITask(
        StorageReplicatedMergeTree & storage_,
        ReplicatedMergeTreeQueue::SelectedEntryPtr & selected_entry_,
        VectorIndicesMgr & builder_,
        Callback && task_result_callback_)
        : VITaskBase(
            storage_,
            builder_,
            task_result_callback_,
            selected_entry_->log_entry->source_parts.at(0),
            selected_entry_->log_entry->index_name,
            selected_entry_->log_entry->slow_mode)
        , selected_entry(selected_entry_)
        , entry(*selected_entry->log_entry)
    {
        log = getLogger("ReplicatedVITask");
    }

    ~ReplicatedVITask() override;

private:
    /// result, need_to_fetch
    VectorIndex::SegmentBuiltStatus prepare() override;

    void remove_processed_entry() override;

    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry;
    ReplicatedMergeTreeLogEntry & entry;

    String replica_to_fetch;
};

}
