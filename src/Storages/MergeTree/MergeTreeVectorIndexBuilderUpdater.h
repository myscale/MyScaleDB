#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>

#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/VectorIndexEntry.h>
#include <Storages/VectorIndexCommands.h>
#include <VectorIndex/Dataset.h>
#include <VectorIndex/VectorSegmentExecutor.h>
#include <VectorIndex/Status.h>
#include <VectorIndex/VectorIndexFactory.h>
#include <Common/logger_useful.h>
#include <Common/ActionBlocker.h>

namespace DB
{

enum class BuildVectorIndexStatus
{
    NO_DATA_PART = 0,
    SUCCESS = 1,
    BUILD_FAIL = 2,
    META_ERROR = 3,
    MISCONFIGURED = 4,
};

class MergeTreeVectorIndexBuilderUpdater
{
public:
    MergeTreeVectorIndexBuilderUpdater(MergeTreeData & data_);

    /// select parts which vector_indexed not containing index names to build vector index
    VectorIndexEntryPtr selectPartsToBuildVectorIndex(
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData::DataParts & currently_vector_indexing_parts,
        size_t background_vector_pool_size);

    void removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot);

    /// handle build index task
    BuildVectorIndexStatus
    buildVectorIndex(const StorageMetadataPtr & metadata_snapshot, const std::vector<MergeTreeDataPartPtr> & parts, bool tune);

private:
    class Counter
    {
    public:
        Counter() = default;
        void put(const String & key, int value);
        int get(const String & key);
        int increaseAndGet(const String & key);
    private:
        std::map<String, int> counter_;
        std::mutex mu_;
    };

    Counter counter;

    MergeTreeData & data;
    //const size_t background_pool_size;

    Poco::Logger * log;

    BuildVectorIndexStatus
    buildVectorIndexForOnePart(const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part, bool tune);

    void undoBuildVectorIndexForOnePart(const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part);
};

}
