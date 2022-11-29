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
#include <VectorIndex/MergeUtils.h>
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
        size_t max_parts_number,
        bool select_slow_mode_parts,
        const MergeTreeData::DataParts & currently_merging_mutating_parts = {});

    void removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot);

    /// handle build index task
    BuildVectorIndexStatus
    buildVectorIndex(const StorageMetadataPtr & metadata_snapshot, const std::vector<String> & part_names, bool tune, bool slow_mode);

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

    time_t last_cache_check_time = 0;

    BuildVectorIndexStatus
    buildVectorIndexForOnePart(const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part, bool tune, bool slow_mode);

    /// Move build vector index files from temporary directory to data part directory, and apply lightweight delete if needed.
    bool moveVectorIndexFilesToFuturePart(const StorageMetadataPtr & metadata_snapshot, const  String & vector_tmp_relative_path, const MergeTreeDataPartPtr & dest_part);

    void undoBuildVectorIndexForOnePart(const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part);

    bool isSlowModePart(const MergeTreeDataPartPtr & part)
    {
        /// Smaller part built with single vector index is also treated as slow mode.
        return part->containRowIdsMaps() || part->rows_count < data.getSettings()->max_rows_for_slow_mode_single_vector_index_build;
    }
};

}
