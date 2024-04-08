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

#include <atomic>
#include <functional>
#include <map>
#include <mutex>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Common/logger_useful.h>
#include <Common/ActionBlocker.h>

#include <VectorIndex/Common/VICommands.h>
#include <VectorIndex/Common/VIWithDataPart.h>
#include <VectorIndex/Common/VectorDataset.h>

namespace DB
{
struct VIEntry;
using VIEntryPtr = std::shared_ptr<VIEntry>;
struct VIContext;
using VIContextPtr = std::shared_ptr<VIContext>;

struct VIBuiltStatus
{
    enum Status
    {
        NO_DATA_PART = 0,
        SUCCESS = 1,
        BUILD_FAIL = 2,
        META_ERROR = 3,
        MISCONFIGURED = 4,
        BUILD_SKIPPED = 5,      /// Index already in built, No need to build vector index for this part
        BUILD_CANCEL = 6,       /// cancel build index
        BUILD_RETRY = 7,        /// Retry to move vector index files to part directory
    };
    Status getStatus() { return status; }

    String statusToString()
    {
        switch (status)
        {
            case Status::NO_DATA_PART:          return "no_data_part";
            case Status::SUCCESS:               return "success";
            case Status::BUILD_FAIL:            return "build_fail";
            case Status::META_ERROR:            return "meta_error";
            case Status::MISCONFIGURED:         return "miss_configured";
            case Status::BUILD_SKIPPED:         return "skipped";
            case Status::BUILD_CANCEL:          return "build_fail";
            case Status::BUILD_RETRY:           return "build_retry";
        }
        UNREACHABLE();
    }

    Status status;
    int err_code{0};
    String err_msg{""};
};

class VIBuilderUpdater
{
public:
    VIBuilderUpdater(MergeTreeData & data_);

    /// Check backgroud pool size for vector index if new log entry is allowed.
    /// True if allowed to select part for build vector index.
    bool allowToBuildVI(const bool slow_mode, const size_t builds_count_in_queue) const;

    /// Select a part without vector index to build vector index
    /// In multiple vectors case, choose a part without a vector index in metadata.
    VIEntryPtr selectPartToBuildVI(
        const StorageMetadataPtr & metadata_snapshot,
        bool select_slow_mode_part);

    /// throw exception when check can build failed
    void checkPartCanBuildVI(MergeTreeDataPartPtr data_part, const VIDescription & vec_desc);

    void removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot);

    /// handle build index task
    VIBuiltStatus buildVI(const VIContextPtr ctx);

    VIContextPtr prepareBuildVIContext(
        const StorageMetadataPtr & metadata_snapshot,
        const String & part_name,
        const String & vector_index_name,
        bool slow_mode,
        bool is_replicated_vector_task = false);
    
    /// Find furture part and do some checks before move vector index files.
    /// Used for first build or retry to move if first build failed to move.
    VIBuiltStatus TryMoveVIFiles(const VIContextPtr ctx);


    /** Is used to cancel all index builds. On cancel() call all currently running actions will throw exception soon.
      * All new attempts to start a vector index build will throw an exception until all 'LockHolder' objects will be destroyed.
      */
    ActionBlocker builds_blocker;

private:
    MergeTreeData & data;
    bool is_replicated = false; /// Mark if replicated
    //const size_t background_pool_size;

    Poco::Logger * log;

    time_t last_cache_check_time = 0;

    VIBuiltStatus buildVIForOnePart(VIContextPtr ctx);

    /// Move build vector index files from temporary directory to data part directory, and apply lightweight delete if needed.
    /// And finally write vector index checksums file.
    void moveVIFilesToFuturePartAndCache(
        const MergeTreeDataPartPtr & dest_part,
        DiskPtr disk,
        VIVariantPtr build_index,
        const String & vector_tmp_relative_path, 
        const VIDescription & vec_index_desc,
        const std::shared_ptr<MergeTreeDataPartChecksums> & checksums);

    bool isSlowModePart(const MergeTreeDataPartPtr & part, const String & index_name)
    {
        /// Smaller part built with single vector index is also treated as slow mode.
        auto column_index = part->vector_index.getColumnIndex(index_name).value();
        
        return column_index->isDecoupleIndexFile() || part->rows_count < data.getSettings()->max_rows_for_slow_mode_single_vector_index_build;
    }
};

}
