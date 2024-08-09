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

#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <VectorIndex/Cache/PKCacheManager.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <Storages/SelectQueryInfo.h>

#include <Common/logger_useful.h>

#include <SearchIndex/Common/DenseBitmap.h>
#include <VectorIndex/Cache/PKCacheManager.h>

namespace DB
{
class MergeTreeSelectWithHybridSearchProcessor final : public MergeTreeSelectAlgorithm
{
public:
    using ReadRange = MergeTreeRangeReader::ReadResult::ReadRangeInfo;
    using ReadRanges = MergeTreeRangeReader::ReadResult::ReadRangesInfo;

    template <typename... Args>
    explicit MergeTreeSelectWithHybridSearchProcessor(MergeTreeBaseSearchManagerPtr base_search_manager_, ContextPtr context_, size_t max_streams, Args &&... args)
        : MergeTreeSelectAlgorithm{std::forward<Args>(args)...}
        , base_search_manager(base_search_manager_)
        , context(context_)
        , max_streams_for_prewhere(max_streams)
    {
        LOG_TRACE(
            log,
            "Reading {} ranges in order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(),
            data_part->name,
            total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
    }

    String getName() const override { return "MergeTreeReadWithHybridSearch"; }

    /// Execute vector scan, text or hybrid search on all parts
    /// For two stage search cases, execute first stage vector scan.
    static VectorAndTextResultInDataParts selectPartsByVectorAndTextIndexes(
        const RangesInDataParts & parts_with_range,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const bool support_two_stage_search,
#if USE_TANTIVY_SEARCH
        const Statistics & bm25_stats_in_table,
#endif
        const PrewhereInfoPtr & prewhere_info,
        StorageSnapshotPtr storage_snapshot,
        ContextPtr context,
        size_t max_block_size,
        size_t num_streams,
        const MergeTreeData & data,
        const MergeTreeReaderSettings & reader_settings);

protected:
    BlockAndProgress readFromPart() override;
    void initializeReaders();

    /// Sets up range readers corresponding to data readers
    void initializeRangeReadersWithHybridSearch(MergeTreeReadTask & task);

    bool readPrimaryKeyBin(Columns & out_columns);

private:
    bool getNewTaskImpl() override;
    void finalizeNewTask() override {}

    BlockAndProgress readFromPartWithHybridSearch();
    BlockAndProgress readFromPartWithPrimaryKeyCache(bool & success);

    /// Evaluate the prehwere condition with the partition key value in part. Similar as PartitionPruner
    static bool canSkipPrewhereForPart(
        const MergeTreeData::DataPartPtr & data_part_,
        const PrewhereInfoPtr & prewhere_info_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot,
        const ContextPtr context_);

    Poco::Logger * log = &Poco::Logger::get("MergeTreeSelectWithHybridSearchProcessor");

    void executeSearch(MarkRanges mark_ranges);

    /// Used for hybrid search refactor, can execute search with provided manager for where or no-where cases
    static void executeSearch(
        MergeTreeBaseSearchManagerPtr search_manager,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeData::DataPartPtr & data_part_,
        const AlterConversionsPtr & alter_conversions_,
        size_t max_block_size,
        UInt64 preferred_block_size_bytes_,
        UInt64 preferred_max_column_in_block_size_bytes_,
        MarkRanges mark_ranges,
        const PrewhereInfoPtr & prewhere_info_copy,
        const MergeTreeReaderSettings & reader_settings_,
        bool use_uncompressed_cache_,
        ContextPtr context_,
        size_t max_streams);

    /// Peform prefilter on provided data part and mark ranges
    static VIBitmapPtr performPrefilter(
        MarkRanges mark_ranges,
        const PrewhereInfoPtr & prewhere_info_copy,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeData::DataPartPtr & data_part_,
        const AlterConversionsPtr & alter_conversions_,
        size_t max_block_size,
        UInt64 preferred_block_size_bytes_,
        UInt64 preferred_max_column_in_block_size_bytes_,
        const MergeTreeReaderSettings & reader_settings_,
        bool use_uncompressed_cache_,
        ContextPtr context_,
        size_t max_streams);

    /// Shared_ptr for base class, the dynamic type may be derived class TextSearch/VectorScan/HybridSearch
    MergeTreeBaseSearchManagerPtr base_search_manager = nullptr;

    ContextPtr context;
    size_t max_streams_for_prewhere;

    /// True if _part_offset column is added for vector scan, but should not exist in select result.
    bool need_remove_part_offset = false;

    /// Logic row id for rows, used for vector index scan.
    const ColumnUInt64 * part_offset = nullptr;

    /// True if the query can use primary key cache.
    bool use_primary_key_cache = false;

    /// Used for cases when prewhere can be skipped before vector search
    /// True when performPreFilter() is skipped, prewhere_info can be performed after vector search, during reading other columns.
    /// False when performPreFilter() is executed before vector search
    bool can_skip_peform_prefilter = false;
};

}
