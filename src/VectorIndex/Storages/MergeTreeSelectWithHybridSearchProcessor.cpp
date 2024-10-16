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

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/scope_guard_safe.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeThreadSelectProcessor.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Storages/MergeTreeSelectWithHybridSearchProcessor.h>
#include <VectorIndex/Storages/MergeTreeHybridSearchManager.h>
#include <VectorIndex/Storages/MergeTreeTextSearchManager.h>
#include <VectorIndex/Utils/VSUtils.h>
#include <VectorIndex/Cache/PKCacheManager.h>

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectHybridSearchThreads;
    extern const Metric MergeTreeDataSelectHybridSearchThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

/// Check if only select primary key column, _part_offset and vector search/text search/hybrid search functions. 
static bool isHybridSearchByPk(const std::vector<String> & pk_col_names, const std::vector<String> & read_col_names)
{
    size_t pk_col_nums = pk_col_names.size();
    size_t read_col_nums = read_col_names.size();

    /// Currently primary key cache support only one column in PK.
    if (read_col_nums <= pk_col_nums || pk_col_nums > 1)
        return false;

    const String pk_col_name = pk_col_names[0];

    bool match = true;
    for (const auto & read_col_name : read_col_names)
    {
        if ((read_col_name == pk_col_name) || read_col_name == "_part_offset"
            || isHybridSearchFunc(read_col_name) || isScoreColumnName(read_col_name))
            continue;
        else
        {
            match = false;
            break;
        }
    }

    return match;
}

/// Referenced from MergeTreeSelectProcessor::initializeReaders()
void MergeTreeSelectWithHybridSearchProcessor::initializeReaders()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::initializeReaders()");

    /// Special handling of partition key condition in prehwere condition
    can_skip_peform_prefilter = canSkipPrewhereForPart(data_part, prewhere_info, storage,
                                    storage_snapshot->getMetadataForQuery(), context);

    if (can_skip_peform_prefilter)
    {
        LOG_DEBUG(log, "Skip to call performPrefilter() for part {} due to a prewhere condition with partition key is true.", data_part->name);

        /// Normal as regular read
        task_columns = getReadTaskColumns(
            LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions), storage_snapshot,
            required_columns, virt_column_names, prewhere_info, actions_settings, reader_settings, /*with_subcolumns=*/ true);
    }
    else
    {
        task_columns = getReadTaskColumns(
            LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions), storage_snapshot,
            required_columns, virt_column_names, /*prewhere_info*/ nullptr, actions_settings, reader_settings, /*with_subcolumns=*/ true);
    }

    /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (use_uncompressed_cache)
        owned_uncompressed_cache = context->getUncompressedCache();

    owned_mark_cache = context->getMarkCache();

    /// Referenced from MergeTreeBaseSelectProcessor::initializeMergeTreeReadersForPart()
    const auto & metadata_snapshot = storage_snapshot->getMetadataForQuery();

    reader = data_part->getReader(task_columns.columns, metadata_snapshot,
        all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), alter_conversions, reader_settings,
        {}, {});

    /// Referenced from IMergeTreeSelectAlgorithm::initializeMergeTreePreReadersForPart()
    pre_reader_for_step.clear();

    /// Add lightweight delete filtering step
    if (reader_settings.apply_deleted_mask && data_part->hasLightweightDelete())
    {
        pre_reader_for_step.push_back(data_part->getReader({LightweightDeleteDescription::FILTER_COLUMN}, metadata_snapshot,
            all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), alter_conversions, reader_settings,
            {}, {}));
    }

    /// Need to apply prewhere if performPrefilter() is skipped
    if (prewhere_info && can_skip_peform_prefilter)
    {
        for (const auto & pre_columns_per_step : task_columns.pre_columns)
        {
            pre_reader_for_step.push_back(
                data_part->getReader(
                    pre_columns_per_step, metadata_snapshot, all_mark_ranges,
                    owned_uncompressed_cache.get(), owned_mark_cache.get(),
                    alter_conversions, reader_settings, {}, {}));
        }
    }
}

/// Referenced from IMergeTreeSelectAlgorithm::initializeRangeReaders()
void MergeTreeSelectWithHybridSearchProcessor::initializeRangeReadersWithHybridSearch(MergeTreeReadTask & current_task)
{
    bool has_lightweight_delete = current_task.data_part->hasLightweightDelete();

    /// Initialize primary key cache
    const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
    const bool enable_primary_key_cache = current_task.data_part->storage.canUsePrimaryKeyCache();
    LOG_DEBUG(log, "Reader setting: enable_primary_key_cache = {}", enable_primary_key_cache);

    /// consider cache if and only if
    /// 1. this task is vector search and no prewhere info
    /// 2. primary key is only a column, and select columns are (pk, hybrid_search_func)
    /// 3. primary key's value is represented by number
    use_primary_key_cache = enable_primary_key_cache && PKCacheManager::isSupportedPrimaryKey(primary_key)
            && isHybridSearchByPk(primary_key.column_names, ordered_names);

    /// Add _part_offset to non_const_virtual_column_names if part has lightweight delete
    /// prewhere info will be apply on read result
    if (has_lightweight_delete || can_skip_peform_prefilter)
    {
        bool found = false;
        for (const auto & column_name : non_const_virtual_column_names)
        {
            if (column_name == "_part_offset")
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            non_const_virtual_column_names.emplace_back("_part_offset");
            need_remove_part_offset = true;
        }
    }

    auto & range_reader = current_task.range_reader;
    auto & pre_range_readers = current_task.pre_range_readers;

    MergeTreeRangeReader* prev_reader = nullptr;
    bool last_reader = false;
    size_t pre_readers_shift = 0;

    /// Add filtering step with lightweight delete mask
    if (reader_settings.apply_deleted_mask && has_lightweight_delete)
    {
        MergeTreeRangeReader pre_range_reader(pre_reader_for_step[0].get(), prev_reader, &lightweight_delete_filter_step, last_reader, non_const_virtual_column_names);
        pre_range_readers.push_back(std::move(pre_range_reader));
        prev_reader = &pre_range_readers.back();
        pre_readers_shift++;
    }

    if (prewhere_info && can_skip_peform_prefilter)
    {
        if (prewhere_actions->steps.size() + pre_readers_shift != pre_reader_for_step.size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "PREWHERE steps count mismatch, actions: {}, readers: {}",
                prewhere_actions->steps.size(), pre_reader_for_step.size());
        }

        for (size_t i = 0; i < prewhere_actions->steps.size(); ++i)
        {
            last_reader = reader->getColumns().empty() && (i + 1 == prewhere_actions->steps.size());

            MergeTreeRangeReader current_reader(pre_reader_for_step[i + pre_readers_shift].get(), prev_reader, &prewhere_actions->steps[i], last_reader, non_const_virtual_column_names);

            pre_range_readers.push_back(std::move(current_reader));
            prev_reader = &pre_range_readers.back();
        }
    }

    range_reader = MergeTreeRangeReader(reader.get(), prev_reader, nullptr, true, non_const_virtual_column_names);
}

bool MergeTreeSelectWithHybridSearchProcessor::canSkipPrewhereForPart(
    const MergeTreeData::DataPartPtr & data_part_,
    const PrewhereInfoPtr & prewhere_info_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr context_)
{
    if (!prewhere_info_ || !metadata_snapshot->hasPartitionKey() || !prewhere_info_->prewhere_actions)
        return false;

    if (!context_->getSettingsRef().optimize_prefilter_in_search)
        return false;

    const auto & partition_key = metadata_snapshot->getPartitionKey();
    const auto & prewhere_actions = prewhere_info_->prewhere_actions;

    /// Get column names required for partition key
    auto minmax_columns_names = storage_.getMinMaxColumnsNames(partition_key);

    /// Quick check if requried column names has any partition key column name
    /// Get column names in prewhere condition
    auto required_columns = prewhere_actions->getRequiredColumnsNames();
    bool exists = false;

    for (const auto & required_column : required_columns)
    {
        if (std::find(minmax_columns_names.begin(), minmax_columns_names.end(), required_column)
            != minmax_columns_names.end())
        {
            exists = true;
            break;
        }
    }

    /// Not found partition key column in prewhere, cannot skip.
    if (!exists)
        return false;

    /// Reference PartitionPrunner using KeyCondition, difference is that FUNCTION_UNKNOWN returns false.
    KeyCondition partition_prunner_condition(
        prewhere_actions, context_, partition_key.column_names,
        partition_key.expression, {}, true /* single_point */, false, true /* known_false */);

    const auto & partition_value = data_part_->partition.value;
    std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
    for (auto & field : index_value)
    {
        // NULL_LAST
        if (field.isNull())
            field = POSITIVE_INFINITY;
    }

    if (partition_prunner_condition.mayBeTrueInRange(
        partition_value.size(), index_value.data(), index_value.data(), partition_key.data_types))
        return true;

    /// Try minmax idx on columns required by partition key
    auto minmax_expression_actions = storage_.getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(context_));
    DataTypes minmax_columns_types = storage_.getMinMaxColumnsTypes(partition_key);

    KeyCondition minmax_idx_condition(
        prewhere_actions, context_, minmax_columns_names,
        minmax_expression_actions, {}, false /* single_point */, false, true /* known_false */);

    return minmax_idx_condition.checkInHyperrectangle(data_part_->minmax_idx->hyperrectangle, minmax_columns_types).can_be_true;
}


bool MergeTreeSelectWithHybridSearchProcessor::readPrimaryKeyBin(Columns & out_columns)
{
    const KeyDescription & primary_key = storage_snapshot->metadata->getPrimaryKey();
    const size_t pk_columns_size = primary_key.column_names.size();

    NamesAndTypesList cols;
    const std::vector<String> pk_column_names = primary_key.column_names;
    for (const String & col_name : pk_column_names)
    {
        std::optional<NameAndTypePair> column_with_type = storage_snapshot->metadata->getColumns().getAllPhysical().tryGetByName(col_name);
        if (column_with_type)
            cols.emplace_back(*column_with_type);
    }
    const size_t cols_size = cols.size();

    if (pk_columns_size == 0 || pk_columns_size != cols_size)
    {
        LOG_ERROR(log, "pk_columns_size = {}, cols_size = {}", pk_columns_size, cols_size);
        return false;
    }

    MutableColumns buffered_columns;
    buffered_columns.resize(cols_size);
    for (size_t i = 0; i < cols_size; ++i)
    {
        buffered_columns[i] = primary_key.data_types[i]->createColumn();
    }

    MergeTreeReaderPtr reader = task->data_part->getReader(
        cols,
        storage_snapshot->metadata,
        MarkRanges{MarkRange(0, task->data_part->getMarksCount())},
        nullptr,
        context->getMarkCache().get(),
        alter_conversions,
        reader_settings,
        {},
        {});

    if (!reader)
    {
        LOG_ERROR(log, "Failed to get reader");
        return false;
    }

    /// begin to read
    const MergeTreeIndexGranularity & index_granularity = task->data_part->index_granularity;

    size_t current_mark = 0;
    const size_t total_mark = task->data_part->getMarksCount();

    size_t num_rows_read = 0;
    const size_t num_rows_total = task->data_part->rows_count;

    bool continue_read = false;

    while (num_rows_read < num_rows_total)
    {
        size_t remaining_size = num_rows_total - num_rows_read;

        Columns result;
        result.resize(cols_size);

        size_t num_rows = reader->readRows(current_mark, 0, continue_read, remaining_size, result);

        continue_read = true;
        num_rows_read += num_rows;

        for (size_t i = 0; i < cols_size; ++i)
        {
            if (result[i]->isSparse())
            {
                auto res = result[i]->convertToFullColumnIfSparse();
                buffered_columns[i]->insertRangeFrom(*res, 0, result[i]->size());
            }
            else
                buffered_columns[i]->insertRangeFrom(*result[i], 0, result[i]->size());
        }

        /// calculate next mark
        for (size_t mark = 0; mark < total_mark - 1; ++mark)
        {
            if (index_granularity.getMarkStartingRow(mark) >= num_rows_read
                && index_granularity.getMarkStartingRow(mark + 1) < num_rows_read)
            {
                current_mark = mark;
            }
        }
    }

    for (auto & buffered_column : buffered_columns)
    {
        buffered_column->protect();
    }

    LOG_DEBUG(log, "Finally, {} rows has been read", buffered_columns[0]->size());

    out_columns.assign(
        std::make_move_iterator(buffered_columns.begin()),
        std::make_move_iterator(buffered_columns.end())
    );

    return true;
}

IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithHybridSearchProcessor::readFromPart()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::readFromPart()");
    if (!task->range_reader.isInitialized())
        initializeRangeReadersWithHybridSearch(*task);

    /// original read logic, considering prewhere optimization
    return readFromPartWithHybridSearch();
}

/// perform actual read and result merge operation, prewhere has been processed ahead
/// Referenced from MergeTreeBaseSelectProcessor::readFromPartImpl()
IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithHybridSearchProcessor::readFromPartWithHybridSearch()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::readFromPartWithHybridSearch()");
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;

    auto read_start_time = std::chrono::system_clock::now();
    UInt64 rows_to_read = std::max(UInt64(1), current_max_block_size_rows);

    if (use_primary_key_cache)
    {
        bool success = false;
        auto res = readFromPartWithPrimaryKeyCache(success);
        
        if (success)
            return res;
    }

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    const auto & sample_block = task->range_reader.getSampleBlock();
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inconsistent number of columns got from MergeTreeRangeReader. Have {} in sample block and {} columns in list",
            toString(sample_block.columns()),
            toString(read_result.columns.size()));

    /// TODO: check columns have the same types as in header.

    UInt64 num_filtered_rows = read_result.numReadRows() - read_result.num_rows;

    size_t num_read_rows = read_result.numReadRows();
    size_t num_read_bytes = read_result.numBytesRead();

    auto read_ranges = read_result.readRanges();

    if (task->size_predictor)
    {
        task->size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);

        if (!read_result.columns.empty())
            task->size_predictor->update(sample_block, read_result.columns, read_result.num_rows);
    }

    if (read_result.num_rows == 0)
        return {Block(), read_result.num_rows, num_read_rows, num_read_bytes};

    /// Support multiple distance functions
    /// Remove distance_func columns from read_result.columns, it will be added by vector search.
    Columns ordered_columns;
    Names vector_scan_cols_names;
    size_t cols_size_in_sample_block = sample_block.columns();
    size_t cols_size_except_search_cols = cols_size_in_sample_block;

    if (base_search_manager)
    {
        vector_scan_cols_names = base_search_manager->getSearchFuncColumnNames();
        cols_size_except_search_cols = cols_size_in_sample_block - vector_scan_cols_names.size();
        ordered_columns.reserve(cols_size_except_search_cols);

        /// Throw exception if vector_scan_cols_names is empty
        if (vector_scan_cols_names.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find any search result column name, this should not happen");
    }
    else
        ordered_columns.reserve(cols_size_in_sample_block);

    /// sample block columns may be:
    /// part without LWD: table columns + distance_func columns + non_const_virtual_columns
    /// part with LWD: non_const_virtual_columns + table columns + distance_func columns
    /// All distances are put at the end of ordered_columns, the order of distances is same as in vector_scan_descriptions. 
    /// This vector is a map of the index of ordered_columns to the sample block
    std::vector<size_t> orig_pos_in_sample_block;
    orig_pos_in_sample_block.resize(cols_size_in_sample_block);
    size_t ordered_index = 0;

    for (size_t ps = 0; ps < sample_block.columns(); ++ps)
    {
        auto & col_name = sample_block.getByPosition(ps).name;

        /// Check if distance_func columns
        bool is_search_func = false;
        for (size_t i = 0; i < vector_scan_cols_names.size(); ++i)
        {
            if (col_name == vector_scan_cols_names[i])
            {
                orig_pos_in_sample_block[cols_size_except_search_cols+i] = ps;
                is_search_func = true;
                break;
            }
        }

        /// No need to put search func cols
        if (is_search_func)
            continue;

        ordered_columns.emplace_back(std::move(read_result.columns[ps]));
        orig_pos_in_sample_block[ordered_index] = ps;
        ordered_index++;

        /// Copy _part_offset column
        if (col_name == "_part_offset")
            part_offset = typeid_cast<const ColumnUInt64 *>(ordered_columns.back().get());
    }

    auto read_end_time = std::chrono::system_clock::now();

    LOG_DEBUG(log, "Read time: {}", std::chrono::duration_cast<std::chrono::milliseconds>(read_end_time - read_start_time).count());

    /// [MQDB] vector search
    if (base_search_manager && base_search_manager->preComputed())
    {
        /// already perform vector scan   
        base_search_manager->mergeResult(
            ordered_columns,
            read_result.num_rows,
            read_ranges, part_offset);
    }

    const size_t final_result_num_rows = read_result.num_rows;

    LOG_DEBUG(log, "mergeResult() finished with result rows: {}", final_result_num_rows);

    Block res_block;

    /// Add prewhere column name to avoid prewhere_column not found error
    /// Used for vector scan to handle cases when both prewhere and where exist
    if (!can_skip_peform_prefilter && prewhere_info && !prewhere_info->remove_prewhere_column)
    {
        ColumnWithTypeAndName prewhere_col;

        const auto & node = prewhere_info->prewhere_actions->findInOutputs(prewhere_info->prewhere_column_name);
        auto filter_type = node.result_type;

        prewhere_col.type = filter_type;
        prewhere_col.name = prewhere_info->prewhere_column_name;
        prewhere_col.column = filter_type->createColumnConst(final_result_num_rows, 1);

        res_block.insert(std::move(prewhere_col));
    }

    /// ordered_columns: non-search functions, search functions cols
    /// Use the map orig_pos_in_sample_block to get column name and type from sample block
    for (size_t i = 0; i < ordered_columns.size(); ++i)
    {
        size_t pos_in_sample = orig_pos_in_sample_block[i];

        ColumnWithTypeAndName ctn;
        ctn.column = ordered_columns[i];
        ctn.type = sample_block.getByPosition(pos_in_sample).type;
        ctn.name = sample_block.getByPosition(pos_in_sample).name;

        res_block.insert(std::move(ctn));
    }

    if (need_remove_part_offset)
    {
        res_block.erase("_part_offset");
    }

    BlockAndProgress res = {res_block, final_result_num_rows, num_read_rows, num_read_bytes};

    return res;
}

IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithHybridSearchProcessor::readFromPartWithPrimaryKeyCache(bool & success)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::readFromPartUsePrimaryKeyCache()");
    LOG_DEBUG(log, "Use primary key cache");

    const String cache_key = task->data_part->getDataPartStorage().getRelativePath() + ":" + task->data_part->name;

    std::optional<Columns> pk_cache_cols_opt = PKCacheManager::getMgr().getPartPkCache(cache_key);

    /// The columns of pk cache obtained by PKCacheManager may be empty
    if (pk_cache_cols_opt.has_value() && !pk_cache_cols_opt.value().empty())
    {
        LOG_DEBUG(log, "Hit primary key cache for part {}, and key is {}", task->data_part->name, cache_key);
    }
    else
    {
        LOG_DEBUG(log, "Miss primary key cache for part {}, will load", task->data_part->name);

        /// load pk's bin to memory
        Columns pk_columns;
        bool result = readPrimaryKeyBin(pk_columns);

        if (result)
        {
            LOG_DEBUG(log, "Load primary key column and will put into cache");
            PKCacheManager::getMgr().setPartPkCache(cache_key, std::move(pk_columns));
            pk_cache_cols_opt = PKCacheManager::getMgr().getPartPkCache(cache_key);
        }
        else
        {
            LOG_DEBUG(log, "Failed to load primary key column for part {}, will back to normal read",  task->data_part->name);
        }
    }

    if (!pk_cache_cols_opt.has_value() || pk_cache_cols_opt.value().empty())
    {
        success = false;
        return {};
    }

    /// Read from part use primary key cache
    success = true;
    Columns pk_cache_cols = pk_cache_cols_opt.value();

    const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
    const size_t pk_col_size = primary_key.column_names.size();

    /// Get pk columns from primary key cache based on mark ranges
    MutableColumns result_pk_cols;
    result_pk_cols.resize(pk_col_size);
    for (size_t i = 0; i < pk_col_size; ++i)
        result_pk_cols[i] = primary_key.data_types[i]->createColumn();

    /// Check if need to fill _part_offset, will be used for mergeResult with lightweight delete
    MutableColumnPtr mutable_part_offset_col = nullptr;
    for (const auto & column_name : non_const_virtual_column_names)
    {
        if (column_name == "_part_offset")
        {
            mutable_part_offset_col = ColumnUInt64::create();
            break;
        }
    }

    MergeTreeRangeReader::ReadResult::ReadRangesInfo read_ranges;
    const MergeTreeIndexGranularity & index_granularity = task->data_part->index_granularity;

    for (const auto & mark_range : task->mark_ranges)
    {
        size_t start_row = index_granularity.getMarkStartingRow(mark_range.begin);
        size_t stop_row = index_granularity.getMarkStartingRow(mark_range.end);

        read_ranges.push_back({start_row, stop_row - start_row, mark_range.begin, mark_range.end});

        for (size_t i = 0; i < pk_col_size; ++i)
            result_pk_cols[i]->insertRangeFrom(*pk_cache_cols[i], start_row, stop_row - start_row);

        if (mutable_part_offset_col)
        {
            auto & data = assert_cast<ColumnUInt64 &>(*mutable_part_offset_col).getData();
            while (start_row < stop_row)
                data.push_back(start_row++);
        }
    }

    Columns tmp_result_columns;
    tmp_result_columns.assign(
        std::make_move_iterator(result_pk_cols.begin()),
        std::make_move_iterator(result_pk_cols.end())
        );

    LOG_DEBUG(log, "Fetch from primary key cache size = {}", tmp_result_columns[0]->size());

    /// Get _part_offset if exists
    bool part_offset_exists_in_result = false;
    if (mutable_part_offset_col)
    {
        /// _part_offset column exists in original select columns
        if (!need_remove_part_offset)
        {
            tmp_result_columns.emplace_back(std::move(mutable_part_offset_col));
            part_offset = typeid_cast<const ColumnUInt64 *>(tmp_result_columns.back().get());

            /// Need to adjust order in results
            part_offset_exists_in_result = true;
        }
        else /// No need to put result columns, it's just used in mergeResult() for LWD
            part_offset = typeid_cast<const ColumnUInt64 *>(mutable_part_offset_col.get());
    }

    if (base_search_manager && base_search_manager->preComputed())
    {
        size_t result_row_num = 0;

        base_search_manager->mergeResult(
            tmp_result_columns, /// _Inout_
            result_row_num, /// _Out_
            read_ranges,
            part_offset);

        /// header_without_const_virtual_columns: pk columns + distance columns + non const virtual columns(_part_offset)
        /// tmp_result_columns: pk columns + non const virtual columns(_part_offset) + distance columns
        Columns result_columns;
        result_columns.resize(tmp_result_columns.size());

        /// _part_offset column exists in original select columns
        if (part_offset_exists_in_result)
        {
            /// Exchange order of non const virtual column and distance columns
            size_t distances_size = base_search_manager->getSearchFuncColumnNames().size();

            /// Throw exception if distances_size is empty
            if (distances_size == 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "[PKCache] Failed to find any search result column name, this should not happen");

            for (size_t i = 0; i < tmp_result_columns.size(); ++i)
            {
                size_t pos_in_result;
                if (i < pk_col_size)
                    pos_in_result = i;
                else if (i == pk_col_size)
                    pos_in_result = i + distances_size;
                else
                    pos_in_result = i - 1; /// non const virtual column has ONE column: _part_offset

                result_columns[pos_in_result] = tmp_result_columns[i];
            }
        }
        else
        {
            /// No _part_offset column possibly added for LWD in tmp result columns
            result_columns = tmp_result_columns;
        }

        task->mark_ranges.clear();
        if (result_row_num > 0)
        {
            BlockAndProgress res = {header_without_const_virtual_columns.cloneWithColumns(result_columns), result_row_num};
            return res;
        }
        else /// result_row_num = 0
            return {Block(), result_row_num};
    }

    return {Block(), 0};
}

/// perform vector scan / text search / hybrid search in getNewTaskImpl
bool MergeTreeSelectWithHybridSearchProcessor::getNewTaskImpl()
try
{
    if (all_mark_ranges.empty())
        return false;

    if (!reader)
        initializeReaders();

    MarkRanges mark_ranges_for_task;
    mark_ranges_for_task = std::move(all_mark_ranges);
    all_mark_ranges.clear();

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    /// Check if have pre-computed search result
    if (base_search_manager && !base_search_manager->preComputed())
    {
        /// Get vector scan and/or text search result
        executeSearch(mark_ranges_for_task);

        /// Filter mark ranges of read task
        filterMarkRangesByVectorScanResult(data_part, base_search_manager, mark_ranges_for_task);
    }

    if (mark_ranges_for_task.empty())
        return false;

    task = std::make_unique<MergeTreeReadTask>(
        data_part,
        alter_conversions,
        mark_ranges_for_task,
        part_index_in_query,
        column_name_set,
        task_columns,
        std::move(size_predictor));

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part);
    throw;
}

void MergeTreeSelectWithHybridSearchProcessor::executeSearch(MarkRanges mark_ranges)
{
    PrewhereInfoPtr prewhere_info_copy = nullptr;
    if (prewhere_info)
    {
        prewhere_info_copy = prewhere_info->clone();
        prewhere_info_copy->need_filter = true;
        prewhere_info_copy->remove_prewhere_column = true;
    }

    executeSearch(base_search_manager, storage, storage_snapshot, data_part, alter_conversions, max_block_size_rows,
                preferred_block_size_bytes, preferred_max_column_in_block_size_bytes, mark_ranges,
                prewhere_info_copy, reader_settings, context, max_streams_for_prewhere);
}

void MergeTreeSelectWithHybridSearchProcessor::executeSearch(
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
    ContextPtr context_,
    size_t max_streams)
{
    bool can_skip_peform_prefilter = canSkipPrewhereForPart(data_part_, prewhere_info_copy, storage_,
                                        storage_snapshot_->getMetadataForQuery(), context_);

    /// perform vector scan
    if (!prewhere_info_copy || can_skip_peform_prefilter)
    {
        search_manager->executeSearchBeforeRead(data_part_);
    }
    else
    {
        /// try to process prewhere here, get part_offset columns
        /// 1 read, then get the filtered part_offsets
        /// 2 perform vector scan based on part_offsets
        auto filter = performPrefilter(mark_ranges, prewhere_info_copy, storage_, storage_snapshot_, data_part_,
                                alter_conversions_, max_block_size, preferred_block_size_bytes_,
                                preferred_max_column_in_block_size_bytes_, reader_settings_,
                                context_, max_streams);

        ReadRanges read_ranges;
        ReadRange read_range{0, data_part_->rows_count, 0, data_part_->index_granularity.getMarksCount()};
        read_ranges.emplace_back(read_range);

        search_manager->executeSearchWithFilter(data_part_, read_ranges, filter);
    }
}

namespace
{

struct PartRangesReadInfo
{
    size_t sum_marks = 0;
    size_t total_rows = 0;
    size_t index_granularity_bytes = 0;
    size_t min_marks_for_concurrent_read = 0;
    size_t min_rows_for_concurrent_read = 0;

    bool use_uncompressed_cache = false;
    bool is_adaptive = false;

    PartRangesReadInfo(
        const MergeTreeData::DataPartPtr & data_part,
        const MarkRanges & mark_ranges,
        const Settings & settings,
        const MergeTreeSettings & data_settings)
    {
        /// Count marks to read for the part.
        total_rows = data_part->index_granularity.getRowsCountInRanges(mark_ranges);
        sum_marks = mark_ranges.getNumberOfMarks();

        is_adaptive = data_part->index_granularity_info.mark_type.adaptive;

        if (is_adaptive)
            index_granularity_bytes = data_settings.index_granularity_bytes;

        auto part_on_remote_disk = data_part->isStoredOnRemoteDisk();

        size_t min_bytes_for_concurrent_read;
        if (part_on_remote_disk)
        {
            min_rows_for_concurrent_read = settings.merge_tree_min_rows_for_concurrent_read_for_remote_filesystem;
            min_bytes_for_concurrent_read = settings.merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem;
        }
        else
        {
            min_rows_for_concurrent_read = settings.merge_tree_min_rows_for_concurrent_read;
            min_bytes_for_concurrent_read = settings.merge_tree_min_bytes_for_concurrent_read;
        }

        min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            min_rows_for_concurrent_read, min_bytes_for_concurrent_read,
            data_settings.index_granularity, index_granularity_bytes, sum_marks);

        /// Don't adjust this value based on sum_marks and max_marks_to_use_cache as in ReadFromMergeTree
        use_uncompressed_cache = settings.use_uncompressed_cache;
    }
};

template<typename PullingExecutor>
VIBitmapPtr getFilterFromPipeline(size_t num_rows, Pipe & pipe)
{
    QueryPipelineBuilder builder;
    builder.init(std::move(pipe));

    QueryPipeline filter_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    /// Use different pipeline executors
    PullingExecutor filter_executor(filter_pipeline);

    Block block;
    VIBitmapPtr filter = std::make_shared<VIBitmap>(num_rows);
    {
        OpenTelemetry::SpanHolder span_pipe("performPrefilter()::getFilterFromPipeline()");
        while (filter_executor.pull(block))
        {
            if (block)
            {
                const PaddedPODArray<UInt64> & col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column)->getData();
                for (size_t i = 0; i < block.rows(); ++i)
                {
                    filter->set(col_data[i]);
                }
            }
        }
    }

    return filter;
}

}

VIBitmapPtr MergeTreeSelectWithHybridSearchProcessor::performPrefilter(
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
    ContextPtr context_,
    size_t max_streams)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::performPrefilter()");
    Names required_columns_prewhere;
    Names system_columns;
    system_columns.emplace_back("_part_offset");

    ExpressionActionsSettings actions_settings;

    /// TODO: confirm columns are valid?
    NameSet pre_name_set;

    /// Add column reading steps:
    /// 1. Columns for row level filter
    if (prewhere_info_copy->row_level_filter)
    {
        Names row_filter_column_names =  prewhere_info_copy->row_level_filter->getRequiredColumnsNames();

        required_columns_prewhere.insert(required_columns_prewhere.end(), row_filter_column_names.begin(), row_filter_column_names.end());
        pre_name_set.insert(row_filter_column_names.begin(), row_filter_column_names.end());
    }

    /// 2. Columns for prewhere
    if (prewhere_info_copy->prewhere_actions)
    {
        Names all_pre_column_names = prewhere_info_copy->prewhere_actions->getRequiredColumnsNames();

        for (const auto & name : all_pre_column_names)
        {
            if (pre_name_set.contains(name))
                continue;
            required_columns_prewhere.push_back(name);
            pre_name_set.insert(name);
        }
    }

    /// Check if parallel reading mark ranges among streams is enabled
    bool enable_parallel_reading = false;
    const auto & settings = context_->getSettingsRef();
    const auto data_settings = storage_.getSettings();
    size_t num_rows = data_part_->rows_count;

    PartRangesReadInfo info(data_part_, mark_ranges, settings, *data_settings);

    /// max streams for performing prewhere
    size_t num_streams = max_streams;

    LOG_DEBUG(&Poco::Logger::get("performPreFilter"), "max_streams = {}, original min_marks_for_concurrent_read = {}, sum_marks = {}, total_rows = {}, min_rows_for_concurrent_read = {}",
            max_streams, info.min_marks_for_concurrent_read, info.sum_marks, info.total_rows, info.min_rows_for_concurrent_read);

    /// Enable parallel when num_streams > 1
    if (num_streams > 1)
    {
        if (settings.parallel_reading_prefilter_option == 2)
            enable_parallel_reading = true;
        else if (settings.parallel_reading_prefilter_option == 1)
        {
            /// Adaptively enable parallel reading based on mark ranges and row count
            /// Reduce the number of num_streams if the data is small.
            if (info.sum_marks < num_streams * info.min_marks_for_concurrent_read)
            {
                const size_t prev_num_streams = num_streams;
                num_streams = (info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read;
                const size_t increase_num_streams_ratio = std::min(prev_num_streams / num_streams, info.min_marks_for_concurrent_read / 8);
                if (increase_num_streams_ratio > 1)
                {
                    num_streams = num_streams * increase_num_streams_ratio;
                    info.min_marks_for_concurrent_read = (info.sum_marks + num_streams - 1) / num_streams;
                }
            }
            else if (info.total_rows < num_streams * info.min_rows_for_concurrent_read)
            {
                num_streams = (info.total_rows + info.min_rows_for_concurrent_read - 1) / info.min_rows_for_concurrent_read;
                const size_t new_min_marks_for_concurrent_read = (info.sum_marks + num_streams -1 ) / num_streams;
                if (new_min_marks_for_concurrent_read > info.min_marks_for_concurrent_read)
                    info.min_marks_for_concurrent_read = new_min_marks_for_concurrent_read;
            }

            if (num_streams > 1)
                enable_parallel_reading = true;
        }
    }

    LOG_DEBUG(&Poco::Logger::get("performPreFilter"), "num_streams = {}, min_marks_for_concurrent_read = {}", max_streams, info.min_marks_for_concurrent_read);

    /// Read in multiple threads will use Async pulling executor
    if (enable_parallel_reading)
    {
        /// spreadMarkRangesAmongStreams()
        /// Only one part
        RangesInDataParts parts_with_ranges;
        parts_with_ranges.emplace_back(data_part_, std::make_shared<AlterConversions>(), 0, mark_ranges);
        Pipes pipes;

        if (max_block_size && !info.is_adaptive)
        {
            size_t fixed_index_granularity = data_settings->index_granularity;
            info.min_marks_for_concurrent_read = (info.min_marks_for_concurrent_read * fixed_index_granularity + max_block_size - 1)
                / max_block_size * max_block_size / fixed_index_granularity;
        }

        MergeTreeReadPoolPtr pool;
        pool = std::make_shared<MergeTreeReadPool>(
            num_streams,
            info.sum_marks,
            info.min_marks_for_concurrent_read,
            std::move(parts_with_ranges),
            storage_snapshot_,
            prewhere_info_copy,
            actions_settings,
            reader_settings_,
            required_columns_prewhere,
            system_columns,
            context_,
            false);

        for (size_t i = 0; i < num_streams; ++i)
        {
            auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(
                i, pool, info.min_marks_for_concurrent_read, max_block_size,
                settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
                storage_, storage_snapshot_, info.use_uncompressed_cache,
                prewhere_info_copy, actions_settings, reader_settings_, system_columns);

            auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

            if (i == 0)
                source->addTotalRowsApprox(info.total_rows);

            pipes.emplace_back(std::move(source));
        }

        Pipe pipe = Pipe::unitePipes(std::move(pipes));

        return getFilterFromPipeline<PullingAsyncPipelineExecutor>(num_rows, pipe);
    }
    else
    {
        /// Read in a single thread
        auto algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(
            storage_,
            storage_snapshot_,
            data_part_,
            alter_conversions_,
            max_block_size,
            preferred_block_size_bytes_,
            preferred_max_column_in_block_size_bytes_,
            required_columns_prewhere,
            mark_ranges,
            info.use_uncompressed_cache,
            prewhere_info_copy,
            actions_settings,
            reader_settings_,
            nullptr,
            system_columns);

        auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

        Pipe pipe = Pipe(std::move(source));

        return getFilterFromPipeline<PullingPipelineExecutor>(num_rows, pipe);
    }
}

VectorAndTextResultInDataParts MergeTreeSelectWithHybridSearchProcessor::selectPartsByVectorAndTextIndexes(
    const RangesInDataParts & parts_with_ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<bool> & vec_support_two_stage_searches,
#if USE_TANTIVY_SEARCH
    const TANTIVY::Statistics & bm25_stats_in_table,
#endif
    const PrewhereInfoPtr & prewhere_info_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context,
    size_t max_block_size,
    size_t num_streams,
    const MergeTreeData & data,
    const MergeTreeReaderSettings & reader_settings_)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::selectPartsByVectorAndTextIndexes()");
    VectorAndTextResultInDataParts parts_with_mix_results;
    if (!query_info.has_hybrid_search)
        return parts_with_mix_results;

    size_t parts_with_ranges_size = parts_with_ranges.size();
    parts_with_mix_results.resize(parts_with_ranges_size);

    PrewhereInfoPtr prewhere_info_copy = nullptr;
    if (prewhere_info_)
    {
        prewhere_info_copy = prewhere_info_->clone();
        prewhere_info_copy->need_filter = true;
        prewhere_info_copy->remove_prewhere_column = true;
    }

    auto & settings = context->getSettingsRef();

    /// Execute vector scan and text search in this part.
    auto process_part = [&](size_t part_index)
    {
        auto & part_with_range = parts_with_ranges[part_index];
        auto & data_part = part_with_range.data_part;
        auto & mark_ranges = part_with_range.ranges;

        /// Save part_index in parts_with_ranges
        VectorAndTextResultInDataPart mix_results(part_index, data_part);

        /// Handle three cases: vector scan, full-text seach and hybrid search
        if (query_info.hybrid_search_info)
        {
            auto hybrid_search_mgr = std::make_shared<MergeTreeHybridSearchManager>(metadata_snapshot, query_info.hybrid_search_info,
                                            context, vec_support_two_stage_searches[0]);
#if USE_TANTIVY_SEARCH
            hybrid_search_mgr->setBM25Stats(bm25_stats_in_table);
#endif
            /// Get vector scan and text search
            executeSearch(hybrid_search_mgr, data, storage_snapshot_, data_part, part_with_range.alter_conversions,
                        max_block_size, settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
                        mark_ranges, prewhere_info_copy, reader_settings_,
                        context, num_streams);

            if (hybrid_search_mgr)
            {
                mix_results.vector_scan_results.emplace_back(hybrid_search_mgr->getVectorScanResult());
                mix_results.text_search_result = hybrid_search_mgr->getTextSearchResult();
            }
        }
        else if (query_info.vector_scan_info)
        {
            auto vector_scan_mgr = std::make_shared<MergeTreeVSManager>(metadata_snapshot, query_info.vector_scan_info,
                                        context, vec_support_two_stage_searches);

            /// Get vector scan
            executeSearch(vector_scan_mgr, data, storage_snapshot_, data_part, part_with_range.alter_conversions,
                        max_block_size, settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
                        mark_ranges, prewhere_info_copy, reader_settings_,
                        context, num_streams);

            /// Support multiple distance functions
            if (vector_scan_mgr && vector_scan_mgr->preComputed())
                mix_results.vector_scan_results = vector_scan_mgr->getVectorScanResults();
        }
        else if (query_info.text_search_info)
        {
            auto text_search_mgr = std::make_shared<MergeTreeTextSearchManager>(metadata_snapshot, query_info.text_search_info, context);
#if USE_TANTIVY_SEARCH
            text_search_mgr->setBM25Stats(bm25_stats_in_table);
#endif
            /// Get vector scan
            executeSearch(text_search_mgr, data, storage_snapshot_, data_part, part_with_range.alter_conversions,
                        max_block_size, settings.preferred_block_size_bytes, settings.preferred_max_column_in_block_size_bytes,
                        mark_ranges, prewhere_info_copy, reader_settings_,
                        context, num_streams);

            if (text_search_mgr && text_search_mgr->preComputed())
                mix_results.text_search_result = text_search_mgr->getSearchResult();
        }

        parts_with_mix_results[part_index] = std::move(mix_results);
    };

    size_t num_threads = std::min<size_t>(num_streams, parts_with_ranges_size);

    if (num_threads <= 1)
    {
        for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
            process_part(part_index);
    }
    else
    {
        /// Parallel loading of data parts.
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreads,
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreadsActive,
            num_threads);

        for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
            pool.scheduleOrThrowOnError([&, part_index, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );
                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);

                process_part(part_index);
            });

        pool.wait();
    }

    return parts_with_mix_results;
}

}
