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

#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <DataTypes/DataTypeTuple.h>

#include <VectorIndex/Storages/MergeTreeSelectWithVectorScanProcessor.h>
#include <VectorIndex/Storages/MergeTreeVectorScanUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

static bool isVectorSearchByPk(const std::vector<String> & pk_col_names, const std::vector<String> & read_col_names)
{
    size_t pk_col_nums = pk_col_names.size();
    size_t read_col_nums = read_col_names.size();

    /// Currently primary key cache support only one column in PK.
    if (read_col_nums <= pk_col_nums || pk_col_nums > 1)
        return false;

    const String pk_col_name = pk_col_names[0];

    /// Read columns can only be primary key columns, or vector scan functions, distance, batch_distance.
    bool match = true;
    for (const auto & read_col_name : read_col_names)
    {
        if ((read_col_name == pk_col_name) || isVectorScanFunc(read_col_name))
            continue;
        else
        {
            match = false;
            break;
        }
    }

    return match;
}

/// Referenced from MergeTreeSelectProcessor::initializeReaders() and MergeTreeBaseSelectProcessor::initializeMergeTreeReadersForPart()
void MergeTreeSelectWithVectorScanProcessor::initializeReadersWithVectorScan()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::initializeReadersWithVectorScan()");
    task_columns = getReadTaskColumns(
        LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions), storage_snapshot,
        required_columns, virt_column_names, nullptr, actions_settings, reader_settings, /*with_subcolumns=*/ true);

    /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    if (use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

/*
    initializeMergeTreeReadersForPart(data_part, task_columns, storage_snapshot->getMetadataForQuery(),
        all_mark_ranges, {}, {});
*/
    reader = data_part->getReader(task_columns.columns, storage_snapshot->getMetadataForQuery(),
        all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), alter_conversions, reader_settings,
        {}, {});

    pre_reader_for_step.clear();

    /// Add lightweight delete filtering step
    if (reader_settings.apply_deleted_mask && data_part->hasLightweightDelete())
    {
        pre_reader_for_step.push_back(data_part->getReader({LightweightDeleteDescription::FILTER_COLUMN}, storage_snapshot->getMetadataForQuery(),
            all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), alter_conversions, reader_settings,
            {}, {}));
    }
}

VectorIndexBitmapPtr MergeTreeSelectWithVectorScanProcessor::performPrefilter(MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::performPrefilter()");
    Names requried_columns;
    Names system_columns;
    system_columns.emplace_back("_part_offset");

    ExpressionActionsSettings actions_settings;

    /// TODO: confirm columns are valid?
    NameSet pre_name_set;

    /// Add column reading steps:
    /// 1. Columns for row level filter
    if (prewhere_info->row_level_filter)
    {
        Names row_filter_column_names =  prewhere_info->row_level_filter->getRequiredColumnsNames();

        requried_columns.insert(requried_columns.end(), row_filter_column_names.begin(), row_filter_column_names.end());
        pre_name_set.insert(row_filter_column_names.begin(), row_filter_column_names.end());
    }

    /// 2. Columns for prewhere
    if (prewhere_info->prewhere_actions)
    {
        Names all_pre_column_names = prewhere_info->prewhere_actions->getRequiredColumnsNames();

        for (const auto & name : all_pre_column_names)
        {
            if (pre_name_set.contains(name))
                continue;
            requried_columns.push_back(name);
            pre_name_set.insert(name);
        }
    }

    /// No need to return prewhere column
    {
        std::lock_guard lock(prewhere_info->prewhere_info_mutex);
        if (!prewhere_info->remove_prewhere_column)
            prewhere_info->remove_prewhere_column = true;
    }

    auto algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(
        storage,
        storage_snapshot,
        data_part,
        alter_conversions,
        max_block_size_rows,
        preferred_block_size_bytes,
        preferred_max_column_in_block_size_bytes,
        requried_columns,
        mark_ranges,
        use_uncompressed_cache,
        prewhere_info,
        actions_settings,
        reader_settings,
        nullptr,
        system_columns);

    auto source = std::make_shared<MergeTreeSource>(std::move(algorithm));

    Pipe pipe(std::move(source));

    QueryPipeline filter_pipeline(std::move(pipe));
    PullingPipelineExecutor filter_executor(filter_pipeline);

    size_t num_rows = data_part->rows_count;

    Block block;
    VectorIndexBitmapPtr filter = std::make_shared<VectorIndexBitmap>(num_rows);
    {
        OpenTelemetry::SpanHolder span_pipe("MergeTreeSelectWithVectorScanProcessor::performPrefilter()::StartPipe");
        while (filter_executor.pull(block))
        {
            const PaddedPODArray<UInt64> & col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column)->getData();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                filter->set(col_data[i]);
            }
        }
    }

    return filter;
}

bool MergeTreeSelectWithVectorScanProcessor::readPrimaryKeyBin(Columns & out_columns)
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
        storage.getContext()->getMarkCache().get(),
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

IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithVectorScanProcessor::readFromPart()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::readFromPart()");
    if (!task->range_reader.isInitialized())
    {
        MergeTreeRangeReader* prev_reader = nullptr;
        bool last_reader = false;

        /// Initialize primary key cache
        const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
        const bool enable_primary_key_cache = task->data_part->storage.canUsePrimaryKeyCache();
        LOG_DEBUG(log, "Reader setting: enable_primary_key_cache = {}", enable_primary_key_cache);

        /// consider cache if and only if
        /// 1. this task is vector search and no prewhere info
        /// 2. primary key is only a column, and select columns are (pk, distance) or (pk, batch_distance)
        /// 3. primary key's value is represented by number
        if (enable_primary_key_cache)
        {
            use_primary_key_cache = PrimaryKeyCacheManager::isSupportedPrimaryKey(primary_key)
                && isVectorSearchByPk(primary_key.column_names, ordered_names);
        }

        /// Add _part_offset to non_const_virtual_column_names if part has lightweight delete
        if (task->data_part->hasLightweightDelete())
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

        /// Add filtering step with lightweight delete mask
        if (reader_settings.apply_deleted_mask && task->data_part->hasLightweightDelete())
        {
            task->pre_range_readers.push_back(
                MergeTreeRangeReader(pre_reader_for_step[0].get(), prev_reader, &lightweight_delete_filter_step, last_reader, non_const_virtual_column_names));
            prev_reader = &task->pre_range_readers.back();
        }

        task->range_reader = MergeTreeRangeReader(reader.get(), prev_reader, nullptr, true, non_const_virtual_column_names);
    }

    /// original read logic, considering prewhere optimization
    return readFromPartWithVectorScan();
}

/// perform actual read and result merge operation, prewhere has been processed ahead
/// Referenced from MergeTreeBaseSelectProcessor::readFromPartImpl()
IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithVectorScanProcessor::readFromPartWithVectorScan()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::readFromPartWithVectorScan()");
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;

    auto read_start_time = std::chrono::system_clock::now();
    UInt64 rows_to_read = std::max(UInt64(1), current_max_block_size_rows);

    if (use_primary_key_cache)
    {
        LOG_DEBUG(log, "Use primary key cache");

        const String cache_key = task->data_part->getDataPartStorage().getRelativePath() + ":" + task->data_part->name;

        std::optional<Columns> pk_cache_cols_opt = PrimaryKeyCacheManager::getMgr().getPartPkCache(cache_key);

        /// The columns of pk cache obtained by PrimaryKeyCacheManager may be empty
        if (pk_cache_cols_opt.has_value() && !pk_cache_cols_opt.value().empty())
        {
            LOG_DEBUG(log, "Hit primary key cache, and key is {}", cache_key);
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
                PrimaryKeyCacheManager::getMgr().setPartPkCache(cache_key, std::move(pk_columns));
                pk_cache_cols_opt = PrimaryKeyCacheManager::getMgr().getPartPkCache(cache_key);
            }
            else
            {
                LOG_DEBUG(log, "Failed to load primary key column for part {}, will back to normal read",  task->data_part->name);
            }
        }

        if (pk_cache_cols_opt.has_value() && !pk_cache_cols_opt.value().empty())
        {
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

            Columns result_columns;
            result_columns.assign(
                std::make_move_iterator(result_pk_cols.begin()),
                std::make_move_iterator(result_pk_cols.end())
                );

            LOG_DEBUG(log, "Fetch from primary key cache size = {}", result_columns[0]->size());

            /// Get _part_offset if exists.
            if (mutable_part_offset_col)
            {
                /// _part_offset column exists in original select columns
                if (!need_remove_part_offset)
                {
                    result_columns.emplace_back(std::move(mutable_part_offset_col));
                    part_offset = typeid_cast<const ColumnUInt64 *>(result_columns.back().get());
                }
                else
                    part_offset = typeid_cast<const ColumnUInt64 *>(mutable_part_offset_col.get());
            }

            if (task->vector_scan_manager && task->vector_scan_manager->preComputed())
            {
                size_t result_row_num = 0;

                task->vector_scan_manager->mergeResult(
                    result_columns, /// _Inout_
                    result_row_num, /// _Out_
                    read_ranges,
                    nullptr,
                    part_offset);

                task->mark_ranges.clear();
                if (result_row_num > 0)
                {
                    BlockAndProgress res = {header_without_const_virtual_columns.cloneWithColumns(result_columns), result_row_num};
                    return res;
                }
                else
                    return {};
            }
        }
    }

    LOG_DEBUG(log, "Begin read, mark_ranges size = {}", task->mark_ranges.size());
    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);
    for (auto it = task->mark_ranges.begin(); it != task->mark_ranges.cend(); ++it)
    {
        LOG_DEBUG(log, "Mark range begin = {}, end = {}", it->begin, it->end);
    }

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

    /// Remove distance_func column from read_result.columns, it will be added by vector search.
    Columns ordered_columns;
    if (task->vector_scan_manager)
        ordered_columns.reserve(sample_block.columns() - 1);
    else
        ordered_columns.reserve(sample_block.columns());
    size_t which_cut = 0;
    String vector_scan_col_name;
    for (size_t ps = 0; ps < sample_block.columns(); ++ps)
    {
        auto & col_name = sample_block.getByPosition(ps).name;
        LOG_DEBUG(log, "Read column: {}", col_name);
        /// TODO: not add distance column to header_without_virtual_columns
        if (isVectorScanFunc(col_name))
        {
            which_cut = ps;
            vector_scan_col_name = col_name;
            continue;
        }

        ordered_columns.emplace_back(std::move(read_result.columns[ps]));

        /// Copy _part_offset column
        if (col_name == "_part_offset")
        {
            part_offset = typeid_cast<const ColumnUInt64 *>(ordered_columns.back().get());
        }
    }

    auto read_end_time = std::chrono::system_clock::now();

    LOG_DEBUG(log, "Read time: {}", std::chrono::duration_cast<std::chrono::milliseconds>(read_end_time - read_start_time).count());

    /// [MQDB] vector search
    if (task->vector_scan_manager && task->vector_scan_manager->preComputed())
    {
        /// already perform vector scan   
        task->vector_scan_manager->mergeResult(
            ordered_columns,
            read_result.num_rows,
            read_ranges, nullptr, part_offset);
    }

    const size_t final_result_num_rows = read_result.num_rows;

    Block res_block;

    /// Add prewhere column name to avoid column not found error
    if (prewhere_info && !original_remove_prewhere_column)
    {
        ColumnWithTypeAndName prewhere_col;

        const auto & node = prewhere_info->prewhere_actions->findInOutputs(prewhere_info->prewhere_column_name);
        auto filter_type = node.result_type;

        prewhere_col.type = filter_type;
        prewhere_col.name = prewhere_info->prewhere_column_name;
        prewhere_col.column = filter_type->createColumnConst(final_result_num_rows, 1);

        res_block.insert(std::move(prewhere_col));
    }

    for (size_t i = 0; i < ordered_columns.size(); ++i)
    {
        ColumnWithTypeAndName ctn;
        ctn.column = ordered_columns[i];

        if (i < ordered_columns.size() - 1)
        {
            size_t src_index = i >= which_cut ? i+1 : i;
            ctn.type = sample_block.getByPosition(src_index).type;
            ctn.name = sample_block.getByPosition(src_index).name;
        }
        else
        {
            ctn.name = vector_scan_col_name;
            if (isBatchDistance(vector_scan_col_name))
            {
                // the result of batch search, it's type is Tuple(UInt32, Float32)
                DataTypes data_types;
                data_types.emplace_back(std::make_shared<DataTypeUInt32>());
                data_types.emplace_back(std::make_shared<DataTypeFloat32>());
                ctn.type = std::make_shared<DataTypeTuple>(data_types);
            }
            else
            {
                // the result of single search, it's type is Float32
                ctn.type = std::make_shared<DataTypeFloat32>();
            }
        }

        res_block.insert(std::move(ctn));
    }

    if (need_remove_part_offset)
    {
        res_block.erase("_part_offset");
    }

    BlockAndProgress res = {res_block, final_result_num_rows, num_read_rows, num_read_bytes};

    return res;
}

/// perform vector scan in getNewTaskImpl
bool MergeTreeSelectWithVectorScanProcessor::getNewTaskImpl()
try
{
    if (all_mark_ranges.empty())
        return false;

    if (!reader)
        initializeReadersWithVectorScan();

    MarkRanges mark_ranges_for_task;
    mark_ranges_for_task = std::move(all_mark_ranges);
    all_mark_ranges.clear();

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    /// perform vector scan, then filter mark ranges of read task
    if (!prewhere_info)
    {
        vector_scan_manager->executeBeforeRead(data_part);
        filterMarkRangesByVectorScanResult(data_part, vector_scan_manager, mark_ranges_for_task);
    }
    else
    {
        /// try to process prewhere here, get part_offset columns
        /// 1 read, then get the filtered part_offsets
        /// 2 perform vector scan based on part_offsets
        /// 3 filter mark_ranges based on vector scan results
        auto filter = performPrefilter(mark_ranges_for_task);
        ReadRanges read_ranges;
        ReadRange read_range{0, data_part->rows_count, 0, data_part->index_granularity.getMarksCount()};
        read_ranges.emplace_back(read_range);
        vector_scan_manager->executeVectorScanWithFilter(data_part, read_ranges, filter);
        filterMarkRangesByVectorScanResult(data_part, vector_scan_manager, mark_ranges_for_task);
    }

    for (const auto & range : mark_ranges_for_task)
    {
        LOG_DEBUG(log, "Keep range: {} - {}", range.begin, range.end);
    }
    
    if (mark_ranges_for_task.empty())
    {
        return false;
    }

    task = std::make_unique<MergeTreeReadTask>(
        data_part,
        alter_conversions,
        mark_ranges_for_task,
        part_index_in_query,
        column_name_set,
        task_columns,
        std::move(size_predictor),
        0,
        std::future<MergeTreeReaderPtr>(),
        std::vector<std::future<MergeTreeReaderPtr>>(),
        vector_scan_manager);

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part);
    throw;
}

}
