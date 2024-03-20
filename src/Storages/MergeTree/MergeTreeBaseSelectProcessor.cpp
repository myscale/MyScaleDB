#include <memory>
#include <optional>
#include <Columns/ColumnArray.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <VectorIndex/Storages/MergeTreeVectorScanManager.h>
#include <VectorIndex/Utils/VectorScanUtils.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <city.h>

namespace ProfileEvents
{
    extern const Event WaitPrefetchTaskMicroseconds;
};

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

static void injectNonConstVirtualColumns(
    size_t rows,
    Block & block,
    const Names & virtual_columns);

static void injectPartConstVirtualColumns(
    size_t rows,
    Block & block,
    MergeTreeReadTask * task,
    const DataTypePtr & partition_value_type,
    const Names & virtual_columns);


IMergeTreeSelectAlgorithm::IMergeTreeSelectAlgorithm(
    Block header,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    UInt64 max_block_size_rows_,
    UInt64 preferred_block_size_bytes_,
    UInt64 preferred_max_column_in_block_size_bytes_,
    const MergeTreeReaderSettings & reader_settings_,
    bool use_uncompressed_cache_,
    const Names & virt_column_names_)
    : storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , prewhere_actions(getPrewhereActions(prewhere_info, actions_settings, reader_settings_.enable_multiple_prewhere_read_steps))
    , max_block_size_rows(max_block_size_rows_)
    , preferred_block_size_bytes(preferred_block_size_bytes_)
    , preferred_max_column_in_block_size_bytes(preferred_max_column_in_block_size_bytes_)
    , reader_settings(reader_settings_)
    , use_uncompressed_cache(use_uncompressed_cache_)
    , virt_column_names(virt_column_names_)
    , partition_value_type(storage.getPartitionValueType())
    , owned_uncompressed_cache(use_uncompressed_cache ? storage.getContext()->getUncompressedCache() : nullptr)
    , owned_mark_cache(storage.getContext()->getMarkCache())
{
    header_without_const_virtual_columns = applyPrewhereActions(std::move(header), prewhere_info);
    size_t non_const_columns_offset = header_without_const_virtual_columns.columns();
    injectNonConstVirtualColumns(0, header_without_const_virtual_columns, virt_column_names);

    for (size_t col_num = non_const_columns_offset; col_num < header_without_const_virtual_columns.columns(); ++col_num)
        non_const_virtual_column_names.emplace_back(header_without_const_virtual_columns.getByPosition(col_num).name);

    result_header = header_without_const_virtual_columns;
    injectPartConstVirtualColumns(0, result_header, nullptr, partition_value_type, virt_column_names);

    if (prewhere_actions)
        LOG_TRACE(log, "PREWHERE condition was split into {} steps: {}", prewhere_actions->steps.size(), prewhere_actions->dumpConditions());

    if (prewhere_info)
        LOG_TEST(log, "Original PREWHERE DAG:\n{}\nPREWHERE actions:\n{}",
            (prewhere_info->prewhere_actions ? prewhere_info->prewhere_actions->dumpDAG(): std::string("<nullptr>")),
            (prewhere_actions ? prewhere_actions->dump() : std::string("<nullptr>")));
}

bool tryBuildPrewhereSteps(PrewhereInfoPtr prewhere_info, const ExpressionActionsSettings & actions_settings, PrewhereExprInfo & prewhere);

std::unique_ptr<PrewhereExprInfo> IMergeTreeSelectAlgorithm::getPrewhereActions(PrewhereInfoPtr prewhere_info, const ExpressionActionsSettings & actions_settings, bool enable_multiple_prewhere_read_steps)
{
    std::unique_ptr<PrewhereExprInfo> prewhere_actions;
    if (prewhere_info)
    {
        prewhere_actions = std::make_unique<PrewhereExprInfo>();

        if (prewhere_info->row_level_filter)
        {
            PrewhereExprStep row_level_filter_step
            {
                .actions = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter, actions_settings),
                .column_name = prewhere_info->row_level_column_name,
                .remove_column = true,
                .need_filter = true
            };

            prewhere_actions->steps.emplace_back(std::move(row_level_filter_step));
        }

        if (!enable_multiple_prewhere_read_steps ||
            !tryBuildPrewhereSteps(prewhere_info, actions_settings, *prewhere_actions))
        {
            PrewhereExprStep prewhere_step
            {
                .actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions, actions_settings),
                .column_name = prewhere_info->prewhere_column_name,
                .remove_column = prewhere_info->remove_prewhere_column,
                .need_filter = prewhere_info->need_filter
            };

            prewhere_actions->steps.emplace_back(std::move(prewhere_step));
        }
    }

    return prewhere_actions;
}


bool IMergeTreeSelectAlgorithm::getNewTask()
{
    if (getNewTaskImpl())
    {
        finalizeNewTask();
        return true;
    }
    return false;
}


ChunkAndProgress IMergeTreeSelectAlgorithm::read()
{
    size_t num_read_rows = 0;
    size_t num_read_bytes = 0;

    while (!is_cancelled)
    {
        try
        {
            if ((!task || task->isFinished()) && !getNewTask())
                break;
        }
        catch (const Exception & e)
        {
            /// See MergeTreeBaseSelectProcessor::getTaskFromBuffer()
            if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                break;
            throw;
        }

        auto res = readFromPart();

        if (res.row_count)
        {
            injectVirtualColumns(res.block, res.row_count, task.get(), partition_value_type, virt_column_names);

            /// Reorder the columns according to result_header
            Columns ordered_columns;
            ordered_columns.reserve(result_header.columns());
            for (size_t i = 0; i < result_header.columns(); ++i)
            {
                auto name = result_header.getByPosition(i).name;
                ordered_columns.push_back(res.block.getByName(name).column);
            }

            /// Account a progress from previous empty chunks.
            res.num_read_rows += num_read_rows;
            res.num_read_bytes += num_read_bytes;

            return ChunkAndProgress{
                .chunk = Chunk(ordered_columns, res.row_count),
                .num_read_rows = res.num_read_rows,
                .num_read_bytes = res.num_read_bytes,
                .is_finished = false};
        }
        else
        {
            return {Chunk(), res.num_read_rows, res.num_read_bytes, false};
        }
    }

    return {Chunk(), num_read_rows, num_read_bytes, true};
}

void IMergeTreeSelectAlgorithm::initializeMergeTreeReadersForCurrentTask(
    const StorageMetadataPtr & metadata_snapshot,
    const IMergeTreeReader::ValueSizeMap & value_size_map,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback)
{
    if (!task)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no task");

    if (task->reader.valid())
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WaitPrefetchTaskMicroseconds);
        reader = task->reader.get();
    }
    else
    {
        reader = task->data_part->getReader(
            task->task_columns.columns, metadata_snapshot, task->mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(),
            task->alter_conversions, reader_settings, value_size_map, profile_callback);
    }

    if (!task->pre_reader_for_step.empty())
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WaitPrefetchTaskMicroseconds);
        pre_reader_for_step.clear();
        for (auto & pre_reader : task->pre_reader_for_step)
            pre_reader_for_step.push_back(pre_reader.get());
    }
    else
    {
        initializeMergeTreePreReadersForPart(
            task->data_part, task->alter_conversions,
            task->task_columns, metadata_snapshot,
            task->mark_ranges, value_size_map, profile_callback);
    }
}

void IMergeTreeSelectAlgorithm::initializeMergeTreeReadersForPart(
    const MergeTreeData::DataPartPtr & data_part,
    const AlterConversionsPtr & alter_conversions,
    const MergeTreeReadTaskColumns & task_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    const IMergeTreeReader::ValueSizeMap & value_size_map,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback)
{
    reader = data_part->getReader(
        task_columns.columns, metadata_snapshot, mark_ranges,
        owned_uncompressed_cache.get(), owned_mark_cache.get(),
        alter_conversions, reader_settings, value_size_map, profile_callback);

    initializeMergeTreePreReadersForPart(
        data_part, alter_conversions, task_columns, metadata_snapshot,
        mark_ranges, value_size_map, profile_callback);
}

void IMergeTreeSelectAlgorithm::initializeMergeTreePreReadersForPart(
    const MergeTreeData::DataPartPtr & data_part,
    const AlterConversionsPtr & alter_conversions,
    const MergeTreeReadTaskColumns & task_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    const IMergeTreeReader::ValueSizeMap & value_size_map,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback)
{
    pre_reader_for_step.clear();

    /// Add lightweight delete filtering step
    if (reader_settings.apply_deleted_mask && data_part->hasLightweightDelete())
    {
        pre_reader_for_step.push_back(
            data_part->getReader(
                {LightweightDeleteDescription::FILTER_COLUMN}, metadata_snapshot,
                mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(),
                alter_conversions, reader_settings, value_size_map, profile_callback));
    }

    if (prewhere_info)
    {
        for (const auto & pre_columns_per_step : task_columns.pre_columns)
        {
            pre_reader_for_step.push_back(
                data_part->getReader(
                    pre_columns_per_step, metadata_snapshot, mark_ranges,
                    owned_uncompressed_cache.get(), owned_mark_cache.get(),
                    alter_conversions, reader_settings, value_size_map, profile_callback));
        }
    }
}

void IMergeTreeSelectAlgorithm::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    /// Add _part_offset to non_const_virtual_column_names if has vector_scan_manager and no prewhere_info
    if (current_task.vector_scan_manager && !prewhere_info)
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

    return initializeRangeReadersImpl(
        current_task.range_reader, current_task.pre_range_readers, prewhere_info, prewhere_actions.get(),
        reader.get(), current_task.data_part->hasLightweightDelete(), reader_settings,
        pre_reader_for_step, lightweight_delete_filter_step, non_const_virtual_column_names);
}

void IMergeTreeSelectAlgorithm::initializeRangeReadersImpl(
    MergeTreeRangeReader & range_reader, std::deque<MergeTreeRangeReader> & pre_range_readers,
    PrewhereInfoPtr prewhere_info, const PrewhereExprInfo * prewhere_actions,
    IMergeTreeReader * reader, bool has_lightweight_delete, const MergeTreeReaderSettings & reader_settings,
    const std::vector<std::unique_ptr<IMergeTreeReader>> & pre_reader_for_step,
    const PrewhereExprStep & lightweight_delete_filter_step, const Names & non_const_virtual_column_names)
{
    MergeTreeRangeReader * prev_reader = nullptr;
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

    if (prewhere_info)
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

    if (!last_reader)
    {
        range_reader = MergeTreeRangeReader(reader, prev_reader, nullptr, true, non_const_virtual_column_names);
    }
    else
    {
        /// If all columns are read by pre_range_readers than move last pre_range_reader into range_reader
        range_reader = std::move(pre_range_readers.back());
        pre_range_readers.pop_back();
    }
}

static UInt64 estimateNumRows(const MergeTreeReadTask & current_task, UInt64 current_preferred_block_size_bytes,
    UInt64 current_max_block_size_rows, UInt64 current_preferred_max_column_in_block_size_bytes, double min_filtration_ratio, size_t min_marks_to_read)
{
    const MergeTreeRangeReader & current_reader = current_task.range_reader;

    if (!current_task.size_predictor)
        return static_cast<size_t>(current_max_block_size_rows);

    /// Calculates number of rows will be read using preferred_block_size_bytes.
    /// Can't be less than avg_index_granularity.
    size_t rows_to_read = current_task.size_predictor->estimateNumRows(current_preferred_block_size_bytes);
    if (!rows_to_read)
        return rows_to_read;
    auto total_row_in_current_granule = current_reader.numRowsInCurrentGranule();
    rows_to_read = std::max(total_row_in_current_granule, rows_to_read);

    if (current_preferred_max_column_in_block_size_bytes)
    {
        /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
        auto rows_to_read_for_max_size_column
            = current_task.size_predictor->estimateNumRowsForMaxSizeColumn(current_preferred_max_column_in_block_size_bytes);
        double filtration_ratio = std::max(min_filtration_ratio, 1.0 - current_task.size_predictor->filtered_rows_ratio);
        auto rows_to_read_for_max_size_column_with_filtration
            = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

        /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than current_index_granularity.
        rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
    }

    auto unread_rows_in_current_granule = current_reader.numPendingRowsInCurrentGranule();
    if (unread_rows_in_current_granule >= rows_to_read)
        return rows_to_read;

    const MergeTreeIndexGranularity & index_granularity = current_task.data_part->index_granularity;

    return index_granularity.countMarksForRows(current_reader.currentMark(), rows_to_read, current_reader.numReadRowsInCurrentGranule(), min_marks_to_read);
}


static bool hasPrefix(const String & s, const String & pre)
{
    return s.size() >= pre.size() && s.find(pre) == 0;
}


static bool isVectorSearchByPk(const std::vector<String> & a, const std::vector<String> & b)
{
    size_t na = a.size();
    size_t nb = b.size();
    if (na + 1 != nb)
    {
        return false;
    }
    for (size_t i = 0; i < na; ++i)
    {
        if (a[i] != b[i])
        {
            return false;
        }
    }
    return hasPrefix(b[nb - 1], "distance") || hasPrefix(b[nb - 1], "batch_distance");
}

IMergeTreeSelectAlgorithm::BlockAndProgress IMergeTreeSelectAlgorithm::readFromPartImpl()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;
    const UInt64 current_preferred_block_size_bytes = preferred_block_size_bytes;
    const UInt64 current_preferred_max_column_in_block_size_bytes = preferred_max_column_in_block_size_bytes;
    const double min_filtration_ratio = 0.00001;

    auto read_start_time = std::chrono::system_clock::now();

    UInt64 recommended_rows = estimateNumRows(*task, current_preferred_block_size_bytes,
                                              current_max_block_size_rows, current_preferred_max_column_in_block_size_bytes, min_filtration_ratio, min_marks_to_read);
    UInt64 rows_to_read = std::max(static_cast<UInt64>(1), std::min(current_max_block_size_rows, recommended_rows));

    bool pk_cache_side = false;

    const KeyDescription & pk_description = storage_snapshot->metadata->getPrimaryKey();
    const size_t pk_col_size = pk_description.column_names.size();

    const bool enable_primary_key_cache = task->data_part->storage.canUsePrimaryKeyCache();
    LOG_DEBUG(log, "Setting enable_primary_key_cache = {}", enable_primary_key_cache);

    if (enable_primary_key_cache && task->vector_scan_manager && !task->data_part->hasLightweightDelete())
    {
        /// consider cache
        /// if and only if
        /// 1. this task is vector search
        /// 2. primary key is only a column, and select columns is (pk, distance) or (pk, batch_distance)
        /// 3. primary key's value is represented by number
        bool match = PrimaryKeyCacheManager::isSupportedPrimaryKey(pk_description)
            && isVectorSearchByPk(pk_description.column_names, task->task_columns.columns.getNames());

        pk_cache_side = match;
    }

    if (pk_cache_side)
    {
        LOG_DEBUG(log, "Entry pk cache side --- yes");
    }
    else
    {
        LOG_DEBUG(log, "Entry pk cache side --- no");
    }

    const String cache_key = task->data_part->getDataPartStorage().getRelativePath()+":"+task->data_part->name;

    if (pk_cache_side)
    {
        std::optional<Columns> cols_opt = PrimaryKeyCacheManager::getMgr().getPartPkCache(cache_key);

        if (cols_opt.has_value())
        {
            LOG_DEBUG(log, "Hit cache, and key is {}", cache_key);

            Columns cols = cols_opt.value();

            /// cut columns

            MutableColumns temp_columns;
            temp_columns.resize(pk_col_size);
            for (size_t i = 0; i < pk_col_size; ++i)
            {
                temp_columns[i] = pk_description.data_types[i]->createColumn();
            }

            MergeTreeRangeReader::ReadResult::ReadRangesInfo read_ranges;
            const MergeTreeIndexGranularity & index_granularity = task->data_part->index_granularity;

            for (auto it = task->mark_ranges.begin(); it != task->mark_ranges.cend(); ++it)
            {
                const MarkRange mr = *it;
                size_t start_row = index_granularity.getMarkStartingRow(mr.begin);
                size_t stop_row = index_granularity.getMarkStartingRow(mr.end);

                MergeTreeRangeReader::ReadResult::ReadRangeInfo rri;
                rri.start_row = start_row;
                rri.row_num = stop_row - start_row;
                rri.start_mark = mr.begin;
                rri.end_mark = mr.end;
                read_ranges.push_back(rri);

                for (size_t i = 0; i < pk_col_size; ++i)
                {
                    temp_columns[i]->insertRangeFrom(*cols[i], start_row, stop_row - start_row);
                }
            }

            Columns result_columns;
            result_columns.assign(
                std::make_move_iterator(temp_columns.begin()),
                std::make_move_iterator(temp_columns.end())
                );

            LOG_DEBUG(log, "Fetch from cache size = {}", result_columns[0]->size());


            if (task->vector_scan_manager->preComputed())
            {
                size_t result_row_num = 0;

                task->vector_scan_manager->mergeResult(
                    result_columns, /// _Inout_
                    result_row_num, /// _Out_
                    read_ranges,
                    nullptr,
                    nullptr);

                if (result_row_num > 0)
                {
                    task->mark_ranges.clear();
                    BlockAndProgress res = { header_without_const_virtual_columns.cloneWithColumns(result_columns), result_row_num };
                    return res;
                }
            }
        }
        else
        {
            LOG_DEBUG(log, "Miss cache, and data part name is {}", cache_key);
        }
    }

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    const auto & sample_block = task->range_reader.getSampleBlock();
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent number of columns got from MergeTreeRangeReader. "
                        "Have {} in sample block and {} columns in list",
                        toString(sample_block.columns()), toString(read_result.columns.size()));

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
        const String col_name = sample_block.getByPosition(ps).name;
        /// TODO: not add distance column to header_without_virtual_columns
        if (isVectorScanFunc(col_name))
        {
            which_cut = ps;
            vector_scan_col_name = col_name;
            continue;
        }

        ColumnPtr column_ptr = read_result.columns[ps];

        /// Copy _part_offset column
        if (col_name == "_part_offset")
        {
            part_offset = typeid_cast<const ColumnUInt64 *>(column_ptr.get());
        }

        ordered_columns.emplace_back(std::move(read_result.columns[ps]));
    }

    auto read_end_time = std::chrono::system_clock::now();

    LOG_DEBUG(log, "Read time: {}", std::chrono::duration_cast<std::chrono::milliseconds>(read_end_time - read_start_time).count());

    /// [MQDB] vector search
    if (task->vector_scan_manager)
    {
        /// already perform vector scan
        if (task->vector_scan_manager->preComputed())
        {
            task->vector_scan_manager->mergeResult(
                ordered_columns,
                read_result.num_rows,
                read_ranges,
                nullptr, // filter
                part_offset);
        }
        else
        {
            VectorIndexBitmapPtr filter = nullptr;
            if (read_result.final_filter.present())
            {
                filter = std::make_shared<VectorIndexBitmap>(read_result.num_rows);
                for (size_t i = 0; i < read_result.final_filter.getData().size(); i++)
                {
                    if (read_result.final_filter.getData()[i])
                    {
                        filter->set(i);
                    }
                }
            }
            task->vector_scan_manager->executeAfterRead(
                task->data_part,
                ordered_columns,
                read_result.num_rows,
                read_ranges,
                this->prewhere_info != nullptr,
                filter);
        }
    }
    part_offset = nullptr; // after merge, it became invalid

    if (pk_cache_side)
    {
        /// load pk's bin to memory
        Columns pk_columns;
        bool r = readPrimaryKeyBin(pk_columns);

        if (r)
        {
            PrimaryKeyCacheManager::getMgr().setPartPkCache(cache_key, pk_columns);
        }
    }

    if (!task->vector_scan_manager)
    {
        LOG_DEBUG(log, "This task's vector_scan_manager is NIL");

        Block block;
        if (read_result.num_rows != 0)
            block = sample_block.cloneWithColumns(ordered_columns);

        BlockAndProgress res = {
            .block = std::move(block),
            .row_count = read_result.num_rows,
            .num_read_rows = num_read_rows,
            .num_read_bytes = num_read_bytes };

        return res;
    }

    const size_t final_result_nun_rows = read_result.num_rows;

    Block res_block;
    for (size_t i = 0; i < ordered_columns.size(); ++i)
    {
        ColumnWithTypeAndName ctn;
        ctn.column = ordered_columns[i];

        if (i < ordered_columns.size() -1)
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

        res_block.insert(ctn);
    }

    /// Remove _part_offset column from result block, it's only used for vector scan manager.
    if (need_remove_part_offset)
    {
        res_block.erase("_part_offset");
    }

    BlockAndProgress res = {res_block, final_result_nun_rows, num_read_rows, num_read_bytes};
    return res;
}


bool IMergeTreeSelectAlgorithm::readPrimaryKeyBin(Columns & out_columns)
{
    const KeyDescription & primary_key = storage_snapshot->metadata->getPrimaryKey();
    const size_t pk_columns_size = primary_key.column_names.size();

    NamesAndTypesList cols;
    const std::vector<String> pk_column_names = primary_key.column_names;
    for (const String & col_name : pk_column_names)
    {
        std::optional<NameAndTypePair> nt = storage_snapshot->metadata->getColumns().getAllPhysical().tryGetByName(col_name);
        if (nt)
        {
            cols.emplace_back(*nt);
        }
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

    MergeTreeReaderPtr pk_reader = task->data_part->getReader(
        cols,
        storage_snapshot->metadata,
        MarkRanges{MarkRange(0, task->data_part->getMarksCount())},
        nullptr,
        storage.getContext()->getMarkCache().get(),
        task->alter_conversions,
        reader_settings,
        {},
        {});

    if (!pk_reader)
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

        size_t num_rows = pk_reader->readRows(current_mark, 0, continue_read, remaining_size, result);

        continue_read = true;
        num_rows_read += num_rows;

        for (size_t i = 0; i < cols_size; ++i)
        {
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


IMergeTreeSelectAlgorithm::BlockAndProgress IMergeTreeSelectAlgorithm::readFromPart()
{
    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    return readFromPartImpl();
}

namespace
{
    struct VirtualColumnsInserter
    {
        explicit VirtualColumnsInserter(Block & block_) : block(block_) {}

        bool columnExists(const String & name) const { return block.has(name); }

        void insertUInt8Column(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeUInt8>(), name});
        }

        void insertUInt64Column(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeUInt64>(), name});
        }

        void insertUUIDColumn(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeUUID>(), name});
        }

        void insertLowCardinalityColumn(const ColumnPtr & column, const String & name)
        {
            block.insert({column, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), name});
        }

        void insertPartitionValueColumn(
            size_t rows, const Row & partition_value, const DataTypePtr & partition_value_type, const String & name)
        {
            ColumnPtr column;
            if (rows)
                column = partition_value_type->createColumnConst(rows, Tuple(partition_value.begin(), partition_value.end()))
                             ->convertToFullColumnIfConst();
            else
                column = partition_value_type->createColumn();

            block.insert({column, partition_value_type, name});
        }

        Block & block;
    };
}

/// Adds virtual columns that are not const for all rows
static void injectNonConstVirtualColumns(
    size_t rows,
    Block & block,
    const Names & virtual_columns)
{
    VirtualColumnsInserter inserter(block);
    for (const auto & virtual_column_name : virtual_columns)
    {
        if (virtual_column_name == "_part_offset")
        {
            if (!rows)
            {
                inserter.insertUInt64Column(DataTypeUInt64().createColumn(), virtual_column_name);
            }
            else
            {
                if (!inserter.columnExists(virtual_column_name))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Column {} must have been filled part reader",
                        virtual_column_name);
            }
        }

        if (virtual_column_name == LightweightDeleteDescription::FILTER_COLUMN.name)
        {
                /// If _row_exists column isn't present in the part then fill it here with 1s
                ColumnPtr column;
                if (rows)
                    column = LightweightDeleteDescription::FILTER_COLUMN.type->createColumnConst(rows, 1)->convertToFullColumnIfConst();
                else
                    column = LightweightDeleteDescription::FILTER_COLUMN.type->createColumn();

                inserter.insertUInt8Column(column, virtual_column_name);
        }
    }
}

/// Adds virtual columns that are const for the whole part
static void injectPartConstVirtualColumns(
    size_t rows,
    Block & block,
    MergeTreeReadTask * task,
    const DataTypePtr & partition_value_type,
    const Names & virtual_columns)
{
    VirtualColumnsInserter inserter(block);
    /// add virtual columns
    /// Except _sample_factor, which is added from the outside.
    if (!virtual_columns.empty())
    {
        if (unlikely(rows && !task))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert virtual columns to non-empty chunk without specified task.");

        const IMergeTreeDataPart * part = nullptr;
        if (rows)
        {
            part = task->data_part.get();
            if (part->isProjectionPart())
                part = part->getParentPart();
        }
        for (const auto & virtual_column_name : virtual_columns)
        {
            if (virtual_column_name == "_part")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}
                                 .createColumnConst(rows, part->name)
                                 ->convertToFullColumnIfConst();
                else
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn();

                inserter.insertLowCardinalityColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, task->part_index_in_query)->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                inserter.insertUInt64Column(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_uuid")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUUID().createColumnConst(rows, part->uuid)->convertToFullColumnIfConst();
                else
                    column = DataTypeUUID().createColumn();

                inserter.insertUUIDColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_id")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}
                                 .createColumnConst(rows, part->info.partition_id)
                                 ->convertToFullColumnIfConst();
                else
                    column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn();

                inserter.insertLowCardinalityColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_value")
            {
                if (rows)
                    inserter.insertPartitionValueColumn(rows, part->partition.value, partition_value_type, virtual_column_name);
                else
                    inserter.insertPartitionValueColumn(rows, {}, partition_value_type, virtual_column_name);
            }
        }
    }
}

void IMergeTreeSelectAlgorithm::injectVirtualColumns(
    Block & block, size_t row_count, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    /// First add non-const columns that are filled by the range reader and then const columns that we will fill ourselves.
    /// Note that the order is important: virtual columns filled by the range reader must go first
    injectNonConstVirtualColumns(row_count, block, virtual_columns);
    injectPartConstVirtualColumns(row_count, block, task, partition_value_type, virtual_columns);
}

Block IMergeTreeSelectAlgorithm::applyPrewhereActions(Block block, const PrewhereInfoPtr & prewhere_info)
{
    if (prewhere_info)
    {
        if (prewhere_info->row_level_filter)
        {
            block = prewhere_info->row_level_filter->updateHeader(std::move(block));
            auto & row_level_column = block.getByName(prewhere_info->row_level_column_name);
            if (!row_level_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Invalid type for filter in PREWHERE: {}",
                    row_level_column.type->getName());
            }

            block.erase(prewhere_info->row_level_column_name);
        }

        if (prewhere_info->prewhere_actions)
        {
            block = prewhere_info->prewhere_actions->updateHeader(std::move(block));

            auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);
            if (!prewhere_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Invalid type for filter in PREWHERE: {}",
                    prewhere_column.type->getName());
            }

            if (prewhere_info->remove_prewhere_column)
            {
                block.erase(prewhere_info->prewhere_column_name);
            }
            else if (prewhere_info->need_filter)
            {
                WhichDataType which(removeNullable(recursiveRemoveLowCardinality(prewhere_column.type)));

                if (which.isNativeInt() || which.isNativeUInt())
                    prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
                else if (which.isFloat())
                    prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1.0f)->convertToFullColumnIfConst();
                else
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                        "Illegal type {} of column for filter",
                        prewhere_column.type->getName());
            }
        }
    }

    return block;
}

Block IMergeTreeSelectAlgorithm::transformHeader(
    Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    auto transformed = applyPrewhereActions(std::move(block), prewhere_info);
    injectVirtualColumns(transformed, 0, nullptr, partition_value_type, virtual_columns);
    return transformed;
}

std::unique_ptr<MergeTreeBlockSizePredictor> IMergeTreeSelectAlgorithm::getSizePredictor(
    const MergeTreeData::DataPartPtr & data_part,
    const MergeTreeReadTaskColumns & task_columns,
    const Block & sample_block)
{
    const auto & required_column_names = task_columns.columns.getNames();
    NameSet complete_column_names(required_column_names.begin(), required_column_names.end());
    for (const auto & pre_columns_per_step : task_columns.pre_columns)
    {
        const auto & required_pre_column_names = pre_columns_per_step.getNames();
        complete_column_names.insert(required_pre_column_names.begin(), required_pre_column_names.end());
    }

    return std::make_unique<MergeTreeBlockSizePredictor>(
        data_part, Names(complete_column_names.begin(), complete_column_names.end()), sample_block);
}


IMergeTreeSelectAlgorithm::~IMergeTreeSelectAlgorithm() = default;

}
