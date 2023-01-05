#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeSelectWithVectorScanProcessor.h>
#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeVectorScanUtils.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

void MergeTreeSelectWithVectorScanProcessor::initializeReadersWithVectorScan()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::initializeReadersWithVectorScan()");
    task_columns = getReadTaskColumns(
        LoadedMergeTreeDataPartInfoForReader(data_part), storage_snapshot,
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

    LOG_DEBUG(log, "[initializeReadersWithVectorScan] task column: {}", task_columns.columns.toString());

    reader = data_part->getReader(task_columns.columns, storage_snapshot->getMetadataForQuery(),
        all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings,
        {}, {});

    pre_reader_for_step.clear();

    /// Add lightweight delete filtering step
    if (reader_settings.apply_deleted_mask && data_part->hasLightweightDelete())
    {
        pre_reader_for_step.push_back(data_part->getReader({LightweightDeleteDescription::FILTER_COLUMN}, storage_snapshot->getMetadataForQuery(),
            all_mark_ranges, owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings,
            {}, {}));
    }
}

ColumnPtr MergeTreeSelectWithVectorScanProcessor::performPrefilter(MarkRanges & mark_ranges)
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

    bool bak_remove_prewhere_column = prewhere_info->remove_prewhere_column;
    prewhere_info->remove_prewhere_column = true;

    /// need_filter is false when both prewhere and where exist, prewhere will be delayed, all read rows with a prehwere_column returned.
    /// In this case, we need only rows statisfied prewhere conditions.
    prewhere_info->need_filter = true;

    auto algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(
        storage,
        storage_snapshot,
        data_part,
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
    auto new_filter = ColumnUInt8::create(num_rows, 0);
    IColumn::Filter & new_data = new_filter->getData();

    /// new_data.resize_fill(num_rows, 0);
    OpenTelemetry::SpanHolder span_pipe("MergeTreeSelectWithVectorScanProcessor::performPrefilter():StartPipe");
    while (filter_executor.pull(block))
    {
        /*
        LOG_DEBUG(log, "[performPrefilter] block column size: {}", block.getNames().size());
        for (const auto & name : block.getNames())
        {
            LOG_DEBUG(log, "[performPrefilter] block column: {}", name);
        }
        */
        // OpenTelemetry::SpanHolder span_pipe("MergeTreeSelectWithVectorScanProcessor::performPrefilter():StartPipe::CopyToFilter");
        const PaddedPODArray<UInt64>& col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column)->getData();
        for (size_t i = 0; i < block.rows(); ++i)
        {
            new_data[col_data[i]] = 1;
        }
    }

    /// Restore the remove_prewhere_column.
    prewhere_info->remove_prewhere_column = bak_remove_prewhere_column;

    return new_filter;
}

IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithVectorScanProcessor::readFromPart()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::readFromPart()");
    if (!task->range_reader.isInitialized())
    {
        MergeTreeRangeReader* prev_reader = nullptr;
        bool last_reader = false;
        /// size_t pre_readers_shift = 0;

        /// Add _part_offset to non_const_virtual_column_names if has vector_scan_manager and no prewhere_info
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

        /// Add filtering step with lightweight delete mask
        if (reader_settings.apply_deleted_mask && task->data_part->hasLightweightDelete())
        {
            task->pre_range_readers.push_back(
                MergeTreeRangeReader(pre_reader_for_step[0].get(), prev_reader, &lightweight_delete_filter_step, last_reader, non_const_virtual_column_names));
            prev_reader = &task->pre_range_readers.back();
            /// pre_readers_shift++;
        }

        task->range_reader = MergeTreeRangeReader(reader.get(), prev_reader, nullptr, true, non_const_virtual_column_names);
    }
    /// initializeRangeReaders(*task);

    /// original read logic, considering prewhere optimization
    return readFromPartWithVectorScan();
}

/// perform actual read and result merge operation, prewhere has been processed ahead
IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithVectorScanProcessor::readFromPartWithVectorScan()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithVectorScanProcessor::readFromPartWithVectorScan()");
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = max_block_size_rows;

    auto read_start_time = std::chrono::system_clock::now();
    UInt64 rows_to_read = std::max(UInt64(1), current_max_block_size_rows);

    LOG_DEBUG(log, "[readFromPartImpl] begin read, mark_ranges size = {}", task->mark_ranges.size());
    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);
    for (auto it = task->mark_ranges.begin(); it != task->mark_ranges.cend(); ++it)
    {
        LOG_DEBUG(log, "[readFromPartImpl] mark_range begin = {}, end = {}", it->begin, it->end);
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

    LOG_DEBUG(log, "[readFromPartImpl] num_rows: {}, read_rows: {}", read_result.num_rows, read_result.numReadRows());

    /// progress({ read_result.numReadRows(), read_result.numBytesRead() });
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
    ordered_columns.reserve(sample_block.columns());
    size_t which_cut = 0;
    String vector_scan_col_name;
    for (size_t ps = 0; ps < sample_block.columns(); ++ps)
    {
        auto & col_name = sample_block.getByPosition(ps).name;
        LOG_DEBUG(log, "[readFromPartImpl]: read column: {}", col_name);
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

    LOG_DEBUG(log, "[readFromPartImpl] read time: {}", std::chrono::duration_cast<std::chrono::milliseconds>(read_end_time - read_start_time).count());


    if (part_offset)
    {
        LOG_DEBUG(log, "[readFromPartImpl] offset values before vector search merge result, and the part name is {}", task->data_part->name);
        const ColumnUInt64::Container & offset_raw_value = part_offset->getData();
        const size_t the_size = part_offset->size();
        for (size_t i = 0; i < the_size && i < 10; ++i)
        {
            UInt64 v = offset_raw_value[i];
            LOG_DEBUG(log, "[readFromPartImpl] offset values --- offset[{}] = {}", i, v);
        }
    }
    /// [MQDB] vector search
    if (task->vector_scan_manager && task->vector_scan_manager->preComputed())
    {
        /// already perform vector scan   
        task->vector_scan_manager->mergeResult(
            ordered_columns,
            read_result.num_rows,
            read_ranges, FilterWithCachedCount(), part_offset);
    }

    const size_t final_result_num_rows = read_result.num_rows;

    Block res_block;

    /// Add prewhere column name to avoid column not found error
    if (prewhere_info && !prewhere_info->remove_prewhere_column)
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
        vector_scan_manager->executeBeforeRead(data_part->getDataPartStorage().getFullPath(), data_part);
        filterMarkRangesByVectorScanResult(data_part, vector_scan_manager, mark_ranges_for_task);
    }
    else
    {
        /// try to process prewhere here, get part_offset columns
        /// 1 read, then get the filtered part_offsets
        /// 2 perform vector scan based on part_offsets
        /// 3 filter mark_ranges based on vector scan results
        auto filter_col = performPrefilter(mark_ranges_for_task);
        /// auto filter = typeid_cast<const ColumnUInt8 *>(filter_col.get());
        ReadRanges read_ranges;
        ReadRange read_range{0, data_part->rows_count, 0, data_part->index_granularity.getMarksCount()};
        read_ranges.emplace_back(read_range);
        vector_scan_manager->executeVectorScanWithFilter(data_part->getDataPartStorage().getFullPath(), data_part, read_ranges, FilterWithCachedCount(filter_col));
        filterMarkRangesByVectorScanResult(data_part, vector_scan_manager, mark_ranges_for_task);
        /// prewhere_info = nullptr;
    }

    for (const auto & range : mark_ranges_for_task)
    {
        LOG_DEBUG(log, "[getNewTaskImpl] keep range: {} - {}", range.begin, range.end);
    }
    
    if (mark_ranges_for_task.empty())
    {
        return false;
    }

    task = std::make_unique<MergeTreeReadTask>(
        data_part,
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
