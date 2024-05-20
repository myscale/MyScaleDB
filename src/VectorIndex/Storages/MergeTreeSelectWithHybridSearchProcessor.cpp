#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeSelectWithVSProcessor.h>
#include <Storages/MergeTree/MergeTreeVectorScanUtils.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PrimaryKeyCacheManager.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Storages/MergeTreeSelectWithHybridSearchProcessor.h>
#include <VectorIndex/Utils/VSUtils.h>
#include <VectorIndex/Cache/PKCacheManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
}

/// Check if only select primary key column and vector search/text search/hybrid search functions. 
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
        if ((read_col_name == pk_col_name) || isHybridSearchFunc(read_col_name))
            continue;
        else
        {
            match = false;
            break;
        }
    }

    return match;
}

MergeTreeSelectWithHybridSearchProcessor::MergeTreeSelectWithHybridSearchProcessor(
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const RangesInDataPart & part_with_ranges_,
    VirtualFields shared_virtual_fields_,
    Names required_columns_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
    const MergeTreeReaderSettings & reader_settings_,
    MergeTreeBaseSearchManagerPtr base_search_manager_)
    : storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , prewhere_actions(MergeTreeSelectProcessor::getPrewhereActions(prewhere_info, actions_settings, reader_settings_.enable_multiple_prewhere_read_steps))
    , reader_settings(reader_settings_)
    , block_size_params(block_size_params_)
    , use_uncompressed_cache(use_uncompressed_cache_)
    , owned_uncompressed_cache(use_uncompressed_cache ? storage.getContext()->getUncompressedCache() : nullptr)
    , owned_mark_cache(storage.getContext()->getMarkCache())
    , required_columns{required_columns_}
    , shared_virtual_fields(shared_virtual_fields_)
    , part_with_ranges(part_with_ranges_)
    , data_part{part_with_ranges.data_part}
    , sample_block(storage_snapshot_->metadata->getSampleBlock())
    , all_mark_ranges(part_with_ranges.ranges)
    , total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
    , base_search_manager(base_search_manager_)
{
    auto header = storage_snapshot_->getSampleBlockForColumns(required_columns);
    result_header = SourceStepWithFilter::applyPrewhereActions(std::move(header), prewhere_info);

    if (reader_settings.apply_deleted_mask)
    {
        PrewhereExprStep step
        {
            .type = PrewhereExprStep::Filter,
            .actions = nullptr,
            .filter_column_name = RowExistsColumn::name,
            .remove_filter_column = true,
            .need_filter = true,
            .perform_alter_conversions = true,
        };

        lightweight_delete_filter_step = std::make_shared<PrewhereExprStep>(std::move(step));
    }

    if (!prewhere_actions.steps.empty())
        LOG_TRACE(log, "PREWHERE condition was split into {} steps: {}", prewhere_actions.steps.size(), prewhere_actions.dumpConditions());

    if (prewhere_info)
        LOG_TEST(log, "Original PREWHERE DAG:\n{}\nPREWHERE actions:\n{}",
            prewhere_info->prewhere_actions.dumpDAG(),
            (!prewhere_actions.steps.empty() ? prewhere_actions.dump() : std::string("<nullptr>")));

    ordered_names = result_header.getNames();

    LOG_TRACE(
        log,
        "Reading {} ranges in order from part {}, approx. {} rows starting from {}",
        all_mark_ranges.size(),
        data_part->name,
        total_rows,
        data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));

    /// Save original remove_prewhere_column, which will be changed to true in performPrefilter()
    if (prewhere_info)
        original_remove_prewhere_column = prewhere_info->remove_prewhere_column;
}


bool MergeTreeSelectWithVSProcessor::getNewTask()
{
    if (getNewTaskImpl())
        return true;

    return false;
}

ChunkAndProgress MergeTreeSelectWithVSProcessor::read()
{
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
            /// Reorder the columns according to result_header
            Columns ordered_columns;
            ordered_columns.reserve(result_header.columns());
            for (size_t i = 0; i < result_header.columns(); ++i)
            {
                auto name = result_header.getByPosition(i).name;
                ordered_columns.push_back(res.block.getByName(name).column);
            }

            auto chunk = Chunk(ordered_columns, res.row_count);

            return ChunkAndProgress{
                .chunk = std::move(chunk),
                .num_read_rows = res.num_read_rows,
                .num_read_bytes = res.num_read_bytes,
                .is_finished = false};
        }

        return {Chunk(), res.num_read_rows, res.num_read_bytes, false};
    }

    return {Chunk(), 0, 0, true};
}

void MergeTreeSelectWithVSProcessor::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    data_part.reset();
}

VIBitmapPtr MergeTreeSelectWithHybridSearchProcessor::performPrefilter(MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::performPrefilter()");
    Names requried_columns;

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
    Names all_pre_column_names = prewhere_info->prewhere_actions.getRequiredColumnsNames();

    for (const auto & name : all_pre_column_names)
    {
        if (pre_name_set.contains(name))
            continue;
        requried_columns.push_back(name);
        pre_name_set.insert(name);
    }


    /// Add _part_offset column
    required_columns.push_back("_part_offset");

    /// No need to return prewhere column
    {
        std::lock_guard lock(prewhere_info->prewhere_info_mutex);
        if (!prewhere_info->remove_prewhere_column)
            prewhere_info->remove_prewhere_column = true;
    }

    /// Refactor to use pool, reference from readInOrder()
    MergeTreeReadPoolPtr pool;

    auto context = storage.getContext();
    const auto & settings = context->getSettingsRef();

    MergeTreeReadPoolBase::PoolSettings pool_settings
    {
        .threads = /*max_streams*/ 1,
        .sum_marks = part_with_ranges.getMarksCount(),
        //.min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .preferred_block_size_bytes = settings.preferred_block_size_bytes,
        .use_uncompressed_cache = use_uncompressed_cache,
        .use_const_size_tasks_for_remote_reading = settings.merge_tree_use_const_size_tasks_for_remote_reading,
    };

    RangesInDataParts parts_with_ranges;
    parts_with_ranges.emplace_back(data_part, std::make_shared<AlterConversions>(), 0, mark_ranges);

    pool = std::make_shared<MergeTreeReadPoolInOrder>(
            /*has_limit_below_one_block*/ false,
            MergeTreeReadType::Default,
            parts_with_ranges,
            shared_virtual_fields,
            storage_snapshot,
            prewhere_info,
            actions_settings,
            reader_settings,
            required_columns,
            pool_settings,
            context);

    auto algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(0);

    auto processor = std::make_unique<MergeTreeSelectProcessor>(
            pool, std::move(algorithm), prewhere_info,
            actions_settings, block_size_params, reader_settings);

    auto source = std::make_shared<MergeTreeSource>(std::move(processor), storage.getLogName());

    Pipe pipe(std::move(source));

    QueryPipeline filter_pipeline(std::move(pipe));
    PullingPipelineExecutor filter_executor(filter_pipeline);

    size_t num_rows = data_part->rows_count;

    Block block;
    VIBitmapPtr filter = std::make_shared<VIBitmap>(num_rows);
    {
        OpenTelemetry::SpanHolder span_pipe("MergeTreeSelectWithHybridSearchProcessor::performPrefilter()::StartPipe");
        while (filter_executor.pull(block))
        {
            const PaddedPODArray<UInt64> & col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column).getData();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                filter->set(col_data[i]);
            }
        }
    }

    return filter;
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

    MergeTreeReaderPtr reader = data_part->getReader(
        cols,
        storage_snapshot,
        MarkRanges{MarkRange(0, data_part->getMarksCount())},
        /*virtual_fields=*/ {},
        nullptr,
        storage.getContext()->getMarkCache().get(),
        part_with_ranges.alter_conversions,
        reader_settings,
        {},
        {});

    if (!reader)
    {
        LOG_ERROR(log, "Failed to get reader");
        return false;
    }

    /// begin to read
    const MergeTreeIndexGranularity & index_granularity = data_part->index_granularity;

    size_t current_mark = 0;
    const size_t total_mark = data_part->getMarksCount();

    size_t num_rows_read = 0;
    const size_t num_rows_total = data_part->rows_count;

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

void MergeTreeSelectWithHybridSearchProcessor::initializeRangeReaders()
{
    PrewhereExprInfo all_prewhere_actions;
    if (lightweight_delete_filter_step && task->getInfo().data_part->hasLightweightDelete())
        all_prewhere_actions.steps.push_back(lightweight_delete_filter_step);

    for (const auto & step : prewhere_actions.steps)
        all_prewhere_actions.steps.push_back(step);

    task->initializeRangeReaders(all_prewhere_actions);
}

MergeTreeReadTask::BlockAndProgress MergeTreeSelectWithHybridSearchProcessor::readFromPart()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::readFromPart()");
    if (!task->getMainRangeReader().isInitialized())
    {
        /// Initialize primary key cache
        const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
        const bool enable_primary_key_cache = data_part->storage.canUsePrimaryKeyCache();
        LOG_DEBUG(log, "Reader setting: enable_primary_key_cache = {}", enable_primary_key_cache);

        /// consider cache if and only if
        /// 1. this task is vector search and no prewhere info
        /// 2. primary key is only a column, and select columns are (pk, hybrid_search_func)
        /// 3. primary key's value is represented by number
        if (enable_primary_key_cache)
        {
            use_primary_key_cache = PKCacheManager::isSupportedPrimaryKey(primary_key)
                && isHybridSearchByPk(primary_key.column_names, ordered_names);
        }
/*
        /// TODO: handle virtual columns
        /// Add _part_offset to non_const_virtual_column_names if part has lightweight delete
        if (data_part->hasLightweightDelete())
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
*/
        initializeRangeReaders();
    }

    /// original read logic, considering prewhere optimization
    return readFromPartWithHybridSearch();
}

/// perform actual read and result merge operation, prewhere has been processed ahead
/// Referenced from MergeTreeReadTask::read()
MergeTreeReadTask::BlockAndProgress MergeTreeSelectWithHybridSearchProcessor::readFromPartWithHybridSearch()
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::readFromPartWithHybridSearch()");

    const UInt64 current_max_block_size_rows = block_size_params.max_block_size_rows;

    auto read_start_time = std::chrono::system_clock::now();
    UInt64 rows_to_read = std::max(UInt64(1), current_max_block_size_rows);

    if (use_primary_key_cache)
    {
        bool success = false;
        auto res = readFromPartWithPrimaryKeyCache(success);
        
        if (success)
            return res;
    }

    auto read_result = task->range_readers.main.read(rows_to_read, task->mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    /// const auto & sample_block = task->getMainRangeReader().getSampleBlock();
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inconsistent number of columns got from MergeTreeRangeReader. Have {} in sample block and {} columns in list",
            toString(sample_block.columns()),
            toString(read_result.columns.size()));

    /// TODO: check columns have the same types as in header.

    /// progress({ read_result.numReadRows(), read_result.numBytesRead() });
    size_t num_read_rows = read_result.numReadRows();
    size_t num_read_bytes = read_result.numBytesRead();

    auto read_ranges = read_result.readRanges();

    if (read_result.num_rows == 0)
        return {Block(), read_result.num_rows, num_read_rows, num_read_bytes};

    /// Remove distance_func column from read_result.columns, it will be added by vector search.
    Columns ordered_columns;
    if (base_search_manager)
        ordered_columns.reserve(sample_block.columns() - 1);
    else
        ordered_columns.reserve(sample_block.columns());

    size_t which_cut = 0;
    String vector_scan_col_name;
    for (size_t ps = 0; ps < sample_block.columns(); ++ps)
    {
        auto & col_name = sample_block.getByPosition(ps).name;

        /// TODO: not add distance column to header_without_virtual_columns
        if (isHybridSearchFunc(col_name))
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
    if (base_search_manager && base_search_manager->preComputed())
    {
        /// already perform vector scan   
        base_search_manager->mergeResult(
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

        const auto & node = prewhere_info->prewhere_actions.findInOutputs(prewhere_info->prewhere_column_name);
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

    MergeTreeReadTask::BlockAndProgress res = {res_block, final_result_num_rows, num_read_rows, num_read_bytes};

    return res;
}

IMergeTreeSelectAlgorithm::BlockAndProgress MergeTreeSelectWithHybridSearchProcessor::readFromPartWithPrimaryKeyCache(bool & success)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::readFromPartUsePrimaryKeyCache()");
    LOG_DEBUG(log, "Use primary key cache");

    const String cache_key = data_part->getDataPartStorage().getRelativePath() + ":" + data_part->name;

    std::optional<Columns> pk_cache_cols_opt = PKCacheManager::getMgr().getPartPkCache(cache_key);

    /// The columns of pk cache obtained by PKCacheManager may be empty
    if (pk_cache_cols_opt.has_value() && !pk_cache_cols_opt.value().empty())
    {
        LOG_DEBUG(log, "Hit primary key cache for part {}, and key is {}", data_part->name, cache_key);
    }
    else
    {
        LOG_DEBUG(log, "Miss primary key cache for part {}, will load", data_part->name);

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
            LOG_DEBUG(log, "Failed to load primary key column for part {}, will back to normal read",  data_part->name);
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
/*
    for (const auto & column_name : non_const_virtual_column_names)
    {
        if (column_name == "_part_offset")
        {
            mutable_part_offset_col = ColumnUInt64::create();
            break;
        }
    }
*/
    MergeTreeRangeReader::ReadResult::ReadRangesInfo read_ranges;
    const MergeTreeIndexGranularity & index_granularity = data_part->index_granularity;

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

    /// Get _part_offset if exists.
    if (mutable_part_offset_col)
    {
        /// _part_offset column exists in original select columns
        if (!need_remove_part_offset)
        {
            tmp_result_columns.emplace_back(std::move(mutable_part_offset_col));
            part_offset = typeid_cast<const ColumnUInt64 *>(tmp_result_columns.back().get());
        }
        else
            part_offset = typeid_cast<const ColumnUInt64 *>(mutable_part_offset_col.get());
    }

    if (base_search_manager && base_search_manager->preComputed())
    {
        size_t result_row_num = 0;

        base_search_manager->mergeResult(
            tmp_result_columns, /// _Inout_
            result_row_num, /// _Out_
            read_ranges,
            nullptr,
            part_offset);

        Columns result_columns;

        if(!need_remove_part_offset){
            result_columns = tmp_result_columns;
        }else{
            result_columns.emplace_back(tmp_result_columns[0]);
            result_columns.emplace_back(tmp_result_columns.back());
        }


        task->mark_ranges.clear();
        if (result_row_num > 0)
        {
            MergeTreeReadTask::BlockAndProgress res = {result_header.cloneWithColumns(result_columns), result_row_num};
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

    MarkRanges mark_ranges_for_task;
    mark_ranges_for_task = std::move(all_mark_ranges);
    all_mark_ranges.clear();

    /// perform vector scan, then filter mark ranges of read task
    if (!prewhere_info)
    {
        base_search_manager->executeSearchBeforeRead(data_part);
        filterMarkRangesByVectorScanResult(data_part, base_search_manager, mark_ranges_for_task);
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
        base_search_manager->executeSearchWithFilter(data_part, read_ranges, filter);
        filterMarkRangesByVectorScanResult(data_part, base_search_manager, mark_ranges_for_task);
    }

    for (const auto & range : mark_ranges_for_task)
        LOG_DEBUG(log, "Keep range: {} - {}", range.begin, range.end);
    
    if (mark_ranges_for_task.empty())
        return false;

    /// Initilize MergeTreeReadTask after vector scan
    task = createTask(mark_ranges_for_task);

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part);
    throw;
}

MergeTreeReadTaskPtr MergeTreeSelectWithVSProcessor::createTask(MarkRanges ranges) const
{
    /// reader and pre_reader_for_step are put inside MergeTreeReadTask
    auto read_task_info = initializeReadTaskInfo();
    auto extras = getExtras();

    MergeTreeReadTask::Readers task_readers = MergeTreeReadTask::createReaders(read_task_info, extras, ranges);

    auto task_size_predictor = read_task_info->shared_size_predictor
        ? std::make_unique<MergeTreeBlockSizePredictor>(*read_task_info->shared_size_predictor)
        : nullptr; /// make a copy

    return std::make_unique<MergeTreeReadTask>(
        read_task_info,
        std::move(task_readers),
        std::move(ranges),
        std::move(task_size_predictor));
}

MergeTreeReadTask::Extras MergeTreeSelectWithVSProcessor::getExtras() const
{
    return
    {
        .uncompressed_cache = owned_uncompressed_cache.get(),
        .mark_cache = owned_mark_cache.get(),
        .reader_settings = reader_settings,
        .storage_snapshot = storage_snapshot,
        ///.profile_callback = profile_callback,
    };
}

MergeTreeReadTaskInfoPtr MergeTreeSelectWithVSProcessor::initializeReadTaskInfo() const
{
    MergeTreeReadTaskInfo read_task_info;
    
    read_task_info.data_part = data_part;
    read_task_info.part_index_in_query = part_with_ranges.part_index_in_query;
    read_task_info.alter_conversions = part_with_ranges.alter_conversions;

    LoadedMergeTreeDataPartInfoForReader part_info(part_with_ranges.data_part, part_with_ranges.alter_conversions);

    read_task_info.task_columns = getReadTaskColumns(
        part_info,
        storage_snapshot,
        required_columns,
        prewhere_info,
        actions_settings,
        reader_settings,
        /*with_subcolumns=*/true);

    read_task_info.const_virtual_fields = shared_virtual_fields;
    read_task_info.const_virtual_fields.emplace("_part_index", read_task_info.part_index_in_query);

    return std::make_shared<MergeTreeReadTaskInfo>(std::move(read_task_info));
}

}
