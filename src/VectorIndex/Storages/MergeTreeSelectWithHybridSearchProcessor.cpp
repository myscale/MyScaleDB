#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/scope_guard_safe.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Storages/MergeTreeSelectWithHybridSearchProcessor.h>
#include <VectorIndex/Storages/MergeTreeHybridSearchManager.h>
#include <VectorIndex/Storages/MergeTreeTextSearchManager.h>
#include <VectorIndex/Storages/MergeTreeWithVectorScanSource.h>
#include <VectorIndex/Utils/VSUtils.h>
#include <VectorIndex/Cache/PKCacheManager.h>

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectHybridSearchThreads;
    extern const Metric MergeTreeDataSelectHybridSearchThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
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
    MergeTreeBaseSearchManagerPtr base_search_manager_,
    ContextPtr context_,
    size_t max_streams,
    bool can_skip_perform_prefilter_)
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
    , all_mark_ranges(part_with_ranges.ranges)
    , total_rows(data_part->index_granularity.getRowsCountInRanges(all_mark_ranges))
    , base_search_manager(base_search_manager_)
    , context(context_)
    , max_streams_for_prewhere(max_streams)
    , can_skip_perform_prefilter(can_skip_perform_prefilter_)
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
}


bool MergeTreeSelectWithHybridSearchProcessor::getNewTask()
{
    if (getNewTaskImpl())
        return true;

    return false;
}

ChunkAndProgress MergeTreeSelectWithHybridSearchProcessor::read()
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

void MergeTreeSelectWithHybridSearchProcessor::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    data_part.reset();
}

bool MergeTreeSelectWithHybridSearchProcessor::canSkipPrewhereForPart(
    const MergeTreeData::DataPartPtr & data_part_,
    const PrewhereInfoPtr & prewhere_info_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr context_)
{
    if (!prewhere_info_ || !metadata_snapshot->hasPartitionKey() || prewhere_info_->prewhere_actions.getNodes().empty())
        return false;

    if (!context_->getSettingsRef().optimize_prefilter_in_search)
        return false;

    const auto & partition_key = metadata_snapshot->getPartitionKey();
    const auto & prewhere_actions = prewhere_info_->prewhere_actions;

    /// Get column names required for partition key
    auto minmax_columns_names = storage_.getMinMaxColumnsNames(partition_key);

    /// Quick check if requried column names has any partition key column name
    /// Get column names in prewhere condition
    auto required_columns = prewhere_actions.getRequiredColumnsNames();
    bool exists = false;

    for (const auto & required_column : required_columns)
    {
        if (std::find(minmax_columns_names.begin(), minmax_columns_names.end(), required_column) != minmax_columns_names.end())
        {
            exists = true;
            break;
        }
    }

    /// Not found partition key column in prewhere, cannot skip.
    if (!exists)
        return false;


    /// prewhere actions has two outputs?
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    filter_nodes.push_back(&prewhere_actions.findInOutputs(prewhere_info_->prewhere_column_name));
    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes);

    if (!filter_actions_dag)
        return false;

    /// Reference PartitionPruner using KeyCondition, difference is that FUNCTION_UNKNOWN returns false.
    KeyCondition partition_pruner_condition(
        &*filter_actions_dag, context_, partition_key.column_names,
        partition_key.expression, true /* single_point */, true /* unknown_false */);

    const auto & partition_value = data_part_->partition.value;
    std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
    for (auto & field : index_value)
    {
        // NULL_LAST
        if (field.isNull())
            field = POSITIVE_INFINITY;
    }

    if (partition_pruner_condition.mayBeTrueInRange(
        partition_value.size(), index_value.data(), index_value.data(), partition_key.data_types))
        return true;

    /// Try minmax idx on columns required by partition key
    auto minmax_expression_actions = storage_.getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(context_));
    DataTypes minmax_columns_types = storage_.getMinMaxColumnsTypes(partition_key);

    KeyCondition minmax_idx_condition(
        &*filter_actions_dag, context_, minmax_columns_names,
        minmax_expression_actions, false /* single_point */, true /* known_false */);

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

    MergeTreeReaderPtr reader = data_part->getReader(
        cols,
        storage_snapshot,
        MarkRanges{MarkRange(0, data_part->getMarksCount())},
        /*virtual_fields=*/ {},
        nullptr,
        context->getMarkCache().get(),
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

    if (can_skip_perform_prefilter)
    {
        for (const auto & step : prewhere_actions.steps)
            all_prewhere_actions.steps.push_back(step);
    }

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

    const auto & sample_block = task->getMainRangeReader().getSampleBlock();
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
        LOG_DEBUG(log, "col_name in sample block {} in pos {}", col_name, ps);

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
    if (!can_skip_perform_prefilter && prewhere_info)
    {
        /// Add outputs of prewhere columns in result headers
        auto prewhere_output_columns  = prewhere_info->prewhere_actions.getNamesAndTypesList();
        for (const auto & output_column : prewhere_output_columns)
        {
            /// TODO: why input source column exists in prewhere output columns?
            if (!sample_block.has(output_column.name) && result_header.has(output_column.name))
            {
                LOG_DEBUG(log, "Add prewhere column, name with {}", output_column.name);
                ColumnWithTypeAndName prewhere_col;

                prewhere_col.type = output_column.type;
                prewhere_col.name = output_column.name;
                prewhere_col.column = output_column.type->createColumnConst(final_result_num_rows, 1);

                res_block.insert(std::move(prewhere_col));
            }
        }
    }

    /// ordered_columns: non-search functions, search functions cols
    /// Use the map orig_pos_in_sample_block to get column name and type from sample block
    for (size_t i = 0; i < ordered_columns.size(); ++i)
    {
        size_t pos_in_sample = orig_pos_in_sample_block[i];

        ColumnWithTypeAndName ctn;
        ctn.column = std::move(ordered_columns[i]);
        ctn.type = sample_block.getByPosition(pos_in_sample).type;
        ctn.name = sample_block.getByPosition(pos_in_sample).name;

        res_block.insert(std::move(ctn));
    }

    if (need_remove_part_offset)
    {
        res_block.erase("_part_offset");
    }

    MergeTreeReadTask::BlockAndProgress res = {
        .block= std::move(res_block),
        .row_count = final_result_num_rows,
        .num_read_rows = num_read_rows,
        .num_read_bytes = num_read_bytes };

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
    if (std::find(required_columns.begin(), required_columns.end(), "_part_offset") != required_columns.end())
        mutable_part_offset_col = ColumnUInt64::create();

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

    /// Sample block for tmp_result_columns
    Block sample_block;
    for (size_t i = 0; i < pk_col_size; ++i)
        sample_block.insert({ nullptr, primary_key.data_types[i], primary_key.column_names[i]});

    LOG_DEBUG(log, "Fetch from primary key cache size = {}", tmp_result_columns[0]->size());

    /// Get _part_offset if exists
    if (mutable_part_offset_col)
    {
        /// _part_offset column exists in original select columns
        if (!need_remove_part_offset)
        {
            tmp_result_columns.emplace_back(std::move(mutable_part_offset_col));
            part_offset = typeid_cast<const ColumnUInt64 *>(tmp_result_columns.back().get());

            sample_block.insert({ nullptr, std::make_shared<DataTypeUInt64>(), "_part_offset"});
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

        /// tmp_result_columns: pk columns + non const virtual columns(_part_offset) + distance columns
        for (const auto & distance_name : base_search_manager->getSearchFuncColumnNames())
        {
            auto & distance_col = result_header.getByName(distance_name);
            sample_block.insert({nullptr, distance_col.type, distance_col.name});
        }

        task->mark_ranges.clear();
        if (result_row_num > 0)
        {
            MergeTreeReadTask::BlockAndProgress res = {sample_block.cloneWithColumns(tmp_result_columns), result_row_num};
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

    /// Add _part_offset to requried_columns if part has lightweight delete
    if (data_part->hasLightweightDelete() || can_skip_perform_prefilter)
    {
        if (std::find(required_columns.begin(), required_columns.end(), "_part_offset") == required_columns.end())
        {
            required_columns.emplace_back("_part_offset");
            need_remove_part_offset = true;
        }
    }

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

MergeTreeReadTaskPtr MergeTreeSelectWithHybridSearchProcessor::createTask(MarkRanges ranges) const
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

MergeTreeReadTask::Extras MergeTreeSelectWithHybridSearchProcessor::getExtras() const
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

MergeTreeReadTaskInfoPtr MergeTreeSelectWithHybridSearchProcessor::initializeReadTaskInfo() const
{
    MergeTreeReadTaskInfo read_task_info;
    
    read_task_info.data_part = data_part;
    read_task_info.part_index_in_query = part_with_ranges.part_index_in_query;
    read_task_info.alter_conversions = part_with_ranges.alter_conversions;

    LoadedMergeTreeDataPartInfoForReader part_info(part_with_ranges.data_part, part_with_ranges.alter_conversions);

    PrewhereInfoPtr prewhere_info_task = can_skip_perform_prefilter ? prewhere_info : nullptr;

    read_task_info.task_columns = getReadTaskColumns(
        part_info,
        storage_snapshot,
        required_columns,
        prewhere_info_task,
        actions_settings,
        reader_settings,
        /*with_subcolumns=*/true);

    read_task_info.const_virtual_fields = shared_virtual_fields;
    read_task_info.const_virtual_fields.emplace("_part_index", read_task_info.part_index_in_query);

    return std::make_shared<MergeTreeReadTaskInfo>(std::move(read_task_info));
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

    can_skip_perform_prefilter = executeSearch(base_search_manager, storage, storage_snapshot, data_part, block_size_params,
                mark_ranges, prewhere_info_copy, reader_settings, context, max_streams_for_prewhere);
}

bool MergeTreeSelectWithHybridSearchProcessor::executeSearch(
    MergeTreeBaseSearchManagerPtr search_manager,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeData::DataPartPtr & data_part_,
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
    MarkRanges mark_ranges,
    const PrewhereInfoPtr & prewhere_info_copy,
    const MergeTreeReaderSettings & reader_settings_,
    ContextPtr context_,
    size_t max_streams)
{
    bool can_skip_perform_prefilter = canSkipPrewhereForPart(data_part_, prewhere_info_copy, storage_,
                                        storage_snapshot_->metadata, context_);

    if (can_skip_perform_prefilter)
        LOG_DEBUG(getLogger("executeSearch"), "Skip to call performPrefilter() for part {} due to a prewhere condition with partition key is true.", data_part_->name);

    /// perform vector scan
    if (!prewhere_info_copy || can_skip_perform_prefilter)
    {
        search_manager->executeSearchBeforeRead(data_part_);
    }
    else
    {
        /// try to process prewhere here, get part_offset columns
        /// 1 read, then get the filtered part_offsets
        /// 2 perform vector scan based on part_offsets
        auto filter = performPrefilter(mark_ranges, prewhere_info_copy, storage_, storage_snapshot_, data_part_,
                                block_size_params_, reader_settings_, context_, max_streams);

        ReadRanges read_ranges;
        ReadRange read_range{0, data_part_->rows_count, 0, data_part_->index_granularity.getMarksCount()};
        read_ranges.emplace_back(read_range);

        search_manager->executeSearchWithFilter(data_part_, read_ranges, filter);
    }

    return can_skip_perform_prefilter;
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
void getFilterFromPipeline(Pipe & pipe, VectorIndex::VIBitmapPtr & filter)
{
    QueryPipelineBuilder builder;
    builder.init(std::move(pipe));

    QueryPipeline filter_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    /// Use different pipeline executors
    PullingExecutor filter_executor(filter_pipeline);

    Block block;
    OpenTelemetry::SpanHolder span_pipe("performPrefilter()::getFilterFromPipeline()");
    while (filter_executor.pull(block))
    {
        if (block)
        {
            const PaddedPODArray<UInt64> & col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column).getData();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                filter->set(col_data[i]);
            }
        }
    }
}

/// In Async pulling pipeline executor cases (multiple threads), set the filter during read in parallel.
void parallelGetFilterFromPipeline(Pipe & pipe)
{
    QueryPipelineBuilder builder;
    builder.init(std::move(pipe));

    QueryPipeline filter_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingAsyncPipelineExecutor filter_executor(filter_pipeline);

    Block block;
    OpenTelemetry::SpanHolder span_pipe("performPrefilter()::parallelGetFilterFromPipeline()");
    while (filter_executor.pull(block))
    {
    }
}

}

VectorIndex::VIBitmapPtr MergeTreeSelectWithHybridSearchProcessor::performPrefilter(
    MarkRanges mark_ranges,
    const PrewhereInfoPtr & prewhere_info_copy,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeData::DataPartPtr & data_part_,
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
    const MergeTreeReaderSettings & reader_settings_,
    ContextPtr context_,
    size_t max_streams)
{
    OpenTelemetry::SpanHolder span("MergeTreeSelectWithHybridSearchProcessor::performPrefilter()");
    Names required_columns_prewhere;
    required_columns_prewhere.emplace_back("_part_offset");

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
    if (!prewhere_info_copy->prewhere_actions.getNodes().empty())
    {
        Names all_pre_column_names = prewhere_info_copy->prewhere_actions.getRequiredColumnsNames();

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

    PartRangesReadInfo info(data_part_, mark_ranges, settings, *data_settings);

    /// max streams for performing prewhere
    size_t num_streams = max_streams;

    LOG_DEBUG(getLogger("performPreFilter"), "max_streams = {}, original min_marks_for_concurrent_read = {}, sum_marks = {}, total_rows = {}, min_rows_for_concurrent_read = {}",
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
                if ((info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read > 1)
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
                else
                    num_streams = 1;
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

    LOG_DEBUG(getLogger("performPreFilter"), "num_streams = {}, min_marks_for_concurrent_read = {}", num_streams, info.min_marks_for_concurrent_read);

    size_t num_rows = data_part_->rows_count;
    VectorIndex::VIBitmapPtr filter = std::make_shared<VectorIndex::VIBitmap>(num_rows);

    /// Only one part
    RangesInDataParts parts_with_ranges;
    parts_with_ranges.emplace_back(data_part_, std::make_shared<AlterConversions>(), 0, mark_ranges);

    /// Read in multiple threads will use Async pulling executor
    if (enable_parallel_reading)
    {
        size_t max_block_size = block_size_params_.max_block_size_rows;

        /// ReadFromMergeTree::readFromPool()
        if (max_block_size && !info.is_adaptive)
        {
            size_t fixed_index_granularity = data_settings->index_granularity;
            info.min_marks_for_concurrent_read = (info.min_marks_for_concurrent_read * fixed_index_granularity + max_block_size - 1)
                / max_block_size * max_block_size / fixed_index_granularity;
        }

        auto pipe = createReadFromPoolWithFilterFromPartSource(
            parts_with_ranges,
            required_columns_prewhere,
            filter,
            storage_snapshot_,
            storage_.getLogName(),
            prewhere_info_copy,
            actions_settings,
            reader_settings_,
            block_size_params_,
            context_,
            num_streams,
            info.min_marks_for_concurrent_read);

        /// filter bitmap will be set during read data
        parallelGetFilterFromPipeline(pipe);
    }
    else
    {
        /// Read in a single thread
        auto source = createReadInOrderFromPartsSource(
            parts_with_ranges,
            required_columns_prewhere,
            storage_snapshot_,
            storage_.getLogName(),
            prewhere_info_copy,
            actions_settings,
            reader_settings_,
            block_size_params_,
            context_,
            /* max_streams= */ 1,
            info.min_marks_for_concurrent_read,
            settings.use_uncompressed_cache);

        Pipe pipe = Pipe(std::move(source));

        getFilterFromPipeline<PullingPipelineExecutor>(pipe, filter);
    }

    return filter;
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
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
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

    /// Execute vector scan and text search in this part.
    auto process_part = [&](size_t part_index)
    {
        auto & part_with_range = parts_with_ranges[part_index];
        auto & data_part_ = part_with_range.data_part;
        auto & mark_ranges = part_with_range.ranges;

        /// Save part_index in parts_with_ranges
        VectorAndTextResultInDataPart mix_results(part_index, data_part_);

        /// Handle three cases: vector scan, full-text seach and hybrid search
        if (query_info.hybrid_search_info)
        {
            auto hybrid_search_mgr = std::make_shared<MergeTreeHybridSearchManager>(metadata_snapshot, query_info.hybrid_search_info,
                                            context, vec_support_two_stage_searches[0]);
#if USE_TANTIVY_SEARCH
            hybrid_search_mgr->setBM25Stats(bm25_stats_in_table);
#endif
            /// Get vector scan and text search
            mix_results.can_skip_perform_prefilter = executeSearch(hybrid_search_mgr, data, storage_snapshot_, data_part_,
                        block_size_params_, mark_ranges, prewhere_info_copy, reader_settings_,
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
            mix_results.can_skip_perform_prefilter = executeSearch(vector_scan_mgr, data, storage_snapshot_, data_part_,
                        block_size_params_, mark_ranges, prewhere_info_copy, reader_settings_,
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
            mix_results.can_skip_perform_prefilter = executeSearch(text_search_mgr, data, storage_snapshot_, data_part_,
                        block_size_params_, mark_ranges, prewhere_info_copy, reader_settings_,
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
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
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
