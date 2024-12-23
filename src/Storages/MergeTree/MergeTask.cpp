#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/MergeTree/MergeTask.h>

#include <memory>
#include <fmt/format.h>

#include <Common/logger_useful.h>
#include <Common/ActionBlocker.h>
#include <Core/Settings.h>
#include <Common/ProfileEvents.h>
#include <Processors/Transforms/CheckSortedTransform.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeSource.h>

#include <DataTypes/ObjectUtils.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <IO/HashingReadBuffer.h>
#include <IO/WriteIntText.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Utils/VIUtils.h>
#include <VectorIndex/Common/SegmentsMgr.h>
#include <VectorIndex/Common/Segment.h>
#include <VectorIndex/Storages/MergeTreeWithVectorScanSource.h>
#include <Common/ActionBlocker.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event Merge;
    extern const Event MergedColumns;
    extern const Event GatheredColumns;
    extern const Event MergeTotalMilliseconds;
    extern const Event MergeExecuteMilliseconds;
    extern const Event MergeHorizontalStageExecuteMilliseconds;
    extern const Event MergeVerticalStageExecuteMilliseconds;
    extern const Event MergeProjectionStageExecuteMilliseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

static ColumnsStatistics getStatisticsForColumns(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot)
{
    ColumnsStatistics all_statistics;
    const auto & all_columns = metadata_snapshot->getColumns();

    for (const auto & column : columns_to_read)
    {
        const auto * desc = all_columns.tryGet(column.name);
        if (desc && !desc->statistics.empty())
        {
            auto statistics = MergeTreeStatisticsFactory::instance().get(desc->statistics);
            all_statistics.push_back(std::move(statistics));
        }
    }
    return all_statistics;
}

/// Manages the "rows_sources" temporary file that is used during vertical merge.
class RowsSourcesTemporaryFile : public ITemporaryFileLookup
{
public:
    /// A logical name of the temporary file under which it will be known to the plan steps that use it.
    static constexpr auto FILE_ID = "rows_sources";

    explicit RowsSourcesTemporaryFile(TemporaryDataOnDiskScopePtr temporary_data_on_disk_)
        : tmp_disk(std::make_unique<TemporaryDataOnDisk>(temporary_data_on_disk_))
        , uncompressed_write_buffer(tmp_disk->createRawStream())
        , tmp_file_name_on_disk(uncompressed_write_buffer->getFileName())
    {
    }

    WriteBuffer & getTemporaryFileForWriting(const String & name) override
    {
        if (name != FILE_ID)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

        if (write_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file was already requested for writing, there musto be only one writer");

        write_buffer = (std::make_unique<CompressedWriteBuffer>(*uncompressed_write_buffer));
        return *write_buffer;
    }

    std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & name) override
    {
        if (name != FILE_ID)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected temporary file name requested: {}", name);

        if (!finalized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file is not finalized yet");

        /// tmp_disk might not create real file if no data was written to it.
        if (final_size == 0)
            return std::make_unique<ReadBufferFromEmptyFile>();

        /// Reopen the file for each read so that multiple reads can be performed in parallel and there is no need to seek to the beginning.
        auto raw_file_read_buffer = std::make_unique<ReadBufferFromFile>(tmp_file_name_on_disk);
        return std::make_unique<CompressedReadBufferFromFile>(std::move(raw_file_read_buffer));
    }

    /// Returns written data size in bytes
    size_t finalizeWriting()
    {
        write_buffer->finalize();
        uncompressed_write_buffer->finalize();
        finalized = true;
        final_size = write_buffer->count();
        return final_size;
    }

private:
    std::unique_ptr<TemporaryDataOnDisk> tmp_disk;
    std::unique_ptr<WriteBufferFromFileBase> uncompressed_write_buffer;
    std::unique_ptr<WriteBuffer> write_buffer;
    const String tmp_file_name_on_disk;
    bool finalized = false;
    size_t final_size = 0;
};

static void addMissedColumnsToSerializationInfos(
    size_t num_rows_in_parts,
    const Names & part_columns,
    const ColumnsDescription & storage_columns,
    const SerializationInfo::Settings & info_settings,
    SerializationInfoByName & new_infos)
{
    NameSet part_columns_set(part_columns.begin(), part_columns.end());

    for (const auto & column : storage_columns)
    {
        if (part_columns_set.contains(column.name))
            continue;

        if (column.default_desc.kind != ColumnDefaultKind::Default)
            continue;

        if (column.default_desc.expression)
            continue;

        auto new_info = column.type->createSerializationInfo(info_settings);
        new_info->addDefaults(num_rows_in_parts);
        new_infos.emplace(column.name, std::move(new_info));
    }
}

/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
void MergeTask::ExecuteAndFinalizeHorizontalPart::extractMergingAndGatheringColumns() const
{
    const auto & sorting_key_expr = global_ctx->metadata_snapshot->getSortingKey().expression;
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();

    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());

    /// Force sign column for Collapsing mode
    if (ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        key_columns.emplace(ctx->merging_params.sign_column);

    /// Force version column for Replacing mode
    if (ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing)
    {
        key_columns.emplace(ctx->merging_params.is_deleted_column);
        key_columns.emplace(ctx->merging_params.version_column);
    }

    /// Force sign column for VersionedCollapsing mode. Version is already in primary key.
    if (ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        key_columns.emplace(ctx->merging_params.sign_column);

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(global_ctx->storage_columns.front().name);

    const auto & skip_indexes = global_ctx->metadata_snapshot->getSecondaryIndices();

    for (const auto & index : skip_indexes)
    {
        auto index_columns = index.expression->getRequiredColumns();

        /// Calculate indexes that depend only on one column on vertical
        /// stage and other indexes on horizonatal stage of merge.
        if (index_columns.size() == 1)
        {
            const auto & column_name = index_columns.front();
            global_ctx->skip_indexes_by_column[column_name].push_back(index);
        }
        else
        {
            std::ranges::copy(index_columns, std::inserter(key_columns, key_columns.end()));
            global_ctx->merging_skip_indexes.push_back(index);
        }
    }

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : global_ctx->storage_columns)
    {
        if (key_columns.contains(column.name))
        {
            global_ctx->merging_columns.emplace_back(column);

            /// If column is in horizontal stage we need to calculate its indexes on horizontal stage as well
            auto it = global_ctx->skip_indexes_by_column.find(column.name);
            if (it != global_ctx->skip_indexes_by_column.end())
            {
                for (auto & index : it->second)
                    global_ctx->merging_skip_indexes.push_back(std::move(index));

                global_ctx->skip_indexes_by_column.erase(it);
            }
        }
        else
        {
            global_ctx->gathering_columns.emplace_back(column);
        }
    }
}

bool MergeTask::ExecuteAndFinalizeHorizontalPart::prepare()
{
    ProfileEvents::increment(ProfileEvents::Merge);

    String local_tmp_prefix;
    if (global_ctx->need_prefix)
    {
        // projection parts have different prefix and suffix compared to normal parts.
        // E.g. `proj_a.proj` for a normal projection merge and `proj_a.tmp_proj` for a projection materialization merge.
        local_tmp_prefix = global_ctx->parent_part ? "" : "tmp_merge_";
    }
    const String local_tmp_suffix = global_ctx->parent_part ? ctx->suffix : "";

    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(global_ctx->future_part->merge_type) && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts with TTL");

    LOG_DEBUG(ctx->log, "Merging {} parts: from {} to {} into {} with storage {}",
        global_ctx->future_part->parts.size(),
        global_ctx->future_part->parts.front()->name,
        global_ctx->future_part->parts.back()->name,
        global_ctx->future_part->part_format.part_type.toString(),
        global_ctx->future_part->part_format.storage_type.toString());

    if (global_ctx->deduplicate)
    {
        if (global_ctx->deduplicate_by_columns.empty())
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY ('{}')", fmt::join(global_ctx->deduplicate_by_columns, "', '"));
    }

    ctx->disk = global_ctx->space_reservation->getDisk();
    auto local_tmp_part_basename = local_tmp_prefix + global_ctx->future_part->name + local_tmp_suffix;

    std::optional<MergeTreeDataPartBuilder> builder;
    if (global_ctx->parent_part)
    {
        auto data_part_storage = global_ctx->parent_part->getDataPartStorage().getProjection(local_tmp_part_basename,  /* use parent transaction */ false);
        builder.emplace(*global_ctx->data, global_ctx->future_part->name, data_part_storage);
        builder->withParentPart(global_ctx->parent_part);
    }
    else
    {
        auto local_single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + global_ctx->future_part->name, ctx->disk, 0);
        builder.emplace(global_ctx->data->getDataPartBuilder(global_ctx->future_part->name, local_single_disk_volume, local_tmp_part_basename));
        builder->withPartStorageType(global_ctx->future_part->part_format.storage_type);
    }

    builder->withPartInfo(global_ctx->future_part->part_info);
    builder->withPartType(global_ctx->future_part->part_format.part_type);

    global_ctx->new_data_part = std::move(*builder).build();
    auto data_part_storage = global_ctx->new_data_part->getDataPartStoragePtr();

    if (data_part_storage->exists())
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory {} already exists", data_part_storage->getFullPath());

    data_part_storage->beginTransaction();
    /// Background temp dirs cleaner will not touch tmp projection directory because
    /// it's located inside part's directory
    if (!global_ctx->parent_part)
        global_ctx->temporary_directory_lock = global_ctx->data->getTemporaryPartDirectoryHolder(local_tmp_part_basename);

    global_ctx->storage_columns = global_ctx->metadata_snapshot->getColumns().getAllPhysical();

    auto object_columns = MergeTreeData::getConcreteObjectColumns(global_ctx->future_part->parts, global_ctx->metadata_snapshot->getColumns());
    extendObjectColumns(global_ctx->storage_columns, object_columns, false);
    global_ctx->storage_snapshot = std::make_shared<StorageSnapshot>(*global_ctx->data, global_ctx->metadata_snapshot, std::move(object_columns));

    extractMergingAndGatheringColumns();

    global_ctx->new_data_part->uuid = global_ctx->future_part->uuid;
    global_ctx->new_data_part->partition.assign(global_ctx->future_part->getPartition());
    global_ctx->new_data_part->is_temp = global_ctx->parent_part == nullptr;

    /// In case of replicated merge tree with zero copy replication
    /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
    /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
    global_ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

    ctx->need_remove_expired_values = false;
    ctx->force_ttl = false;

    if (enabledBlockNumberColumn(global_ctx))
        addGatheringColumn(global_ctx, BlockNumberColumn::name, BlockNumberColumn::type);

    if (enabledBlockOffsetColumn(global_ctx))
        addGatheringColumn(global_ctx, BlockOffsetColumn::name, BlockOffsetColumn::type);

    SerializationInfo::Settings info_settings =
    {
        .ratio_of_defaults_for_sparse = global_ctx->data->getSettings()->ratio_of_defaults_for_sparse_serialization,
        .choose_kind = true,
    };

    SerializationInfoByName infos(global_ctx->storage_columns, info_settings);

    for (const auto & part : global_ctx->future_part->parts)
    {
        global_ctx->new_data_part->ttl_infos.update(part->ttl_infos);
        if (global_ctx->metadata_snapshot->hasAnyTTL() && !part->checkAllTTLCalculated(global_ctx->metadata_snapshot))
        {
            LOG_INFO(ctx->log, "Some TTL values were not calculated for part {}. Will calculate them forcefully during merge.", part->name);
            ctx->need_remove_expired_values = true;
            ctx->force_ttl = true;
        }

        if (!info_settings.isAlwaysDefault())
        {
            auto part_infos = part->getSerializationInfos();

            addMissedColumnsToSerializationInfos(
                part->rows_count,
                part->getColumns().getNames(),
                global_ctx->metadata_snapshot->getColumns(),
                info_settings,
                part_infos);

            infos.add(part_infos);
        }
    }

    const auto & local_part_min_ttl = global_ctx->new_data_part->ttl_infos.part_min_ttl;
    if (local_part_min_ttl && local_part_min_ttl <= global_ctx->time_of_merge)
        ctx->need_remove_expired_values = true;

    global_ctx->new_data_part->setColumns(global_ctx->storage_columns, infos, global_ctx->metadata_snapshot->getMetadataVersion());

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
    {
        LOG_INFO(ctx->log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", global_ctx->new_data_part->name);
        ctx->need_remove_expired_values = false;
    }

    ctx->sum_input_rows_upper_bound = global_ctx->merge_list_element_ptr->total_rows_count;
    ctx->sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;

    global_ctx->chosen_merge_algorithm = chooseMergeAlgorithm();
    global_ctx->merge_list_element_ptr->merge_algorithm.store(global_ctx->chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(ctx->log, "Selected MergeAlgorithm: {}", toString(global_ctx->chosen_merge_algorithm));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    ctx->compression_codec = global_ctx->data->getCompressionCodecForPart(
        global_ctx->merge_list_element_ptr->total_size_bytes_compressed, global_ctx->new_data_part->ttl_infos, global_ctx->time_of_merge);

    switch (global_ctx->chosen_merge_algorithm)
    {
        case MergeAlgorithm::Horizontal:
        {
            global_ctx->merging_columns = global_ctx->storage_columns;
            global_ctx->merging_skip_indexes = global_ctx->metadata_snapshot->getSecondaryIndices();
            global_ctx->gathering_columns.clear();
            global_ctx->skip_indexes_by_column.clear();
            break;
        }
        case MergeAlgorithm::Vertical:
        {
            ctx->rows_sources_temporary_file = std::make_shared<RowsSourcesTemporaryFile>(global_ctx->context->getTempDataOnDisk());
            std::map<String, UInt64> local_merged_column_to_size;
            for (const auto & part : global_ctx->future_part->parts)
                part->accumulateColumnSizes(local_merged_column_to_size);

            ctx->column_sizes = ColumnSizeEstimator(
                std::move(local_merged_column_to_size),
                global_ctx->merging_columns,
                global_ctx->gathering_columns);

            break;
        }
        default :
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge algorithm must be chosen");
    }

    assert(global_ctx->gathering_columns.size() == global_ctx->gathering_column_names.size());
    assert(global_ctx->merging_columns.size() == global_ctx->merging_column_names.size());

    /// Check if decoupled data part is enabled. If true, we can use old vector indices before new index is built.
    if (global_ctx->data->getSettings()->enable_decouple_vector_index)
    {
        size_t num_parts = global_ctx->future_part->parts.size();
        Int64 first_part_with_data = -1;
        size_t max_part_with_index = 0;

        /// Support multiple vector indices. Check if merged part can be decouple for each vector index.
        for (const auto & vec_index : global_ctx->metadata_snapshot->getVectorIndices())
        {
            size_t num_parts_with_vector_index = 0;
            size_t empty_parts_count = 0;
            size_t not_empty_part_size = 0;

            /// We use old vector indices only when all the merged source parts have index.
            for (size_t i = 0; i <  global_ctx->future_part->parts.size(); ++i)
            {
                auto & part = global_ctx->future_part->parts[i];
                auto vi_status = part->segments_mgr->getSegmentStatus(vec_index.name);
                if (vi_status == VectorIndex::SegmentStatus::BUILT)
                    num_parts_with_vector_index++;
                
                if (part->rows_count == 0)
                    empty_parts_count++;

                if (first_part_with_data == -1 && part->rows_count != 0)
                {
                    first_part_with_data = i;
                    global_ctx->first_part_with_data = i;
                }
            }
            max_part_with_index = max_part_with_index < num_parts_with_vector_index ? num_parts_with_vector_index : max_part_with_index;

            if (not_empty_part_size == 0)
                not_empty_part_size = num_parts - empty_parts_count;

            if (num_parts > 0 && ((num_parts_with_vector_index + empty_parts_count) == num_parts))
            {
                global_ctx->all_parts_have_vector_index.insert_or_assign(vec_index.name, true);
                global_ctx->can_be_decouple = true;
            }
        }

        /// When only one part is merged, the merged part can be decouple only when LWD exists.
        /// If no LWD, still a VPart after merge.
        if (global_ctx->can_be_decouple && max_part_with_index == 1 && !global_ctx->future_part->parts[first_part_with_data]->hasLightweightDelete())
        {
            LOG_DEBUG(ctx->log, "Merge single VPart without LWD to VPart. With vector index in part_id {}", global_ctx->first_part_with_data);
            global_ctx->only_one_vpart_merged = true;
            global_ctx->can_be_decouple = false;  /// No need to create row ids map
        }
    }

    if (global_ctx->can_be_decouple)
    {
        /// we need rows_sources info in horizontal merge for vector index case
        if (!ctx->rows_sources_temporary_file)
            ctx->rows_sources_temporary_file = std::make_shared<RowsSourcesTemporaryFile>(global_ctx->context->getTempDataOnDisk());

        /// create inverted row ids map
        global_ctx->inverted_row_ids_map_file_path
            = global_ctx->new_data_part->getDataPartStorage().getFullPath() + "merged-inverted_row_ids_map" + VECTOR_INDEX_FILE_SUFFIX;

        /// create row ids map for each old part
        for (size_t i = 0; i < global_ctx->future_part->parts.size(); ++i)
        {
            String row_ids_map_file = global_ctx->new_data_part->getDataPartStorage().getFullPath() + "merged-" + toString(i) + "-"
                + global_ctx->future_part->parts[i]->name + "-row_ids_map" + VECTOR_INDEX_FILE_SUFFIX;
            global_ctx->row_ids_map_files.emplace_back(row_ids_map_file);
        }

        /// Save RowsSourcesTemporaryFile for inverted_rows_sources_map_file
        global_ctx->inverted_rows_sources_map_file = ctx->rows_sources_temporary_file;
    }

    /// If merge is vertical we cannot calculate it
    ctx->blocks_are_granules_size = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);

    /// Merged stream will be created and available as merged_stream variable
    createMergedStream();

    /// Skip fully expired columns manually, since in case of
    /// need_remove_expired_values is not set, TTLTransform will not be used,
    /// and columns that had been removed by TTL (via TTLColumnAlgorithm) will
    /// be added again with default values.
    ///
    /// Also note, that it is better to do this here, since in other places it
    /// will be too late (i.e. they will be written, and we will burn CPU/disk
    /// resources for this).
    if (!ctx->need_remove_expired_values)
    {
        auto part_serialization_infos = global_ctx->new_data_part->getSerializationInfos();

        NameSet columns_to_remove;
        for (auto & [column_name, ttl] : global_ctx->new_data_part->ttl_infos.columns_ttl)
        {
            if (ttl.finished())
            {
                global_ctx->new_data_part->expired_columns.insert(column_name);
                LOG_TRACE(ctx->log, "Adding expired column {} for part {}", column_name, global_ctx->new_data_part->name);
                columns_to_remove.insert(column_name);
                part_serialization_infos.erase(column_name);
            }
        }

        if (!columns_to_remove.empty())
        {
            global_ctx->gathering_columns = global_ctx->gathering_columns.eraseNames(columns_to_remove);
            global_ctx->merging_columns = global_ctx->merging_columns.eraseNames(columns_to_remove);
            global_ctx->storage_columns = global_ctx->storage_columns.eraseNames(columns_to_remove);

            global_ctx->new_data_part->setColumns(
                global_ctx->storage_columns,
                part_serialization_infos,
                global_ctx->metadata_snapshot->getMetadataVersion());
        }
    }

    global_ctx->to = std::make_shared<MergedBlockOutputStream>(
        global_ctx->new_data_part,
        global_ctx->metadata_snapshot,
        global_ctx->merging_columns,
        MergeTreeIndexFactory::instance().getMany(global_ctx->merging_skip_indexes),
        getStatisticsForColumns(global_ctx->merging_columns, global_ctx->metadata_snapshot),
        ctx->compression_codec,
        global_ctx->txn ? global_ctx->txn->tid : Tx::PrehistoricTID,
        /*reset_columns=*/ true,
        ctx->blocks_are_granules_size,
        global_ctx->context->getWriteSettings());

    global_ctx->rows_written = 0;
    ctx->initial_reservation = global_ctx->space_reservation ? global_ctx->space_reservation->getSize() : 0;

    ctx->is_cancelled = [merges_blocker = global_ctx->merges_blocker,
        ttl_merges_blocker = global_ctx->ttl_merges_blocker,
        need_remove = ctx->need_remove_expired_values,
        merge_list_element = global_ctx->merge_list_element_ptr]() -> bool
    {
        return merges_blocker->isCancelled()
            || (need_remove && ttl_merges_blocker->isCancelled())
            || merge_list_element->is_cancelled.load(std::memory_order_relaxed);
    };

    /// This is the end of preparation. Execution will be per block.
    return false;
}

bool MergeTask::enabledBlockNumberColumn(GlobalRuntimeContextPtr global_ctx)
{
    return global_ctx->data->getSettings()->enable_block_number_column && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
}

bool MergeTask::enabledBlockOffsetColumn(GlobalRuntimeContextPtr global_ctx)
{
    return global_ctx->data->getSettings()->enable_block_offset_column && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
}

void MergeTask::addGatheringColumn(GlobalRuntimeContextPtr global_ctx, const String & name, const DataTypePtr & type)
{
    if (global_ctx->storage_columns.contains(name))
        return;

    global_ctx->storage_columns.emplace_back(name, type);
    global_ctx->gathering_columns.emplace_back(name, type);
}


MergeTask::StageRuntimeContextPtr MergeTask::ExecuteAndFinalizeHorizontalPart::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeHorizontalStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<VerticalMergeRuntimeContext>();

    new_ctx->rows_sources_temporary_file = std::move(ctx->rows_sources_temporary_file);
    new_ctx->column_sizes = std::move(ctx->column_sizes);
    new_ctx->compression_codec = std::move(ctx->compression_codec);
    new_ctx->it_name_and_type = std::move(ctx->it_name_and_type);
    new_ctx->read_with_direct_io = std::move(ctx->read_with_direct_io);
    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}

MergeTask::StageRuntimeContextPtr MergeTask::VerticalMergeStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeVerticalStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<MergeProjectionsRuntimeContext>();
    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::executeImpl()
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms = global_ctx->data->getSettings()->background_task_preferred_step_execution_time_ms.totalMilliseconds();

    do
    {
        Block block;

        if (ctx->is_cancelled() || !global_ctx->merging_executor->pull(block))
        {
            finalize();
            return false;
        }

        global_ctx->rows_written += block.rows();
        const_cast<MergedBlockOutputStream &>(*global_ctx->to).write(block);

        UInt64 result_rows = 0;
        UInt64 result_bytes = 0;
        global_ctx->merged_pipeline.tryGetResultRowsAndBytes(result_rows, result_bytes);
        global_ctx->merge_list_element_ptr->rows_written = result_rows;
        global_ctx->merge_list_element_ptr->bytes_written_uncompressed = result_bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (global_ctx->space_reservation && ctx->sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * global_ctx->rows_written / ctx->sum_input_rows_upper_bound)
                : std::min(1., global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed));

            global_ctx->space_reservation->update(static_cast<size_t>((1. - progress) * ctx->initial_reservation));
        }
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}

void MergeTask::ExecuteAndFinalizeHorizontalPart::finalize() const
{
    global_ctx->merging_executor.reset();
    global_ctx->merged_pipeline.reset();

    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts with expired TTL");

    const size_t sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;
    ctx->need_sync = needSyncPart(ctx->sum_input_rows_upper_bound, sum_compressed_bytes_upper_bound, *global_ctx->data->getSettings());
}

bool MergeTask::ExecuteAndFinalizeHorizontalPart::generateRowIdsMap()
{
    if (global_ctx->inverted_row_ids_map_file_path.empty())
        return false;

    const auto & primary_key = global_ctx->metadata_snapshot->getPrimaryKey();
    Names columns_to_read = primary_key.column_names;
    columns_to_read.emplace_back("_part_offset");

    size_t old_parts_num = global_ctx->future_part->parts.size();
    std::vector<std::vector<UInt64>> part_offsets(old_parts_num);

    auto pipeline_settings = BuildQueryPipelineSettings::fromContext(global_ctx->context);
    auto optimization_settings = QueryPlanOptimizationSettings::fromContext(global_ctx->context);

    for (size_t part_num = 0; part_num < old_parts_num; ++part_num)
    {
        auto part = global_ctx->future_part->parts[part_num];

        auto part_marks = part->index_granularity.getMarksCount();
        if (part_marks == 0)
            continue;

        MarkRanges ranges;
        ranges.emplace_back(0, part_marks);

        auto alter_conversions = part->storage.getAlterConversionsForPart(part);

        const auto & settings = global_ctx->context->getSettingsRef();

        ExpressionActionsSettings actions_settings;
        MergeTreeReaderSettings reader_settings;

        MergeTreeReadTask::BlockSizeParams block_size{
            .max_block_size_rows = settings.max_block_size,
            .preferred_block_size_bytes = settings.preferred_block_size_bytes,
            .preferred_max_column_in_block_size_bytes = settings.preferred_max_column_in_block_size_bytes};

        RangesInDataParts parts_with_ranges;
        parts_with_ranges.emplace_back(part, alter_conversions, 0, ranges);

        auto source = createReadInOrderFromPartsSource(
            parts_with_ranges,
            columns_to_read,
            global_ctx->storage_snapshot,
            global_ctx->data->getLogName(),
            /*prewhere_info*/ nullptr,
            actions_settings,
            reader_settings,
            block_size,
            global_ctx->context,
            /*max_streams*/ 1,
            /*min_marks_for_concurrent_read*/ 0,
            settings.use_uncompressed_cache);

        Pipe pipe = Pipe(std::move(source));

        QueryPipeline filter_pipeline(std::move(pipe));
        PullingPipelineExecutor filter_executor(filter_pipeline);

        Block block;
        while (filter_executor.pull(block))
        {
            const PaddedPODArray<UInt64>& col_data = checkAndGetColumn<ColumnUInt64>(*block.getByName("_part_offset").column).getData();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                part_offsets[part_num].emplace_back(col_data[i]);
            }
        }
    }

    try
    {
        /// ctx->rows_sources_file is removed by PR #57275, cherry-pick PR #69383
        if (!ctx->rows_sources_temporary_file)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read rows sources temporary file");

        /// Ensure data has written to disk.
        size_t rows_sources_count = ctx->rows_sources_temporary_file->finalizeWriting();

        auto rows_sources_read_buf = ctx->rows_sources_temporary_file->getTemporaryFileForReading(RowsSourcesTemporaryFile::FILE_ID);
        LOG_DEBUG(ctx->log, "rows_sources_count: {}", rows_sources_count);

        /// inverted_row_ids_map file write buffer
        global_ctx->inverted_row_ids_map_uncompressed_buf = global_ctx->new_data_part->getDataPartStorage().writeFile(
            global_ctx->inverted_row_ids_map_file_path, 4096, global_ctx->context->getWriteSettings());
        global_ctx->inverted_row_ids_map_buf = std::make_unique<CompressedWriteBuffer>(*global_ctx->inverted_row_ids_map_uncompressed_buf);

        /// row_ids_map file write buffers
        global_ctx->row_ids_map_bufs.clear();
        global_ctx->row_ids_map_uncompressed_bufs.clear();
        for (const auto & row_ids_map_file : global_ctx->row_ids_map_files)
        {
            auto row_ids_map_uncompressed_buf
                = global_ctx->new_data_part->getDataPartStorage().writeFile(row_ids_map_file, 4096, global_ctx->context->getWriteSettings());
            global_ctx->row_ids_map_bufs.emplace_back(std::make_unique<CompressedWriteBuffer>(*row_ids_map_uncompressed_buf));
            global_ctx->row_ids_map_uncompressed_bufs.emplace_back(std::move(row_ids_map_uncompressed_buf));
        }

        /// read data into buffer
        uint64_t new_part_row_id = 0;
        std::vector<uint64_t> source_row_ids(global_ctx->future_part->parts.size(), 0);
        /// used to store new row ids for each old part
        std::vector<std::unordered_map<UInt64, UInt64>> parts_new_row_ids(global_ctx->future_part->parts.size());
        /// TODO: confirm read all in one round?

        /// Replacing Merge Tree
        if (ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing
            || ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing
            || ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        {
            /// write one file(inverted row ids map), new part -> pos in old part, if not in, skip writing
            while (!rows_sources_read_buf->eof())
            {
                RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(rows_sources_read_buf->position());
                RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(rows_sources_read_buf->buffer().end());
                while (row_source_pos < row_sources_end)
                {
                    /// row_source is the part from which row comes
                    RowSourcePart row_source = *row_source_pos;
                    /// part pos number in part_offsets
                    size_t source_num = row_source.getSourceNum() ;

                    if (!row_source_pos->getSkipFlag())
                    {
                        /// source_row_ids stores the row offset of the corresponding part
                        auto old_part_offset = part_offsets[source_num][source_row_ids[source_num]];

                        /// parts_new_row_ids stores mapping from a formal row in old part to its current pos in new merged part
                        parts_new_row_ids[source_num][old_part_offset] = new_part_row_id;
                        writeIntText(old_part_offset, *global_ctx->inverted_row_ids_map_buf);
                        /// need to add this, or we cannot correctly read uint64 value
                        writeChar('\t', *global_ctx->inverted_row_ids_map_buf);
                        ++new_part_row_id;
                    }
                    ++source_row_ids[source_num];

                    ++row_source_pos;
                }
                rows_sources_read_buf->position() = reinterpret_cast<char *>(row_source_pos);
            }

            /// write row_ids_map_bufs,
            for (size_t source_num = 0; source_num < old_parts_num; source_num++)
            /// write multiple files(row id map buf), old part -> pos in new part,if not in skip writing
            {
                auto metadata_snapshot = global_ctx->data->getInMemoryMetadataPtr();
                UInt64 old_row_id = 0;
                auto partRowNum = global_ctx->future_part->parts[source_num]->rows_count;
                std::vector<uint64_t> deleteRowIds(partRowNum, 0);
                int i = 0;
                while (old_row_id < partRowNum)
                {
                    UInt64 new_row_id = -1;
                    if (parts_new_row_ids[source_num].count(old_row_id) > 0)
                    {
                        new_row_id = parts_new_row_ids[source_num][old_row_id];
                    }
                    else
                    {
                        //generate delete row id for using in vector index
                        deleteRowIds[i] = static_cast<UInt64>(old_row_id);
                        i++;
                    }
                    writeIntText(new_row_id, *global_ctx->row_ids_map_bufs[source_num]);
                    writeChar('\t', *global_ctx->row_ids_map_bufs[source_num]);
                    ++old_row_id;
                }

                if (i > 0)
                {
                    /// Support multiple vector indices
                    VectorIndex::VIBitmapPtr delete_bit_map = std::make_shared<VectorIndex::VIBitmap>(partRowNum, true);
                    for (size_t row_id : deleteRowIds)
                    {
                        if (row_id)
                            delete_bit_map->unset(row_id);
                    }
                    for (const auto & vec_index_desc : metadata_snapshot->getVectorIndices())
                    {
                        auto vi_segment = global_ctx->future_part->parts[source_num]->segments_mgr->getSegment(vec_index_desc.name);
                        if (vi_segment)
                            vi_segment->updateCachedBitMap(delete_bit_map);
                    }
                }
            }
        }
        else
        {
            while (!rows_sources_read_buf->eof())
            {
                RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(rows_sources_read_buf->position());
                RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(rows_sources_read_buf->buffer().end());
                while (row_source_pos < row_sources_end)
                {
                    /// row_source is the part from which row comes
                    RowSourcePart row_source = *row_source_pos;
                    /// part pos number in part_offsets
                    size_t source_num = row_source.getSourceNum();
                    /// source_row_ids stores the row offset of the corresponding part
                    auto old_part_offset = part_offsets[source_num][source_row_ids[source_num]];
                    /// stores mapping from a formal row in old part to its current pos in new merged part
                    parts_new_row_ids[source_num][old_part_offset] = new_part_row_id;

                    /// writeIntText(new_part_row_id, *global_ctx->row_ids_map_bufs[source_num]);
                    writeIntText(old_part_offset, *global_ctx->inverted_row_ids_map_buf);
                    /// need to add this, or we cannot correctly read uint64 value
                    /// writeChar('\t', *global_ctx->row_ids_map_bufs[source_num]);
                    writeChar('\t', *global_ctx->inverted_row_ids_map_buf);

                    ++new_part_row_id;
                    ++source_row_ids[source_num];

                    ++row_source_pos;
                }

                rows_sources_read_buf->position() = reinterpret_cast<char *>(row_source_pos);
            }

            /// write row_ids_map_bufs
            for (size_t source_num = 0; source_num < old_parts_num; source_num++)
            {
                UInt64 old_row_id = 0;
                while (old_row_id < global_ctx->future_part->parts[source_num]->rows_count)
                {
                    UInt64 new_row_id = -1;
                    if (parts_new_row_ids[source_num].count(old_row_id) > 0)
                    {
                        new_row_id = parts_new_row_ids[source_num][old_row_id];
                    }
                    writeIntText(new_row_id, *global_ctx->row_ids_map_bufs[source_num]);
                    writeChar('\t', *global_ctx->row_ids_map_bufs[source_num]);
                    ++old_row_id;
                }
            }
        }

        LOG_DEBUG(ctx->log, "After write row_source_pos: inverted_row_ids_map_buf size: {}", global_ctx->inverted_row_ids_map_buf->count());

        for (size_t i = 0; i < global_ctx->future_part->parts.size(); ++i)
        {
            global_ctx->row_ids_map_bufs[i]->next();
            global_ctx->row_ids_map_uncompressed_bufs[i]->next();
            global_ctx->row_ids_map_uncompressed_bufs[i]->finalize();
        }
        global_ctx->inverted_row_ids_map_buf->next();
        global_ctx->inverted_row_ids_map_uncompressed_buf->next();
        global_ctx->inverted_row_ids_map_uncompressed_buf->finalize();

        return false;
    }
    catch (...)
    {
        /// Release the buffer in advance to prevent fatal occurrences during subsequent buffer destruction.
        for (size_t i = 0; i < global_ctx->row_ids_map_bufs.size(); ++i)
            global_ctx->row_ids_map_bufs[i].reset();

        for (size_t i = 0; i < global_ctx->row_ids_map_uncompressed_bufs.size(); ++i)
            global_ctx->row_ids_map_uncompressed_bufs[i].reset();

        global_ctx->inverted_row_ids_map_buf.reset();
        global_ctx->inverted_row_ids_map_uncompressed_buf.reset();

        throw;
    }

}

bool MergeTask::VerticalMergeStage::prepareVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    size_t sum_input_rows_exact = global_ctx->merge_list_element_ptr->rows_read;
    size_t input_rows_filtered = *global_ctx->input_rows_filtered;
    global_ctx->merge_list_element_ptr->columns_written = global_ctx->merging_columns.size();
    global_ctx->merge_list_element_ptr->progress.store(ctx->column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

    /// Ensure data has written to disk.
    size_t rows_sources_count = ctx->rows_sources_temporary_file->finalizeWriting();
    /// In special case, when there is only one source part, and no rows were skipped, we may have
    /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
    /// number of input rows.
    if ((rows_sources_count > 0 || global_ctx->future_part->parts.size() > 1) && sum_input_rows_exact != rows_sources_count + input_rows_filtered)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of rows in source parts ({}) excluding filtered rows ({}) differs from number of bytes written to rows_sources file ({}). It is a bug.",
            sum_input_rows_exact, input_rows_filtered, rows_sources_count);

    ctx->it_name_and_type = global_ctx->gathering_columns.cbegin();

    const auto & settings = global_ctx->context->getSettingsRef();

    size_t max_delayed_streams = 0;
    if (global_ctx->new_data_part->getDataPartStorage().supportParallelWrite())
    {
        if (settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_delayed_streams = settings.max_insert_delayed_streams_for_parallel_write;
        else
            max_delayed_streams = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
    }

    ctx->max_delayed_streams = max_delayed_streams;

    bool all_parts_on_remote_disks = std::ranges::all_of(global_ctx->future_part->parts, [](const auto & part) { return part->isStoredOnRemoteDisk(); });
    ctx->use_prefetch = all_parts_on_remote_disks && global_ctx->data->getSettings()->vertical_merge_remote_filesystem_prefetch;

    if (ctx->use_prefetch && ctx->it_name_and_type != global_ctx->gathering_columns.end())
        ctx->prepared_pipe = createPipeForReadingOneColumn(ctx->it_name_and_type->name);

    return false;
}

Pipe MergeTask::VerticalMergeStage::createPipeForReadingOneColumn(const String & column_name) const
{
    Pipes pipes;
    for (size_t part_num = 0; part_num < global_ctx->future_part->parts.size(); ++part_num)
    {
        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            *global_ctx->data,
            global_ctx->storage_snapshot,
            global_ctx->future_part->parts[part_num],
            Names{column_name},
            /*mark_ranges=*/ {},
            global_ctx->input_rows_filtered,
            /*apply_deleted_mask=*/ true,
            ctx->read_with_direct_io,
            ctx->use_prefetch);

        pipes.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(pipes));
}

void MergeTask::VerticalMergeStage::prepareVerticalMergeForOneColumn() const
{
    const auto & column_name = ctx->it_name_and_type->name;

    ctx->progress_before = global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed);
    global_ctx->column_progress = std::make_unique<MergeStageProgress>(ctx->progress_before, ctx->column_sizes->columnWeight(column_name));

    Pipe pipe;
    if (ctx->prepared_pipe)
    {
        pipe = std::move(*ctx->prepared_pipe);

        auto next_column_it = std::next(ctx->it_name_and_type);
        if (next_column_it != global_ctx->gathering_columns.end())
            ctx->prepared_pipe = createPipeForReadingOneColumn(next_column_it->name);
    }
    else
    {
        pipe = createPipeForReadingOneColumn(column_name);
    }

    if (!ctx->rows_sources_temporary_file)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read rows sources temporary file");

    auto rows_sources_read_buf = ctx->rows_sources_temporary_file->getTemporaryFileForReading(RowsSourcesTemporaryFile::FILE_ID);
    bool is_result_sparse = global_ctx->new_data_part->getSerialization(column_name)->getKind() == ISerialization::Kind::SPARSE;

    const auto data_settings = global_ctx->data->getSettings();
    auto transform = std::make_unique<ColumnGathererTransform>(
        pipe.getHeader(),
        pipe.numOutputPorts(),
        std::move(rows_sources_read_buf),
        data_settings->merge_max_block_size,
        data_settings->merge_max_block_size_bytes,
        is_result_sparse);

    pipe.addTransform(std::move(transform));

    MergeTreeIndices indexes_to_recalc;
    auto indexes_it = global_ctx->skip_indexes_by_column.find(column_name);

    if (indexes_it != global_ctx->skip_indexes_by_column.end())
    {
        indexes_to_recalc = MergeTreeIndexFactory::instance().getMany(indexes_it->second);

        pipe.addTransform(std::make_shared<ExpressionTransform>(
            pipe.getHeader(),
            indexes_it->second.getSingleExpressionForIndices(global_ctx->metadata_snapshot->getColumns(),
            global_ctx->data->getContext())));

        pipe.addTransform(std::make_shared<MaterializingTransform>(pipe.getHeader()));
    }

    ctx->column_parts_pipeline = QueryPipeline(std::move(pipe));

    /// Dereference unique_ptr
    ctx->column_parts_pipeline.setProgressCallback(MergeProgressCallback(
        global_ctx->merge_list_element_ptr,
        global_ctx->watch_prev_elapsed,
        *global_ctx->column_progress));

    /// Is calculated inside MergeProgressCallback.
    ctx->column_parts_pipeline.disableProfileEventUpdate();
    ctx->executor = std::make_unique<PullingPipelineExecutor>(ctx->column_parts_pipeline);
    NamesAndTypesList columns_list = {*ctx->it_name_and_type};

    ctx->column_to = std::make_unique<MergedColumnOnlyOutputStream>(
        global_ctx->new_data_part,
        global_ctx->metadata_snapshot,
        columns_list,
        ctx->compression_codec,
        indexes_to_recalc,
        getStatisticsForColumns(columns_list, global_ctx->metadata_snapshot),
        &global_ctx->written_offset_columns,
        global_ctx->to->getIndexGranularity());

    ctx->column_elems_written = 0;
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForOneColumn() const
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms = global_ctx->data->getSettings()->background_task_preferred_step_execution_time_ms.totalMilliseconds();

    do
    {
        Block block;

        if (global_ctx->merges_blocker->isCancelled()
            || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed)
            || !ctx->executor->pull(block))
            return false;

        ctx->column_elems_written += block.rows();
        ctx->column_to->write(block);
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}


void MergeTask::VerticalMergeStage::finalizeVerticalMergeForOneColumn() const
{
    const String & column_name = ctx->it_name_and_type->name;
    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    ctx->executor.reset();
    auto changed_checksums = ctx->column_to->fillChecksums(global_ctx->new_data_part, global_ctx->checksums_gathered_columns);
    global_ctx->checksums_gathered_columns.add(std::move(changed_checksums));

    ctx->delayed_streams.emplace_back(std::move(ctx->column_to));

    while (ctx->delayed_streams.size() > ctx->max_delayed_streams)
    {
        ctx->delayed_streams.front()->finish(ctx->need_sync);
        ctx->delayed_streams.pop_front();
    }

    if (global_ctx->rows_written != ctx->column_elems_written)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Written {} elements of column {}, but {} rows of PK columns",
                        toString(ctx->column_elems_written), column_name, toString(global_ctx->rows_written));
    }

    UInt64 rows = 0;
    UInt64 bytes = 0;
    ctx->column_parts_pipeline.tryGetResultRowsAndBytes(rows, bytes);

    /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

    global_ctx->merge_list_element_ptr->columns_written += 1;
    global_ctx->merge_list_element_ptr->bytes_written_uncompressed += bytes;
    global_ctx->merge_list_element_ptr->progress.store(ctx->progress_before + ctx->column_sizes->columnWeight(column_name), std::memory_order_relaxed);

    /// This is the external loop increment.
    ++ctx->it_name_and_type;
}


bool MergeTask::VerticalMergeStage::finalizeVerticalMergeForAllColumns() const
{
    for (auto & stream : ctx->delayed_streams)
        stream->finish(ctx->need_sync);

    return false;
}


bool MergeTask::MergeProjectionsStage::mergeMinMaxIndexAndPrepareProjections() const
{
    for (const auto & part : global_ctx->future_part->parts)
    {
        /// Skip empty parts,
        /// (that can be created in StorageReplicatedMergeTree::createEmptyPartInsteadOfLost())
        /// since they can incorrectly set min,
        /// that will be changed after one more merge/OPTIMIZE.
        if (!part->isEmpty())
            global_ctx->new_data_part->minmax_idx->merge(*part->minmax_idx);
    }

    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        ProfileEvents::increment(ProfileEvents::MergedColumns, global_ctx->merging_columns.size());
        ProfileEvents::increment(ProfileEvents::GatheredColumns, global_ctx->gathering_columns.size());

        double elapsed_seconds = global_ctx->merge_list_element_ptr->watch.elapsedSeconds();
        LOG_DEBUG(ctx->log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            global_ctx->merge_list_element_ptr->rows_read,
            global_ctx->storage_columns.size(),
            global_ctx->merging_columns.size(),
            global_ctx->gathering_columns.size(),
            elapsed_seconds,
            global_ctx->merge_list_element_ptr->rows_read / elapsed_seconds,
            ReadableSize(global_ctx->merge_list_element_ptr->bytes_read_uncompressed / elapsed_seconds));
    }


    const auto mode = global_ctx->data->getSettings()->deduplicate_merge_projection_mode;
    /// Under throw mode, we still choose to drop projections due to backward compatibility since some
    /// users might have projections before this change.
    if (global_ctx->data->merging_params.mode != MergeTreeData::MergingParams::Ordinary
        && (mode == DeduplicateMergeProjectionMode::THROW || mode == DeduplicateMergeProjectionMode::DROP))
    {
        ctx->projections_iterator = ctx->tasks_for_projections.begin();
        return false;
    }

    const auto & projections = global_ctx->metadata_snapshot->getProjections();

    for (const auto & projection : projections)
    {
        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : global_ctx->future_part->parts)
        {
            auto actual_projection_parts = part->getProjectionParts();
            auto it = actual_projection_parts.find(projection.name);
            if (it != actual_projection_parts.end() && !it->second->is_broken)
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() < global_ctx->future_part->parts.size())
        {
            LOG_DEBUG(ctx->log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }

        LOG_DEBUG(
            ctx->log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
        projection_future_part->assign(std::move(projection_parts));
        projection_future_part->name = projection.name;
        // TODO (ab): path in future_part is only for merge process introspection, which is not available for merges of projection parts.
        // Let's comment this out to avoid code inconsistency and add it back after we implement projection merge introspection.
        // projection_future_part->path = global_ctx->future_part->path + "/" + projection.name + ".proj/";
        projection_future_part->part_info = {"all", 0, 0, 0};

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        ctx->tasks_for_projections.emplace_back(std::make_shared<MergeTask>(
            projection_future_part,
            projection.metadata,
            global_ctx->merge_entry,
            std::make_unique<MergeListElement>((*global_ctx->merge_entry)->table_id, projection_future_part, global_ctx->context),
            global_ctx->time_of_merge,
            global_ctx->context,
            global_ctx->space_reservation,
            global_ctx->deduplicate,
            global_ctx->deduplicate_by_columns,
            global_ctx->cleanup,
            projection_merging_params,
            global_ctx->need_prefix,
            global_ctx->new_data_part.get(),
            ".proj",
            NO_TRANSACTION_PTR,
            global_ctx->data,
            global_ctx->mutator,
            global_ctx->merges_blocker,
            global_ctx->ttl_merges_blocker));
    }

    /// We will iterate through projections and execute them
    ctx->projections_iterator = ctx->tasks_for_projections.begin();

    return false;
}


bool MergeTask::MergeProjectionsStage::executeProjections() const
{
    if (ctx->projections_iterator == ctx->tasks_for_projections.end())
        return false;

    if ((*ctx->projections_iterator)->execute())
        return true;

    ++ctx->projections_iterator;
    return true;
}


bool MergeTask::MergeProjectionsStage::finalizeProjectionsAndWholeMerge() const
{
    for (const auto & task : ctx->tasks_for_projections)
    {
        auto part = task->getFuture().get();
        global_ctx->new_data_part->addProjectionPart(part->name, std::move(part));
    }

    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        global_ctx->to->finalizePart(global_ctx->new_data_part, ctx->need_sync);
    else
        global_ctx->to->finalizePart(global_ctx->new_data_part, ctx->need_sync, &global_ctx->storage_columns, &global_ctx->checksums_gathered_columns);

    if (global_ctx->new_data_part->rows_count == 0)
    {
        global_ctx->can_be_decouple = false;
        global_ctx->only_one_vpart_merged = false;
    }

    /// In decouple case, finalize row ids map info to new data part dir
    /// generate new merged vector index files checksums and combine them
    std::unordered_map<String, DB::MergeTreeDataPartChecksums> vector_index_checksums_map_tmp;
    if (global_ctx->can_be_decouple)
    {
        /// Support multiple vector indices
        for (auto & vec_index : global_ctx->metadata_snapshot->getVectorIndices())
        {
            auto it = global_ctx->all_parts_have_vector_index.find(vec_index.name);
            if (it != global_ctx->all_parts_have_vector_index.end() && it->second)
            {
                /// All the source parts have same vector indices
                for (size_t i = 0; i < global_ctx->future_part->parts.size(); ++i)
                {
                    auto old_part = global_ctx->future_part->parts[i];
                    if (old_part->rows_count == 0)
                        continue;

                    /// move vector index files for this index to new dir
                    auto merged_index_checksums = moveVectorIndexFiles(
                                true, /* decouple */
                                toString(i),
                                old_part->name,
                                vec_index.name,
                                old_part,
                                global_ctx->new_data_part);

                    vector_index_checksums_map_tmp[vec_index.name].add(std::move(merged_index_checksums));
                }
            }
        }
        /// When an exception occurs at the end of move index, 
        /// the move task will have an error loop due to the non-existence of the index file of the source part.
        /// [TODO] Maintain the integrity of the vector index file in the source part

        /// finalize row sources map info to new data part dir
        if (!global_ctx->inverted_rows_sources_map_file)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read global inverted rows sources temporary file");

        auto rows_sources_read_buf = global_ctx->inverted_rows_sources_map_file->getTemporaryFileForReading(RowsSourcesTemporaryFile::FILE_ID);

        String inverted_row_sources_file_path
            = global_ctx->new_data_part->getDataPartStorage().getFullPath() + "merged-inverted_row_sources_map" + VECTOR_INDEX_FILE_SUFFIX;
        auto inverted_row_sources_map_uncompressed_buf = global_ctx->new_data_part->getDataPartStorage().writeFile(
            inverted_row_sources_file_path, 4096, global_ctx->context->getWriteSettings());
        auto inverted_row_sources_map_buf = std::make_unique<CompressedWriteBuffer>(*inverted_row_sources_map_uncompressed_buf);

        DB::copyData(*rows_sources_read_buf, *inverted_row_sources_map_buf);
        inverted_row_sources_map_buf->finalize();
        inverted_row_sources_map_uncompressed_buf->next();
        inverted_row_sources_map_uncompressed_buf->finalize();

        /// add merged-inverted_row_ids_map and merged-inverted_row_sources_map to vector_index_checksums_map
        NameSet index_map_filenames
            = {toString("merged-inverted_row_sources_map") + VECTOR_INDEX_FILE_SUFFIX,
               toString("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX};

        /// add merged-<old_part_id>-<part_name>-row_ids_map to map
        for (size_t i = 0; i < global_ctx->future_part->parts.size(); ++i)
        {
            String row_ids_map_filename
                = "merged-" + toString(i) + "-" + global_ctx->future_part->parts[i]->name + "-row_ids_map" + VECTOR_INDEX_FILE_SUFFIX;
            if (global_ctx->future_part->parts[i]->rows_count == 0)
                continue;
            index_map_filenames.emplace(row_ids_map_filename);
        }

        std::vector<std::tuple<String, UInt64, MergeTreeDataPartChecksum::uint128>> checksums_results;
        for (const auto & map_filename : index_map_filenames)
        {
            auto file_buf = global_ctx->new_data_part->getDataPartStoragePtr()->readFile(map_filename, {}, std::nullopt, std::nullopt);
            HashingReadBuffer hashing_buf(*file_buf);
            hashing_buf.ignoreAll();
            checksums_results.emplace_back(map_filename, hashing_buf.count(), hashing_buf.getHash());
        }
        
        std::set<String> decouple_index_name;
        /// write index checksum file to disk
        for (auto & [vector_index_name, vector_index_checksums] : vector_index_checksums_map_tmp)
        {
            decouple_index_name.insert(vector_index_name);
            for (const auto & [filename_, file_size_, hash_] : checksums_results)
                vector_index_checksums.addFile(filename_, file_size_, hash_);
            
            /// write new part decoupled vector index checksums file
            VectorIndex::dumpCheckSums(global_ctx->new_data_part->getDataPartStoragePtr(), vector_index_name, vector_index_checksums);
        }
    }
    else if (global_ctx->only_one_vpart_merged)
    {
        /// In single one VPart case, move vector index files to new data part dir
        /// Support multiple vector indices
        auto old_part = global_ctx->future_part->parts[global_ctx->first_part_with_data];
        for (auto & vec_index : global_ctx->metadata_snapshot->getVectorIndices())
        {
            auto it = global_ctx->all_parts_have_vector_index.find(vec_index.name);
            if (it != global_ctx->all_parts_have_vector_index.end() && it->second)
            {
                /// move vector index files for this index to new dir
                auto index_checksums = moveVectorIndexFiles(
                    false, /* decouple */
                    toString(0),
                    old_part->name,
                    vec_index.name,
                    old_part,
                    global_ctx->new_data_part);

                /// write new part vector index checksums file
                VectorIndex::dumpCheckSums(global_ctx->new_data_part->getDataPartStoragePtr(), vec_index.name, index_checksums);
            }
        }
    }

    global_ctx->new_data_part->segments_mgr->initSegment();

    global_ctx->new_data_part->getDataPartStorage().precommitTransaction();
    global_ctx->promise.set_value(global_ctx->new_data_part);

    return false;

}

MergeTask::StageRuntimeContextPtr MergeTask::MergeProjectionsStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    /// The projection stage has its own empty projection stage which may add a drift of several milliseconds.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeProjectionStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    return nullptr;
}

bool MergeTask::VerticalMergeStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}

bool MergeTask::MergeProjectionsStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    /// This is the external cycle condition
    if (ctx->it_name_and_type == global_ctx->gathering_columns.end())
        return false;

    switch (ctx->vertical_merge_one_column_state)
    {
        case VerticalMergeRuntimeContext::State::NEED_PREPARE:
        {
            prepareVerticalMergeForOneColumn();
            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_EXECUTE;
            return true;
        }
        case VerticalMergeRuntimeContext::State::NEED_EXECUTE:
        {
            if (executeVerticalMergeForOneColumn())
                return true;

            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_FINISH;
            return true;
        }
        case VerticalMergeRuntimeContext::State::NEED_FINISH:
        {
            finalizeVerticalMergeForOneColumn();
            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_PREPARE;
            return true;
        }
    }
    return false;
}


bool MergeTask::execute()
{
    chassert(stages_iterator != stages.end());
    const auto & current_stage = *stages_iterator;

    if (current_stage->execute())
        return true;

    /// Stage is finished, need to initialize context for the next stage and update profile events.

    UInt64 current_elapsed_ms = global_ctx->merge_list_element_ptr->watch.elapsedMilliseconds();
    UInt64 stage_elapsed_ms = current_elapsed_ms - global_ctx->prev_elapsed_ms;
    global_ctx->prev_elapsed_ms = current_elapsed_ms;

    auto next_stage_context = current_stage->getContextForNextStage();

    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(current_stage->getTotalTimeProfileEvent(), stage_elapsed_ms);
        ProfileEvents::increment(ProfileEvents::MergeTotalMilliseconds, stage_elapsed_ms);
    }

    /// Move to the next stage in an array of stages
    ++stages_iterator;
    if (stages_iterator == stages.end())
        return false;

    (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
    return true;
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::createMergedStream()
{
    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    global_ctx->watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    ctx->read_with_direct_io = false;
    const auto data_settings = global_ctx->data->getSettings();
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : global_ctx->future_part->parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= data_settings->min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(ctx->log, "Will merge parts reading files in O_DIRECT");
                ctx->read_with_direct_io = true;

                break;
            }
        }
    }

    /// Using unique_ptr, because MergeStageProgress has no default constructor
    global_ctx->horizontal_stage_progress = std::make_unique<MergeStageProgress>(
        ctx->column_sizes ? ctx->column_sizes->keyColumnsWeight() : 1.0);

    for (const auto & part : global_ctx->future_part->parts)
    {
        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            *global_ctx->data,
            global_ctx->storage_snapshot,
            part,
            global_ctx->merging_columns.getNames(),
            /*mark_ranges=*/ {},
            global_ctx->input_rows_filtered,
            /*apply_deleted_mask=*/ true,
            ctx->read_with_direct_io,
            /*prefetch=*/ false);

        if (global_ctx->metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([this](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, global_ctx->metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }


    Names sort_columns = global_ctx->metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    sort_description.compile_sort_description = global_ctx->data->getContext()->getSettingsRef().compile_sort_description;
    sort_description.min_count_to_compile_sort_description = global_ctx->data->getContext()->getSettingsRef().min_count_to_compile_sort_description;

    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = global_ctx->metadata_snapshot->getPartitionKey().column_names;

    Block header = pipes.at(0).getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

#ifndef NDEBUG
    if (!sort_description.empty())
    {
        for (size_t i = 0; i < pipes.size(); ++i)
        {
            auto & pipe = pipes[i];
            pipe.addSimpleTransform([&](const Block & header_)
            {
                auto transform = std::make_shared<CheckSortedTransform>(header_, sort_description);
                transform->setDescription(global_ctx->future_part->parts[i]->name);
                return transform;
            });
        }
    }
#endif

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    const bool is_vertical_merge = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);
    /// If merge is vertical we cannot calculate it
    ctx->blocks_are_granules_size = is_vertical_merge;

    /// There is no sense to have the block size bigger than one granule for merge operations.
    const UInt64 merge_block_size_rows = data_settings->merge_max_block_size;
    const UInt64 merge_block_size_bytes = data_settings->merge_max_block_size_bytes;

    WriteBuffer * rows_sources_write_buf = nullptr;
    if (is_vertical_merge || global_ctx->can_be_decouple)
    {
        if (!ctx->rows_sources_temporary_file)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write rows sources temporary file");
        rows_sources_write_buf = &ctx->rows_sources_temporary_file->getTemporaryFileForWriting(RowsSourcesTemporaryFile::FILE_ID);
    }

    switch (ctx->merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_transform = std::make_shared<MergingSortedTransform>(
                header,
                pipes.size(),
                sort_description,
                merge_block_size_rows,
                merge_block_size_bytes,
                SortingQueueStrategy::Default,
                /* limit_= */0,
                /* always_read_till_end_= */false,
                rows_sources_write_buf,
                ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_transform = std::make_shared<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.sign_column, false,
                merge_block_size_rows, merge_block_size_bytes, rows_sources_write_buf, ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_transform = std::make_shared<SummingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.columns_to_sum, partition_key_columns, merge_block_size_rows, merge_block_size_bytes);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_transform = std::make_shared<AggregatingSortedTransform>(header, pipes.size(), sort_description, merge_block_size_rows, merge_block_size_bytes);
            break;

        case MergeTreeData::MergingParams::Replacing:
            if (global_ctx->cleanup && !data_settings->allow_experimental_replacing_merge_with_cleanup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental merges with CLEANUP are not allowed");

            merged_transform = std::make_shared<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.is_deleted_column, ctx->merging_params.version_column,
                merge_block_size_rows, merge_block_size_bytes, rows_sources_write_buf, ctx->blocks_are_granules_size,
                global_ctx->cleanup);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_transform = std::make_shared<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size_rows, merge_block_size_bytes,
                ctx->merging_params.graphite_params, global_ctx->time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_transform = std::make_shared<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.sign_column,
                merge_block_size_rows, merge_block_size_bytes, rows_sources_write_buf, ctx->blocks_are_granules_size);
            break;
    }

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));
    builder->addTransform(std::move(merged_transform));

#ifndef NDEBUG
    if (!sort_description.empty())
    {
        builder->addSimpleTransform([&](const Block & header_)
        {
            auto transform = std::make_shared<CheckSortedTransform>(header_, sort_description);
            return transform;
        });
    }
#endif

    if (global_ctx->deduplicate)
    {
        const auto & virtuals = *global_ctx->data->getVirtualsPtr();

        /// We don't want to deduplicate by virtual persistent column.
        /// If deduplicate_by_columns is empty, add all columns except virtuals.
        if (global_ctx->deduplicate_by_columns.empty())
        {
            for (const auto & column : global_ctx->merging_columns)
            {
                if (virtuals.tryGet(column.name, VirtualsKind::Persistent))
                    continue;

                global_ctx->deduplicate_by_columns.emplace_back(column.name);
            }
        }

        if (DistinctSortedTransform::isApplicable(header, sort_description, global_ctx->deduplicate_by_columns))
            builder->addTransform(std::make_shared<DistinctSortedTransform>(
                builder->getHeader(), sort_description, SizeLimits(), 0 /*limit_hint*/, global_ctx->deduplicate_by_columns));
        else
            builder->addTransform(std::make_shared<DistinctTransform>(
                builder->getHeader(), SizeLimits(), 0 /*limit_hint*/, global_ctx->deduplicate_by_columns));
    }

    PreparedSets::Subqueries subqueries;

    if (ctx->need_remove_expired_values)
    {
        auto transform = std::make_shared<TTLTransform>(global_ctx->context, builder->getHeader(), *global_ctx->data, global_ctx->metadata_snapshot, global_ctx->new_data_part, global_ctx->time_of_merge, ctx->force_ttl);
        subqueries = transform->getSubqueries();
        builder->addTransform(std::move(transform));
    }

    if (!global_ctx->merging_skip_indexes.empty())
    {
        builder->addTransform(std::make_shared<ExpressionTransform>(
            builder->getHeader(),
            global_ctx->merging_skip_indexes.getSingleExpressionForIndices(global_ctx->metadata_snapshot->getColumns(),
            global_ctx->data->getContext())));

        builder->addTransform(std::make_shared<MaterializingTransform>(builder->getHeader()));
    }

    if (!subqueries.empty())
        builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), global_ctx->context);

    global_ctx->merged_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    /// Dereference unique_ptr and pass horizontal_stage_progress by reference
    global_ctx->merged_pipeline.setProgressCallback(MergeProgressCallback(global_ctx->merge_list_element_ptr, global_ctx->watch_prev_elapsed, *global_ctx->horizontal_stage_progress));
    /// Is calculated inside MergeProgressCallback.
    global_ctx->merged_pipeline.disableProfileEventUpdate();

    global_ctx->merging_executor = std::make_unique<PullingPipelineExecutor>(global_ctx->merged_pipeline);
}


MergeAlgorithm MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm() const
{
    const size_t total_rows_count = global_ctx->merge_list_element_ptr->total_rows_count;
    const size_t total_size_bytes_uncompressed = global_ctx->merge_list_element_ptr->total_size_bytes_uncompressed;
    const auto data_settings = global_ctx->data->getSettings();

    if (global_ctx->deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data_settings->enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (ctx->need_remove_expired_values)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->future_part->part_format.part_type != MergeTreeDataPartType::Wide)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->future_part->part_format.storage_type != MergeTreeDataPartStorageType::Full)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->cleanup)
        return MergeAlgorithm::Horizontal;

    if (!data_settings->allow_vertical_merges_from_compact_to_wide_parts)
    {
        for (const auto & part : global_ctx->future_part->parts)
        {
            if (!isWidePart(part))
                return MergeAlgorithm::Horizontal;
        }
    }

    bool is_supported_storage =
        ctx->merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = global_ctx->gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = total_rows_count >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool enough_total_bytes = total_size_bytes_uncompressed >= data_settings->vertical_merge_algorithm_min_bytes_to_activate;

    bool no_parts_overflow = global_ctx->future_part->parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_total_bytes && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}

}
