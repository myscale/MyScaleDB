#include <DataTypes/DataTypeArray.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVectorIndexBuilderUpdater.h>
#include <VectorIndex/DiskIOReader.h>
#include <VectorIndex/VectorSegmentExecutor.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

/// #define build_fail_test

namespace ProfileEvents
{
extern const Event VectorIndexBuildFailEvents;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/// minimum interval (seconds) between check if need to remove dropped vector index cache.
static const auto RECHECK_VECTOR_INDDEX_CACHE_INTERVAL_SECONDS = 600;

MergeTreeVectorIndexBuilderUpdater::MergeTreeVectorIndexBuilderUpdater(MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (VectorIndexUpdater)"))
{
}

void MergeTreeVectorIndexBuilderUpdater::removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot)
{
    auto now = time(nullptr);
    if (last_cache_check_time == 0)
        last_cache_check_time = now;

    /// we don't want to check vector index too frequent.
    if (now - last_cache_check_time < RECHECK_VECTOR_INDDEX_CACHE_INTERVAL_SECONDS)
        return;

    /// Update last_cache_check
    last_cache_check_time = now;

    ///check existing parts to see if any cached vector index need cleaning
    std::list<std::pair<VectorIndex::CacheKey, VectorIndex::Parameters>> cached_item_list
        = VectorIndex::VectorSegmentExecutor::getAllCacheNames();

    /// getRelativeDataPath() contains '/' in the tail, but table_path in cache key doesn't have.
    std::string relative_data_path = fs::path(data.getRelativeDataPath()).parent_path().string();
    for (const auto & cache_item : cached_item_list)
    {
        bool existed = false;

        /// not this table
        if (cache_item.first.table_path.find(relative_data_path) == std::string::npos)
            continue;

        const auto cache_key = cache_item.first;

        /// Need to check part no matter exists or not exists.
        MergeTreeDataPartPtr part = data.getActiveContainingPart(cache_item.first.part_name_no_mutation);

        /// Check vector index in cache is same as metadata
        if (!metadata_snapshot->vec_indices.empty())
        {
            /// Currently only one vector index is allowed.
            const auto & vec_index_desc = metadata_snapshot->vec_indices[0];

            LOG_DEBUG(log, "cache: {} {}, metadata: {} {}", cache_item.first.vector_index_name, cache_item.first.column_name, vec_index_desc.name, vec_index_desc.column);

            /// Further check the part status, decouple part or VPart with single vector index
            if (cache_item.first.vector_index_name == vec_index_desc.name && cache_item.first.column_name == vec_index_desc.column &&
                (part && (part->containVectorIndex(cache_item.first.vector_index_name, cache_item.first.column_name) || part->containRowIdsMaps())))
            {
                LOG_DEBUG(log, "Find Vector Index in metadata");
                VectorIndex::Parameters params = cache_item.second;
                VectorIndex::IndexType t = VectorIndex::VectorIndexFactory::createIndexType(params.find("type")->second);
                params.erase("type");

                LOG_DEBUG(log, "params: {}, desc params: {}", VectorIndex::ParametersToString(params),
                    VectorIndex::ParametersToString(VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters)));
                
                if (VectorIndex::VectorSegmentExecutor::compareVectorIndexParameters(
                        t,
                        params,
                        VectorIndex::VectorIndexFactory::createIndexType(vec_index_desc.type),
                        VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters)))
                {
                    LOG_DEBUG(log, "Vector Index parameters match!");
                    existed = true;
                }
            }
        }

        if (!existed)
        {
            LOG_DEBUG(log, "Find not existed cache, remove it: {}", cache_key.toString());
            VectorIndex::VectorSegmentExecutor::removeFromCache(cache_key);

            /// Clear vector files in active part
            if (part)
            {
                if (part->containVectorIndex(cache_key.vector_index_name, cache_key.column_name))
                {
                    LOG_DEBUG(log, "Remove files of dropped vector index {} for part {}", cache_key.vector_index_name, part->name);
                    part->removeVectorIndex(cache_key.vector_index_name, cache_key.column_name);
                }
                else if (part->containRowIdsMaps()) /// Decouple part
                {
                    LOG_DEBUG(log, "Remove old parts' vector index files {} for decouple part {}", cache_key.vector_index_name, part->name);
                    part->removeAllRowIdsMaps();
                }
            }
        }
    }
}

VectorIndexEntryPtr MergeTreeVectorIndexBuilderUpdater::selectPartsToBuildVectorIndex(
    const StorageMetadataPtr & metadata_snapshot,
    size_t max_parts_number,
    bool select_slow_mode_parts,
    const MergeTreeData::DataParts & currently_merging_mutating_parts)
{
    if (metadata_snapshot->vec_indices.empty())
    {
        return {};
    }

    std::vector<String> part_names;

    size_t min_rows_to_build_vector_index = data.getSettings()->min_rows_to_build_vector_index;
    for (const auto & part : data.getDataPartsForInternalUsage())
    {
        /// LOG_DEBUG(log, "[selectPartsToBuildVectorIndex] part name: {}, count: {}", part->name, currently_vector_indexing_parts.count(part->name));

        /// Skip empty part
        if (part->isEmpty())
            continue;

        /// need to check currently_vector_indexing_parts.count(part) > 0
        if (data.currently_vector_indexing_parts.count(part->name) > 0 || part->vector_index_build_error || currently_merging_mutating_parts.count(part) > 0)
            continue;

        if (part->containRowIdsMaps() && data.getSettings()->distable_rebuild_for_decouple)
            continue;

        /// Since building vector index doesn't block mutation on the part, the new part need to check if any covered part is building vindex.
        /// The new part already blocked merge to select it, hence it's safe here. all_1_1_0 can avoid index build selection for future parts all_1_1_0_*
        bool skip_build_index = false;
        for (const auto & part_name : data.currently_vector_indexing_parts)
        {
            auto info = MergeTreePartInfo::fromPartName(part_name, data.format_version);
            if (part->info.contains(info))
            {
                LOG_DEBUG(log, "[selectPartsToBuildVectorIndex] skip for future part {} due to origin part {}", part->name, part_name);
                skip_build_index = true;
                break;
            }
        }

        if (skip_build_index)
            continue;

        for (const auto & vec_index : metadata_snapshot->vec_indices)
        {
            if (!part->containVectorIndex(vec_index.name, vec_index.column) && !part->isSmallPart(min_rows_to_build_vector_index))
            {
                if (select_slow_mode_parts)
                {
                    if (!isSlowModePart(part))
                        continue;

                    LOG_DEBUG(log, "[selectPartsToBuildVectorIndex] select slow mode part name: {}, count: {}", part->name, data.currently_vector_indexing_parts.count(part->name));
                    part_names.emplace_back(part->name);
                }
                else /// normal fast mode
                {
                    if (isSlowModePart(part))
                        continue;

                    LOG_DEBUG(log, "[selectPartsToBuildVectorIndex] select part name: {}, count: {}", part->name, data.currently_vector_indexing_parts.count(part->name));
                    part_names.emplace_back(part->name);
                }

                LOG_TRACE(log, "this part will get built index: {}", part->name);
                ///since each index building task is time-consuming, it's pointless to have a long list
                if (part_names.size() >= max_parts_number)
                {
                    return std::make_shared<VectorIndexEntry>(part_names, data);
                }
                break;
            }
        }
    }

    if (part_names.empty())
    {
        return {};
    }
    else
    {
        return std::make_shared<VectorIndexEntry>(part_names, data);
    }
}

BuildVectorIndexStatus MergeTreeVectorIndexBuilderUpdater::buildVectorIndex(
    const StorageMetadataPtr & metadata_snapshot, const std::vector<String> & part_names, bool tune, bool slow_mode)
{
    if (part_names.empty())
    {
        LOG_INFO(log, "no data");
        return BuildVectorIndexStatus::NO_DATA_PART;
    }

    if (metadata_snapshot->vec_indices.empty())
    {
        LOG_INFO(log, "no vector index declared");
        return BuildVectorIndexStatus::SUCCESS;
    }

    Stopwatch watch;
    /// build vector index part by part
    /// we may consider building vector index in parallel in the future.
    if (!part_names.empty())
        LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask for {} start, slow_mode: {}", part_names[0], slow_mode);
    for (auto & part_name : part_names)
    {
        MergeTreeDataPartPtr part = data.getActiveContainingPart(part_name);
        if (!part)
        {
            continue;
        }

        if (part->vector_index_build_cancelled)
        {
            LOG_INFO(log, "[buildVectorIndex] part:{}, build index job has been cancelled.", part->name);
            continue;
        }

        /// Check latest metadata
        if (part->storage.getInMemoryMetadataPtr()->vec_indices.empty())
        {
            LOG_INFO(log, "Vector index has been dropped, no need to build it.");
            return BuildVectorIndexStatus::SUCCESS;
        }

        const DataPartStorageOnDiskBase * part_storage
            = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
        if (part_storage == nullptr)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
        }
        String vector_tmp_full_path = data.getFullPathOnDisk(part_storage->getDisk()) + "vector_tmp_" + part->info.getPartNameWithoutMutation() + "/";
        constexpr int maxBuildRetryCount = 3;
        int failed_count = counter.get(vector_tmp_full_path);
        if (failed_count >= maxBuildRetryCount)
        {
            part->setBuildError();
            throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "part = {}, has MEMORY_LIMIT_EXCEEDED for max retry times {}", part->name, failed_count);
        }

        bool mem_limit_happened = false;

        BuildVectorIndexStatus status = BuildVectorIndexStatus::SUCCESS;
        try
        {
            LOG_INFO(log, "[buildVectorIndex] begin to build vector index of one part {}", part->name);
            status = buildVectorIndexForOnePart(metadata_snapshot, part, tune, slow_mode);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            {
                status = BuildVectorIndexStatus::BUILD_FAIL;
                int temp_value = counter.increaseAndGet(part->getDataPartStorage().getRelativePath());
                LOG_WARNING(log, "[buildVectorIndex] part = {}, has MEMORY_LIMIT_EXCEEDED for {} times", part->name, temp_value);
                mem_limit_happened = true;
            }
            else
            {
                throw;
            }
        }

        if (status != BuildVectorIndexStatus::SUCCESS)
        {
            if (mem_limit_happened)
            {
                undoBuildVectorIndexForOnePart(metadata_snapshot, part);
            }
            if (status == BuildVectorIndexStatus::BUILD_FAIL)
            {
                part->setBuildError();
                ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);
            }
        }

        LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask finished for part {}, status {}", part->name, status);
    }

    watch.stop();
    if (!part_names.empty())
        LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask for {} finished in {} sec, slow_mode: {}", part_names[0], watch.elapsedSeconds(), slow_mode);

#ifdef build_fail_test
    LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask increment VectorIndexBuildFailEvents.");
    ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);
#endif
    // TODO: handle fail case
    return BuildVectorIndexStatus::SUCCESS;
}

BuildVectorIndexStatus MergeTreeVectorIndexBuilderUpdater::buildVectorIndexForOnePart(
    const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part, bool tune, bool slow_mode)
{
    MergeTreeReaderSettings reader_settings;

    /// float incremental_ratio = data.getContext()->getSettingsRef().incremental_build_index_ratio;
    /// size_t max_build_index_block_size_rows = data.getContext()->getSettingsRef().max_build_index_block_size_rows;

    /// try to control memory usage only use max_build_index_block_size_bytes
    size_t max_build_index_block_size_bytes
        = data.getContext()->getConfigRef().getUInt64("max_build_index_block_size_bytes", 512 * 1024 * 1024);

    LOG_INFO(log, "[buildVectorIndex] part:{}, start checking for build index", part->name);
    for (auto & vec_index_desc : metadata_snapshot->vec_indices)
    {
        LOG_INFO(log, "[buildVectorIndex] vec_index_desc data column: {}", vec_index_desc.column);
        for (auto & param : VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters))
        {
            LOG_INFO(log, "[buildVectorIndex] vec_index_desc parameters: {},{}", param.first, param.second);
        }
        LOG_INFO(log, "[buildVectorIndex] vec_index_desc type: {}", vec_index_desc.type);
        auto col_names = part->getColumns().getNames();
        NamesAndTypesList cols;

        /// only one column to build vector index, using a large dimension as default value.
        uint64_t dim = 960;

        /// read all the columns which are marked as having vector index from the part
        /// there should only be one column here
        for (const auto & col : col_names)
        {
            if (vec_index_desc.column == col && (!part->vector_indexed.contains(vec_index_desc.name + "_" + vec_index_desc.column)))
            {
                auto col_and_type = metadata_snapshot->getColumns().getAllPhysical().tryGetByName(col);
                if (col_and_type)
                {
                    cols.emplace_back(*col_and_type);
                    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(col_and_type->getTypeInStorage().get());
                    if (array_type)
                    {
                        LOG_INFO(log, "[buildVectorIndex] dim: {}", array_type->getDim());
                        dim = array_type->getDim();
                        if (dim == 0)
                        {
                            LOG_ERROR(log, "[buildVectorIndex] wrong dimension: 0");
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "wrong dimension: 0");
                        }
                    }
                    ///only reading one column
                    break;
                }
                else
                {
                    LOG_INFO(
                        log, "[buildVectorIndex] found column {} in part and vectorIndexDexcription, but not in metadata snapshot.", col);
                    return BuildVectorIndexStatus::META_ERROR;
                }
            }
        }
        if (cols.empty())
        {
            LOG_DEBUG(
                log,
                "vec_index_desc {} has being built for part {} or no column can match vec_index_desc",
                vec_index_desc.name,
                part->name);
            part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
            return BuildVectorIndexStatus::SUCCESS;
        }

        /// below is a horror to test whether a moved part need to rebuild its index. basially is reads from vector_index_ready if there is one,
        /// creates a pesudo vector index using parameters recorded in vector_index_ready and compare with the new index to see if they are the same.
        const DataPartStorageOnDiskBase * part_storage
            = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
        if (part_storage == nullptr)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
        }
        auto disk = part_storage->getDisk();
        String read_file_path;
        String vector_index_ready_file_name = DB::toString("vector_index_ready") + VECTOR_INDEX_FILE_SUFFIX;
        bool from_part = true; // false from temporary directory

        /// Create temporary directory to store built vector index files.
        /// The director name starts with prefix "vector_tmp_" + part_name w/o_mutation, e.g. part all_1_1_0_5: vector_tmp_all_1_1_0
        String part_name_prefix = part->info.getPartNameWithoutMutation();
        String vector_tmp_relative_path = data.getRelativeDataPath() + "vector_tmp_" + part_name_prefix + "/";
        String vector_tmp_full_path = data.getFullPathOnDisk(disk) + "vector_tmp_" + part_name_prefix + "/";

        /// Since the vector index is stored in a temporary directory, add check for it too.
        if (disk->exists(part->getDataPartStorage().getRelativePath() + vector_index_ready_file_name))
            read_file_path = part->getDataPartStorage().getFullPath() + vector_index_ready_file_name;
        else
        {
            /// Loop through the relative_data_path to check if any directory with vector_tmp_<part_name_without_mutation> exists
            if (disk->exists(vector_tmp_relative_path))
            {
                if (disk->exists(vector_tmp_relative_path + "/" + vector_index_ready_file_name))
                {
                    read_file_path = vector_tmp_full_path + vector_index_ready_file_name;
                    from_part = false;
                }
                else
                {
                    disk->removeRecursive(vector_tmp_relative_path);
                    LOG_DEBUG(log, "[buildVectorIndex] remove incomplete temporary directory {}", vector_tmp_relative_path);
                }
            }
        }

        VectorIndex::Parameters parameters = VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters);
        std::unordered_map<std::string, VectorIndex::Parameters> params_from_record;
        VectorIndex::DiskIOReader disk_reader;
        std::vector<String> index_names;
        std::string index_name = vec_index_desc.name + "_" + vec_index_desc.column;
        index_names.emplace_back(index_name);

        if (!read_file_path.empty())
        {
            std::unordered_map<String, int64_t> original_binary_sizes
                    = readVectorIndexReadyFile(disk_reader, read_file_path, index_names, params_from_record);

            if (!original_binary_sizes.empty())
            {
                VectorIndex::Parameters & single_params_from_record = params_from_record.find(index_name)->second;
                if (!single_params_from_record.empty() && original_binary_sizes.find(index_name)->second != -1)
                {
                    VectorIndex::IndexType t = VectorIndex::VectorIndexFactory::createIndexType(single_params_from_record.find("type")->second);
                    single_params_from_record.erase("type");
                    if (VectorIndex::VectorSegmentExecutor::compareVectorIndexParameters(
                            t, single_params_from_record, VectorIndex::VectorIndexFactory::createIndexType(vec_index_desc.type), parameters))
                    {
                        if (from_part)
                        {
                            LOG_INFO(log, "[buildVectorIndex] the index is built for part: {}", part->name);
                            part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
                        }
                        else /// Need to move built vector index files to the part.
                        {
                            LOG_INFO(log, "[buildVectorIndex] the index is built for part: {} and stored in temporary directory {}", part->name, vector_tmp_full_path);
                            MergeTreeDataPartPtr future_part = nullptr;
                            if (part->getState() == DB::MergeTreeDataPartState::Active)
                                future_part = part;
                            else
                            {
                                /// Find future active part
                                future_part = data.getActiveContainingPart(part->name);
                                if (!future_part)
                                {
                                    LOG_WARNING(log, "[buildVectorIndex] failed to find future part for part {}, leave the temporary directory", part->name);
                                    return BuildVectorIndexStatus::SUCCESS;
                                }
                            }

                            if (future_part && !future_part->getPartIsMutating())
                            {
                                moveVectorIndexFilesToFuturePart(metadata_snapshot, vector_tmp_relative_path, future_part);

                                if (future_part->containRowIdsMaps())
                                {
                                    auto lock = data.lockParts();
                                    VectorIndex::removeRowIdsMaps(future_part);
                                }
                            }
                            /// else future part will pick up later at the next time when index built for it.
                        }

                        return BuildVectorIndexStatus::SUCCESS;
                    }
                }
            }
        }

        auto reader = part->getReader(
            cols,
            metadata_snapshot,
            MarkRanges{MarkRange(0, part->getMarksCount())},
            /* uncompressed_cache = */ nullptr,
            data.getContext()->getMarkCache().get(),
            reader_settings,
            {},
            {});

        size_t num_rows_read = 0;
        /// max read block rows for each round
        /// size_t read_block_rows_num = std::max(max_build_index_block_size_rows, static_cast<size_t>(part->rows_count * incremental_ratio));

        /// never divide a zero
        size_t read_block_rows_num = max_build_index_block_size_bytes / 4 / (dim > 1 ? dim : 1);
        LOG_INFO(log, "[buildVectorIndex] set read_block_rows_num to {}", read_block_rows_num);
        bool continue_read = false;
        bool training = true;

        VectorIndex::VectorDatasetPtr vec_data;
        VectorIndex::VectorSegmentExecutorPtr vec_index_builder;
        std::vector<int64_t> empty_ids;
        size_t current_round_start_row = 0;

        size_t current_mask = 0;
        size_t total_mask = part->getMarksCount();

        auto & index_granularity = part->index_granularity;

        /// process data block by block
        while (num_rows_read < part->rows_count)
        {
            if (part->vector_index_build_cancelled)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector index build is cancelled for part {}", part->name);
            }
            empty_ids.clear();
            size_t remaining_size = part->rows_count - num_rows_read;
            size_t max_read_row = std::min(remaining_size, read_block_rows_num);

            Columns result(cols.size());
            size_t num_rows = reader->readRows(current_mask, 0, continue_read, max_read_row, result);

            continue_read = true;

            num_rows_read += num_rows;

            for (size_t mask = 0; mask < total_mask - 1; ++mask)
            {
                if (index_granularity.getMarkStartingRow(mask) >= num_rows_read
                    && index_granularity.getMarkStartingRow(mask + 1) < num_rows_read)
                {
                    current_mask = mask;
                }
            }

            LOG_DEBUG(log, "[buildVectorIndex] part:{}, read num_rows: {}, col size: {}", part->name, num_rows, cols.size());

            if (num_rows == 0)
            {
                LOG_WARNING(log, "[buildVectorIndex] part:{}, no data read for column {}", part->name, cols.back().name);
                part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
                break;
            }

            const auto & one_column = result.back();
            const ColumnArray * array = checkAndGetColumn<ColumnArray>(one_column.get());
            const IColumn & src_data = array->getData();
            const ColumnArray::Offsets & offsets = array->getOffsets();
            const ColumnFloat32 * src_data_concrete = checkAndGetColumn<ColumnFloat32>(&src_data);
            const PaddedPODArray<Float32> & src_vec = src_data_concrete->getData();
            // size_t size = offsets.size();
            if (src_vec.empty())
            {
                LOG_WARNING(log, "[buildVectorIndex] part:{}, no data read for column {}", part->name, cols.back().name);
                part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
                return BuildVectorIndexStatus::SUCCESS;
            }

            size_t i = 0;

            /// skip empty arrays, to compute dimension of vector data
            /// offsets.size(): vector data number
            while (i < offsets.size() && offsets[i] == 0)
            {
                ++i;
            }

            /// the real dimension created from data
            int32_t dim = static_cast<int32_t>(offsets[i]);

            std::vector<float> vector_raw_data(dim * offsets.size(), 0.0);
            current_round_start_row = num_rows_read - num_rows;

            for (size_t row = 0; row < offsets.size(); ++row)
            {
                size_t vec_start_offset = row != 0 ? offsets[row - 1] : 0;
                size_t vec_end_offset = offsets[row];
                if (vec_start_offset != vec_end_offset)
                {
                    for (size_t offset = vec_start_offset; offset < vec_end_offset; ++offset)
                    {
                        vector_raw_data[row * dim + offset - vec_start_offset] = src_vec[offset];
                    }
                }
                else
                {
                    /// add this read round empty ids
                    empty_ids.emplace_back(current_round_start_row + row);
                }
            }

            LOG_DEBUG(
                log,
                "[buildVectorIndex] part:{}, raw_data size: {}, empty_ids size: {}",
                part->name,
                vector_raw_data.size(),
                empty_ids.size());

            vec_data = std::make_shared<VectorIndex::VectorDataset>(
                static_cast<int32_t>(offsets.size()), static_cast<int32_t>(dim), std::move(vector_raw_data));

            /// only run in the first read round
            if (training)
            {
                LOG_INFO(
                    log,
                    "[buildVectorIndex] index train: part_name: {}, num_rows: {}, vector index name: {}, path: {}",
                    part->name,
                    num_rows,
                    vec_index_desc.name,
                    vector_tmp_relative_path + index_name);

                /// Create temp directory before serialize.
                if (disk->exists(vector_tmp_relative_path))
                {
                    LOG_DEBUG(log, "[buildVectorIndex] the temporary directory to store vector index files already exists, will be removed {}", vector_tmp_relative_path);
                    disk->removeRecursive(vector_tmp_relative_path);
                }

                disk->createDirectories(vector_tmp_relative_path);

                VectorIndex::SegmentId segment_id(vector_tmp_full_path, part->name, part->name, vec_index_desc.name, vec_index_desc.column, 0);

                vec_index_builder = std::make_shared<VectorIndex::VectorSegmentExecutor>(
                    VectorIndex::VectorIndexFactory::createIndexType(vec_index_desc.type),
                    segment_id,
                    parameters,
                    dim);
                VectorIndex::Status build_status = vec_index_builder->buildIndex(vec_data, part->rows_count, slow_mode);

                if (!build_status.fine())
                {
                    LOG_ERROR(log, "[buildVectorIndex] failed to build vector index for part {}", part->name);
                    disk->removeRecursive(vector_tmp_relative_path);
                    throw Exception(build_status.getCode(), build_status.getMessage().data());
                }
                training = false;
            }
            LOG_INFO(log, "[buildVectorIndex] index add vectors: read vector num: {}", vec_data->getVectorNum());
            vec_index_builder->addVectors(vec_data);
            if (!empty_ids.empty())
            {
                vec_index_builder->removeByIds(empty_ids.size(), empty_ids.data());
            }
            LOG_INFO(log, "[buildVectorIndex] index after read vectors: read vector num: {}", vec_data->getVectorNum());
        }

        if (num_rows_read == 0 && part->rows_count == 0)
        {
            LOG_WARNING(log, "[buildVectorIndex] part {} is empty", part->name);
            continue;
        }
        else if (num_rows_read < part->rows_count)
        {
            LOG_ERROR(log, "[buildVectorIndex] failed to build vector index for part {}", part->name);
            disk->removeRecursive(vector_tmp_relative_path);
            return BuildVectorIndexStatus::BUILD_FAIL;
        }

        if (!part->vector_index_build_cancelled)
        {
            if (tune)
            {
                vec_index_builder->dispathAutoTuneTask(vec_data);
            }
            /// zili's profiler tuning logic
            vec_index_builder->tune(vec_data, empty_ids, current_round_start_row);

            /// remove empty vectors
            LOG_INFO(log, "[buildVectorIndex] index serialize");
            VectorIndex::Status seri_status = vec_index_builder->serialize();
            LOG_INFO(log, "[buildVectorIndex] after serialize status: {}", seri_status.getCode());
            if (!seri_status.fine())
            {
                /// Remove temporay directory
                disk->removeRecursive(vector_tmp_relative_path);

                throw Exception(seri_status.getCode(), seri_status.getMessage().data());
            }

            /// Done with writing vector index files to temporary directory.
            /// Decide to move index files to which part direcory.
            MergeTreeDataPartPtr future_part = nullptr;
            if (part->getState() == DB::MergeTreeDataPartState::Active)
            {
                future_part = part;
            }
            else
            {
                /// Find future active part
                future_part = data.getActiveContainingPart(part->name);
                if (!future_part)
                {
                    LOG_WARNING(log, "[buildVectorIndex] failed to find future part for part {}, leave the temporary directory", part->name);
                    return BuildVectorIndexStatus::SUCCESS;
                }
            }

            if (future_part)
            {
                /// Check the latest metadata before move files, in case drop index submitted during index building.
                if (future_part->storage.getInMemoryMetadataPtr()->vec_indices.empty())
                {
                    LOG_INFO(log, "Vector index has been dropped, no need to build it.");
                    disk->removeRecursive(vector_tmp_relative_path);
                    return BuildVectorIndexStatus::SUCCESS;
                }

                /// First, move index files to part and apply lightweight delete.
                moveVectorIndexFilesToFuturePart(metadata_snapshot, vector_tmp_relative_path, future_part);

                /// Second, update delete bitmap in memory in currently builder, which will be put in cache.
                /// Update segment id with correct part name and path.
                VectorIndex::SegmentId segment_id(future_part->getDataPartStorage().getFullPath(), future_part->name, future_part->name, vec_index_desc.name, vec_index_desc.column, 0);
                vec_index_builder->updateSegmentId(segment_id);

                /// Need to reload delete bitmap from disk. The delete_bitmap in vec_index_builder doesn't contain rows deleted by lightweight.
                if (future_part->hasLightweightDelete())
                    vec_index_builder->reloadDeleteBitMap();

                LOG_INFO(log, "[buildVectorIndex] index cache: status: {}", seri_status.getCode());
                vec_index_builder->cache();
                LOG_INFO(log, "[buildVectorIndex] index after cache: status: {}", seri_status.getCode());

                if (future_part->containRowIdsMaps())
                {
                    auto lock = data.lockParts();
                    VectorIndex::removeRowIdsMaps(future_part);
                }
            }
        }
    }

    LOG_INFO(log, "[buildVectorIndex] index build complete");

    return BuildVectorIndexStatus::SUCCESS;
}

void MergeTreeVectorIndexBuilderUpdater::undoBuildVectorIndexForOnePart(
    const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part)
{
    for (auto & vec_index_desc : metadata_snapshot->vec_indices)
    {
        String index_name = vec_index_desc.name + "_" + vec_index_desc.column;
        part->removeVectorIndex(vec_index_desc.name, vec_index_desc.column);
        VectorIndex::SegmentId segment_id(part->getDataPartStorage().getFullPath(), part->name, vec_index_desc.name, vec_index_desc.column, 0);
        VectorIndex::VectorSegmentExecutor::removeFromCache(segment_id.getCacheKey());
    }
}

void MergeTreeVectorIndexBuilderUpdater::Counter::put(const String & key, int value)
{
    std::lock_guard<std::mutex> lg(mu_);

    counter_[key] = value;
}

int MergeTreeVectorIndexBuilderUpdater::Counter::get(const String & key)
{
    std::lock_guard<std::mutex> lg(mu_);

    if (counter_.find(key) == counter_.cend())
    {
        return 0;
    }
    else
    {
        return counter_[key];
    }
}

int MergeTreeVectorIndexBuilderUpdater::Counter::increaseAndGet(const String & key)
{
    std::lock_guard<std::mutex> lg(mu_);

    if (counter_.find(key) == counter_.cend())
    {
        counter_[key] = 1;
        return 1;
    }
    else
    {
        int t = counter_[key] + 1;
        counter_[key] = t;
        return t;
    }
}

bool MergeTreeVectorIndexBuilderUpdater::moveVectorIndexFilesToFuturePart(const StorageMetadataPtr & metadata_snapshot, const String & vector_tmp_relative_path, const MergeTreeDataPartPtr & dest_part)
{
    if (!dest_part)
        return false;

    LOG_DEBUG(log, "[buildVectorIndex] current active part {} is selected to store vector index {}", dest_part->name, dest_part->getState());

    const DataPartStorageOnDiskBase * dest_part_storage
        = dynamic_cast<const DataPartStorageOnDiskBase *>(dest_part->getDataPartStoragePtr().get());
    if (dest_part_storage == nullptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
    }
    auto disk = dest_part_storage->getDisk();
    String dest_relative_path = dest_part->getDataPartStorage().getRelativePath();

    /// Move to current part which is active.
    bool found_vector_file = false;
    for (auto it = disk->iterateDirectory(vector_tmp_relative_path); it->isValid(); it->next())
    {
        if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX))
            continue;
        disk->moveFile(vector_tmp_relative_path + it->name(), dest_relative_path + it->name());

        if (!found_vector_file)
            found_vector_file = true;
    }

    if (!found_vector_file)
    {
        LOG_DEBUG(log, "[buildVectorIndex] failed to find any vector index files in directory {}, will remove it", vector_tmp_relative_path);
        disk->removeRecursive(vector_tmp_relative_path);

        return false;
    }

    for (auto & vec_index_desc : metadata_snapshot->vec_indices)
        dest_part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);

    disk->removeRecursive(vector_tmp_relative_path);

    LOG_INFO(log, "[buildVectorIndex] move vector index files to part {}", dest_part->name);

    /// Apply lightweight delete bitmap to index's bitmap
    if (dest_part->hasLightweightDelete())
    {
        LOG_DEBUG(log, "[buildVectorIndex] apply lightweight delete to vector index in part {}", dest_part->name);
        dest_part->onLightweightDelete();
    }

    return true;
}

}
