#include <DataTypes/DataTypeArray.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVectorIndexBuilderUpdater.h>
#include <VectorIndex/DiskIOReader.h>
#include <VectorIndex/VectorSegmentExecutor.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <VectorIndex/MergeUtils.h>
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
}

MergeTreeVectorIndexBuilderUpdater::MergeTreeVectorIndexBuilderUpdater(MergeTreeData & data_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + " (VectorIndexUpdater)"))
{
}

void MergeTreeVectorIndexBuilderUpdater::removeDroppedVectorIndices(const StorageMetadataPtr & metadata_snapshot)
{
    ///check existing parts to see if any cached vector index need cleaning
    std::list<std::pair<VectorIndex::CacheKey, VectorIndex::Parameters>> cached_item_list
        = VectorIndex::VectorSegmentExecutor::getAllCacheNames();

    for (const auto & cache_item : cached_item_list)
    {
        bool existed = false;
        /// LOG_DEBUG(log, "relative_path: {}", data.getRelativeDataPath());
        /// not this table
        if (cache_item.first.table_path.find(data.getRelativeDataPath()) == std::string::npos)
        {
            continue;
        }
        for (const auto & vec_index_desc : metadata_snapshot->vec_indices)
        {
            LOG_DEBUG(log, "cache: {} {}, metadata: {} {}", cache_item.first.vector_index_name, cache_item.first.column_name, vec_index_desc.name, vec_index_desc.column);
            if (cache_item.first.vector_index_name == vec_index_desc.name && cache_item.first.column_name == vec_index_desc.column)
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
            LOG_DEBUG(log, "Find not existed cache, remove it: {}", cache_item.first.toString());
            VectorIndex::VectorSegmentExecutor::removeFromCache(cache_item.first);
            for (const auto& part : data.getDataPartsForInternalUsage())
            {
                part->removeVectorIndex(cache_item.first.vector_index_name, cache_item.first.column_name);
                part->vector_index_build_error = false;
            }
        }
    }
}

VectorIndexEntryPtr MergeTreeVectorIndexBuilderUpdater::selectPartsToBuildVectorIndex(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData::DataParts & currently_vector_indexing_parts,
    size_t background_vector_pool_size)
{
    if (metadata_snapshot->vec_indices.empty())
    {
        return {};
    }

    MergeTreeData::DataPartsVector parts;
    size_t min_rows_to_build_vector_index = data.getSettings()->min_rows_to_build_vector_index;
    for (const auto & part : data.getDataPartsForInternalUsage())
    {
        /// need to check currently_vector_indexing_parts.count(part) > 0
        if (currently_vector_indexing_parts.count(part) > 0 || part->vector_index_build_error)
        {
            continue;
        }

        for (const auto & vec_index : metadata_snapshot->vec_indices)
        {
            if (!part->containVectorIndex(vec_index.name, vec_index.column) && !part->isSmallPart(min_rows_to_build_vector_index))
            {
                parts.emplace_back(part);
                LOG_TRACE(log, "this part will get built index: {}", part->name);
                ///since each index building task is time-consuming, it's pointless to have a long list
                if (parts.size() >= background_vector_pool_size)
                {
                    return std::make_shared<VectorIndexEntry>(parts);
                }
                break;
            }
        }
    }

    if (parts.empty())
    {
        return {};
    }
    else
    {
        return std::make_shared<VectorIndexEntry>(parts);
    }
}

BuildVectorIndexStatus MergeTreeVectorIndexBuilderUpdater::buildVectorIndex(
    const StorageMetadataPtr & metadata_snapshot, const std::vector<MergeTreeDataPartPtr> & parts, bool tune)
{
    if (parts.empty())
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
    LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask start.");
    for (auto & part : parts)
    {
        if (part->vector_index_build_cancelled)
        {
            LOG_INFO(log, "[buildVectorIndex] part:{}, build index job has been cancelled.", part->name);
            continue;
        }

        constexpr int maxBuildRetryCount = 3;
        int failed_count = counter.get(part->getDataPartStorage().getRelativePath());
        if (failed_count >= maxBuildRetryCount)
        {
            part->setBuildError();
            return BuildVectorIndexStatus::BUILD_FAIL;
        }

        bool mem_limit_happened = false;

        BuildVectorIndexStatus status = BuildVectorIndexStatus::SUCCESS;
        try
        {
            LOG_DEBUG(log, "[buildVectorIndex] begin to build vector index of one part");
            status = buildVectorIndexForOnePart(metadata_snapshot, part, tune);
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
                ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);
            }
        } 
        else
        {
            if (VectorIndex::containRowIdsMaps(part->getDataPartStorage().getFullPath()))
            {
                auto lock = data.lockParts();
                LOG_INFO(log, "[buildVectorIndex] try to remove row ids maps files in {}", part->getDataPartStorage().getFullPath());
                /// currently only consider one vector index
                auto vec_index_desc = metadata_snapshot->vec_indices[0];
                auto old_segments = VectorIndex::getAllSegmentIds(part->getDataPartStorage().getFullPath(), part->name, vec_index_desc.name, vec_index_desc.column);
                for (auto& old_segment : old_segments)
                {
                    VectorIndex::VectorSegmentExecutor::removeFromCache(old_segment.getCacheKey());
                }
                VectorIndex::removeAllRowIdsMaps(part->getDataPartStorage().getFullPath());
            }
        }
    }

    watch.stop();
    LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask finished in {} sec.", watch.elapsedSeconds());

#ifdef build_fail_test
    LOG_INFO(log, "[buildVectorIndex] VectorIndexBuildTask increment VectorIndexBuildFailEvents.");
    ProfileEvents::increment(ProfileEvents::VectorIndexBuildFailEvents);
#endif
    // TODO: handle fail case
    return BuildVectorIndexStatus::SUCCESS;
}

BuildVectorIndexStatus MergeTreeVectorIndexBuilderUpdater::buildVectorIndexForOnePart(
    const StorageMetadataPtr & metadata_snapshot, const MergeTreeDataPartPtr & part, bool tune)
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
                            return BuildVectorIndexStatus::BUILD_FAIL;
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
        VectorIndex::Parameters parameters = VectorIndex::convertPocoJsonToMap(vec_index_desc.parameters);
        std::unordered_map<std::string, VectorIndex::Parameters> params_from_record;
        VectorIndex::DiskIOReader disk_reader;
        String read_file_path = part->getDataPartStorage().getFullPath() + "vector_index_ready";
        std::vector<String> index_names;
        std::string index_name = vec_index_desc.name + "_" + vec_index_desc.column;
        index_names.emplace_back(index_name);
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
                    LOG_INFO(log, "[buildVectorIndex] the index is built for part: {}", part->name);
                    part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
                    return BuildVectorIndexStatus::SUCCESS;
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
        Columns result;

        /// process data block by block
        while (num_rows_read < part->rows_count)
        {
            if (part->vector_index_build_cancelled)
            {
                break;
            }
            empty_ids.clear();
            size_t remaining_size = part->rows_count - num_rows_read;
            size_t max_read_row = std::min(remaining_size, read_block_rows_num);
            result.resize(cols.size());

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

            result.clear();

            /// only run in the first read round
            if (training)
            {
                LOG_INFO(
                    log,
                    "[buildVectorIndex] index train: part_name: {}, num_rows: {}, vector index name: {}, path: {}",
                    part->name,
                    num_rows,
                    vec_index_desc.name,
                    part->getDataPartStorage().getFullPath() + "/" + index_name);

                VectorIndex::SegmentId segment_id(part->getDataPartStorage().getFullPath(), part->name, part->name, vec_index_desc.name, vec_index_desc.column, 0);

                vec_index_builder = std::make_shared<VectorIndex::VectorSegmentExecutor>(
                    VectorIndex::VectorIndexFactory::createIndexType(vec_index_desc.type),
                    segment_id,
                    parameters,
                    dim);
                VectorIndex::Status build_status = vec_index_builder->buildIndex(vec_data, part->rows_count);

                if (build_status.getCode() == 11)
                {
                    part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
                    return BuildVectorIndexStatus::MISCONFIGURED;
                }

                if (!build_status.fine())
                {
                    part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
                    return BuildVectorIndexStatus::BUILD_FAIL;
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
                return BuildVectorIndexStatus::BUILD_FAIL;
            }
            LOG_INFO(log, "[buildVectorIndex] index cache: status: {}", seri_status.getCode());
            vec_index_builder->cache();
            LOG_INFO(log, "[buildVectorIndex] index after cache: status: {}", seri_status.getCode());
        }
        part->addVectorIndex(vec_index_desc.name + "_" + vec_index_desc.column);
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

}
