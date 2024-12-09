#include "MergeIdMaps.h"

#include <base/types.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <VectorIndex/Utils/VIUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace VectorIndex
{


void MergeIdMaps::transferToNewRowIds(const UInt8 own_id, SearchResultPtr & result)
{
    /// search index result may be nullptr, no need to transfer
    if (!result)
        return;

    if (!initialized)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "MergeIdMaps is not initialized");

    if (!row_ids_maps.contains(own_id))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No row ids map for part id {}", std::to_string(own_id));

    const auto & row_ids = row_ids_maps.at(own_id);
    for (size_t k = 0; k < result->numQueries(); k++)
    {
        for (auto & label : result->getResultIndices(k))
            if (label != -1)
                label = (*row_ids)[label];
    }
}


void MergeIdMaps::transferToOldRowIds(const UInt8 own_id, SearchResultPtr & result)
{
    if (!result)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No search result");

    if (!initialized)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "MergeIdMaps is not initialized");

    if (!inverted_row_ids_map || inverted_row_ids_map->empty() || !inverted_row_sources_map || inverted_row_sources_map->empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No inverted row ids map");

    long inverted_size = static_cast<long>(inverted_row_sources_map->size());

    /// Transfer row IDs in the decoupled data part to real row IDs of the old data part.
    /// TODO: Not handle batch distance cases.
    auto new_distances = result->getResultDistances();
    auto new_ids = result->getResultIndices();

    std::vector<UInt64> real_row_ids;
    std::vector<Float32> distances;
    for (int i = 0; i < result->getNumCandidates(); i++)
    {
        auto new_row_id = new_ids[i];

        if (new_row_id == -1 || new_row_id >= inverted_size)
            continue;

        if (own_id == (*(inverted_row_sources_map))[new_row_id])
        {
            real_row_ids.emplace_back((*(inverted_row_ids_map))[new_row_id]);
            distances.emplace_back(new_distances[i]);
        }
    }

    if (real_row_ids.size() != 0)
    {
        /// Prepare search result for this old part
        size_t real_num_reorder = real_row_ids.size();
        SearchResultPtr real_search_result = SearchResult::createTopKHolder(result->numQueries(), real_num_reorder);

        auto per_ids = real_search_result->getResultIndices();
        auto per_distances = real_search_result->getResultDistances();

        for (size_t i = 0; i < real_num_reorder; i++)
        {
            per_ids[i] = real_row_ids[i];
            per_distances[i] = distances[i];
        }

        result = real_search_result;
    }
    else
    {
        result = nullptr;
    }
}

void MergeIdMaps::initRealFilter(const UInt8 own_id, VIBitmapPtr & filter, const RowIds del_rows)
{
    if (!initialized)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "MergeIdMaps is not initialized");

    if (!row_ids_maps.contains(own_id))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No row ids map for part id {}", std::to_string(own_id));
    for (auto & del_row : del_rows)
    {
        if (own_id == (*(inverted_row_sources_map))[del_row])
        {
            UInt64 real_row_id = (*(inverted_row_ids_map))[del_row];
            if (filter->is_member(real_row_id))
                filter->unset((*(inverted_row_ids_map))[del_row]);
        }
    }
}


const VIBitmapPtr MergeIdMaps::getRealFilter(const UInt8 own_id, const VIBitmapPtr & filter)
{
    if (!initialized)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "MergeIdMaps is not initialized");

    if (!filter)
        return nullptr;
    if (!row_ids_maps.contains(own_id))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No row ids map for part id {}", std::to_string(own_id));
    size_t old_part_rows_nums = row_ids_maps.at(own_id)->size();
    VIBitmapPtr real_filter = std::make_shared<VIBitmap>(old_part_rows_nums);
    for (auto & new_row_id : filter->to_vector())
    {
        if (own_id == (*(inverted_row_sources_map))[new_row_id])
        {
            real_filter->set((*(inverted_row_ids_map))[new_row_id]);
        }
    }
    return real_filter;
}

const std::unordered_map<UInt8, VIBitmapPtr> MergeIdMaps::getRealFilters(const VIBitmapPtr & filter)
{
    if (!initialized)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "MergeIdMaps is not initialized");
    
    if (!filter)
        return std::unordered_map<UInt8, VIBitmapPtr>();

    std::unordered_map<UInt8, VIBitmapPtr> real_filters;

    /// Need parallelize this loop?
    for (auto & new_row_id : filter->to_vector())
    {
        const UInt8 & own_id = (*(inverted_row_sources_map))[new_row_id];
        auto & real_filter = real_filters[own_id];
        if (real_filter)
        {
            real_filter->set((*(inverted_row_ids_map))[new_row_id]);
        }
        else
        {
            if (!row_ids_maps.contains(own_id))
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No row ids map for part id {}", std::to_string(own_id));

            size_t old_part_rows_nums = row_ids_maps.at(own_id)->size();
            real_filter = std::make_shared<VIBitmap>(old_part_rows_nums);
            real_filter->set((*(inverted_row_ids_map))[new_row_id]);
        }
    }

    return real_filters;
}

void MergeIdMaps::lazyInitOnce(const DB::IMergeTreeDataPart & data_part)
{
    std::call_once(init_flag, [&data_part, this] {
        MergeIdMapsPtr new_vi_merged_maps = loadFromDecouplePart(data_part);
        const_cast<RowIdsPtr &>(this->inverted_row_ids_map) = new_vi_merged_maps->inverted_row_ids_map;
        const_cast<RowSourcePtr &>(this->inverted_row_sources_map) = new_vi_merged_maps->inverted_row_sources_map;
        const_cast<std::unordered_map<UInt8, RowIdsPtr> &>(this->row_ids_maps) = new_vi_merged_maps->row_ids_maps;
        this->merged_maps_memory = std::move(new_vi_merged_maps->merged_maps_memory);
        this->initialized = true;
    });
}

MergeIdMapsPtr MergeIdMaps::loadFromDecouplePart(const DB::IMergeTreeDataPart & data_part)
{
    LOG_DEBUG(getLogger("MergedMaps"), "Load row ids map from decoupled part: {}", data_part.name);
    MergeIdMapsPtr merged_maps = std::make_shared<MergeIdMaps>();
    const_cast<RowIdsPtr &>(merged_maps->inverted_row_ids_map) = std::make_shared<RowIds>();
    const_cast<RowSourcePtr &>(merged_maps->inverted_row_sources_map) = std::make_shared<RowSource>();
    const String row_ids_map_file_suffix = String("-row_ids_map") + VECTOR_INDEX_FILE_SUFFIX;
    const String inverted_row_ids_map_file_suffix = String("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX;
    const String inverted_row_source_map_file_suffix = String("merged-inverted_row_sources_map") + VECTOR_INDEX_FILE_SUFFIX;
    long total_size = 0;

    auto inverted_row_ids_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(data_part.getDataPartStoragePtr()->readFile(inverted_row_ids_map_file_suffix, {}, {}, {}));
    auto inverted_row_sources_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(data_part.getDataPartStoragePtr()->readFile(inverted_row_source_map_file_suffix, {}, {}, {}));

    UInt64 row_id = 0;
    while (!inverted_row_ids_map_buf->eof())
    {
        DB::readIntText(row_id, *inverted_row_ids_map_buf);
        inverted_row_ids_map_buf->ignore();
        merged_maps->inverted_row_ids_map->push_back(row_id);
    }
    inverted_row_ids_map_buf.reset();
    merged_maps->inverted_row_ids_map->shrink_to_fit();
    total_size +=  merged_maps->inverted_row_ids_map->capacity() * sizeof(UInt64);

    while (!inverted_row_sources_map_buf->eof())
    {
        uint8_t * row_source_pos = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->position());
        uint8_t * row_sources_end = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->buffer().end());

        while (row_source_pos < row_sources_end)
        {
            /// in replacing merge, the same row may be in multiple parts,
            /// one of them is marked as MASK_FLAG, we need skip it
            if (*row_source_pos & DB::RowSourcePart::MASK_FLAG)
            {
                ++row_source_pos;
                continue;
            }

            merged_maps->inverted_row_sources_map->push_back(*row_source_pos);
            ++row_source_pos;
        }

        inverted_row_sources_map_buf->position() = reinterpret_cast<char *>(row_source_pos);
    }
    inverted_row_sources_map_buf.reset();
    merged_maps->inverted_row_sources_map->shrink_to_fit();
    total_size +=  merged_maps->inverted_row_sources_map->capacity() * sizeof(uint8_t);
    for (const auto & it = data_part.getDataPartStoragePtr()->iterate(); it->isValid(); it->next())
    {
        const String & file_name = it->name();
        if (!endsWith(file_name, row_ids_map_file_suffix))
            continue;
        /// row ids map file name: merged-<part_id>-<part_name>-row_ids_map.vidx
        auto tokens = splitString(file_name, '-');
        if (tokens.size() != 4)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid row ids map file name: {}", file_name);

        /// init row ids map
        {
            if (std::stoul(tokens[1]) > DB::RowSourcePart::MAX_PARTS)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid part {}, own part id {}", tokens[2], tokens[1]);
            const UInt8 part_id = static_cast<UInt8>(std::stoul(tokens[1]));
            auto row_ids_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(data_part.getDataPartStoragePtr()->readFile(file_name, {}, {}, {}));
            RowIdsPtr & part_row_ids = const_cast<std::unordered_map<UInt8, RowIdsPtr> &>(merged_maps->row_ids_maps)[part_id];
            part_row_ids = std::make_shared<RowIds>();
            while (!row_ids_map_buf->eof())
            {
                DB::readIntText(row_id, *row_ids_map_buf);
                row_ids_map_buf->ignore();
                part_row_ids->push_back(row_id);
            }
            row_ids_map_buf.reset();
            part_row_ids->shrink_to_fit();
            total_size += part_row_ids->capacity() * sizeof(UInt64);
        }
    }
    merged_maps->merged_maps_memory = CurrentMetrics::Increment(CurrentMetrics::LoadedVectorIndexMemorySize, total_size);
    std::call_once(merged_maps->init_flag, [&] {
        merged_maps->initialized = true;
    }); /// init flag
    return merged_maps;
}

void MergeIdMaps::removeMergedMapsFiles(const DB::IMergeTreeDataPart & data_part)
{
    const String row_ids_map_file_suffix = String("-row_ids_map") + VECTOR_INDEX_FILE_SUFFIX;
    const String inverted_row_ids_map_file_suffix = String("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX;
    const String inverted_row_source_map_file_suffix = String("merged-inverted_row_sources_map") + VECTOR_INDEX_FILE_SUFFIX;
    for (const auto & it = data_part.getDataPartStoragePtr()->iterate(); it->isValid(); it->next())
    {
        const String & file_name = it->name();
        if (endsWith(file_name, row_ids_map_file_suffix)
            || endsWith(file_name, inverted_row_ids_map_file_suffix)
            || endsWith(file_name, inverted_row_source_map_file_suffix))
        {
            LOG_DEBUG(getLogger("MergedMaps"), "Remove vector index file: {}", file_name);
            const_cast<DB::IDataPartStorage &>(data_part.getDataPartStorage()).removeFile(file_name);
        }
    }
}
}
