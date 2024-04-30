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

#include <filesystem>
#include <memory>
#include <Disks/IDisk.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <VectorIndex/Common/VectorIndexCommon.h>
#include <VectorIndex/Common/SegmentId.h>
#include <base/types.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{
class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class DataPartStorageOnDiskBase;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace VectorIndex
{
/// Get the disk from data part storage to read/write vector index files.
void SegmentId::initDisk(DB::DataPartStoragePtr data_part_storage)
{
    const DB::DataPartStorageOnDiskBase * part_storage = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(data_part_storage.get());
    if (part_storage == nullptr)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
    }

    disk = getVolumeFromPartStorage(*part_storage)->getDisk();
}

String SegmentId::getPathPrefix() const
{
    /// normal vector index
    if (owner_part_name == current_part_name)
    {
        return data_part_path;
    }
    else
    {
        return data_part_path + "merged-" + DB::toString(owner_part_id) + "-" + owner_part_name + "-";
    }
}

std::tuple<std::shared_ptr<RowIds>, std::shared_ptr<RowIds>, std::shared_ptr<RowSource>> SegmentId::getMergedMaps()
{
    std::shared_ptr<RowIds> row_ids_map = std::make_shared<RowIds>(RowIds());
    std::shared_ptr<RowIds> inverted_row_ids_map = std::make_shared<RowIds>(RowIds());
    std::shared_ptr<RowSource> inverted_row_sources_map = std::make_shared<RowSource>(RowSource());
    /// not from merge or have already loaded related row ids maps
    if (!fromMergedParts())
        return std::make_tuple(row_ids_map, inverted_row_ids_map, inverted_row_sources_map);

    auto row_ids_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(getDisk()->readFile(getRowIdsMapFilePath()));
    auto inverted_row_ids_map_buf
        = std::make_unique<DB::CompressedReadBufferFromFile>(getDisk()->readFile(getInvertedRowIdsMapFilePath()));
    auto inverted_row_sources_map_buf
        = std::make_unique<DB::CompressedReadBufferFromFile>(getDisk()->readFile(getInvertedRowSourcesMapFilePath()));

    while (!inverted_row_sources_map_buf->eof())
    {
        uint8_t * row_source_pos = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->position());
        uint8_t * row_sources_end = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->buffer().end());

        while (row_source_pos < row_sources_end)
        {
            inverted_row_sources_map->push_back(*row_source_pos);
            ++row_source_pos;
        }

        inverted_row_sources_map_buf->position() = reinterpret_cast<char *>(row_source_pos);
    }

    UInt64 row_id;

    while (!row_ids_map_buf->eof())
    {
        DB::readIntText(row_id, *row_ids_map_buf);
        row_ids_map_buf->ignore();
        row_ids_map->push_back(row_id);
    }

    while (!inverted_row_ids_map_buf->eof())
    {
        DB::readIntText(row_id, *inverted_row_ids_map_buf);
        inverted_row_ids_map_buf->ignore();
        inverted_row_ids_map->push_back(row_id);
    }

    return std::make_tuple(
        row_ids_map, inverted_row_ids_map, inverted_row_sources_map);
}

}
