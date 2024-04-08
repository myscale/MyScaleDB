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

#include <VectorIndex/Common/VIMetadata.h>
#include <VectorIndex/Common/VIWithDataPart.h>
#include <VectorIndex/Storages/VIInfo.h>

namespace DB
{

VIInfo::VIInfo(
    const String & database_,
    const String & table_,
    const String & index_name,
    VectorIndex::SegmentId & segment_id,
    const VIState & state_,
    const String & err_msg_)
    : database(database_)
    , table(table_)
    , part(segment_id.current_part_name)
    , owner_part(segment_id.current_part_name == segment_id.owner_part_name ? "" : segment_id.owner_part_name)
    , owner_part_id(segment_id.owner_part_id)
    , name(index_name)
    , state(state_)
    , err_msg(err_msg_)
{
    try
    {
        VectorIndex::VIMetadata metadata(segment_id);
        auto buf = segment_id.getDisk()->readFile(segment_id.getVectorDescriptionFilePath());
        metadata.readText(*buf);

        type = Search::enumToString(metadata.type);

        dimension = metadata.dimension;
        total_vec = metadata.total_vec;

        if (metadata.infos.find(MEMORY_USAGE_BYTES) != metadata.infos.end())
            memory_usage_bytes = std::stol(metadata.infos[MEMORY_USAGE_BYTES]);

        if (metadata.infos.find(DISK_USAGE_BYTES) != metadata.infos.end())
            disk_usage_bytes = std::stol(metadata.infos[DISK_USAGE_BYTES]);

        vector_memory_size_metric.changeTo(memory_usage_bytes);
    }
    catch (...)
    {
        state = VIState::ERROR;
        err_msg = getCurrentExceptionMessage(false);
    }
}

VIInfo::VIInfo(
    const String & database_,
    const String & table_,
    const VIWithColumnInPart & column_index,
    const VIState & state_,
    const String & err_msg_)
    : database(database_)
    , table(table_)
    , part(column_index.current_part_name)
    , owner_part(column_index.current_part_name)
    , owner_part_id(0)
    , name(column_index.index_name)
    , type(Search::enumToString(column_index.index_type))
    , dimension(column_index.dimension)
    , total_vec(column_index.total_vec)
    , memory_usage_bytes(column_index.getMemoryUsage())
    , disk_usage_bytes(column_index.getDiskUsage())
    , state(state_)
    , err_msg(err_msg_)
{
    vector_memory_size_metric.changeTo(memory_usage_bytes);
}

String VIInfo::statusString() const
{
    return Search::enumToString(state);
}

}
