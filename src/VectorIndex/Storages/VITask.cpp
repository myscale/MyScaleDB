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

#include <VectorIndex/Storages/VITask.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int VECTOR_INDEX_ALREADY_EXISTS;
}

VIBuiltStatus VITask::prepare()
{
    try
    {
        ctx = builder.prepareBuildVIContext(
            metadata_snapshot, vector_index_entry->part_name, vector_index_entry->vector_index_name, slow_mode);

        return VIBuiltStatus{VIBuiltStatus::SUCCESS};
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("VITask"),
            "Prepare build vector index {} error {}: {}",
            vector_index_entry->part_name,
            e.code(),
            e.message());
        if (e.code() == ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART)
            return VIBuiltStatus{VIBuiltStatus::NO_DATA_PART, e.code(), e.message()};
        else if (e.code() == ErrorCodes::VECTOR_INDEX_ALREADY_EXISTS)
            return VIBuiltStatus{VIBuiltStatus::BUILD_SKIPPED};
        else
            return VIBuiltStatus{VIBuiltStatus::BUILD_FAIL, e.code(), e.message()};
    }
}

VITask::~VITask()
{
    LOG_DEBUG(log, "Destroy vector index job with vector index entry: {}", vector_index_entry->part_name);
}

}
