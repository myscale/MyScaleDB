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

#include <VectorIndex/Storages/VITaskBase.h>

namespace DB
{
VITaskBase::State VITaskBase::getNextState()
{
    if (build_status.getStatus() == VIBuiltStatus::SUCCESS && state != VITaskBase::State::NEED_FINALIZE)
        return static_cast<VITaskBase::State>(static_cast<int>(state) + 1);
    else if (build_status.getStatus() == VIBuiltStatus::BUILD_RETRY)
        return state;
    else
        /// build error
        return VITaskBase::State::NEED_FINALIZE;
}

void VITaskBase::recordBuildStatus()
{
    /// Check vector index exists in table's latest metadata
    auto & latest_vec_indices = storage.getInMemoryMetadataPtr()->getVectorIndices();

    VIDescription vec_desc;
    for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
        if (vec_index_desc.name == vector_index_name)
            vec_desc = vec_index_desc;

    bool is_success
        = build_status.getStatus() == VIBuiltStatus::SUCCESS || build_status.getStatus() == VIBuiltStatus::BUILD_SKIPPED;

    if (!latest_vec_indices.has(vec_desc))
        return;

    bool record_build_status = true;
    if (ctx)
    {
        if (ctx->source_column_index->isShutdown() && build_status.getStatus() != VIBuiltStatus::BUILD_CANCEL)
            record_build_status = false;
        ctx->source_column_index->onBuildFinish(is_success, build_status.err_msg);
        if (is_success)
        {
            LOG_INFO(
                ctx->log,
                "Vector index build task for part {} index name {} finished in {} sec, slow_mode: {}",
                ctx->source_part->name,
                ctx->vector_index_name,
                ctx->watch.elapsedSeconds(),
                ctx->slow_mode);

            ctx->write_event_log(VIEventLogElement::BUILD_SUCCEED, 0, "");
            state = State::SUCCESS;
        }
        else
        {
            LOG_ERROR(
                ctx->log,
                "Vector index build task for part {} index name {} failed, fail reason: {}, {}",
                ctx->source_part->name,
                ctx->vector_index_name,
                build_status.statusToString(),
                build_status.err_msg);
            VIEventLogElement::Type event_type = build_status.getStatus() == VIBuiltStatus::BUILD_CANCEL
                ? VIEventLogElement::BUILD_CANCELD
                : VIEventLogElement::BUILD_ERROR;
            ctx->write_event_log(event_type, build_status.err_code, build_status.err_msg);
        }
    }
    else
    {
        MergeTreeDataPartPtr part = storage.getActiveContainingPart(part_name);
        if (!part || !part->vector_index.getColumnIndex(vector_index_name).has_value())
            return;

        bool is_same = part->info.getPartNameWithoutMutation() == VectorIndex::cutMutVer(part_name);
        if (!is_same)
            return;

        auto source_column_index = part->vector_index.getColumnIndex(vector_index_name).value();
        source_column_index->onBuildFinish(is_success, build_status.err_msg);
    }

    if (record_build_status)
        storage.updateVectorIndexBuildStatus(part_name, vector_index_name, is_success, build_status.err_msg);
}

bool VITaskBase::executeStep()
{
    switch (state)
    {
        case State::NEED_PREPARE: {
            build_status = prepare();

            state = getNextState();
            return true;
        }
        case State::NEED_EXECUTE_BUILD_VECTOR_INDEX: {
            build_status = builder.buildVI(ctx);

            state = getNextState();
            return true;
        }
        case State::NEED_MOVE_VECTOR_INDEX: {
            try
            {
                build_status = builder.TryMoveVIFiles(ctx);
            }
            catch (Exception & e)
            {
                LOG_WARNING(
                    ctx->log,
                    "Move Index {} to part {} Error {}: {}",
                    ctx->vector_index_name,
                    ctx->source_part->name,
                    e.code(),
                    e.message());
                build_status = VIBuiltStatus{VIBuiltStatus::BUILD_FAIL, e.code(), e.message()};
            }

            state = getNextState();
            return true;
        }
        case State::NEED_FINALIZE: {
            recordBuildStatus();
            remove_processed_entry();
            /// clean build temp folder
            if (ctx)
                ctx->clean_tmp_folder_callback(ctx->vector_tmp_relative_path);

            return false;
        }
        case State::SUCCESS: {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Do not call execute on previously succeeded task");
        }
    }
}

StorageID VITaskBase::getStorageID()
{
    return storage.getStorageID();
}

void VITaskBase::onCompleted()
{
    bool delay = state == State::SUCCESS;
    if (delay)
        LOG_DEBUG(log, "On complete: {}", part_name);

    task_result_callback(delay);
}

VITaskBase::~VITaskBase()
{
}

}
