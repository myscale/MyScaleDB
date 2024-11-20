#include <VectorIndex/Storages/VITaskBase.h>

namespace DB
{
using namespace VectorIndex;
VITaskBase::State VITaskBase::getNextState()
{
    if (build_status.getStatus() == VectorIndex::SegmentBuiltStatus::SUCCESS && state != VITaskBase::State::NEED_FINALIZE)
        return static_cast<VITaskBase::State>(static_cast<int>(state) + 1);
    else if (build_status.getStatus() == VectorIndex::SegmentBuiltStatus::BUILD_RETRY)
        return state;
    else
        /// build error
        return VITaskBase::State::NEED_FINALIZE;
}

void VITaskBase::recordBuildStatus()
{
    /// Check vector index exists in table's latest metadata
    auto & latest_vec_indices = storage.getInMemoryMetadataPtr()->getVectorIndices();

    std::optional<VIDescription> vec_desc = std::nullopt;
    for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
        if (vec_index_desc.name == vector_index_name)
            vec_desc = vec_index_desc;

    if (!vec_desc.has_value() || !latest_vec_indices.has(vec_desc.value()))
        return;

    bool is_success
        = build_status.getStatus() == VectorIndex::SegmentBuiltStatus::SUCCESS || build_status.getStatus() == VectorIndex::SegmentBuiltStatus::BUILD_SKIPPED;
    if (ctx)
    {
        ctx->vi_status->setStatus(is_success ? SegmentStatus::BUILT : SegmentStatus::ERROR, build_status.err_msg);
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
            VIEventLogElement::Type event_type = build_status.getStatus() == SegmentBuiltStatus::BUILD_CANCEL
                ? VIEventLogElement::BUILD_CANCELD
                : VIEventLogElement::BUILD_ERROR;
            ctx->write_event_log(event_type, build_status.err_code, build_status.err_msg);
        }
    }
    else
    {
        MergeTreeDataPartPtr part = storage.getActiveContainingPart(part_name);
        auto vi_seg = part->segments_mgr->getSegment(vector_index_name);
        if (!part || !vi_seg)
            return;

        bool is_same = part->info.getPartNameWithoutMutation() == VectorIndex::cutMutVer(part_name);
        if (!is_same)
            return;
        vi_seg->getSegmentStatus()->setStatus(is_success ? VectorIndex::SegmentStatus::BUILT : VectorIndex::SegmentStatus::ERROR, build_status.err_msg);
    }
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
                build_status = SegmentBuiltStatus{SegmentBuiltStatus::BUILD_FAIL, e.code(), e.message()};
            }

            state = getNextState();
            return true;
        }
        case State::NEED_FINALIZE: {
            recordBuildStatus();
            remove_processed_entry();
            /// clean build temp folder
            if (ctx)
                ctx->clean_tmp_folder(ctx->vector_tmp_relative_path);

            return false;
        }
        case State::SUCCESS: {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Do not call execute on previously succeeded task");
        }
    }
}

StorageID VITaskBase::getStorageID() const
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
