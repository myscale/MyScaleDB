#include <Common/ErrorCodes.h>
#include <Common/callOnce.h>

#include <Storages/StorageReplicatedMergeTree.h>
#include <VectorIndex/Common/VectorIndexObject.h>
#include <VectorIndex/Common/StorageVectorIndicesMgr.h>
#include <VectorIndex/Common/SegmentsMgr.h>
#include <VectorIndex/Common/Segment.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_VECTOR_INDEX;
}

const UInt64 WAIT_VECTOR_INDEX_BUILD_TIMEOUT_SEC = 300;
void VectorIndexObject::init() const
{
    auto init_func = [&] {
        for (const auto & part : storage.getDataPartsForInternalUsage())
            part->segments_mgr->addSegment(vec_desc);
        if (is_replica)
        {
            auto & replica_storage = dynamic_cast<StorageReplicatedMergeTree &>(storage);
            if (replica_storage.getSettings()->build_vector_index_on_random_single_replica)
            {
                /// Check if vidx_build_parts exists in zookeer.
                auto zookeeper = replica_storage.getZooKeeper();

                String zookeeper_build_status_path = fs::path(replica_storage.replica_path) / "vidx_build_parts" / vec_desc.name;
                zookeeper->createAncestors(zookeeper_build_status_path);
                auto code = zookeeper->tryCreate(zookeeper_build_status_path, "", zkutil::CreateMode::Persistent);

                if (code == Coordination::Error::ZNODEEXISTS)
                { /// The table has been dropped vector index early.
                    LOG_DEBUG(
                        storage.log,
                        "Build vector index status for index {} on path {} has already been created",
                        vec_desc.name,
                        zookeeper_build_status_path);
                }
                else if (code != Coordination::Error::ZOK)
                {
                    throw zkutil::KeeperException::fromPath(code, zookeeper_build_status_path);
                }
            }
        }
    };

    callOnce(init_flag, init_func);
}

void VectorIndexObject::drop()
{
    LOG_DEBUG(storage.log, "Drop VectorIndexObject, clear all segments");
    is_dropped.store(true);
    for (const auto & part : storage.getDataPartsForInternalUsage())
        part->segments_mgr->removeSegment(vec_desc.name);
    if (is_replica)
    {
        auto & replica_storage = dynamic_cast<StorageReplicatedMergeTree &>(storage);
        if (replica_storage.getSettings()->build_vector_index_on_random_single_replica)
        {
            /// Remove vector index build status for parts from zookeeper
            auto zookeeper = replica_storage.getZooKeeper();

            auto index_build_status_path = fs::path(replica_storage.replica_path) / "vidx_build_parts" / vec_desc.name;

            /// Clean up vidx_build_parts/<index_name>/<part_name> nodes
            bool removed_quickly = zookeeper->tryRemoveChildrenRecursive(index_build_status_path, /* probably flat */ true);
            if (!removed_quickly)
            {
                LOG_WARNING(
                    storage.log,
                    "Failed to quickly remove node 'vidx_build_parts/{}' and its children, fell back to recursive removal",
                    vec_desc.name);
                zookeeper->tryRemoveChildrenRecursive(index_build_status_path);
            }

            /// Remove vidx_build_parts/<index_name>
            zookeeper->tryRemove(index_build_status_path);

            LOG_DEBUG(storage.log, "Cleaned up vector index `{}` build status from zookeeper", vec_desc.name);
        }
        replica_storage.vi_manager->writeVectorIndexInfoToZookeeper(true);
    }
}

void VectorIndexObject::waitBuildFinish()
{
    Stopwatch watch;
    watch.start();
    bool all_built = false;
    while (watch.elapsedSeconds() < WAIT_VECTOR_INDEX_BUILD_TIMEOUT_SEC && !all_built && !is_dropped.load())
    {
        const auto data_parts = storage.getDataPartsVectorForInternalUsage();
        if (data_parts.empty())
            return;
        for (const auto & part : data_parts)
        {
            if (is_dropped.load())
                return;
            auto seg = part->segments_mgr->getSegment(vec_desc.name);
            if (seg)
            {
                auto seg_status = seg->getSegmentStatus()->getStatus();
                if (seg_status >= VectorIndex::SegmentStatus::ERROR)
                {
                    throw Exception(
                        ErrorCodes::INVALID_VECTOR_INDEX,
                        "Vector index build failed for part {}. Reason: {}",
                        part->name,
                        seg->getSegmentStatus()->getErrMsg());
                }
                else if (seg->canBuildIndex() || seg_status == VectorIndex::SegmentStatus::BUILDING)
                {
                    /// pending for build or currently building
                    all_built = false;
                    break;
                }
                else
                {
                    /// all built or decouple segment no need to build
                    all_built = true;
                }
            }
        }
        if (!all_built)
            std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}


VectorIndexObject::MergeTreeVectorIndexStatus VectorIndexObject::getVectorIndexBuildStatus() const
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot->getVectorIndices().has(vec_desc.name))
        return {};
    for (const auto & part : storage.getDataPartsVectorForInternalUsage())
    {
        auto seg = part->segments_mgr->getSegment(vec_desc.name);
        if (seg)
        {
            if (seg->getSegmentStatus()->getStatus() >= VectorIndex::SegmentStatus::ERROR)
            {
                /// record error status
                MergeTreeVectorIndexStatus status;
                status.latest_failed_part = part->name;
                status.latest_failed_part_info = part->info;
                status.latest_fail_reason = seg->getSegmentStatus()->getErrMsg();
                return status;
            }
        }
    }
    /// no error status found
    return {};

}

} // namespace DB
