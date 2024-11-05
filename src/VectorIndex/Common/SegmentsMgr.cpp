#include "SegmentsMgr.h"
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <VectorIndex/Common/Segment.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DEADLOCK_AVOIDED;
}
}

namespace VectorIndex
{
void SegmentsMgr::addSegment(const VIDescription & vec_desc)
{
    std::unique_lock lock(segments_mutex);
    if (segments.find(vec_desc.name) != segments.end())
    {
        /// check if the segment is already exist
        if (segments[vec_desc.name]->getVIDescription() == vec_desc)
        {
            LOG_DEBUG(log, "Segment {} already exist, skip add segment", vec_desc.name);
            return;
        }
        else
        {
            auto expired_seg = segments[vec_desc.name];
            segments.erase(vec_desc.name);
            expired_seg->setVIExpiredFlag(VIExpireFlag::VI_FILES_EXPIRED | VIExpireFlag::VI_CHECKSUM_EXPIRED);
        }
    }
    segments[vec_desc.name] = createSegment(vec_desc, data_part.shared_from_this());
}

void SegmentsMgr::removeSegment(const String & vi_name)
{
    std::unique_lock lock(segments_mutex);
    auto it = segments.find(vi_name);
    if (it != segments.end())
    {
        auto expired_seg = it->second;
        segments.erase(it);
        expired_seg->setVIExpiredFlag(VIExpireFlag::VI_FILES_EXPIRED | VIExpireFlag::VI_CHECKSUM_EXPIRED);
    }
}

bool SegmentsMgr::containDecoupleSegment() const
{
    std::shared_lock lock(segments_mutex);
    for (const auto & [_, segment] : segments)
    {
        if (segment->isDecoupled())
            return true;
    }
    return false;
}

bool SegmentsMgr::hasSegmentInReady() const
{
    std::shared_lock lock(segments_mutex);
    for (const auto & [_, segment] : segments)
    {
        if (segment->isDecoupled() || segment->getSegmentStatus()->getStatus() == SegmentStatus::BUILT)
            return true;
    }
    return false;
}


SegmentPtr SegmentsMgr::getSegment(const String & vi_name) const
{
    std::shared_lock lock(segments_mutex);
    if (segments.find(vi_name) == segments.end())
        return nullptr;
    return segments.find(vi_name)->second;
}

SegmentPtr SegmentsMgr::updateSegment(const String & vi_name, const SegmentPtr & vi_segment)
{
    SegmentPtr old_seg = getSegment(vi_name);

    std::unique_lock lock(segments_mutex);
    segments[vi_name] = vi_segment;
    old_seg->setVIExpiredFlag(VIExpireFlag::VI_FILES_EXPIRED);
    return old_seg;
}


SegmentPtr SegmentsMgr::getSegmentByColumn(const String & column_name) const
{
    std::shared_lock lock(segments_mutex);
    for (auto & [_, segment] : segments)
    {
        if (segment->getVIColumnName() == column_name)
            return segment;
    }
    return nullptr;
}

SegmentStatus::Status SegmentsMgr::getSegmentStatus(const String & vi_name) const
{
    auto vi_seg = getSegment(vi_name);
    if (vi_seg)
        return vi_seg->getSegmentStatus()->getStatus();
    else
        return SegmentStatus::UNKNOWN;
}

void SegmentsMgr::setSegmentStatus(const String & vi_name, SegmentStatus::Status status, const String & message)
{
    auto vi_seg = getSegment(vi_name);
    if (vi_seg)
        vi_seg->vi_status->setStatus(status, message);
}

VISegWithPartUniquePtr SegmentsMgr::mutation(const MergeTreeDataPartPtr & new_data_part, const NameSet & rebuild_index_column)
{
    VISegWithPartUniquePtr new_vi_segments = std::make_unique<SegmentsMgr>(*new_data_part);
    std::unique_lock lock(segments_mutex);
    for (const auto & [vi_name, segment] : segments)
    {
        if (rebuild_index_column.contains(segment->getVIColumnName()))
        {
            /// [TODO] need remove old segmnets files?
            SegmentPtr new_seg = createSegment(segment->getVIDescription(), new_data_part);
            new_vi_segments->segments[vi_name] = new_seg;
        }
        else
        {
            SegmentPtr new_seg = segment->mutation(new_data_part);
            new_vi_segments->segments[vi_name] = new_seg;
        }
    }
    this->validateSegmentsInLock(lock);
    new_vi_segments->is_init = true;
    return new_vi_segments;
}

void SegmentsMgr::mutateFrom(const SegmentsMgr & from, const NameSet & rebuild_index_column)
{
    std::unique_lock lock(from.segments_mutex);
    for (const auto & [vi_name, segment] : from.segments)
    {
        if (segments.find(vi_name) != segments.end() && !rebuild_index_column.contains(segment->getVIColumnName()))
        {
            segments[vi_name]->inheritMetadata(segment);
        }
        else
        {
            /// [TODO] need remove old segmnets files?
            segments[vi_name] = createSegment(segment->getVIDescription(), data_part.shared_from_this());
        }
    }
    this->validateSegmentsInLock(lock);
    this->is_init = true;
}

void SegmentsMgr::cancelAllSegmentsActions()
{
    std::shared_lock lock(segments_mutex);
    for (const auto & [_, segment] : segments)
        segment->vi_status->setStatus(SegmentStatus::CANCELLED);
}

void SegmentsMgr::removeSegMemoryResource()
{
    std::shared_lock lock(segments_mutex);
    for (const auto & [_, segment] : segments)
        segment->removeMemoryRecords();
}

void SegmentsMgr::initSegment()
{
    this->initSegmentsImpl();
}

void SegmentsMgr::initSegmentsImpl()
{
    if (is_init)
        return;

    auto metadata_snapshot = data_part.storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasVectorIndices())
        return;

    try
    {
        convertIndexFileForUpgrade(data_part);
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to convert vector index files for part {}: {}", data_part.name, getCurrentExceptionMessage(false));
    }

    /// upgrade vi index file
    const IDataPartStorage & part_storage = data_part.getDataPartStorage();
    MergeIdMapsPtr merged_maps = nullptr;
    std::unique_lock lock(segments_mutex);
    for (const auto & vec_index_desc : metadata_snapshot->getVectorIndices())
    {
        const auto & vector_index_name = vec_index_desc.name;
        String checksums_filename = getVectorIndexChecksumsFileName(vector_index_name);
        bool is_decoupled = false;
        bool has_ready_vi = false;
        if (part_storage.exists(checksums_filename))
        {
            std::shared_ptr<MergeTreeDataPartChecksums> vector_index_checksums = std::make_shared<MergeTreeDataPartChecksums>();

            try
            {
                auto buf = part_storage.readFile(checksums_filename, {}, std::nullopt, std::nullopt);
                // LOG_INFO(log, "Reading vector index {} checksums for part {}", vector_index_name, data_part.name);
                if (vector_index_checksums->read(*buf))
                    assertEOF(*buf);
                if (vector_index_checksums->empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Vector index {} Checksums file is empty", vector_index_name);

                if (!checkConsistencyForVectorIndex(data_part.getDataPartStoragePtr(), *vector_index_checksums))
                {
                    /// remove checksums file
                    const_cast<IDataPartStorage &>(part_storage).removeFileIfExists(checksums_filename);
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Vector index {} checksums file is corrupted", vector_index_name);
                }

                for (const auto & [file, _] : vector_index_checksums->files)
                {
                    if (startsWith(file, "merged"))
                    {
                        is_decoupled = true;
                        break;
                    }
                }

                SegmentPtr segment;
                if (is_decoupled && !merged_maps)
                    merged_maps = std::make_shared<MergeIdMaps>();
                if (!is_decoupled)
                    segment = createSegment(vec_index_desc, data_part.shared_from_this(), vector_index_checksums);
                else
                    segment = createVIDecoupleSegment(vec_index_desc, merged_maps, data_part.shared_from_this(), vector_index_checksums);

                segments[vector_index_name] = segment;
                has_ready_vi = true;
            }
            catch (...)
            {
                LOG_WARNING(
                    log,
                    "An error occurred while checking vector index {} files consistency for part {}: {}",
                    vector_index_name,
                    data_part.name,
                    getCurrentExceptionMessage(false));
            }
        }
        if (!has_ready_vi)
            segments[vector_index_name] = createSegment(vec_index_desc, data_part.shared_from_this());
    }
    validateSegmentsInLock(lock);
    is_init = true;
}

void SegmentsMgr::validateSegmentsInLock(const std::unique_lock<std::shared_mutex> & /*lock*/)
{
    /// remove invalid vi files according to the checksums file
    removeIncompleteMovedVectorIndexFiles(data_part);
    
    for (auto & [vi_name, segment] : segments)
    {
        if (segment->validateSegments())
            continue;
        LOG_WARNING(log, "Vector index {} files are corrupted, remove all files", vi_name);
        segment->removeVIFiles(true);
        segment->removeCache();
        segments.erase(vi_name);
        segments[vi_name] = createSegment(segment->getVIDescription(), data_part.shared_from_this());
    }
}


RWLockImpl::LockHolder SegmentsMgr::tryLockSegmentsTimed(RWLockImpl::Type type, const std::chrono::milliseconds & acquire_timeout) const
{
    auto lock_holder = move_lock->getLock(type, "", acquire_timeout);
    if (!lock_holder)
    {
        const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
        throw Exception(
            ErrorCodes::DEADLOCK_AVOIDED,
            "{} locking attempt on {}'s vector index has timed out! ({}ms) Possible deadlock avoided.",
            type_str,
            data_part.name,
            acquire_timeout.count());
    }
    return lock_holder;
}


}
