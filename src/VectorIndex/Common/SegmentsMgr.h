#pragma once

#include <shared_mutex>

#include <VectorIndex/Common/SegmentStatus.h>
#include <VectorIndex/Storages/VIDescriptions.h>
#include <Poco/Logger.h>
#include <Common/RWLock.h>

namespace DB
{
class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
}

namespace VectorIndex
{
using namespace DB;

class BaseSegment;
using SegmentPtr = std::shared_ptr<BaseSegment>;
class MergeIdMaps;
using MergeIdMapsPtr = std::shared_ptr<MergeIdMaps>;
class SegmentsMgr;
using VISegWithPartUniquePtr = std::unique_ptr<SegmentsMgr>;

class SegmentsMgr
{
public:
    SegmentsMgr(const IMergeTreeDataPart & data_part_) : data_part(data_part_) { }

    ~SegmentsMgr() = default;

    void addSegment(const VIDescription & vec_desc);
    void removeSegment(const String & vi_name);
    bool containDecoupleSegment() const;
    bool hasSegmentInReady() const;
    SegmentPtr getSegment(const String & vi_name) const;
    SegmentPtr getSegmentByColumn(const String & column_name) const;
    SegmentPtr updateSegment(const String & vi_name, const SegmentPtr & vi_segment);
    SegmentStatus::Status getSegmentStatus(const String & vi_name) const;
    void setSegmentStatus(const String & vi_name, SegmentStatus::Status status, const String & message = "");
    void initSegment();
    VISegWithPartUniquePtr mutation(const MergeTreeDataPartPtr & new_data_part, const NameSet & rebuild_index_column = {});
    void mutateFrom(const SegmentsMgr & from, const NameSet & rebuild_index_column = {});
    void cancelAllSegmentsActions();
    void removeSegMemoryResource();
    RWLockImpl::LockHolder tryLockSegmentsTimed(RWLockImpl::Type type, const std::chrono::milliseconds & acquire_timeout) const;

private:
    void initSegmentsImpl();
    /// acording to the local checksums file, validate the segments
    void validateSegmentsInLock(const std::unique_lock<std::shared_mutex> & lock);

private:
    std::once_flag init_flag;
    bool is_init = false;
    const IMergeTreeDataPart & data_part;
    mutable std::shared_mutex segments_mutex;
    std::unordered_map<String, SegmentPtr> segments;
    mutable RWLock move_lock = RWLockImpl::create();
    LoggerPtr log = getLogger("SegmentsMgr");
};


}
