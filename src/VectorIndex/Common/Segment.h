#pragma once

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Daemon/BaseDaemon.h>

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Utils/MergeIdMaps.h>

#include <VectorIndex/Common/VIBuildMemoryUsageHelper.h>
#include <VectorIndex/Common/VICommon.h>

#include <VectorIndex/Common/ScanThreadLimiter.h>
#include <VectorIndex/Common/VIPartReader.h>
#include <VectorIndex/Common/VectorDataset.h>
#include <VectorIndex/Common/VectorIndexIO.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <VectorIndex/Storages/VIDescriptions.h>
#include <VectorIndex/Common/SegmentInfo.h>
#include <VectorIndex/Utils/VIUtils.h>
#include <Common/CurrentMetrics.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int VECTOR_INDEX_CACHE_LOAD_ERROR;
    extern const int INVALID_VECTOR_INDEX;
}

class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeDataPartWeakPtr = std::weak_ptr<const IMergeTreeDataPart>;
class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;
using MergeTreeDataPartChecksumsPtr = std::shared_ptr<MergeTreeDataPartChecksums>;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
}

namespace CurrentMetrics
{
extern const Metric AllVectorIndexMemorySize;
}

namespace VectorIndex
{
using namespace DB;
struct SegmentStatus;
using SegmentStatusPtr = std::shared_ptr<SegmentStatus>;
class BaseSegment;
using SegmentPtr = std::shared_ptr<BaseSegment>;
struct CachedSegment;
using CachedSegmentPtr = std::shared_ptr<CachedSegment>;
class SegmentsMgr;
struct CachedSegmentKey;
using CachedSegmentKeyList = std::vector<CachedSegmentKey>;
using CachedSegmentholderList = std::vector<CachedSegmentHolderPtr>;

/// design for lazey remove segment cache and files, similar to MergeTreeDataPart destory
struct VIExpireFlag
{
    UInt8 data = 0;

    VIExpireFlag() = default;
    void setVIExpiredFlag(UInt8 flag) { data |= flag; }
    bool isVIFIleExpired() const { return (data & VI_FILES_EXPIRED) != 0; }
    bool isChecksumExpired() const { return (data & VI_CHECKSUM_EXPIRED) != 0; }
    bool isMergedMapsExpired() const { return (data & VI_MERGED_MAPS_EXPIRED) != 0; }

    static constexpr UInt8 VI_FILES_EXPIRED = 0x01;
    static constexpr UInt8 VI_CHECKSUM_EXPIRED = 0x02;
    static constexpr UInt8 VI_MERGED_MAPS_EXPIRED = 0x04;
};

/// The BaseSegment class serves as a foundational class for managing vector index segments.
/// It provides essential functionalities for building, serializing, searching, and managing vector indexes.
/// The class defines a set of pure virtual functions that outline the core operations of
/// a vector index segment, allowing derived classes to implement specific behaviors.
class BaseSegment
{
public:
    using BasePtr = std::shared_ptr<BaseSegment>;
    friend class SegmentsMgr;
    friend class VectorIndicesMgr;

    /// Constructor for BaseSegment，
    /// nitializes a vector index segment with the given description and data part.
    BaseSegment(
        const VIDescription & vec_desc_,
        const MergeTreeDataPartWeakPtr part,
        const MergeTreeDataPartChecksumsPtr & vi_checksums_ = nullptr,
        const String & owner_part_name_ = "",
        UInt8 owner_part_id_ = 0);

    virtual ~BaseSegment()
    {
        if (flag_vi_expired.isVIFIleExpired())
        {
            removeCache();
            removeVIFiles(flag_vi_expired.isChecksumExpired());
        }
    }

    /// The Metadata struct stores metadata information for a vector index segment.
    struct Metadata
    {
        VIType index_type; /// The type of vector index.
        VIMetric index_metric; /// The metric used to measure the distance between vectors.
        size_t dimension; /// The dimension of the vectors.
        size_t total_vec; /// The total number of vectors in the segment.
        bool fallback_to_flat = false; /// A flag indicating whether to fall back to a flat index.
        VIParameter build_params; /// The parameters used to build the vector index.
        std::unordered_map<std::string, std::string> properties; /// Additional properties of the vector index.
        String index_version; /// The version of the vector index.

        static Metadata readFromBuffer(ReadBuffer & buf);
        static void writeToBuffer(const Metadata & meta, WriteBuffer & buf);
    };

    String getSegmentPathPrefix() const
    {
        auto lock_part = getDataPart();
        String full_path = lock_part->getDataPartStorage().getFullPath();
        if (owner_part_name.empty())
            return full_path;
        else
            return full_path + "merged-" + DB::toString(owner_part_id) + "-" + owner_part_name + "-";
    }

    String getSegmentFullPath() const { return getSegmentPathPrefix() + vec_desc.name + "-"; }

    String getVectorDescriptionFilePath() const { return getSegmentPathPrefix() + getVectorIndexDescriptionFileName(vec_desc.name); }

    virtual CachedSegmentPtr
    buildVI(bool slow_mode, VIBuildMemoryUsageHelper & build_memory_lock, std::function<bool()> cancel_build_callback = {})
        = 0;

    virtual void serialize(
        DiskPtr disk,
        const CachedSegmentPtr vi_entry,
        const String & serialize_local_folder,
        MergeTreeDataPartChecksumsPtr & vector_index_checksum,
        VIBuildMemoryUsageHelper & build_memory_lock)
        = 0;

    virtual SearchResultPtr searchVI(
        const VectorDatasetVariantPtr & queries,
        int32_t k,
        const VIBitmapPtr filter,
        const VIParameter & parameters,
        bool & first_stage_only,
        const MergeIdMapsPtr & vi_merged_maps_ = nullptr)
        = 0;

    virtual SearchResultPtr computeTopDistanceSubset(
        VectorDatasetVariantPtr queries,
        SearchResultPtr first_stage_result,
        int32_t top_k,
        const MergeIdMapsPtr & vi_merged_maps_ = nullptr)
        = 0;

    virtual const CachedSegmentKeyList getCachedSegmentKeys() const;

    const CachedSegmentholderList getCachedSegmentHolders() const;

    virtual CachedSegmentHolderPtr loadVI(const MergeIdMapsPtr & vi_merged_maps = nullptr) = 0;

    virtual bool isDecoupled() const { return false; }

    virtual SegmentInfoPtrList getSegmentInfoList() const = 0;

    virtual void updateCachedBitMap(const VIBitmapPtr & bitmap) = 0;

    virtual bool supportTwoStageSearch() const { return false; }

    virtual void inheritMetadata(const SegmentPtr & old_seg);
    virtual SegmentPtr mutation(const MergeTreeDataPartPtr new_data_part);

    virtual void removeMemoryRecords() { vi_memory_metric_increment.changeTo(0); }

    virtual void setVIExpiredFlag(UInt8 flag)
    {
        /// only set flag for built segment, otherwise,
        /// we will destory cache and files which are belong to other mutations part segments
        if (vi_status->getStatus() == SegmentStatus::BUILT)
        {
            /// only remove cache, vi files when vi is built
            flag_vi_expired.setVIExpiredFlag(flag);
        }
    }

    bool validateSegments() const;

    MergeTreeDataPartPtr getDataPart() const
    {
        auto lock_part = data_part.lock();
        if (!lock_part)
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Data part is expired.");
        return lock_part;
    }

    void removeVIFiles(bool remove_checksums_file = false);

    bool containCachedSegmentKey(const CachedSegmentKey & cache_key) const;

    bool cache(CachedSegmentPtr vi_entry)
    {
        if (vi_entry == nullptr)
            return false;
        auto write_event_log = [&](VIEventLogElement::Type event, int code = 0, String msg = "")
        {
            VIEventLog::addEventLog(
                Context::getGlobalContextInstance(), getDataPart(), vec_desc.name, event, getOwnPartName(), ExecutionStatus(code, msg));
        };
        write_event_log(VIEventLogElement::LOAD_START);
        auto cache_keys = getCachedSegmentKeys();
        if (cache_keys.empty())
            throw VIException(ErrorCodes::LOGICAL_ERROR, "No cache key for segment, it is a bug.");

        bool success = VICacheManager::getInstance()->load(cache_keys.front(), [vi_entry]() { return vi_entry; }) != nullptr;
        write_event_log(success ? VIEventLogElement::LOAD_SUCCEED : VIEventLogElement::LOAD_FAILED);
        return success;
    }

    bool inCache() const
    {
        for (const auto & cache_key : getCachedSegmentKeys())
        {
            if (VICacheManager::getInstance()->get(cache_key) != nullptr)
                return true;
        }
        return false;
    }

    void removeCache()
    {
        for (const auto & cache_key : getCachedSegmentKeys())
            VICacheManager::removeFromCache(cache_key);
    }

    bool canBuildIndex() const
    {
        /// only build index when vi is pending and if decoupled, rebuild for decouple is enabled
        return vi_status->getStatus() == SegmentStatus::PENDING
            && (!isDecoupled() || getDataPart()->storage.getSettings()->enable_rebuild_for_decouple);
    }

    const MergeTreeDataPartChecksumsPtr getVIChecksum() const { return vi_checksum; }

    String getVIColumnName() const { return vec_desc.column; }

    static Metadata generateVIMetadata(const IMergeTreeDataPart & data_part, const VIDescription & vec_desc);

    VIDescription getVIDescription() const { return vec_desc; }

    String getOwnPartName() const;

    UInt8 getOwnerPartId() const { return owner_part_id; }

    void updateSegmentStatus(SegmentStatusPtr status) { vi_status = status; }

    SegmentStatusPtr getSegmentStatus() const { return vi_status; }

    static bool canMergeForSegs(const SegmentPtr & left_seg, const SegmentPtr & right_seg);

    DiskPtr getDiskFromLockPart() const
    {
        auto lock_part = getDataPart();
        /// checkout bad cast error
        const DataPartStorageOnDiskBase & part_storage = dynamic_cast<const DataPartStorageOnDiskBase &>(lock_part->getDataPartStorage());
        DiskPtr disk = getVolumeFromPartStorage(part_storage)->getDisk();
        return disk;
    }

public:
    static UInt8 max_threads;
    LoggerPtr log = getLogger("BaseSegment");

protected:
    const VIDescription vec_desc;
    const MergeTreeDataPartWeakPtr data_part;
    SegmentStatusPtr vi_status;
    const String owner_part_name;
    const UInt8 owner_part_id;
    Metadata vi_metadata;
    MergeTreeDataPartChecksumsPtr vi_checksum{nullptr};
    CurrentMetrics::Increment vi_memory_metric_increment{CurrentMetrics::AllVectorIndexMemorySize, 0};
    VIExpireFlag flag_vi_expired;
};

/// The SimpleSegment class is a specialized implementation of the BaseSegment class.
/// The class is templated to support different data types, such as float or binary vectors.
template <Search::DataType data_type>
class SimpleSegment : public BaseSegment
{
public:
    /// init vi with part without ready vi, will build vi later
    template <typename... Args>
    SimpleSegment(Args &&... args) : BaseSegment(std::forward<Args>(args)...)
    {
    }
    ~SimpleSegment() override = default;

    using VI = Search::VectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, data_type>;
    using VIPtr = std::shared_ptr<VI>;
    using DatasetPtr = std::shared_ptr<VectorDataset<data_type>>;
    using VIDataReaderPtr = std::shared_ptr<VIPartReader<data_type>>;

    CachedSegmentPtr prepareVIEntry(std::optional<Metadata> vi_metadata_ = std::nullopt);
    VIDataReaderPtr getVIDataReader(std::function<bool()> cancel_callback);

    CachedSegmentPtr
    buildVI(bool slow_mode, VIBuildMemoryUsageHelper & build_memory_lock, std::function<bool()> cancel_build_callback = {}) override;

    void serialize(
        DiskPtr disk,
        const CachedSegmentPtr vi_entry,
        const String & serialize_local_folder,
        MergeTreeDataPartChecksumsPtr & vector_index_checksum,
        VIBuildMemoryUsageHelper & build_memory_lock) override;

    CachedSegmentHolderPtr loadVI(const MergeIdMapsPtr & vi_merged_maps = nullptr) override;

    SearchResultPtr searchVI(
        const VectorDatasetVariantPtr & queries,
        int32_t k,
        const VIBitmapPtr filter,
        const VIParameter & parameters,
        bool & first_stage_only,
        const MergeIdMapsPtr & vi_merged_maps_ = nullptr) override;

    SearchResultPtr computeTopDistanceSubset(
        const VectorDatasetVariantPtr queries,
        SearchResultPtr first_stage_result,
        int32_t top_k,
        const MergeIdMapsPtr & vi_merged_maps_ = nullptr) override;

    void updateCachedBitMap(const VIBitmapPtr & bitmap) override;

    SegmentInfoPtrList getSegmentInfoList() const override;

    bool supportTwoStageSearch() const override;

    static void searchWithoutIndex(
        DatasetPtr query_data, DatasetPtr base_data, int32_t k, float *& distances, int64_t *& labels, const VIMetric & metric);
};

/// The DecoupleSegment class is a specialized implementation of the BaseSegment class.
/// It is designed to handle vector index segments that are Merged, meaning they can
/// manage multiple sub-segments independently.
template <Search::DataType data_type>
class DecoupleSegment : public SimpleSegment<data_type>
{
public:
    DecoupleSegment(
        const VIDescription & vec_desc_,
        const MergeIdMapsPtr vi_merged_map,
        const MergeTreeDataPartWeakPtr part,
        const MergeTreeDataPartChecksumsPtr & vi_checksums_);
    DecoupleSegment(
        const VIDescription & vec_desc_,
        const MergeIdMapsPtr vi_merged_map,
        const MergeTreeDataPartWeakPtr part,
        const std::vector<std::shared_ptr<SimpleSegment<data_type>>> & segments_);

    ~DecoupleSegment() override
    {
        if (this->flag_vi_expired.isVIFIleExpired())
        {
            auto lock_part = this->getDataPart();
            if (!lock_part->segments_mgr->containDecoupleSegment())
                MergeIdMaps::removeMergedMapsFiles(*lock_part);
        }
    }

    SegmentPtr mutation(const MergeTreeDataPartPtr new_data_part) override;

    void inheritMetadata(const SegmentPtr & old_seg) override;

    CachedSegmentHolderPtr loadVI(const MergeIdMapsPtr & vi_merged_maps = nullptr) override;

    SearchResultPtr searchVI(
        const VectorDatasetVariantPtr & queries,
        int32_t k,
        const VIBitmapPtr filter,
        const VIParameter & parameters,
        bool & first_stage_only,
        const MergeIdMapsPtr & vi_merged_maps_) override;
    SearchResultPtr computeTopDistanceSubset(
        VectorDatasetVariantPtr queries,
        SearchResultPtr first_stage_result,
        int32_t top_k,
        const MergeIdMapsPtr & vi_merged_maps_) override;

    void updateCachedBitMap(const VIBitmapPtr & bitmap) override;

    bool supportTwoStageSearch() const override { return segments.front()->supportTwoStageSearch(); }

    const CachedSegmentKeyList getCachedSegmentKeys() const override;

    bool isDecoupled() const override { return true; }

    SegmentInfoPtrList getSegmentInfoList() const override;

    void removeMemoryRecords() override
    {
        for (const auto & seg : segments)
            seg->removeMemoryRecords();
    }

    void setVIExpiredFlag(UInt8 flag) override
    {
        /// for decouple seg, we need to set flag for all sub segs
        for (const auto & seg : segments)
            seg->setVIExpiredFlag(flag);
        this->flag_vi_expired.setVIExpiredFlag(flag);
    }

    MergeIdMapsPtr getOrInitMergeMaps(bool need_initialize = true)
    {
        if (!merged_maps)
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Merged maps is not inited.");
        if (need_initialize && !merged_maps->isInited())
        {
            auto lock_part = this->getDataPart();
            merged_maps->lazyInitOnce(*lock_part);
        }
        return merged_maps;
    }

    const std::vector<UInt8> getOwnIds() const
    {
        std::vector<UInt8> res;
        for (const auto & seg : segments)
            res.emplace_back(seg->getOwnerPartId());
        return res;
    }

private:
    std::vector<std::shared_ptr<SimpleSegment<data_type>>> segments;
    MergeIdMapsPtr merged_maps;
};

template <typename... Args>
SegmentPtr createSegment(const VIDescription & vec_desc, Args &&... args)
{
    switch (vec_desc.vector_search_type)
    {
        case Search::DataType::FloatVector:
            return std::make_shared<SimpleSegment<Search::DataType::FloatVector>>(vec_desc, std::forward<Args>(args)...);
        case Search::DataType::BinaryVector:
            return std::make_shared<SimpleSegment<Search::DataType::BinaryVector>>(vec_desc, std::forward<Args>(args)...);
        default:
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Unknown data type");
    }
};

template <typename... Args>
SegmentPtr createVIDecoupleSegment(const VIDescription & vec_desc, Args &&... args)
{
    switch (vec_desc.vector_search_type)
    {
        case Search::DataType::FloatVector:
            return std::make_shared<DecoupleSegment<Search::DataType::FloatVector>>(vec_desc, std::forward<Args>(args)...);
        case Search::DataType::BinaryVector:
            return std::make_shared<DecoupleSegment<Search::DataType::BinaryVector>>(vec_desc, std::forward<Args>(args)...);
        default:
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Unknown data type");
    }
}

extern template class SimpleSegment<Search::DataType::FloatVector>;
extern template class SimpleSegment<Search::DataType::BinaryVector>;
extern template class DecoupleSegment<Search::DataType::FloatVector>;
extern template class DecoupleSegment<Search::DataType::BinaryVector>;

} // namespace VectorIndex
