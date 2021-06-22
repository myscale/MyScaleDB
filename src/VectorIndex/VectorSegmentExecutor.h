#pragma once
#pragma GCC diagnostic ignored "-Wunused-function"
#include <string>
#include <Compression/CompressionInfo.h>
#include <Storages/VectorIndicesDescription.h>
#include <Common/logger_useful.h>
#include <VectorIndex/SegmentId.h>
#include <VectorIndex/Autotuner.h>
#include <VectorIndex/FlatIndex.h>
#include <VectorIndex/GeneralBitMap.h>
#include <VectorIndex/HNSWIndex.h>
#include <VectorIndex/IVFFlatIndex.h>
#include <VectorIndex/IVFPQIndex.h>
#include <VectorIndex/Status.h>
#include <VectorIndex/IndexException.h>

namespace VectorIndex
{
static size_t optimal_segment_size = (1LL << 32) - 10; //This is 2^32, or 4GB, we are not maxing it since compression introduce a header.

struct OperatingPoint
{
    Parameters op_point;
    String getPointAt(String index) { return (op_point[index]); }

    void insertPoint(String name, String t) { op_point.insert(std::pair<String, String>(name, t)); }
};
using OperatingPointPtr = std::shared_ptr<OperatingPoint>;
using OPsPtr = std::shared_ptr<std::vector<std::pair<float, OperatingPointPtr>>>;

struct IndexWithMeta
{
    IndexWithMeta() = default;
    IndexWithMeta(VectorIndexPtr & index_, uint64_t total_vec_, OPsPtr op_points_, GeneralBitMapPtr delete_bitMap_,
            Parameters des_)
        : index(index_), total_vec(total_vec_), op_points(op_points_), delete_bitmap(delete_bitMap_), des(des_) {};
    
    IndexWithMeta(VectorIndexPtr & index_, uint64_t total_vec_, OPsPtr op_points_, GeneralBitMapPtr delete_bitMap_,
            Parameters des_, std::shared_ptr<std::vector<UInt64>> row_ids_map_, std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map_,
            std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map_)
        : index(index_), total_vec(total_vec_), op_points(op_points_), delete_bitmap(delete_bitMap_), des(des_),
          row_ids_map(row_ids_map_), inverted_row_ids_map(inverted_row_ids_map_), inverted_row_sources_map(inverted_row_sources_map_){};
    VectorIndexPtr index;
    uint64_t total_vec;
    OPsPtr op_points;
private:
    GeneralBitMapPtr delete_bitmap;
    mutable std::mutex mutex_of_delete_bitmap;
public:
    Parameters des;
    std::shared_ptr<std::vector<UInt64>> row_ids_map;
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map;
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map;

    void setDeleteBitmap(GeneralBitMapPtr delete_bitmap_)
    {
        std::lock_guard<std::mutex> lg(mutex_of_delete_bitmap);
        delete_bitmap = std::move(delete_bitmap_);
    }

    GeneralBitMapPtr getDeleteBitmap() const
    {
        std::lock_guard<std::mutex> lg(mutex_of_delete_bitmap);
        return delete_bitmap;
    }
};
using IndexWithMetaPtr = std::shared_ptr<IndexWithMeta>;


class VectorSegmentExecutor
{
    /// the exposed api set which should be called by users trying to use vector index;
    /// the user should not visit any index directly.
public:
    ///create the index but not inserting any data
    VectorSegmentExecutor(IndexType type_, const SegmentId & segment_id_, Parameters des_, size_t dimension_ = 0);

    explicit VectorSegmentExecutor(const SegmentId & segment_id_);

    ///serialize and store index at segment_id
    Status serialize();

    /// load index from segment_id,
    /// if hit in cache then simply redirect pointer.
    Status load();

    ///gpu related.
    // Status copyToGpu(int32_t device_id, [[maybe_unused]] bool hybrid = false);
    //TODO

    Status copyToCpu();
    //TODO

    /// a c style method that wraps VectorIndex::Search function and does some preprocessing.
    /// distance and labels are the pointers to expected results and should be declared before calling this method with proper size.
    Status
    search(VectorDatasetPtr dataset, int32_t k, float *& distances, int64_t *& labels, GeneralBitMapPtr filter, Parameters parameters);

    /// buildIndex method use data to train an index, does not add data for search.
    /// it'll call index's train(), but does not write to file io.
    Status buildIndex(VectorDatasetPtr data_set, int64_t total_vectors_expected, bool slow_mode);

    ///put the index stored in VectorSegmentExecutor into cache.
    Status cache();

    ///return index type.
    IndexType indexType();

    ///simply add vectors into index for search.
    Status addVectors(VectorDatasetPtr data_set);

    Status removeByIds(int64_t n, int64_t * ids);

    GeneralBitMapPtr getDeleteBitMap() { return this->delete_bitmap; }

    ///return total number of vectors.
    int64_t getRawDataSize();

    ///call to set build parameters in this VectorSegmentExecutor
    ///just put the values of parameters as "parameter_name":"value"
    /// exp: "nprobes":128
    void setIndexParameters(Parameters parameters);

    ///automatically compute some operating points for the current index. An operating point is a
    /// combination of all parameters of the index which corresponds to an estimated accuracy of the inedx while searching.
    /// the accuracy is calculated by testing the base vectors against itself.
    ///this function call only does some preprocessing and then dispatch the task to the autotuner.
    Status dispathAutoTuneTask(VectorDatasetPtr base); /// deprecated


    /// this function is for zili's profiler.
    Status tune(VectorDatasetPtr base, std::vector<int64_t> & empty_ids, size_t current_round_start_row);


    ///look into cached OPs in memory, if found nothing then look into file system and read
    ///the OP file generated by autotuner.
    Status getOps();

    /// cancel building the current vector index, free associated resources.
    Status cancelBuild();

    void updateCacheValueWithRowIdsMaps();

    static bool compareVectorIndexParameters(IndexType t1, Parameters p1, IndexType t2, Parameters p2);

    static void setCacheManagerSizeInBytes(size_t size);

    static void setSerializeSegmentSize(size_t size);

    static std::list<std::pair<CacheKey, Parameters>> getAllCacheNames();

    static Status
    searchWithoutIndex(VectorDatasetPtr query_data, VectorDatasetPtr bash_data, int32_t k, float *& distances, int64_t *& labels, const Metrics& metrics);

    ///expire the related index from cache.
    static Status removeFromCache(const CacheKey & cache_key);

    GeneralBitMapPtr getRealBitMap(const std::vector<UInt64>& selected_row_ids)
    {
        GeneralBitMapPtr bits = std::make_shared<GeneralBitMap>(total_vec);
        if (segment_id.fromMergedParts())
        {
            /// need to transfer merged row id to real row id of this old data part.
            for (auto & new_row_id : selected_row_ids)
            {
                if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
                {
                    bits->set((*inverted_row_ids_map)[new_row_id]);
                }
            }
        }
        else
        {
            for (auto & row_id : selected_row_ids)
            {
                bits->set(row_id);
            }
        }
        return bits;
    }

    /// Update SegmentId
    void updateSegmentId(const SegmentId & new_segment_id)
    {
        segment_id = new_segment_id;
    }

    /// Reload delete bitmap from disk.
    bool reloadDeleteBitMap() { return readBitMap(); }

    /// Update part's single delete bitmap after lightweight delete on disk and cache if exists.
    void updateBitMap(const std::vector<UInt64>& deleted_row_ids);

    /// Update merged old part's delete bitmap after lightweight delete on disk and cache if exists.
    void updateMergedBitMap(const std::vector<UInt64>& deleted_row_ids);

private:
    Status startWrite();

    Status finishWrite(int64_t binary_total_size);

    bool writeBitMap();

    bool readBitMap();

    Status writePart(bool final, int segment_count, uint8_t * index_segment_offset, size_t index_segment_size);

    Status readPart(bool & next, int part_count, uint8_t* index_binary, int64_t & current_loaded_size);

    ///put a checksum of 12 bytes at the head of binary, then append the compressed bytes.
    ///this only compress and checksum the binary index file, not including the metadata.
    uint32_t compressWithCheckSum(uint8_t * source, size_t size, BinaryPtr des);

    ///take the compreseed binary of index file, check the checksum, them decompress it using
    ///method provided in header to des.
    uint32_t validateAndDecompress(BinaryPtr source, size_t uncompressed_size, uint8_t * des);

    void handleMergedMaps();

    void readTotalVec();

    void transferToNewRowIds(int64_t *& labels, int size)
    {
        if (row_ids_map->empty())
        {
            return;
        }

        LOG_DEBUG(log, "[transferToNewRowIds] size: {}", size);

        for (int i = 0; i < size; i++)
        {
            if (labels[i] != -1)
            {
                labels[i] = (*row_ids_map)[labels[i]];
            }
        }
    }

    const static UInt32 COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER = LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER;
    const UInt32 dimension;
    const IndexType type;
    IndexMode mode = IndexMode::CPU;
    Metrics me = Metrics::L2;
    bool auto_tune = false;
    uint8_t cmb = static_cast<uint8_t>(DB::CompressionMethodByte::LZ4);
    VectorIndexPtr index = nullptr; //index related to this VectorSegmentExecutor
    SegmentId segment_id; //this index's related segment_id and file write position.
    Poco::Logger * log;
    UInt64 total_vec = 0;
    OPsPtr op_points = nullptr; //operating points precomputed as an <accuracy,parameter> map,ordered by acc.
    GeneralBitMapPtr delete_bitmap = nullptr; //manage deletion from database
    Parameters des;
    std::shared_ptr<std::vector<UInt64>> row_ids_map = std::make_shared<std::vector<UInt64>>();
    std::shared_ptr<std::vector<UInt64>> inverted_row_ids_map = std::make_shared<std::vector<UInt64>>();
    std::shared_ptr<std::vector<uint8_t>> inverted_row_sources_map = std::make_shared<std::vector<uint8_t>>();
};

using VectorSegmentExecutorPtr = std::shared_ptr<VectorSegmentExecutor>;


}
