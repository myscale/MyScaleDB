#pragma once

#include <memory>
#include <unordered_map>
#include <Common/logger_useful.h>
#include <VectorIndex/Binary.h>
#include <VectorIndex/Dataset.h>
#include <VectorIndex/GeneralBitMap.h>
#include <VectorIndex/IndexException.h>

namespace VectorIndex
{
enum IndexType
{
    IVFFLAT,
    IVFPQ,
    IVFSQ,
    FLAT,
    HNSWFLAT,
    HNSWPQ,
    HNSWSQ
};

enum IndexMode
{
    CPU,
    GPU,
    FPGA
};

enum Metrics
{
    L2,
    IP,
    Cosine
};

extern std::atomic_int count;
extern int num_thread_for_vector;

using Parameters = std::unordered_map<std::string, std::string>;
using AccParametersPack = std::unordered_map<float, Parameters>;
#define PARAMETER_PACK_NAME "_parameter"


class VectorIndex
{
    /// interface for vector index definitions,
    /// user-defined vector index should implement all these methods.
public:
    VectorIndex() = default;
    VectorIndex(IndexType it_, IndexMode im_, Metrics me_, int dimension_) : it(it_), me(me_), im(im_), dimension(dimension_) { }

    ///train vector index on a set of data, should not insert the data.
    virtual void train(const VectorDatasetPtr dataset, const int64_t total_vector_expected) = 0;

    ///adding data into index, the data is supposed to be stored with an <id, vector> mapping inside
    ///the index such that id autoincrement and one can easily map from an id to the vector inserted.
    virtual void addWithoutId(const VectorDatasetPtr dataset) = 0;

    virtual int64_t removeWithIds(int64_t n, int64_t * ids) = 0;

    ///search the index with the given dataset and a filter, returns id and distance of each result up to topK results.
    virtual void search(
        const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & params, GeneralBitMapPtr filter)
        = 0;

    ///serialize index into binaries in memory, returns a pointer to that binary.
    virtual BinaryPtr serialize(size_t max_bytes_to_serialize, bool & finished) = 0;

    ///load index from binaries into a usable index.
    virtual void load(BinaryPtr & bi, int64_t total_vec) = 0;

    // virtual void remove(const int32_t * ids) = 0; //从 index 中删除 id 对应的 vector.

    ///the type of index（IVFFLAT,HNSW, etc）
    IndexType indexType() { return it; }

    IndexMode indexMode() { return im; }

    Metrics metrics() { return me; }

    ///whether the data in index is uncompressed
    bool inMemVectors() { return in_mem; }

    void setTrained() { trained = true; }

    bool trainStatus() { return trained; }

    void setRawData(BinaryPtr ptr) { rawData = ptr; }

    size_t sizeInBytes() const
    {
        if (rawData != nullptr && rawData->size >= 0)
        {
            return static_cast<size_t>(rawData->size);
        }
        else
        {
            return 0;
        }
    }

    ///if possible, get uncompressed vectors stored in memory
    virtual VectorDatasetPtr getInMemVectors() = 0;

    /// set parameters for build and search
    virtual void getMyParameters(Parameters params) = 0;

    virtual AccParametersPack exploreTask(
        const float * query_data,
        const int64_t * gt,
        int topK,
        int query_size,
        bool oneRecall,
        std::mutex & m,
        std::condition_variable & cv,
        bool & go,
        Poco::Logger * log)
        = 0;

    /// compare this vector index with another, see if all their parameters are the same.
    virtual bool compare(const VectorIndex & other) = 0;


    bool parseParameter(Parameters & param)
    {
        try
        {
            getMyParameters(param);
        }
        catch (IndexException & e)
        {
            (void)e;
        }
        return true;
    }

    virtual ~VectorIndex() = default;

protected:
    IndexType it; //ivfpq, flat, hnsw
    Metrics me; // L1, L2, ip, shannon
    IndexMode im; // cpu, gpu
    int dimension; // dimension
    bool in_mem; // if vectors are stored fully in mem
    bool trained = false; // searchabled
    BinaryPtr rawData; // unfortunately to boost load speed we have to manage rawData ourselves
    int64_t total_vector = 0;

protected:
    virtual void * convertInnerBitMap(GeneralBitMapPtr sharedPtr) = 0; //TODO might be too expensive
    virtual BinaryPtr convertStructToBinary(uint8_t * index_data, uint64_t written_size) = 0;

private:
/// used to store old2new row id map for merge operation.
    std::vector<int> row_ids_map; 
};

using VectorIndexPtr = std::shared_ptr<VectorIndex>;

}
