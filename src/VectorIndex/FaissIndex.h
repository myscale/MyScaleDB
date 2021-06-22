#pragma once
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#pragma GCC diagnostic ignored "-Wdocumentation"
#include <unordered_map>
#include <faiss/Index.h>
#include "VectorIndex.h"


namespace VectorIndex
{
#define parallel_mode_4_threadhold 16
#define min_centroid_size 39
class FaissIndex : public VectorIndex
{
public:
    FaissIndex(IndexType it_, IndexMode im_, Metrics me_, int dimension_) : VectorIndex(it_, im_, me_, dimension_) { }
    virtual BinaryPtr serialize(size_t max_bytes, bool & finished) override;
    virtual void load(BinaryPtr & bi, int64_t total_vec) override;
    int64_t removeWithIds(int64_t n, int64_t * ids) override;
    AccParametersPack exploreTask(
        const float * query_data,
        const int64_t * gt,
        int topK,
        int query_size,
        bool oneRecall,
        std::mutex & m,
        std::condition_variable & cv,
        bool & go,
        Poco::Logger * log) override;
    std::shared_ptr<faiss::Index> index = nullptr;

protected:
    void * convertInnerBitMap(GeneralBitMapPtr sharedPtr) override;
    BinaryPtr convertStructToBinary(uint8_t * index_data, uint64_t written_size) override;
    Parameters convertParamsToMap(std::string keys);
};

}
