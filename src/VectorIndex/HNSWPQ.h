#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wshadow-field"
#pragma GCC diagnostic ignored "-Wdocumentation"
#pragma GCC diagnostic ignored "-Wcast-qual"
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#pragma GCC diagnostic ignored "-Wcast-align"
#pragma GCC diagnostic ignored "-Wsuggest-destructor-override"
#include <faiss/IndexHNSWfast.h>
#include "IndexException.h"
#pragma GCC diagnostic pop

#include <string>
#include "Binary.h"
#include "VectorIndex.h"

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
extern const int EMPTY_DATA_PASSED;
}

namespace VectorIndex
{
class HNSWpq : public VectorIndex
{
public:
    HNSWpq(IndexType it_, IndexMode im_, Metrics me_, int dimension_, Parameters parameters);

    void train(const VectorDatasetPtr, int64_t total) override; //give a dataset for training.

    void addWithoutId(const VectorDatasetPtr dataset) override; //give index a set of data to add index，
    // they'll be stored as <id, vector> in index, with auto-incremental ids.

    int64_t removeWithIds(int64_t n, int64_t * ids) override;

    void search(
        const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & param, GeneralBitMapPtr filter)
        override;
    //filter the index with bitmap and perform searching.

    BinaryPtr serialize(size_t max_bytes_to_serialize, bool & finished) override; //searilize index

    void load(BinaryPtr & bi, int64_t total_vec) override; //reverse serialize index

    // void remove(const int32_t * ids) override; //remove id corresponded vectors from index.

    VectorDatasetPtr getInMemVectors() override;

    std::shared_ptr<faiss::IndexHNSWfastPQ> index = nullptr;
    //when index is neither held by an explicit pointer or held in cache,
    //it'll destruct automatically.

    void getMyParameters(Parameters p) override;

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

    bool compare(const VectorIndex & other) override;

private:
    int neighbor = 16;
    int ef_c = 100;
    int pq_m = 8;
    int bit_size = 8;
    void * convertInnerBitMap(GeneralBitMapPtr sharedPtr) override;
    BinaryPtr convertStructToBinary(uint8_t * index_data, size_t written_size) override;
};
}
