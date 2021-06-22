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
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic pop

#include <string>
#include "Binary.h"
#include "VectorIndex.h"
#include "hnswlib/hnswalg.h"

namespace VectorIndex
{
class HNSWIndex : public VectorIndex
{
public:
    HNSWIndex(IndexType it_, IndexMode im_, Metrics me_, int dimension_, Parameters parameters) : VectorIndex(it_, im_, me_, dimension_)
    {
        getMyParameters(parameters);
    }

    void train(const VectorDatasetPtr, int64_t total) override;

    void addWithoutId(const VectorDatasetPtr dataset) override;

    void search(
        const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & param, GeneralBitMapPtr filter)
        override;

    int64_t removeWithIds(int64_t n, int64_t * ids) override;

    BinaryPtr serialize(size_t max_bytes_to_serialize, bool & finished) override;

    void load(BinaryPtr & bi, int64_t total_vec) override;

    VectorDatasetPtr getInMemVectors() override;

    /// When index is neither held by an explicit pointer or held in cache,
    /// it'll destruct automatically.
    std::shared_ptr<hnswlib::HierarchicalNSW<float>> index = nullptr;

    void getMyParameters(Parameters params) override;

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
    int max_element;
    int neighbor = 16;
    int ef_c = 100;

    void * convertInnerBitMap(GeneralBitMapPtr sharedPtr) override;
    BinaryPtr convertStructToBinary(uint8_t * index_data, uint64_t written_size) override;
};
}
