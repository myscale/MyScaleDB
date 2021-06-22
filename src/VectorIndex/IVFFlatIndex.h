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
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlatFilter.h>
#include <faiss/index_io.h>
#include <faiss/utils/bitMap.h>
#include "FaissIndex.h"
#pragma GCC diagnostic pop

#include <string>
#include "Binary.h"

namespace VectorIndex
{
class IVFFlatIndex : public FaissIndex
{
    ///the IVFFlat index type conforming to the generalized vector index standard,
    ///with these standarlization it could be used by execution engine.
public:
    IVFFlatIndex(IndexType it_, IndexMode im_, Metrics me_, int dimension_, Parameters parameters) : FaissIndex(it_, im_, me_, dimension_)
    {
        in_mem = false;
        //TODO initialized index with dynamic fields
        getMyParameters(parameters);
        //index->verbose = true;
    }

    void train(const VectorDatasetPtr dataset, int64_t total) override; //give a dataset for training.

    void addWithoutId(const VectorDatasetPtr dataset) override; //give index a set of data to add index，
    // they'll be stored as <id, vector> in index, with auto-incremental ids.

    void search(
        const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & params, GeneralBitMapPtr filter)
        override;
    //filter the index with bitmap and perform searching.

    // void remove(const int32_t * ids) override; //remove id corresponded vectors from index.

    VectorDatasetPtr getInMemVectors() override;

    void getMyParameters(Parameters params) override;

    bool compare(const VectorIndex & other) override;

    void tune(VectorDatasetPtr base, int topK);

    //when index is neither held by an explicit pointer or held in cache,
    //it'll destruct automatically.

private:
    ///These are just default values, outside class shouldn't access them for reference.
    int ncentroids = 1024;
    float std_m = 6.0;
    float multiplier = 1.3;
    bool profiler = false;
};
}
