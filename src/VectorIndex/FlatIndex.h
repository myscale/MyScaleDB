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
#include <faiss/IndexFlatFilter.h>
#include <faiss/index_io.h>
#include <faiss/utils/bitMap.h>
#pragma GCC diagnostic pop

#include <string>
#include "Binary.h"
#include "FaissIndex.h"
#include "IndexException.h"

namespace VectorIndex
{
class FlatIndex : public FaissIndex
{
public:
    FlatIndex(IndexType it_, IndexMode im_, Metrics me_, int dimension_, Parameters parameters) : FaissIndex(it_, im_, me_, dimension_)
    {
        faiss::MetricType metrictype;
        switch (me)
        {
            case Metrics::L2:
                metrictype = faiss::METRIC_L2;
                break;
            case Metrics::IP:
                metrictype = faiss::METRIC_INNER_PRODUCT;
                break;
            case Metrics::Cosine:
                metrictype = faiss::METRIC_Cosine;
        }
        getMyParameters(parameters);
        index = std::make_shared<faiss::IndexFlatFilter>(dimension_, metrictype);
    }

    void train(const VectorDatasetPtr dataset, int64_t total) override;

    void addWithoutId(const VectorDatasetPtr dataset) override;

    void search(
        const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & params, GeneralBitMapPtr filter)
        override;

    VectorDatasetPtr getInMemVectors() override;

    void getMyParameters(Parameters params) override;

    bool compare(const VectorIndex & other) override;
};
}
