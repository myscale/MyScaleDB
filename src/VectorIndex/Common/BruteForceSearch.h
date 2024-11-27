#pragma once
#include <faiss/utils/distances.h>

#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Storages/VSDescription.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wcast-qual"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#include <faiss/utils/hamming.h>
#pragma clang diagnostic pop

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wunused-function"
#include <faiss/utils/jaccard.h>
#pragma clang diagnostic pop
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace Search
{
enum class DataType;
}

namespace VectorIndex
{
    /// x: the query vector
    /// y: the base vector
    /// d: the dimension of both vectors
    /// k: the top k we desired after distance calculation.
    template <Search::DataType T>
    void tryBruteForceSearch(
            const typename SearchIndexDataTypeMap<T>::VectorDatasetType * x,
            const typename SearchIndexDataTypeMap<T>::VectorDatasetType * y,
            size_t d,
            size_t k,
            size_t nx,
            size_t ny,
            int64_t * result_id,
            float * distance,
            const VIMetric & metric_type)
    {
        LoggerPtr log = getLogger("BruteForce");
        if constexpr (T == Search::DataType::FloatVector)
        {
            if (metric_type == VIMetric::IP)
            {
                LOG_DEBUG(log, "Metric is IP");
                faiss::float_minheap_array_t res = {size_t(nx), size_t(k), result_id, distance};
                faiss::knn_inner_product(x, y, d, nx, ny, &res, nullptr);
            }
            else if (metric_type == VIMetric::L2)
            {
                LOG_DEBUG(log, "Metric is L2");
                faiss::float_maxheap_array_t res = {size_t(nx), size_t(k), result_id, distance};
                faiss::knn_L2sqr(x, y, d, nx, ny, &res, nullptr);
            }
            else
            {
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Metric not implemented in brute force search for Float32 Vector");
            }
        }
        else if constexpr (T == Search::DataType::BinaryVector)
        {
            if (metric_type == VIMetric::Hamming)
            {
                LOG_DEBUG(log, "Metric is Hamming");
                faiss::hammings_knn_mc(x, y, nx, ny, k, d / 8, reinterpret_cast<int32_t*>(distance), result_id, nullptr);
            }
            else if (metric_type == VIMetric::Jaccard)
            {
                LOG_DEBUG(log, "Metric is Jaccard");
                jaccard_knn(x, y, nx, ny, k, d / 8, distance, result_id, nullptr);
             }
            else
            {
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Metric not implemented in brute force search for Binary Vector");
            }
        }
    }
}
