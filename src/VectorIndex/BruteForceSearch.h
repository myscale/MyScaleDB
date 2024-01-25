#pragma once
#include <faiss/utils/distances.h>
#include <SearchIndex/VectorIndex.h>
#include <VectorIndex/Status.h>
#include <Interpreters/VectorScanDescription.h>
#include <VectorIndex/VectorIndexCommon.h>

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


namespace VectorIndex
{
    /// x: the query vector
    /// y: the base vector
    /// d: the dimension of both vectors
    /// k: the top k we desired after distance calculation.
    template <DB::VectorSearchType T>
    Status tryBruteForceSearch(
            const typename VectorSearchTypeMap<T>::VectorDatasetType * x,
            const typename VectorSearchTypeMap<T>::VectorDatasetType * y,
            size_t d,
            size_t k,
            size_t nx,
            size_t ny,
            int64_t * result_id,
            float * distance,
            const Search::Metric & metric_type)
    {
        Poco::Logger * log = &Poco::Logger::get("BruteForce");
        if constexpr (T == DB::VectorSearchType::Float32Vector)
        {
            if (metric_type == Search::Metric::IP)
            {
                LOG_DEBUG(log, "Metric is IP");
                faiss::float_minheap_array_t res = {size_t(nx), size_t(k), result_id, distance};
                faiss::knn_inner_product(x, y, d, nx, ny, &res, nullptr);
            }
            else if (metric_type == Search::Metric::L2)
            {
                LOG_DEBUG(log, "Metric is L2");
                faiss::float_maxheap_array_t res = {size_t(nx), size_t(k), result_id, distance};
                faiss::knn_L2sqr(x, y, d, nx, ny, &res, nullptr);
            }
            else
            {
                return Status(8, "Metric not implemented in brute force search for Float32 Vector");
            }
        }
        else if constexpr (T == DB::VectorSearchType::BinaryVector)
        {
            if (metric_type == Search::Metric::Hamming)
            {
                LOG_DEBUG(log, "Metric is Hamming");
                faiss::hammings_knn_mc(x, y, nx, ny, k, d / 8, reinterpret_cast<int32_t*>(distance), result_id, nullptr);
            }
            else if (metric_type == Search::Metric::Jaccard)
            {
                LOG_DEBUG(log, "Metric is Jaccard");
                jaccard_knn(x, y, nx, ny, k, d / 8, distance, result_id, nullptr);
             }
            else
            {
                return Status(8, "Metric not implemented in brute force search for Binary Vector");
            }
        }
        return Status();
    }
}
