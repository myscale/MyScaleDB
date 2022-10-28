#include "BruteForceSearch.h"
#include <Common/logger_useful.h>

namespace VectorIndex
{

Status tryBruteForceSearch(
    const float * x,
    const float * y,
    size_t d,
    size_t k,
    size_t nx,
    size_t ny,
    int64_t * result_id,
    float * distance,
    const Search::Metric & m)
{
    Poco::Logger * log = &Poco::Logger::get("BruteForce");
    if (m == Search::Metric::IP)
    {
        LOG_DEBUG(log, "Metric is IP");
        faiss::float_minheap_array_t res = {size_t(nx), size_t(k), result_id, distance};
        faiss::knn_inner_product(x, y, d, nx, ny, &res, nullptr);
    }
    else if (m == Search::Metric::L2)
    {
        LOG_DEBUG(log, "Metric is L2");
        faiss::float_maxheap_array_t res = {size_t(nx), size_t(k), result_id, distance};
        faiss::knn_L2sqr(x, y, d, nx, ny, &res, nullptr);
    }
    else
    {
        return Status(8, "Metric not implemented in brute force search");
    }
    return Status();
}
}
