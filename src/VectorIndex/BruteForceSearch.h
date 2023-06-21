#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdocumentation"
#include <faiss/utils/distances.h>
#pragma GCC diagnostic pop
#include <SearchIndex/VectorIndex.h>
#include <VectorIndex/Status.h>

namespace VectorIndex
{
/// x: the query vector
/// y: the base vector
/// d: the dimension of both vectors
/// k is the top k we desired after distance calculation.

Status tryBruteForceSearch(
    const float * x,
    const float * y,
    size_t d,
    size_t k,
    size_t nx,
    size_t ny,
    int64_t * result_id,
    float * distance,
    const Search::Metric & m);
}
