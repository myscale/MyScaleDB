#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdocumentation"
#pragma GCC diagnostic ignored "-Wdeprecated"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
#pragma GCC diagnostic ignored "-Wimplicit-float-conversion"
#include <faiss/utils/distances.h>
#include <SearchIndex/VectorIndex.h>
#pragma GCC diagnostic pop
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
