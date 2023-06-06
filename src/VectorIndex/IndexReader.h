#pragma once

#include <vector>

#include <Common/logger_useful.h>

#include "faiss/impl/io.h"

namespace VectorIndex
{

struct IndexReader : faiss::IOReader
{
    size_t read(void * ptr, size_t size, size_t nitems = 1) { return operator()(ptr, size, nitems); }
};

struct BufferIndexReader : IndexReader
{
    uint8_t * data;
    uint64_t total = 0;
    uint64_t rp = 0;

    size_t operator()(void * ptr, size_t size, size_t nitems) override;
};
}
