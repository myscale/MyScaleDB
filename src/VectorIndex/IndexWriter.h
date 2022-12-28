#pragma once

#include <cstring>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#include "faiss/impl/io.h"
#pragma GCC diagnostic pop

namespace VectorIndex
{
struct IndexWriter : faiss::IOWriter
{
    size_t write(const void * ptr, size_t size, size_t nitems = 1) { return operator()(ptr, size, nitems); }
};

// in memory reader conforming to faiss IOwriter that's used to produce
// a binary object
struct BufferIndexWriter : IndexWriter
{
    uint8_t * data;
    uint64_t actual_size = 0;
    uint64_t reserved_size = 0;
    size_t operator()(const void * ptr, size_t size, size_t nitems) override;
};
}
