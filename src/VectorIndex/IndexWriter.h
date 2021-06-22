#pragma once

#include <cstring>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#include "faiss/impl/io.h"
#pragma GCC diagnostic pop

namespace VectorIndex
{
//in memory reader conforming to faiss IOwriter that's used to produce
// a binary object
class IndexWriter : public faiss::VectorIOWriter
{
public:
    size_t operator()(const void * ptr, size_t size, size_t nitems) override;
    size_t write(const void * ptr, size_t size, size_t nitems = 1) { return operator()(ptr, size, nitems); }
};

#define RESERVE 2

}
