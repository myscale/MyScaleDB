#pragma once
#include "faiss/impl/io.h"

namespace VectorIndex
{
//in memory reader conforming to faiss IOreader that's used to produce
// a binary object
class IndexReader : public faiss::VectorIOReader
{
public:
    /// This is less elegant than it should be for the sack of performance.
    /// we have two overloaded read functions, the first one, which takes void * as input
    /// will copy the data from source into ptr.
    /// The second one, which takes char * & as input, will change the address pointed to
    /// by the ptr to an existing memory space, saving the copy overhead.

    /// copy operator, copy memory into ptr.
    size_t operator()(void * ptr, size_t size, size_t nitems) override;
    size_t read(void * ptr, size_t size, size_t nitems = 1) { return operator()(ptr, size, nitems); }
    /// assign operator, change address pointed to by ptr.
    size_t operator()(char *& ptr, size_t size) override;
    size_t read(char *& ptr, size_t size, size_t nitems = 1) { return operator()(ptr, size * nitems); }
};

}
