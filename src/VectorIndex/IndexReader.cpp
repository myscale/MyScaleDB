//
// Created by 陈卓 on 2021/6/29.
//
#pragma GCC diagnostic ignored "-Wunused-but-set-parameter"
#include "IndexReader.h"

namespace VectorIndex
{
///copy data
size_t IndexReader::operator()(void * ptr, size_t size, size_t nitems)
{
    if (rp >= total)
        return 0;
    size_t nremain = (total - rp) / size;
    if (nremain < nitems)
        nitems = nremain;
    if (size * nitems > 0)
    {
        memcpy(ptr, data + rp, size * nitems);
        rp += size * nitems;
    }
    return nitems;
}

///used for assiging data directly to respective position, no copying
size_t IndexReader::operator()(char *& ptr, size_t size)
{
    if (rp >= total)
        return 0;
    size_t nremain = total - rp;
    if (nremain < size)
        size = nremain;
    if (size > 0)
    {
        ptr = reinterpret_cast<char *>(data) + rp;
        rp += size;
    }
    return size;
}
}
