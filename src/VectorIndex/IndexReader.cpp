#include <VectorIndex/IndexException.h>
#include <VectorIndex/IndexReader.h>

namespace VectorIndex
{

size_t BufferIndexReader::operator()(void * ptr, size_t size, size_t nitems)
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
}
