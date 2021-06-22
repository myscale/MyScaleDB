
#include "IndexWriter.h"
namespace VectorIndex
{
size_t IndexWriter::operator()(const void * ptr, size_t size, size_t nitems)
{
    //a copy of VectorIOWriter from faiss
    size_t total_need = size * nitems + actual_size;
    if (total_need > 0)
    {
        if (reserved_size == 0)
        {
            reserved_size = total_need * RESERVE;
            actual_size = size * nitems;
            data = new uint8_t[reserved_size];
            memcpy(data, ptr, actual_size);
        }
        else if (reserved_size < total_need)
        { //if reserved space if not enough
            reserved_size = total_need * RESERVE;
            auto * new_data = new uint8_t[reserved_size];
            memcpy(new_data, data, actual_size); //copy old data to new data chunk
            delete[] data;
            data = new_data;
            memcpy(data + actual_size, ptr, size * nitems); //copy increment to data
            actual_size = total_need;
        }
        else
        {
            memcpy(data + actual_size, ptr, size * nitems); //if still got reserved space
            actual_size = total_need;
        }
    }
    return nitems;
}
}
