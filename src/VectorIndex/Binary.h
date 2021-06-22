#pragma once

namespace VectorIndex
{
struct Binary
{
    //a tuple class containing binary representation of anything, used to handle IO
public:
    uint8_t * data;
    int64_t size;

    ~Binary() { delete[] data; }
};
using BinaryPtr = std::shared_ptr<Binary>;

}
