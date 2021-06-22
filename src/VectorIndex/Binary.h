#pragma once

#include <Common/logger_useful.h>

namespace VectorIndex
{
struct Binary
{
    //a tuple class containing binary representation of anything, used to handle IO
public:
    uint8_t * data;
    int64_t size;

    Poco::Logger * const log;

    Binary():data(nullptr),size(0),log(&Poco::Logger::get("Binary"))
    {}

    ~Binary() noexcept
    {
        delete []data;
    }
};
using BinaryPtr = std::shared_ptr<Binary>;

}
