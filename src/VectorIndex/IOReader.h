#pragma once

#include <memory>
#include <string>

namespace VectorIndex
{

class IOReader
{
    //generic type for IO readers that could be used to read  binaries from disk or any other persistent medium
public:
    virtual bool open(const std::string & name) = 0;

    virtual void read(void * ptr, int64_t size) = 0;

    virtual void seekg(int64_t pos) = 0;

    virtual int64_t length() = 0;

    virtual void close() = 0;

    virtual bool good() = 0;

    virtual ~IOReader() = default;
};

using IOReaderPtr = std::shared_ptr<IOReader>;

}
