#pragma once

#include <memory>
#include <string>
namespace VectorIndex
{
enum class seekdir
{
    beg,
    cur,
    end
};
class IOWriter
{
    //generic type for IO readers that could be used to write binaries to disk or any other persistent medium

public:
    virtual bool open(const std::string & name, bool append = false) = 0;

    virtual void write(void * ptr, int64_t size) = 0;

    virtual int64_t length() = 0;

    virtual void close() = 0;

    virtual void seekp(int64_t pos, seekdir seek) = 0;

    virtual ~IOWriter() = default;
};

using IOWriterPtr = std::shared_ptr<IOWriter>;

}
