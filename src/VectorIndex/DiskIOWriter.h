#pragma once

#include <fstream>
#include "IOWriter.h"

namespace VectorIndex
{
class DiskIOWriter : public IOWriter
//writer for writing serialized index to disk, use a standard fstream as medium.
{
public:
    DiskIOWriter() = default;

    bool open(const std::string & name, bool append) override;

    void write(void * ptr, int64_t size) override;

    int64_t length() override;

    void seekp(int64_t pos, seekdir seek) override;

    void close() override;

    std::string name;
    int64_t len;
    std::fstream fs;
};
}
