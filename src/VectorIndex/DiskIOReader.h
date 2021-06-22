#pragma once

#include <fstream>
#include "IOReader.h"
namespace VectorIndex
{
class DiskIOReader : public IOReader
//Reader for reading serialized index from disk, use a standard fstream as medium.
{
public:
    DiskIOReader() = default;

    bool open(const std::string & name) override;

    void read(void * ptr, int64_t size) override;

    void seekg(int64_t pos) override;

    int64_t length() override;

    void close() override;

    bool good() override;

    std::string name;
    std::fstream fs;
};
}
