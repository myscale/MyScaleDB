#include "DiskIOReader.h"
namespace VectorIndex
{
bool DiskIOReader::open(const std::string & nameO)
{
    name = nameO;
    fs = std::fstream(nameO, std::ios::in | std::ios::binary);
    return fs.good();
}

void DiskIOReader::read(void * ptr, int64_t size)
{
    fs.read(reinterpret_cast<char *>(ptr), size);
}

void DiskIOReader::seekg(int64_t pos)
{
    fs.seekg(pos);
}

int64_t DiskIOReader::length()
{
    fs.seekg(0, fs.end);
    int64_t len = fs.tellg();
    fs.seekg(0, fs.beg);
    return len;
}

void DiskIOReader::close()
{
    fs.close();
}

bool DiskIOReader::good()
{
    return fs.good();
}
}
