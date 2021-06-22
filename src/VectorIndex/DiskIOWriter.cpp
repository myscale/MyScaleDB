#include "DiskIOWriter.h"
namespace VectorIndex
{
void DiskIOWriter::close()
{
    fs.close();
}

int64_t DiskIOWriter::length()
{
    return len;
}

bool DiskIOWriter::open(const std::string & nameO, bool append)
{
    name = nameO;
    len = 0;
    if (append)
    {
        fs = std::fstream(name, std::ios::out | std::ios::in | std::ios::app | std::ios::binary);
    }
    else
    {
        fs = std::fstream(name, std::ios::out | std::ios::binary);
    }
    return fs.good();
}

void DiskIOWriter::write(void * ptr, int64_t size)
{
    //cast unsigned char to char
    fs.write(reinterpret_cast<char *>(ptr), size);
}

void DiskIOWriter::seekp(int64_t pos, seekdir seek)
{
    if (seek == seekdir::beg)
    {
        fs.seekp(pos, std::ios::beg);
    }
    else if (seek == seekdir::cur)
    {
        fs.seekp(pos, std::ios::cur);
    }
    else if (seek == seekdir::end)
    {
        fs.seekp(pos, std::ios::end);
    }
}

}
