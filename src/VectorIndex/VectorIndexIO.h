#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/logger_useful.h>

#include <SearchIndex/Common/IndexDataIO.h>

#include <memory>


namespace VectorIndex
{

class VectorIndexReader : public Search::AbstractIStream
{
public:
    explicit VectorIndexReader(DB::DiskPtr _disk, DB::String _file)
    {
        try
        {
            in = _disk->readFile(_file);
        }
        catch (DB::Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("VectorIndexReader"), "Failed to read file {}: {}", _file, e.what());
        }
    }

    Search::AbstractIStream & read(char * s, std::streamsize count) override
    {
        if (in)
            last_read_bytes = in->read(s, count);
        else
            last_read_bytes = 0;

        return *this;
    }

    bool is_open() const override
    {
        if (in)
            return true;
        else
            return false;
    }

    bool fail() const override
    {
        if (in)
            return !(operator bool());
        else
            return true;
    }

    bool eof() const override
    {
        if (in)
            return in->eof();
        else
            return true;
    }

    std::streamsize gcount() const override { return last_read_bytes; }

    explicit operator bool() const override
    {
        if (in)
            return in.operator bool();
        else
            return false;
    }

    Search::AbstractIStream & seekg(std::streampos offset, std::ios_base::seekdir dir) override
    {
        if (in)
            in->seek(offset, dir);

        return *this;
    }

private:
    std::unique_ptr<DB::ReadBufferFromFileBase> in;
    size_t last_read_bytes = 0;
};

class VectorIndexWriter : public Search::AbstractOStream
{
public:
    explicit VectorIndexWriter(DB::DiskPtr _disk, DB::String _file)
    {
        try
        {
            out = _disk->writeFile(_file);
        }
        catch (DB::Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("VectorIndexWriter"), "Failed to write file {}: {}", _file, e.what());
        }
    }

    Search::AbstractOStream & write(const char * s, std::streamsize count) override
    {
        if (out)
            out->write(s, count);

        return *this;
    }

    bool good() override
    {
        if (out)
            return out->hasPendingData();
        else
            return false;
    }

    void close() override
    {
        if (out)
            out->finalize();
    }

    Search::AbstractOStream & seekp(std::streampos /*offset*/, std::ios_base::seekdir /*dir*/) override
    {
        /// TODO: implement
        return *this;
    }

private:
    std::unique_ptr<DB::WriteBufferFromFileBase> out;
};

}
