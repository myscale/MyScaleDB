#pragma once

#include <Disks/IDisk.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
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
            LOG_ERROR(getLogger("VectorIndexReader"), "Failed to read file {}: {}", _file, e.what());
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
    explicit VectorIndexWriter(DB::DiskPtr _disk, DB::String _file, std::shared_ptr<DB::MergeTreeDataPartChecksums> & _checksums)
        : checksums(_checksums)
    {
        fs::path file_path(_file);
        file_name = file_path.filename();

        try
        {
            out = _disk->writeFile(_file);
            hashing_out = std::make_unique<DB::HashingWriteBuffer>(*out);
        }
        catch (DB::Exception & e)
        {
            LOG_ERROR(getLogger("VectorIndexWriter"), "Failed to write file {}: {}", _file, e.what());
        }
    }

    ~VectorIndexWriter() override
    {
        if (checksums && !file_name.empty() && hashing_out)
            checksums->addFile(file_name, hashing_out->count(), hashing_out->getHash());

        close();
    }

    Search::AbstractOStream & write(const char * s, std::streamsize count) override
    {
        if (hashing_out)
            hashing_out->write(s, count);

        return *this;
    }

    bool good() override
    {
        if (hashing_out)
            return hashing_out->hasPendingData();
        else
            return false;
    }

    void close() override
    {
        if (hashing_out)
            hashing_out->finalize();
        if (out)
            out->finalize();
    }

    Search::AbstractOStream & seekp(std::streampos /*offset*/, std::ios_base::seekdir /*dir*/) override
    {
        /// TODO: implement
        return *this;
    }

private:
    String file_name;
    std::unique_ptr<DB::WriteBufferFromFileBase> out;
    std::unique_ptr<DB::HashingWriteBuffer> hashing_out;
    std::shared_ptr<DB::MergeTreeDataPartChecksums> checksums;
};

}
