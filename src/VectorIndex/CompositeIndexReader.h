#pragma once

#include <VectorIndex/IndexReader.h>
#include <VectorIndex/SegmentId.h>

namespace VectorIndex
{

/// Put a checksum of 12 bytes at the head of binary, then append the compressed bytes.
/// this only compress and checksum the binary index file, not including the metadata.
uint32_t compressWithCheckSum(uint8_t cmb, uint8_t * source, size_t size, BinaryPtr des);

uint32_t validateAndDecompress(const BinaryPtr source, size_t uncompressed_size, uint8_t * des);

/// CompositeIndexReader reads from multiple compressed .vidx files.
struct CompositeIndexReader : IndexReader
{
    SegmentId segment_id;
    int64_t original_binary_size;
    size_t offset = 0;
    int part_count = 0;
    int64_t current_buffer_start = 0;
    int64_t current_loaded_size = 0;
    bool final_mark = false;
    Poco::Logger * log = &Poco::Logger::get("CompositeIndexReader");
    std::vector<uint8_t> buffer;

    CompositeIndexReader(SegmentId segment_id_, int64_t original_binary_size_);
    void read_part();
    size_t operator()(void * ptr, size_t size, size_t nitems) override;
};
}
