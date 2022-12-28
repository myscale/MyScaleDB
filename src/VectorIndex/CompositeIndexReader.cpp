#include <Compression/LZ4_decompress_faster.h>
#include <VectorIndex/CompositeIndexReader.h>
#include <VectorIndex/DiskIOReader.h>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

namespace VectorIndex
{
const static UInt32 COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER = LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER;

uint32_t compressWithCheckSum(uint8_t cmb, uint8_t * source, size_t size, BinaryPtr des)
{
    DB::CompressionCodecPtr codec = DB::CompressionCodecFactory::instance().get(cmb);
    size_t decompressed_size = size;
    des->data = new uint8_t[codec->getCompressedReserveSize(decompressed_size) + COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER];
    /// the reserved size might be much larger than actual compressed size
    uint32_t size_compressed
        = codec->compress(reinterpret_cast<const char *>(source), decompressed_size, reinterpret_cast<char *>(des->data));
    des->size = size_compressed;
    return size_compressed;
}

/// Take the compreseed binary of index file, check the checksum, them decompress it using
/// method provided in header to des.
uint32_t validateAndDecompress(const BinaryPtr source, size_t uncompressed_size, uint8_t * des)
{
    uint8_t method = DB::ICompressionCodec::readMethod(reinterpret_cast<const char *>(source->data));
    DB::CompressionCodecPtr codec = DB::CompressionCodecFactory::instance().get(method);

    uint32_t size_decompressed
        = codec->decompress(reinterpret_cast<const char *>(source->data), source->size, reinterpret_cast<char *>(des));

    if (uncompressed_size != size_decompressed)
    {
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "vector index on disk is corrupted");
    }
    return size_decompressed;
}

CompositeIndexReader::CompositeIndexReader(SegmentId segment_id_, int64_t original_binary_size_)
    : segment_id(segment_id_), original_binary_size(original_binary_size_)
{
}

void CompositeIndexReader::read_part()
{
    if (final_mark)
        return;

    String path = segment_id.getFullPath() + "_" + ItoS(part_count++) + VECTOR_INDEX_FILE_SUFFIX;
    DiskIOReader reader;
    if (!reader.open(path))
    {
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "failed to open " + path);
    }
    BinaryPtr index_binary_compressed = std::make_shared<Binary>();
    /// first 8 bytes is final mark, deciding if this is the last segment
    reader.read(&final_mark, sizeof(final_mark));

    /// second 8 bytes are meta recording compressed binary size of index
    reader.seekg(sizeof(int64_t));
    int64_t binary_length;
    reader.read(&binary_length, sizeof(binary_length));
    LOG_INFO(log, "[readPart] compressed data size: {}", binary_length);

    /// third 8 bytes are meta recording uncompressed binary size of index
    reader.seekg(sizeof(int64_t) * 2);
    int64_t binary_length_original;
    reader.read(&binary_length_original, sizeof(binary_length_original));
    buffer.resize(binary_length_original + COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER);
    LOG_INFO(log, "[readPart] uncompressed data size: {}", binary_length_original);

    /// fourth 8 bytes records total vectors stored, this is repeated many times. Could be d, or not.
    reader.seekg(sizeof(int64_t) * 3);
    int64_t total_vec_bin;
    reader.read(&total_vec_bin, sizeof(total_vec_bin));
    LOG_INFO(log, "[readPart] total vectors: {}", total_vec_bin);

    /// finally we have the compressed binaries
    reader.seekg(sizeof(int64_t) * 4);
    index_binary_compressed->data = new uint8_t[binary_length + COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER];
    index_binary_compressed->size = binary_length;

    reader.read(index_binary_compressed->data, binary_length);

    validateAndDecompress(index_binary_compressed, binary_length_original, buffer.data());

    current_buffer_start = current_loaded_size;
    current_loaded_size += binary_length_original;
    LOG_INFO(log, "[readPart] current_buffer_start: {}, current_loaded_size: {}", current_buffer_start, current_loaded_size);

    if (final_mark && current_loaded_size != original_binary_size)
    {
        LOG_ERROR(log, "current_loaded_size {} != original_binary_size {}", current_loaded_size, original_binary_size);
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "current_loaded_size != original_binary_size");
    }
}

size_t CompositeIndexReader::operator()(void * ptr, size_t size, size_t nitems)
{
    size_t to_read = size * nitems;
    size_t ret = 0;

    while (offset + to_read > static_cast<size_t>(current_loaded_size))
    {
        if (offset < static_cast<size_t>(current_loaded_size))
        {
            size_t len = current_loaded_size - offset;
            LOG_DEBUG(log, "offset: {}, len: {}, to_read: {}", offset, len, to_read);
            memcpy(ptr, &buffer[offset - current_buffer_start], len);
            offset += len;
            ret += len;
            to_read -= len;
            ptr = static_cast<char *>(ptr) + len;
        }
        read_part();
        if (final_mark)
            break;
    }

    if (offset + to_read <= static_cast<size_t>(current_loaded_size))
    {
        LOG_DEBUG(log, "offset: {}, to_read: {}", offset, to_read);
        memcpy(ptr, &buffer[offset - current_buffer_start], to_read);
        offset += to_read;
        ret += to_read;
    }
    if (ret == size * nitems)
        return nitems;
    else
    {
        LOG_WARNING(log, "ret {} != size {} * nitems {}", ret, size, nitems);
        return ret / size;
    }
}
}
