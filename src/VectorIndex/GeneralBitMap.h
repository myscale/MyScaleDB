#pragma once

#include <cstring>
#include <memory>
#include <Common/logger_useful.h>
namespace VectorIndex
{
class GeneralBitMap
{
    /// The General form of bitmap which will be used by Clickhouse,
    /// since any user-defined index form may have its own bitmap format,
    /// the user shall define the convertInnerBitMap.
    /// The bitmap marks available items as 1, unavailable items as 0.
public:
    GeneralBitMap() = default;

    explicit GeneralBitMap(int64_t size_)
    {
        size = size_;
        int bytes_count = (size >> 3) + 1; // size/8 = bytes
        bitmap = new char[bytes_count];
        memset(bitmap, 0, bytes_count);
    }

    int32_t get_size() { return size; }

    bool test(int32_t id)
    {
        // TODO should we verify id <=size ?
        return (bitmap[id >> 3] & (0x1 << (id & 0x7)));
    }

    void set(int32_t id) { bitmap[id >> 3] |= (0x1 << (id & 0x7)); }

    void unset(int64_t id) { bitmap[id >> 3] &= ~(0x1 << (id & 0x7)); }

    /// Set bit corresponding to a region, this is much faster than set(),
    /// but can introduce up to 14 items been wrongly set at the margins.
    void set_range(int64_t start_id, int64_t end_id)
    {
        int64_t range = (end_id - start_id) / 8 + 1;
        int64_t start_byte = start_id / 8;
        memset(bitmap + start_byte, 255, range);
    }

    /// There are two inner bitmap in faiss and hnsw, they mimic this
    /// structure but don't free that char* at deallocation; rather, it's freed by this
    /// bitmap at the outer layer.
    ~GeneralBitMap()
    {
        if (bitmap)
        {
            LOG_DEBUG(&Poco::Logger::get("GeneralBitMap"), "delete bitmap");
            delete[] bitmap;
        }
    }

    char * bitmap;
    int32_t size;
};

using GeneralBitMapPtr = std::shared_ptr<GeneralBitMap>;
}
