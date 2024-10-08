#pragma once
#include <Core/Types.h>
#include <base/hex.h>
#include <Core/UUID.h>

namespace DB
{

struct FileCacheKey
{
    UInt128 key;

    String toString() const { return getHexUIntLowercase(key); }

    FileCacheKey() = default;

    explicit FileCacheKey(const UInt128 & key_) : key(key_) { }

    static FileCacheKey random() { return FileCacheKey(UUIDHelpers::generateV4().toUnderType()); }

    bool operator==(const FileCacheKey & other) const { return key == other.key; }
};

}

namespace std
{
template <>
struct hash<DB::FileCacheKey>
{
    std::size_t operator()(const DB::FileCacheKey & k) const { return hash<UInt128>()(k.key); }
};

}
