#pragma once

#include <Core/Block.h>

namespace DB
{
enum class SkipIndexType
{
    TantivyIndex = 0,
    SparseIndex
};

inline const String & toSkipIndexName(const SkipIndexType & index_type)
{
    static const String & fts = "FTS";
    static const String & sparse = "Sparse";
    static const String & unknown = "Unknown";
    if (index_type == SkipIndexType::TantivyIndex)
    {
        return fts;
    }
    else if (index_type == SkipIndexType::SparseIndex)
    {
        return sparse;
    }
    return unknown;
}


}
