#pragma once

#include <Core/Block.h>

namespace DB
{

/**
 * @struct IndexFileMeta
 * @brief Represents the meta data for a pieces of index files. 
 */
struct IndexFileMeta
{
    IndexFileMeta();

    IndexFileMeta(const String & file_name_, UInt64 offset_begin_, UInt64 offset_end_);

    bool operator==(const IndexFileMeta & other) const;

    char file_name[512];
    UInt64 offset_begin;
    UInt64 offset_end;
};


using IndexFileMetas = std::vector<IndexFileMeta>;

}
