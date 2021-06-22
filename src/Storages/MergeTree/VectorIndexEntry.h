#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

class StorageMergeTree;

struct VectorIndexEntry
{
    std::vector<MergeTreeDataPartPtr> data_parts;

    VectorIndexEntry(const std::vector<MergeTreeDataPartPtr> & data_parts_) : data_parts(std::move(data_parts_)) { }
};

using VectorIndexEntryPtr = std::shared_ptr<VectorIndexEntry>;

}
