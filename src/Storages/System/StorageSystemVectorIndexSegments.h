#pragma once


#include <Storages/IStorage.h>


namespace DB
{

/// Implement system.vector_index_segments table that contains information of vector indices in each data part
class StorageSystemVectorIndexSegments : public IStorage, boost::noncopyable
{
public:
    std::string getName() const override { return "StorageSystemVectorIndexSegments"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    explicit StorageSystemVectorIndexSegments(const StorageID & table_id_);
};

}
