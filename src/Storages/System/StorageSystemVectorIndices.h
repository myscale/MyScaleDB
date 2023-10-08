#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/IStorage.h>


namespace DB
{

/// For system.vector_indices table - describes the vector indices in tables, similar to system.columns and system.data_skipping_indices.
class StorageSystemVectorIndices : public IStorage, boost::noncopyable
{
public:
    std::string getName() const override { return "StorageSystemVectorIndices"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    StorageSystemVectorIndices(const StorageID & table_id_);
    bool isSystemStorage() const override { return true; }
};

}
