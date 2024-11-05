#pragma once

#include <Storages/MergeTree/MergeTreeThreadSelectProcessor.h>
#include <VectorIndex/Common/VICommon.h>

namespace DB
{

/**
 *  Used in conjunction with MergeTreeThreadSelectAlgorithm, set bitmap filter for a block during readFromPart().
 */
class MergeTreeThreadSelectWithFilterAlgorithm : public MergeTreeThreadSelectAlgorithm
{
public:
    MergeTreeThreadSelectWithFilterAlgorithm(
        size_t thread_,
        MergeTreeReadPoolPtr pool_,
        size_t min_marks_to_read_,
        size_t max_block_size_,
        size_t preferred_block_size_bytes_,
        size_t preferred_max_column_in_block_size_bytes_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        bool use_uncompressed_cache_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & virt_column_names_,
        VectorIndex::VIBitmapPtr filter_)
        : MergeTreeThreadSelectAlgorithm(thread_, pool_, min_marks_to_read_, max_block_size_,
            preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
            storage_, storage_snapshot_, use_uncompressed_cache_,
            prewhere_info_, actions_settings_, reader_settings_, virt_column_names_)
        , filter(filter_)
    {
    }

    String getName() const override { return "MergeTreeThreadWithFilter"; }

protected:
    BlockAndProgress readFromPart() override
    {
        BlockAndProgress res = MergeTreeThreadSelectAlgorithm::readFromPart();

        Block & block = res.block;
        if (block)
        {
            const auto * column = block.getByName("_part_offset").column.get();

            if (const auto * column_uint64 = checkAndGetColumn<ColumnUInt64>(column))
            {
                const auto & offsets = column_uint64->getData();
                for (const auto offset : offsets)
                {
                    filter->set(offset);
                }
            }
        }

        return res;
    }

private:
    VectorIndex::VIBitmapPtr filter;
};

}
