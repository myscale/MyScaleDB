#pragma once

#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <VectorIndex/Common/VICommon.h>

namespace DB
{

/**
 *  Used in conjunction with MergeTreeThreadSelectAlgorithm, set bitmap filter for a block during readFromTask().
 */
class MergeTreeThreadSelectWithFilterAlgorithm : public MergeTreeThreadSelectAlgorithm
{
public:
    MergeTreeThreadSelectWithFilterAlgorithm(
        size_t thread_idx_,
        VectorIndex::VIBitmapPtr filter_)
        : MergeTreeThreadSelectAlgorithm(thread_idx_)
        , filter(filter_)
    {
    }

    String getName() const override { return "MergeTreeThreadWithFilter"; }

protected:
    BlockAndProgress readFromTask(MergeTreeReadTask & task, const BlockSizeParams & params) override
    {
        BlockAndProgress res = MergeTreeThreadSelectAlgorithm::readFromTask(task, params);

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
