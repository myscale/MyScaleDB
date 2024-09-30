#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

/// Deletes rows according to table TTL description with
/// possible optional condition in 'WHERE' clause.
class TTLDeleteAlgorithm final : public ITTLAlgorithm
{
public:
    TTLDeleteAlgorithm(
        const TTLDescription & description_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_,
        bool force_,
        std::shared_ptr<std::unordered_set<UInt64>> ttl_delete_row_ids_
    );

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;
    size_t getNumberOfRemovedRows() const { return rows_removed; }

private:
    size_t rows_removed = 0;
    size_t part_row_id = 0;
    std::shared_ptr<std::unordered_set<UInt64>> ttl_delete_row_ids;
};

}
