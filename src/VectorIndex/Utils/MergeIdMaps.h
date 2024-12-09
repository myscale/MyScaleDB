#pragma once

#include <vector>
#include <functional>
#include <base/types.h>
#include <VectorIndex/Common/VICommon.h>
#include <Common/CurrentMetrics.h>

namespace DB
{
class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
}

namespace CurrentMetrics
{
    extern const Metric LoadedVectorIndexMemorySize;
}

namespace VectorIndex
{

using RowIds = std::vector<UInt64>;
using RowSource = std::vector<uint8_t>;
using RowIdsPtr = std::shared_ptr<RowIds>;
using RowSourcePtr = std::shared_ptr<RowSource>;
class MergeIdMaps;
using MergeIdMapsPtr = std::shared_ptr<MergeIdMaps>;

/// MergeIdMaps is a class that manages the mapping between the row IDs of the old and new parts.
class MergeIdMaps
{
public:
    MergeIdMaps() = default;
    ~MergeIdMaps() = default;

    void transferToNewRowIds(const UInt8 own_id, SearchResultPtr & result);
    void transferToOldRowIds(const UInt8 own_id, SearchResultPtr & result);
    const VIBitmapPtr getRealFilter(const UInt8 own_id, const VIBitmapPtr & filter);
    /// return real filters for specified own_ids
    const std::unordered_map<UInt8, VIBitmapPtr> getRealFilters(const VIBitmapPtr & filter);
    void initRealFilter(const UInt8 own_id, VIBitmapPtr & filter, const RowIds del_rows);
    void lazyInitOnce(const DB::IMergeTreeDataPart & data_part);
    bool isInited() const { return initialized; }
    static MergeIdMapsPtr loadFromDecouplePart(const DB::IMergeTreeDataPart & data_part);
    static void removeMergedMapsFiles(const DB::IMergeTreeDataPart & data_part);

private:
    /// part_name_without_mutation -> row_ids_map
    std::once_flag init_flag;
    bool initialized = false;
    /// owner_part_id -> row_ids map
    const std::unordered_map<UInt8, RowIdsPtr> row_ids_maps;
    const RowIdsPtr inverted_row_ids_map;
    const RowSourcePtr inverted_row_sources_map;

    CurrentMetrics::Increment merged_maps_memory{CurrentMetrics::LoadedVectorIndexMemorySize, 0};
};

}

