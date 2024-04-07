#pragma once
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>

namespace DB
{

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges);

}
