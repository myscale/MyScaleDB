#pragma once
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>

namespace DB
{

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges);

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesBySearchResult(MergeTreeData::DataPartPtr part, const Settings & settings, CommonSearchResultPtr common_search_result, MarkRanges & mark_ranges);

/// if we has labels from precompute search result, use it to filter mark ranges
void filterMarkRangesByLabels(MergeTreeData::DataPartPtr part, const Settings & settings, const std::set<UInt64> labels, MarkRanges & mark_ranges);

}
