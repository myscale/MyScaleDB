#pragma once
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <VectorIndex/Storages/MergeTreeVSManager.h>

namespace DB
{

/// if we has precompute vector scan result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeVectorScanManagerPtr vector_scan_mgr, MarkRanges & mark_ranges);

void filterPartsMarkRangesByVectorScanResult(RangesInDataParts & parts_with_ranges, const VSDescriptions & vector_scan_descs);

void mergeDataPartsResult(RangesInDataParts & parts_with_ranges, int top_k, const VSDescriptions & vector_scan_descs);

void mergeDataPartsBatchResult(RangesInDataParts & parts_with_ranges, int top_k, const VSDescriptions & vector_scan_descs);

}
