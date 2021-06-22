#pragma once
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

/// if we has precompute vector scan result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(RangesInDataParts & parts_with_ranges, const VectorScanDescriptions& vector_scan_descs, const Settings & settings);

void mergeDataPartsResult(RangesInDataParts & parts_with_ranges, int top_k, const VectorScanDescriptions& vector_scan_descs);

void mergeDataPartsBatchResult(RangesInDataParts & parts_with_ranges, int top_k, const VectorScanDescriptions& vector_scan_descs);

}
