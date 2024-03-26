#pragma once
#include "../../Storages/MergeTree/MergeTreeData.h"
#include "../../Storages/MergeTree/RangesInDataPart.h"
#include "../Storages/MergeTreeVSManager.h"

namespace DB
{

/// if we has precompute vector scan result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeVSManagerPtr vector_scan_mgr, MarkRanges & mark_ranges);

}
