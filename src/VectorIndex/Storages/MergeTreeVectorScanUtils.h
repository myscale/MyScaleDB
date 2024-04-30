/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <VectorIndex/Storages/MergeTreeVectorScanManager.h>

namespace DB
{

/// if we has precompute vector scan result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeVectorScanManagerPtr vector_scan_mgr, MarkRanges & mark_ranges);

void filterPartsMarkRangesByVectorScanResult(RangesInDataParts & parts_with_ranges, const VectorScanDescriptions& vector_scan_descs);

void mergeDataPartsResult(RangesInDataParts & parts_with_ranges, int top_k, const VectorScanDescriptions& vector_scan_descs);

void mergeDataPartsBatchResult(RangesInDataParts & parts_with_ranges, int top_k, const VectorScanDescriptions& vector_scan_descs);

}
