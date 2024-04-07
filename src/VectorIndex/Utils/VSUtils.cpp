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

#include <pdqsort.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <VectorIndex/Utils/VSUtils.h>

namespace DB
{

void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesByVectorScanResult()");
    MarkRanges res;

    if (!base_search_mgr->preComputed())
    {
        mark_ranges = res;
        return;
    }

    size_t marks_count = part->index_granularity.getMarksCount();
    /// const auto & index = part->index;
    /// marks_count should not be 0 if we reach here

    auto settings = base_search_mgr->getSettings();

    size_t min_marks_for_seek = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_seek,
        settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    auto need_this_range = [&](MarkRange & range)
    {
        auto begin = range.begin;
        auto end = range.end;
        auto start_row = part->index_granularity.getMarkStartingRow(begin);
        auto end_row = start_row + part->index_granularity.getRowsCountInRange(range);

        auto result = base_search_mgr->getSearchResult();

        const ColumnUInt32 * label_column
            = checkAndGetColumn<ColumnUInt32>(result->result_columns[0].get());
        for (size_t ind = 0; ind < label_column->size(); ++ind)
        {
            auto label = label_column->getUInt(ind);
            if (label >= start_row && label < end_row)
            {
                LOG_TRACE(
                    &Poco::Logger::get("MergeTreeVectorScanUtils"),
                    "Keep range: {}-{} in part: {}",
                    begin,
                    end,
                    part->name);
                return true;
            }
        }
        return false;
    };

    std::vector<MarkRange> ranges_stack = {{0, marks_count}};

    while (!ranges_stack.empty())
    {
        MarkRange range = ranges_stack.back();
        ranges_stack.pop_back();

        if (!need_this_range(range))
            continue;

        if (range.end == range.begin + 1)
        {
            if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                res.push_back(range);
            else
                res.back().end = range.end;
        }
        else
        {
            /// Break the segment and put the result on the stack from right to left.
            size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
            size_t end;

            for (end = range.end; end > range.begin + step; end -= step)
                ranges_stack.emplace_back(end - step, end);

            ranges_stack.emplace_back(range.begin, end);
        }
    }

    mark_ranges = res;
}

}
