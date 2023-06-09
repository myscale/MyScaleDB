#include <pdqsort.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeVectorScanUtils.h>

namespace DB
{
static String getMetricTypeFromVectorScanDescriptions(const VectorScanDescriptions & vector_scan_descs)
{
    if (vector_scan_descs.empty())
    {
        return String();
    }

    return vector_scan_descs[0].vector_parameters->getValue<String>("metric_type");
}

void mergeDataPartsBatchResult(RangesInDataParts & parts_with_ranges, int top_k, const VectorScanDescriptions & vector_scan_descs)
{
    /// (vector_id_num, (distance, data_part_name, label))
    std::unordered_map<UInt64, std::vector<std::tuple<float, String, uint32_t>>> all_batch_result;
    std::unordered_map<String, RangesInDataPart> part_map;

    for (auto & part_with_ranges : parts_with_ranges)
    {
        VectorScanResultPtr result = part_with_ranges.vector_scan_manager->getVectorScanResult();
        const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(result->result_columns[0].get());
        const ColumnUInt32 * vector_id_column = checkAndGetColumn<ColumnUInt32>(result->result_columns[1].get());
        const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(result->result_columns[2].get());
        for (size_t ind = 0; ind < label_column->size(); ++ind)
        {
            all_batch_result[vector_id_column->getUInt(ind)].emplace_back(
                make_tuple(distance_column->getFloat32(ind), part_with_ranges.data_part->name, label_column->getUInt(ind)));
        }
        part_map[part_with_ranges.data_part->name] = part_with_ranges;
        part_with_ranges.vector_scan_manager->eraseResult();
    }

    auto comparator = getMetricTypeFromVectorScanDescriptions(vector_scan_descs) == "IP"
        ? [](const std::tuple<float, String, uint32_t> & lhs, const std::tuple<float, String, uint32_t> & rhs)
    { return std::get<0>(lhs) > std::get<0>(rhs); }
        : [](const std::tuple<float, String, uint32_t> & lhs, const std::tuple<float, String, uint32_t> & rhs)
    { return std::get<0>(lhs) < std::get<0>(rhs); };

    for (auto & kv : all_batch_result)
    {
        auto & vector_id = kv.first;
        auto & result = kv.second;
        pdqsort(result.begin(), result.end(), comparator);
        for (size_t i = 0; i < std::min(static_cast<size_t>(top_k), result.size()); ++i)
        {
            RangesInDataPart & p = part_map[get<1>(result[i])];
            p.vector_scan_manager->getVectorScanResult()->result_columns[0]->insert(get<2>(result[i]));
            p.vector_scan_manager->getVectorScanResult()->result_columns[1]->insert(vector_id);
            p.vector_scan_manager->getVectorScanResult()->result_columns[2]->insert(get<0>(result[i]));
        }
    }
}
void mergeDataPartsResult(RangesInDataParts & parts_with_ranges, int top_k, const VectorScanDescriptions & vector_scan_descs)
{
    /// distance, data_part_name, label
    std::vector<std::tuple<float, String, uint32_t>> all_result;
    std::unordered_map<String, RangesInDataPart> part_map;
    Poco::Logger const * log = &Poco::Logger::get("MergeTreeVectorScanUtils");
    LOG_DEBUG(log, "parts_with_ranges size: {}", parts_with_ranges.size());

    for (auto & part_with_ranges : parts_with_ranges)
    {
        VectorScanResultPtr result = part_with_ranges.vector_scan_manager->getVectorScanResult();
        if (!result)
        {
            LOG_DEBUG(log, "Result is empty");
            continue;
        }
        const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(result->result_columns[0].get());
        const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(result->result_columns[1].get());
        for (size_t ind = 0; ind < label_column->size(); ++ind)
        {
            all_result.emplace_back(
                make_tuple(distance_column->getFloat32(ind), part_with_ranges.data_part->name, label_column->getUInt(ind)));
        }
        part_map[part_with_ranges.data_part->name] = part_with_ranges;
        part_with_ranges.vector_scan_manager->eraseResult();
    }

    auto comparator = getMetricTypeFromVectorScanDescriptions(vector_scan_descs) == "IP"
        ? [](const std::tuple<float, String, uint32_t> & lhs, const std::tuple<float, String, uint32_t> & rhs)
    { return std::get<0>(lhs) > std::get<0>(rhs); }
        : [](const std::tuple<float, String, uint32_t> & lhs, const std::tuple<float, String, uint32_t> & rhs)
    { return std::get<0>(lhs) < std::get<0>(rhs); };

    pdqsort(all_result.begin(), all_result.end(), comparator);

    for (size_t i = 0; i < std::min(static_cast<size_t>(top_k), all_result.size()); ++i)
    {
        RangesInDataPart & p = part_map[get<1>(all_result[i])];
        p.vector_scan_manager->getVectorScanResult()->result_columns[0]->insert(get<2>(all_result[i]));
        p.vector_scan_manager->getVectorScanResult()->result_columns[1]->insert(get<0>(all_result[i]));
    }

/*
    for (auto & part_with_ranges : parts_with_ranges)
    {
        LOG_DEBUG(
            &Poco::Logger::get("MergeTreeVectorScanUtils"),
            "[mergeDataPartsResult] after merge, part {}, vector result size: {}",
            part_with_ranges.data_part->name,
            part_with_ranges.vector_scan_manager->getVectorScanResult()->result_columns[0]->size());
    }
*/
}

void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeVectorScanManagerPtr vector_scan_mgr, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesByVectorScanResult()");
    MarkRanges res;
    // bool has_final_mark = part->index_granularity.hasFinalMark();
    size_t marks_count = part->index_granularity.getMarksCount();
    /// const auto & index = part->index;
    /// marks_count should not be 0 if we reach here

    auto settings = vector_scan_mgr->getSettings();

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

        auto result = vector_scan_mgr->getVectorScanResult();

        const ColumnUInt32 * label_column
            = checkAndGetColumn<ColumnUInt32>(vector_scan_mgr->getVectorScanResult()->result_columns[0].get());
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

void filterPartsMarkRangesByVectorScanResult(
    RangesInDataParts & parts_with_ranges, const VectorScanDescriptions & vector_scan_descs)
{
    int top_k = parts_with_ranges.back().vector_scan_manager->getVectorScanResult()->top_k;
    bool is_batch = parts_with_ranges.back().vector_scan_manager->getVectorScanResult()->is_batch;

    if (is_batch)
    {
        mergeDataPartsBatchResult(parts_with_ranges, top_k, vector_scan_descs);
    }
    else
    {
        mergeDataPartsResult(parts_with_ranges, top_k, vector_scan_descs);
    }

    /// ref: MergeTreeDataSelectExecutor::markRangesFromPKRange

    for (size_t i = 0; i < parts_with_ranges.size(); ++i)
    {
        filterMarkRangesByVectorScanResult(parts_with_ranges[i].data_part, parts_with_ranges[i].vector_scan_manager, parts_with_ranges[i].ranges);
    }

    /// erase empty part
    for (auto it = parts_with_ranges.begin(); it != parts_with_ranges.end();)
    {
        if ((*it).ranges.empty())
            it = parts_with_ranges.erase(it);
        else
            ++it;
    }
}

}
