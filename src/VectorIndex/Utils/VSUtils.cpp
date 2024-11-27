#include <pdqsort.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <VectorIndex/Utils/VSUtils.h>

#include <VectorIndex/Common/VICommon.h>

namespace DB
{

void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesByVectorScanResult()");
    MarkRanges res;

    if (!base_search_mgr || !base_search_mgr->preComputed())
    {
        mark_ranges = res;
        return;
    }

    filterMarkRangesBySearchResult(part, base_search_mgr->getSettings(), base_search_mgr->getSearchResult(), mark_ranges);
}

void filterMarkRangesBySearchResult(MergeTreeData::DataPartPtr part, const Settings & settings, CommonSearchResultPtr common_search_result, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesBySearchResult()");
    MarkRanges res;

    if (!common_search_result || !common_search_result->computed)
    {
        mark_ranges = res;
        return;
    }

    size_t marks_count = part->index_granularity.getMarksCount();
    /// const auto & index = part->index;
    /// marks_count should not be 0 if we reach here

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

        const ColumnUInt32 * label_column
            = checkAndGetColumn<ColumnUInt32>(common_search_result->result_columns[0].get());
        for (size_t ind = 0; ind < label_column->size(); ++ind)
        {
            auto label = label_column->getUInt(ind);
            if (label >= start_row && label < end_row)
            {
                LOG_TRACE(
                    getLogger("MergeTreeVectorScanUtils"),
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

void filterMarkRangesByLabels(MergeTreeData::DataPartPtr part, const Settings & settings, const std::set<UInt64> labels, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesByLabels()");
    MarkRanges res;

    size_t marks_count = part->index_granularity.getMarksCount();
    /// marks_count should not be 0 if we reach here
    if (marks_count == 0)
        mark_ranges = res;

    size_t min_marks_for_seek = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_seek,
        settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    std::vector<UInt64> labels_vec(labels.size());
    for (const auto & label : labels)
        labels_vec.emplace_back(label);

    auto need_this_range = [&](MarkRange & range)
    {
        auto begin = range.begin;
        auto end = range.end;
        auto start_row = part->index_granularity.getMarkStartingRow(begin);
        auto end_row = start_row + part->index_granularity.getRowsCountInRange(range);

        /// Use binary search due to labels are sorted
        size_t low = 0;
        size_t high = labels_vec.size();
        while (low < high)
        {
            const size_t middle = low + (high - low) / 2;
            auto label_middle = labels_vec[middle];
            if (label_middle >= start_row && label_middle < end_row)
            {
                LOG_TRACE(
                    getLogger("filterMarkRangesByLabels"),
                    "Keep range: {}-{} in part: {}",
                    begin,
                    end,
                    part->name);
                return true;
            }
            else if (label_middle < start_row)
                low = middle + 1;
            else
                high = middle;
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

UInt64 getTopKFromLimit(const ASTSelectQuery * select_query, ContextPtr context, bool is_batch)
{
    UInt64 topk = 0;

    if (!select_query)
        return topk;

    /// topk for search is sum of length and offset in limit
    UInt64 length = 0, offset = 0;
    ASTPtr length_ast = nullptr;
    ASTPtr offset_ast = nullptr;

    if (is_batch)
    {
        /// LIMIT m OFFSET n BY expressions
        length_ast = select_query->limitByLength();
        offset_ast = select_query->limitByOffset();
    }
    else
    {
        /// LIMIT m OFFSET n
        length_ast = select_query->limitLength();
        offset_ast = select_query->limitOffset();
    }

    if (length_ast)
    {
        const auto & [field, type] = evaluateConstantExpression(length_ast, context);

        if (isNativeNumber(type))
        {
            Field converted = convertFieldToType(field, DataTypeUInt64());
            if (!converted.isNull())
                length = converted.safeGet<UInt64>();
        }
    }

    if (offset_ast)
    {
        const auto & [field, type] = evaluateConstantExpression(offset_ast, context);

        if (isNativeNumber(type))
        {
            Field converted = convertFieldToType(field, DataTypeUInt64());
            if (!converted.isNull())
                offset = converted.safeGet<UInt64>();
        }
    }

    topk = length + offset;

    /// Check when offset n is provided
    if (offset > 0 && topk > context->getSettingsRef().max_search_result_window)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sum of m and n in limit ({}) should not exceed `max_search_result_window`({})", topk, context->getSettingsRef().max_search_result_window);

    return topk;
}

VectorIndex::VIMetric getVSMetric(MergeTreeData::DataPartPtr part, const VSDescription & desc)
{
    String metric_name = "L2";
    bool found = false;
    auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
    for (const auto & vi_desc : metadata_snapshot->getVectorIndices())
    {
        if (vi_desc.column == desc.search_column_name)
        {
            if (vi_desc.parameters && vi_desc.parameters->has("metric_type"))
            {
                found = true;
                metric_name = vi_desc.parameters->getValue<String>("metric_type");
            }
            break;
        }
    }
    if (!found)
    {
        if (desc.vector_search_type == Search::DataType::FloatVector)
            metric_name = part->storage.getSettings()->float_vector_search_metric_type;
        else
            metric_name = part->storage.getSettings()->binary_vector_search_metric_type;
    }
    return Search::getMetricType(metric_name, desc.vector_search_type);
}

}
