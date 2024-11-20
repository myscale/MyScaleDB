#include <queue>
#include <VectorIndex/Storages/MergeTreeBaseSearchManager.h>
#include <VectorIndex/Common/VICommon.h>

namespace DB
{

/// TODO: remove duplicated code in
void MergeTreeBaseSearchManager::mergeSearchResultImpl(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    CommonSearchResultPtr tmp_result,
    const ColumnUInt64 * part_offset)
{
    LoggerPtr log = getLogger("MergeTreeBaseSearchManager");

    OpenTelemetry::SpanHolder span("MergeTreeBaseSearchManager::mergeSearchResultImpl()");
    const ColumnUInt32 * label_column = checkAndGetColumn<ColumnUInt32>(tmp_result->result_columns[0].get());
    const ColumnFloat32 * distance_column = checkAndGetColumn<ColumnFloat32>(tmp_result->result_columns[1].get());

    if (!label_column)
    {
        LOG_DEBUG(log, "Label colum is null");
    }

    /// Initialize was_result_processed
    if (was_result_processed.size() == 0)
        was_result_processed.assign(label_column->size(), false);

    auto final_distance_column = DataTypeFloat32().createColumn();

    /// create new column vector to save final results
    MutableColumns final_result;
    LOG_DEBUG(log, "Create final result");
    for (auto & col : pre_result)
    {
        final_result.emplace_back(col->cloneEmpty());
    }

    if (part_offset == nullptr)
    {
        LOG_DEBUG(log, "No part offset");
        size_t start_pos = 0;
        size_t end_pos = 0;
        size_t prev_row_num = 0;

        for (auto & read_range : read_ranges)
        {
            start_pos = read_range.start_row;
            end_pos = read_range.start_row + read_range.row_num;
            for (size_t ind = 0; ind < label_column->size(); ++ind)
            {
                if (was_result_processed[ind])
                    continue;

                const UInt64 label_value = label_column->getUInt(ind);
                if (label_value >= start_pos && label_value < end_pos)
                {
                    const size_t index_of_arr = label_value - start_pos + prev_row_num;
                    for (size_t i = 0; i < final_result.size(); ++i)
                    {
                        Field field;
                        pre_result[i]->get(index_of_arr, field);
                        final_result[i]->insert(field);
                    }

                    final_distance_column->insert(distance_column->getFloat32(ind));

                    was_result_processed[ind] = true;
                }
            }
            prev_row_num += read_range.row_num;
        }
    }
    else if (part_offset->size() > 0)
    {
        LOG_DEBUG(log, "Get part offset");

        /// When lightweight delete applied, the rowid in the label column cannot be used as index of pre_result.
        /// Match the rowid in the value of label col and the value of part_offset to find the correct index.
        const ColumnUInt64::Container & offset_raw_value = part_offset->getData();
        size_t part_offset_size = part_offset->size();

        size_t start_pos = 0;
        size_t end_pos = 0;

        for (auto & read_range : read_ranges)
        {
            start_pos = read_range.start_row;
            end_pos = read_range.start_row + read_range.row_num;

            for (size_t ind = 0; ind < label_column->size(); ++ind)
            {
                if (was_result_processed[ind])
                    continue;

                const UInt64 label_value = label_column->getUInt(ind);

                /// Check if label_value inside this read range
                if (label_value < start_pos || (label_value >= end_pos))
                    continue;

                /// read range doesn't consider LWD, hence start_row and row_num in read range cannot be used in this case.
                int low = 0;
                int mid;
                if (part_offset_size - 1 > static_cast<size_t>(std::numeric_limits<int>::max()))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of part_offset_size exceeds the limit of int data type");
                int high = static_cast<int>(part_offset_size - 1);

                /// label_value (row id) = part_offset.
                /// We can use binary search to quickly locate part_offset for current label.
                while (low <= high)
                {
                    mid = low + (high - low) / 2;

                    if (label_value == offset_raw_value[mid])
                    {
                        /// Use the index of part_offset to locate other columns in pre_result and fill final_result.
                        for (size_t i = 0; i < final_result.size(); ++i)
                        {
                            Field field;
                            pre_result[i]->get(mid, field);
                            final_result[i]->insert(field);
                        }

                        final_distance_column->insert(distance_column->getFloat32(ind));

                        was_result_processed[ind] = true;

                        /// break from binary search loop
                        break;
                    }
                    else if (label_value > offset_raw_value[mid])
                        low = mid + 1;
                    else
                        high = mid - 1;
                }
            }
        }
    }

    for (size_t i = 0; i < pre_result.size(); ++i)
    {
        pre_result[i] = std::move(final_result[i]);
    }
    read_rows = final_distance_column->size();

    pre_result.emplace_back(std::move(final_distance_column));
}

ScoreWithPartIndexAndLabels MergeTreeBaseSearchManager::getTotalTopKVSResult(
    const VectorAndTextResultInDataParts & vector_results,
    const size_t vec_res_index,
    const VSDescription & vector_scan_desc,
    LoggerPtr log)
{
    bool desc_direction = vector_scan_desc.direction == -1;
    int top_k = vector_scan_desc.topk > 0 ? vector_scan_desc.topk : VectorIndex::DEFAULT_TOPK;

    LOG_TRACE(log, "Total top k vector scan results for {} with size {}", vector_scan_desc.column_name, top_k);

    return getTotalTopSearchResultImpl(vector_results, static_cast<UInt64>(top_k), desc_direction, log, true, vec_res_index);
}

ScoreWithPartIndexAndLabels MergeTreeBaseSearchManager::getTotalTopKTextResult(
    const VectorAndTextResultInDataParts & text_results,
    const TextSearchInfoPtr & text_info,
    LoggerPtr log)
{
    int top_k = text_info->topk;

    LOG_TRACE(log, "Total top k text results with size {}", top_k);

    return getTotalTopSearchResultImpl(text_results, static_cast<UInt64>(top_k), true, log, false);
}

ScoreWithPartIndexAndLabels MergeTreeBaseSearchManager::getTotalCandidateVSResult(
        const VectorAndTextResultInDataParts & parts_with_vector_text_result,
        const size_t vec_res_index,
        const VSDescription & vector_scan_desc,
        const UInt64 & num_reorder,
        LoggerPtr log)
{
    bool desc_direction = vector_scan_desc.direction == -1;

    LOG_TRACE(log, "Total candidate vector scan results for {} with size {}", vector_scan_desc.column_name, num_reorder);

    /// Get top num_reorder candidates: part index + label + score
    return getTotalTopSearchResultImpl(parts_with_vector_text_result, num_reorder, desc_direction, log, true, vec_res_index);
}

ScoreWithPartIndexAndLabels MergeTreeBaseSearchManager::getTotalTopSearchResultImpl(
    const VectorAndTextResultInDataParts & vector_text_results,
    const UInt64 & top_k,
    const bool & desc_direction,
    LoggerPtr log,
    const bool need_vector,
    const size_t vec_res_index)
{
    String search_name = need_vector ? "vector scan" : "text search";

    /// Sort search results from selected parts based on score to get total top-k result.
    std::multimap<Float32, ScoreWithPartIndexAndLabel> sorted_score_with_index_labels;

    for (const auto & mix_results_in_part : vector_text_results)
    {
        /// part + top-k result in part
        CommonSearchResultPtr search_result;
        if (need_vector)
        {
            /// Support multiple distance functions
            if (vec_res_index < mix_results_in_part.vector_scan_results.size())
                search_result = mix_results_in_part.vector_scan_results[vec_res_index];
        }
        else
            search_result = mix_results_in_part.text_search_result;

        const auto & part_index = mix_results_in_part.part_index;

        if (search_result && search_result->computed)
        {
            const ColumnUInt32 * label_col = checkAndGetColumn<ColumnUInt32>(search_result->result_columns[0].get());
            const ColumnFloat32 * score_col = checkAndGetColumn<ColumnFloat32>(search_result->result_columns[1].get());

            if (!label_col)
            {
                LOG_DEBUG(log, "getTotalTopKSearchResult: label column in {} result is null", search_name);
            }
            else if (!score_col)
            {
                LOG_DEBUG(log, "getTotalTopKSearchResult: score column in {} result is null", search_name);
            }
            else
            {
                /// Store search result (label_id, score) + part_index
                for (size_t idx = 0; idx < label_col->size(); idx++)
                {
                    auto label_id = label_col->getElement(idx);
                    auto score = score_col->getFloat32(idx);

                    ScoreWithPartIndexAndLabel score_with_index(score, part_index, label_id);
                    sorted_score_with_index_labels.emplace(score, score_with_index);
                }
            }
        }
    }

    ScoreWithPartIndexAndLabels result_score_with_index_labels;
    result_score_with_index_labels.reserve(top_k);

    UInt64 count = 0;

    /// Reverse iteration from the end (the largest)
    if (desc_direction)
    {
        for (auto rit = sorted_score_with_index_labels.rbegin(); rit != sorted_score_with_index_labels.rend(); ++rit)
        {
            LOG_TRACE(log, "part_index={}, label_id={}, score={}", rit->second.part_index,
                    rit->second.label_id, rit->second.score);

            result_score_with_index_labels.emplace_back(rit->second);
            count++;

            if (count == top_k)
                break;
        }
    }
    else
    {
        for (const auto & [_, score_with_index_label] : sorted_score_with_index_labels)
        {
            LOG_TRACE(log, "part_index={}, label_id={}, score={}", score_with_index_label.part_index,
                    score_with_index_label.label_id, score_with_index_label.score);

            result_score_with_index_labels.emplace_back(score_with_index_label);
            count++;

            if (count == top_k)
                break;
        }
    }

    return result_score_with_index_labels;
}

std::set<UInt64> MergeTreeBaseSearchManager::getLabelsInSearchResults(
    const VectorAndTextResultInDataPart & mix_results,
    LoggerPtr log)
{
    OpenTelemetry::SpanHolder span("MergeTreeBaseSearchManager::getLabelsInSearchResults()");
    std::set<UInt64> label_ids;

    TextSearchResultPtr text_result = mix_results.text_search_result;

    if (text_result && text_result->computed)
        getLabelsInSearchResult(label_ids, text_result, log);

    /// Support multiple distance functions
    for (const auto & vector_scan_result : mix_results.vector_scan_results)
    {
        if (vector_scan_result && vector_scan_result->computed)
            getLabelsInSearchResult(label_ids, vector_scan_result, log);
    }

    return label_ids;
}

void MergeTreeBaseSearchManager::getLabelsInSearchResult(
    std::set<UInt64> & label_ids,
    const CommonSearchResultPtr & search_result,
    LoggerPtr log)
{
    if (!search_result || !search_result->computed)
        return;

    const ColumnUInt32 * label_col = checkAndGetColumn<ColumnUInt32>(search_result->result_columns[0].get());

    if (!label_col)
    {
        LOG_DEBUG(log, "getLabelsInSearchResult: label column in search result is null");
    }
    else
    {
        /// Store label_id
        for (size_t idx = 0; idx < label_col->size(); idx++)
        {
            auto label_id = label_col->getElement(idx);
            label_ids.emplace(label_id);
        }
    }
}

void MergeTreeBaseSearchManager::filterSearchResultsByFinalLabels(
    VectorAndTextResultInDataPart & mix_results,
    std::set<UInt64> & label_ids,
    LoggerPtr log)
{
    LOG_DEBUG(log, "filterSearchResultsByFinalLabels: part = {}", mix_results.data_part->name);

    if (label_ids.empty())
    {
        mix_results.text_search_result = nullptr;
        mix_results.vector_scan_results.clear();
        return;
    }

    TextSearchResultPtr text_result = mix_results.text_search_result;

    if (text_result && text_result->computed)
        mix_results.text_search_result = filterSearchResultByFinalLabels(text_result, label_ids, log);

    /// Support multiple distance functions
    for (auto & vector_scan_result : mix_results.vector_scan_results)
    {
        if (vector_scan_result && vector_scan_result->computed)
            vector_scan_result = filterSearchResultByFinalLabels(vector_scan_result, label_ids, log);
    }
}

CommonSearchResultPtr MergeTreeBaseSearchManager::filterSearchResultByFinalLabels(
    const CommonSearchResultPtr & pre_search_result,
    std::set<UInt64> & label_ids,
    LoggerPtr log)
{
    if (!pre_search_result || !pre_search_result->computed)
        return nullptr;

    const ColumnUInt32 * pre_label_col = checkAndGetColumn<ColumnUInt32>(pre_search_result->result_columns[0].get());
    const ColumnFloat32 * pre_score_col = checkAndGetColumn<ColumnFloat32>(pre_search_result->result_columns[1].get());

    if (!pre_label_col)
    {
        LOG_DEBUG(log, "filterSearchResult: label column in result is null");
        return nullptr;
    }
    else if (!pre_score_col)
    {
        LOG_DEBUG(log, "filterSearchResult: score column in result is null");
        return nullptr;
    }

    CommonSearchResultPtr final_result = std::make_shared<CommonSearchResult>();

    final_result->result_columns.resize(2);
    auto res_score_column = DataTypeFloat32().createColumn();
    auto res_label_column = DataTypeUInt32().createColumn();

    /// Remove labels in pre_label_col if not exists in final label ids.
    for (size_t idx = 0; idx < pre_label_col->size(); idx++)
    {
        auto label_id = pre_label_col->getElement(idx);

        if (label_ids.contains(label_id))
        {
            res_label_column->insert(label_id);
            res_score_column->insert(pre_score_col->getFloat32(idx));
        }
    }

    if (res_label_column->size() > 0)
    {
        final_result->computed = true;
        final_result->result_columns[0] = std::move(res_label_column);
        final_result->result_columns[1] = std::move(res_score_column);

        /// Add column name
        final_result->name = pre_search_result->name;
    }

    return final_result;
}

}
