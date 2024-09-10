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

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <VectorIndex/Storages/MergeTreeHybridSearchManager.h>
#include <VectorIndex/Utils/VSUtils.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int ILLEGAL_COLUMN;
}

HybridSearchResultPtr getHybridResultFromScoresWithPartIndex(
    const ScoreWithPartIndexAndLabels & score_with_part_index_labels,
    const String & part_name,
    Poco::Logger * log)
{
    HybridSearchResultPtr tmp_hybrid_search_result = std::make_shared<CommonSearchResult>();

    tmp_hybrid_search_result->result_columns.resize(2);
    auto score_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    LOG_TRACE(log, "Search result for part {}:", part_name);
    for (const auto & score_with_part_index_label : score_with_part_index_labels)
    {
        const auto & label_id = score_with_part_index_label.label_id;
        const auto & score = score_with_part_index_label.score;

        LOG_TRACE(log, "Label: {}, score: {}", label_id, score);
        label_column->insert(label_id);
        score_column->insert(score);
    }

    if (label_column->size() > 0)
    {
        tmp_hybrid_search_result->computed = true;
        tmp_hybrid_search_result->result_columns[0] = std::move(label_column);
        tmp_hybrid_search_result->result_columns[1] = std::move(score_column);
    }

    return tmp_hybrid_search_result;
}

void MergeTreeHybridSearchManager::executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeHybridSearchManager::executeSearchBeforeRead");
    if (vector_scan_manager)
        vector_scan_manager->executeSearchBeforeRead(data_part);

    if (text_search_manager)
        text_search_manager->executeSearchBeforeRead(data_part);
}

void MergeTreeHybridSearchManager::executeSearchWithFilter(
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & read_ranges,
    const Search::DenseBitmapPtr filter)
{
    if (vector_scan_manager)
        vector_scan_manager->executeSearchWithFilter(data_part, read_ranges, filter);

    if (text_search_manager)
        text_search_manager->executeSearchWithFilter(data_part, read_ranges, filter);
}

ScoreWithPartIndexAndLabels MergeTreeHybridSearchManager::hybridSearch(
    const ScoreWithPartIndexAndLabels & vec_scan_result_with_part_index,
    const ScoreWithPartIndexAndLabels & text_search_result_with_part_index,
    const HybridSearchInfoPtr & hybrid_info,
    Poco::Logger * log)
{
    /// Get fusion type from hybrid_info
    String fusion_type = hybrid_info->fusion_type;

    /// Store result after fusion. (<part_index, label_id>, score)
    std::map<std::pair<size_t, UInt32>, Float32> part_index_labels_with_fusion_score;

    /// Relative Sore Fusion
    if (isRelativeScoreFusion(fusion_type))
    {
        LOG_DEBUG(log, "Use Relative Score Fusion");
        /// Get fusion weight, assume fusion_weight is handled by ExpressionAnalyzer
        float weight = hybrid_info->fusion_weight;
        /// Use direction in vector scan info
        int vec_scan_direction = hybrid_info->vector_scan_info ? hybrid_info->vector_scan_info->vector_scan_descs[0].direction : 1;
        RelativeScoreFusion(part_index_labels_with_fusion_score, vec_scan_result_with_part_index, text_search_result_with_part_index, weight, vec_scan_direction, log);
    }
    else
    {
        /// Reciprocal Rank Fusion
        LOG_DEBUG(log, "Use Reciprocal Rank Fusion");

        /// Assume fusion_k is handled by ExpressionAnalyzer
        int fusion_k = hybrid_info->fusion_k <= 0 ? 60 : hybrid_info->fusion_k;
        RankFusion(part_index_labels_with_fusion_score, vec_scan_result_with_part_index, text_search_result_with_part_index, fusion_k);
    }

    /// Sort hybrid search result based on fusion score and return top-k rows.
    LOG_TEST(log, "hybrid scores after fusion");
    std::multimap<Float32, std::pair<size_t, UInt32>, std::greater<Float32>> sorted_fusion_scores_with_part_index_label;
    for (const auto & [part_index_label_id, fusion_score] : part_index_labels_with_fusion_score)
    {
        LOG_TEST(log, "part_index={}, label_id={}, hybrid_score={}", part_index_label_id.first, part_index_label_id.second, fusion_score);
        sorted_fusion_scores_with_part_index_label.emplace(fusion_score, part_index_label_id);
    }

    /// Save topk part indexes, label ids and fusion score into hybrid_result.
    int topk = hybrid_info->topk;
    ScoreWithPartIndexAndLabels hybrid_result;

    int count = 0;
    for (const auto & [fusion_score, part_index_label_id] : sorted_fusion_scores_with_part_index_label)
    {
        hybrid_result.emplace_back(fusion_score, part_index_label_id.first, part_index_label_id.second);
        count++;

        if (count == topk)
            break;
    }

    return hybrid_result;
}

void MergeTreeHybridSearchManager::RelativeScoreFusion(
    std::map<std::pair<size_t, UInt32>, Float32> & part_index_labels_with_convex_score,
    const ScoreWithPartIndexAndLabels & vec_scan_result_with_part_index,
    const ScoreWithPartIndexAndLabels & text_search_result_with_part_index,
    const float weight_of_text,
    const int vector_scan_direction,
    Poco::Logger * log)
{
    /// min-max normalization on text search score
    std::vector<Float32> norm_score;
    norm_score.reserve(text_search_result_with_part_index.size());
    computeMinMaxNormScore(text_search_result_with_part_index, norm_score, log);

    LOG_TEST(log, "text bm25 scores:");
    /// final score = norm-BM25 * w + (1-w) * norm-distance
    for (size_t idx = 0; idx < text_search_result_with_part_index.size(); idx++)
    {
        const auto & text_score_with_part_index = text_search_result_with_part_index[idx];
        auto part_index_label_id = std::make_pair(text_score_with_part_index.part_index, text_score_with_part_index.label_id);

        LOG_TEST(log, "part_index={}, label_id={}, origin_score={}, norm_score={}",
                    text_score_with_part_index.part_index, text_score_with_part_index.label_id, text_score_with_part_index.score, norm_score[idx]);

        /// label_ids from text search are unique
        part_index_labels_with_convex_score[part_index_label_id] = norm_score[idx] * weight_of_text;
    }

    /// min-max normalization on text search score
    norm_score.clear();
    computeMinMaxNormScore(vec_scan_result_with_part_index, norm_score, log);

    LOG_TEST(log, "distance scores:");
    /// The Relative score fusion with distance score depends on the metric type.
    for (size_t idx = 0; idx < vec_scan_result_with_part_index.size(); idx++)
    {
        const auto & vec_score_with_part_index = vec_scan_result_with_part_index[idx];
        auto part_index_label_id = std::make_pair(vec_score_with_part_index.part_index, vec_score_with_part_index.label_id);

        LOG_TEST(log, "part_index={}, label_id={}, origin_score={}, norm_score={}",
                    vec_score_with_part_index.part_index, vec_score_with_part_index.label_id, vec_score_with_part_index.score, norm_score[idx]);

        Float32 fusion_score = 0;

        /// 1 - ascending, -1 - descending
        if (vector_scan_direction == -1)
            fusion_score = norm_score[idx] * (1 - weight_of_text);
        else
            fusion_score = (1 - weight_of_text) * (1 - norm_score[idx]);

        /// Insert or update score for label_id
        part_index_labels_with_convex_score[part_index_label_id] += fusion_score;
    }
}

void MergeTreeHybridSearchManager::computeMinMaxNormScore(
    const ScoreWithPartIndexAndLabels & search_result_with_part_index,
    std::vector<Float32> & norm_score_vec,
    Poco::Logger * log)
{
    const auto result_size = search_result_with_part_index.size();
    if (result_size == 0)
    {
        LOG_DEBUG(log, "search result is empty");
        return;
    }

    /// Here assume the scores in score column are ordered.
    /// Thus the min score and max score are the first and last.
    Float32 min_score, max_score, min_max_scale;
    min_score = search_result_with_part_index[0].score;
    max_score = search_result_with_part_index[result_size - 1].score;

    /// When min_score = max_score, norm_score = 1.0;
    if (min_score == max_score)
    {
        LOG_DEBUG(log, "max_score and min_score are equal");
        for (size_t idx = 0; idx < result_size; idx++)
            norm_score_vec.emplace_back(1.0);

        return;
    }
    else if (min_score > max_score) /// DESC
    {
        Float32 tmp_score = min_score;
        min_score = max_score;
        max_score = tmp_score;
    }

    min_max_scale = max_score - min_score;

    /// min-max normalization score = (score - min_score) / (max_score - min_score)
    for (size_t idx = 0; idx < result_size; idx++)
    {
        Float32 norm_score = (search_result_with_part_index[idx].score - min_score) / min_max_scale;
        norm_score_vec.emplace_back(norm_score);
    }
}

void MergeTreeHybridSearchManager::RankFusion(
    std::map<std::pair<size_t, UInt32>, Float32> & part_index_labels_with_ranked_score,
    const ScoreWithPartIndexAndLabels & vec_scan_result_with_part_index,
    const ScoreWithPartIndexAndLabels & text_search_result_with_part_index,
    int k)
{
    /// Ranked score = 1.0 / (k + rank(label_id))
    size_t idx = 0;
    for (const auto & score_with_part_index_label : vec_scan_result_with_part_index)
    {
        Float32 rank_score = 1.0f / (k + idx + 1);
        auto part_index_label = std::make_pair(score_with_part_index_label.part_index, score_with_part_index_label.label_id);

        /// For new (part_index, label_id) pair, map will insert.
        /// part_index_labels_with_ranked_score map saved the fusion score for a (part_index, label_id) pair.
        part_index_labels_with_ranked_score[part_index_label] += rank_score;

        idx++;
    }

    idx = 0;
    for (const auto & score_with_part_index_label : text_search_result_with_part_index)
    {
        Float32 rank_score = 1.0f / (k + idx + 1);
        auto part_index_label = std::make_pair(score_with_part_index_label.part_index, score_with_part_index_label.label_id);

        /// For new (part_index, label_id) pair, map will insert.
        /// part_index_labels_with_ranked_score map saved the fusion score for a (part_index, label_id) pair.
        part_index_labels_with_ranked_score[part_index_label] += rank_score;

        idx++;
    }
}

SearchResultAndRangesInDataParts MergeTreeHybridSearchManager::FilterPartsWithHybridResults(
    const VectorAndTextResultInDataParts & parts_with_vector_text_result,
    const ScoreWithPartIndexAndLabels & hybrid_result_with_part_index,
    const Settings & settings,
    Poco::Logger * log)
{
    /// Merge hybrid results from the same part index into a vector
    std::map<size_t, std::vector<ScoreWithPartIndexAndLabel>> part_index_merged_map;

    for (const auto & score_with_part_index_label : hybrid_result_with_part_index)
    {
        const auto & part_index = score_with_part_index_label.part_index;
        part_index_merged_map[part_index].emplace_back(score_with_part_index_label);
    }

    SearchResultAndRangesInDataParts parts_with_ranges_hybrid_result;

    /// Filter data part with part index in hybrid search and label ids for mark ranges
    for (const auto & mix_results_in_part : parts_with_vector_text_result)
    {
        const auto & part_with_ranges = mix_results_in_part.part_with_ranges;
        size_t part_index = part_with_ranges.part_index_in_query;

        /// Check if part_index exists in map
        if (part_index_merged_map.contains(part_index))
        {
            /// Found data part
            /// Construct hybrid search result
            auto tmp_hybrid_search_result = getHybridResultFromScoresWithPartIndex(
                                    part_index_merged_map[part_index], part_with_ranges.data_part->name, log);

            /// Filter mark ranges with label ids in hybrid search result
            MarkRanges mark_ranges_for_part = part_with_ranges.ranges;
            filterMarkRangesBySearchResult(part_with_ranges.data_part, settings, tmp_hybrid_search_result, mark_ranges_for_part);

            if (!mark_ranges_for_part.empty())
            {
                parts_with_ranges_hybrid_result.emplace_back(
                    part_with_ranges.data_part,
                    part_with_ranges.alter_conversions,
                    part_index,
                    std::move(mark_ranges_for_part),
                    tmp_hybrid_search_result);
            }
        }
    }

    return parts_with_ranges_hybrid_result;
}

void MergeTreeHybridSearchManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const Search::DenseBitmapPtr filter,
    const ColumnUInt64 * part_offset)
{
    mergeSearchResultImpl(pre_result, read_rows, read_ranges, hybrid_search_result, filter, part_offset);
}

}
