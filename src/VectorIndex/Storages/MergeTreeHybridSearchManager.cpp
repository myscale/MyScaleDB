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

void MergeTreeHybridSearchManager::executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeHybridSearchManager::executeSearchBeforeRead");
    if (vector_scan_manager)
        vector_scan_manager->executeSearchBeforeRead(data_part);

    if (text_search_manager)
        text_search_manager->executeSearchBeforeRead(data_part);

    /// Combine top-k results from vector scan and text search
    hybrid_search_result = hybridSearch();
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

    /// Combine top-k results from vector scan and text search
    hybrid_search_result = hybridSearch();
}

HybridSearchResultPtr MergeTreeHybridSearchManager::hybridSearch()
{
    OpenTelemetry::SpanHolder span("MergeTreeHybridSearchManager::hybridSearch()");

    /// Get top-k search result from vector search
    VectorScanResultPtr vec_scan_result = nullptr;
    if (vector_scan_manager && vector_scan_manager->preComputed())
        vec_scan_result = vector_scan_manager->getSearchResult();

    /// Get top-k search result from text search
    TextSearchResultPtr text_search_result = nullptr;
    if (text_search_manager && text_search_manager->preComputed())
        text_search_result = text_search_manager->getSearchResult();

    /// Get fusion type from hybrid_search_info
    String fusion_type = hybrid_search_info->fusion_type;

    /// Store result after fusion
    std::map<UInt32, Float32> labels_with_fusion_score;

    /// Relative Sore Fusion
    if (isRelativeScoreFusion(fusion_type))
    {
        LOG_DEBUG(log, "Use Relative Score Fusion");
        /// Get fusion weight, assume fusion_weight is handled by ExpressionAnalyzer
        float weight = hybrid_search_info->fusion_weight;
        auto vec_index_metric = hybrid_search_info->metric_type;
        RelativeScoreFusion(labels_with_fusion_score, vec_scan_result, text_search_result, weight, vec_index_metric);
    }
    else
    {
        /// Reciprocal Rank Fusion
        LOG_DEBUG(log, "Use Reciprocal Rank Fusion");

        /// Assume fusion_k is handled by ExpressionAnalyzer
        int fusion_k = hybrid_search_info->fusion_k;
        RankFusion(labels_with_fusion_score, vec_scan_result, text_search_result, fusion_k);
    }

    /// Sort hybrid search result based on fusion score and return top-k rows.
    std::multimap<Float32, UInt32, std::greater<Float32>> sorted_fusion_scores_with_label;
    for (const auto & [label_id, fusion_score] : labels_with_fusion_score)
        sorted_fusion_scores_with_label.emplace(fusion_score, label_id);

    HybridSearchResultPtr tmp_hybrid_search_result = std::make_shared<CommonSearchResult>();

    tmp_hybrid_search_result->result_columns.resize(2);
    auto score_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    /// Save topk label ids and fusion score into label and score column.
    int topk = hybrid_search_info->topk;

    int count = 0;
    for (const auto & [fusion_score, label_id] : sorted_fusion_scores_with_label)
    {
        label_column->insert(label_id);
        score_column->insert(fusion_score);
        count++;

        if (count == topk)
            break;
    }

    if (label_column->size() > 0)
    {
        tmp_hybrid_search_result->computed = true;
        tmp_hybrid_search_result->result_columns[0] = std::move(label_column);
        tmp_hybrid_search_result->result_columns[1] = std::move(score_column);
    }

    return tmp_hybrid_search_result;
}

void MergeTreeHybridSearchManager::RelativeScoreFusion(
    std::map<UInt32, Float32> & labels_with_convex_score,
    const VectorScanResultPtr vec_scan_result,
    const TextSearchResultPtr text_search_result,
    const float weight_of_text,
    const Search::Metric vector_index_metric)
{
    OpenTelemetry::SpanHolder span("MergeTreeHybridSearchManager::RelativeScoreFusion()");

    /// Get labels and scores from text search result
    if (text_search_result)
    {
        const ColumnUInt32 * text_label_col = checkAndGetColumn<ColumnUInt32>(text_search_result->result_columns[0].get());
        const ColumnFloat32 * text_score_col = checkAndGetColumn<ColumnFloat32>(text_search_result->result_columns[1].get());

        if (!text_label_col)
        {
            LOG_DEBUG(log, "RelativeScoreFusion: label column in text search result is null");
        }
        else if (!text_score_col)
        {
            LOG_DEBUG(log, "RelativeScoreFusion: score column in text search result is null");
        }
        else
        {
            /// label and score column are not null
            /// min-max normalization on score
            auto norm_score_col = DataTypeFloat32().createColumn();
            computeMinMaxNormScore(text_score_col, norm_score_col);

            /// final score = norm-BM25 * w + (1-w) * norm-distance
            for (size_t idx = 0; idx < text_label_col->size(); idx++)
            {
                auto label_id = text_label_col->getElement(idx);
                auto norm_score = norm_score_col->getFloat32(idx);

                /// label_ids from text search are unique
                labels_with_convex_score.emplace(label_id, norm_score * weight_of_text);
            }
        }
    }

    /// Get labels and scores from vector scan result
    if (vec_scan_result)
    {
        const ColumnUInt32 * vec_scan_label_col = checkAndGetColumn<ColumnUInt32>(vec_scan_result->result_columns[0].get());
        const ColumnFloat32 * vec_scan_score_col = checkAndGetColumn<ColumnFloat32>(vec_scan_result->result_columns[1].get());

        if (!vec_scan_label_col)
        {
            LOG_DEBUG(log, "RelativeScoreFusion: label column in vector scan result is null");
        }
        else if (!vec_scan_score_col)
        {
            LOG_DEBUG(log, "RelativeScoreFusion: score column in vector scan result is null");
        }
        else
        {
            /// label and score column are not null
            /// min-max normalization on score
            auto norm_score_col = DataTypeFloat32().createColumn();
            computeMinMaxNormScore(vec_scan_score_col, norm_score_col);

            /// The Relative score fusion with distance score depends on the metric type.
            for (size_t idx = 0; idx < vec_scan_label_col->size(); idx++)
            {
                auto label_id = vec_scan_label_col->getElement(idx);
                auto norm_score = norm_score_col->getFloat32(idx);

                Float32 fusion_score = 0;

                if (vector_index_metric == Search::Metric::IP)
                    fusion_score = norm_score * (1-weight_of_text);
                else if (vector_index_metric == Search::Metric::L2 || vector_index_metric == Search::Metric::Cosine
                    || vector_index_metric == Search::Metric::Hamming || vector_index_metric == Search::Metric::Jaccard)
                    fusion_score = (1-weight_of_text) * (1-norm_score);
                else
                {
                    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implement metric type {} for hybrid search", vector_index_metric);
                }

                /// Insert or update score for label_id
                if (labels_with_convex_score.contains(label_id))
                    labels_with_convex_score[label_id] += fusion_score;
                else
                    labels_with_convex_score.emplace(label_id, fusion_score);
            }
        }
    }
}

void MergeTreeHybridSearchManager::computeMinMaxNormScore(
    const ColumnFloat32 * score_col,
    MutableColumnPtr & norm_score_col)
{
    if (!score_col)
    {
        LOG_DEBUG(log, "[computeMinMaxNormScore] score col is null");
        return;
    }

    if (score_col->size() == 0)
    {
        LOG_DEBUG(log, "[computeMinMaxNormScore] score col is empty");
        return;
    }

    /// Here assume the scores in score column are ordered.
    /// Thus the min score and max score are the first and last.
    Float32 min_score, max_score, min_max_scale;
    min_score = score_col->getFloat32(0);
    max_score = score_col->getFloat32(score_col->size()-1);

    /// When min_score = max_score, norm_score = 1.0;
    if (min_score == max_score)
    {
        LOG_DEBUG(log, "[computeMinMaxNormScore] max_score and min_score are equal");
        for (size_t idx = 0; idx < score_col->size(); idx++)
            norm_score_col->insert(1.0);

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
    for (size_t idx = 0; idx < score_col->size(); idx++)
    {
        Float32 norm_score = (score_col->getFloat32(idx) - min_score) / min_max_scale;
        norm_score_col->insert(norm_score);
    }
}

void MergeTreeHybridSearchManager::RankFusion(
    std::map<UInt32, Float32> & labels_with_rank_score,
    const VectorScanResultPtr vec_scan_result,
    const TextSearchResultPtr text_search_result,
    const int k)
{
    OpenTelemetry::SpanHolder span("MergeTreeHybridSearchManager::RankedFusion()");
    HybridSearchResultPtr tmp_hybrid_search_result = std::make_shared<CommonSearchResult>();

    tmp_hybrid_search_result->result_columns.resize(2);
    auto score_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

    /// Get default value for ranked constant k
    int fusion_k = k <= 0 ? 60 : k;

    /// Get labels and scores from vector scan result
    if (vec_scan_result)
    {
        const ColumnUInt32 * vec_scan_label_col = checkAndGetColumn<ColumnUInt32>(vec_scan_result->result_columns[0].get());
        const ColumnFloat32 * vec_scan_score_col = checkAndGetColumn<ColumnFloat32>(vec_scan_result->result_columns[1].get());

        if (!vec_scan_label_col)
        {
            LOG_DEBUG(log, "Rank fusion: label column in vector scan result is null");
        }
        else if (!vec_scan_score_col)
        {
            LOG_DEBUG(log, "Rank fusion: Score column in vector scan result is null");
        }
        else
        {
            /// label and score column are not null
            /// Get rank score for label and add it to labels_with_rank_score for fusion score.
            computeRankFusionScore(labels_with_rank_score, vec_scan_label_col, fusion_k);
        }
    }

    /// Get labels and scores from text search result
    if (text_search_result)
    {
        const ColumnUInt32 * text_search_label_col = checkAndGetColumn<ColumnUInt32>(text_search_result->result_columns[0].get());
        const ColumnFloat32 * text_search_score_col = checkAndGetColumn<ColumnFloat32>(text_search_result->result_columns[1].get());

        if (!text_search_label_col)
        {
            LOG_DEBUG(log, "Label column in text search result is null");
        }
        else if (!text_search_score_col)
        {
            LOG_DEBUG(log, "Score column in text search result is null");
        }
        else
        {
            /// label and score column are not null
            computeRankFusionScore(labels_with_rank_score, text_search_label_col, fusion_k);
        }
    }
}

void MergeTreeHybridSearchManager::computeRankFusionScore(
    std::map<UInt32, Float32> & labels_with_rank_score,
    const ColumnUInt32 * label_col,
    int k)
{
    /// Ranked score = 1.0 / (k + rank(label_id))
    for (size_t idx = 0; idx < label_col->size(); idx++)
    {
        Float32 rank_score = 1.0f / (k + idx + 1);
        auto id = label_col->getElement(idx);

        /// For new label id, map will insert.
        /// labels_with_rank_score map saved the fusion score for a label id.
        if (labels_with_rank_score.contains(id))
            labels_with_rank_score[id] += rank_score;
        else
            labels_with_rank_score.emplace(id, rank_score);
    }
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
