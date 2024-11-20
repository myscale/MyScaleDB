#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <VectorIndex/Utils/CommonUtils.h>
#include <VectorIndex/Utils/HybridSearchUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Replace limit with num_candidates that is equal to limit * hybrid_search_top_k_multiple_base
inline void replaceLimitAST(ASTPtr & ast, UInt64 replaced_limit)
{
    const auto * select_query = ast->as<ASTSelectQuery>();
    if (!select_query->limitLength())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No limit in Distributed HybridSearch AST");

    auto limit_ast = select_query->limitLength()->as<ASTLiteral>();
    if (!limit_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad limit in Distributed HybridSearch AST");

    limit_ast->value = replaced_limit;
}

/// Use Distributed Hybrid Search AST to create two separate ASTs for vector search and text search
void splitHybridSearchAST(
    ASTPtr & hybrid_search_ast,
    ASTPtr & vector_search_ast,
    ASTPtr & text_search_ast,
    int distance_order_by_direction,
    UInt64 vector_limit,
    UInt64 text_limit,
    bool enable_nlq,
    String text_operator)
{
    /// Replace the ASTFunction, ASTOrderByElement and LimitAST for Vector Search
    {
        vector_search_ast = hybrid_search_ast->clone();
        const auto * select_vector_query = vector_search_ast->as<ASTSelectQuery>();

        for (auto & child : select_vector_query->select()->children)
        {
            auto function = child->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                child = makeASTFunction(
                    DISTANCE_FUNCTION, function->arguments->children[0]->clone(), function->arguments->children[2]->clone());
            }

            auto identifier = child->as<ASTIdentifier>();
            if (!identifier)
                continue;
            else if (identifier->name() == SCORE_TYPE_COLUMN.name)
            {
                /// Delete the SCORE_TYPE_COLUMN from the select list
                select_vector_query->select()->children.erase(
                    std::remove(select_vector_query->select()->children.begin(), select_vector_query->select()->children.end(), child),
                    select_vector_query->select()->children.end());
            }
        }

        /// Replace the HybridSearch function with DISTANCE_FUNCTION in the ORDER BY
        if (!select_vector_query->orderBy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No ORDER BY in Distributed HybridSearch AST");
        for (auto & child : select_vector_query->orderBy()->children)
        {
            auto * order_by_element = child->as<ASTOrderByElement>();
            if (!order_by_element || order_by_element->children.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad ORDER BY expression AST");

            auto function = order_by_element->children.at(0)->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                order_by_element->children.at(0) = makeASTFunction(
                    DISTANCE_FUNCTION, function->arguments->children[0]->clone(), function->arguments->children[2]->clone());
                order_by_element->direction = distance_order_by_direction;
            }
        }

        replaceLimitAST(vector_search_ast, vector_limit);
    }

    /// Replace the ASTFunction, ASTOrderByElement and LimitAST for Text Search
    {
        text_search_ast = hybrid_search_ast->clone();
        const auto * select_text_query = text_search_ast->as<ASTSelectQuery>();

        auto text_search_function_parameters = std::make_shared<ASTExpressionList>();
        text_search_function_parameters->children.push_back(std::make_shared<ASTLiteral>("enable_nlq=" + std::to_string(enable_nlq)));
        text_search_function_parameters->children.push_back(std::make_shared<ASTLiteral>("operator=" + text_operator));

        /// Replace the HybridSearch function with TEXT_SEARCH_FUNCTION in the select list
        for (auto & child : select_text_query->select()->children)
        {
            auto function = child->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                std::shared_ptr<ASTFunction> text_search_function = makeASTFunction(
                    TEXT_SEARCH_FUNCTION, function->arguments->children[1]->clone(), function->arguments->children[3]->clone());
                text_search_function->parameters = text_search_function_parameters->clone();
                text_search_function->children.push_back(text_search_function->parameters);
                child = text_search_function;
            }

            auto identifier = child->as<ASTIdentifier>();
            if (!identifier)
                continue;
            else if (identifier->name() == SCORE_TYPE_COLUMN.name)
            {
                /// Delete the SCORE_TYPE_COLUMN from the select list
                select_text_query->select()->children.erase(
                    std::remove(select_text_query->select()->children.begin(), select_text_query->select()->children.end(), child),
                    select_text_query->select()->children.end());
            }
        }

        /// Replace the HybridSearch function with TEXT_SEARCH_FUNCTION in the ORDER BY
        if (!select_text_query->orderBy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No ORDER BY in Distributed HybridSearch AST");
        for (auto & child : select_text_query->orderBy()->children)
        {
            auto * order_by_element = child->as<ASTOrderByElement>();
            if (!order_by_element || order_by_element->children.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad ORDER BY expression AST");

            auto function = order_by_element->children.at(0)->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                std::shared_ptr<ASTFunction> text_search_function = makeASTFunction(
                    TEXT_SEARCH_FUNCTION, function->arguments->children[1]->clone(), function->arguments->children[3]->clone());
                text_search_function->parameters = text_search_function_parameters->clone();
                text_search_function->children.push_back(text_search_function->parameters);

                order_by_element->children.at(0) = text_search_function;
                order_by_element->direction = -1;
            }
        }

        replaceLimitAST(text_search_ast, text_limit);
    }
}

/// RRF_score = 1.0 / (fusion_k + rank(fusion_id_bm25)) + 1.0 / (fusion_k + rank(fusion_id_distance))
void RankFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & vec_scan_result_dataset,
    const ScoreWithPartIndexAndLabels & text_search_result_dataset,
    const UInt64 fusion_k,
    LoggerPtr log)
{
    size_t idx = 0;
    for (const auto & vector_score_with_label : vec_scan_result_dataset)
    {
        auto fusion_id
            = std::make_tuple(vector_score_with_label.shard_num, vector_score_with_label.part_index, vector_score_with_label.label_id);

        /// For new (shard_num, part_index, label_id) tuple, map will insert.
        /// fusion_id_with_score map saved the fusion score for a (shard_num, part_index, label_id) tuple.
        /// AS for single-shard hybrid search, shard_num is always 0.
        fusion_id_with_score[fusion_id] += 1.0f / (fusion_k + idx + 1);
        idx++;

        LOG_TRACE(
            log,
            "fusion_id: [{}, {}, {}], ranked_score: {}",
            vector_score_with_label.shard_num,
            vector_score_with_label.part_index,
            vector_score_with_label.label_id,
            fusion_id_with_score[fusion_id]);
    }

    idx = 0;
    for (const auto & text_score_with_label : text_search_result_dataset)
    {
        auto fusion_id = std::make_tuple(text_score_with_label.shard_num, text_score_with_label.part_index, text_score_with_label.label_id);

        /// Insert or update fusion score for fusion_id
        fusion_id_with_score[fusion_id] += 1.0f / (fusion_k + idx + 1);
        idx++;

        LOG_TRACE(
            log,
            "fusion_id: [{}, {}, {}], ranked_score: {}",
            text_score_with_label.shard_num,
            text_score_with_label.part_index,
            text_score_with_label.label_id,
            fusion_id_with_score[fusion_id]);
    }
}

/// RSF_score = normalized_bm25_score * fusion_weight + normalized_distance_score * (1 - fusion_weight)
void RelativeScoreFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & vec_scan_result_dataset,
    const ScoreWithPartIndexAndLabels & text_search_result_dataset,
    const Float32 fusion_weight,
    const Int8 vector_scan_direction,
    LoggerPtr log)
{
    /// Normalize text search score
    std::vector<Float32> norm_score;
    computeNormalizedScore(text_search_result_dataset, norm_score, log);

    LOG_TRACE(log, "Text Search Scores:");
    for (size_t idx = 0; idx < text_search_result_dataset.size(); idx++)
    {
        auto fusion_id = std::make_tuple(
            text_search_result_dataset[idx].shard_num,
            text_search_result_dataset[idx].part_index,
            text_search_result_dataset[idx].label_id);

        LOG_TRACE(
            log,
            "fusion_id=[{}, {}, {}], origin_score={}, norm_score={}",
            text_search_result_dataset[idx].shard_num,
            text_search_result_dataset[idx].part_index,
            text_search_result_dataset[idx].label_id,
            text_search_result_dataset[idx].score,
            norm_score[idx]);

        fusion_id_with_score[fusion_id] = norm_score[idx] * fusion_weight;
    }

    /// Normalize vector search score
    norm_score.clear();
    computeNormalizedScore(vec_scan_result_dataset, norm_score, log);

    LOG_TRACE(log, "Vector Search Scores:");
    for (size_t idx = 0; idx < vec_scan_result_dataset.size(); idx++)
    {
        auto fusion_id = std::make_tuple(
            vec_scan_result_dataset[idx].shard_num, vec_scan_result_dataset[idx].part_index, vec_scan_result_dataset[idx].label_id);

        LOG_TRACE(
            log,
            "fusion_id=[{}, {}, {}], origin_score={}, norm_score={}",
            vec_scan_result_dataset[idx].shard_num,
            vec_scan_result_dataset[idx].part_index,
            vec_scan_result_dataset[idx].label_id,
            vec_scan_result_dataset[idx].score,
            norm_score[idx]);

        Float32 fusion_distance_score = 0;

        /// 1 - ascending, -1 - descending
        if (vector_scan_direction == -1)
            fusion_distance_score = norm_score[idx] * (1 - fusion_weight);
        else
            fusion_distance_score = (1 - norm_score[idx]) * (1 - fusion_weight);

        /// Insert or update fusion score for fusion_id
        fusion_id_with_score[fusion_id] += fusion_distance_score;
    }
}

void computeNormalizedScore(
    const ScoreWithPartIndexAndLabels & search_result_dataset, std::vector<Float32> & norm_score, LoggerPtr log)
{
    const auto result_size = search_result_dataset.size();
    if (result_size == 0)
    {
        LOG_DEBUG(log, "search result is empty");
        return;
    }

    norm_score.reserve(result_size);

    /// The search_result_dataset is already ordered
    /// As for bm25 score and metric_type=IP, the scores are in ascending order; otherwise, it is in descending order.
    Float32 min_score, max_score, min_max_scale;

    /// Here assume the scores in score column are ordered in descending order.
    min_score = search_result_dataset[result_size - 1].score;
    max_score = search_result_dataset[0].score;

    /// When min_score == max_score, all scores are the same, so the normalized score is 1.0
    if (min_score == max_score)
    {
        for (size_t idx = 0; idx < result_size; idx++)
            norm_score.emplace_back(1.0f);
        return;
    }
    else if (min_score > max_score) /// ASC
    {
        std::swap(min_score, max_score);
    }

    min_max_scale = max_score - min_score;
    for (size_t idx = 0; idx < result_size; idx++)
    {
        Float32 normalizing_score = (search_result_dataset[idx].score - min_score) / min_max_scale;
        norm_score.emplace_back(normalizing_score);
    }
}

}
