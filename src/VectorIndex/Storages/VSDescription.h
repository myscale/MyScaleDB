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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>
#include <VectorIndex/Utils/CommonUtils.h>

namespace Search
{
enum class Metric;
}

namespace DB
{

// namespace JSONBuilder { class JSONMap; }

struct VSDescription
{
    Array parameters;        /// Parameters of the (parametric) vector scan function.

    Poco::JSON::Object::Ptr vector_parameters;         /// Parameters of the (parametric) vector scan function. Such as: for IVFFLAT index, nprobe=128

    String search_column_name;      /// used if no `arguments` are specified.
    ColumnPtr query_column;         /// const column for query
    String query_column_name;
    String column_name;             /// What name to use for a column with vector scan function values

    Search::DataType vector_search_type;

    uint64_t search_column_dim{0};
    int topk = -1;    /// topK value extracted from limit N
    int direction = 1;  /// 1 - ascending, -1 - descending.
};

using VSDescriptions = std::vector<VSDescription>;

struct VectorScanInfo
{
    VSDescriptions vector_scan_descs;
    bool is_batch;

    VectorScanInfo(const VSDescriptions & vector_scan_descs_)
        : vector_scan_descs(vector_scan_descs_) {
        is_batch = !vector_scan_descs.empty() && isBatchDistance(vector_scan_descs[0].column_name);
    }
};

using VectorScanInfoPtr = std::shared_ptr<const VectorScanInfo>;

struct TextSearchInfo
{
    String text_column_name;        /// text column name with inverted index
    String query_text;              /// query text for full-text search
    String function_column_name;    /// What name to use for a column with text search function values

    int topk = -1;          /// topK value extracted from limit N
    int direction = -1;     /// 1 - ascending, -1 - descending.

    TextSearchInfo(const String text_col_name_, const String query_text_, const String function_column_name_, int topk_, int direction_)
        : text_column_name(text_col_name_)
        , query_text(query_text_)
        , function_column_name(function_column_name_)
        , topk(topk_)
        , direction(direction_)
    {
    }
};

using TextSearchInfoPtr = std::shared_ptr<const TextSearchInfo>;

struct HybridSearchInfo
{
    VectorScanInfoPtr vector_scan_info;
    TextSearchInfoPtr text_search_info;

    String function_column_name;    /// What name to use for a column with hybrid search function values
    int topk = -1;                  /// topK value
    String fusion_type;             /// Fusion type, Relative Score Fustion(RSF) or Reciprocal Rank Fusion(RRF)

    /// Used for score fusion
    float fusion_weight = -1;       /// weight of text search
    Search::Metric metric_type;     /// metric_type in vector index

    /// Used for rank fusion
    int fusion_k = -1;

    HybridSearchInfo(
        VectorScanInfoPtr vec_scan_info_,
        TextSearchInfoPtr text_search_info_,
        String func_col_name_, int topk_, String fusion_type_, float fusion_weight_, Search::Metric metric_type_)
        : vector_scan_info(vec_scan_info_)
        , text_search_info(text_search_info_)
        , function_column_name(func_col_name_)
        , topk(topk_)
        , fusion_type(fusion_type_)
        , fusion_weight(fusion_weight_)
        , metric_type(metric_type_)
    {
    }

    HybridSearchInfo(
        VectorScanInfoPtr vec_scan_info_,
        TextSearchInfoPtr text_search_info_,
        String func_col_name_, int topk_, String fusion_type_, int fusion_k_)
        : vector_scan_info(vec_scan_info_)
        , text_search_info(text_search_info_)
        , function_column_name(func_col_name_)
        , topk(topk_)
        , fusion_type(fusion_type_)
        , fusion_k(fusion_k_)
    {
    }
};

using HybridSearchInfoPtr = std::shared_ptr<const HybridSearchInfo>;

}
