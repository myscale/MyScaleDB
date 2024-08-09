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

#include <Columns/IColumn.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <memory>

namespace DB
{
class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// Common search result for vector scan, text search and hybrid search
struct CommonSearchResult
{
    bool computed = false;
    MutableColumns result_columns;
    std::vector<bool> was_result_processed;  /// Mark if the result was processed or not.
};

using CommonSearchResultPtr = std::shared_ptr<CommonSearchResult>;

using VectorScanResultPtr = std::shared_ptr<CommonSearchResult>;
using TextSearchResultPtr = std::shared_ptr<CommonSearchResult>;
using HybridSearchResultPtr = std::shared_ptr<CommonSearchResult>;

/// Extend RangesInDataPart to include search result for hybrid, vector scan or text search
struct SearchResultAndRangesInDataPart
{
    DataPartPtr data_part;
    AlterConversionsPtr alter_conversions;
    size_t part_index_in_query;
    MarkRanges ranges;
    CommonSearchResultPtr search_result;

    SearchResultAndRangesInDataPart() = default;

    SearchResultAndRangesInDataPart(
        const DataPartPtr & data_part_,
        const AlterConversionsPtr & alter_conversions_,
        const size_t part_index_in_query_,
        const MarkRanges & ranges_ = MarkRanges{},
        const CommonSearchResultPtr & search_result_ = nullptr)
        : data_part{data_part_}
        , alter_conversions{alter_conversions_}
        , part_index_in_query{part_index_in_query_}
        , ranges{ranges_}
        , search_result{search_result_}
    {}
};

using SearchResultAndRangesInDataParts = std::vector<SearchResultAndRangesInDataPart>;

/// Save vector scan and/or text search result for a part
struct VectorAndTextResultInDataPart
{
    RangesInDataPart part_with_ranges;
    VectorScanResultPtr vector_scan_result;
    TextSearchResultPtr text_search_result;

    VectorAndTextResultInDataPart() = default;

    VectorAndTextResultInDataPart(
        const RangesInDataPart & part_with_ranges_,
        const VectorScanResultPtr & vector_scan_result_ = nullptr,
        const TextSearchResultPtr & text_search_result_ = nullptr)
        : part_with_ranges{part_with_ranges_}
        , vector_scan_result{vector_scan_result_}
        , text_search_result{text_search_result_}
    {}
};

using VectorAndTextResultInDataParts = std::vector<VectorAndTextResultInDataPart>;

/// Internal struct for a search result with part index and label id
struct ScoreWithPartIndexAndLabel
{
    Float32 score;
    size_t part_index;
    UInt32 label_id;

    ScoreWithPartIndexAndLabel() = default;

    ScoreWithPartIndexAndLabel(const Float32 score_, const size_t part_index_, const UInt32 label_id_)
    : score{score_}, part_index{part_index_}, label_id{label_id_}
    {}
};

using ScoreWithPartIndexAndLabels = std::vector<ScoreWithPartIndexAndLabel>;

}
