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
    String name; /// vector scan function (distance) result column name
};

using CommonSearchResultPtr = std::shared_ptr<CommonSearchResult>;

using VectorScanResultPtr = std::shared_ptr<CommonSearchResult>;
using ManyVectorScanResults = std::vector<VectorScanResultPtr>;

using TextSearchResultPtr = std::shared_ptr<CommonSearchResult>;
using HybridSearchResultPtr = std::shared_ptr<CommonSearchResult>;

/// Extend RangesInDataPart to include search result for hybrid, vector scan or text search
struct SearchResultAndRangesInDataPart
{
    RangesInDataPart part_with_ranges;

    /// Valid for text, hybrid search
    CommonSearchResultPtr search_result = nullptr;

    /// Support multiple distance functions
    /// Valid for vector scan
    ManyVectorScanResults multiple_vector_scan_results = {};

    SearchResultAndRangesInDataPart() = default;

    SearchResultAndRangesInDataPart(
        const RangesInDataPart & part_with_ranges_,
        const CommonSearchResultPtr & search_result_ = nullptr)
        : part_with_ranges{part_with_ranges_}
        , search_result{search_result_}
    {}

    SearchResultAndRangesInDataPart(
        const RangesInDataPart & part_with_ranges_,
        const ManyVectorScanResults & multiple_vector_scan_results_ = {})
        : part_with_ranges{part_with_ranges_}
        , multiple_vector_scan_results{multiple_vector_scan_results_}
    {}
};

using SearchResultAndRangesInDataParts = std::vector<SearchResultAndRangesInDataPart>;

/// Internal structure of intermediate results
/// Save vector scan and/or text search result for a part
struct VectorAndTextResultInDataPart
{
    /// Other data part info can be accessed by part_index from parts_with_ranges
    size_t part_index;
    DataPartPtr data_part;

    /// Valid for text and hybrid search
    TextSearchResultPtr text_search_result = nullptr;

    /// Support multiple distance functions
    /// Use the first element for hybrid search and second stage of two-stage vector search
    ManyVectorScanResults vector_scan_results = {};

    VectorAndTextResultInDataPart() = default;

    VectorAndTextResultInDataPart(const size_t part_index_, const DataPartPtr & data_part_)
        : part_index{part_index_}
        , data_part{data_part_}
    {}
};

using VectorAndTextResultInDataParts = std::vector<VectorAndTextResultInDataPart>;

/// Internal struct for a search result with part index and label id
struct ScoreWithPartIndexAndLabel
{
    Float32 score;
    UInt64 part_index; /// part index in parts_with_ranges
    UInt64 label_id;

    /// Only used in multi-shard hybrid search
    UInt32 shard_num = 0;

    ScoreWithPartIndexAndLabel() = default;

    ScoreWithPartIndexAndLabel(const Float32 score_, const UInt64 part_index_, const UInt64 label_id_)
        : score{score_}, part_index{part_index_}, label_id{label_id_}
    {
    }

    ScoreWithPartIndexAndLabel(const Float32 score_, const UInt64 part_index_, const UInt64 label_id_, const UInt32 shard_num_)
        : score{score_}, part_index{part_index_}, label_id{label_id_}, shard_num{shard_num_}
    {
    }

    String dump() const
    {
        return fmt::format("[shard_num: {}, part_index: {}, label_id: {}]: {}", shard_num, part_index, label_id, score);
    }
};

using ScoreWithPartIndexAndLabels = std::vector<ScoreWithPartIndexAndLabel>;

}
