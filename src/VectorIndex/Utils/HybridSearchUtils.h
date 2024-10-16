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
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <VectorIndex/Storages/HybridSearchResult.h>

namespace DB
{
using ASTPtr = std::shared_ptr<IAST>;

const String HYBRID_SEARCH_SCORE_COLUMN_NAME = "HybridSearch_func";

/// Distributed Hybrid Search additional columns
const NameAndTypePair SCORE_TYPE_COLUMN{"_distributed_hybrid_search_score_type", std::make_shared<DataTypeUInt8>()};

void splitHybridSearchAST(
    ASTPtr & hybrid_search_ast,
    ASTPtr & vector_search_ast,
    ASTPtr & text_search_ast,
    int distance_order_by_direction,
    UInt64 vector_limit,
    UInt64 text_limit,
    bool enable_nlq,
    String text_operator);

void RankFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & vec_scan_result_dataset,
    const ScoreWithPartIndexAndLabels & text_search_result_dataset,
    const UInt64 fusion_k,
    Poco::Logger * log);

void RelativeScoreFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & vec_scan_result_dataset,
    const ScoreWithPartIndexAndLabels & text_search_result_dataset,
    const Float32 fusion_weight,
    const Int8 vector_scan_direction,
    Poco::Logger * log);

void computeNormalizedScore(
    const ScoreWithPartIndexAndLabels & search_result_dataset, std::vector<Float32> & norm_score, Poco::Logger * log);
}
