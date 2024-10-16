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

#include <queue>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/IAccumulatingTransform.h>
#include <VectorIndex/Utils/CommonUtils.h>
#include <VectorIndex/Utils/HybridSearchUtils.h>
#include <Common/logger_useful.h>


namespace DB
{

class HybridSearchFusionTransform final : public IAccumulatingTransform
{
public:
    enum HybridSearchFusionType
    {
        RSF,
        RRF
    };

    String getName() const override { return "HybridSearchFusionTransform"; }

    explicit HybridSearchFusionTransform(
        Block header,
        UInt64 num_candidates_,
        String fusion_type_,
        UInt64 fusion_k_,
        Float32 fusion_weight_,
        Int8 vector_scan_order_direction_)
        : IAccumulatingTransform(header, header)
        , num_candidates(num_candidates_)
        , fusion_k(fusion_k_)
        , fusion_weight(fusion_weight_)
        , vector_scan_order_direction(vector_scan_order_direction_)
    {
        score_column_pos = header.getPositionByName(HYBRID_SEARCH_SCORE_COLUMN_NAME);
        score_type_column_pos = header.getPositionByName(SCORE_TYPE_COLUMN.name);

        fusion_shard_num_pos = header.getPositionByName("shardNum()");
        fusion_part_index_pos = header.getPositionByName("_part_index");
        fusion_part_offset_pos = header.getPositionByName("_part_offset");

        if (isRelativeScoreFusion(fusion_type_))
            fusion_type = HybridSearchFusionType::RSF;
        else if (isRankFusion(fusion_type_))
            fusion_type = HybridSearchFusionType::RRF;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown HybridSearch fusion type: {}", fusion_type_);
    }

    void consume(Chunk block) override { chunks.push(std::move(block)); }

    Chunk generate() override;

private:
    std::queue<Chunk> chunks;

    UInt64 num_candidates = 0;
    HybridSearchFusionType fusion_type;
    UInt64 fusion_k;
    Float32 fusion_weight;

    /// Vector scan order direction in vector scan query
    /// 1 - ascending, -1 - descending
    Int8 vector_scan_order_direction;

    size_t score_column_pos;
    size_t score_type_column_pos;

    /// Combine shard_num, part_index, part_offset into fusion_id
    size_t fusion_shard_num_pos;
    size_t fusion_part_index_pos;
    size_t fusion_part_offset_pos;

    Poco::Logger * log = &Poco::Logger::get("HybridSearchFusionTransform");
};

}
