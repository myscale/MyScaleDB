#include <VectorIndex/Processors/HybridSearchFusionTransform.h>
#include <VectorIndex/Storages/HybridSearchResult.h>

namespace DB
{

Chunk HybridSearchFusionTransform::generate()
{
    if (chunks.empty())
        return {};

    Chunk merged_chunk = std::move(chunks.front());
    chunks.pop();
    while (!chunks.empty())
    {
        merged_chunk.append(std::move(chunks.front()));
        chunks.pop();
    }

    auto merged_chunk_columns = merged_chunk.getColumns();
    auto total_rows = merged_chunk.getNumRows();

    /// Row range is stored in the following format: {start_index, length}
    std::pair<size_t, size_t> distance_row_range = {0, 0};
    std::pair<size_t, size_t> bm25_row_range = {0, 0};

    auto & merged_score_column = assert_cast<const ColumnFloat32 &>(*merged_chunk_columns[score_column_pos]);
    auto & merged_score_type_column = assert_cast<const ColumnUInt8 &>(*merged_chunk_columns[score_type_column_pos]);

    auto & merged_shard_num_column = assert_cast<const ColumnUInt32 &>(*merged_chunk_columns[fusion_shard_num_pos]);
    auto & merged_part_index_column = assert_cast<const ColumnUInt64 &>(*merged_chunk_columns[fusion_part_index_pos]);
    auto & merged_part_offset_column = assert_cast<const ColumnUInt64 &>(*merged_chunk_columns[fusion_part_offset_pos]);

    /// Get the distance and bm25 score range
    {
        for (size_t row = 0; row < total_rows; ++row)
        {
            if (merged_score_type_column.get64(row) == 0)
                bm25_row_range.first++;
            else
                break;
        }

        bm25_row_range.second = std::min(total_rows - bm25_row_range.first, num_candidates);

        if (vector_scan_order_direction == -1)
        {
            distance_row_range.first = 0;
            distance_row_range.second = std::min(bm25_row_range.first, num_candidates);
        }
        else if (vector_scan_order_direction == 1)
        {
            if (bm25_row_range.first >= num_candidates)
            {
                distance_row_range.first = bm25_row_range.first - num_candidates;
                distance_row_range.second = num_candidates;
            }
            else if (bm25_row_range.first > 0)
            {
                distance_row_range.first = 0;
                distance_row_range.second = bm25_row_range.first;
            }
        }

        LOG_DEBUG(log, "distance_row_range, start index: {}, count: {}", distance_row_range.first, distance_row_range.second);
        LOG_DEBUG(log, "bm25_row_range, start index: {}, count: {}", bm25_row_range.first, bm25_row_range.second);
    }

    /// Create distance and bm25 score dataset
    ScoreWithPartIndexAndLabels bm25_score_dataset, distance_score_dataset;
    for (size_t offset = 0; offset < distance_row_range.second; ++offset)
    {
        size_t row;
        if (vector_scan_order_direction == -1)
            row = distance_row_range.first + offset;
        else
            row = distance_row_range.first + distance_row_range.second - 1 - offset;

        distance_score_dataset.emplace_back(
            merged_score_column.getFloat32(row),
            merged_part_index_column.get64(row),
            merged_part_offset_column.get64(row),
            merged_shard_num_column.get64(row));

        LOG_TRACE(log, "distance_score_dataset: {}", distance_score_dataset.back().dump());
    }
    for (size_t offset = 0; offset < bm25_row_range.second; ++offset)
    {
        size_t row = bm25_row_range.first + offset;
        bm25_score_dataset.emplace_back(
            merged_score_column.getFloat32(row),
            merged_part_index_column.get64(row),
            merged_part_offset_column.get64(row),
            merged_shard_num_column.get64(row));

        LOG_TRACE(log, "bm25_score_dataset: {}", bm25_score_dataset.back().dump());
    }

    /// Calculate the fusion score
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> fusion_id_with_score;
    if (fusion_type == HybridSearchFusionType::RSF)
    {
        RelativeScoreFusion(
            fusion_id_with_score, distance_score_dataset, bm25_score_dataset, fusion_weight, vector_scan_order_direction, log);
    }
    else if (fusion_type == HybridSearchFusionType::RRF)
    {
        RankFusion(fusion_id_with_score, distance_score_dataset, bm25_score_dataset, fusion_k, log);
    }

    auto fusion_result_columns = merged_chunk.cloneEmptyColumns();
    auto & result_score_column = assert_cast<ColumnFloat32 &>(*fusion_result_columns[score_column_pos]);

    auto & result_shard_num_column = assert_cast<ColumnUInt32 &>(*fusion_result_columns[fusion_shard_num_pos]);
    auto & result_part_index_column = assert_cast<ColumnUInt64 &>(*fusion_result_columns[fusion_part_index_pos]);
    auto & result_part_offset_column = assert_cast<ColumnUInt64 &>(*fusion_result_columns[fusion_part_offset_pos]);

    /// Copy bm25 score rows to fusion_result_columns
    for (size_t position = 0; position < merged_chunk_columns.size(); ++position)
    {
        auto column = merged_chunk_columns[position];
        fusion_result_columns[position]->insertRangeFrom(*column, bm25_row_range.first, bm25_row_range.second);
    }

    /// Replace the bm25 score with fusion score
    for (size_t row = 0; row < bm25_row_range.second; ++row)
    {
        auto fusion_id = std::make_tuple(
            static_cast<UInt32>(result_shard_num_column.get64(row)),
            result_part_index_column.get64(row),
            result_part_offset_column.get64(row));

        result_score_column.getElement(row) = fusion_id_with_score[fusion_id];
        fusion_id_with_score.erase(fusion_id);
    }

    /// Copy distance score rows to fusion_result_columns and replace the distance score with fusion score
    for (size_t offset = 0; offset < distance_row_range.second; ++offset)
    {
        size_t row = distance_row_range.first + offset;

        auto fusion_id = std::make_tuple(
            static_cast<UInt32>(merged_shard_num_column.get64(row)),
            merged_part_index_column.get64(row),
            merged_part_offset_column.get64(row));

        if (fusion_id_with_score.find(fusion_id) != fusion_id_with_score.end())
        {
            for (size_t position = 0; position < merged_chunk_columns.size(); ++position)
            {
                if (position == score_column_pos)
                {
                    fusion_result_columns[position]->insert(fusion_id_with_score[fusion_id]);
                }
                else
                {
                    fusion_result_columns[position]->insertFrom(*merged_chunk_columns[position], row);
                }
            }
        }
    }

    merged_chunk.setColumns(std::move(fusion_result_columns), result_score_column.size());
    return merged_chunk;
}

}
