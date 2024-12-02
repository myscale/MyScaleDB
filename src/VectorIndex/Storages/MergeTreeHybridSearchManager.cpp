#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Common/CurrentMetrics.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <VectorIndex/Storages/MergeTreeHybridSearchManager.h>
#include <VectorIndex/Utils/VSUtils.h>
#include <VectorIndex/Utils/HybridSearchUtils.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>

#include <memory>

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectHybridSearchThreads;
    extern const Metric MergeTreeDataSelectHybridSearchThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
}

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
    LoggerPtr log)
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
    LoggerPtr log)
{
    /// Get fusion type from hybrid_info
    String fusion_type = hybrid_info->fusion_type;

    /// Store result after fusion. (<shard_num, part_index, label_id>, score)
    /// As for single-shard hybrid search, shard_num is always 0.
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> part_index_labels_with_fusion_score;

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
        RankFusion(part_index_labels_with_fusion_score, vec_scan_result_with_part_index, text_search_result_with_part_index, fusion_k, log);
    }

    /// Sort hybrid search result based on fusion score and return top-k rows.
    LOG_TEST(log, "hybrid scores after fusion");
    std::multimap<Float32, std::pair<size_t, UInt32>, std::greater<Float32>> sorted_fusion_scores_with_part_index_label;
    for (const auto & [part_index_label_id, fusion_score] : part_index_labels_with_fusion_score)
    {
        LOG_TEST(
            log,
            "part_index={}, label_id={}, hybrid_score={}",
            std::get<1>(part_index_label_id),
            std::get<2>(part_index_label_id),
            fusion_score);

        sorted_fusion_scores_with_part_index_label.emplace(fusion_score, std::make_pair(std::get<1>(part_index_label_id), std::get<2>(part_index_label_id)));
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

SearchResultAndRangesInDataParts MergeTreeHybridSearchManager::FilterPartsWithHybridResults(
    const VectorAndTextResultInDataParts & parts_with_vector_text_result,
    const RangesInDataParts & parts_with_ranges,
    const ScoreWithPartIndexAndLabels & hybrid_result_with_part_index,
    const Settings & settings,
    LoggerPtr log)
{
    /// Merge hybrid results from the same part index into a vector
    std::map<size_t, std::vector<ScoreWithPartIndexAndLabel>> part_index_merged_map;

    for (const auto & score_with_part_index_label : hybrid_result_with_part_index)
    {
        const auto & part_index = score_with_part_index_label.part_index;
        part_index_merged_map[part_index].emplace_back(score_with_part_index_label);
    }

    size_t parts_with_ranges_size = parts_with_ranges.size();
    SearchResultAndRangesInDataParts parts_with_ranges_hybrid_result;
    parts_with_ranges_hybrid_result.resize(parts_with_ranges_size);

    /// Filter data part with part index in hybrid search and label ids for mark ranges
    auto filter_part_with_results = [&](size_t part_index)
    {
        const auto & part_with_ranges = parts_with_ranges[part_index];

        /// Check if part_index for this part_with_ranges exists in map
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
                /// Save can_skip_perform_prefilter
                bool can_skip_perform_prefilter = false;
                for (const auto & part_with_mix_results : parts_with_vector_text_result)
                {
                    if (part_with_mix_results.part_index == part_index)
                    {
                        can_skip_perform_prefilter = part_with_mix_results.can_skip_perform_prefilter;
                        break;
                    }
                }

                RangesInDataPart ranges(part_with_ranges.data_part,
                                        part_with_ranges.alter_conversions,
                                        part_with_ranges.part_index_in_query,
                                        std::move(mark_ranges_for_part));

                SearchResultAndRangesInDataPart result_with_ranges(std::move(ranges), can_skip_perform_prefilter, tmp_hybrid_search_result);
                parts_with_ranges_hybrid_result[part_index] = std::move(result_with_ranges);
            }
        }
    };

    size_t num_threads = std::min<size_t>(settings.max_threads, parts_with_ranges_size);
    if (num_threads <= 1)
    {
        for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
            filter_part_with_results(part_index);
    }
    else
    {
        /// Parallel executing filter parts_in_ranges with total top-k results
        ThreadPool pool(
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreads,
            CurrentMetrics::MergeTreeDataSelectHybridSearchThreadsActive,
            CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
            num_threads);

        for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
            pool.scheduleOrThrowOnError([&, part_index]()
                {
                    filter_part_with_results(part_index);
                });

        pool.wait();
    }

    /// Skip empty search result
    size_t next_part = 0;
    for (size_t part_index = 0; part_index < parts_with_ranges_size; ++part_index)
    {
        auto & part_with_results = parts_with_ranges_hybrid_result[part_index];
        if (!part_with_results.search_result)
            continue;

        if (next_part != part_index)
            std::swap(parts_with_ranges_hybrid_result[next_part], part_with_results);
        ++next_part;
    }

    parts_with_ranges_hybrid_result.resize(next_part);

    return parts_with_ranges_hybrid_result;
}

void MergeTreeHybridSearchManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const ColumnUInt64 * part_offset)
{
    mergeSearchResultImpl(pre_result, read_rows, read_ranges, hybrid_search_result, part_offset);
}

}
