#include <stdexcept>
#include <IO/Operators.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <VectorIndex/Processors/FusionSortingStep.h>
#include <VectorIndex/Processors/HybridSearchFusionTransform.h>
#include <VectorIndex/Utils/CommonUtils.h>

namespace CurrentMetrics
{
extern const Metric TemporaryFilesForSort;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }};
}

FusionSortingStep::FusionSortingStep(
    const DataStream & input_stream,
    SortDescription sort_description_,
    UInt64 limit_,
    UInt64 num_candidates_,
    String fusion_type_,
    UInt64 fusion_k_,
    Float32 fusion_weight_,
    Int8 distance_score_order_direction_)
    : ITransformingStep(input_stream, input_stream.header, getTraits(limit_))
    , result_description(std::move(sort_description_))
    , limit(limit_)
    , num_candidates(num_candidates_)
    , fusion_type(fusion_type_)
    , fusion_k(fusion_k_)
    , fusion_weight(fusion_weight_)
    , distance_score_order_direction(distance_score_order_direction_)
{
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}

void FusionSortingStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    output_stream->sort_description = result_description;
    output_stream->sort_scope = DataStream::SortScope::Global;
}


void FusionSortingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto fusion_transform = std::make_shared<HybridSearchFusionTransform>(
        pipeline.getHeader(), num_candidates, fusion_type, fusion_k, fusion_weight, distance_score_order_direction);
    pipeline.addTransform(std::move(fusion_transform));

    auto sort_transform = std::make_shared<PartialSortingTransform>(pipeline.getHeader(), result_description);
    pipeline.addTransform(std::move(sort_transform));
}

void FusionSortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix << "HybridSearch Fusion description: " << fusion_type;
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void FusionSortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("HybridSearch Fusion Description", fusion_type);

    if (limit)
        map.add("Limit", limit);
}

}
