#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB
{

/// Fusing and sorting data stream with distance or bm25
class FusionSortingStep : public ITransformingStep
{
public:
    FusionSortingStep(
        const DataStream & input_stream,
        SortDescription sort_description_,
        UInt64 limit_,
        UInt64 num_candidates_,
        String fusion_type_,
        UInt64 fusion_k_,
        Float32 fusion_weight_,
        Int8 distance_score_order_direction_);

    String getName() const override { return "HybridSearchFusionSorting"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    const SortDescription result_description;
    UInt64 limit;

    UInt64 num_candidates = 0;
    String fusion_type;

    UInt64 fusion_k;
    Float32 fusion_weight;

    Int8 distance_score_order_direction;
};

}
