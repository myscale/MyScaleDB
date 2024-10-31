#pragma once
#include <Processors/ISource.h>

namespace DB
{

class MergeTreeSelectWithVectorScanProcessor;
using MergeTreeSelectWithVectoScanProcessorPtr = std::unique_ptr<MergeTreeSelectWithVectorScanProcessor>;

struct ChunkAndProgress;

/// Reference from MergeTreeSource, without async read in linux
class MergeTreeWithVectorScanSource final : public ISource
{
public:
    explicit MergeTreeWithVectorScanSource(MergeTreeSelectWithVectoScanProcessorPtr processor_, const std::string & log_name_);
    ~MergeTreeWithVectorScanSource() override;

    std::string getName() const override;

    Status prepare() override;

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() noexcept override;

private:
    MergeTreeSelectWithVectoScanProcessorPtr processor;
    const std::string log_name;

    Chunk processReadResult(ChunkAndProgress chunk);
};

}
