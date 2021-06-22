#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/// Implementation for vector scan reorder in two stage search, split num_reorder Candidates based on part name.
/// This processor support one input and multiple outputs.
class VectorScanSplitTransform : public IProcessor
{
private:

    Chunk current_chunk;
    Chunks chunks;   /// MergingSortedTransform may return several chunks
    bool has_data = false;

    size_t part_column_pos;
    size_t num_streams;
    UInt64 limit = 0;
    size_t rows_read = 0;

    std::vector<char> was_output_processed;

    /// State of rows for output ports
    struct OutputPortData
    {
        MutableColumns split_columns;  /// Save rows for this output port
        size_t num_rows;
    };

    std::vector<OutputPortData> out_ports_data;

    Status prepareGenerate();
    Status prepareConsume();
    void splitChunk(Chunks result_chunks);

public:
    VectorScanSplitTransform(
        const Block & header,
        size_t num_outputs,
        UInt64 limit_);

    String getName() const override { return "VectorScanSplit"; }

    Status prepare() override;

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }
};

}
