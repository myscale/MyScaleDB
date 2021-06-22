#pragma once

#include <Columns/IColumn.h>

#include <memory>

namespace DB
{

struct VectorScanResult
{
    bool is_batch;
    bool computed;
    int top_k;   /// Will be reused to store num_reorder in two stage search
    int query_vector_num;
    MutableColumns result_columns;
    std::vector<bool> was_result_processed;  /// Mark if the result was processed or not.
    UInt64 vector_scan_duration_ms;
    UInt64 read_duration_ms;

    VectorScanResult(): is_batch(false), computed(false) {}
};

using VectorScanResultPtr = std::shared_ptr<VectorScanResult>;

}
