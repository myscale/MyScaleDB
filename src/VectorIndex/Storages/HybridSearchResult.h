#pragma once

#include <Columns/IColumn.h>

#include <memory>

namespace DB
{

/// Common search result for vector scan, text search and hybrid search
struct CommonSearchResult
{
    bool computed = false;
    MutableColumns result_columns;
    std::vector<bool> was_result_processed;  /// Mark if the result was processed or not.
};

using CommonSearchResultPtr = std::shared_ptr<CommonSearchResult>;

using VectorScanResultPtr = std::shared_ptr<CommonSearchResult>;
using TextSearchResultPtr = std::shared_ptr<CommonSearchResult>;
using HybridSearchResultPtr = std::shared_ptr<CommonSearchResult>;
}
