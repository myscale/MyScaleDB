#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

// namespace JSONBuilder { class JSONMap; }

struct VectorScanDescription
{
    Array parameters;        /// Parameters of the (parametric) vector scan function.

    Poco::JSON::Object::Ptr vector_parameters;         /// Parameters of the (parametric) vector scan function. Such as: for IVFFLAT index, nprobe=128

    String search_column_name;      /// used if no `arguments` are specified.
    ColumnPtr query_column;         /// const column for query
    String query_column_name;
    String column_name;             /// What name to use for a column with vector scan function values

    uint64_t search_column_dim{0};
    int topk = -1;    /// topK value extracted from limit N
    int direction = 1;  /// 1 - ascending, -1 - descending.

    // void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
    // void explain(JSONBuilder::JSONMap & map) const;
};

using VectorScanDescriptions = std::vector<VectorScanDescription>;

}
