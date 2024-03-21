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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>

namespace Search
{
enum class DataType;
}

namespace DB
{

// namespace JSONBuilder { class JSONMap; }

struct VSDescription
{
    Array parameters;        /// Parameters of the (parametric) vector scan function.

    Poco::JSON::Object::Ptr vector_parameters;         /// Parameters of the (parametric) vector scan function. Such as: for IVFFLAT index, nprobe=128

    String search_column_name;      /// used if no `arguments` are specified.
    ColumnPtr query_column;         /// const column for query
    String query_column_name;
    String column_name;             /// What name to use for a column with vector scan function values

    Search::DataType vector_search_type;

    uint64_t search_column_dim{0};
    int topk = -1;    /// topK value extracted from limit N
    int direction = 1;  /// 1 - ascending, -1 - descending.
};

using VSDescriptions = std::vector<VSDescription>;

}
