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

#include "../../../base/base/types.h"
#include "../../../base/poco/Foundation/include/Poco/String.h"

namespace Search
{
enum class DataType;
}

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

inline bool isDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find("distance") == 0;
}

inline bool isBatchDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find("batch_distance") == 0;
}

inline bool isVectorScanFunc(const String & func)
{
    return isDistance(func) || isBatchDistance(func);
}

Search::DataType getSearchIndexDataType(DataTypePtr &data_type);

void checkVectorDimension(const Search::DataType & search_type, const uint64_t & dim);

}
