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

#include <Core/Field.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Common/logger_useful.h>

#include <SearchIndex/SearchIndexCommon.h>
#include <VectorIndex/Storages/VSDescription.h>
#include <VectorIndex/Utils/CommonUtils.h>

namespace Search
{
enum class DataType;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

Search::DataType getSearchIndexDataType(DataTypePtr &data_type)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::Array:
        {
            const DataTypeArray *array_type = typeid_cast<const DataTypeArray *>(data_type.get());
            if (array_type)
            {
                WhichDataType which(array_type->getNestedType());
                if (!which.isFloat32())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The element type inside the array must be `Float32`");
                return Search::DataType::FloatVector;
            }
            break;
        }
        case TypeIndex::FixedString:
            return Search::DataType::BinaryVector;
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Vector search can be used with `Array(Float32)` or `FixedString` column");
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Vector search Type");
}

void checkVectorDimension(const Search::DataType & search_type, const uint64_t & dim)
{
    if (search_type == Search::DataType::FloatVector && dim == 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong dimension for Float32 Vector: 0, please check length constraint on search column");
    }
    /// BinaryVector is represented as FixedString(N), N > 0 has already been verified
    else if (search_type == Search::DataType::BinaryVector && dim % 8 != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong dimension for Binary Vector: {}, dimension must be a multiple of 8", dim);
    }
}

void checkTextSearchColumnDataType(DataTypePtr &data_type, bool & is_mapKeys)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::String:
            break;
        case TypeIndex::Array:
        {
            const DataTypeArray *array_type = typeid_cast<const DataTypeArray *>(data_type.get());
            if (array_type)
            {
                WhichDataType which(array_type->getNestedType());
                if (!which.isString())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The element type inside the array must be `String` for text search column");
            }
            else
                throw Exception(ErrorCodes::INCORRECT_DATA, "Search text column type is incorrect Array type");
            break;
        }
        case TypeIndex::Map:
        {
            const DataTypeMap *map_type = typeid_cast<const DataTypeMap *>(data_type.get());
            if (map_type)
            {
                WhichDataType which_key(map_type->getKeyType());
                WhichDataType which_value(map_type->getValueType());
                if (!is_mapKeys || !which_key.isString() || !which_value.isString())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "When the text search column type is map, the key and value must be strings. Additionally, the mapKeys() function must be used to process the map column");
            }
            else
                throw Exception(ErrorCodes::INCORRECT_DATA, "Search text column type is incorrect Map type");
            break;
        }
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Text search can be used with `String`, `Array(String)` or `Map(String, String)` column");
    }
}

}
