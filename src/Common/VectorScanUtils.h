#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <base/types.h>
#include <Poco/String.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Interpreters/VectorScanDescription.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Common/logger_useful.h>

namespace DB
{

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

inline VectorSearchType getVectorSearchType(DataTypePtr &data_type)
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
                return VectorSearchType::Float32Vector;
            }
            break;
        }
        case TypeIndex::FixedString:
            return VectorSearchType::BinaryVector;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector search can be used with `Array(Float32)` or `FixedString` column");
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Vector search Type");
}

inline void checkVectorDimension(const VectorSearchType & search_type, const uint64_t & dim)
{
    if (search_type == VectorSearchType::Float32Vector && dim == 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong dimension for Float32 Vector: 0, please check length constraint on search column");
    }
    /// BinaryVector is represented as FixedString(N), N > 0 has already been verified
    else if (search_type == VectorSearchType::BinaryVector && dim % 8 != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong dimension for Binary Vector: {}, dimension must be a multiple of 8", dim);
    }
}

}
