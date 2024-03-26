#include <Core/Field.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
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

}
