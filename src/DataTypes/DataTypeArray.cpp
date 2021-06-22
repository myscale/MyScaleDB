#include <Columns/ColumnArray.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationArray.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_}
{
}

DataTypeArray::DataTypeArray(const DataTypePtr & nested_, const uint64_t dim_)
    : nested{nested_}, dim{dim_}
{
}


MutableColumnPtr DataTypeArray::createColumn() const
{
    if (dim > 0)
    {
        auto column = ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create());
        column->setDim(dim);
        return column;
    }
    else
    {
        return ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create());
    }
}


Field DataTypeArray::getDefault() const
{
    return Array();
}


bool DataTypeArray::equals(const IDataType & rhs) const
{
    if(typeid(rhs) != typeid(*this)){
        return false;
    }
    const DataTypeArray* other = dynamic_cast<const DataTypeArray*>(&rhs);
    if(other!= nullptr){
        return other->dim == dim && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
    } else{
        return false;
    }
}

SerializationPtr DataTypeArray::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationArray>(nested->getDefaultSerialization());
}

size_t DataTypeArray::getNumberOfDimensions() const
{
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->getNumberOfDimensions();   /// Every modern C++ compiler optimizes tail recursion.
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Array data type family must have exactly one argument - type of elements");

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]));
}

static DataTypePtr createFixed(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Array data type family must have exactly tow argument - dimension and type of elements");

    if (const auto * ast_literal = typeid_cast<const ASTLiteral *>(arguments->children[1].get()))
    {
        uint64_t dim = ast_literal->value.get<UInt64>();
        return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]), dim);
    }
    else
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Fixed array data type family must have exactly two argument - type of elements and dimension");
    }
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("Array", create);
}

void registerDataTypeFixedArray(DataTypeFactory & factory)
{
    factory.registerDataType("FixedArray", createFixed);
}

}
