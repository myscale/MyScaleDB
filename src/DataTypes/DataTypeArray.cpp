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
#include <Columns/ColumnConst.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
using FieldType = Array;


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

String DataTypeArray::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "Array(" << nested->getPrettyName(indent) << ')';
    return s.str();
}

void DataTypeArray::forEachChild(const ChildCallback & callback) const
{
    callback(*nested);
    nested->forEachChild(callback);
}

std::unique_ptr<ISerialization::SubstreamData> DataTypeArray::getDynamicSubcolumnData(std::string_view subcolumn_name, const DB::IDataType::SubstreamData & data, bool throw_if_null) const
{
    auto nested_type = assert_cast<const DataTypeArray &>(*data.type).nested;
    auto nested_data = std::make_unique<ISerialization::SubstreamData>(nested_type->getDefaultSerialization());
    nested_data->type = nested_type;
    nested_data->column = data.column ? assert_cast<const ColumnArray &>(*data.column).getDataPtr() : nullptr;

    auto nested_subcolumn_data = nested_type->getSubcolumnData(subcolumn_name, *nested_data, throw_if_null);
    if (!nested_subcolumn_data)
        return nullptr;

    auto creator = SerializationArray::SubcolumnCreator(data.column ? assert_cast<const ColumnArray &>(*data.column).getOffsetsPtr() : nullptr);
    auto res = std::make_unique<ISerialization::SubstreamData>();
    res->serialization = creator.create(nested_subcolumn_data->serialization);
    res->type = creator.create(nested_subcolumn_data->type);
    if (data.column)
        res->column = creator.create(nested_subcolumn_data->column);

    return res;
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
