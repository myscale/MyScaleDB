#pragma once

#include <Analyzer/ColumnNode.h>
#include <Core/IResolvedFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <VectorIndex/Utils/CommonUtils.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

class SpecialSearchFunction;
using SpecialSearchFunctionPtr = std::shared_ptr<SpecialSearchFunction>;
using ConstSpecialSearchFunctionPtr = std::shared_ptr<const SpecialSearchFunction>;

/** Special search functions interface.
  * Instances of classes with this interface do not contain the data itself for special search,
  *  but contain only metadata (description) of the special search function,
  *  as well as methods for creating, deleting and working with data.
  * The data resulting from the aggregation (intermediate computing states) is stored in other objects
  *  (which can be created in some memory pool),
  *  and IAggregateFunction is the external interface for manipulating them.
  */
class SpecialSearchFunction : public std::enable_shared_from_this<SpecialSearchFunction>, public IResolvedFunction
{
public:
    SpecialSearchFunction(const String & name_, const DataTypes & argument_types_, const Array & parameters_, const ColumnsWithTypeAndName & argument_columns_)
        : name(name_)
        , argument_types(argument_types_)
        , parameters(parameters_)
        , argument_columns(argument_columns_)
    {
        if (isBatchDistance(name))
        {
            auto id_type = std::make_shared<DataTypeUInt32>();
            auto distance_type = std::make_shared<DataTypeFloat32>();
            DataTypes types;
            types.emplace_back(id_type);
            types.emplace_back(distance_type);
            result_type = std::make_shared<DataTypeTuple>(types);
        }
        else
            result_type = std::make_shared<DataTypeFloat32>();
    }

    String getName() const { return name; }

    ~SpecialSearchFunction() override = default;

    const DataTypePtr & getResultType() const override { return result_type; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const Array & getParameters() const override { return parameters; }

protected:
    String name;
    DataTypes argument_types;
    Array parameters;
    DataTypePtr result_type;
    ColumnsWithTypeAndName argument_columns;
};

}
