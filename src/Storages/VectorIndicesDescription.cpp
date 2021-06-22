#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTVectorIndexDeclaration.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/extractKeyExpressionList.h>
#include <Storages/VectorIndicesDescription.h>
#include <Common/quoteString.h>

#include <VectorIndex/VectorIndexFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
};

VectorIndexDescription::VectorIndexDescription(const VectorIndexDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , name(other.name)
    , type(other.type)
    , arguments(other.arguments)
    , parameters(other.parameters)
    , column(other.column)
    , data_type(other.data_type)
    , sample_block(other.sample_block)
    //, granularity(other.granularity)
{
    if (other.expression)
        expression = other.expression->clone();
}


VectorIndexDescription & VectorIndexDescription::operator=(const VectorIndexDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    name = other.name;
    type = other.type;

    arguments = other.arguments;
    column = other.column;
    data_type = other.data_type;
    sample_block = other.sample_block;
    parameters = other.parameters;
    // granularity = other.granularity;
    return *this;
}

bool VectorIndexDescription::operator==(const VectorIndexDescription & other) const
{
    return name == other.name &&
        type == other.type &&
        arguments == other.arguments &&
        data_type == other.data_type &&
        sample_block.equal(other.sample_block) &&
        parameters == other.parameters &&
        definition_ast == other.definition_ast;
}

VectorIndexDescription VectorIndexDescription::getVectorIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
{
    const auto * vec_index_definition = definition_ast->as<ASTVectorIndexDeclaration>();
    if (!vec_index_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create vector index from non ASTVectorIndexDeclaration AST");

    if (vec_index_definition->name.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector index must have name in definition.");

    if (vec_index_definition->column.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector index must have column name in definition.");

    if (!vec_index_definition->type)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TYPE is required for index");

    if (vec_index_definition->type->parameters && !vec_index_definition->type->parameters->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index type cannot have parameters");

    VectorIndexDescription result;
    result.definition_ast = vec_index_definition->clone();
    result.name = vec_index_definition->name;
    result.column = vec_index_definition->column;
    result.data_type = columns.get(result.column).type;

    result.type = Poco::toUpper(vec_index_definition->type->name);
    if (!VectorIndex::VectorIndexFactory::typeExist(result.type)) {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index type {} is unknown", result.type);
    }


    /// currently not used
    const auto & definition_arguments = vec_index_definition->type->arguments;
    if (definition_arguments)
    {
        for (size_t i = 0; i < definition_arguments->children.size(); ++i)
        {
            const auto * argument = definition_arguments->children[i]->as<ASTLiteral>();
            if (!argument)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only literals can be skip index arguments");
            result.arguments.emplace_back(argument->value);
        }
    }

    /// validate for vector index params
    if (result.data_type->getTypeId() != TypeIndex::Array)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector index can be used only with `FixedArray` column.");

    /*
    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(result.data_type.get());
    if (array_type)
    {
        if (array_type->getDim() == 0)
            throw Exception("Vector index can be used only with `FixedArray` column with dim > 0.", ErrorCodes::INCORRECT_QUERY);
    }
*/
    for (const auto & arg : result.arguments)
        if (arg.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters to vector index must be String");


    /// transfer arguments to vector index param
    String param_str;

    /// parse JSON str
    if (result.arguments.size() == 1 && (result.arguments[0].get<String>().find('=')) == String::npos)
    {
        param_str = result.arguments[0].get<String>();
        if ( (param_str.find('{')) == String::npos || (param_str.find('}')) == String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON parameters to vector index must must have a `{` and `}`");
        //LOG_DEBUG(&Poco::Logger::get("test parse arg"), param_str);
    }
    /// parse key-value str
    else
    {
        param_str = "{ ";
        for (auto & arg : result.arguments)
        {
            String argument = arg.get<String>();
            param_str += result.parse_arg(argument);
        }
        param_str += " }";
        size_t comma_index = 0;
        if ((comma_index = param_str.rfind(',')) != String::npos)
            param_str.erase(comma_index, 1);

        //LOG_DEBUG(&Poco::Logger::get("test parse arg"), param_str);
        
    }
    if (result.arguments.size() > 0)
    {
        try
        {
            Poco::JSON::Parser json_parser;
            auto json_res = json_parser.parse(param_str);
            Poco::JSON::Object::Ptr object = json_res.extract<Poco::JSON::Object::Ptr>();
            result.parameters = object;
            // String test = result.parameters->get("metric");
            // LOG_DEBUG(&Poco::Logger::get("test parse arg"), test);
        }
        catch([[maybe_unused]] const std::exception& e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The input JSON's format is illegal ");
        }
        
    }
    return result;
}


String VectorIndexDescription::parse_arg(String & input)
{
    // LOG_DEBUG(&Poco::Logger::get("parse arg"), input);
    size_t index = 0;
    if ( !input.empty())
    {
        while ( (index = input.find(' ',index)) != String::npos)
        {
            input.erase(index, 1);
        }
        
    }
    index = 0;
    size_t number = std::count(input.begin(), input.end(), '=');
    if (number != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key-value String parameter to vector index has only one `=` ");
    index = input.find('=', index);
    String key = input.substr(0, index);
    String value = input.substr(index + 1, input.size() - index - 1);
    bool check_ = true;
    try
    {
        [[maybe_unused]] int test_e = std::stoi(value);
    }
    catch([[maybe_unused]] const std::exception& e)
    {
        check_ = false;
    }
    
    // LOG_DEBUG(&Poco::Logger::get("parse arg"), "{}:{} {}", key, value, check_);
    if (check_)
        return "\"" + key + "\":" + value + ", ";
    return "\"" + key + "\":\"" + value + "\", ";
}

void VectorIndexDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns)
{
    *this = getVectorIndexFromAST(definition_ast, new_columns);
}

bool VectorIndicesDescription::has(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return true;
    return false;
}

String VectorIndicesDescription::toString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
        list.children.push_back(index.definition_ast);

    return serializeAST(list, true);
}


VectorIndicesDescription VectorIndicesDescription::parse(const String & str, const ColumnsDescription & columns)
{
    VectorIndicesDescription result;
    if (str.empty())
        return result;

    ParserVectorIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & index : list->children)
        result.emplace_back(VectorIndexDescription::getVectorIndexFromAST(index, columns));

    return result;
}


ExpressionActionsPtr VectorIndicesDescription::getSingleExpressionForVectorIndices(const ColumnsDescription & columns, ContextPtr context) const
{
    ASTPtr combined_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & index : *this)
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

}
