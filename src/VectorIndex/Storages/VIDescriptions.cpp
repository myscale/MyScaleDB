#include <Storages/IStorage.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/quoteString.h>

#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Interpreters/parseVSParameters.h>
#include <VectorIndex/Parsers/ASTVIDeclaration.h>
#include <VectorIndex/Storages/VIDescriptions.h>

namespace Search
{
enum class DataType;
std::string getDefaultIndexType(const DataType & search_type);
}
namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
};

VIDescription::VIDescription(const VIDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , name(other.name)
    , type(other.type)
    , arguments(other.arguments)
    , parameters(other.parameters)
    , column(other.column)
    , data_type(other.data_type)
    , sample_block(other.sample_block)
    , vector_search_type(other.vector_search_type)
    //, granularity(other.granularity)
{
    if (other.expression)
        expression = other.expression->clone();
}


VIDescription & VIDescription::operator=(const VIDescription & other)
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
    dim = other.dim;
    vector_search_type = other.vector_search_type;
    return *this;
}

bool VIDescription::operator==(const VIDescription & other) const
{
    /// Compare the definition_ast string. In replicated cases, the new metadata is newly construted from log entry,
    /// hence the data_type, parameters, and definition_ast are different.
    /// TODO: May optimize when parameters can be compared.
    String ast_string = serializeAST(*definition_ast);
    String other_ast_string = serializeAST(*(other.definition_ast));
    return ast_string == other_ast_string;
}

VIDescription VIDescription::getVectorIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
{
    return VIDescription::getVectorIndexFromAST(definition_ast, columns, {}, false);
}

VIDescription VIDescription::getVectorIndexFromAST(
    const ASTPtr & definition_ast, const ColumnsDescription & columns, const ConstraintsDescription & constraints, bool check_parameter)
{
    const auto * vec_index_definition = definition_ast->as<ASTVIDeclaration>();
    if (!vec_index_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create vector index from non ASTVIDeclaration AST");

    if (vec_index_definition->name.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector index must have name in definition.");

    if (vec_index_definition->column.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Vector index must have column name in definition.");

    auto vec_index_type = vec_index_definition->getType();
    if (!vec_index_type)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TYPE is required for index");

    if (vec_index_type->parameters && !vec_index_type->parameters->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index type cannot have parameters");

    VIDescription result;
    result.definition_ast = vec_index_definition->clone();
    result.name = vec_index_definition->name;
    result.column = vec_index_definition->column;
    result.data_type = columns.get(result.column).type;
    result.vector_search_type = getSearchIndexDataType(result.data_type);
    result.type = Poco::toUpper(vec_index_type->name) == "DEFAULT" ? Search::getDefaultIndexType(result.vector_search_type)
                                                                               : vec_index_type->name;

    /// check the validity of vector column type
    Search::getVectorIndexType(result.type, result.vector_search_type);

    /// currently not used
    const auto & definition_arguments = vec_index_type->arguments;
    if (definition_arguments)
    {
        for (size_t i = 0; i < definition_arguments->children.size(); ++i)
        {
            const auto * argument = definition_arguments->children[i]->as<ASTLiteral>();
            if (!argument)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only literals can be search index arguments");
            result.arguments.emplace_back(argument->value);
        }
    }

    if (result.vector_search_type == Search::DataType::FloatVector && !constraints.empty())
    {
        result.dim = static_cast<int>(constraints.getArrayLengthByColumnName(result.column).first);
    }
    else if (result.vector_search_type == Search::DataType::BinaryVector)
    {
        const DataTypeFixedString *fixed_string_type = typeid_cast<const DataTypeFixedString *>(result.data_type.get());
        if (fixed_string_type)
            result.dim = static_cast<int>(fixed_string_type->getN() * 8);
    }

    for (const auto & arg : result.arguments)
        if (arg.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters to vector index must be String");


    /// transfer arguments to vector index param
    String param_str;

    Poco::JSON::Parser json_parser;
    auto sass_index_params = json_parser.parse(Search::MYSCALE_VALID_INDEX_PARAMETER);
    Poco::JSON::Object::Ptr sass_index_obj = sass_index_params.extract<Poco::JSON::Object::Ptr>();
    Poco::Dynamic::Var body = sass_index_obj->get(Poco::toUpper(result.type));
    /// parse JSON str
    if (result.arguments.size() == 1 && (result.arguments[0].safeGet<String>().find('=')) == String::npos)
    {
        param_str = result.arguments[0].safeGet<String>();
        if ( (param_str.find('{')) == String::npos || (param_str.find('}')) == String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON parameters to vector index must must have a `{}` and `{}`", "{", "}");
    }
    /// parse key-value str
    else
    {
        param_str = "{ ";
        for (auto & arg : result.arguments)
        {
            String argument = arg.safeGet<String>();
            param_str += result.parse_arg(argument, body.toString(), result.type, result.dim, check_parameter);
        }
        param_str += " }";
        size_t comma_index = 0;
        if ((comma_index = param_str.rfind(',')) != String::npos)
            param_str.erase(comma_index, 1);
    }
    LOG_TRACE(getLogger("get vector index from ast"), "after parameter check, param_str is {}", param_str);
    if (result.arguments.size() > 0)
    {
        try
        {
            auto json_res = json_parser.parse(param_str);
            Poco::JSON::Object::Ptr object = json_res.extract<Poco::JSON::Object::Ptr>();
            result.parameters = object;
        }
        catch ([[maybe_unused]] const std::exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The input JSON's format is illegal ");
        }
    }

    return result;
}


String VIDescription::parse_arg(String & input, const String verify_json, const String index_type, int _dim, bool check_parameter)
{
    size_t index = 0;
    if (!input.empty())
    {
        while ((index = input.find(' ', index)) != String::npos)
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
        size_t idx_int, idx_double;
        std::stoi(value, &idx_int);
        std::stod(value, &idx_double);
        if (idx_int < value.size() && idx_double < value.size())
        {
            check_ = false;
        }
    }
    catch ([[maybe_unused]] const std::exception & e)
    {
        check_ = false;
    }

    // Check whether the index create parameters are valid.
    if (check_parameter)
    {
        Poco::JSON::Parser json_parser;
        auto verify_json_parased = json_parser.parse(verify_json);
        Poco::JSON::Object::Ptr verify_json_obj = verify_json_parased.extract<Poco::JSON::Object::Ptr>();
        auto keys = verify_json_obj->getNames();
        auto it = std::find(keys.begin(), keys.end(), key);
        if (it == keys.end())
        {
            std::string valid_candidates_str = keys.empty()
                ? ""
                : std::accumulate(std::next(keys.begin()), keys.end(), keys[0], [](String a, String b) { return a + ',' + b; });
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} doesn't support index parameter: `{}`, valid parameters is [{}]",
                Poco::toUpper(index_type),
                key,
                valid_candidates_str);
        }
        else
        {
            Poco::JSON::Object::Ptr inner = verify_json_obj->get(key).extract<Poco::JSON::Object::Ptr>();
            // Boundary handling, checking for parameters that are required to be of int type.
            if (inner->get("type").toString() == "int")
            {
                try
                {
                    size_t idx;
                    std::stoi(value, &idx);
                    if (idx < value.size())
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "{} expects an integer value for parameter: `{}`, but got `{}`",
                            Poco::toUpper(index_type),
                            key,
                            value);
                    }
                }
                catch (...)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "{} expects an integer value for parameter: `{}`, but got `{}`",
                        Poco::toUpper(index_type),
                        key,
                        value);
                }
            }
            // Boundary handling, checking for parameters that are required not to be of string type.
            if (inner->get("type").toString() != "string")
            {
                try
                {
                    size_t idx;
                    std::stod(value, &idx);
                    if (idx < value.size())
                    {
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS, "{}: Value for parameter `{}` can't be string", Poco::toUpper(index_type), key);
                    }
                }
                catch (...)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "{}: Value for parameter `{}` can't be string", Poco::toUpper(index_type), key);
                }
            }
            // Checking IVFPQ
            if (Search::getVectorIndexType(index_type, vector_search_type) == Search::IndexType::IVFPQ)
            {
                if (key == "M" && (std::stoi(value) == 0 || _dim == 0 || _dim % std::stoi(value) != 0))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} needs `dim`%`M`==0, `dim`!=0, `M`!=0", Poco::toUpper(index_type));
                }
                if (key == "M" && (std::stoi(value) > _dim || std::stoi(value) < 1))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} needs `M`>=`1` and `M`<=`dim`", Poco::toUpper(index_type));
                }
            }
            auto range_ptr = inner->getArray("range");
            auto candidates_ptr = inner->getArray("candidates");
            bool use_range = inner->get("type").toString() != "string" && range_ptr->size() == 2 && candidates_ptr->size() == 0;
            if (use_range)
            {
                if (std::stod(value) < range_ptr->get(0) || std::stod(value) > range_ptr->get(1))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "{} parameter `{}` range needs to be {}~{}",
                        Poco::toUpper(index_type),
                        key,
                        range_ptr->get(0).convert<String>(),
                        range_ptr->get(1).convert<String>());
                }
            }
            else
            {
                if (inner->get("type") == "string")
                {
                    std::vector<String> candidates;
                    bool case_sensitive = inner->get("case_sensitive").convert<bool>();
                    for (Poco::JSON::Array::ConstIterator _it = candidates_ptr->begin(); _it != candidates_ptr->end(); ++_it)
                    {
                        if (!case_sensitive)
                        {
                            candidates.push_back(Poco::toUpper(_it->convert<String>()));
                        }
                        else
                        {
                            candidates.push_back(_it->convert<String>());
                        }
                    }
                    if (std::find(candidates.begin(), candidates.end(), case_sensitive ? value : Poco::toUpper(value)) == candidates.end())
                    {
                        std::string valid_candidates_str = candidates.empty() ? ""
                                                                              : std::accumulate(
                                                                                  std::next(candidates.begin()),
                                                                                  candidates.end(),
                                                                                  candidates[0],
                                                                                  [](String a, String b) { return a + ", " + b; });
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "{} parameter `{}` should be one of [{}]",
                            Poco::toUpper(index_type),
                            key,
                            valid_candidates_str);
                    }
                }
                else
                {
                    if (std::find(candidates_ptr->begin(), candidates_ptr->end(), std::stof(value)) == candidates_ptr->end())
                    {
                        std::vector<String> candidates;
                        for (Poco::JSON::Array::ConstIterator _it = candidates_ptr->begin(); _it != candidates_ptr->end(); ++_it)
                        {
                            candidates.push_back(_it->convert<String>());
                        }
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "{} parameter `{}` should be one of [{}]",
                            Poco::toUpper(index_type),
                            key,
                            std::accumulate(
                                std::next(candidates.begin()),
                                candidates.end(),
                                candidates[0],
                                [](String a, String b) { return a + ", " + b; }));
                    }
                }
            }
        }
    }

    if (check_)
        return "\"" + key + "\":" + value + ", ";
    return "\"" + key + "\":\"" + value + "\", ";
}

void VIDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns)
{
    *this = getVectorIndexFromAST(definition_ast, new_columns);
}

bool VIDescriptions::has(const String & name) const
{
    for (const auto & index : *this)
        if (index.name == name)
            return true;
    return false;
}

bool VIDescriptions::has(const VIDescription & vec_index_desc) const
{
    for (const auto & index : *this)
        if (index == vec_index_desc)
            return true;
    return false;
}

String VIDescriptions::toString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & index : *this)
        list.children.push_back(index.definition_ast);

    return serializeAST(list);
}


VIDescriptions VIDescriptions::parse(const String & str, const ColumnsDescription & columns)
{
    VIDescriptions result;
    if (str.empty())
        return result;

    ParserVectorIndexDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & index : list->children)
        result.emplace_back(VIDescription::getVectorIndexFromAST(index, columns));

    return result;
}


ExpressionActionsPtr VIDescriptions::getSingleExpressionForVectorIndices(const ColumnsDescription & columns, ContextPtr context) const
{
    ASTPtr combined_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & index : *this)
        for (const auto & index_expr : index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

}
