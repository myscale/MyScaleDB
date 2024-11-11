#include <numeric>
#include <Core/Field.h>
#include <VectorIndex/Interpreters/parseVSParameters.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Parser.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wfloat-conversion"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#pragma clang diagnostic ignored "-Wcovered-switch-default"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-function"
#include <SearchIndex/VectorSearch.h>
#pragma clang diagnostic pop
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String parse_arg(String & input, const String index_type, bool check_parameter)
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
    if (key.empty() || value.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "parameters' key or value may be empty");
    }
    bool check_ = true;
    try
    {
        [[maybe_unused]] int _to_int = std::stoi(value);
        [[maybe_unused]] float _to_float = std::stof(value);
    }
    catch ([[maybe_unused]] const std::exception & e)
    {
        check_ = false;
    }
    // Check whether the search parameters are valid.
    if (index_type != "" && check_parameter)
    {
        Poco::JSON::Parser json_parser;
        auto sass_index_params = json_parser.parse(Search::MYSCALE_VALID_INDEX_PARAMETER);
        Poco::JSON::Object::Ptr parameters_obj = sass_index_params.extract<Poco::JSON::Object::Ptr>();

        // Boundary handling to avoid performing vector search on unsupported indices.
        auto keys = parameters_obj->getNames();
        auto it = std::find(keys.begin(), keys.end(), index_type);
        if (it == keys.end())
        {
            std::string valid_index_type = keys.empty()
                ? ""
                : std::accumulate(std::next(keys.begin()), keys.end(), keys[0], [](String a, String b) { return a + ',' + b; });
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Vector search is not supported on `{}`, valid index type is [{}]",
                index_type,
                valid_index_type);
        }

        // Boundary handling, throwing an error for unsupported index search parameters.
        Poco::JSON::Object::Ptr inner = parameters_obj->get(Poco::toUpper(index_type)).extract<Poco::JSON::Object::Ptr>();
        auto inner_keys = inner->getNames();
        auto inner_it = std::find(inner_keys.begin(), inner_keys.end(), key);
        if (inner_it == inner_keys.end())
        {
            std::string valid_search_arguments = inner_keys.empty()
                ? ""
                : std::accumulate(
                    std::next(inner_keys.begin()), inner_keys.end(), inner_keys[0], [](String a, String b) { return a + ',' + b; });
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unsupported vector search argument `{}` on vector index `{}`, supported arguments is [{}]",
                key,
                index_type,
                valid_search_arguments);
        }
        // Boundary handling, checking for parameters that are required to be of int type.
        Poco::JSON::Object::Ptr single_parameter = inner->get(key).extract<Poco::JSON::Object::Ptr>();
        if (single_parameter->get("type").toString() == "int")
        {
            try
            {
                size_t idx;
                std::stoi(value, &idx);
                if (idx < value.size())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expects an integer value for parameter: `{}`, but got `{}`", key, value);
                }
            }
            catch (...)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expects an integer value for parameter: `{}`, but got `{}`", key, value);
            }
        }
        // Boundary handling, checking that the parameter value cannot be of string type.
        if (single_parameter->get("type").toString() != "string")
        {
            try
            {
                size_t idx;
                std::stof(value, &idx);
                if (idx < value.size())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value for parameter `{}` can't be string", key);
                }
            }
            catch (...)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value for parameter `{}` can't be string", key);
            }
        }
        // Validate the range of search parameter values.
        auto candidates_obj = single_parameter->getArray("candidates");
        auto range_obj = single_parameter->getArray("range");
        bool use_range = single_parameter->get("type").toString() != "string" && range_obj->size() == 2 && candidates_obj->size() == 0;
        if (use_range)
        {
            if (std::stof(value) < range_obj->get(0) || std::stof(value) > range_obj->get(1))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Value for parameter `{}` range needs to be {}~{}",
                    key,
                    range_obj->get(0).convert<String>(),
                    range_obj->get(1).convert<String>());
            }
        }
        else
        {
            if (single_parameter->get("type") == "string")
            {
                std::vector<String> candidates;
                bool case_sensitive = single_parameter->get("case_sensitive").convert<bool>();
                for (Poco::JSON::Array::ConstIterator _it = candidates_obj->begin(); _it != candidates_obj->end(); ++_it)
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
                if (!candidates.empty()
                    && std::find(candidates.begin(), candidates.end(), case_sensitive ? value : Poco::toUpper(value)) == candidates.end())
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Value for parameter `{}` should be one of [{}]",
                        key,
                        std::accumulate(
                            std::next(candidates.begin()),
                            candidates.end(),
                            candidates[0],
                            [](String a, String b) { return a + ',' + b; }));
                }
            }
            else
            {
                if (candidates_obj->size() > 0
                    && std::find(candidates_obj->begin(), candidates_obj->end(), std::stof(value)) == candidates_obj->end())
                {
                    std::vector<String> candidates;
                    for (Poco::JSON::Array::ConstIterator _it = candidates_obj->begin(); _it != candidates_obj->end(); ++_it)
                    {
                        candidates.push_back(_it->convert<String>());
                    }
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Value for parameter `{}` should be one of [{}]",
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

    if (check_)
        return "\"" + key + "\":" + value + ", ";
    return "\"" + key + "\":\"" + value + "\", ";
}

String parseVectorScanParameters(const ASTFunction * node, ContextPtr context)
{
    return parseVectorScanParameters(node, context, "", false);
}

String parseVectorScanParameters(const ASTFunction * node, ContextPtr context, const String index_type, bool check_parameter)
{
    Array parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters, "", context) : Array();
    return parseVectorScanParameters(parameters, index_type, check_parameter);
}

/// vector scan parameters from hybrid search
String parseVectorScanParameters(const std::vector<String> & vector_scan_parameter, const String index_type, bool check_parameter)
{
    Array params_row(vector_scan_parameter.size());
    for (size_t i = 0; i < vector_scan_parameter.size(); ++i)
        params_row[i] = vector_scan_parameter[i];

    return parseVectorScanParameters(params_row, index_type, check_parameter);
}

String parseVectorScanParameters(const Array & parameters, const String index_type, bool check_parameter)
{
    for (const auto & arg : parameters)
        if (arg.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters to vector scan must be String or JSON String");

    /// transfer array param to vector string param
    String param_str;
    /// parse JSON str
    if (parameters.size() == 1 && (parameters[0].safeGet<String>().find('=')) == String::npos)
    {
        param_str = parameters[0].safeGet<String>();
        if ((param_str.find('{')) == String::npos || (param_str.find('}')) == String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON parameters to vector index must must have a `{}` and `{}`", "{", "}");
    }
    else
    {
        param_str = "{ ";
        for (auto & arg : parameters)
        {
            String argument = arg.safeGet<String>();
            param_str += parse_arg(argument, index_type, check_parameter);
        }
        param_str += " }";
        size_t comma_index = 0;
        if ((comma_index = param_str.rfind(',')) != String::npos)
            param_str.erase(comma_index, 1);
    }
    return param_str;
}
}
