#include <Interpreters/parseVectorScanParameters.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String parse_arg(String & input)
{
    // LOG_DEBUG(&Poco::Logger::get("parse arg"), input);
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
        [[maybe_unused]] int test_e = std::stoi(value);
    }
    catch ([[maybe_unused]] const std::exception & e)
    {
        check_ = false;
    }

    // LOG_DEBUG(&Poco::Logger::get("parse arg"), "{}:{} {}", key, value, check_);
    if (check_)
        return "\"" + key + "\":" + value + ", ";
    return "\"" + key + "\":\"" + value + "\", ";
}

String parseVectorScanParameters(const ASTFunction * node, ContextPtr context)
{
    Array parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters, "", context) : Array();

    for (const auto & arg : parameters)
        if (arg.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters to vector scan must be String or JSON String");

    /// transfer array param to vector string param
    String param_str;
    /// parse JSON str
    if (parameters.size() == 1 && (parameters[0].get<String>().find('=')) == String::npos)
    {
        param_str = parameters[0].get<String>();
        if ((param_str.find('{')) == String::npos || (param_str.find('}')) == String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON parameters to vector index must must have a `{` and `}`");
        //LOG_DEBUG(&Poco::Logger::get("test parse arg"), param_str);
    }
    else
    {
        param_str = "{ ";
        for (auto & arg : parameters)
        {
            String argument = arg.get<String>();
            param_str += parse_arg(argument);
        }
        param_str += " }";
        size_t comma_index = 0;
        if ((comma_index = param_str.rfind(',')) != String::npos)
            param_str.erase(comma_index, 1);

        //LOG_DEBUG(&Poco::Logger::get("test parse arg"), param_str);
    }
    return param_str;
}
}
