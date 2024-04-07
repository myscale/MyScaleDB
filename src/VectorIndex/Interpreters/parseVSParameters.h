#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
    String parseVectorScanParameters(const ASTFunction * node, ContextPtr context);
    String parseVectorScanParameters(const ASTFunction * node, ContextPtr context, const String index_type, bool check_parameter);
    String parseVectorScanParameters(const std::vector<String> & vector_scan_parameter, const String index_type, bool check_parameter);
    String parseVectorScanParameters(const Array & parameters, const String index_type, bool check_parameter);
}
