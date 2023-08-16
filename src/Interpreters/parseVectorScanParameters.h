#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

String parseVectorScanParameters(const ASTFunction * node, ContextPtr context);
String parseVectorScanParameters(const ASTFunction * node, ContextPtr context, const String index_type, bool check_parameter);
}
