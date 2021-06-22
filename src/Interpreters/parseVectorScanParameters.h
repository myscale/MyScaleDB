#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

String parseVectorScanParameters(const ASTFunction * node, ContextPtr context);

}
