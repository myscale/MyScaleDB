#include <VectorIndex/Storages/StorageFtsIndex.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <VectorIndex/TableFunctions/TableFunctionFtsIndex.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

void TableFunctionFtsIndex::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function ({}) must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    if (args.size() != 4 && args.size() != 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have 4 or 5 arguments, got: {}", getName(), args.size());

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
    args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
    args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);

    if (args.size() == 5)
    {
        const auto * function = args[4]->as<ASTFunction>();
        if (!function || function->name != "equals")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function ({}) failed to get key value from 5th argument", getName());

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
        const auto & function_args = function_args_expr->children;

        if (function_args.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function ({}) failed to get key value from 5th argument", getName());

        auto literal_key = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[0], context);
        if (checkAndGetLiteralArgument<String>(literal_key, "key") != "search_with_index_name")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function ({}) failed to get key 'search_with_index_name' from 5th argument", getName());

        ASTPtr literal_value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
        auto value = literal_value->as<ASTLiteral>()->value;
        if (value.getType() != Field::Types::Bool && value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table function '{}' expected bool flag for 'search_with_index_name' argument", getName());

        if (value.getType() == Field::Types::Bool)
            search_with_index_name = value.safeGet<bool>();
        else
            search_with_index_name = value.safeGet<UInt64>();
    }

    auto database = checkAndGetLiteralArgument<String>(args[0], "database");
    auto table = checkAndGetLiteralArgument<String>(args[1], "table");
    source_table_id = StorageID{database, table};

    if (search_with_index_name)
        fts_index_name = checkAndGetLiteralArgument<String>(args[2], "fts_index_name");
    else
        search_column_name = checkAndGetLiteralArgument<String>(args[2], "search_column_name");
    query_text = checkAndGetLiteralArgument<String>(args[3], "query_text");
}

ColumnsDescription TableFunctionFtsIndex::getActualTableStructure(ContextPtr, bool /*is_insert_query*/) const
{
    ColumnsDescription columns;
    for (const auto & column : StorageFtsIndex::virtuals_sample_block)
        columns.add({column.name, column.type});

    return columns;
}

StoragePtr TableFunctionFtsIndex::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, context);
    auto columns = getActualTableStructure(context, false);

    StorageID storage_id(getDatabaseName(), table_name);
    auto res = std::make_shared<StorageFtsIndex>(std::move(storage_id), std::move(source_table), std::move(columns), search_with_index_name, search_column_name, fts_index_name, query_text);

    res->startup();
    return res;
}

void registerTableFunctionFtsIndex(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFtsIndex>({.documentation = {
        .description=R"(
The table function is used to gather global information about the FTS index on the table. The returned results can serve as essential parameters for the precise calculation of BM25 in distributed text search.

The syntax is ftsIndex(db_name, table_name, search_column_name|fts_index_name, query_text [,search_with_index_name = false])

The parameter search_with_index_name determines whether the third parameter should be search_column_name or fts_index_name. By default, it is set to false and can be omitted.
)",
        .examples={{"ftsIndex", "SELECT * FROM ftsIndex(currentDatabase(), local_table, text_column, 'hello')", ""}}
    }});
}

}
