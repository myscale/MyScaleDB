#include <VectorIndex/TableFunctions/TableFunctionFullTextSearch.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <VectorIndex/Storages/StorageFullTextSearch.h>
#include <VectorIndex/Utils/CommonUtils.h>

#if USE_TANTIVY_SEARCH
#    include <Interpreters/TantivyFilter.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TEXT_SEARCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

inline void checkTantivyIndex([[maybe_unused]]const StoragePtr & storage, [[maybe_unused]]const String & text_index_name, [[maybe_unused]]const String & table_name)
{
    bool find_tantivy_index = false;

#if USE_TANTIVY_SEARCH
    auto metadata_snapshot =  storage ? storage->getInMemoryMetadataPtr() : nullptr;

    if (metadata_snapshot)
    {
        for (const auto & index_desc : metadata_snapshot->getSecondaryIndices())
        {
            /// Find tantivy inverted index with the name as text_index_name
            if (index_desc.type == TANTIVY_INDEX_NAME && index_desc.name == text_index_name)
            {
                find_tantivy_index = true;
                break;
            }
        }
    }
#endif

    if (!find_tantivy_index)
        throw Exception(ErrorCodes::ILLEGAL_TEXT_SEARCH, "The FTS index `{}` doesn't exist in table {}", text_index_name, table_name);
}

}

StoragePtr TableFunctionFullTextSearch::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name_, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    auto columns = getActualTableStructure(context, false);

    /// Use real score column name no matter with_score is true or false
    String score_col_name = SCORE_COLUMN_NAME;

    auto storage = std::make_shared<StorageFullTextSearch>(
        StorageID(getDatabaseName(), table_name_), table_storage, ast_function, index_name, query_text, score_col_name,
        enable_nlq, text_operator, columns, context, query_text_ast);
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionFullTextSearch::getActualTableStructure(ContextPtr /* context */, bool /*is_insert_query*/) const
{
    /// Get table structure of underlying table and full-text search related columns
    ColumnsDescription columns_description;
    if (table_storage)
        columns_description = ColumnsDescription{table_storage->getInMemoryMetadataPtr()->getColumns()};
    
    if (with_score)
    {
        ColumnDescription score_col;
        score_col.name = SCORE_COLUMN_NAME;
        score_col.type = std::make_shared<DataTypeFloat32>();
        columns_description.add(score_col);
    }

    return columns_description;
}

void TableFunctionFullTextSearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = func_args.arguments->children;

    if (args.size() < 3 || args.size() > 6)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires 3 to 6 arguments: "
            "table_name, index_name, query, with_score (default 0), enable_nlq (default true), operator (default OR)", getName());

    for (size_t i = 0; i < 2; i++)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    String tmp_table_name = checkAndGetLiteralArgument<String>(args[0], "table_name");
    index_name = checkAndGetLiteralArgument<String>(args[1], "index_name");

    /// Special handling for query text which maybe WITH statement
    if (const auto * identifier = args[2]->as<ASTIdentifier>())
    {
        /// WITH statement, will be replaced in StorageFullTextSearch::read()
        query_text_ast = args[2];
    }
    else
    {
        /// Literal or subquery
        args[2] = evaluateConstantExpressionAsLiteral(args[2], context);
        query_text = checkAndGetLiteralArgument<String>(args[2], "query");
    }

    /// Support 'key=value' format for optional arguments
    for (size_t i = 3; i < args.size(); ++i)
    {
        if (const auto * ast_func = typeid_cast<const ASTFunction *>(args[i].get()))
        {
            const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_func->arguments.get());
            auto function_args = args_expr->children;
            if (function_args.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

            auto arg_name = function_args[0]->as<ASTIdentifier>()->name();

            auto eval_arg = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);

            if (arg_name == "with_score")
                with_score = checkAndGetLiteralArgument<bool>(eval_arg, "with_score");
            else if (arg_name == "enable_nlq")
                enable_nlq = checkAndGetLiteralArgument<bool>(eval_arg, "enable_nlq");
            else if (arg_name == "operator")
                text_operator = checkAndGetLiteralArgument<String>(eval_arg, "operator");
        }
        else
        {
            args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);
            if (i == 3)
                with_score = checkAndGetLiteralArgument<bool>(args[i], "with_score");
            else if (i == 4)
                enable_nlq = checkAndGetLiteralArgument<bool>(args[i], "enable_nlq");
            else if (i == 5)
                text_operator = checkAndGetLiteralArgument<String>(args[i], "operator");
        }
    }

    /// Valid check for arguments
    if (text_operator != "OR" && text_operator != "AND")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of table function '{}' argument `operator` should be OR or AND", getName());

    /// table_name may be db.table
    size_t found_pos = tmp_table_name.find(".");
    if (found_pos == std::string::npos)
    {
        database_name = context->getCurrentDatabase();
        table_name = tmp_table_name;
    }
    else
    {
        database_name = tmp_table_name.substr(0, found_pos);
        table_name = tmp_table_name.substr(found_pos + 1);
    }

    table_storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), context);

    /// Check if the index exists in the table
    /// Skip the fts index check when table is distributed.
    if (table_storage && !table_storage->isRemote())
        checkTantivyIndex(table_storage, index_name, tmp_table_name);

    /// Check if table has same column name with score column
    auto metadata_snapshot =  table_storage ? table_storage->getInMemoryMetadataPtr() : nullptr;
    if (metadata_snapshot)
    {
        auto column_names = metadata_snapshot->getColumns().getNamesOfPhysical();
        for (const auto & col_name : column_names)
        {
            if (col_name == SCORE_COLUMN_NAME)
                throw Exception(ErrorCodes::ILLEGAL_TEXT_SEARCH,
                                "Disallow to execute table function '{}' on table {} containing the same column name as the bm25 score column({})",
                                getName(), tmp_table_name, SCORE_COLUMN_NAME);
        }
    }
}

void registerTableFunctionFullTextSearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFullTextSearch>();
}

}
