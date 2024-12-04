#include <VectorIndex/Storages/StorageFullTextSearch.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>
#include <VectorIndex/Utils/CommonUtils.h>
#include <VectorIndex/Utils/VSUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

StorageFullTextSearch::StorageFullTextSearch(
    const StorageID & table_id,
    const StoragePtr & nested_storage_,
    const ASTPtr & table_function_ast_,
    const String & index_name_,
    const String & query_text_,
    const String & score_col_name,
    const bool & enable_nlq_,
    const String & text_operator_,
    const ColumnsDescription & columns_,
    const ContextPtr & /* context_ */,
    const ASTPtr & query_text_ast_)
    : IStorage(table_id)
    , nested_storage(nested_storage_)
    , table_function_ast(table_function_ast_)
    , index_name{index_name_}
    , query_text{query_text_}
    , score_column_name{score_col_name}
    , enable_nlq{enable_nlq_}
    , text_operator{text_operator_}
    , query_text_ast{query_text_ast_}
    , log(getLogger("StorageFullTextSearch (" + nested_storage->getStorageID().getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    setVirtuals(*nested_storage->getVirtualsPtr());
}

void StorageFullTextSearch::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    /// Construct TextSearchInfo for full text search
    const auto * select_query = query_info.query->as<ASTSelectQuery>();

    /// Get topK from limit N
    UInt64 limit_length = getTopKFromLimit(select_query, context);

    if (limit_length == 0)
    {
        LOG_DEBUG(log, "Use default limit value {}", FULL_TEXT_SEARCH_DEFULT_LIMIT);
        limit_length = FULL_TEXT_SEARCH_DEFULT_LIMIT;
    }

    /// Handle query text as an identifier of a WITH statement
    if (query_text_ast)
    {
        bool found = false;
        if (const auto * identifier = query_text_ast->as<ASTIdentifier>())
        {
            String query_text_ident_name = identifier->name();

            if (select_query && select_query->with())
            {
                /// Find matched with statement
                for (const auto & child : select_query->with()->children)
                {
                    if (child->getAliasOrColumnName() == query_text_ident_name)
                    {
                        ASTPtr literal = evaluateConstantExpressionAsLiteral(child, context);
                        query_text = checkAndGetLiteralArgument<String>(literal, "query");
                        found = true;
                    }
                }
            }
        }

        if (!found)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong const query text type for argument {} in table function 'full_text_search'", query_text_ast->getColumnName());
    }

    query_info.text_search_info = std::make_shared<TextSearchInfo>(
                true, index_name, query_text, score_column_name, static_cast<int>(limit_length), text_operator, enable_nlq);
    query_info.has_hybrid_search = true;

    const auto & nested_metadata = nested_storage->getInMemoryMetadataPtr();
    auto nested_snapshot = nested_storage->getStorageSnapshot(nested_metadata, context);

    /// Special handling for full_text_search table function on distributed table cases
    if (isRemote())
    {
        return distributedRead(query_plan, column_names, nested_snapshot, query_info, context, processed_stage,
                        max_block_size, num_streams, limit_length);
    }

    /// Add score column name in result if not exists
    Names new_column_names = column_names;
    if (std::find(column_names.begin(), column_names.end(), score_column_name) == column_names.end())
        new_column_names.emplace_back(score_column_name);

    /// Read from non-distributed table
    nested_storage->read(query_plan, new_column_names, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

void StorageFullTextSearch::distributedRead(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & nested_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    UInt64 limit)
{
    auto nested_distributed = std::dynamic_pointer_cast<StorageDistributed>(nested_storage);

    if (!nested_distributed)
        return;

    size_t shard_count = nested_distributed->getShardCount();

    /// Make clone for query
    ASTPtr modified_query_ast = query_info.query->clone();
    ASTSelectQuery & modified_select_query = modified_query_ast->as<ASTSelectQuery &>();

    ASTPtr table_function = extractTableExpression(modified_select_query, 0);
    if (!table_function)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table function expected");

    ASTFunction & orignal_table_func = table_function->as<ASTFunction &>();

    /// When distributed table has at least two shards, or query doesn't contain order by clause,
    /// we need to sort based on score_column_name column after read, hence table function should be with_score=1
    if (shard_count > 1 || !modified_select_query.orderBy())
    {
        if (!getInMemoryMetadataPtr()->columns.has(score_column_name))
        {
            /// Add "with_score=1" to function arguments
            orignal_table_func.arguments->children.push_back(
                makeASTFunction("equals", std::make_shared<ASTIdentifier>("with_score"), std::make_shared<ASTLiteral>(static_cast<UInt64>(1))));
        }

        /// Change the order by clause to use bm25_score DESC
        ASTPtr order_by_bm25_ast = std::make_shared<ASTExpressionList>();
        auto bm25_score_order = std::make_shared<ASTOrderByElement>();
        bm25_score_order->direction = -1;
        bm25_score_order->children.emplace_back(std::make_shared<ASTIdentifier>(score_column_name));

        order_by_bm25_ast->children.push_back(std::move(bm25_score_order));

        modified_select_query.setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_bm25_ast));

        /// Add bm25_score to select list if not exists, only needed for at least two shards cases
        if (shard_count > 1)
        {
            if (std::find(column_names.begin(), column_names.end(), score_column_name) == column_names.end())
                modified_select_query.select()->children.push_back(std::make_shared<ASTIdentifier>(score_column_name));
        }
    }

    /// Prepare for table function with remote_database and remote table
    ASTPtr modified_table_func_ast = table_function->clone();
    ASTFunction & modified_table_func = modified_table_func_ast->as<ASTFunction &>();

    /// Replace the first argument to remote_database and remote table
    ASTs & args = modified_table_func.arguments->children;
    args[0] = std::make_shared<ASTIdentifier>(std::vector<String>{nested_distributed->getRemoteDatabaseName(), nested_distributed->getRemoteTableName()});

    /// Save modified table function ast to query_info
    query_info.full_text_search_table_func_ast = std::move(modified_table_func_ast);

    /// Save modified query ast to query_info
    ASTPtr bak_query_distributed = query_info.query;
    query_info.query = modified_query_ast;

    nested_distributed->read(query_plan, column_names, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    /// Restore query
    query_info.query = bak_query_distributed;

    if (nested_distributed->getShardCount() > 1)
    {
        /// Sort results from shards based on 'score_column_name' column
        executeMergeSorted(query_plan, context, modified_select_query, limit);

        /// If original query contains order by clause, add a sort step
        const auto & select_query = query_info.query->as<ASTSelectQuery &>();
        if (select_query.orderBy())
            executeOrder(query_plan, context, select_query);
    }
}

void StorageFullTextSearch::executeMergeSorted(
    QueryPlan & query_plan,
    ContextPtr context,
    const ASTSelectQuery & select_query,
    UInt64 limit)
{
    SortDescription sort_description = InterpreterSelectQuery::getSortDescription(select_query, context);
    const auto max_block_size = context->getSettingsRef().max_block_size;
    const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;

    auto merging_sorted = std::make_unique<SortingStep>(
        query_plan.getCurrentDataStream(), std::move(sort_description), max_block_size, limit, exact_rows_before_limit);
    merging_sorted->setStepDescription("Merge sorted streams for ORDER BY bm25 score after distributed read");
    query_plan.addStep(std::move(merging_sorted));
}

void StorageFullTextSearch::executeOrder(QueryPlan & query_plan, ContextPtr context, const ASTSelectQuery & select_query)
{
    SortDescription order_descr = InterpreterSelectQuery::getSortDescription(select_query, context);

    const Settings & settings = context->getSettingsRef();
    SortingStep::Settings sort_settings(*context);

    /// Merge the sorted blocks
    auto sorting_step = std::make_unique<SortingStep>(
        query_plan.getCurrentDataStream(),
        order_descr,
        0 /* limit */,
        sort_settings,
        settings.optimize_sorting_by_input_stream_properties);

    sorting_step->setStepDescription("Sorting for ORDER BY");
    query_plan.addStep(std::move(sorting_step));
}
}
