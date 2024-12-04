#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

const UInt64 FULL_TEXT_SEARCH_DEFULT_LIMIT = 10000;

class StorageFullTextSearch final : public IStorage
{
public:
    StorageFullTextSearch(
        const StorageID & table_id,
        const StoragePtr & nested_storage_,
        const ASTPtr & table_function_ast_,
        const String & index_name,
        const String & query_text,
        const String & score_col_name,
        const bool & enable_nlq,
        const String & text_operator,
        const ColumnsDescription & columns_,
        const ContextPtr & context,
        const ASTPtr & query_text_ast_);

    String getName() const override { return "FullTextSearch"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams) override;

    /// Read from distributed table
    void distributedRead(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & nested_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams,
        UInt64 limit);

    bool supportsPrewhere() const override
    {
        if (nested_storage)
            return nested_storage->supportsPrewhere();
        else
            return true;
    }

    bool supportsFinal() const override { return true; }

    ColumnSizeByName getColumnSizes() const override { return nested_storage->getColumnSizes(); }

    bool isRemote() const override { return nested_storage->isRemote(); }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr local_context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo & query_info) const override
    {
        const auto & nested_metadata = nested_storage->getInMemoryMetadataPtr();
        auto nested_snapshot = nested_storage->getStorageSnapshot(nested_metadata, local_context);
        return nested_storage->getQueryProcessingStage(local_context, to_stage, nested_snapshot, query_info);
    }

private:
    void executeMergeSorted(
        QueryPlan & query_plan,
        ContextPtr context,
        const ASTSelectQuery & select_query,
        UInt64 limit);

    void executeOrder(QueryPlan & query_plan, ContextPtr context, const ASTSelectQuery & select_query);

    StoragePtr nested_storage;
    ASTPtr table_function_ast; /// table function's ast

    String index_name;
    String query_text;
    String score_column_name; /// column name for bm25 score
    bool enable_nlq = true;
    String text_operator = "OR";
    ASTPtr query_text_ast;  /// query text is an identifier (alias name of a WITH statement)
    LoggerPtr log;
};

}
