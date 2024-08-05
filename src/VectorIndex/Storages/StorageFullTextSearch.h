#pragma once

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
        const String & index_name,
        const String & query_text,
        const String & score_col_name,
        const bool & enable_nlq,
        const String & text_operator,
        const ColumnsDescription & columns_,
        const ContextPtr & context);

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

    bool supportsPrewhere() const override
    {
        if (nested_storage)
            return nested_storage->supportsPrewhere();
        else
            return true;
    }

    bool supportsFinal() const override { return true; }

    ColumnSizeByName getColumnSizes() const override
    {
        if (nested_storage)
            return nested_storage->getColumnSizes();
        else
            return {};
    }

private:
    StoragePtr nested_storage;

    String index_name;
    String query_text;
    String score_column_name; /// column name for bm25 score
    bool enable_nlq = true;
    String text_operator = "OR";
    Poco::Logger * log;
};

}
