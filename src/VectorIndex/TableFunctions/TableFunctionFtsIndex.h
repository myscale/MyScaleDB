#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{

/* ftsIndex (db_name, table_name, search_column_name|fts_index_name, query_text [,search_with_index_name = false]) - creates a temporary StorageFtsIndex.
 * The structure of the table is [total_docs UInt64, total_tokens UInt64, terms_freq Array(tuple(text String, field_id UInt32, doc_freq UInt64))].
 * If there is no such table, an exception is thrown.
 */
class TableFunctionFtsIndex : public ITableFunction
{
public:
    static constexpr auto name = "ftsIndex";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "FtsIndex"; }

    StorageID source_table_id{StorageID::createEmpty()};
    bool search_with_index_name = false;
    String search_column_name;
    String fts_index_name;
    String query_text;
};

}
