/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{

class Context;

/*
 * full_text_search(table_name, index_name, query[, with_score]) - creates a temporary StorageFullTextSearch.
 * The structure of the table is taken from columns in table_name plus other full-text search columns
 * does full text search on table's tantivy index with query
 */
class TableFunctionFullTextSearch : public ITableFunction
{
public:
    static constexpr auto name = "full_text_search";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const override;

    const char * getStorageTypeName() const override { return "FullTextSearch"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String database_name; /// database name of table with tantivy index
    String table_name; /// table name with tantivy index
    String index_name; /// index name of tantivy index
    String query_text; /// query text for full-text search
    bool with_score = false;  /// If true, tantivy_score_bm25 column exists in table structure
    bool enable_nlq = true; /// If true, enable natural language query
    String text_operator = "OR"; /// Boolean logic used to interpret text in the query value

    ASTPtr query_text_ast = nullptr; /// query text is an identifier (alias name of a WITH statement)
    StoragePtr table_storage; /// storage of table
};

}
