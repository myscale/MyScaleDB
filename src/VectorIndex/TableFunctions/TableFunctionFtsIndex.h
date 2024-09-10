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
    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "FtsIndex"; }

    StorageID source_table_id{StorageID::createEmpty()};
    bool search_with_index_name = false;
    String search_column_name;
    String fts_index_name;
    String query_text;
};

}
