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

#include <VectorIndex/Storages/StorageFullTextSearch.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/SelectQueryInfo.h>
#include <VectorIndex/Utils/CommonUtils.h>
#include <VectorIndex/Utils/VSUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StorageFullTextSearch::StorageFullTextSearch(
    const StorageID & table_id,
    const StoragePtr & nested_storage_,
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
    , index_name{index_name_}
    , query_text{query_text_}
    , score_column_name{score_col_name}
    , enable_nlq{enable_nlq_}
    , text_operator{text_operator_}
    , query_text_ast{query_text_ast_}
    , log(&Poco::Logger::get("StorageFullTextSearch (" + nested_storage->getStorageID().getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
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

    /// Add score column name in result if not exists
    Names new_column_names = column_names;
    bool has_score_col = false;
    for (const auto & col_name : new_column_names)
    {
        if (col_name == score_column_name)
        {
            has_score_col = true;
            break;
        }
    }

    if (!has_score_col)
        new_column_names.emplace_back(score_column_name);

    const auto & nested_metadata = nested_storage->getInMemoryMetadataPtr();
    auto nested_snapshot = nested_storage->getStorageSnapshot(nested_metadata, context);
    nested_storage->read(query_plan, new_column_names, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

}
