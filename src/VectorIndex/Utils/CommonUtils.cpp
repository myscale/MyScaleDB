#include <Core/Field.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Common/logger_useful.h>

#include <SearchIndex/SearchIndexCommon.h>
#include <VectorIndex/Storages/VSDescription.h>
#include <VectorIndex/Utils/CommonUtils.h>

#if USE_TANTIVY_SEARCH
#    include <Columns/ColumnConst.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/IColumn.h>
#    include <Interpreters/InterpreterSelectWithUnionQuery.h>
#    include <Parsers/ASTExpressionList.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/ASTSelectQuery.h>
#    include <Parsers/ASTSelectWithUnionQuery.h>
#    include <Parsers/ASTTablesInSelectQuery.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <VectorIndex/Storages/StorageFtsIndex.h>
#endif


namespace Search
{
enum class DataType;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int QUERY_WAS_CANCELLED;
}

Search::DataType getSearchIndexDataType(DataTypePtr &data_type)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::Array:
        {
            const DataTypeArray *array_type = typeid_cast<const DataTypeArray *>(data_type.get());
            if (array_type)
            {
                WhichDataType which(array_type->getNestedType());
                if (!which.isFloat32())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The element type inside the array must be `Float32`");
                return Search::DataType::FloatVector;
            }
            break;
        }
        case TypeIndex::FixedString:
            return Search::DataType::BinaryVector;
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Vector search can be used with `Array(Float32)` or `FixedString` column");
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Vector search Type");
}

void checkVectorDimension(const Search::DataType & search_type, const uint64_t & dim)
{
    if (search_type == Search::DataType::FloatVector && dim == 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong dimension for Float32 Vector: 0, please check length constraint on search column");
    }
    /// BinaryVector is represented as FixedString(N), N > 0 has already been verified
    else if (search_type == Search::DataType::BinaryVector && dim % 8 != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong dimension for Binary Vector: {}, dimension must be a multiple of 8", dim);
    }
}

void checkTextSearchColumnDataType(DataTypePtr &data_type, bool & is_mapKeys)
{
    switch (data_type->getTypeId())
    {
        case TypeIndex::String:
            break;
        case TypeIndex::Array:
        {
            const DataTypeArray *array_type = typeid_cast<const DataTypeArray *>(data_type.get());
            if (array_type)
            {
                WhichDataType which(array_type->getNestedType());
                if (!which.isString())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "The element type inside the array must be `String` for text search column");
            }
            else
                throw Exception(ErrorCodes::INCORRECT_DATA, "Search text column type is incorrect Array type");
            break;
        }
        case TypeIndex::Map:
        {
            const DataTypeMap *map_type = typeid_cast<const DataTypeMap *>(data_type.get());
            if (map_type)
            {
                WhichDataType which_key(map_type->getKeyType());
                WhichDataType which_value(map_type->getValueType());
                if (!is_mapKeys || !which_key.isString() || !which_value.isString())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "When the text search column type is map, the key and value must be strings. Additionally, the mapKeys() function must be used to process the map column");
            }
            else
                throw Exception(ErrorCodes::INCORRECT_DATA, "Search text column type is incorrect Map type");
            break;
        }
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Text search can be used with `String`, `Array(String)` or `Map(String, String)` column");
    }
}

#if USE_TANTIVY_SEARCH

/*
 * Build an AST for collecting BM25 statistics with the following structure:
 *
 * SelectWithUnionQuery (children 1)
 *  ExpressionList (children 1)
 *   SelectQuery (children 2)
 *    ExpressionList (children 1)
 *     Identifier total_docs
 *     Identifier total_tokens
 *     Identifier terms_freq
 *    TablesInSelectQuery (children 1)
 *     TablesInSelectQueryElement (children 1)
 *      TableExpression (children 1)
 *       Function <...>
 *
 */
inline ASTPtr buildCollectionFromFunction(const std::shared_ptr<ASTFunction> & ast_function)
{
    auto select_ast = std::make_shared<ASTSelectQuery>();
    auto projection_ast = std::make_shared<ASTExpressionList>();
    projection_ast->children.push_back(std::make_shared<ASTIdentifier>("total_docs"));
    projection_ast->children.push_back(std::make_shared<ASTIdentifier>("field_tokens"));
    projection_ast->children.push_back(std::make_shared<ASTIdentifier>("terms_freq"));

    select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection_ast));

    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->children.push_back(std::move(ast_function));
    table_expr->table_function = table_expr->children.back();

    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_elem->children.push_back(std::move(table_expr));
    tables_elem->table_expression = tables_elem->children.back();

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    tables->children.push_back(std::move(tables_elem));

    select_ast->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_ast));

    result_select_query->children.push_back(std::move(list_of_selects));
    result_select_query->list_of_selects = result_select_query->children.back();

    return result_select_query;
}

void collectStatisticForBM25Calculation(ContextMutablePtr & context, String cluster_name, String database_name, String table_name, String query_column_name, String query_text)
{
    if (context->hasScalar("_fts_statistic_info"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to collect bm25 statistics info, the _fts_statistic_info scalar is already exists.");

    auto fts_index_function = makeASTFunction(
        "ftsIndex",
        std::make_shared<ASTIdentifier>(database_name),
        std::make_shared<ASTIdentifier>(table_name),
        std::make_shared<ASTLiteral>(query_column_name),
        std::make_shared<ASTLiteral>(query_text));

    auto cluster_fts_index_function = makeASTFunction("cluster", std::make_shared<ASTIdentifier>(cluster_name), fts_index_function);

    /// Create an AST for collecting BM25 statistics using a distributed SQL query
    /// The SQL is similar to: "SELECT total_docs, field_tokens, terms_freq FROM cluster('cluster_name', ftsIndex('database_name', 'table_name', 'query_column_name', 'query_text'))"
    auto collection_query = buildCollectionFromFunction(cluster_fts_index_function);

    std::unique_ptr<InterpreterSelectWithUnionQuery> interpreter
        = std::make_unique<InterpreterSelectWithUnionQuery>(collection_query, Context::createCopy(context), SelectQueryOptions());

    auto io = interpreter->execute();
    PullingPipelineExecutor executor(io.pipeline);

    UInt64 total_docs = 0;
    std::map<UInt32, UInt64> total_tokens_map;
    std::map<std::pair<UInt32, String>, UInt64> terms_freq_map;

    Block block;
    while (executor.pull(block))
    {
        for (size_t row = 0; row < block.rows(); row++)
        {
            parseBM25StaisiticsInfo(block, row, total_docs, total_tokens_map, terms_freq_map);
        }
    }

    Columns total_tokens_tuple;
    {
        /// The total_tokens column structure is Array(Tuple(field_id UInt32, total_tokens UInt64))
        auto field_id_col = ColumnUInt32::create();
        auto total_tokens_col = ColumnUInt64::create();
        for (const auto & [field_id, total_tokens] : total_tokens_map)
        {
            field_id_col->insert(field_id);
            total_tokens_col->insert(total_tokens);
        }

        total_tokens_tuple.emplace_back(std::move(field_id_col));
        total_tokens_tuple.emplace_back(std::move(total_tokens_col));
    }


    Columns doc_freq_tuple;
    {
        /// The terms_freq column structure is Array(Tuple(term_str String, field_id UInt32, doc_freq UInt64))
        auto term_str_col = ColumnString::create();
        auto field_id_col = ColumnUInt32::create();
        auto doc_freq_col = ColumnUInt64::create();
        for (const auto & [field_and_term, doc_freq] : terms_freq_map)
        {
            term_str_col->insertData(field_and_term.second.data(), field_and_term.second.size());
            field_id_col->insert(field_and_term.first);
            doc_freq_col->insert(doc_freq);
        }

        doc_freq_tuple.emplace_back(std::move(term_str_col));
        doc_freq_tuple.emplace_back(std::move(field_id_col));
        doc_freq_tuple.emplace_back(std::move(doc_freq_col));
    }

    auto res_block = Block({
        {ColumnUInt64::create(1, total_docs), StorageFtsIndex::virtuals_sample_block.getByPosition(0).type, StorageFtsIndex::virtuals_sample_block.getByPosition(0).name},
        {ColumnArray::create(ColumnTuple::create(std::move(total_tokens_tuple)), ColumnArray::ColumnOffsets::create(1, total_tokens_map.size())), StorageFtsIndex::virtuals_sample_block.getByPosition(1).type, StorageFtsIndex::virtuals_sample_block.getByPosition(1).name},
        {ColumnArray::create(ColumnTuple::create(std::move(doc_freq_tuple)), ColumnArray::ColumnOffsets::create(1, terms_freq_map.size())), StorageFtsIndex::virtuals_sample_block.getByPosition(2).type, StorageFtsIndex::virtuals_sample_block.getByPosition(2).name}});

    context->getQueryContext()->addScalar("_fts_statistic_info", res_block);
}

void parseBM25StaisiticsInfo(const Block & block, size_t row, UInt64 & total_docs, std::map<UInt32, UInt64> & total_tokens_map, std::map<std::pair<UInt32, String>, UInt64> & terms_freq_map)
{
    if (block.columns() != 3)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fts statistic info block should have 3 columns, but get {} columns", block.columns());

    auto getColumnPtr = [&block](size_t index) -> ColumnPtr {
        /// Sometimes the column is ColumnConst, so we need to get the data column
        ColumnPtr column = checkAndGetColumn<ColumnConst>(block.getColumns()[index].get()) 
                ? checkAndGetColumn<ColumnConst>(block.getColumns()[index].get())->getDataColumnPtr() 
                : block.getColumns()[index];

        if (!column)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error");

        return column;
    };

    ColumnPtr column_total_docs = getColumnPtr(0);
    ColumnPtr column_total_tokens = getColumnPtr(1);
    ColumnPtr column_docs_freq = getColumnPtr(2);

    total_docs += column_total_docs->get64(row);

    {
        /// The column column_total_tokens is a Array(Tuple(UInt32, UInt64)) column, which contains field_id, total_tokens
        const ColumnArray * tokens_array = checkAndGetColumn<ColumnArray>(column_total_tokens.get());
        if (!tokens_array)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error, docs_freq type is {}", column_total_tokens->dumpStructure());

        const auto * tokens_tuple = checkAndGetColumn<ColumnTuple>(&tokens_array->getData());
        if (!tokens_tuple || tokens_tuple->getColumns().size() != 2)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error, docs_freq type is {}", column_total_tokens->dumpStructure());

        const auto * tuple_column0 = checkAndGetColumn<ColumnUInt32>(tokens_tuple->getColumnPtr(0).get());
        const auto * tuple_column1 = checkAndGetColumn<ColumnUInt64>(tokens_tuple->getColumnPtr(1).get());

        if (!tuple_column0 || !tuple_column1)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error, docs_freq type is {}", column_total_tokens->dumpStructure());

        const IColumn::Offsets & offsets = tokens_array->getOffsets();
        size_t start = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];

        for (size_t i = start; i < end; ++i)
        {
            total_tokens_map[static_cast<UInt32>(tuple_column0->getUInt(i))] += tuple_column1->get64(i);
        }
    }

    {
        /// The column column_docs_freq is a Array(Tuple(String, UInt32, UInt64)) column, which contains term, field_id, doc_freq
        const ColumnArray * freq_array = checkAndGetColumn<ColumnArray>(column_docs_freq.get());
        if (!freq_array)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error, docs_freq type is {}", column_docs_freq->dumpStructure());

        const auto * freq_tuple = checkAndGetColumn<ColumnTuple>(&freq_array->getData());
        if (!freq_tuple || freq_tuple->getColumns().size() != 3)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error, docs_freq type is {}", column_docs_freq->dumpStructure());

        const auto * tuple_column0 = checkAndGetColumn<ColumnString>(&freq_tuple->getColumn(0));
        const auto * tuple_column1 = checkAndGetColumn<ColumnUInt32>(&freq_tuple->getColumn(1));
        const auto * tuple_column2 = checkAndGetColumn<ColumnUInt64>(&freq_tuple->getColumn(2));

        if (!tuple_column0 || !tuple_column1 || !tuple_column2)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Fts statistic info block type is error, docs_freq type is {}", column_docs_freq->dumpStructure());

        const IColumn::Offsets & offsets = freq_array->getOffsets();
        size_t start = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];

        for (size_t i = start; i < end; ++i)
        {
            terms_freq_map[{static_cast<uint32_t>(tuple_column1->getUInt(i)), tuple_column0->getDataAt(i).toString()}] += tuple_column2->get64(i);
        }
    }
}
#endif

}
