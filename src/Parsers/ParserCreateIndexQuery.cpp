#include <Parsers/ParserCreateIndexQuery.h>

#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <VectorIndex/Parsers/ASTVIDeclaration.h>


namespace DB
{

bool ParserCreateVectorIndexDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_type(Keyword::TYPE);

    ParserCompoundIdentifier column_p;
    ParserExpressionWithOptionalArguments type_p;

    ASTPtr column;
    ASTPtr type;

    if (!column_p.parse(pos, column, expected))
        return false;

    if (s_type.ignore(pos, expected))
    {
        if (!type_p.parse(pos, type, expected))
            return false;
    }
    else
    {
        /// The "TYPE typename(args)" field in "CREATE VECTOR INDEX" query is omitted, creating a default vector index
        auto function_node = std::make_shared<ASTFunction>();
        function_node->name = "DEFAULT";
        function_node->no_empty_args = true;
        type = function_node;
    }

    auto index = std::make_shared<ASTVIDeclaration>(type, "", column->as<ASTIdentifier &>().name());
    index->part_of_create_index_query = true;
    node = index;

    return true;
}

bool ParserCreateIndexDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_granularity(Keyword::GRANULARITY);
    ParserToken open_p(TokenType::OpeningRoundBracket);
    ParserToken close_p(TokenType::ClosingRoundBracket);
    ParserOrderByExpressionList order_list_p;

    ParserExpressionWithOptionalArguments type_p;
    ParserExpression expression_p;
    ParserUnsignedInteger granularity_p;

    ASTPtr expr;
    ASTPtr type;
    ASTPtr granularity;

    if (open_p.ignore(pos, expected))
    {
        ASTPtr order_list;
        if (!order_list_p.parse(pos, order_list, expected))
            return false;

        if (!close_p.ignore(pos, expected))
            return false;

        if (order_list->children.empty())
            return false;

        /// CREATE INDEX with ASC, DESC is implemented only for SQL compatibility.
        /// ASC and DESC modifiers are not supported and are ignored further.
        if (order_list->children.size() == 1)
        {
            auto order_by_elem = order_list->children[0];
            expr = order_by_elem->children[0];
        }
        else
        {
            auto tuple_func = makeASTFunction("tuple");
            tuple_func->arguments = std::make_shared<ASTExpressionList>();

            for (const auto & order_by_elem : order_list->children)
            {
                auto elem_expr = order_by_elem->children[0];
                tuple_func->arguments->children.push_back(std::move(elem_expr));
            }
            expr = std::move(tuple_func);
        }
    }
    else if (!expression_p.parse(pos, expr, expected))
    {
        return false;
    }

    if (s_type.ignore(pos, expected))
    {
        if (!type_p.parse(pos, type, expected))
            return false;
    }

    if (s_granularity.ignore(pos, expected))
    {
        if (!granularity_p.parse(pos, granularity, expected))
            return false;
    }

    /// name is set below in ParserCreateIndexQuery
    auto index = std::make_shared<ASTIndexDeclaration>(expr, type, "");
    index->part_of_create_index_query = true;

    if (granularity)
    {
        index->granularity = granularity->as<ASTLiteral &>().value.safeGet<UInt64>();
    }
    else
    {
        auto index_type = index->getType();
        if (index_type && index_type->name == "vector_similarity")
            index->granularity = ASTIndexDeclaration::DEFAULT_VECTOR_SIMILARITY_INDEX_GRANULARITY;
        else
            index->granularity = ASTIndexDeclaration::DEFAULT_INDEX_GRANULARITY;
    }
    node = index;
    return true;
}

bool ParserCreateIndexQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTCreateIndexQuery>();
    node = query;

    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_vector(Keyword::VECTOR);
    ParserKeyword s_unique(Keyword::UNIQUE);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);

    ParserIdentifier index_name_p;
    ParserCreateIndexDeclaration parser_create_idx_decl;
    ParserCreateVectorIndexDeclaration parser_create_vec_idx_decl;

    ASTPtr index_name;
    ASTPtr index_decl;

    String cluster_str;
    bool if_not_exists = false;
    bool unique = false;
    bool is_vector_index = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_unique.ignore(pos, expected))
        unique = true;

    if (s_vector.ignore(pos, expected))
        is_vector_index = true;

    if (!s_index.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!index_name_p.parse(pos, index_name, expected))
        return false;

    /// ON [db.] table_name
    if (!s_on.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    /// [ON cluster_name]
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    /// Fill name in IndexDeclaration
    if (is_vector_index)
    {
        if (!parser_create_vec_idx_decl.parse(pos, index_decl, expected))
            return false;
        auto & ast_vec_index_decl = index_decl->as<ASTVIDeclaration &>();
        ast_vec_index_decl.name = index_name->as<ASTIdentifier &>().name();
    }
    else
    {
        if (!parser_create_idx_decl.parse(pos, index_decl, expected))
            return false;
        auto & ast_index_decl = index_decl->as<ASTIndexDeclaration &>();
        ast_index_decl.name = index_name->as<ASTIdentifier &>().name();
    }

    query->index_name = index_name;
    query->children.push_back(index_name);

    query->index_decl = index_decl;
    query->children.push_back(index_decl);

    query->if_not_exists = if_not_exists;
    query->unique = unique;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    query->is_vector_index = is_vector_index;

    return true;
}

}
