#include <Parsers/ParserCreateConnectionQuery.h>
#include <Parsers/ASTCreateConnectionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace
{
    bool parseValue(IParserBase::Pos & pos, Expected & expected, Field & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::Equals}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserLiteral{}.parse(pos, ast, expected))
                return false;

            res = ast->as<ASTLiteral &>().value;
            return true;
        });
    }
}


bool ParserCreateConnectionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_alter("ALTER");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_connection("CONNECTION");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_on("ON");
    ParserKeyword s_rename_to("RENAME TO");
    ParserKeyword s_provider("PROVIDER");
    ParserKeyword s_aws_role_arn("AWS_ROLE_ARN");
    ParserKeyword s_aws_role_external_id("AWS_ROLE_EXTERNAL_ID");
    ParserKeyword s_aws_role_credential_duration("AWS_ROLE_CREDENTIAL_DURATION");
    ParserIdentifier connection_name_p;
 
    bool alter = false;
    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String cluster;
    ASTPtr connection_name_ast;
    ASTPtr new_name_ast;
    Field provider_name_value;
    Field role_arn_value;
    Field external_id_value;
    Field duration_value;

    if (attach_mode)
    {
        if (!s_attach.ignore(pos, expected))
            return false;
    }
    else if (s_alter.ignore(pos, expected))
        alter = true;
    else if (s_create.ignore(pos, expected))
    {
        if (s_or_replace.ignore(pos, expected))
            or_replace = true;
    }
    else
        return false;

    if (!s_connection.ignore(pos, expected))
        return false;

    if (alter)
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;
    }
    else if (!attach_mode || !or_replace)
    {
        if (s_if_not_exists.ignore(pos, expected))
            if_not_exists = true;
    }

    if (!connection_name_p.parse(pos, connection_name_ast, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
    }

    if (alter)
    {
        if (s_rename_to.ignore(pos, expected) && !connection_name_p.parse(pos, new_name_ast, expected))
            return false;

        if (s_provider.ignore(pos, expected))
        {
            if (!parseValue(pos, expected, provider_name_value))
               return false;
        }

        if (s_aws_role_arn.ignore(pos, expected))
        {
            if (!parseValue(pos, expected, role_arn_value))
               return false;
        }
    }
    else /// For create or attach, provider and role_arn are needed.
    {
        if (!s_provider.ignore(pos, expected))
            return false;

        if (!parseValue(pos, expected, provider_name_value))
            return false;

        if (!s_aws_role_arn.ignore(pos, expected))
            return false;
 
        if (!parseValue(pos, expected, role_arn_value))
            return false;
    }

    if (s_aws_role_external_id.ignore(pos, expected))
    {
        if (!parseValue(pos, expected, external_id_value))
           return false;
    }

    if (s_aws_role_credential_duration.ignore(pos, expected))
    {
        if (!parseValue(pos, expected, duration_value))
           return false;
    }
 
    auto query = std::make_shared<ASTCreateConnectionQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->name = connection_name_ast->as<ASTIdentifier &>().name();

    if (new_name_ast)
        query->new_name = new_name_ast->as<ASTIdentifier &>().name();

    query->provider_name_value = std::move(provider_name_value);
    query->role_arn_value = std::move(role_arn_value);
    query->external_id_value = std::move(external_id_value);
    query->duration_value = std::move(duration_value);

    return true;
}
}
