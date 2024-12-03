#include <Parsers/ASTDeleteQuery.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTLWDeleteCommand::clone() const
{
    auto res = std::make_shared<ASTLWDeleteCommand>(*this);
    res->children.clear();

    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }

    return res;
}

void ASTLWDeleteCommand::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "LIGHTWEIGHT DELETE" << (settings.hilite ? hilite_none : "");

    if (predicate)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        predicate->formatImpl(settings, state, frame);
    }
}

String ASTDeleteQuery::getID(char delim) const
{
    return "DeleteQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDeleteQuery::clone() const
{
    auto res = std::make_shared<ASTDeleteQuery>(*this);
    res->children.clear();

    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }

    if (settings_ast)
    {
        res->settings_ast = settings_ast->clone();
        res->children.push_back(res->settings_ast);
    }

    cloneTableOptions(*res);
    return res;
}

void ASTDeleteQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DELETE FROM " << (settings.hilite ? hilite_none : "");

    if (database)
    {
        database->formatImpl(settings, state, frame);
        settings.ostr << '.';
    }

    chassert(table);
    table->formatImpl(settings, state, frame);

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
    predicate->formatImpl(settings, state, frame);
}

ASTPtr ASTDeleteQuery::convertToASTLWDeleteCommand() const
{
    auto res = std::make_shared<ASTLWDeleteCommand>();

    if (predicate)
    {
        res->predicate = predicate;
        res->children.push_back(res->predicate);
    }

    return res;
}

}
