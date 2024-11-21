#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{
/// Lightweight DELETE command stored in mutation commands
class ASTLWDeleteCommand : public IAST
{
public:
    String getID(char /*delim*/) const final { return "LWDeleteCommand"; }
    ASTPtr clone() const final;

    ASTPtr predicate;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/// DELETE FROM [db.]name WHERE ...
class ASTDeleteQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    String getID(char delim) const final;
    ASTPtr clone() const final;
    QueryKind getQueryKind() const override { return QueryKind::Delete; }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTDeleteQuery>(clone(), params.default_database);
    }

    /// The ast for lightweight delete stored in MutationCommand
    ASTPtr convertToASTLWDeleteCommand() const;

    ASTPtr predicate;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
