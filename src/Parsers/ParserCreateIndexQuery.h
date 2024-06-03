#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON [db].name (expression) TYPE type GRANULARITY value
  * CREATE VECTOR INDEX [IF NOT EXISTS] name on [db].name column [TYPE typename(args)]
  */

class ParserCreateIndexQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE INDEX query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Parser for index declaration in create index, where name is ignored. */
class ParserCreateIndexDeclaration : public IParserBase
{
public:
    ParserCreateIndexDeclaration() = default;

protected:
    const char * getName() const override { return "index declaration in create index"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Parser for vector index declaration in create vector index. */
class ParserCreateVectorIndexDeclaration : public IParserBase
{
public:
    ParserCreateVectorIndexDeclaration() {}

protected:
    const char * getName() const override { return "vector index declaration in create vector index"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
