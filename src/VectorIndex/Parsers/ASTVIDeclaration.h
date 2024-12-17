#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
class ASTFunction;

/** name BY expr TYPE typename(args) in create query
 * expr TYPE typename(args) in create vector index query
  */
class ASTVIDeclaration : public IAST
{
public:
    ASTVIDeclaration(ASTPtr type, const String & name_, const String & column_);

    String name;
    String column;
    bool part_of_create_index_query = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "VectorIndex"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    std::shared_ptr<ASTFunction> getType() const;

private:
  static constexpr size_t type_idx = 0;
};

}
