/* Please note that the file has been modified by Moqi Technology (Beijing) Co.,
 * Ltd. All the modifications are Copyright (C) 2022 Moqi Technology (Beijing)
 * Co., Ltd. */


#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
class ASTFunction;

/** name BY expr TYPE typename(args) in create query
 * expr TYPE typename(args) in create vector index query
  */
class ASTVectorIndexDeclaration : public IAST
{
public:
    String name;
    String column;
    bool std_create = false;
    ASTFunction * type;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "VectorIndex"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
