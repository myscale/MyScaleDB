/* Please note that the file has been modified by Moqi Technology (Beijing) Co.,
 * Ltd. All the modifications are Copyright (C) 2022 Moqi Technology (Beijing)
 * Co., Ltd. */


#include <Parsers/ASTVectorIndexDeclaration.h>

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>


namespace DB
{
ASTPtr ASTVectorIndexDeclaration::clone() const
{
    auto res = std::make_shared<ASTVectorIndexDeclaration>();

    res->name = name;
    res->column = column;
    // res->granularity = granularity;

    if (type)
        res->set(res->type, type->clone());
    return res;
}


void ASTVectorIndexDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (!std_create)
        s.ostr << backQuoteIfNeed(name);
    s.ostr << " ";
    s.ostr << backQuoteIfNeed(column);
    s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
    type->formatImpl(s, state, frame);
    // s.ostr << (s.hilite ? hilite_keyword : "") << " GRANULARITY " << (s.hilite ? hilite_none : "");
    // s.ostr << granularity;
}

}
