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

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>

#include <VectorIndex/Parsers/ASTVectorIndexDeclaration.h>


namespace DB
{
ASTPtr ASTVectorIndexDeclaration::clone() const
{
    auto res = std::make_shared<ASTVectorIndexDeclaration>();

    res->name = name;
    res->column = column;

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
}

}
