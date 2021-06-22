/* Please note that the file has been modified by Moqi Technology (Beijing) Co.,
 * Ltd. All the modifications are Copyright (C) 2022 Moqi Technology (Beijing)
 * Co., Ltd. */


#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;

class InterpreterCreateIndexQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateIndexQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_)
        , query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
