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

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/queryToString.h>
#include <Common/logger_useful.h>

#include <VectorIndex/Parsers/ASTVIDeclaration.h>
#include <VectorIndex/Storages/VICommands.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_VECTOR_INDEX_COMMAND;
}

std::optional<VICommand> VICommand::parse(ASTAlterCommand * command)
{
    Poco::Logger * log = &Poco::Logger::get("VICommand");
    if (command->type == ASTAlterCommand::ADD_VECTOR_INDEX)
    {
        VICommand res;
        res.drop_command = false;
        res.ast = command->ptr();
        res.index_name = command->vec_index_decl->as<ASTIdentifier &>().name();
        res.column_name = getIdentifierName(command->column);
        res.index_type = Poco::toUpper(command->vec_index_decl->as<ASTVIDeclaration>()->type->name);
        LOG_DEBUG(log, "Add vector index: name: {}, index_type: {}", res.index_name, res.index_type);
        return res;
    }
    else if (command->type == ASTAlterCommand::DROP_VECTOR_INDEX)
    {
        VICommand res;
        res.drop_command = true;
        res.index_name = command->vec_index_decl->as<ASTIdentifier &>().name();
        res.column_name = getIdentifierName(command->column);
        res.index_type = Poco::toUpper(command->vec_index_decl->as<ASTVIDeclaration>()->type->name);
        LOG_DEBUG(log, "Drop vector index: name: {}, index_type: {}", res.index_name, res.index_type);
        return res;
    }
    else
    {
        return {};
    }
}

std::shared_ptr<ASTExpressionList> VICommands::ast() const
{
    auto res = std::make_shared<ASTExpressionList>();
    for (const VICommand & command : *this)
        res->children.push_back(command.ast->clone());
    return res;
}


void VICommands::writeText(WriteBuffer & out) const
{
    WriteBufferFromOwnString commands_buf;
    formatAST(*ast(), commands_buf, /* hilite = */ false, /* one_line = */ true);
    writeEscapedString(commands_buf.str(), out);
}

void VICommands::readText(ReadBuffer & in)
{
    String commands_str;
    readEscapedString(commands_str, in);

    ParserAlterCommandList p_alter_commands;
    auto commands_ast = parseQuery(
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "vector index commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    for (const auto & child : commands_ast->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        auto command = VICommand::parse(command_ast);
        if (!command)
            throw Exception(ErrorCodes::UNKNOWN_VECTOR_INDEX_COMMAND, "Unknown vector index command type: {}", DB::toString<int>(command_ast->type));
        push_back(std::move(*command));
    }
}

}
