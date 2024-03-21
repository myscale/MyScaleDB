#include <Storages/VectorIndexCommands.h>
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
#include <Parsers/ASTVectorIndexDeclaration.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/queryToString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_VECTOR_INDEX_COMMAND;
}

std::optional<VectorIndexCommand> VectorIndexCommand::parse(ASTAlterCommand * command)
{
    Poco::Logger * log = &Poco::Logger::get("VectorIndexCommand");
    if (command->type == ASTAlterCommand::ADD_VECTOR_INDEX)
    {
        VectorIndexCommand res;
        res.drop_command = false;
        res.ast = command->ptr();
        res.index_name = command->vec_index_decl->as<ASTIdentifier &>().name();
        res.column_name = getIdentifierName(command->column);
        res.index_type = Poco::toUpper(command->vec_index_decl->as<ASTVectorIndexDeclaration>()->type->name);
        LOG_DEBUG(log, "Add vector index: name: {}, index_type: {}", res.index_name, res.index_type);
        return res;
    }
    else if (command->type == ASTAlterCommand::DROP_VECTOR_INDEX)
    {
        VectorIndexCommand res;
        res.drop_command = true;
        res.index_name = command->vec_index_decl->as<ASTIdentifier &>().name();
        res.column_name = getIdentifierName(command->column);
        res.index_type = Poco::toUpper(command->vec_index_decl->as<ASTVectorIndexDeclaration>()->type->name);
        LOG_DEBUG(log, "Drop vector index: name: {}, index_type: {}", res.index_name, res.index_type);
        return res;
    }
    else
    {
        return {};
    }
}

std::shared_ptr<ASTExpressionList> VectorIndexCommands::ast() const
{
    auto res = std::make_shared<ASTExpressionList>();
    for (const VectorIndexCommand & command : *this)
        res->children.push_back(command.ast->clone());
    return res;
}


void VectorIndexCommands::writeText(WriteBuffer & out) const
{
    WriteBufferFromOwnString commands_buf;
    formatAST(*ast(), commands_buf, /* hilite = */ false, /* one_line = */ true);
    writeEscapedString(commands_buf.str(), out);
}

void VectorIndexCommands::readText(ReadBuffer & in)
{
    String commands_str;
    readEscapedString(commands_str, in);

    ParserAlterCommandList p_alter_commands;
    auto commands_ast = parseQuery(
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "vector index commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    for (const auto & child : commands_ast->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        auto command = VectorIndexCommand::parse(command_ast);
        if (!command)
            throw Exception(ErrorCodes::UNKNOWN_VECTOR_INDEX_COMMAND, "Unknown vector index command type: {}", DB::toString<int>(command_ast->type));
        push_back(std::move(*command));
    }
}

}
