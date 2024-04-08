#pragma once

#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage_fwd.h>

#include <optional>
#include <unordered_map>


namespace DB
{
struct VICommand
{
    ASTPtr ast; /// The AST of the whole command

    bool drop_command;
    String index_name;
    String column_name;
    String index_type;
    DataTypePtr data_type;

    static std::optional<VICommand> parse(ASTAlterCommand * command);
};

class VICommands : public std::vector<VICommand>
{
public:
    std::shared_ptr<ASTExpressionList> ast() const;

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
