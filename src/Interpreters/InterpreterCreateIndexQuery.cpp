/* Please note that the file has been modified by Moqi Technology (Beijing) Co.,
 * Ltd. All the modifications are Copyright (C) 2022 Moqi Technology (Beijing)
 * Co., Ltd. */


#include <Interpreters/InterpreterCreateIndexQuery.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTVectorIndexDeclaration.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageDistributed.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
}


BlockIO InterpreterCreateIndexQuery::execute()
{
    auto current_context = getContext();
    auto & create_index = query_ptr->as<ASTCreateIndexQuery &>();

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_ADD_INDEX, create_index.getDatabase(), create_index.getTable());

    if (!create_index.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(required_access);
    auto table_id = current_context->resolveStorageID(create_index, Context::ResolveOrdinary);
    query_ptr->as<ASTCreateIndexQuery &>().setDatabase(table_id.database_name);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, current_context);
    }
    
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, current_context);
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

    if (create_index.is_vector_index)
    {
        if (auto dist_table = typeid_cast<StorageDistributed *>(table.get()))
        {
            /// We only check the first command, and not check if alter table contains mixed table struct and data commands.
            create_index.setTable(dist_table->getRemoteTableName());
            create_index.cluster = dist_table->getClusterName();

            String remote_database;
            if (!dist_table->getRemoteDatabaseName().empty())
                remote_database = dist_table->getRemoteDatabaseName();
            else
                remote_database = dist_table->getCluster()->getShardsAddresses().front().front().default_database;

            create_index.setDatabase(remote_database);
        }
    }

    /// Convert ASTCreateIndexQuery to AlterCommand.
    AlterCommands alter_commands;

    AlterCommand command;
    command.ast = create_index.convertToASTAlterCommand();
    if(create_index.is_vector_index)
    {
        command.ast = create_index.convertToASTAlterCommand();
        command.vec_index_decl = create_index.index_decl;
        command.type = AlterCommand::ADD_VECTOR_INDEX;
        command.vec_index_name = create_index.index_name->as<ASTIdentifier &>().name();

        auto & ast_vec_index_decl = command.vec_index_decl->as<ASTVectorIndexDeclaration &>();
        command.column_name = ast_vec_index_decl.column;
    }
    else
    {
        command.index_decl = create_index.index_decl;
        command.type = AlterCommand::ADD_INDEX;
        command.index_name = create_index.index_name->as<ASTIdentifier &>().name();
    }
    command.if_not_exists = create_index.if_not_exists;

    alter_commands.emplace_back(std::move(command));

    auto alter_lock = table->lockForAlter(current_context->getSettingsRef().lock_acquire_timeout);
    StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
    alter_commands.validate(table, current_context);
    alter_commands.prepare(metadata);
    table->checkAlterIsPossible(alter_commands, current_context);
    table->alter(alter_commands, current_context, alter_lock);

    return {};
}

}
