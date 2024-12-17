#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropIndexQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageDistributed.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
}


BlockIO InterpreterDropIndexQuery::execute()
{
    auto current_context = getContext();
    auto & drop_index = query_ptr->as<ASTDropIndexQuery &>();

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_DROP_INDEX, drop_index.getDatabase(), drop_index.getTable());

    auto table_id = current_context->resolveStorageID(drop_index, Context::ResolveOrdinary);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, current_context);

    /// Convert drop vector index command on distributed table into an equivalent distributed ddl on local tables.
    /// Support drop index for full-text search
    if (auto dist_table = typeid_cast<StorageDistributed *>(table.get()))
    {
        /// We only check the first command, and not check if alter table contains mixed table struct and data commands.
        drop_index.setTable(dist_table->getRemoteTableName());
        drop_index.cluster = dist_table->getClusterName();

        String remote_database;
        if (!dist_table->getRemoteDatabaseName().empty())
            remote_database = dist_table->getRemoteDatabaseName();
        else
            remote_database = dist_table->getCluster()->getShardsAddresses().front().front().default_database;

        drop_index.setDatabase(remote_database);
    }

    if (!drop_index.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(required_access);
    query_ptr->as<ASTDropIndexQuery &>().setDatabase(table_id.database_name);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, current_context);
    }

    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

    /// Convert ASTDropIndexQuery to AlterCommand.
    AlterCommands alter_commands;

    AlterCommand command;
    command.ast = drop_index.convertToASTAlterCommand();
    if(drop_index.is_vector_index)
    {
        command.vec_index_name = drop_index.index_name->as<ASTIdentifier &>().name();
        command.type = AlterCommand::DROP_VECTOR_INDEX;
    }
    else
    {
        command.index_name = drop_index.index_name->as<ASTIdentifier &>().name();
        command.type = AlterCommand::DROP_INDEX;
    }
    command.if_exists = drop_index.if_exists;

    alter_commands.emplace_back(std::move(command));

    auto alter_lock = table->lockForAlter(current_context->getSettingsRef().lock_acquire_timeout);
    StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
    alter_commands.validate(table, current_context);
    alter_commands.prepare(metadata);
    table->checkAlterIsPossible(alter_commands, current_context);
    table->alter(alter_commands, current_context, alter_lock);

    return {};
}

void registerInterpreterDropIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropIndexQuery", create_fn);
}

}
