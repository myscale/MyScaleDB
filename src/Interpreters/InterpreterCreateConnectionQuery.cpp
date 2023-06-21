#include <Interpreters/InterpreterCreateConnectionQuery.h>
#include <Parsers/ASTCreateConnectionQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Access/AccessControl.h>
#include <Access/AWSConnection.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Role.h>


namespace DB
{
namespace
{
    void updateConnectionFromQueryImpl(
        AWSConnection & conn,
        const ASTCreateConnectionQuery & query,
        const String & override_name,
        const String & override_provider_name,
        const String & override_role_arn,
        const String & override_external_id,
        const UInt32 & overide_duration)
    {
        if (!override_name.empty())
            conn.setName(override_name);
        else if (!query.new_name.empty())
            conn.setName(query.new_name);
        else if (!query.name.empty())
            conn.setName(query.name);

        if (!override_provider_name.empty())
            conn.provider_name = override_provider_name;
        else if (!query.provider_name_value.isNull())
            conn.provider_name = query.provider_name_value.get<String>();

        if (!override_role_arn.empty())
            conn.aws_role_arn = override_role_arn;
        else if (!query.role_arn_value.isNull())
            conn.aws_role_arn = query.role_arn_value.get<String>();

        if (!override_external_id.empty())
            conn.aws_role_external_id = override_external_id;
        else if (!query.external_id_value.isNull())
            conn.aws_role_external_id = query.external_id_value.get<String>();

        if (overide_duration > 0)
            conn.aws_role_credential_duration = overide_duration;
        else if (!query.duration_value.isNull())
            conn.aws_role_credential_duration = static_cast<UInt32>(query.duration_value.get<UInt32>());
    }
}


BlockIO InterpreterCreateConnectionQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateConnectionQuery &>();
    auto & access_control = getContext()->getAccessControl();
    if (query.alter)
        getContext()->checkAccess(AccessType::ALTER_CONNECTION);
    else
        getContext()->checkAccess(AccessType::CREATE_CONNECTION);

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

    String provider_name;
    String role_arn;
    String external_id;
    UInt32 duration = 0;

    if (!query.provider_name_value.isNull())
        provider_name = query.provider_name_value.get<String>();

    if (!query.role_arn_value.isNull())
        role_arn = query.role_arn_value.get<String>();

    if (!query.external_id_value.isNull())
        external_id = query.external_id_value.get<String>();

    if (!query.duration_value.isNull())
        duration = static_cast<UInt32>(query.duration_value.get<UInt32>());

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_conn = typeid_cast<std::shared_ptr<AWSConnection>>(entity->clone());
            updateConnectionFromQueryImpl(*updated_conn, query, {}, provider_name, role_arn, external_id, duration);
            return updated_conn;
        };
        if (query.if_exists)
        {
            auto id = access_control.find<AWSConnection>(query.name);
            access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<AWSConnection>(query.name), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_conns;
        auto new_conn = std::make_shared<AWSConnection>();
        updateConnectionFromQueryImpl(*new_conn, query, query.name, provider_name, role_arn, external_id, duration);

        new_conns.emplace_back(std::move(new_conn));

        if (query.if_not_exists)
            access_control.tryInsert(new_conns);
        else if (query.or_replace)
            access_control.insertOrReplace(new_conns);
        else
            access_control.insert(new_conns);
    }

    return {};
}


void InterpreterCreateConnectionQuery::updateConnectionFromQuery(AWSConnection & conn, const ASTCreateConnectionQuery & query)
{
    updateConnectionFromQueryImpl(conn, query, {}, {}, {}, {}, {});
}
}
