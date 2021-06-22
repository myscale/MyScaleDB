#include <Storages/System/StorageSystemConnections.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControl.h>
#include <Access/AWSConnection.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemConnections::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"provider", std::make_shared<DataTypeString>()},
        {"arn", std::make_shared<DataTypeString>()},
        {"external_id", std::make_shared<DataTypeString>()},
        {"duration", std::make_shared<DataTypeUInt32>()},
    };
    return names_and_types;
}


void StorageSystemConnections::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_CONNECTIONS);
    const auto & access_control = context->getAccessControl();
    std::vector<UUID> ids = access_control.findAll<AWSConnection>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_provider = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_arn = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_external_id = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_duration = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & provider_name,
                       const String & role_arn,
                       const String & external_id,
                       const UInt32 & duration)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_provider.insertData(provider_name.data(), provider_name.length());
        column_arn.insertData(role_arn.data(), role_arn.length());
        column_external_id.insertData(external_id.data(), external_id.length());
        column_duration.push_back(duration);
    };

    for (const auto & id : ids)
    {
        auto conn = access_control.tryRead<AWSConnection>(id);
        if (!conn)
            continue;

        add_row(conn->getName(), id, conn->provider_name, conn->aws_role_arn, conn->aws_role_external_id, conn->aws_role_credential_duration);
    }
}

}
