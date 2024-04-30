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

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Common/CurrentThread.h>

#include <VectorIndex/Interpreters/VIEventLog.h>

namespace DB
{

NamesAndTypesList VIEventLogElement::getNamesAndTypes()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"DefinitionCreated",       static_cast<Int8>(DEFINITION_CREATED)},
            {"DefinitionDroped",    static_cast<Int8>(DEFINITION_DROPPED)},
            {"DefinitionError",    static_cast<Int8>(DEFINITION_ERROR)},
            {"BuildStart",  static_cast<Int8>(BUILD_START)},
            {"BuildSucceed",    static_cast<Int8>(BUILD_SUCCEED)},
            {"BuildError",    static_cast<Int8>(BUILD_ERROR)},
            {"BuildCanceld",      static_cast<Int8>(BUILD_CANCELD)},
            {"LoadStart",      static_cast<Int8>(LOAD_START)},
            {"LoadSucceed",      static_cast<Int8>(LOAD_SUCCEED)},
            {"LoadCanceled",      static_cast<Int8>(LOAD_CANCELD)},
            {"LoadFailed",    static_cast<Int8>(LOAD_FAILED)},
            {"LoadError",    static_cast<Int8>(LOAD_ERROR)},
            // {"Unload",      static_cast<Int8>(UNLOAD)},
            {"CacheExpire",      static_cast<Int8>(CACHE_EXPIRE)},
            {"WillUnload",      static_cast<Int8>(WILLUNLOAD)},
            {"Cleared",      static_cast<Int8>(CLEARED)},
        }
    );

    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"index_name", std::make_shared<DataTypeString>()},
        {"part_name", std::make_shared<DataTypeString>()},
        {"current_part_name", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"thread_id", std::make_shared<DataTypeString>()},

        {"event_type", std::move(event_type_datatype)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"error", std::make_shared<DataTypeUInt16>()},
        {"exception", std::make_shared<DataTypeString>()},
    };
};

void VIEventLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(index_name);
    columns[i++]->insert(part_name);
    columns[i++]->insert(current_part_name);
    columns[i++]->insert(partition_id);
    columns[i++]->insert(thread_id);

    columns[i++]->insert(event_type);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(error_code);
    columns[i++]->insert(exception);
};

void VIEventLog::addEventLog(
    VIEventLogPtr log_entry,
    const String & db_name,
    const String & table_name,
    const String & index_name,
    const String & part_name,
    const String & partition_id,
    VIEventLogElement::Type event_type,
    const String & current_part_name,
    const ExecutionStatus & execution_status)
{
    if (!log_entry) return;
    VIEventLogElement elem;
    elem.database_name = db_name;
    elem.table_name = table_name;
    elem.index_name = index_name;
    elem.part_name = part_name;
    if (current_part_name != "")
        elem.current_part_name = current_part_name;
    else
        elem.current_part_name = part_name;
    elem.partition_id = partition_id;
    if (unlikely(!current_thread))
        elem.thread_id = toString(0);
    else
        elem.thread_id = toString(current_thread->thread_id);
    elem.event_type = event_type;
    const auto time_now = std::chrono::system_clock::now();

    elem.event_time = timeInSeconds(time_now);
    elem.event_time_microseconds = timeInMicroseconds(time_now);

    elem.error_code = static_cast<UInt16>(execution_status.code);
    elem.exception = execution_status.message;

    log_entry->add(elem);
}

void VIEventLog::addEventLog(
    ContextPtr current_context,
    const String & db_name,
    const String & table_name,
    const String & index_name,
    const String & part_name,
    const String & partition_id,
    VIEventLogElement::Type event_type,
    const String & current_part_name,
    const ExecutionStatus & execution_status)
{
    VIEventLogPtr log_entry = current_context->getVectorIndexEventLog();

    try
    {
        if (log_entry)
            addEventLog(log_entry,
                        db_name,
                        table_name,
                        index_name,
                        part_name,
                        partition_id,
                        event_type,
                        current_part_name,
                        execution_status);
    }
    catch (...)
    {
        tryLogCurrentException(log_entry ? log_entry->log : &Poco::Logger::get("VIEventLog"), __PRETTY_FUNCTION__);
    }
}

void VIEventLog::addEventLog(
    ContextPtr current_context,
    const MergeTreeDataPartPtr & data_part,
    const String & index_name,
    VIEventLogElement::Type event_type,
    const ExecutionStatus & execution_status)
{
    VIEventLogPtr log_entry = current_context->getVectorIndexEventLog();

    try
    {
        if(log_entry)
            addEventLog(log_entry,
                        data_part->storage.getStorageID().database_name,
                        data_part->storage.getStorageID().table_name,
                        index_name,
                        data_part->name,
                        data_part->info.partition_id,
                        event_type,
                        data_part->name,
                        execution_status);
    }
    catch (...)
    {
        tryLogCurrentException(log_entry ? log_entry->log : &Poco::Logger::get("VIEventLog"), __PRETTY_FUNCTION__);
    }
}

void VIEventLog::addEventLog(
    ContextPtr current_context,
    const String & table_uuid,
    const String & index_name,
    const String & part_name,
    const String & partition_id,
    VIEventLogElement::Type event_type,
    const String & current_part_name,
    const ExecutionStatus & execution_status)
{
    VIEventLogPtr log_entry = current_context->getVectorIndexEventLog();
    
    try
    {
        if(log_entry)
        {
            UUID tb_uuid = VIEventLog::parseUUID(table_uuid);
            auto ret = getDbAndTableNameFromUUID(tb_uuid);
            if (ret.has_value())
                addEventLog(log_entry, 
                            ret->first,
                            ret->second,
                            index_name,
                            part_name,
                            partition_id,
                            event_type,
                            current_part_name,
                            execution_status);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log_entry ? log_entry->log : &Poco::Logger::get("VIEventLog"), __PRETTY_FUNCTION__);
    }
}

std::optional<std::pair<String, String>> VIEventLog::getDbAndTableNameFromUUID(const UUID & table_uuid)
{
    if (!DatabaseCatalog::instance().tryGetByUUID(table_uuid).second)
        return std::nullopt;
    auto table_id = DatabaseCatalog::instance().tryGetByUUID(table_uuid).second->getStorageID();
    if (table_id)
    {
        String database_name = table_id.database_name;
        String table_name = table_id.table_name;
        return std::make_pair(database_name, table_name);
    }
    return std::nullopt;
}

}
