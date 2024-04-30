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

#pragma once
#include <optional>
#include <Interpreters/SystemLog.h>
#include <Common/DateLUT.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class IMergeTreeDataPart;

struct VIEventLogElement
{
    enum Type
    {
        DEFINITION_CREATED = 1,
        DEFINITION_DROPPED = 2,
        DEFINITION_ERROR = 3,

        BUILD_START = 4,
        BUILD_SUCCEED = 5,
        BUILD_ERROR = 6,
        BUILD_CANCELD = 7,

        LOAD_START = 8,
        LOAD_SUCCEED = 9,
        LOAD_CANCELD = 10,
        LOAD_FAILED = 11,
        LOAD_ERROR = 12,
        CACHE_EXPIRE = 13,
        WILLUNLOAD = 14,
        CLEARED = 15,
        DEFAULT = 16,
    };
    String database_name;
    String table_name;
    String index_name;
    mutable String part_name;
    mutable String current_part_name;
    mutable String partition_id;
    mutable String thread_id;

    Type event_type = DEFAULT;
    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;

    UInt16 error_code = 0;
    String exception;

    static std::string name() { return "VIEventLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class VIEventLog : public SystemLog<VIEventLogElement>
{
    using SystemLog<VIEventLogElement>::SystemLog;
    using VIEventLogPtr = std::shared_ptr<VIEventLog>;
    using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

public:
    static void addEventLog(
        VIEventLogPtr log_entry,
        const String & db_name,
        const String & table_name,
        const String & index_name,
        const String & part_name,
        const String & partition_id,
        VIEventLogElement::Type event_type,
        const String & current_part_name = "",
        const ExecutionStatus & execution_status = {});
    
    static void addEventLog(
        ContextPtr current_context,
        const String & db_name,
        const String & table_name,
        const String & index_name,
        const String & part_name,
        const String & partition_id,
        VIEventLogElement::Type event_type,
        const String & current_part_name = "",
        const ExecutionStatus & execution_status = {});
    
    static void addEventLog(
        ContextPtr current_context,
        const MergeTreeDataPartPtr & data_part,
        const String & index_name,
        VIEventLogElement::Type event_type,
        const ExecutionStatus & execution_status = {});

    static void addEventLog(
        ContextPtr current_context,
        const String & table_uuid,
        const String & index_name,
        const String & part_name,
        const String & partition_id,
        VIEventLogElement::Type event_type,
        const String & current_part_name = "",
        const ExecutionStatus & execution_status = {});

    static std::optional<std::pair<String, String>> getDbAndTableNameFromUUID(const UUID & table_uuid);

    static inline UUID parseUUID(const String & table_uuid)
    {
        UUID uuid = UUIDHelpers::Nil;
        auto buffer = ReadBufferFromMemory(table_uuid.data(), table_uuid.length());
        readUUIDText(uuid, buffer);
        return uuid;
    }

};

}
