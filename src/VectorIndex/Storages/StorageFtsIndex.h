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

#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB
{

/// Internal temporary storage for table function ftsIndex(...)
class StorageFtsIndex final : public IStorage
{
public:
    static const ColumnWithTypeAndName total_docs;
    static const ColumnWithTypeAndName field_tokens;
    static const ColumnWithTypeAndName terms_freq;
    static const Block virtuals_sample_block;

    StorageFtsIndex(
        const StorageID & table_id_,
        const StoragePtr & source_table_,
        const ColumnsDescription & columns,
        const bool search_with_index_name_,
        const String & search_column_name_,
        const String & fts_index_name_,
        const String & query_text_);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getName() const override { return "FtsIndex"; }
    MergeTreeData::DataPartsVector getDataParts() const { return data_parts; }

private:
    friend class ReadFromFtsIndex;

    StoragePtr source_table;
    MergeTreeData::DataPartsVector data_parts;
    bool search_with_index_name;
    String search_column_name;
    String fts_index_name;
    String query_text;
};

class ReadFromFtsIndex : public SourceStepWithFilter
{
public:
    ReadFromFtsIndex(
        Block sample_block,
        std::shared_ptr<StorageFtsIndex> storage_,
        String tantivy_index_file_name_,
        String query_text_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)})
        , storage(std::move(storage_))
        , tantivy_index_file_name(tantivy_index_file_name_)
        , query_text(query_text_)
    {}

    std::string getName() const override { return "ReadFromFtsIndex"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::shared_ptr<StorageFtsIndex> storage;
    String tantivy_index_file_name;
    String query_text;
};

}
