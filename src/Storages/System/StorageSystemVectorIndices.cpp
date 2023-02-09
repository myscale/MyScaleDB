#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/queryToString.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/System/StorageSystemVectorIndices.h>
#include <Storages/VirtualColumnUtils.h>
#include <base/getFQDNOrHostName.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{
StorageSystemVectorIndices::StorageSystemVectorIndices(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"expr", std::make_shared<DataTypeString>()},
        {"total_parts", std::make_shared<DataTypeInt64>()},
        {"parts_with_vector_index", std::make_shared<DataTypeInt64>()},
        {"small_parts", std::make_shared<DataTypeInt64>()},
        {"status", std::make_shared<DataTypeString>()},
        {"host_name", std::make_shared<DataTypeString>()},
        {"latest_failed_part", std::make_shared<DataTypeString>()},
        {"latest_fail_reason",  std::make_shared<DataTypeString>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

class DataVectorIndicesSource : public ISource
{
public:
    DataVectorIndicesSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
        , database_idx(0)
    {}

    String getName() const override { return "DataVectorIndices"; }

protected:
    size_t getBuiltParts(const MergeTreeData::DataPartsVector& data_parts, const VectorIndexDescription& index) 
    {
        size_t built_parts = 0;
        for (auto & data_part : data_parts)
        {
            if (data_part->containVectorIndex(index.name, index.column))
                ++built_parts;
        }
        return built_parts;
    }

    size_t getSmallParts(const MergeTreeData* data, const MergeTreeData::DataPartsVector & data_parts, const VectorIndexDescription& index)
    {
        size_t small_parts = 0;
        size_t min_rows_to_build_vector_index = data->getSettings()->min_rows_to_build_vector_index;
        for (auto & data_part : data_parts)
        {
            /// if we enlarge small part size, there may exists some parts with indices built earlier.
            if (data_part->isSmallPart(min_rows_to_build_vector_index) && !data_part->containVectorIndex(index.name, index.column))
                ++small_parts;
        }
        return small_parts;
    }


    Chunk generate() override
    {
        if (database_idx >= databases->size())
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx).toString();
                database = DatabaseCatalog::instance().tryGetDatabase(database_name);

                if (database)
                    break;
                ++database_idx;
            }

            if (database_idx >= databases->size())
                break;

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesIterator(context);

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                const auto table = tables_it->table();
                if (!table)
                    continue;
                StorageMetadataPtr metadata_snapshot = table->getInMemoryMetadataPtr();
                if (!metadata_snapshot)
                    continue;

                MergeTreeData * data = dynamic_cast<MergeTreeData *>(table.get());
                if (!data)
                    continue;

                auto data_parts = data->getDataPartsVectorForInternalUsage();
                
                const auto indices = metadata_snapshot->getVectorIndices();

                for (const auto & index : indices)
                {
                    ++rows_count;

                    const auto fail_status = data->getVectorIndexBuildStatus();
                    size_t src_index = 0;
                    size_t res_index = 0;

                    // 'database' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(database_name);
                    // 'table' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(table_name);
                    // 'name' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(index.name);
                    // 'type' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(index.type);
                    // 'expr' column
                    if (column_mask[src_index++])
                    {
                        if (auto expression = index.definition_ast)
                            res_columns[res_index++]->insert(queryToString(expression));
                        else
                            res_columns[res_index++]->insertDefault();
                    }
                    // total data parts
                    if (column_mask[src_index++])
                    {
                        res_columns[res_index++]->insert(data_parts.size());
                    }
                    // vector index built parts
                    if (column_mask[src_index++])
                    {
                        res_columns[res_index++]->insert(getBuiltParts(data_parts, index));
                    }
                    if (column_mask[src_index++])
                    {
                        res_columns[res_index++]->insert(getSmallParts(data, data_parts, index));
                    }
                    // vector index status
                    if (column_mask[src_index++])
                    {
                        size_t built_parts = getBuiltParts(data_parts, index);
                        size_t small_parts = getSmallParts(data, data_parts, index);

                        if (built_parts + small_parts == data_parts.size())
                        {
                            res_columns[res_index++]->insert("Built");
                        }
                        else if (fail_status.latest_failed_part.empty())
                        {
                            res_columns[res_index++]->insert("InProgress");
                        }
                        else
                        {
                            res_columns[res_index++]->insert("Error");
                        }
                    }
                    /// host
                    if (column_mask[src_index++])
                    {
                        auto host = escapeForFileName(getFQDNOrHostName()) + ':' + DB::toString(context->getTCPPort());
                        res_columns[res_index++]->insert(host);
                    }

                    /// latest failed part
                    if (column_mask[src_index++])
                    {
                        res_columns[res_index++]->insert(fail_status.latest_failed_part);
                    }
                    /// latest fail reason
                    if (column_mask[src_index++])
                    {
                        res_columns[res_index++]->insert(fail_status.latest_fail_reason);
                    }
                }
            }
        }
        return Chunk(std::move(res_columns), rows_count);
    }

private:
    std::vector<UInt8> column_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    ContextPtr context;
    size_t database_idx;
    DatabasePtr database;
    std::string database_name;
    DatabaseTablesIteratorPtr tables_it;
};

Pipe StorageSystemVectorIndices::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t /* num_streams */)
{
    auto metadata_snapshot = storage_snapshot->metadata;

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = metadata_snapshot->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        /// Lazy database can contain only very primitive tables,
        /// it cannot contain tables with data skipping indices.
        /// Skip it to avoid unnecessary tables loading in the Lazy database.
        if (database->getEngineName() != "Lazy")
            column->insert(database_name);
    }

    /// Condition on "database" in a query acts like an index.
    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;
    return Pipe(std::make_shared<DataVectorIndicesSource>(
        std::move(columns_mask), std::move(header), max_block_size, std::move(filtered_databases), context));
}

}
