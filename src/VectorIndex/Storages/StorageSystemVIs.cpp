#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/queryToString.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/VirtualColumnUtils.h>
#include <VectorIndex/Storages/StorageSystemVIs.h>
#include <base/getFQDNOrHostName.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <VectorIndex/Common/SegmentsMgr.h>
#include <VectorIndex/Common/StorageVectorIndicesMgr.h>
#include <VectorIndex/Common/SegmentStatus.h>

namespace DB
{
StorageSystemVIs::StorageSystemVIs(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"name", std::make_shared<DataTypeString>(), "Index name."},
        {"type", std::make_shared<DataTypeString>(), "Index type."},
        {"expr", std::make_shared<DataTypeString>(), "Index expression."},
        {"total_parts", std::make_shared<DataTypeInt64>(), "Total number of parts."},
        {"parts_with_vector_index", std::make_shared<DataTypeInt64>(), "Number of parts with vector index built."},
        {"small_parts", std::make_shared<DataTypeInt64>(), "Number of small parts."},
        {"status", std::make_shared<DataTypeString>(), "Index status."},
        {"host_name", std::make_shared<DataTypeString>(), "Host name of the server where the index is built."},
        {"latest_failed_part", std::make_shared<DataTypeString>(), "Name of the latest failed part."},
        {"latest_fail_reason", std::make_shared<DataTypeString>(), "Reason of the latest failure."},
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
    size_t getBuiltParts(const MergeTreeData::DataPartsVector& data_parts, const VIDescription & index)
    {
        size_t built_parts = 0;
        for (auto & data_part : data_parts)
        {
            if (data_part->segments_mgr->getSegmentStatus(index.name) == VectorIndex::SegmentStatus::BUILT)
                ++built_parts;
        }
        return built_parts;
    }

    size_t getSmallParts(const MergeTreeData* /*data*/, const MergeTreeData::DataPartsVector & data_parts, const VIDescription & index)
    {
        size_t small_parts = 0;
        for (auto & data_part : data_parts)
        {
            /// if we enlarge small part size, there may exists some parts with indices built earlier.
            if (data_part->segments_mgr->getSegmentStatus(index.name) == VectorIndex::SegmentStatus::SMALL_PART)
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

                    const auto fail_status = data->getVectorIndexManager()->getVectorIndexObject(index.name)->getVectorIndexBuildStatus();
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

Pipe StorageSystemVIs::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t /* num_streams */)
{
    storage_snapshot->check(column_names);

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.contains(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    /// Add `database` column.
    MutableColumnPtr database_column_mut = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.columns

        /// We are skipping "Lazy" database because we cannot afford initialization of all its tables.
        /// This should be documented.

        if (database->getEngineName() != "Lazy")
            database_column_mut->insert(database_name);
    }

    /// Condition on "database" in a query acts like an index.
    Block block_to_filter { ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database") };

    ActionDAGNodes added_filter_nodes;
    if (query_info.prewhere_info)
    {
        const auto & node = query_info.prewhere_info->prewhere_actions.findInOutputs(query_info.prewhere_info->prewhere_column_name);
        added_filter_nodes.nodes.push_back(&node);
    }

    std::optional<ActionsDAG> filter;
    if (auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes))
        filter = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter);

    const ActionsDAG::Node * predicate = filter ? filter->getOutputs().at(0) : nullptr;

    /// Filter block with `database` column.
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block_to_filter, context);

    if (!block_to_filter.rows())
        return Pipe(std::make_shared<NullSource>(header));

    ColumnPtr & database_column = block_to_filter.getByName("database").column;
    return Pipe(std::make_shared<DataVectorIndicesSource>(
        std::move(columns_mask), std::move(header), max_block_size, std::move(database_column), context));
}

}
