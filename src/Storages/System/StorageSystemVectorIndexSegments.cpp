#include "StorageSystemVectorIndexSegments.h"
#include <mutex>
#include <set>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/VectorIndexInfo.h>
#include <Storages/VirtualColumnUtils.h>

#include <SearchIndex/Common/Utils.h>
#include <VectorIndex/Metadata.h>
#include <VectorIndex/SegmentId.h>
#include <VectorIndex/VectorSegmentExecutor.h>

namespace DB
{

StorageSystemVectorIndexSegments::StorageSystemVectorIndexSegments(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"part", std::make_shared<DataTypeString>()},
        {"owner_part", std::make_shared<DataTypeString>()},
        {"owner_part_id", std::make_shared<DataTypeInt32>()},
        {"name", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeString>()},
        {"total_vectors", std::make_shared<DataTypeUInt64>()},
        {"memory_usage_bytes", std::make_shared<DataTypeUInt64>()},
        {"disk_usage_bytes", std::make_shared<DataTypeUInt64>()},
        {"progress", std::make_shared<DataTypeUInt8>()},
        {"elapsed", std::make_shared<DataTypeUInt64>()},
        {"error", std::make_shared<DataTypeString>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

class DataVectorIndexSegmentsSource : public ISource
{
public:
    DataVectorIndexSegmentsSource(
        std::vector<UInt8> columns_mask_, Block header, UInt64 max_block_size_, ColumnPtr databases_, ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
        , database_idx(0)
    {
    }

    String getName() const override { return "DataVectorIndexSegments"; }

protected:
    void getVectorIndexInfo(
        const VectorIndexDescription & index,
        const VectorIndexInfoPtr & vec_info,
        const std::string & table_name,
        const MergeTreeData::DataPartPtr & part,
        const std::set<std::string> & cached_indices,
        MutableColumns & res_columns)
    {
        std::string owner_part;
        int owner_part_id;
        if (vec_info == nullptr || vec_info->owner_part.empty())
        {
            owner_part = part->name;
            owner_part_id = 0;
        }
        else
        {
            owner_part = vec_info->owner_part;
            owner_part_id = vec_info->owner_part_id;
        }

        String vector_index_cache_prefix = fs::path(part->storage.getContext()->getVectorIndexCachePath())
            / part->storage.getRelativeDataPath() / part->info.getPartNameWithoutMutation() / "";

        const DataPartStorageOnDiskBase * part_storage
            = dynamic_cast<const DataPartStorageOnDiskBase *>(part->getDataPartStoragePtr().get());
        if (part_storage == nullptr)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");
        }

        VectorIndex::SegmentId segment_id(
            getVolumeFromPartStorage(*part_storage),
            part->getDataPartStorage().getFullPath(),
            part->name,
            owner_part,
            index.name,
            index.column,
            vector_index_cache_prefix,
            owner_part_id);

        VectorIndex::Metadata metadata(segment_id);
        bool metadata_loaded = true;

        try
        {
            auto buf = segment_id.volume->getDisk()->readFile(segment_id.getVectorReadyFilePath());
            metadata.readText(*buf);
        }
        catch (...)
        {
            metadata_loaded = false;
        }

        size_t src_index = 0;
        size_t res_index = 0;

        /// 'database' column
        if (column_mask[src_index++])
        {
            res_columns[res_index++]->insert(database_name);
        }
        /// 'table' column
        if (column_mask[src_index++])
            res_columns[res_index++]->insert(table_name);
        /// 'part' column
        if (column_mask[src_index++])
            res_columns[res_index++]->insert(part->name);
        /// 'owner_part' column
        if (column_mask[src_index++])
            res_columns[res_index++]->insert(owner_part);
        /// 'owner_part_id' column
        if (column_mask[src_index++])
            res_columns[res_index++]->insert(owner_part_id);
        /// 'name' column
        if (column_mask[src_index++])
        {
            res_columns[res_index++]->insert(index.name);
        }
        /// 'type' column
        if (column_mask[src_index++])
        {
            if (metadata_loaded && metadata.fallback_to_flat)
                res_columns[res_index++]->insert(Search::enumToString(Search::IndexType::FLAT));
            else
                res_columns[res_index++]->insert(index.type);
        }
        /// 'status' column
        if (column_mask[src_index++])
        {
            if (cached_indices.contains(segment_id.getCacheKey().toString()))
                res_columns[res_index++]->insert(Search::enumToString(VectorIndexStatus::LOADED));
            else if (vec_info)
                res_columns[res_index++]->insert(vec_info->statusString());
            else
                res_columns[res_index++]->insertDefault();
        }
        /// 'total_vectors' column
        if (column_mask[src_index++])
        {
            if (vec_info)
                res_columns[res_index++]->insert(vec_info->total_vec);
            else
                res_columns[res_index++]->insertDefault();
        }
        /// 'memory_usage_bytes' column
        if (column_mask[src_index++])
        {
            if (vec_info)
                res_columns[res_index++]->insert(vec_info->memory_usage_bytes);
            else
                res_columns[res_index++]->insertDefault();
        }
        /// 'disk_usage_bytes' column
        if (column_mask[src_index++])
        {
            if (vec_info)
                res_columns[res_index++]->insert(vec_info->disk_usage_bytes);
            else
                res_columns[res_index++]->insertDefault();
        }
        /// 'progress' column
        if (column_mask[src_index++])
        {
            if (vec_info)
                res_columns[res_index++]->insert(vec_info->progress());
            else
                res_columns[res_index++]->insertDefault();
        }
        /// 'elapsed' column
        if (column_mask[src_index++])
        {
            if (vec_info)
                res_columns[res_index++]->insert(static_cast<UInt64>(vec_info->elapsed()));
            else
                res_columns[res_index++]->insertDefault();
        }
        /// 'error' column
        if (column_mask[src_index++])
        {
            if (vec_info)
                res_columns[res_index++]->insert(vec_info->err_msg);
            else
                res_columns[res_index++]->insertDefault();
        }
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

            std::set<std::string> cached_indices;
            for (const auto & it : VectorIndex::VectorSegmentExecutor::getAllCacheNames())
                cached_indices.emplace(it.first.toString());

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

                for (const auto & part : data_parts)
                {
                    std::lock_guard lock(part->vector_indices_mutex);

                    for (const auto & index : indices)
                    {
                        bool found = false;
                        if (part->containRowIdsMaps())
                        {
                            if (auto it = part->vector_indices_decoupled.find(index.name); it != part->vector_indices_decoupled.end())
                            {
                                found = true;

                                for (const auto & info : it->second)
                                {
                                    ++rows_count;
                                    getVectorIndexInfo(index, info, table_name, part, cached_indices, res_columns);
                                }
                            }
                        }

                        if (!found)
                        {
                            ++rows_count;

                            VectorIndexInfoPtr info{nullptr};
                            if (auto it = part->vector_indices.find(index.name); it != part->vector_indices.end())
                                info = it->second;

                            getVectorIndexInfo(index, info, table_name, part, cached_indices, res_columns);
                        }
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

Pipe StorageSystemVectorIndexSegments::read(
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
    Block block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database")};
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;
    return Pipe(std::make_shared<DataVectorIndexSegmentsSource>(
        std::move(columns_mask), std::move(header), max_block_size, std::move(filtered_databases), context));
}

}
