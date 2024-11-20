#include <Access/Common/AccessFlags.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <VectorIndex/Storages/StorageFtsIndex.h>

#if USE_TANTIVY_SEARCH
#include <Interpreters/TantivyFilter.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

const ColumnWithTypeAndName StorageFtsIndex::total_docs{std::make_shared<DataTypeUInt64>(), "total_docs"};
const ColumnWithTypeAndName StorageFtsIndex::field_tokens{
        std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(
                DataTypes{
                    std::make_shared<DataTypeUInt32>(),
                    std::make_shared<DataTypeUInt64>()
                },
                Names{"field_id", "total_tokens"})
        ),
        "field_tokens"
    };
const ColumnWithTypeAndName StorageFtsIndex::terms_freq{
        std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(
                DataTypes{
                    std::make_shared<DataTypeString>(),
                    std::make_shared<DataTypeUInt32>(),
                    std::make_shared<DataTypeUInt64>()
                },
                Names{"term_str", "field_id", "doc_freq"})
        ),
        "terms_freq"
    };
const Block StorageFtsIndex::virtuals_sample_block{total_docs, field_tokens, terms_freq};

StorageFtsIndex::StorageFtsIndex(
    const StorageID & table_id_,
    const StoragePtr & source_table_,
    const ColumnsDescription & columns,
    const bool search_with_index_name_,
    const String & search_column_name_,
    const String & fts_index_name_,
    const String & query_text_)
    : IStorage(table_id_)
    , source_table(source_table_)
    , search_with_index_name(search_with_index_name_)
    , query_text(query_text_)
{
    if (search_with_index_name)
        fts_index_name = fts_index_name_;
    else
        search_column_name = search_column_name_;

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage FtsIndex expected MergeTree table, got: {}", source_table->getName());

    data_parts = merge_tree->getDataPartsVectorForInternalUsage();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);
}

void StorageFtsIndex::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & ,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    const auto & table_metadata = source_table->getInMemoryMetadataPtr();

    String tantivy_index_file_name;
#if USE_TANTIVY_SEARCH
    for (const auto & index_desc : table_metadata->getSecondaryIndices())
    {
        if ((search_with_index_name && index_desc.type == TANTIVY_INDEX_NAME && index_desc.name == fts_index_name) ||
            (!search_with_index_name && index_desc.type == TANTIVY_INDEX_NAME && index_desc.column_names.size() == 1 && index_desc.column_names[0] == search_column_name))
        {
            tantivy_index_file_name = INDEX_FILE_PREFIX + index_desc.name;
            break;
        }
    }
#endif

    if (tantivy_index_file_name.empty())
    {
        if (search_with_index_name)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fts index name '{}' not found in the table {}", fts_index_name, source_table->getStorageID().getTableName());
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "No Fts index found on column '{}' in the '{}' table", search_column_name, source_table->getStorageID().getTableName());
    }

    const auto & storage_columns = table_metadata->getColumns();
    Names columns_from_storage;

    for (const auto & column_name : column_names)
    {
        if (storage_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, column_name))
        {
            columns_from_storage.push_back(column_name);
            continue;
        }
    }

    context->checkAccess(AccessType::SELECT, source_table->getStorageID(), columns_from_storage);

    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    auto this_ptr = std::static_pointer_cast<StorageFtsIndex>(shared_from_this());

    auto reading = std::make_unique<ReadFromFtsIndex>(
        std::move(sample_block), 
        std::move(this_ptr), 
        tantivy_index_file_name, 
        query_text);

    query_plan.addStep(std::move(reading));
}

void ReadFromFtsIndex::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto total_docs_column = StorageFtsIndex::virtuals_sample_block.getByPosition(0).type->createColumn();
    auto total_tokens_column = StorageFtsIndex::virtuals_sample_block.getByPosition(1).type->createColumn();
    auto terms_freq_column = StorageFtsIndex::virtuals_sample_block.getByPosition(2).type->createColumn();

    UInt64 final_total_docs = 0;
    std::map<UInt32, UInt64> total_tokens_map;
    std::map<std::pair<UInt32, String>, UInt64> terms_freq_map;

#if USE_TANTIVY_SEARCH
    for (auto const & part : storage->getDataParts())
    {
        if (!part->getDataPartStorage().exists(tantivy_index_file_name + ".idx"))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fts index file {} does not exist", tantivy_index_file_name);

        auto tantivy_store = TantivyIndexStoreFactory::instance().getOrLoadForSearch(tantivy_index_file_name, part->getDataPartStoragePtr());

        final_total_docs += tantivy_store->getTotalNumDocs();

        auto field_token_vec = tantivy_store->getTotalNumTokens();
        for (const auto & [field_id, total_tokens] : field_token_vec)
        {
            total_tokens_map[field_id] += total_tokens;
        }

        auto term_freq_vec = tantivy_store->getDocFreq(query_text);
        for (const auto & [term_str, field_id, doc_freq] : term_freq_vec)
        {
            terms_freq_map[std::make_pair(field_id, std::string(term_str))] += doc_freq;
        }
    }
#endif

    total_docs_column->insert(final_total_docs);

    {
        /// Fill total_tokens_column, the structure is Array(Tuple(UInt32, UInt64))
        auto & nested_column = static_cast<ColumnTuple &>(static_cast<ColumnArray &>(*total_tokens_column).getData());
        for (const auto & [key, value] : total_tokens_map)
        {
            nested_column.getColumn(0).insert(key);
            nested_column.getColumn(1).insert(value);
        }
        auto & array_offsets = static_cast<ColumnArray &>(*total_tokens_column).getOffsets();
        array_offsets.push_back(nested_column.size());
    }

    {
        /// Fill terms_freq_column, the structure is Array(Tuple(String, UInt32, UInt64))
        auto & nested_column = static_cast<ColumnTuple &>(static_cast<ColumnArray &>(*terms_freq_column).getData());
        for (const auto & [key, value] : terms_freq_map)
        {
            nested_column.getColumn(0).insertData(key.second.data(), key.second.size());
            nested_column.getColumn(1).insert(key.first);
            nested_column.getColumn(2).insert(value);
        }
        auto & array_offsets = static_cast<ColumnArray &>(*terms_freq_column).getOffsets();
        array_offsets.push_back(nested_column.size());
    }

    Block block = Block({
        {std::move(total_docs_column), StorageFtsIndex::virtuals_sample_block.getByPosition(0).type, StorageFtsIndex::virtuals_sample_block.getByPosition(0).name},
        {std::move(total_tokens_column), StorageFtsIndex::virtuals_sample_block.getByPosition(1).type, StorageFtsIndex::virtuals_sample_block.getByPosition(1).name},
        {std::move(terms_freq_column), StorageFtsIndex::virtuals_sample_block.getByPosition(2).type, StorageFtsIndex::virtuals_sample_block.getByPosition(2).name}});

    pipeline.init(Pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), Chunk(block.getColumns(), block.rows()))));
}

}
