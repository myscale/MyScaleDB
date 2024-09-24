#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <VectorIndex/Storages/MergeTreeTextSearchManager.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>

#if USE_TANTIVY_SEARCH
#    include <Interpreters/TantivyFilter.h>
#    include <Storages/MergeTree/MergeTreeIndexTantivy.h>
#    include <Storages/MergeTree/TantivyIndexStore.h>
#    include <Storages/MergeTree/TantivyIndexStoreFactory.h>
#endif

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int ILLEGAL_COLUMN;
}

void MergeTreeTextSearchManager::executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeTextSearchManager::executeSearchBeforeRead");
    if (!preComputed() && text_search_info)
        text_search_result = textSearch(data_part);
}

void MergeTreeTextSearchManager::executeSearchWithFilter(
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & /* read_ranges */,
    const Search::DenseBitmapPtr filter)
{
    if (!preComputed() && text_search_info)
        text_search_result = textSearch(data_part, filter);
}

TextSearchResultPtr MergeTreeTextSearchManager::textSearch(
    const MergeTreeData::DataPartPtr & data_part,
    [[maybe_unused]] const Search::DenseBitmapPtr filter)
{
    OpenTelemetry::SpanHolder span("MergeTreeTextSearchManager::textSearch()");
    TextSearchResultPtr tmp_text_search_result = std::make_shared<CommonSearchResult>();

    if (!data_part)
    {
        LOG_DEBUG(log, "Data part is null");
        return tmp_text_search_result;
    }

    tmp_text_search_result->result_columns.resize(2);
    auto score_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

#if USE_TANTIVY_SEARCH
    const String search_column_name = text_search_info->text_column_name;
    size_t k = static_cast<UInt32>(text_search_info->topk);

    /// Natural language query
    bool enable_nlq = text_search_info->enable_nlq;
    bool operator_or = text_search_info->text_operator == "OR";
    LOG_DEBUG(log, "enable_nlq={}, operator={}", enable_nlq, text_search_info->text_operator);

    /// Support multiple text columns in table function
    bool from_table_function = text_search_info->from_table_func;

    TantivyIndexStorePtr tantivy_store = nullptr;

    /// Suggested index log
    String suggestion_index_log;
    String index_name;
    String db_table_name;

    if (!data_part->storage.getStorageID().database_name.empty())
        db_table_name = data_part->storage.getStorageID().database_name + ".";
    db_table_name += data_part->storage.getStorageID().table_name;

    /// Find inverted index on the search column OR search index name from table function
    bool find_index = false;
    for (const auto & index_desc : metadata->getSecondaryIndices())
    {
        if (index_desc.type == TANTIVY_INDEX_NAME)
        {
            if (from_table_function)
            {
                /// Check index name when from table function
                if (index_desc.name != text_search_info->index_name)
                    continue;
            }
            else /// from hybrid or text search
            {
                /// Check if index contains the text column name
                auto & column_names = index_desc.column_names;
                if (std::find(column_names.begin(), column_names.end(), search_column_name) == column_names.end())
                    continue;
            }

            /// Found matched fts index based index name or text column name
            index_name = index_desc.name;
            suggestion_index_log = "ALTER TABLE " + db_table_name + " MATERIALIZE INDEX " + index_desc.name;

            OpenTelemetry::SpanHolder span2("MergeTreeTextSearchManager::textSearch()::find_index::initialize index store");
            /// Initialize TantivyIndexStore
            auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
            if (!index_helper->getDeserializedFormat(data_part->getDataPartStorage(), index_helper->getFileName()))
            {
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                                "The query was canceled because the FTS index {} has not been built for part {} on table {}. "
                                "Please run the MATERIALIZE INDEX command {} to build the FTS index for existing data. "
                                "If you have already run this command, please wait for it to finish.",
                                index_name, data_part->name, db_table_name, suggestion_index_log);
            }

            if (dynamic_cast<const MergeTreeIndexTantivy *>(&*index_helper) != nullptr)
                tantivy_store = TantivyIndexStoreFactory::instance().getOrLoadForSearch(
                    index_helper->getFileName(), data_part->getDataPartStoragePtr());

            if (tantivy_store)
            {
                find_index = true;
                if (from_table_function)
                    LOG_DEBUG(log, "Find FTS index {} in part {}", index_desc.name, data_part->name);
                else
                    LOG_DEBUG(log, "Find FTS index {} for column {} in part {}", index_desc.name, search_column_name, data_part->name);

                break;
            }
        }
    }

    if (!find_index)
    {
        /// No tantivy index available
        if (index_name.empty()) /// no tantivy index
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                        "The query was canceled because the table {} lacks a full-text search (FTS) index. "
                        "Please create a FTS index before running TextSearch() or HybridSearch() or full_text_search()", db_table_name);
        else
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                            "The query was canceled because the FTS index {} has not been built for part {} on table {}. "
                            "Please run the MATERIALIZE INDEX command {} to build the FTS index for existing data. "
                            "If you have already run this command, please wait for it to finish.",
                            index_name, data_part->name, db_table_name, suggestion_index_log);
    }

    /// Initialize vector for text columns names
    std::vector<String> text_column_names;
    if (!search_column_name.empty())
        text_column_names.emplace_back(search_column_name);

    /// Find index, load index and do text search
    rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> search_results;
    if (filter)
    {
        OpenTelemetry::SpanHolder span3("MergeTreeTextSearchManager::textSearch()::data_part_generate_results_with_filter");
        LOG_DEBUG(log, "Text search with filter");

        /// Construct an uint8_t vector from filter bitmap
        std::vector<uint8_t> filter_bitmap_vector;
        auto byte_size = filter->byte_size();
        auto * bitmap = filter->get_bitmap();
        for (size_t i = 0; i < byte_size; i++)
            filter_bitmap_vector.emplace_back(bitmap[i]);

        search_results = tantivy_store->bm25SearchWithFilter(
            text_search_info->query_text, enable_nlq, operator_or, bm25_stats_in_table, k, filter_bitmap_vector, text_column_names);
    }
    else if (data_part->hasLightweightDelete())
    {
        /// Get delete bitmap if LWD exists
        OpenTelemetry::SpanHolder span3("MergeTreeTextSearchManager::textSearch()::data_part_generate_results_with_lwd");
        LOG_DEBUG(log, "Text search with delete bitmap");

        std::vector<uint8_t> u8_delete_bitmap_vec;

        {
            /// Avoid multiple read from part and set delete bitmap
            std::unique_lock<std::mutex> lock(tantivy_store->mutex_of_delete_bitmap);

            /// Check tantivy index store has non empty delete bitmap
            auto stored_delete_bitmap_ptr = tantivy_store->getDeleteBitmap();
            if (!stored_delete_bitmap_ptr)
            {
                /// Get delete row ids from _row_exists column
                auto del_row_ids = data_part->getDeleteBitmapFromRowExists();
                if (del_row_ids.empty())
                {
                    LOG_DEBUG(log, "The value of row exists column is all 1, delete bitmap will be empty in part {}", data_part->name);
                }
                else
                {
                    /// Construct a DenseBitmap from delete row ids. 0 - deleted, 1 - existing.
                    Search::DenseBitmapPtr del_filter = std::make_shared<Search::DenseBitmap>(data_part->rows_count, true);
                    for (auto del_row_id : del_row_ids)
                        del_filter->unset(del_row_id);

                    /// Construct an u_int8_t vector from DenseBitmap
                    auto byte_size = del_filter->byte_size();
                    auto * bitmap = del_filter->get_bitmap();
                    for (size_t i = 0; i < byte_size; i++)
                        u8_delete_bitmap_vec.emplace_back(bitmap[i]);

                    LOG_DEBUG(log, "Save delete bitmap to tantivy store in part {}", data_part->name);
                }

                tantivy_store->setDeleteBitmap(u8_delete_bitmap_vec);
            }
            else
            {
                u8_delete_bitmap_vec = *stored_delete_bitmap_ptr;
            }
        }

        /// Get non empty delete bitmap (from store or data part) OR fail to get delete bitmap from part
        if (u8_delete_bitmap_vec.empty())
        {
            search_results = tantivy_store->bm25Search(text_search_info->query_text, enable_nlq, operator_or, bm25_stats_in_table, k, text_column_names);
        }
        else
        {
            search_results = tantivy_store->bm25SearchWithFilter(
                                text_search_info->query_text, enable_nlq, operator_or, bm25_stats_in_table, k, u8_delete_bitmap_vec, text_column_names);
        }
    }
    else
    {
        OpenTelemetry::SpanHolder span3("MergeTreeTextSearchManager::textSearch()::data_part_generate_results_no_filter");
        LOG_DEBUG(log, "Text search no filter");
        search_results = tantivy_store->bm25Search(text_search_info->query_text, enable_nlq, operator_or, bm25_stats_in_table, k, text_column_names);
    }

    for (size_t i = 0; i < search_results.size(); i++)
    {
        LOG_TRACE(log, "Label: {}, score in text search: {}", search_results[i].row_id, search_results[i].score);
        label_column->insert(search_results[i].row_id);
        score_column->insert(search_results[i].score);
    }
#endif

    if (label_column->size() > 0)
    {
        tmp_text_search_result->result_columns[0] = std::move(label_column);
        tmp_text_search_result->result_columns[1] = std::move(score_column);
        tmp_text_search_result->computed = true;
    }

    return tmp_text_search_result;
}

void MergeTreeTextSearchManager::mergeResult(
    Columns & pre_result,
    size_t & read_rows,
    const ReadRanges & read_ranges,
    const ColumnUInt64 * part_offset)
{
    mergeSearchResultImpl(pre_result, read_rows, read_ranges, text_search_result, part_offset);
}

}
