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
#include <Storages/MergeTree/MergeTreeIndexTantivy.h>
#include <Storages/MergeTree/TantivyIndexStore.h>
#include <Interpreters/TantivyFilter.h>
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
    text_search_result = textSearch(data_part);
}

void MergeTreeTextSearchManager::executeSearchWithFilter(
    const MergeTreeData::DataPartPtr & data_part,
    const ReadRanges & /* read_ranges */,
    const Search::DenseBitmapPtr filter)
{
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

    TantivyIndexStorePtr tantivy_store = nullptr;

    /// Find inverted index on the search column
    bool find_index = false;
    for (const auto & index_desc : metadata->getSecondaryIndices())
    {
        /// Find tantivy inverted index on the search column
        if (index_desc.type == TANTIVY_INDEX_NAME && index_desc.column_names.size() == 1 &&
            index_desc.column_names[0] == search_column_name)
        {
            OpenTelemetry::SpanHolder span2("MergeTreeTextSearchManager::textSearch()::find_index::initialize index store");
            /// Initialize TantivyIndexStore
            auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
            if (!index_helper->getDeserializedFormat(data_part->getDataPartStorage(), index_helper->getFileName()))
            {
                LOG_DEBUG(log, "File for tantivy index {} does not exist ({}.*). Skipping it.", backQuote(index_helper->index.name),
                    (fs::path(data_part->getDataPartStorage().getFullPath()) / index_helper->getFileName()).string());

                break;
            }

            if (dynamic_cast<const MergeTreeIndexTantivy *>(&*index_helper) != nullptr)
                tantivy_store = TantivyIndexStoreFactory::instance().get(index_helper->getFileName(), data_part->getDataPartStoragePtr());

            if (tantivy_store)
            {
                find_index = true;
                LOG_DEBUG(log, "Find tantivy index {} for column {} in part {}", index_desc.name, search_column_name, data_part->name);

                break;
            }
        }
    }

    if (!find_index)
    {
        /// No tantivy index available
        LOG_DEBUG(log, "Failed to find tantivy index for column {} in part {}", search_column_name, data_part->name);
        tmp_text_search_result->computed = false;
        return tmp_text_search_result;
    }

    /// Find index, load index and do text search
    rust::cxxbridge1::Vec<RowIdWithScore> search_results;
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
            text_search_info->query_text, k, filter_bitmap_vector);
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
            search_results = tantivy_store->bm25Search(text_search_info->query_text, k);
        }
        else
        {
            search_results = tantivy_store->bm25SearchWithFilter(
                                text_search_info->query_text, k, u8_delete_bitmap_vec);
        }
    }
    else
    {
        OpenTelemetry::SpanHolder span3("MergeTreeTextSearchManager::textSearch()::data_part_generate_results_no_filter");
        LOG_DEBUG(log, "Text search no filter");
        search_results = tantivy_store->bm25Search(text_search_info->query_text, k);
    }

    for (size_t i = 0; i < search_results.size(); i++)
    {
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
    const Search::DenseBitmapPtr filter,
    const ColumnUInt64 * part_offset)
{
    mergeSearchResultImpl(pre_result, read_rows, read_ranges, text_search_result, filter, part_offset);
}

}
