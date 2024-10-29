#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <VectorIndex/Storages/MergeTreeSparseSearchManager.h>

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeReader.h>

#if USE_CUSTOM_SKIP_INDEX
#    include <Interpreters/SparseFilter.h>
#    include <Storages/MergeTree/MergeTreeIndexSparse.h>
#    include <Storages/MergeTree/SkipIndex/Factory/SparseIndexFactory.h>
#endif

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int ILLEGAL_COLUMN;
}

void MergeTreeSparseSearchManager::executeSearchBeforeRead(const MergeTreeData::DataPartPtr & data_part)
{
    DB::OpenTelemetry::SpanHolder span("MergeTreeSparseSearchManager::executeSearchBeforeRead");
    if (!preComputed() && sparse_search_info)
        sparse_search_result = sparseSearch(data_part);
}

void MergeTreeSparseSearchManager::executeSearchWithFilter(
    const MergeTreeData::DataPartPtr & data_part, const ReadRanges & /* read_ranges */, const Search::DenseBitmapPtr filter)
{
    if (!preComputed() && sparse_search_info)
        sparse_search_result = sparseSearch(data_part, filter);
}

SparseSearchResultPtr MergeTreeSparseSearchManager::sparseSearch(
    const MergeTreeData::DataPartPtr & data_part, [[maybe_unused]] const Search::DenseBitmapPtr filter)
{
    OpenTelemetry::SpanHolder span("MergeTreeSparseSearchManager::sparseSearch()");
    SparseSearchResultPtr tmp_sparse_search_result = std::make_shared<CommonSearchResult>();

    if (!data_part)
    {
        LOG_DEBUG(log, "Data part is null");
        return tmp_sparse_search_result;
    }

    tmp_sparse_search_result->result_columns.resize(2);
    auto score_column = DataTypeFloat32().createColumn();
    auto label_column = DataTypeUInt32().createColumn();

#if USE_CUSTOM_SKIP_INDEX
    const String search_column_name = sparse_search_info->sparse_column_name;
    UInt32 topk = static_cast<UInt32>(sparse_search_info->topk);

    SparseIndexStorePtr sparse_store = nullptr;

    /// Suggested index log
    String suggestion_index_log;
    String index_name;
    String db_table_name;

    if (!data_part->storage.getStorageID().database_name.empty())
        db_table_name = data_part->storage.getStorageID().database_name + ".";
    db_table_name += data_part->storage.getStorageID().table_name;

    /// Find sparse index on the search column
    bool find_index = false;
    for (const auto & index_desc : metadata->getSecondaryIndices())
    {
        if (index_desc.type == SPARSE_INDEX_NAME)
        {
            /// Check if index contains the sparse column name
            auto & column_names = index_desc.column_names;
            if (std::find(column_names.begin(), column_names.end(), search_column_name) == column_names.end())
                continue;

            /// Found matched sparse index based index name or sparse column name
            index_name = index_desc.name;
            suggestion_index_log = "ALTER TABLE " + db_table_name + " MATERIALIZE INDEX " + index_desc.name;

            OpenTelemetry::SpanHolder span2("MergeTreeSparseSearchManager::sparseSearch()::find_index::initialize index store");
            /// Initialize SparseIndexStore
            auto index_helper = MergeTreeIndexFactory::instance().get(index_desc);
            if (!index_helper->getDeserializedFormat(data_part->getDataPartStorage(), index_helper->getFileName()))
            {
                throw Exception(
                    ErrorCodes::QUERY_WAS_CANCELLED,
                    "The query was canceled because the sparse index {} has not been built for part {} on table {}. "
                    "Please run the MATERIALIZE INDEX command {} to build the sparse index for existing data. "
                    "If you have already run this command, please wait for it to finish.",
                    index_name,
                    data_part->name,
                    db_table_name,
                    suggestion_index_log);
            }

            if (dynamic_cast<const MergeTreeIndexSparse *>(&*index_helper) != nullptr)
                sparse_store
                    = SparseIndexFactory::instance().getOrLoadForSearch(index_helper->getFileName(), data_part->getDataPartStoragePtr());

            if (sparse_store)
            {
                find_index = true;
                LOG_DEBUG(log, "Find sparse index {} for column {} in part {}", index_desc.name, search_column_name, data_part->name);
                break;
            }
        }
    }

    if (!find_index)
    {
        /// No sparse index available
        if (index_name.empty()) /// no sparse index
            throw Exception(
                ErrorCodes::QUERY_WAS_CANCELLED,
                "The query was canceled because the table {} lacks a sparse index. "
                "Please create a sparse index before running SparseSearch()",
                db_table_name);
        else
            throw Exception(
                ErrorCodes::QUERY_WAS_CANCELLED,
                "The query was canceled because the sparse index {} has not been built for part {} on table {}. "
                "Please run the MATERIALIZE INDEX command {} to build the sparse index for existing data. "
                "If you have already run this command, please wait for it to finish.",
                index_name,
                data_part->name,
                db_table_name,
                suggestion_index_log);
    }

    /// Find index, load index and do sparse search
    rust::cxxbridge1::Vec<SPARSE::ScoredPointOffset> search_results;
    if (filter)
    {
        OpenTelemetry::SpanHolder span3("MergeTreeSparseSearchManager::sparseSearch()::data_part_generate_results_with_filter");
        LOG_DEBUG(log, "Sparse search with filter");

        /// Construct an uint8_t vector from filter bitmap
        std::vector<uint8_t> filter_bitmap_vector;
        auto byte_size = filter->byte_size();
        auto * bitmap = filter->get_bitmap();
        for (size_t i = 0; i < byte_size; i++)
            filter_bitmap_vector.emplace_back(bitmap[i]);

        search_results = sparse_store->sparseSearch(sparse_search_info->query_sparse_vector, topk, filter_bitmap_vector);
    }
    else if (data_part->hasLightweightDelete())
    {
        /// Get delete bitmap if LWD exists
        OpenTelemetry::SpanHolder span3("MergeTreeSparseSearchManager::sparseSearch()::data_part_generate_results_with_lwd");
        LOG_DEBUG(log, "Sparse search with delete bitmap");

        std::vector<uint8_t> u8_delete_bitmap_vec;

        {
            /// Avoid multiple read from part and set delete bitmap
            std::unique_lock<std::mutex> lock(sparse_store->mutex_of_delete_bitmap);

            /// Check sparse index store has non empty delete bitmap
            auto stored_delete_bitmap_ptr = sparse_store->getDeleteBitmap();
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

                sparse_store->setDeleteBitmap(u8_delete_bitmap_vec);
            }
            else
            {
                u8_delete_bitmap_vec = *stored_delete_bitmap_ptr;
            }
        }

        search_results = sparse_store->sparseSearch(sparse_search_info->query_sparse_vector, topk, u8_delete_bitmap_vec);
    }
    else
    {
        OpenTelemetry::SpanHolder span3("MergeTreeSparseSearchManager::sparseSearch()::data_part_generate_results_no_filter");
        LOG_DEBUG(log, "Sparse search no filter");
        search_results = sparse_store->sparseSearch(sparse_search_info->query_sparse_vector, topk);
    }

    for (size_t i = 0; i < search_results.size(); i++)
    {
        LOG_TRACE(log, "Label: {}, score in sparse search: {}", search_results[i].row_id, search_results[i].score);
        label_column->insert(search_results[i].row_id);
        score_column->insert(search_results[i].score);
    }
#endif

    if (label_column->size() > 0)
    {
        tmp_sparse_search_result->result_columns[0] = std::move(label_column);
        tmp_sparse_search_result->result_columns[1] = std::move(score_column);
        tmp_sparse_search_result->computed = true;
    }

    return tmp_sparse_search_result;
}

void MergeTreeSparseSearchManager::mergeResult(
    Columns & pre_result, size_t & read_rows, const ReadRanges & read_ranges, const ColumnUInt64 * part_offset)
{
    mergeSearchResultImpl(pre_result, read_rows, read_ranges, sparse_search_result, part_offset);
}

}
