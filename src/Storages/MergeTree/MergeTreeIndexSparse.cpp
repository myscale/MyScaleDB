
#include <Storages/MergeTree/MergeTreeIndexSparse.h>

#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


MergeTreeIndexGranuleSparse::MergeTreeIndexGranuleSparse(
    const String & index_name_, const Block & index_sample_block_, const String & json_parameter_)
    : index_name(index_name_), index_sample_block(index_sample_block_), json_parameter(json_parameter_)
{
}

void MergeTreeIndexGranuleSparse::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty sparse granule range {}.", backQuote(index_name));

    const auto & size_type = std::make_shared<DataTypeUInt64>();
    auto size_serialization = size_type->getDefaultSerialization();

    size_serialization->serializeBinary(this->ranges.size(), ostr, {});
    ostr.write(reinterpret_cast<const char *>(this->ranges.data()), this->ranges.size() * sizeof(RowIdRanges::value_type));
}

void MergeTreeIndexGranuleSparse::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown sparse index version {}.", version);

    const auto & size_type = std::make_shared<DataTypeUInt64>();
    auto size_serialization = size_type->getDefaultSerialization();

    Field field_rows;
    size_serialization->deserializeBinary(field_rows, istr, {});

    size_t range_size = field_rows.get<size_t>();
    this->ranges.assign(range_size, {});

    istr.readStrict(reinterpret_cast<char *>(this->ranges.data()), range_size * sizeof(RowIdRanges::value_type));

    if (range_size != 0)
    {
        has_elems = true;
    }
}

MergeTreeIndexAggregatorSparse::MergeTreeIndexAggregatorSparse(
    const String & index_name_, const Block & index_sample_block_, const String & json_parameter_, SparseIndexStorePtr store_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , json_parameter(json_parameter_)
    , store(store_)
    , granule(std::make_shared<MergeTreeIndexGranuleSparse>(index_name, index_sample_block, json_parameter))
{
    SparseIndexSettingsPtr index_settings = std::make_shared<SparseIndexSettings>();
    index_settings->json_parameter = json_parameter;
    index_settings->column = this->index_sample_block.getNames()[0];
    store->setIndexSettings(index_settings);
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSparse::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleSparse>(index_name, index_sample_block, json_parameter);
}

void MergeTreeIndexAggregatorSparse::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. "
            "Position: {}, Block rows: {}.",
            toString(*pos),
            toString(block.rows()));
    }

    // Each column can read rows limit.
    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    // Each column start row_id, they are same in each columns.
    auto start_row_id = store->getNextRowId(rows_read);

    if (index_sample_block.columns() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one column is supported");

    // Traverse all rows that need to be read vertically.
    // Horizontal processing for each row
    for (size_t row_idx = 0; row_idx < rows_read; row_idx++)
    {
        // Record each row sparse-vectors need indexed.
        // Currently only support one column. That's to say each `*_all_*` size is 1.
        std::vector<std::vector<UInt32>> sparse_all_dim_ids;
        std::vector<std::vector<Float32>> sparse_all_f32_weights;
        std::vector<std::vector<UInt8>> sparse_all_u8_weights;

        std::vector<rust::Vec<SPARSE::TupleElement>> sparse_vectors;

        // traverse each column vertically.
        for (const auto & sample_col : this->index_sample_block.getNamesAndTypes())
        {
            rust::Vec<SPARSE::TupleElement> cur_sparse_vector;

            // Get current column with type.
            const auto & col_with_type = block.getByName(sample_col.name);

            if (isMap(col_with_type.type))
            {
                // Cut a single row data, with setting start postion (row_idx).
                const auto & column = col_with_type.column->cut(*pos + row_idx, 1);

                const auto & map_column = assert_cast<const ColumnMap &>(*column);
                const auto * column_array = typeid_cast<const ColumnArray *>(map_column.getNestedColumnPtr().get());
                const auto * column_tuple = typeid_cast<const ColumnTuple *>(column_array->getDataPtr().get());

                const auto * dim_ids_ptr = typeid_cast<const ColumnVector<UInt32> *>(&column_tuple->getColumn(0));
                if (!dim_ids_ptr)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Only u32 types are allowed for the first element of the tuple.");

                if (column_tuple->tupleSize() != 2)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Each tuple in the array must contain exactly 2 elements.");

                const auto * f32_value_ptr = typeid_cast<const ColumnVector<Float32> *>(&column_tuple->getColumn(1));
                const auto * u8_value_ptr = typeid_cast<const ColumnVector<UInt8> *>(&column_tuple->getColumn(1));

                std::uint8_t value_type = 0; // default 0 means f32.
                if (!f32_value_ptr && !u8_value_ptr)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Only u8 and f32 types are allowed for the second element of the tuple.");
                }
                else if (f32_value_ptr && u8_value_ptr)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error, can't convert to f32 and u8 the same time.");
                }
                else if (u8_value_ptr)
                {
                    value_type = 1;
                }

                const auto & dim_ids_data = dim_ids_ptr->getData();
                for (size_t i = 0; i < dim_ids_data.size(); i++)
                {
                    SPARSE::TupleElement element;
                    element.dim_id = dim_ids_data[i];

                    if (value_type == 0)
                    {
                        const auto & f32_data = f32_value_ptr->getData();
                        element.weight_f32 = f32_data[i];
                    }
                    else if (value_type == 1)
                    {
                        const auto & u8_data = u8_value_ptr->getData();
                        element.weight_u8 = u8_data[i];
                    }

                    element.value_type = value_type;

                    cur_sparse_vector.emplace_back(element);
                }
                sparse_vectors.emplace_back(std::move(cur_sparse_vector));
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Only support Map type");
            }
        }
        granule->addRowIdRange(start_row_id + row_idx, static_cast<UInt64>(start_row_id + row_idx));

        if (sparse_vectors.size() != this->index_sample_block.getNames().size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "columns indexed size should be same with sparse_vectors");
        }

        bool status = this->store->indexSparseVector(start_row_id + row_idx, this->index_sample_block.getNames(), sparse_vectors);
        LOG_INFO(
            &Poco::Logger::get("MergeTreeIndexAggregatorSparse"),
            "[update] indexed one row for sparse_index, row_id: {}, sparse_vectors size: {}, insert status: {}",
            start_row_id + row_idx,
            sparse_vectors.size(),
            status);
    }

    granule->has_elems = true;
    *pos += rows_read;
}


bool MergeTreeIndexConditionSparse::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /* idx_granule */) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "mayBeTrueOnGranule is not supported for Sparse skip indexes");
}

bool MergeTreeIndexConditionSparse::alwaysUnknownOrTrue() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "alwaysUnknownOrTrue is not supported for Sparse skip indexes");
}


MergeTreeIndexGranulePtr MergeTreeIndexSparse::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSparse>(index.name, index.sample_block, json_parameter);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSparse::createIndexAggregatorForPart(SparseIndexStorePtr & store) const
{
    return std::make_shared<MergeTreeIndexAggregatorSparse>(index.name, index.sample_block, json_parameter, store);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSparse::createIndexAggregator() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "createIndexAggregator is not supported for Sparse skip indexes");
}

MergeTreeIndexConditionPtr MergeTreeIndexSparse::createIndexCondition(const SelectQueryInfo & /* query */, ContextPtr /* context */) const
{
    return std::make_shared<MergeTreeIndexConditionSparse>();
};

MergeTreeIndexPtr sparseIndexCreator(const IndexDescription & index)
{
    String sparse_index_parameter = index.arguments.empty() ? "{}" : index.arguments[0].get<String>();
    return std::make_shared<MergeTreeIndexSparse>(index, sparse_index_parameter);
}

void sparseIndexValidator(const IndexDescription & index, bool /* attach */)
{
    // TODO test for multi columns.
    if (index.data_types.size() != 1)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SparseIndex only support single column.");
    }

    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isMap())
        {
            const auto & map_data_type = assert_cast<const DataTypeMap &>(*index_data_type);

            const auto & key_type = WhichDataType(map_data_type.getKeyType());
            const auto & val_type = WhichDataType(map_data_type.getValueType());

            if (!key_type.isUInt32())
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY, "The first element of Sparse index Map must be UInt32.");
            }

            if (!val_type.isFloat32() && !val_type.isUInt8())
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY, "The second element of Sparse index Map must be either Float32 or UInt8.");
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY, "Sparse index can only be used with `Map(UInt32, Float32)` or `Map(UInt32, UInt8)`.");
        }
    }
}


}
