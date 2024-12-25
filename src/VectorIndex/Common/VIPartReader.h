#pragma once

#include <random>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <SearchIndex/VectorIndex.h>
#include <Storages/MergeTree/AlterConversions.h>

#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Storages/VSDescription.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int ABORTED;
}

namespace VectorIndex
{
template<Search::DataType T>
class VIPartReader : public VISourcePartReader<T>
{
public:
    using DataChunk = typename VISourcePartReader<T>::DataChunk;
    using CheckBuildCanceledFunction = std::function<bool()>;

    VIPartReader(
        const DB::MergeTreeDataPartPtr & part_,
        const DB::NamesAndTypesList & cols_,
        const DB::StorageMetadataPtr & metadata_snapshot_,
        DB::MarkCache * mark_cache_,
        const CheckBuildCanceledFunction & check_build_canceled_callback_,
        size_t dimension_,
        bool enforce_fixed_array_)
        : part(part_)
        , cols(std::move(cols_))
        , index_granularity(part->index_granularity)
        , check_build_canceled_callback(check_build_canceled_callback_)
        , dimension(dimension_)
        , total_mask(part->getMarksCount())
        , enforce_fixed_array(enforce_fixed_array_)
    {
        DB::MergeTreeReaderSettings reader_settings;
        DB::StorageSnapshotPtr storage_snapshot_ptr = std::make_shared<DB::StorageSnapshot>(part->storage, metadata_snapshot_);
        reader = part->getReader(
            cols,
            storage_snapshot_ptr,
            DB::MarkRanges{DB::MarkRange(0, total_mask)},
            /*vitual_fields = */ {},
            /* uncompressed_cache = */ nullptr,
            mark_cache_,
            std::make_shared<DB::AlterConversions>(),
            reader_settings,
            {},
            {});
    }

    ~VIPartReader() override
    {}

    size_t numDataRead() const override
    { return num_rows_read; }

    size_t dataDimension() const override
    { return dimension; }

    bool eof() override
    { return num_rows_read == part->rows_count; }

    void seekg(std::streamsize /* offset */, std::ios::seekdir /* dir */) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "seekg() is not implemented in VIPartReader");
    }

    const std::vector<size_t> &emptyIds() const
    { return empty_ids; }

    std::shared_ptr<DataChunk> sampleData(size_t n) override
    {
        LOG_DEBUG(logger, "Sample {} rows from part {}", n, part->name);
        std::vector<std::shared_ptr<DataChunk>> chunks;
        size_t num_rows = 0;
        while (num_rows < n)
        {
            auto chunk = readDataImpl(n - num_rows);
            if (chunk == nullptr)
                break;
            num_rows += chunk->numData();
            chunks.push_back(chunk);
        }
        reset();
        auto ret = merge(chunks);
        if (ret == nullptr)
            throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Cannot sample data from part {}", part->name);
        return ret;
    }

    std::shared_ptr<DataChunk> merge(const std::vector<std::shared_ptr<DataChunk>> &chunks)
    {
        if (chunks.empty())
            return nullptr;
        if (dimension != chunks.back()->dimension())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Wrong dimension of vector data chunk");

        size_t total = 0;
        for (auto chunk: chunks)
            total += chunk->numData();

        Search::idx_t *ids = new Search::idx_t[total]();
        typename SearchIndexDataTypeMap<T>::IndexDatasetType *data;

        if constexpr (T == Search::DataType::FloatVector)
        {
            data = new float[dimension * total]();
            auto cur_data = data;
            auto cur_ids = ids;
            for (auto chunk: chunks)
            {
                auto chunk_data = chunk->getData();
                auto chunk_ids = chunk->getDataID();
                auto chunk_size = chunk->numData();
                memcpy(cur_data, chunk_data, chunk_size * dimension * sizeof(float));
                memcpy(cur_ids, chunk_ids, chunk_size * sizeof(Search::idx_t));
                cur_data += chunk_size * dimension;
                cur_ids += chunk_size;
            }
        }
        else if constexpr (T == Search::DataType::BinaryVector)
        {
            if (dimension % 8 != 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "BinaryVector dimension must be a multiple of 8");
            size_t fixed_N = dimension / 8;

            // Search-index demand `bool*` type to store binary vector data, and each byte stores 8-bit binary data
            data = new bool[total * fixed_N]();
            auto cur_data = data;
            auto cur_ids = ids;
            for (auto chunk: chunks)
            {
                auto chunk_data = chunk->getData();
                auto chunk_ids = chunk->getDataID();
                auto chunk_size = chunk->numData();
                memcpy(cur_data, chunk_data, chunk_size * fixed_N);
                memcpy(cur_ids, chunk_ids, chunk_size * sizeof(Search::idx_t));
                cur_data += chunk_size * fixed_N;
                cur_ids += chunk_size;
            }
        }
        auto merged_chunk = std::make_shared<DataChunk>(data, total, dimension, [=](){ delete[] data; });
        merged_chunk->setDataID(ids, [=](){ delete[] ids; });
        return merged_chunk;
    }

protected:
    std::shared_ptr<DataChunk> readDataImpl(size_t n) override
    {
        if (n == 0)
            return nullptr;

        if (check_build_canceled_callback())
            throw DB::Exception(DB::ErrorCodes::ABORTED, "Cancelled building vector index");

        size_t remaining_size = part->rows_count - num_rows_read;
        size_t max_read_row = std::min(remaining_size, n);
        LOG_DEBUG(logger, "Column size is {}", cols.size());
        DB::Columns result(cols.size());
        LOG_DEBUG(logger, "Reading {} rows from part {} from row {}", max_read_row, part->name, num_rows_read);
        size_t num_rows = reader->readRows(current_mask, total_mask, continue_read, max_read_row, result);
        LOG_DEBUG(logger, "Read {} rows from part {}", num_rows, part->name);
        if (num_rows == 0)
            return nullptr;
        continue_read = true;
        size_t current_round_start_row = num_rows_read;
        num_rows_read += num_rows;
        for (size_t mask = current_mask; mask < total_mask - 1; ++mask)
        {
            if (index_granularity.getMarkStartingRow(mask) >= num_rows_read && index_granularity.getMarkStartingRow(mask + 1) < num_rows_read)
            {
                current_mask = mask;
            }
        }
        const auto &one_column = result.back();

        Search::idx_t * ids;
        typename SearchIndexDataTypeMap<T>::IndexDatasetType *vector_raw_data;
        size_t total_rows = 0;

        if constexpr (T == Search::DataType::FloatVector)
        {
            const DB::ColumnArray *array = DB::checkAndGetColumn<DB::ColumnArray>(one_column.get());
            if (!array)
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector column type for FloatVector is not Array in part {}", part->name);
            }
            const DB::IColumn &src_data = array->getData();
            const DB::ColumnArray::Offsets &offsets = array->getOffsets();
            const DB::ColumnFloat32 *src_data_concrete = DB::checkAndGetColumn<DB::ColumnFloat32>(&src_data);
            if (!src_data_concrete)
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector column inner type in Array is not Float32 in part {}", part->name);
            }
            const DB::PaddedPODArray<Float32> &src_vec = src_data_concrete->getData();

            total_rows = offsets.size();
            if (total_rows == 0)
                return nullptr;

            ids = new Search::idx_t[total_rows]();
            vector_raw_data = new float[dimension * total_rows]();
            for (size_t row = 0; row < total_rows; ++row)
            {
                /// get the start and end offset of the vector in the src_vec
                size_t vec_start_offset = row != 0 ? offsets[row - 1] : 0;
                size_t vec_end_offset = offsets[row];
                if (enforce_fixed_array && vec_end_offset - vec_start_offset != dimension)
                    throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Vector column data length does not meet constraint in part {}", part->name);
                if (vec_start_offset != vec_end_offset)
                {
                    /// Legal vector, copy it to the result
                    for (size_t i = 0; i < dimension && i < vec_end_offset - vec_start_offset; ++i)
                    {
                        vector_raw_data[row * dimension + i] = src_vec[vec_start_offset + i];
                    }
                    ids[row] = current_round_start_row + row;
                } else
                {
                    /// Illegal vector
                    empty_ids.emplace_back(current_round_start_row + row);
                }
            }
        }
        else if constexpr (T == Search::DataType::BinaryVector)
        {
            if (const DB::ColumnFixedString * fixed_string = DB::checkAndGetColumn<DB::ColumnFixedString>(&*one_column))
            {
                auto fixed_N = fixed_string->getN();
                if (fixed_N * 8 != dimension)
                    throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Vector column for BinaryVector, {} * 8 of FixedString(N) does not match dimension {} in part {}", static_cast<UInt64>(fixed_N), dimension, part->name);

                total_rows = fixed_string->size();
                if (total_rows == 0)
                    return nullptr;

                ids = new Search::idx_t[total_rows]();
                vector_raw_data = new bool[total_rows * fixed_N];
                for (size_t row = 0; row < total_rows; row++)
                {
                    const char *binary_vector_data = fixed_string->getDataAt(row).data;
                    memcpy(vector_raw_data + row * fixed_N, binary_vector_data, fixed_N);
                    ids[row] = current_round_start_row + row;
                }
            }
            /// BinaryVector is represented as FixedString(N), sometimes it maybe Sparse(FixedString(N))
            else if (const DB::ColumnSparse *sparse_column = checkAndGetColumn<DB::ColumnSparse>(one_column.get()))
            {
                const DB::ColumnFixedString * sparse_fixed_string = checkAndGetColumn<DB::ColumnFixedString>(&(sparse_column->getValuesColumn()));
                if (!sparse_fixed_string)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector column type for BinaryVector is not FixString(N) in part {}", part->name);

                auto fixed_N = sparse_fixed_string->getN();
                if (fixed_N * 8 != dimension)
                    throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Vector column for BinaryVector, {} * 8 of FixedString(N) does not match dimension {} in part {}", static_cast<UInt64>(fixed_N), dimension, part->name);

                total_rows = sparse_column->size();
                if (total_rows == 0)
                    return nullptr;

                ids = new Search::idx_t[total_rows]();
                vector_raw_data = new bool[total_rows * fixed_N];
                for (size_t row = 0; row < total_rows; row++)
                {
                    const char *binary_vector_data = sparse_column->getDataAt(row).data;
                    memcpy(vector_raw_data + row * fixed_N, binary_vector_data, fixed_N);
                    ids[row] = current_round_start_row + row;
                }
            }
            else
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector column type for BinaryVector is not FixString(N) in part {}", part->name);
        }

        std::shared_ptr<DataChunk> chunk = std::make_shared<DataChunk>(
                vector_raw_data,
                total_rows,
                dimension,
                [=]()
                { delete[] vector_raw_data; });
        chunk->setDataID(ids, [=](){ delete[] ids; });
        return chunk;
    }

    void reset()
    {
        num_rows_read = 0;
        current_mask = 0;
        continue_read = false;
        empty_ids.clear();
    }

private:
    using MergeTreeReaderPtr = std::unique_ptr<DB::IMergeTreeReader>;

    const LoggerPtr logger = getLogger("VIPartReader");
    const DB::MergeTreeDataPartPtr part;
    const DB::NamesAndTypesList cols;
    const DB::MergeTreeIndexGranularity &index_granularity;
    CheckBuildCanceledFunction check_build_canceled_callback;
    const size_t dimension = 0;
    const size_t total_mask;
    const bool enforce_fixed_array;

    MergeTreeReaderPtr reader;
    std::vector<size_t> empty_ids;
    size_t num_rows_read = 0;
    bool continue_read = false;
    size_t current_mask = 0;
};

extern template class VIPartReader<Search::DataType::FloatVector>;
extern template class VIPartReader<Search::DataType::BinaryVector>;
};
