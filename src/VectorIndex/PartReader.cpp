#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <VectorIndex/PartReader.h>

namespace VectorIndex
{
PartReader::PartReader(
    const DB::MergeTreeDataPartPtr & part_,
    const DB::NamesAndTypesList & cols_,
    const DB::StorageMetadataPtr & metadata_snapshot_,
    DB::MarkCache * mark_cache_,
    const CheckBuildCanceledFunction & check_build_canceled_callback_,
    size_t dimension_,
    bool enforce_fixed_array_)
    : part(part_)
    , cols(cols_)
    , index_granularity(part->index_granularity)
    , check_build_canceled_callback(check_build_canceled_callback_)
    , dimension(dimension_)
    , total_mask(part->getMarksCount())
    , enforce_fixed_array(enforce_fixed_array_)
{
    DB::MergeTreeReaderSettings reader_settings;
    reader = part->getReader(
        cols,
        metadata_snapshot_,
        DB::MarkRanges{DB::MarkRange(0, total_mask)},
        /* uncompressed_cache = */ nullptr,
        mark_cache_,
        reader_settings,
        {},
        {});
}

std::shared_ptr<PartReader::DataChunk> merge(const std::vector<std::shared_ptr<PartReader::DataChunk>> & chunks)
{
    if (chunks.empty())
        return nullptr;
    auto dimension = chunks.back()->dimension();
    size_t total = 0;
    for (auto chunk : chunks)
        total += chunk->numData();
    float * data = new float[dimension * total]();
    Search::idx_t * ids = new Search::idx_t[total]();
    auto merged_chunk = std::make_shared<PartReader::DataChunk>(data, total, dimension, [=]() { delete[] data; });
    merged_chunk->setDataID(ids, [=]() { delete[] ids; });
    auto cur_data = data;
    auto cur_ids = ids;
    for (auto chunk : chunks)
    {
        auto chunk_data = chunk->getData();
        auto chunk_ids = chunk->getDataID();
        auto chunk_size = chunk->numData();
        memcpy(cur_data, chunk_data, chunk_size * dimension * sizeof(float));
        memcpy(cur_ids, chunk_ids, chunk_size * sizeof(Search::idx_t));
        cur_data += chunk_size * dimension;
        cur_ids += chunk_size;
    }
    return merged_chunk;
}

std::shared_ptr<PartReader::DataChunk> PartReader::sampleData(size_t n)
{
    LOG_DEBUG(logger, "Sample {} rows from part {}", n, part->name);
    std::vector<std::shared_ptr<PartReader::DataChunk>> chunks;
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

size_t PartReader::numDataRead() const
{
    return num_rows_read;
}

size_t PartReader::dataDimension() const
{
    return dimension;
}

bool PartReader::eof()
{
    return num_rows_read == part->rows_count;
}

std::shared_ptr<PartReader::DataChunk> PartReader::readDataImpl(size_t n)
{
    if (n == 0)
        return nullptr;

    if (check_build_canceled_callback())
        throw DB::Exception(DB::ErrorCodes::ABORTED, "Cancelled building vector index");

    size_t remaining_size = part->rows_count - num_rows_read;
    size_t max_read_row = std::min(remaining_size, n);
    DB::Columns result(cols.size());
    LOG_DEBUG(logger, "Reading {} rows from part {} from row {}", max_read_row, part->name, num_rows_read);
    LOG_DEBUG(logger, "Column size is {}", cols.size());
    size_t num_rows = reader->readRows(current_mask, 0, continue_read, max_read_row, result);
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
    const auto & one_column = result.back();
    const DB::ColumnArray * array = DB::checkAndGetColumn<DB::ColumnArray>(one_column.get());
    if (!array)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "vector column type is not Array in part {}", part->name);
    }
    const DB::IColumn & src_data = array->getData();
    const DB::ColumnArray::Offsets & offsets = array->getOffsets();
    const DB::ColumnFloat32 * src_data_concrete = DB::checkAndGetColumn<DB::ColumnFloat32>(&src_data);
    if (!src_data_concrete)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "vector column inner type in Array is not Float32 in part {}", part->name);
    }

    const DB::PaddedPODArray<DB::Float32> & src_vec = src_data_concrete->getData();

    if (enforce_fixed_array && src_vec.size() != dimension * offsets.size())
    {
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Vector column data length does not meet constraint in part {}", part->name);
    }

    if (src_vec.empty())
        return nullptr;

    float * vector_raw_data = new float[dimension * offsets.size()]();
    std::shared_ptr<PartReader::DataChunk> chunk
        = std::make_shared<PartReader::DataChunk>(vector_raw_data, offsets.size(), dimension, [=]() { delete[] vector_raw_data; });
    Search::idx_t * ids = new Search::idx_t[offsets.size()]();
    chunk->setDataID(ids, [=]() { delete[] ids; });

    for (size_t row = 0; row < offsets.size(); ++row)
    {
        /// get the start and end offset of the vector in the src_vec
        size_t vec_start_offset = row != 0 ? offsets[row - 1] : 0;
        size_t vec_end_offset = offsets[row];
        if (enforce_fixed_array && vec_end_offset - vec_start_offset != dimension)
            throw DB::Exception(
                DB::ErrorCodes::INCORRECT_DATA, "Vector column data length does not meet constraint in part {}", part->name);
        if (vec_start_offset != vec_end_offset)
        {
            /// this is a valid vector, copy it to the result
            for (size_t i = 0; i < dimension && i < vec_end_offset - vec_start_offset; ++i)
            {
                vector_raw_data[row * dimension + i] = src_vec[vec_start_offset + i];
            }
            ids[row] = current_round_start_row + row;
        }
        else
        {
            /// this is an empty vector
            empty_ids.emplace_back(current_round_start_row + row);
        }
    }

    return chunk;
}

void PartReader::reset()
{
    num_rows_read = 0;
    current_mask = 0;
    continue_read = false;
    empty_ids.clear();
}
}
