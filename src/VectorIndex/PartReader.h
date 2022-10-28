#pragma once

#include <random>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

#include <SearchIndex/VectorIndex.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int ABORTED;
}

namespace VectorIndex
{

class PartReader : public Search::IndexSourceDataReader<float>
{
public:

    using CheckBuildCanceledFunction = std::function<bool()>;

    PartReader(
        const DB::MergeTreeDataPartPtr & part_,
        const DB::NamesAndTypesList & cols_,
        const DB::StorageMetadataPtr & metadata_snapshot_,
        DB::MarkCache * mark_cache_,
        const CheckBuildCanceledFunction & check_build_canceled_callback_,
        size_t dimension_,
        bool enforce_fixed_array);
    ~PartReader() override { }

    std::shared_ptr<DataChunk> sampleData(size_t n) override;

    size_t numDataRead() const override;

    size_t dataDimension() const override;

    bool eof() override;

    void seekg(std::streamsize /* offset */, std::ios::seekdir /* dir */) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "seekg() is not implemented in PartReader");
    }

    const std::vector<size_t> & emptyIds() const { return empty_ids; }

protected:
    std::shared_ptr<DataChunk> readDataImpl(size_t n) override;
    void reset();

private:
    using MergeTreeReaderPtr = std::unique_ptr<DB::IMergeTreeReader>;

    const Poco::Logger * logger = &Poco::Logger::get("PartReader");
    const DB::MergeTreeDataPartPtr & part;
    const DB::NamesAndTypesList & cols;
    const DB::MergeTreeIndexGranularity & index_granularity;
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
};
