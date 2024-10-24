#pragma once

#include <array>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/SkipIndex/Common/IndexFilesManager.h>
#include <Storages/MergeTree/SkipIndex/Common/IndexSettings.h>

// using namespace SPARSE;


namespace DB
{


static constexpr size_t DESERIALIZE_MAX_RETRY_TIMES = 3;

using SkipIndexDeleteBitmap = std::vector<uint8_t>;
using SkipIndexDeleteBitmapPtr = std::shared_ptr<SkipIndexDeleteBitmap>;

using BoolWithMessage = std::pair<bool, std::pair<bool, String>>;

#define FFI_BOOL_CONVERT(bool_result) \
    std::make_pair(bool_result.result, std::make_pair(bool_result.error.is_error, std::string(bool_result.error.message)))

struct IndexRowId
{
    UInt64 next_row_id = 0;
};


class IndexStore
{
public:
    IndexStore(
        const SkipIndexType index_type_,
        const String & index_name_,
        const String & index_meta_file_suffix,
        const String & index_data_file_suffix,
        const DataPartStoragePtr storage_,
        const MutableDataPartStoragePtr storage_builder_ = nullptr);

    /// @brief Needs free resources, such as index cache files, readers and writers.
    virtual ~IndexStore();

    /// @brief This function is expected to be called only once after the part build index is completed.
    String updateFullIndexPathInCache(const String & new_part_path_in_cache);

    /// @brief Called when building index row by row.
    UInt64 getNextRowId(size_t rows_read);

    /// @brief Maybe many index files in cache, we need serialize them to part file.
    ChecksumPairs serializeIndex();

    /// @brief commit index means that index files in memory will flush to disk.
    bool commitIndex();

    /// @brief Free resources, finish index build.
    bool finalizeIndex();

    /// @brief set current store's index setting, includes index parameter.
    inline void setIndexSettings(IndexSettingsPtr index_settings_) { this->index_settings = index_settings_; }

    String getFullIndexPathInCache();

    bool freeIndexReaderAndWriter();
    bool freeIndexReader();
    bool freeIndexWriter();

    bool loadIndexReader();
    bool loadIndexWriter();


    mutable std::mutex mutex_of_delete_bitmap;
    inline void setDeleteBitmap(const SkipIndexDeleteBitmap & u8_delete_bitmap)
    {
        auto bitmap_ptr = std::make_shared<SkipIndexDeleteBitmap>(u8_delete_bitmap);
        std::atomic_store(&delete_bitmap, std::move(bitmap_ptr));
    }

    SkipIndexDeleteBitmapPtr getDeleteBitmap() { return std::atomic_load(&delete_bitmap); }


protected:
    SkipIndexType index_type;
    String index_name;
    const DataPartStoragePtr storage;
    const MutableDataPartStoragePtr storage_builder;
    std::unique_ptr<IndexFilesManager> index_files_manager = nullptr;
    Poco::Logger * log;

    IndexRowId index_row_id;

    IndexSettingsPtr index_settings = nullptr;

    /// @brief DeleteBitmap used for part with lightweight delete
    SkipIndexDeleteBitmapPtr delete_bitmap = nullptr;

    std::mutex index_reader_mutex;
    bool index_reader_status = false;

    std::mutex index_writer_mutex;
    bool index_writer_status = false;

    void setIndexWriterStatus(bool status)
    {
        std::lock_guard<std::mutex> lock(this->index_writer_mutex);
        index_writer_status = status;
    }

    bool getIndexWriterStatus()
    {
        std::lock_guard<std::mutex> lock(this->index_writer_mutex);
        return index_writer_status;
    }

    virtual BoolWithMessage freeIndexReaderImpl(const String & full_index_path) = 0;
    virtual BoolWithMessage freeIndexWriterImpl(const String & full_index_path) = 0;

    virtual BoolWithMessage commitIndexImpl(const String & full_index_path) = 0;

    virtual BoolWithMessage loadIndexReaderImpl(const String & full_index_path) = 0;
    virtual BoolWithMessage loadIndexWriterImpl(const String & full_index_path) = 0;
};

using IndexStorePtr = std::shared_ptr<IndexStore>;


}
