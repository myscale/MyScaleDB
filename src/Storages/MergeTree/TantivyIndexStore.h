#pragma once

#include <array>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <tantivy_search.h>
#include <tantivy_search_cxx.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <roaring.hh>
#include <Common/FST.h>


namespace DB
{

static constexpr auto TANTIVY_INDEX_OFFSET_FILE_TYPE = ".meta";
static constexpr auto TANTIVY_INDEX_DATA_FILE_TYPE = ".data";
static constexpr auto TANTIVY_TEMP_DISK_NAME = "_tmp_tantivy";

struct TantivyIndexRowId
{
    UInt64 next_row_id = 0;
};

struct TantivyIndexFileMeta
{
    TantivyIndexFileMeta() { }

    TantivyIndexFileMeta(const String & file_name_, UInt64 offset_begin_, UInt64 offset_end_)
    {
        strcpy(file_name, file_name_.c_str());
        offset_begin = offset_begin_;
        offset_end = offset_end_;
    }

    /// The length of file_name should not exceed 128.
    char file_name[512] = {};
    UInt64 offset_begin = 0;
    UInt64 offset_end = 0;
};

using TantivyIndexFileMetas = std::vector<TantivyIndexFileMeta>;
using ChecksumPairs = std::vector<std::pair<String, MergeTreeDataPartChecksums::Checksum>>;

struct TantivyIndexSettings
{
    std::vector<String> indexed_columns = {};
    String index_json_parameter = "{}";
    TantivyIndexSettings & operator=(const TantivyIndexSettings & other)
    {
        if (this != &other)
        {
            indexed_columns = other.indexed_columns;
            index_json_parameter = other.index_json_parameter;
        }
        return *this;
    }
};


class TantivyIndexFilesManager
{
public:
    TantivyIndexFilesManager(const String & skp_index_name_, const DataPartStoragePtr storage_);

    TantivyIndexFilesManager(const String & skp_index_name_, const DataPartStoragePtr storage_, MutableDataPartStoragePtr storage_builder_);

    ChecksumPairs serialize();

    void deserialize();

    String getTantivyIndexCacheDirectory();

    /// @brief update data part path in cache.
    /// @param target_part_cache_path example: `store/xxx/xxx/all_1_1_0_2`
    String updateCacheDataPartRelativeDirectory(const String & target_part_cache_path);

    void removeTantivyIndexCacheDirectory();

    /// @brief recursive remove `tantivy_index_cache_path` in cache. /store/xx/xxx/all_1_1_0/skp_xx/ -> /store/.
    static void removeTantivyIndexInCache(const String & relative_data_part_in_cache, const String & skp_index_name);

    /// @brief recursive remove `tantivy_index_cache_path` in cache. /store/xx/xxx/all_1_1_0/skp_xx/ -> /store/.
    static void removeTantivyIndexInCache(const String & tantivy_index_cache_directory);

    /// @brief recursive remove `data_part_path` in cache. /store/xx/xxx/all_1_1_0 -> /store/.
    static void removeDataPartInCache(const String & relative_data_part_in_cache);

    /// @brief recursive remove `table_uuid_path` in tantivy cache. /store/xx/xxx/ -> /store/.
    static void removeEmptyTableUUIDInCache(const String & relative_data_part_in_cache);

private:
    String skp_index_name;
    DataPartStoragePtr storage;
    MutableDataPartStoragePtr storage_builder;
    LoggerPtr log;

    DiskPtr tmp_disk;

    String index_meta_file_name;
    String index_data_file_name;

    String tantivy_index_cache_directory = "";
    mutable std::shared_mutex tantivy_index_cache_directory_mutex;

    void initTantivyIndexCacheDirectory();

    bool hasTantivyIndexInDataPart();

    /// @brief get Full `data_part_path` in tantivy cache.
    /// @param relative_data_part_in_cache data part relative path.
    static std::optional<fs::path> getDataPartFullPathInCache(const String & relative_data_part_in_cache);
};


using TantivyDelBitmap = std::vector<uint8_t>;
using TantivyDelBitmapPtr = std::shared_ptr<TantivyDelBitmap>;

class TantivyIndexStore
{
public:
    TantivyIndexStore(const String & skp_index_name_, DataPartStoragePtr storage_);
    TantivyIndexStore(const String & skp_index_name_, DataPartStoragePtr storage_, MutableDataPartStoragePtr storage_builder_);
    ~TantivyIndexStore();

    // static String simplifyRelativePartPath(const String & relative_data_part_path);

    inline void setTantivyIndexSettings(TantivyIndexSettings & index_settings_) { index_settings = index_settings_; }

    ChecksumPairs serialize();
    String updateCacheDataPartRelativeDirectory(const String & target_part_cache_path);
    String getTantivyIndexCacheDirectory();

    mutable std::mutex mutex_of_delete_bitmap;

    void setDeleteBitmap(const TantivyDelBitmap & u8_delete_bitmap)
    {
        auto bitmap_ptr = std::make_shared<TantivyDelBitmap>(u8_delete_bitmap);
        std::atomic_store(&delete_bitmap, std::move(bitmap_ptr));
    }

    TantivyDelBitmapPtr getDeleteBitmap() { return std::atomic_load(&delete_bitmap); }

    /// For index build.
    UInt64 getNextRowId(size_t rows_read);
    bool indexMultiColumnDoc(uint64_t row_id, std::vector<String> & column_names, std::vector<String> & docs);
    bool finalizeTantivyIndex();
    void removeTantivyIndexCache();
    bool loadTantivyIndexReader();

    /// For Skip Index Query.
    rust::cxxbridge1::Vec<std::uint8_t> singleTermQueryBitmap(String column_name, String term);
    rust::cxxbridge1::Vec<std::uint8_t> sentenceQueryBitmap(String column_name, String sentence);
    rust::cxxbridge1::Vec<std::uint8_t> regexTermQueryBitmap(String column_name, String pattern);
    rust::cxxbridge1::Vec<std::uint8_t> termsQueryBitmap(String column_name, std::vector<String> terms);

    /// For BM25Search and HybridSearch. If enable_nlq is true, use Natural Language Search. If enable_nlq is false, use Standard Search.
    rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> bm25Search(
        String sentence,
        bool enable_nlq,
        bool operator_or,
        TANTIVY::Statistics & statistics,
        size_t topk,
        std::vector<String> column_names = {});

    rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> bm25SearchWithFilter(
        String sentence,
        bool enable_nlq,
        bool operator_or,
        TANTIVY::Statistics & statistics,
        size_t topk,
        const std::vector<uint8_t> & u8_alived_bitmap,
        std::vector<String> column_names = {});

    /// Get current part sentence doc_freq, sentence will be tokenized by tokenizer with each indexed column.
    rust::cxxbridge1::Vec<TANTIVY::DocWithFreq> getDocFreq(String sentence);
    /// Get current part total_num_docs, each column will have same total_num_docs.
    UInt64 getTotalNumDocs();
    /// Get current part total_num_tokens, each column will have it's own total_num_tokens.
    rust::cxxbridge1::Vec<TANTIVY::FieldTokenNums> getTotalNumTokens();
    /// Get the number of documents stored in the index file.
    UInt64 getIndexedDocsNum();


private:
    // The name of the created index.
    String skp_index_name;
    // We use `storage` to read FTS index (meta/data) file from data part.
    DataPartStoragePtr storage;
    // We use `storage_builder` to write FTS index (meta/data) file into data part.
    MutableDataPartStoragePtr storage_builder;
    // `index_files_manager` is responsible for manipulating FTS index files directly.
    std::unique_ptr<TantivyIndexFilesManager> index_files_manager;

    TantivyIndexRowId tantivy_index_row_id;

    // Represent whether FTS index reader has been loaded.
    std::mutex index_reader_mutex;
    bool index_reader_status = false;

    // Represent whether FTS index writer has been loaded.
    std::mutex index_writer_mutex;
    bool index_writer_status = false;

    TantivyIndexSettings index_settings;

    LoggerPtr log;

    /// DeleteBitmap used for part with lightweight delete
    TantivyDelBitmapPtr delete_bitmap = nullptr;

    void setIndexWriterStatus(bool status)
    {
        std::lock_guard<std::mutex> lock(index_writer_mutex);
        index_writer_status = status;
    }

    bool getIndexWriterStatus()
    {
        std::lock_guard<std::mutex> lock(index_writer_mutex);
        return index_writer_status;
    }

    bool freeTantivyIndexReader();
    bool freeTantivyIndexWriter();

    bool freeTantivyIndex();
    void commitTantivyIndex();

    bool getTantivyIndexReader();
    bool getTantivyIndexWriter();
};

using TantivyIndexStorePtr = std::shared_ptr<TantivyIndexStore>;
}
