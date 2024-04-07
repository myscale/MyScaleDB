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

    TantivyIndexFileMeta(const String & file_name_, UInt32 offset_begin_, UInt32 offset_end_)
    {
        strcpy(file_name, file_name_.c_str());
        offset_begin = offset_begin_;
        offset_end = offset_end_;
    }

    /// The length of file_name should not exceed 128.
    char file_name[128] = {};
    UInt32 offset_begin = 0;
    UInt32 offset_end = 0;
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

using TantivyDelBitmap = std::vector<uint8_t>;
using TantivyDelBitmapPtr = std::shared_ptr<TantivyDelBitmap>;

class TantivyIndexStore
{
public:
    TantivyIndexStore(const String & name_, DataPartStoragePtr storage_);
    TantivyIndexStore(const String & name_, DataPartStoragePtr storage_, MutableDataPartStoragePtr data_part_storage_builder_);
    ~TantivyIndexStore();

    /// Check existence by checking the existence of file .meta
    bool exists() const;

    UInt64 getNextRowId(size_t rows_read);

    inline void setTantivyIndexSettings(TantivyIndexSettings & index_settings_) { index_settings = index_settings_; }

    /// Return the name of the created index.
    inline String getName() { return name; }

    /// Serialize tantivy index files from `index_files_cache_path` to current data part.
    ChecksumPairs serialize();

    /// Deserialize tantivy index files(`*.data`, `*.meta`) from current data part to `index_files_cache_path`.
    void deserialize();

    /// Index a document.
    bool indexMultiColumnDoc(uint64_t row_id, std::vector<String> & column_names, std::vector<String> & docs);

    /// Submit the index and release resources.
    bool finalizeTantivyIndex();

    /// For Skip Index Row Id Range Query.(Deprecated)
    bool singleTermQueryWithRowIdRange(String column_name, String term, UInt64 lrange, UInt64 rrange);
    bool regexTermQueryWithRowIdRange(String column_name, String pattern, UInt64 lrange, UInt64 rrange);
    bool sentenceQueryWithRowIdRange(String column_name, String pattern, UInt64 lrange, UInt64 rrange);
    bool termsQueryWithRowIdRange(String column_name, std::vector<String> terms, UInt64 lrange, UInt64 rrange);

    /// For Skip Index Query.
    rust::cxxbridge1::Vec<std::uint8_t> singleTermQueryBitmap(String column_name, String term);
    rust::cxxbridge1::Vec<std::uint8_t> sentenceQueryBitmap(String column_name, String sentence);
    rust::cxxbridge1::Vec<std::uint8_t> regexTermQueryBitmap(String column_name, String pattern);
    rust::cxxbridge1::Vec<std::uint8_t> termsQueryBitmap(String column_name, std::vector<String> terms);

    /// For BM25Search and HybridSearch
    rust::cxxbridge1::Vec<RowIdWithScore> bm25Search(String sentence, size_t topk);

    rust::cxxbridge1::Vec<RowIdWithScore> bm25SearchWithFilter(String sentence, size_t topk, const std::vector<uint8_t> & u8_alived_bitmap);

    /// Get the number of documents stored in the index file.
    UInt64 getIndexedDocsNum();

    /// Delete tantivy index files in `index_files_cache_path`.
    void removeTantivyIndexCache();
    static void removeTantivyIndexCache(DB::ContextPtr global_context_, std::string index_cache_part_path);

    /// If index_files_path is a data part path or table path, no need to delete parent path.
    /// If index_files_path is a tantivy index path, need to delete parent part path if it's empty.
    static void removeTantivyIndexCache(DiskPtr tmp_disk_, std::string index_files_path, bool need_delete_parent_path = false);

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

    mutable std::mutex mutex_of_delete_bitmap;

    void setDeleteBitmap(const TantivyDelBitmap & u8_delete_bitmap)
    {
        auto bitmap_ptr = std::make_shared<TantivyDelBitmap>(u8_delete_bitmap);
        std::atomic_store(&delete_bitmap, std::move(bitmap_ptr));
    }

    TantivyDelBitmapPtr getDeleteBitmap() { return std::atomic_load(&delete_bitmap); }

private:
    // The name of the created index.
    String name;
    DataPartStoragePtr storage;
    MutableDataPartStoragePtr data_part_storage_builder;
    TantivyIndexRowId tantivy_index_row_id;
    bool index_reader_status = false;

    std::mutex index_writer_mutex;
    bool index_writer_status = false;

    String index_meta_file_name;
    String index_data_file_name;
    std::unique_ptr<WriteBufferFromFileBase> meta_data_write_stream;
    std::unique_ptr<WriteBufferFromFileBase> index_data_write_stream;
    std::unique_ptr<ReadBufferFromFileBase> meta_data_read_stream;
    std::unique_ptr<ReadBufferFromFileBase> index_data_read_stream;
    String index_files_cache_path = "";
    DiskPtr temp_data_on_disk;
    TantivyIndexSettings index_settings;
    std::mutex deserialize_mutex;

    Poco::Logger * log;

    /// DeleteBitmap used for part with lightweight delete
    TantivyDelBitmapPtr delete_bitmap = nullptr;

    /// resource initialize
    void initStore();
    void initFileWriteStreams();
    void initFileReadStreams();
    bool freeTantivyIndex();
    bool commitTantivyIndex();

    bool getTantivyIndexReader();
    bool getTantivyIndexWriter();
};

using TantivyIndexStorePtr = std::shared_ptr<TantivyIndexStore>;
using TantivyIndexStores = std::unordered_map<std::string, TantivyIndexStorePtr>;

/// A singleton for storing TantivyIndexStores
class TantivyIndexStoreFactory : private boost::noncopyable
{
public:
    /// Get singleton of TantivyIndexStoreFactory
    static TantivyIndexStoreFactory & instance();

    /// Get TantivyIndexStore by using index name, disk and part_path (which are combined to create key in stores)
    TantivyIndexStorePtr get(const String & name, DataPartStoragePtr storage);

    /// Remove all Tantivy index files which are under the same part_path
    void remove(const String & part_path);

private:
    TantivyIndexStores stores;
    std::mutex mutex;
};

}
