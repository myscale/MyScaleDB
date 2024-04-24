#include <algorithm>
#include <iostream>
#include <numeric>
#include <unordered_map>
#include <vector>
#include <tantivy_search.h>
#include <Columns/ColumnString.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/TantivyIndexStore.h>
#include <Common/FST.h>
#include <Storages/MergeTree/MergeTreeIndices.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TANTIVY_INDEX_STORE_INTERNAL_ERROR;
    extern const int TANTIVY_INDEX_FILES_SERIALIZE_ERROR;
    extern const int TANTIVY_INDEX_FILES_DESERIALIZE_ERROR;
    extern const int TANTIVY_BUILD_INDEX_INTERNAL_ERROR;
    extern const int TANTIVY_SEARCH_INTERNAL_ERROR;
}


TantivyIndexStore::TantivyIndexStore(const String & name_, DataPartStoragePtr storage_)
    : name(name_), storage(storage_), log(&Poco::Logger::get("TantivyIndexStore"))
{
    initStore();
}

TantivyIndexStore::TantivyIndexStore(
    const String & name_, DataPartStoragePtr storage_, MutableDataPartStoragePtr data_part_storage_builder_)
    : name(name_), storage(storage_), data_part_storage_builder(data_part_storage_builder_), log(&Poco::Logger::get("TantivyIndexStore"))
{
    initStore();
}

void TantivyIndexStore::initStore()
{
    index_meta_file_name = name + TANTIVY_INDEX_OFFSET_FILE_TYPE;
    index_data_file_name = name + TANTIVY_INDEX_DATA_FILE_TYPE;

    auto global_context = Context::getGlobalContextInstance();

    if (global_context)
    {
        /**
         * global_context->getTantivyIndexCachePath():  /var/lib/clickhouse/tantivy_index_cache/
         * getStorePath(storage->getFullPath()):  store/5bc/5bc3950c-1073-4032-b79b-b5e1cac03d43/tmp_mut_all_1_1_0_2/  or store/5bc/5bc3950c-1073-4032-b79b-b5e1cac03d43/all_1_1_0_2/
         * index_directory_with_backslash:  skp_idx_xxxx_idx/
         * */
        fs::path cache_prefix = global_context->getTantivyIndexCachePath();
        index_files_cache_path = cache_prefix / storage->getRelativePath() / name / "";

        temp_data_on_disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, global_context->getPath(), 0);
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::TANTIVY_INDEX_STORE_INTERNAL_ERROR, "Can't get tantivy index files cache prefix.");
    }
}

TantivyIndexStore::~TantivyIndexStore()
{
}

bool TantivyIndexStore::exists() const
{
    return storage->exists(index_meta_file_name);
}

void TantivyIndexStore::removeTantivyIndexCache(DB::ContextPtr global_context_, std::string index_cache_part_path)
{
    auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, global_context_->getPath(), 0);
    removeTantivyIndexCache(disk, index_cache_part_path);
}

void TantivyIndexStore::removeTantivyIndexCache(DiskPtr tmp_disk_, std::string index_files_path, bool need_delete_parent_path)
{
    if (tmp_disk_ && tmp_disk_->isDirectory(index_files_path))
    {
        tmp_disk_->removeRecursive(index_files_path);
        LOG_DEBUG(&Poco::Logger::get("TantivyIndexStore"), "[removeTantivyIndexCache] directory [{}] has been removed", index_files_path);

        if (need_delete_parent_path)
        {
            /// There is tailing slash inside index_files_path
            String part_cache_path = fs::path(index_files_path).parent_path().parent_path();
            if (tmp_disk_->isDirectoryEmpty(part_cache_path))
            {
                /// Multiple tantivy indexes in a part share the same data part directory.
                tmp_disk_->removeDirectory(part_cache_path);
                LOG_DEBUG(&Poco::Logger::get("TantivyIndexStore"), "[removeTantivyIndexCache] parent data part directory [{}] has been removed", part_cache_path);
            }
        }
    }
}

void TantivyIndexStore::removeTantivyIndexCache()
{
    ffi_free_index_reader(index_files_cache_path);
    index_reader_status = false;
    removeTantivyIndexCache(temp_data_on_disk, index_files_cache_path, true /* need delete empty parent path */);
}

void TantivyIndexStore::initFileWriteStreams()
{
    if (!data_part_storage_builder)
    {
        throw DB::Exception(
            DB::ErrorCodes::TANTIVY_INDEX_STORE_INTERNAL_ERROR,
            "DataPartStorageBuilder can't be null_ptr while rewriting serialized index file!");
    }
    try
    {
        meta_data_write_stream
            = data_part_storage_builder->writeFile(index_meta_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
        index_data_write_stream
            = data_part_storage_builder->writeFile(index_data_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    }
    catch (...)
    {
        throw DB::Exception(
            DB::ErrorCodes::TANTIVY_INDEX_STORE_INTERNAL_ERROR,
            "Can't create write stream for target index file: [{}, {}]",
            index_meta_file_name,
            index_data_file_name);
    }
}

void TantivyIndexStore::initFileReadStreams()
{
    try
    {
        meta_data_read_stream = storage->readFile(index_meta_file_name, {}, std::nullopt, std::nullopt);
        index_data_read_stream = storage->readFile(index_data_file_name, {}, std::nullopt, std::nullopt);
    }
    catch (...)
    {
        throw DB::Exception(
            DB::ErrorCodes::TANTIVY_INDEX_STORE_INTERNAL_ERROR,
            "Can't create read stream for target index file: [{}, {}]",
            index_meta_file_name,
            index_data_file_name);
    }
}

UInt64 TantivyIndexStore::getNextRowId(size_t rows_read)
{
    UInt64 res = tantivy_index_row_id.next_row_id;
    tantivy_index_row_id.next_row_id += rows_read;
    return res;
}

ChecksumPairs TantivyIndexStore::serialize()
{
    if (!fs::is_directory(index_files_cache_path))
    {
        ChecksumPairs checksums;
        return checksums;
    }

    initFileWriteStreams();

    TantivyIndexFileMetas metas;
    UInt64 written_bytes = 0;
    /// index_data_write_stream -> data_hashing_uncompressed_stream
    auto data_hashing_uncompressed_stream = std::make_unique<HashingWriteBuffer>(*index_data_write_stream);
    /// meta_data_write_stream -> meta_hashing_uncompressed_stream
    auto meta_hashing_uncompressed_stream = std::make_unique<HashingWriteBuffer>(*meta_data_write_stream);

    LOG_DEBUG(
        log,
        "[serialize] Try serializing tantivy index files from index_cache:{} to data_part:{}.",
        index_files_cache_path,
        storage->getRelativePath());

    for (const auto & entry : fs::directory_iterator(index_files_cache_path))
    {
        if (fs::is_regular_file(entry))
        {
            std::unique_ptr<ReadBufferFromFileBase> temp_file_read_stream
                = temp_data_on_disk->readFile(entry.path(), {}, std::nullopt, std::nullopt);
            UInt64 file_size = static_cast<DB::UInt64>(temp_file_read_stream->getFileSize());
            copyData(*temp_file_read_stream, *data_hashing_uncompressed_stream);
            metas.emplace_back(entry.path().filename(), written_bytes, written_bytes + file_size);
            written_bytes += file_size;
            LOG_TRACE(
                log,
                "[serialize] tantivy index file: {}, file size: {}, written_bytes: {}",
                entry.path().filename(),
                file_size,
                written_bytes);
        }
        else
        {
            LOG_ERROR(
                log,
                "[serialize] Can't serialize tantivy index file [written_bytes:{} file_path:{}] from index_cache:{} to data_part: {}",
                written_bytes,
                entry.path(),
                index_files_cache_path,
                storage->getRelativePath());
            throw DB::Exception(
                DB::ErrorCodes::TANTIVY_INDEX_FILES_SERIALIZE_ERROR,
                "Can't serialize tantivy index file [written_bytes:{} file_path:{}] from index_cache:{} to data_part: {}",
                written_bytes,
                entry.path(),
                index_files_cache_path,
                storage->getRelativePath());
        }
    }
    data_hashing_uncompressed_stream->preFinalize();

    /// Stores metadata information
    size_t metas_size = metas.size();
    meta_hashing_uncompressed_stream->write(reinterpret_cast<const char *>(&metas_size), sizeof(size_t));
    meta_hashing_uncompressed_stream->write(reinterpret_cast<const char *>(metas.data()), metas_size * sizeof(TantivyIndexFileMeta));
    meta_hashing_uncompressed_stream->preFinalize();

    LOG_DEBUG(
        log,
        "[serialize] Finish serialized [{}] tantivy index files from index_cache:{} to data_part:{}, total bytes: {}",
        metas_size,
        index_files_cache_path,
        storage->getRelativePath(),
        written_bytes);

    ChecksumPairs checksums;
    checksums.emplace_back(
        index_data_file_name,
        DB::MergeTreeDataPartChecksums::Checksum(data_hashing_uncompressed_stream->count(), data_hashing_uncompressed_stream->getHash()));
    checksums.emplace_back(
        index_meta_file_name,
        DB::MergeTreeDataPartChecksums::Checksum(meta_hashing_uncompressed_stream->count(), meta_hashing_uncompressed_stream->getHash()));

    return checksums;
}

void TantivyIndexStore::deserialize()
{
    initFileReadStreams();

    size_t metas_size = 0;
    meta_data_read_stream->readStrict(reinterpret_cast<char *>(&metas_size), sizeof(size_t));
    TantivyIndexFileMetas metas(metas_size);
    meta_data_read_stream->readStrict(reinterpret_cast<char *>(metas.data()), metas_size * sizeof(TantivyIndexFileMeta));

    LOG_DEBUG(
        log,
        "[deserialize] Trying deserialize [{}] tantivy index files from data_part:{} to index_cache:{}",
        metas_size,
        storage->getRelativePath(),
        index_files_cache_path);

    if (!temp_data_on_disk->isDirectory(index_files_cache_path))
    {
        temp_data_on_disk->createDirectories(index_files_cache_path);
    }

    for (size_t i = 0; i < metas_size; i++)
    {
        try
        {
            UInt64 file_size = metas[i].offset_end - metas[i].offset_begin;
            index_data_read_stream->seek(metas[i].offset_begin, SEEK_SET);
            std::unique_ptr<WriteBufferFromFileBase> temp_data_write_stream = temp_data_on_disk->writeFile(
                index_files_cache_path + metas[i].file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
            char * buffer = new char[file_size];
            index_data_read_stream->readStrict(buffer, file_size);
            temp_data_write_stream->write(buffer, file_size);
            temp_data_write_stream->finalize();
            delete[] buffer;
            LOG_TRACE(
                log,
                "[deserialize] tantivy index file [file_idx:{}, file_name{}] from data_part:{} to index_cache:{}: ",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_cache_path);
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "[deserialize] Can't deserialize tantivy index file [file_idx:{}, file_name{}] from data_part:{} to index_cache:{}: ",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_cache_path);
            throw DB::Exception(
                DB::ErrorCodes::TANTIVY_INDEX_FILES_DESERIALIZE_ERROR,
                "Can't deserialize tantivy index file [file_idx:{}, file_name{}] from data_part:{} to index_cache:{}: ",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_cache_path);
        }
    }
    LOG_DEBUG(
        log,
        "[deserialize] Finish deserialized [{}] tantivy index files from data_part:{} to index_cache:{}",
        metas_size,
        storage->getRelativePath(),
        index_files_cache_path);
}

bool TantivyIndexStore::getTantivyIndexReader()
{
    if (!index_reader_status)
    {
        std::lock_guard<std::mutex> lock(deserialize_mutex);
        LOG_DEBUG(log, "[getTantivyIndexReader] initialize indexReader from index_cache:{}", index_files_cache_path);
        /// double checked lock
        if (!index_reader_status)
        {
            if (!temp_data_on_disk->isDirectory(index_files_cache_path))
            {
                LOG_DEBUG(log, "[getTantivyIndexReader] trying call deserialize");
                deserialize();
            }
            else
            {
                LOG_DEBUG(log, "[getTantivyIndexReader] index is already deserialized");
            }

            index_reader_status = ffi_load_index_reader(index_files_cache_path);
            if (!index_reader_status)
            {
                LOG_ERROR(log, "[getTantivyIndexReader] indexReader initialized failed.");
                throw DB::Exception(
                    DB::ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR,
                    "Can't get indexReader from index_cache: {}",
                    index_files_cache_path);
            }
        }
    }

    return index_reader_status;
}


bool TantivyIndexStore::getTantivyIndexWriter()
{
    bool writer_ready = getIndexWriterStatus();
    if (writer_ready)
        return writer_ready;

    LOG_DEBUG(log, "[getTantivyIndexWriter] initialize indexWriter with directory:{}", index_files_cache_path);

    writer_ready = ffi_create_index_with_parameter(
            index_files_cache_path, index_settings.indexed_columns, index_settings.index_json_parameter);

    if (!writer_ready)
    {
        LOG_ERROR(
            log,
            "[getTantivyIndexWriter] Error happend when create tantivy index under index_cache:{}",
            index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::TANTIVY_BUILD_INDEX_INTERNAL_ERROR,
            "Error happend when create tantivy index under index_cache:{}",
            index_files_cache_path);
    }

    setIndexWriterStatus(writer_ready);
    return writer_ready;
}


bool TantivyIndexStore::indexMultiColumnDoc(uint64_t row_id, std::vector<String> & column_names, std::vector<String> & docs)
{
    if (!getIndexWriterStatus())
        getTantivyIndexWriter();

    bool status = ffi_index_multi_column_docs(index_files_cache_path, row_id, column_names, docs);

    if (!status)
    {
        LOG_ERROR(
            log, "[indexMultiColumnDoc] Error happend when tantivy_search indexing doc under index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::TANTIVY_BUILD_INDEX_INTERNAL_ERROR,
            "Error happend when tantivy_search indexing doc under index_cache:{}",
            index_files_cache_path);
    }

    return true;
}

bool TantivyIndexStore::freeTantivyIndex()
{
    bool writer_freed = true;
    bool reader_freed = true;

    if (getIndexWriterStatus())
    {
        writer_freed = ffi_free_index_writer(index_files_cache_path);
        if (writer_freed)
        {
            setIndexWriterStatus(false);
        }
    }

    if (index_reader_status)
    {
        reader_freed = ffi_free_index_reader(index_files_cache_path);
        if (reader_freed)
        {
            index_reader_status = false;
        }
    }

    return writer_freed && reader_freed;
}

bool TantivyIndexStore::commitTantivyIndex()
{
    if (!getIndexWriterStatus())
    {
        // LOG_ERROR(log, "[commitTantivyIndex] Tantivy index writer hasn't been initialized, can't execute commit, index_cache:{}", index_files_cache_path);
        // throw DB::Exception(ErrorCodes::TANTIVY_INDEX_STORE_INTERNAL_ERROR, "Tantivy index writer hasn't been initialized, can't execute commit, index_cache:{}", index_files_cache_path);
        return true;
    }

    if (!ffi_index_writer_commit(index_files_cache_path))
    {
        LOG_ERROR(log, "[commitTantivyIndex] Error happened when committing tantivy index, index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::TANTIVY_BUILD_INDEX_INTERNAL_ERROR,
            "Error happened when committing tantivy index, index_cache:{}",
            index_files_cache_path);
    }

    return true;
}

bool TantivyIndexStore::finalizeTantivyIndex()
{
    commitTantivyIndex();
    freeTantivyIndex();
    return true;
}

bool TantivyIndexStore::singleTermQueryWithRowIdRange(String column_name, String term, UInt64 lrange, UInt64 rrange)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    bool result = ffi_query_term_with_range(index_files_cache_path, column_name, term, lrange, rrange);
    return result;
}

bool TantivyIndexStore::regexTermQueryWithRowIdRange(String column_name, String pattern, UInt64 lrange, UInt64 rrange)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    bool result = ffi_regex_term_with_range(index_files_cache_path, column_name, pattern, lrange, rrange);
    return result;
}

bool TantivyIndexStore::sentenceQueryWithRowIdRange(String column_name, String sentence, UInt64 lrange, UInt64 rrange)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    bool result = ffi_query_sentence_with_range(index_files_cache_path, column_name, sentence, lrange, rrange);
    return result;
}

bool TantivyIndexStore::termsQueryWithRowIdRange(String column_name, std::vector<String> terms, UInt64 lrange, UInt64 rrange)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    bool result = ffi_query_terms_with_range(index_files_cache_path, column_name, terms, lrange, rrange);
    return result;
}

rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::singleTermQueryBitmap(String column_name, String term)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_query_term_bitmap(index_files_cache_path, column_name, term);
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::sentenceQueryBitmap(String column_name, String sentence)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_query_sentence_bitmap(index_files_cache_path, column_name, sentence);
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::regexTermQueryBitmap(String column_name, String pattern)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_regex_term_bitmap(index_files_cache_path, column_name, pattern);
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::termsQueryBitmap(String column_name, std::vector<String> terms)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_query_terms_bitmap(index_files_cache_path, column_name, terms);
}

rust::cxxbridge1::Vec<RowIdWithScore> TantivyIndexStore::bm25Search(String sentence, Statistics & statistics, size_t topk)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    std::vector<uint8_t> u8_alived_bitmap;
    return ffi_bm25_search(index_files_cache_path, sentence, static_cast<uint32_t>(topk), u8_alived_bitmap, false, statistics);
}

rust::cxxbridge1::Vec<RowIdWithScore> TantivyIndexStore::bm25SearchWithFilter(
    String sentence, Statistics & statistics, size_t topk, const std::vector<uint8_t> & u8_alived_bitmap)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_bm25_search(index_files_cache_path, sentence, static_cast<uint32_t>(topk), u8_alived_bitmap, true, statistics);
}

rust::cxxbridge1::Vec<DocWithFreq> TantivyIndexStore::getDocFreq(String sentence)
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_doc_freq(index_files_cache_path, sentence);
}

UInt64 TantivyIndexStore::getTotalNumDocs()
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_total_num_docs(index_files_cache_path);
}

UInt64 TantivyIndexStore::getTotalNumTokens()
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_total_num_tokens(index_files_cache_path);
}

UInt64 TantivyIndexStore::getIndexedDocsNum()
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_indexed_doc_counts(index_files_cache_path);
}

TantivyIndexStoreFactory & TantivyIndexStoreFactory::instance()
{
    static TantivyIndexStoreFactory instance;
    return instance;
}

TantivyIndexStorePtr TantivyIndexStoreFactory::get(const String & name, DataPartStoragePtr storage)
{
    const String & part_path = storage->getRelativePath();
    String store_key = name + ":" + part_path;

    std::lock_guard lock(mutex);

    size_t stores_size = stores.size();
    auto it = stores.find(store_key);

    if (it == stores.end())
    {
        TantivyIndexStorePtr store = std::make_shared<TantivyIndexStore>(name, storage);
        if (!store->exists())
            return nullptr;

        /// TODO: add deserializer here like GinIndexStore
        stores[store_key] = store;

        LOG_INFO(
            &Poco::Logger::get("TantivyIndexStoreFactory"),
            "[get] Generate tantivy index store with key [{}], stores size increased from {} to {}",
            store_key,
            stores_size,
            stores.size());
        return store;
    }
    return it->second;
}

void TantivyIndexStoreFactory::remove(const String & part_path)
{
    auto * log = &Poco::Logger::get("TantivyIndexStoreFactory");

    /// In cases when restart happened, factory may not contain the store for this part, we need to clear cache.
    /// But for stores existing in factory, an non-static removeTantivyIndexCache() will be called.
    bool cache_removed = false;

    std::lock_guard lock(mutex);
    size_t stores_size = stores.size();
    for (auto it = stores.begin(); it != stores.end();)
    {
        if (it->first.find(part_path) != String::npos)
        {
            /// Remove tantivy files in index cache directory
            it->second->removeTantivyIndexCache();
            cache_removed = true;

            it = stores.erase(it);

            LOG_DEBUG(log,
            "[remove] Tantivy index store has been removed, stores size decreased from {} to {}",
            stores_size, stores.size());
        }
        else
            ++it; /// After erasure, point 'it' to the next key-value pair.
    }

    /// Try to clear cache when not found in store factory
    if (!cache_removed)
    {
        try
        {
            auto context = Context::getGlobalContextInstance();
            fs::path cache_prefix = context->getTantivyIndexCachePath();
            TantivyIndexStore::removeTantivyIndexCache(context, cache_prefix / part_path);
        }
        catch (Exception & e)
        {
            LOG_ERROR(log, "[remove] Error happend when remove tantivy index_cache path for part {}: {}", part_path, e.what());
        }
    }
}

}
