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
    extern const int TANTIVY_INDEX_FILE_MANAGEMENT_ERROR;
}


TantivyIndexFilesManager::TantivyIndexFilesManager(const String & skp_index_name_, const DataPartStoragePtr storage_)
    : skp_index_name(skp_index_name_), storage(storage_), log(&Poco::Logger::get("FTSIndexFilesManager"))
{
    this->index_meta_file_name = skp_index_name_ + TANTIVY_INDEX_OFFSET_FILE_TYPE;
    this->index_data_file_name = skp_index_name_ + TANTIVY_INDEX_DATA_FILE_TYPE;
    auto global_context = Context::getGlobalContextInstance();
    this->tmp_disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, global_context->getPath(), 0);
    this->initTantivyIndexCacheDirectory();
}

TantivyIndexFilesManager::TantivyIndexFilesManager(
    const String & skp_index_name_, const DataPartStoragePtr storage_, MutableDataPartStoragePtr storage_builder_)
    : skp_index_name(skp_index_name_), storage(storage_), storage_builder(storage_builder_), log(&Poco::Logger::get("FTSIndexFilesManager"))
{
    this->index_meta_file_name = skp_index_name_ + TANTIVY_INDEX_OFFSET_FILE_TYPE;
    this->index_data_file_name = skp_index_name_ + TANTIVY_INDEX_DATA_FILE_TYPE;
    auto global_context = Context::getGlobalContextInstance();
    this->tmp_disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, global_context->getPath(), 0);
    this->initTantivyIndexCacheDirectory();
}

bool TantivyIndexFilesManager::hasTantivyIndexInDataPart()
{
    return this->storage->exists(this->index_meta_file_name);
}


void TantivyIndexFilesManager::initTantivyIndexCacheDirectory()
{
    auto global_context = Context::getGlobalContextInstance();

    if (global_context)
    {
        // /var/lib/clickhouse/tantivy_index_cache/
        fs::path cache_prefix = global_context->getTantivyIndexCachePath();
        // store/20a/20add947-81e3-41d1-a429-0c4f43e711be/tmp_mut_all_1_1_1_2/
        fs::path storage_relative_path = storage->getRelativePath();
        // store/20a/20add947-81e3-41d1-a429-0c4f43e711be/
        fs::path storage_relative_parent_path = storage_relative_path.parent_path().parent_path();
        // tmp_mut_all_1_1_1_2 -> all_1_1_1_2
        // String part_name = storage->getPartDirectory();
        String part_name = storage->getPartDirectory();
        std::unique_lock<std::shared_mutex> lock(tantivy_index_cache_directory_mutex);
        this->tantivy_index_cache_directory = cache_prefix / storage_relative_parent_path / part_name / this->skp_index_name / "";
        LOG_DEBUG(this->log, "init FTS index cache directory: {}", this->tantivy_index_cache_directory);
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::TANTIVY_INDEX_FILE_MANAGEMENT_ERROR, "Can't init FTS index files cache directory.");
    }
}

String TantivyIndexFilesManager::getTantivyIndexCacheDirectory()
{
    std::shared_lock<std::shared_mutex> lock(tantivy_index_cache_directory_mutex);
    return this->tantivy_index_cache_directory;
}

String TantivyIndexFilesManager::updateCacheDataPartRelativeDirectory(const String & target_part_cache_path)
{
    auto data_part_path_in_cache = fs::path(this->tantivy_index_cache_directory).parent_path().parent_path();
    auto target_tantivy_index_cache_directory = fs::path(target_part_cache_path) / this->skp_index_name / "";

    if (this->tantivy_index_cache_directory != target_tantivy_index_cache_directory)
    {
        LOG_INFO(
            this->log,
            "update FTS index cache directory from `{}` to `{}`",
            this->tantivy_index_cache_directory,
            target_tantivy_index_cache_directory);

        // rename tantivy index path in `TantivyIndexFilesManager`.
        std::unique_lock<std::shared_mutex> lock(tantivy_index_cache_directory_mutex);
        this->tantivy_index_cache_directory = target_tantivy_index_cache_directory;

        // rename data part path in tantivy cache directory.
        if (fs::exists(data_part_path_in_cache))
        {
            fs::rename(data_part_path_in_cache, target_part_cache_path);
        }
        else
        {
            if (fs::exists(target_part_cache_path))
                LOG_INFO(this->log, "FTS cache directory `{}` has already been renamed.", data_part_path_in_cache);
            else
                LOG_WARNING(this->log, "can't find and rename FTS cache directory `{}`.", data_part_path_in_cache);
        }
    }
    return this->tantivy_index_cache_directory;
}

ChecksumPairs TantivyIndexFilesManager::serialize()
{
    String index_files_directory = this->getTantivyIndexCacheDirectory();

    if (!this->tmp_disk->isDirectory(index_files_directory))
    {
        LOG_WARNING(log, "[serialize] index_files_directory({}) is not a directory, may be an empty part.", index_files_directory);
        ChecksumPairs checksums;
        return checksums;
    }

    if (!this->storage_builder)
    {
        LOG_ERROR(log, "[serialize] storage_builder can't be null when serializing FTS index files.");
        throw DB::Exception(
            DB::ErrorCodes::TANTIVY_INDEX_FILE_MANAGEMENT_ERROR, "storage_builder can't be null when serializing FTS index files.");
    }

    std::unique_ptr<WriteBufferFromFileBase> meta_data_write_stream
        = storage_builder->writeFile(this->index_meta_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    std::unique_ptr<WriteBufferFromFileBase> index_data_write_stream
        = storage_builder->writeFile(this->index_data_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});

    TantivyIndexFileMetas metas;
    UInt64 written_bytes = 0;
    /// index_data_write_stream -> data_hashing_uncompressed_stream
    auto data_hashing_uncompressed_stream = std::make_unique<HashingWriteBuffer>(*index_data_write_stream);
    /// meta_data_write_stream -> meta_hashing_uncompressed_stream
    auto meta_hashing_uncompressed_stream = std::make_unique<HashingWriteBuffer>(*meta_data_write_stream);

    for (const auto & entry : fs::directory_iterator(index_files_directory))
    {
        if (fs::is_regular_file(entry))
        {
            std::unique_ptr<ReadBufferFromFileBase> temp_file_read_stream
                = this->tmp_disk->readFile(entry.path(), {}, std::nullopt, std::nullopt);
            UInt64 file_size = static_cast<DB::UInt64>(temp_file_read_stream->getFileSize());
            copyData(*temp_file_read_stream, *data_hashing_uncompressed_stream);
            metas.emplace_back(entry.path().filename(), written_bytes, written_bytes + file_size);
            written_bytes += file_size;
            LOG_TRACE(
                this->log,
                "[serialize] Serializing FTS index file [file_name:{}, file_size:{}, written_bytes:{}]",
                entry.path().filename(),
                file_size,
                written_bytes);
        }
        else
        {
            LOG_ERROR(
                log,
                "[serialize] Can't serialize FTS index file [file_path:{}, written_bytes:{}] from index_cache:{} to data_part:{}",
                entry.path(),
                written_bytes,
                index_files_directory,
                this->storage_builder->getRelativePath());
            throw DB::Exception(
                DB::ErrorCodes::TANTIVY_INDEX_FILES_SERIALIZE_ERROR,
                "Can't serialize FTS index file [file_path:{}, written_bytes:{}] from index_cache:{} to data_part:{}",
                entry.path(),
                written_bytes,
                index_files_directory,
                this->storage_builder->getRelativePath());
        }
    }

    /// Stores FTS index data information
    data_hashing_uncompressed_stream->finalize();

    /// Stores FTS index meta information
    size_t metas_size = metas.size();
    meta_hashing_uncompressed_stream->write(reinterpret_cast<const char *>(&metas_size), sizeof(size_t));
    meta_hashing_uncompressed_stream->write(reinterpret_cast<const char *>(metas.data()), metas_size * sizeof(TantivyIndexFileMeta));
    meta_hashing_uncompressed_stream->finalize();

    LOG_INFO(
        this->log,
        "[serialize] Serialization of FTS index files from the index_cache:`{}` to the data_part:`{}` is complete, total bytes: {}, total "
        "files: {}",
        index_files_directory,
        storage_builder->getRelativePath(),
        written_bytes,
        metas_size);

    ChecksumPairs checksums;
    // To prevent inconsistency issues with FTS index file checksums across multiple replicas, an empty checksum is generated here.
    checksums.emplace_back(index_data_file_name, DB::MergeTreeDataPartChecksums::Checksum());
    // DB::MergeTreeDataPartChecksums::Checksum(data_hashing_uncompressed_stream->count(), data_hashing_uncompressed_stream->getHash()));
    checksums.emplace_back(index_meta_file_name, DB::MergeTreeDataPartChecksums::Checksum());
    // DB::MergeTreeDataPartChecksums::Checksum(meta_hashing_uncompressed_stream->count(), meta_hashing_uncompressed_stream->getHash()));

    return checksums;
}

void TantivyIndexFilesManager::deserialize()
{
    String index_files_directory = this->getTantivyIndexCacheDirectory();

    // TODO Possible optimization plan:
    // In tantivy_search, check if the path is valid and if the index files can be loaded successfully.
    // If it is invalid, clear it and re-serialize it.
    if (this->tmp_disk->isDirectory(index_files_directory))
    {
        LOG_INFO(
            log,
            "[deserialize] directory `{}` not empty. Assuming the FTS index files have already been deserialized",
            index_files_directory);
        return;
    }
    else
    {
        this->tmp_disk->createDirectories(index_files_directory);
        if (!this->hasTantivyIndexInDataPart())
        {
            throw DB::Exception(
                DB::ErrorCodes::TANTIVY_INDEX_FILES_DESERIALIZE_ERROR,
                "Can't perform deserialization operation. The data part `{}` does not contain any index files, bad fts index file.",
                storage->getRelativePath());
        }
    }

    if (!this->storage)
    {
        LOG_ERROR(log, "[initIndexReadStreams] storage can't be null when deserializing FTS index meta/data file.");
        throw DB::Exception(
            DB::ErrorCodes::TANTIVY_INDEX_FILE_MANAGEMENT_ERROR, "storage can't be null when deserializing FTS index meta/data file.");
    }

    std::unique_ptr<ReadBufferFromFileBase> meta_data_read_stream
        = storage->readFile(this->index_meta_file_name, {}, std::nullopt, std::nullopt);
    std::unique_ptr<ReadBufferFromFileBase> index_data_read_stream
        = storage->readFile(this->index_data_file_name, {}, std::nullopt, std::nullopt);

    // read tantivy index meta file in data part.
    size_t metas_size = 0;
    meta_data_read_stream->readStrict(reinterpret_cast<char *>(&metas_size), sizeof(size_t));
    TantivyIndexFileMetas metas(metas_size);
    meta_data_read_stream->readStrict(reinterpret_cast<char *>(metas.data()), metas_size * sizeof(TantivyIndexFileMeta));

    LOG_INFO(
        this->log,
        "[deserialize] Deserializing FTS index files from data_part:{} to index_cache:{}, total files: {}.",
        storage->getRelativePath(),
        index_files_directory,
        metas_size);

    // read tantivy index data file in data part.
    for (size_t i = 0; i < metas_size; i++)
    {
        try
        {
            UInt64 file_size = metas[i].offset_end - metas[i].offset_begin;
            index_data_read_stream->seek(metas[i].offset_begin, SEEK_SET);
            std::unique_ptr<WriteBufferFromFileBase> temp_data_write_stream
                = this->tmp_disk->writeFile(index_files_directory + metas[i].file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
            char * buffer = new char[file_size];
            index_data_read_stream->readStrict(buffer, file_size);
            temp_data_write_stream->write(buffer, file_size);
            temp_data_write_stream->finalize();
            delete[] buffer;
            LOG_TRACE(
                log,
                "[deserialize] FTS index file [file_idx:{}, file_name{}] from data_part:{} to index_cache:{}",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_directory);
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "[deserialize] Can't deserialize FTS index file [file_idx:{}, file_name{}] from data_part:{} to index_cache:{}",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_directory);

            throw DB::Exception(
                DB::ErrorCodes::TANTIVY_INDEX_FILES_DESERIALIZE_ERROR,
                "Can't deserialize fts index file [file_idx:{}, file_name{}] from data_part:{} to index_cache:{}",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_directory);
        }
    }
}

void TantivyIndexFilesManager::removeTantivyIndexCacheDirectory()
{
    std::shared_lock<std::shared_mutex> lock(tantivy_index_cache_directory_mutex);
    TantivyIndexFilesManager::removeTantivyIndexInCache(this->tantivy_index_cache_directory);
}


std::optional<fs::path> TantivyIndexFilesManager::getDataPartFullPathInCache(const String & relative_data_part_in_cache)
{
    try
    {
        auto context = Context::getGlobalContextInstance();
        fs::path tantivy_index_cache_prefix = context->getTantivyIndexCachePath();
        auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, context->getPath(), 0);

        // example: /var/lib/clickhouse/tantivy_index_cache/store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2/
        auto data_part_full_cache_path = tantivy_index_cache_prefix / relative_data_part_in_cache / "";

        // example-1: fs::path("store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2/") distance is 5
        // example-2: fs::path("store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2") distance is 4
        constexpr int required_depth = 4; // Corrected depth
        if (std::distance(data_part_full_cache_path.begin(), data_part_full_cache_path.end()) < required_depth)
        {
            return std::nullopt;
        }

        fs::path store_path = data_part_full_cache_path;
        for (int i = 0; i < required_depth; ++i)
        {
            store_path = store_path.parent_path();
        }

        if (!store_path.has_filename() || (store_path.filename() != "store" && store_path.filename() != "data"))
        {
            return std::nullopt;
        }

        return data_part_full_cache_path;
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("FTSIndexFilesManager"),
            "[remove] Error happend when geting data part full path in cache, rel_data_part: `{}`, exception is {}",
            relative_data_part_in_cache,
            e.what());
        return std::nullopt;
    }
}

void removeDirectoryIfEmpty(const std::shared_ptr<DiskLocal> & disk, const fs::path & directory)
{
    if (disk->isDirectory(directory) && disk->isDirectoryEmpty(directory))
    {
        disk->removeRecursive(directory);
    }
}

void removeDirectoryDirectly(const std::shared_ptr<DiskLocal> & disk, const fs::path & directory)
{
    if (disk->isDirectory(directory))
    {
        disk->removeRecursive(directory);
    }
}

void TantivyIndexFilesManager::removeDataPartInCache(const String & relative_data_part_in_cache)
{
    try
    {
        std::optional<fs::path> res = getDataPartFullPathInCache(relative_data_part_in_cache);
        if (res.has_value())
        {
            fs::path data_part_full_path_in_cache = res.value();
            auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, Context::getGlobalContextInstance()->getPath(), 0);
            if (disk->isDirectory(data_part_full_path_in_cache))
            {
                LOG_INFO(&Poco::Logger::get("FTSIndexFilesManager"), "try remove data part in cache `{}`", data_part_full_path_in_cache);
            }

            // data_part_full_path_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
            removeDirectoryDirectly(disk, data_part_full_path_in_cache);

            // table_uuid_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
            auto table_uuid_directory_in_cache = data_part_full_path_in_cache.parent_path().parent_path();
            removeDirectoryIfEmpty(disk, table_uuid_directory_in_cache);

            // table_uuid_prefix_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0`
            auto table_uuid_prefix_directory_in_cache = table_uuid_directory_in_cache.parent_path();
            removeDirectoryIfEmpty(disk, table_uuid_prefix_directory_in_cache);
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("FTSIndexFilesManager"),
            "[remove] Error happend when removing data part in cache, rel_data_part: `{}`, exception is {}",
            relative_data_part_in_cache,
            e.what());
    }
}

void TantivyIndexFilesManager::removeTantivyIndexInCache(const String & relative_data_part_in_cache, const String & skp_index_name)
{
    try
    {
        std::optional<fs::path> res = getDataPartFullPathInCache(relative_data_part_in_cache);
        if (res.has_value())
        {
            fs::path data_part_full_path_in_cache = res.value();
            fs::path index_full_path_in_cache = data_part_full_path_in_cache / skp_index_name / "";
            auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, Context::getGlobalContextInstance()->getPath(), 0);
            if (disk->isDirectory(index_full_path_in_cache))
            {
                LOG_INFO(&Poco::Logger::get("FTSIndexFilesManager"), "try remove FTS index in cache `{}`", index_full_path_in_cache);
            }

            // index_full_path_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/skp_idx_test_idx/`
            removeDirectoryDirectly(disk, index_full_path_in_cache);

            // data_part_full_path_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
            removeDirectoryIfEmpty(disk, data_part_full_path_in_cache);

            // table_uuid_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
            auto table_uuid_directory_in_cache = data_part_full_path_in_cache.parent_path().parent_path();
            removeDirectoryIfEmpty(disk, table_uuid_directory_in_cache);

            // table_uuid_prefix_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0`
            auto table_uuid_prefix_directory_in_cache = table_uuid_directory_in_cache.parent_path();
            removeDirectoryIfEmpty(disk, table_uuid_prefix_directory_in_cache);
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("FTSIndexFilesManager"),
            "[remove] Error happend when removing FTS index in cache, rel_data_part: `{}`, skp_idx_name: `{}`, exception is {}",
            relative_data_part_in_cache,
            skp_index_name,
            e.what());
    }
}

void TantivyIndexFilesManager::removeTantivyIndexInCache(const String & tantivy_index_cache_directory)
{
    try
    {
        // /var/lib/clickhouse/tantivy_index_cache/store/1e5/1e5452bd-c37a-4da4-a285-f9c591cf05ac/all_9959_9959_0_9969/skp_idx_fts_dqi/
        fs::path tantivy_index_cache_full_directory = fs::path(tantivy_index_cache_directory);
        constexpr int required_depth = 5; // Corrected depth

        // varify index cache path.
        fs::path store_path = tantivy_index_cache_full_directory;
        for (int i = 0; i < required_depth; ++i)
        {
            if (!store_path.has_parent_path())
            {
                LOG_INFO(
                    &Poco::Logger::get("FTSIndexFilesManager"),
                    "Can't remove fts index cache directory `{}`",
                    tantivy_index_cache_full_directory);
                return;
            }
            store_path = store_path.parent_path();
        }
        if (!store_path.has_filename() || (store_path.filename() != "store" && store_path.filename() != "data"))
        {
            LOG_INFO(
                &Poco::Logger::get("FTSIndexFilesManager"),
                "Can't remove fts index cache directory `{}`",
                tantivy_index_cache_full_directory);
            return;
        }


        auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, Context::getGlobalContextInstance()->getPath(), 0);
        if (disk->isDirectory(tantivy_index_cache_full_directory))
        {
            LOG_INFO(
                &Poco::Logger::get("FTSIndexFilesManager"),
                "try remove FTS index directory inner `Store`, path: `{}`",
                tantivy_index_cache_full_directory);
        }

        // index_full_path_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/skp_idx_test_idx/`
        removeDirectoryDirectly(disk, tantivy_index_cache_full_directory);

        // data_part_full_path_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
        auto data_part_full_path_in_cache = tantivy_index_cache_full_directory.parent_path().parent_path();
        removeDirectoryIfEmpty(disk, data_part_full_path_in_cache);

        // table_uuid_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
        auto table_uuid_directory_in_cache = data_part_full_path_in_cache.parent_path();
        removeDirectoryIfEmpty(disk, table_uuid_directory_in_cache);

        // table_uuid_prefix_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0`
        auto table_uuid_prefix_directory_in_cache = table_uuid_directory_in_cache.parent_path();
        removeDirectoryIfEmpty(disk, table_uuid_prefix_directory_in_cache);
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("FTSIndexFilesManager"),
            "[remove] Error happend when removing FTS index in cache `{}`, exception is {}",
            tantivy_index_cache_directory,
            e.what());
    }
}

void TantivyIndexFilesManager::removeEmptyTableUUIDInCache(const String & relative_data_part_in_cache)
{
    try
    {
        std::optional<fs::path> res = getDataPartFullPathInCache(relative_data_part_in_cache);
        if (res.has_value())
        {
            fs::path data_part_full_path_in_cache = res.value();
            auto disk = std::make_shared<DiskLocal>(TANTIVY_TEMP_DISK_NAME, Context::getGlobalContextInstance()->getPath(), 0);

            // table_uuid_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
            auto table_uuid_directory_in_cache = data_part_full_path_in_cache.parent_path().parent_path();
            removeDirectoryIfEmpty(disk, table_uuid_directory_in_cache);

            // table_uuid_prefix_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0`
            auto table_uuid_prefix_directory_in_cache = table_uuid_directory_in_cache.parent_path();
            removeDirectoryIfEmpty(disk, table_uuid_prefix_directory_in_cache);
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("FTSIndexFilesManager"),
            "[remove] Error happend when removing empty table UUID cache, rel_data_part: `{}`, exception is {}",
            relative_data_part_in_cache,
            e.what());
    }
}


TantivyIndexStore::TantivyIndexStore(const String & skp_index_name_, const DataPartStoragePtr storage_)
    : skp_index_name(skp_index_name_)
    , storage(storage_)
    , index_files_manager(std::make_unique<TantivyIndexFilesManager>(skp_index_name_, storage_))
    , log(&Poco::Logger::get("FTSIndexStore"))
{
}

TantivyIndexStore::TantivyIndexStore(
    const String & skp_index_name_, const DataPartStoragePtr storage_, MutableDataPartStoragePtr storage_builder_)
    : skp_index_name(skp_index_name_)
    , storage(storage_)
    , storage_builder(storage_builder_)
    , index_files_manager(std::make_unique<TantivyIndexFilesManager>(skp_index_name_, storage_, storage_builder_))
    , log(&Poco::Logger::get("FTSIndexStore"))
{
}


TantivyIndexStore::~TantivyIndexStore()
{
    LOG_INFO(
        log, "[~] trigger TantivyIndexStore destroy, FTS index path is `{}`", this->index_files_manager->getTantivyIndexCacheDirectory());
    this->removeTantivyIndexCache();
}


void TantivyIndexStore::removeTantivyIndexCache()
{
    this->freeTantivyIndex();
    this->index_files_manager->removeTantivyIndexCacheDirectory();
}


ChecksumPairs TantivyIndexStore::serialize()
{
    return this->index_files_manager->serialize();
}

String TantivyIndexStore::updateCacheDataPartRelativeDirectory(const String & target_part_cache_path)
{
    if (this->index_reader_status)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Can't update FTS store cache path from `{}` to `{}` while index reader is true",
            this->index_files_manager->getTantivyIndexCacheDirectory(),
            target_part_cache_path);
    }

    // Should free tantivy index reader first, avoid unnecessary mistakes.
    this->freeTantivyIndexReader();
    return this->index_files_manager->updateCacheDataPartRelativeDirectory(target_part_cache_path);
}

UInt64 TantivyIndexStore::getNextRowId(size_t rows_read)
{
    UInt64 res = tantivy_index_row_id.next_row_id;
    tantivy_index_row_id.next_row_id += rows_read;
    return res;
}

bool TantivyIndexStore::getTantivyIndexReader()
{
    String index_files_cache_path = this->index_files_manager->getTantivyIndexCacheDirectory();
    if (!index_reader_status)
    {
        std::lock_guard<std::mutex> lock(index_reader_mutex);
        LOG_INFO(log, "[getTantivyIndexReader] initializing FTS index reader, FTS index cache directory is {}", index_files_cache_path);
        /// double checked lock
        if (!index_reader_status)
        {
            this->index_files_manager->deserialize();
            index_reader_status = ffi_load_index_reader(index_files_cache_path);
            if (!index_reader_status)
            {
                LOG_ERROR(log, "[getTantivyIndexReader] Failed to initialize FTS index reader.");
                throw DB::Exception(
                    DB::ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR,
                    "Can't get FTS index reader from index_cache: {}",
                    index_files_cache_path);
            }
        }
    }

    return index_reader_status;
}


bool TantivyIndexStore::getTantivyIndexWriter()
{
    String index_files_cache_path = this->index_files_manager->getTantivyIndexCacheDirectory();
    bool writer_ready = getIndexWriterStatus();
    if (writer_ready)
        return writer_ready;

    LOG_INFO(log, "[getTantivyIndexWriter] initializing FTS index writer, FTS index cache directory is {}", index_files_cache_path);
    writer_ready = ffi_create_index_with_parameter(
            index_files_cache_path, index_settings.indexed_columns, index_settings.index_json_parameter);

    if (!writer_ready)
    {
        LOG_ERROR(log, "[getTantivyIndexWriter] Error happend when create FTS index under index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::TANTIVY_BUILD_INDEX_INTERNAL_ERROR,
            "Error happend when create FTS index under index_cache:{}",
            index_files_cache_path);
    }

    setIndexWriterStatus(writer_ready);
    return writer_ready;
}


bool TantivyIndexStore::indexMultiColumnDoc(uint64_t row_id, std::vector<String> & column_names, std::vector<String> & docs)
{
    if (!getIndexWriterStatus())
        getTantivyIndexWriter();
    String index_files_cache_path = this->index_files_manager->getTantivyIndexCacheDirectory();

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

bool TantivyIndexStore::freeTantivyIndexReader()
{
    std::lock_guard<std::mutex> lock(index_reader_mutex);
    bool reader_freed = true;
    String index_files_cache_path = this->index_files_manager->getTantivyIndexCacheDirectory();

    if (this->index_reader_status)
    {
        reader_freed = ffi_free_index_reader(index_files_cache_path);
        if (reader_freed)
        {
            this->index_reader_status = false;
        }
    }

    return reader_freed;
}

bool TantivyIndexStore::freeTantivyIndexWriter()
{
    bool writer_freed = true;

    String index_files_cache_path = this->index_files_manager->getTantivyIndexCacheDirectory();

    if (getIndexWriterStatus())
    {
        writer_freed = ffi_free_index_writer(index_files_cache_path);
        if (writer_freed)
        {
            setIndexWriterStatus(false);
        }
    }
    return writer_freed;
}

bool TantivyIndexStore::freeTantivyIndex()
{
    bool writer_freed = freeTantivyIndexWriter();
    bool reader_freed = freeTantivyIndexReader();
    return writer_freed && reader_freed;
}

void TantivyIndexStore::commitTantivyIndex()
{
    String index_files_cache_path = this->index_files_manager->getTantivyIndexCacheDirectory();

    if (!getIndexWriterStatus())
    {
        getTantivyIndexWriter();
        LOG_WARNING(
            log, "[commitTantivyIndex] data part may be empty, initialize FTS index writer, index_cache_path({})", index_files_cache_path);
    }

    if (!ffi_index_writer_commit(index_files_cache_path))
    {
        LOG_ERROR(log, "[commitTantivyIndex] Error happened when committing FTS index, index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::TANTIVY_BUILD_INDEX_INTERNAL_ERROR,
            "Error happened when committing FTS index, index_cache:{}",
            index_files_cache_path);
    }
}

bool TantivyIndexStore::finalizeTantivyIndex()
{
    commitTantivyIndex();
    freeTantivyIndex();
    return true;
}

rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::singleTermQueryBitmap(String column_name, String term)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_query_term_bitmap(this->index_files_manager->getTantivyIndexCacheDirectory(), column_name, term);
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::sentenceQueryBitmap(String column_name, String sentence)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_query_sentence_bitmap(this->index_files_manager->getTantivyIndexCacheDirectory(), column_name, sentence);
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::regexTermQueryBitmap(String column_name, String pattern)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_regex_term_bitmap(this->index_files_manager->getTantivyIndexCacheDirectory(), column_name, pattern);
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::termsQueryBitmap(String column_name, std::vector<String> terms)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_query_terms_bitmap(this->index_files_manager->getTantivyIndexCacheDirectory(), column_name, terms);
}

rust::cxxbridge1::Vec<RowIdWithScore> TantivyIndexStore::bm25Search(String sentence, bool enable_nlq, bool operator_or, Statistics & statistics, size_t topk)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    std::vector<uint8_t> u8_alived_bitmap;
    return ffi_bm25_search(
        this->index_files_manager->getTantivyIndexCacheDirectory(),
        sentence,
        static_cast<uint32_t>(topk),
        u8_alived_bitmap,
        false,
        enable_nlq,
        operator_or,
        statistics);
}

rust::cxxbridge1::Vec<RowIdWithScore> TantivyIndexStore::bm25SearchWithFilter(
    String sentence, bool enable_nlq, bool operator_or, Statistics & statistics, size_t topk, const std::vector<uint8_t> & u8_alived_bitmap)
{
    if (!index_reader_status)
        getTantivyIndexReader();

    return ffi_bm25_search(
        this->index_files_manager->getTantivyIndexCacheDirectory(),
        sentence,
        static_cast<uint32_t>(topk),
        u8_alived_bitmap,
        true,
        enable_nlq,
        operator_or,
        statistics);
}

rust::cxxbridge1::Vec<DocWithFreq> TantivyIndexStore::getDocFreq(String sentence)
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_doc_freq(this->index_files_manager->getTantivyIndexCacheDirectory(), sentence);
}

UInt64 TantivyIndexStore::getTotalNumDocs()
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_total_num_docs(this->index_files_manager->getTantivyIndexCacheDirectory());
}

UInt64 TantivyIndexStore::getTotalNumTokens()
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_total_num_tokens(this->index_files_manager->getTantivyIndexCacheDirectory());
}

UInt64 TantivyIndexStore::getIndexedDocsNum()
{
    if (!index_reader_status)
        getTantivyIndexReader();
    return ffi_get_indexed_doc_counts(this->index_files_manager->getTantivyIndexCacheDirectory());
}
}
