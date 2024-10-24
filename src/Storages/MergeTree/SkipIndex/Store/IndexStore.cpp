#include <Storages/MergeTree/SkipIndex/Store/IndexStore.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SKIP_INDEX_BUILD_INTERNAL_ERROR;
    extern const int SKIP_INDEX_SEARCH_INTERNAL_ERROR;
}


IndexStore::IndexStore(
    const SkipIndexType index_type_,
    const String & index_name_,
    const String & index_meta_file_suffix,
    const String & index_data_file_suffix,
    const DataPartStoragePtr storage_,
    const MutableDataPartStoragePtr storage_builder_)
    : index_type(index_type_)
    , index_name(index_name_)
    , storage(storage_)
    , storage_builder(storage_builder_)
    , index_files_manager(std::make_unique<IndexFilesManager>(
          index_type_, index_meta_file_suffix, index_data_file_suffix, index_name_, storage_, storage_builder_))
    , log(&Poco::Logger::get(toSkipIndexName(index_type_) + "IndexStore"))
{
}

IndexStore::~IndexStore()
{
    LOG_INFO(log, "[~] trigger IndexStore destroy, remove index path: `{}`", this->index_files_manager->getFullIndexPathInCache());
    this->index_files_manager->removeFullIndexPathInCacheForward();
}

String IndexStore::updateFullIndexPathInCache(const String & new_part_path_in_cache)
{
    // This function is expected to be called only once after the part build index is completed.
    if (this->index_reader_status)
    {
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't update index cache path: `{}`, new part full path: `{}`, cause `index_reader_status` is true",
            this->index_files_manager->getFullIndexPathInCache(),
            new_part_path_in_cache);
    }

    // This is a redundant free operation; it's better not to remove it.
    this->freeIndexReader();
    return this->index_files_manager->updateFullIndexPathInCache(new_part_path_in_cache);
}

String IndexStore::getFullIndexPathInCache()
{
    return this->index_files_manager->getFullIndexPathInCache();
}

UInt64 IndexStore::getNextRowId(size_t rows_read)
{
    UInt64 res = this->index_row_id.next_row_id;
    this->index_row_id.next_row_id += rows_read;
    return res;
}

ChecksumPairs IndexStore::serializeIndex()
{
    return this->index_files_manager->serialize();
}

bool IndexStore::commitIndex()
{
    if (!getIndexWriterStatus())
    {
        String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();
        this->loadIndexWriter();
        LOG_WARNING(
            log, "[commitIndex] data part may be empty, needs initialize index writer, full_index_path: `{}`", index_files_cache_path);
    }

    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();
    auto commit_status = this->commitIndexImpl(index_files_cache_path);
    if (!commit_status.first || commit_status.second.first)
    {
        LOG_ERROR(log, "[commitIndex] Error happened when committing index, full_index_path: `{}`", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::SKIP_INDEX_BUILD_INTERNAL_ERROR,
            "Error happened when committing index, full_index_path: {}",
            index_files_cache_path);
    }

    return true;
}


bool IndexStore::finalizeIndex()
{
    bool committed_status = this->commitIndex();
    bool freed_status = this->freeIndexReaderAndWriter();
    return committed_status && freed_status;
}

bool IndexStore::freeIndexReaderAndWriter()
{
    bool writer_freed = this->freeIndexWriter();
    bool reader_freed = this->freeIndexReader();
    return writer_freed && reader_freed;
}

bool IndexStore::freeIndexReader()
{
    std::lock_guard<std::mutex> lock(this->index_reader_mutex);
    bool reader_freed = true;
    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();

    if (this->index_reader_status)
    {
        auto freed_status = this->freeIndexReaderImpl(index_files_cache_path);

        if (freed_status.second.first)
        {
            throw DB::Exception(ErrorCodes::SKIP_INDEX_SEARCH_INTERNAL_ERROR, "{}", freed_status.second.second);
        }
        reader_freed = freed_status.first;
        if (reader_freed)
        {
            this->index_reader_status = false;
        }
    }

    return reader_freed;
}

bool IndexStore::freeIndexWriter()
{
    bool writer_freed = true;

    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();

    if (getIndexWriterStatus())
    {
        auto freed_status = this->freeIndexWriterImpl(index_files_cache_path);
        if (freed_status.second.first)
        {
            throw DB::Exception(ErrorCodes::SKIP_INDEX_BUILD_INTERNAL_ERROR, "{}", freed_status.second.second);
        }
        writer_freed = freed_status.first;
        if (writer_freed)
        {
            setIndexWriterStatus(false);
        }
    }
    return writer_freed;
}


bool IndexStore::loadIndexReader()
{
    DB::OpenTelemetry::SpanHolder span("IndexStore::load_index_reader");
    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();
    if (!this->index_reader_status)
    {
        std::lock_guard<std::mutex> lock(this->index_reader_mutex);
        /// double checked lock
        if (!this->index_reader_status)
        {
            LOG_INFO(log, "[loadIndexReader] initializing index reader, full_index_cache: `{}`", index_files_cache_path);
            this->index_files_manager->deserialize();
            auto load_status = this->loadIndexReaderImpl(index_files_cache_path);
            size_t retry_times = 0;
            const int base_wait_slots = 50;
            while (load_status.second.first && retry_times < DESERIALIZE_MAX_RETRY_TIMES)
            {
                int wait_slots = base_wait_slots * (1 << retry_times);
                std::this_thread::sleep_for(std::chrono::milliseconds(wait_slots));
                LOG_ERROR(
                    log, "[loadIndexReader] Failed to load index reader, {}, retry {} times.", load_status.second.second, retry_times);
                this->index_files_manager->removeFullIndexPathInCacheForward();
                this->index_files_manager->deserialize();
                load_status = this->loadIndexReaderImpl(index_files_cache_path);
                retry_times += 1;
            }
            if (load_status.second.first)
            {
                throw DB::Exception(ErrorCodes::SKIP_INDEX_SEARCH_INTERNAL_ERROR, "{}", load_status.second.second);
            }

            this->index_reader_status = load_status.first;
            if (!this->index_reader_status)
            {
                LOG_ERROR(log, "[loadIndexReader] Failed to initialize index reader. full_index_path: `{}`", index_files_cache_path);
                throw DB::Exception(
                    DB::ErrorCodes::SKIP_INDEX_SEARCH_INTERNAL_ERROR,
                    "Failed to initialize index reader. full_index_path: `{}`",
                    index_files_cache_path);
            }
        }
    }

    return index_reader_status;
}


bool IndexStore::loadIndexWriter()
{
    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();
    bool writer_ready = getIndexWriterStatus();
    if (writer_ready)
        return writer_ready;

    LOG_INFO(log, "[loadIndexWriter] initializing index_writer, full_index_path: `{}`", index_files_cache_path);
    auto load_status = this->loadIndexWriterImpl(index_files_cache_path);

    if (load_status.second.first)
    {
        throw DB::Exception(ErrorCodes::SKIP_INDEX_BUILD_INTERNAL_ERROR, "{}", std::string(load_status.second.second));
    }

    writer_ready = load_status.first;

    if (!writer_ready)
    {
        LOG_ERROR(log, "[loadIndexWriter] Error happend when create index under index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::SKIP_INDEX_BUILD_INTERNAL_ERROR,
            "Error happend when create index_writer under full_index_path:`{}`",
            index_files_cache_path);
    }

    setIndexWriterStatus(writer_ready);
    return writer_ready;
}


}
