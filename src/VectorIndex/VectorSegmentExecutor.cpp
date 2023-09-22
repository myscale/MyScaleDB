#include <VectorIndex/VectorSegmentExecutor.h>

#include <thread>
#include <sys/resource.h>
#include <omp.h>

#include <boost/algorithm/string/split.hpp>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/MemoryStatisticsOS.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/VectorIndexEventLog.h>
#include <VectorIndex/BruteForceSearch.h>
#include <VectorIndex/CacheManager.h>
#include <VectorIndex/IndexException.h>
#include <VectorIndex/MergeUtils.h>
#include <VectorIndex/Metadata.h>
#include <VectorIndex/VectorIndexIO.h>
#include <SearchIndex/Common/Utils.h>
#include <SearchIndex/IndexDataFileIO.h>
#include <Storages/IStorage.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int STD_EXCEPTION;
        extern const int CORRUPTED_DATA;
        extern const int LOGICAL_ERROR;
        extern const int CANNOT_OPEN_FILE;
        extern const int INVALID_VECTOR_INDEX;
    }
    using StoragePtr = std::shared_ptr<IStorage>;
}

namespace fs = std::filesystem;

namespace VectorIndex
{

void printMemoryInfo(const Poco::Logger * log, std::string msg)
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    DB::MemoryStatisticsOS memory_stat;
    DB::MemoryStatisticsOS::Data data = memory_stat.get();
    LOG_INFO(
        log,
        "{}: peak resident memory {} MB, resident memory {} MB, virtual memory {} MB",
        msg,
        usage.ru_maxrss / 1024,
        data.resident / 1024 / 1024,
        data.virt / 1024 / 1024);
#endif
}

class SearchThreadLimiter
{
public:
    SearchThreadLimiter(const Poco::Logger * log, int max_threads)
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        cv.wait(lock, [&] { return count.load() < max_threads; });
        count.fetch_add(1);
        LOG_DEBUG(log, "Index search uses {}/{} threads", count.load(), max_threads);
    }

    ~SearchThreadLimiter()
    {
        count.fetch_sub(1);
        cv.notify_one();
    }
private:
    static std::shared_mutex mutex;
    static std::condition_variable_any cv;
    static std::atomic_int count;
};

std::shared_mutex SearchThreadLimiter::mutex;
std::condition_variable_any SearchThreadLimiter::cv;
std::atomic_int SearchThreadLimiter::count(0);

String cutMutVer(const String & part_name)
{
    std::vector<String> tokens;
    boost::split(tokens, part_name, boost::is_any_of("_"));
    if (tokens.size() <= 4) /// without mutation version
    {
        return part_name;
    }
    else
        return tokens[0] + "_" + tokens[1] + "_" + tokens[2] + "_" + tokens[3];
}

std::once_flag VectorSegmentExecutor::once;
int VectorSegmentExecutor::max_threads = 0;
String cutPartitionID(const String & part_name)
{
    std::vector<String> tokens;
    boost::split(tokens, part_name, boost::is_any_of("_"));
    return tokens[0];
}

String cutTableUUIDFromCacheKey(const String & cache_key)
{
    std::vector<String> tokens;
    boost::split(tokens, cache_key, boost::is_any_of("/"));
    return tokens[tokens.size() - 3];
}

String cutPartNameFromCacheKey(const String & cache_key)
{
    std::vector<String> tokens;
    boost::split(tokens, cache_key, boost::is_any_of("/"));
    return tokens[tokens.size() - 2];
}

void VectorSegmentExecutor::init()
{
    std::call_once(
        once,
        [&]
        {
            max_threads = getNumberOfPhysicalCPUCores() * 2;
            LOG_INFO(log, "Max threads for vector index: {}", max_threads);
        });
}

VectorSegmentExecutor::VectorSegmentExecutor(
    const SegmentId & segment_id_,
    Search::IndexType type_,
    Search::Metric metric_,
    size_t dimension_,
    size_t total_vec_,
    Search::Parameters des_,
    size_t min_bytes_to_build_vector_index_,
    int DEFAULT_DISK_MODE_)
    : DEFAULT_DISK_MODE(DEFAULT_DISK_MODE_)
    , segment_id(segment_id_)
    , type(type_)
    , metric(metric_)
    , dimension(dimension_)
    , total_vec(total_vec_)
    , des(des_)
    , min_bytes_to_build_vector_index(min_bytes_to_build_vector_index_)
{
    init();
}

VectorSegmentExecutor::VectorSegmentExecutor(const SegmentId & segment_id_) : DEFAULT_DISK_MODE(0), segment_id(segment_id_)
{
    init();
}

void VectorSegmentExecutor::buildIndex(PartReader * reader, const std::function<bool()> & check_build_canceled_callbak, bool slow_mode, size_t train_block_size, size_t add_block_size)
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::buildIndex");

    auto num_threads = max_threads;
    if (slow_mode)
        num_threads = num_threads / 2;
    if (num_threads == 0)
        num_threads = 1;

    if (total_vec * dimension * sizeof(float) < min_bytes_to_build_vector_index)
    {
        fallback_to_flat = true;
        type = Search::IndexType::FLAT;
        std::erase_if(
            des,
            [](const auto & item)
            {
                auto const & [key, value] = item;
                return key != "metric_type";
            });
    }

    try
    {
        configureDiskMode();
        index = Search::
            createVectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::FloatVector>(
                segment_id.getIndexNameWithColumn(),
                type,
                metric,
                dimension,
                total_vec,
                des,
                false /* load_diskann_after_build */,
                vector_index_cache_prefix,
#ifdef ENABLE_SCANN
                getDiskIOManager(),
#endif
                true /* use_file_checksum */,
                true /* manage_cache_folder */);
        index->setTrainDataChunkSize(train_block_size);
        index->setAddDataChunkSize(add_block_size);
        printMemoryInfo(log, "Before build");
        index->build(reader, num_threads, check_build_canceled_callbak);
        printMemoryInfo(log, "After build");
    }
    catch (const SearchIndexException & e)
    {
        LOG_WARNING(log, "Failed to build index for {}: {}", segment_id.current_part_name, e.what());
        throw IndexException(e.getCode(), e.what());
    }
    catch (const DB::Exception & e)
    {
        throw e;
    }

    delete_bitmap = std::make_shared<Search::DenseBitmap>(total_vec, true);
}

void VectorSegmentExecutor::updateCacheValueWithRowIdsMaps(const IndexWithMetaHolderPtr index_holder)
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::updateCacheValueWithRowIdsMaps");
    try
    {
        handleMergedMaps();
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, "Failed to load inverted row ids map entries, error: {}", e.what());
        throw;
    }

    if (inverted_row_sources_map->empty())
    {
        return;
    }
    if (index_holder)
    {
        IndexWithMeta & index_with_meta = index_holder->value();
        auto update_row_id_maps_lock = index_with_meta.tryLockIndexForUpdateRowIdsMaps();
        if (update_row_id_maps_lock.owns_lock() && index_with_meta.inverted_row_sources_map->empty())
        {
            LOG_DEBUG(log, "Update row id maps, cache key = {}", segment_id.getCacheKey().toString());
            index_with_meta.fallback_to_flat = fallback_to_flat;
            index_with_meta.inverted_row_ids_map = this->inverted_row_ids_map;
            index_with_meta.inverted_row_sources_map = this->inverted_row_sources_map;
            index_with_meta.row_ids_map = this->row_ids_map;
        }
        else
        {
            LOG_DEBUG(log, "Other thread is updating row id maps, no need to update, cache key = {}", segment_id.getCacheKey().toString());
        }
    }
    /// not handle empty cache case here.
}


Status VectorSegmentExecutor::cache()
{
    CacheManager * mgr = CacheManager::getInstance();
    if (index == nullptr)
    {
        LOG_INFO(log, "{} index is null, not caching", segment_id.getCacheKey().toString());
        return Status(3);
    }
    /// when cacheIndexAndMeta() is called, related files should have already been loaded.
    IndexWithMetaPtr cache_item = std::make_shared<IndexWithMeta>(
        index,
        total_vec,
        delete_bitmap,
        des,
        row_ids_map,
        inverted_row_ids_map,
        inverted_row_sources_map,
        disk_mode,
        fallback_to_flat,
        vector_index_cache_prefix);

    LOG_DEBUG(log, "Cache key: {}", segment_id.getCacheKey().toString());
    mgr->put(segment_id.getCacheKey(), cache_item);
    LOG_DEBUG(log, "Num of item after cache {}", mgr->countItem());
    return Status();
}

Status VectorSegmentExecutor::serialize()
{
    try
    {
        auto file_writer = Search::IndexDataFileWriter<Search::AbstractOStream>(
            segment_id.getFullPath(),
            [this](const std::string & name, std::ios::openmode /*mode*/)
            { return std::make_shared<VectorIndexWriter>(segment_id.volume->getDisk(), name); });

        index->serialize(&file_writer);
        index->saveDataID(&file_writer);
        printMemoryInfo(log, "After serialization");

        std::string version = index->getVersion().toString();
        auto usage = index->getResourceUsage();
        LOG_INFO(log, "memory_usage_bytes: {}, disk_usage_bytes: {}", usage.memory_usage_bytes, usage.disk_usage_bytes);
        std::unordered_map<std::string, std::string> infos;
        infos["memory_usage_bytes"] = std::to_string(usage.memory_usage_bytes);
        infos["disk_usage_bytes"] = std::to_string(usage.disk_usage_bytes);
        Metadata metadata(segment_id, version, type, metric, dimension, total_vec, fallback_to_flat, des, infos);
        auto buf = segment_id.volume->getDisk()->writeFile(segment_id.getVectorDescriptionFilePath(), 4096);
        metadata.writeText(*buf);

        return Status(0);
    }
    catch (const SearchIndexException & e)
    {
        LOG_ERROR(log, "SearchIndexException: {}", e.what());
        return Status(e.getCode(), e.what());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Failed to serialize due to {}", e.what());
        return Status(1, e.what());
    }
}

void VectorSegmentExecutor::handleMergedMaps()
{
    /// not from merge or have already loaded related row ids maps
    if (!segment_id.fromMergedParts() || !inverted_row_ids_map->empty())
    {
        return;
    }

    try
    {
        auto row_ids_map_buf
            = std::make_unique<DB::CompressedReadBufferFromFile>(segment_id.volume->getDisk()->readFile(segment_id.getRowIdsMapFilePath()));
        auto inverted_row_ids_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(
            segment_id.volume->getDisk()->readFile(segment_id.getInvertedRowIdsMapFilePath()));
        auto inverted_row_sources_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(
            segment_id.volume->getDisk()->readFile(segment_id.getInvertedRowSourcesMapFilePath()));

        while (!inverted_row_sources_map_buf->eof())
        {
            uint8_t * row_source_pos = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->position());
            uint8_t * row_sources_end = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->buffer().end());
            LOG_DEBUG(log, "Read from rows_sources_file: size {}", row_sources_end - row_source_pos);

            while (row_source_pos < row_sources_end)
            {
                inverted_row_sources_map->push_back(*row_source_pos);
                ++row_source_pos;
            }

            inverted_row_sources_map_buf->position() = reinterpret_cast<char *>(row_source_pos);
        }

        LOG_DEBUG(log, "Loaded {} inverted row sources map entries", inverted_row_sources_map->size());

        UInt64 row_id;

        while (!row_ids_map_buf->eof())
        {
            readIntText(row_id, *row_ids_map_buf);
            row_ids_map_buf->ignore();
            row_ids_map->push_back(row_id);
        }

        LOG_DEBUG(log, "Loaded {} row ids map entries", row_ids_map->size());

        while (!inverted_row_ids_map_buf->eof())
        {
            readIntText(row_id, *inverted_row_ids_map_buf);
            inverted_row_ids_map_buf->ignore();
            inverted_row_ids_map->push_back(row_id);
        }
    }
    catch (const DB::Exception &)
    {
        throw;
    }

    LOG_DEBUG(log, "Loaded {} inverted row ids map entries", inverted_row_ids_map->size());
}

/// For INVALID_VECTOR_INDEX error, vectorscan query will failed, will not retry
Status VectorSegmentExecutor::load(bool isActivePart)
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::load");
    CacheManager * mgr = CacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();
    const String cache_key_str = cache_key.toString();

    LOG_DEBUG(log, "segment_id.getPathPrefix() = {}", segment_id.getPathPrefix());
    LOG_DEBUG(log, "segment_id.getBitMapFilePath() = {}", segment_id.getBitMapFilePath());
    LOG_DEBUG(log, "cache_key_str = {}", cache_key_str);

    IndexWithMetaHolderPtr index_holder = mgr->get(cache_key);
    if (index_holder == nullptr)
    {
        if (!isActivePart)
            /// InActive part, Cancel Load Vector Index
            return Status(DB::ErrorCodes::INVALID_VECTOR_INDEX, "Part is inactive, will not reload index!");
        LOG_DEBUG(log, "Miss cache, cache_key_str = {}", cache_key_str);
        /// We don't want multiple execution engines loading index concurrently, so we use getOrSet method of LRUResourceCache
        /// to ensure that only one execution engine may read from disk at any time.
        LOG_DEBUG(log, "Num of item before cache {}", mgr->countItem());
        DB::VectorIndexEventLog::addEventLog(
            DB::Context::getGlobalContextInstance(),
            cache_key.getTableUUID(),
            cache_key.getPartName(),
            cache_key.getPartitionID(),
            DB::VectorIndexEventLogElement::LOAD_START);

        auto load_func = [&]() -> IndexWithMetaPtr
        {
            try
            {
                if (!segment_id.volume->getDisk()->exists(segment_id.getVectorReadyFilePath()))
                    throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "Index is not in the ready state and cannot be loaded");
                Metadata metadata(segment_id);
                auto buf = segment_id.volume->getDisk()->readFile(segment_id.getVectorDescriptionFilePath());
                metadata.readText(*buf);
                fallback_to_flat = metadata.fallback_to_flat;
                if (fallback_to_flat)
                {
                    type = Search::IndexType::FLAT;
                    std::erase_if(
                        des,
                        [](const auto & item)
                        {
                            auto const & [key, value] = item;
                            return key != "metric_type";
                        });
                }
                des.setParam("load_index_version", metadata.version);

                configureDiskMode();
                index = Search::
                    createVectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::FloatVector>(
                        segment_id.getIndexNameWithColumn(),
                        type,
                        metric,
                        dimension,
                        total_vec,
                        des,
                        false /* load_diskann_after_build */,
                        vector_index_cache_prefix,
#ifdef ENABLE_SCANN
                        getDiskIOManager(),
#endif
                        true /* use_file_checksum */,
                        true /* manage_cache_folder */);

                LOG_INFO(log, "loading vector index from {}", segment_id.getFullPath());
                auto file_reader = Search::IndexDataFileReader<Search::AbstractIStream>(
                    segment_id.getFullPath(),
                    [this](const std::string & name, std::ios::openmode /*mode*/)
                    { return std::make_shared<VectorIndexReader>(segment_id.volume->getDisk(), name); });
                printMemoryInfo(log, "Before load");
                index->load(&file_reader);
                index->loadDataID(&file_reader);
                printMemoryInfo(log, "After load");
                total_vec = index->numData();
                LOG_INFO(log, "load total_vec={}", total_vec);
                /// May failed to load merged row ids map due to background index build may remove them when finished.
                handleMergedMaps();

                auto del_row_ids = readDeleteBitmapAccordingSegmentId();
                delete_bitmap = std::make_shared<Search::DenseBitmap>(total_vec, true);
                convertBitmap(del_row_ids);

                return std::make_shared<IndexWithMeta>(
                    index,
                    total_vec,
                    delete_bitmap,
                    des,
                    row_ids_map,
                    inverted_row_ids_map,
                    inverted_row_sources_map,
                    disk_mode,
                    fallback_to_flat,
                    vector_index_cache_prefix);
            }
            catch (const SearchIndexException & e)
            {
                LOG_ERROR(log, "SearchIndexException: {}", e.what());
                /// Destruct the index in advance to ensure that removing vector index cache will not affect the next load
                index.reset();
                throw IndexException(e.getCode(), e.what());
            }
            catch (...)
            {
                LOG_ERROR(log, "Failed to load vector index, cache key={}", segment_id.getCacheKey().toString());
                index.reset();
                throw;
            }
        };
        auto global_context = DB::Context::getGlobalContextInstance();
        auto release_callback = [cache_key, global_context]()
        {
            if (!global_context->isShutdown())
                DB::VectorIndexEventLog::addEventLog(
                    global_context,
                    cache_key.getTableUUID(),
                    cache_key.getPartName(),
                    cache_key.getPartitionID(),
                    DB::VectorIndexEventLogElement::UNLOAD);
        };

        try
        {
            index_holder = mgr->load(cache_key, load_func, release_callback);
            LOG_DEBUG(log, "Num of item after cache {}", mgr->countItem());
            if (index_holder)
            {
                IndexWithMeta & new_index = index_holder->value();
                index = new_index.index;
                total_vec = new_index.total_vec;
                delete_bitmap = new_index.getDeleteBitmap();
                des = new_index.des;
                fallback_to_flat = new_index.fallback_to_flat;

                // For the vector index of the load decouple part, if there are
                // multiple threads performing load at the same time, only one
                // thread will actually execute load_func to update its own row ids map,
                // and other threads need to update their own row ids map according to the results of load_func
                if (!new_index.row_ids_map->empty())
                {
                    row_ids_map = new_index.row_ids_map;
                    inverted_row_ids_map = new_index.inverted_row_ids_map;
                    inverted_row_sources_map = new_index.inverted_row_sources_map;
                }

                DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                                     cache_key.getTableUUID(),
                                                     cache_key.getPartName(),
                                                     cache_key.getPartitionID(),
                                                     DB::VectorIndexEventLogElement::LOAD_SUCCEED);
                return Status();
            }
        }
        catch (const IndexException & e)
        {
            DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                                 cache_key.getTableUUID(),
                                                 cache_key.getPartName(),
                                                 cache_key.getPartitionID(),
                                                 DB::VectorIndexEventLogElement::LOAD_ERROR,
                                                 DB::ExecutionStatus(e.code(), e.message()));
            return Status(e.code(), e.message());
        }
        catch (const DB::Exception & e)
        {
            DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                                 cache_key.getTableUUID(),
                                                 cache_key.getPartName(),
                                                 cache_key.getPartitionID(),
                                                 DB::VectorIndexEventLogElement::LOAD_ERROR,
                                                 DB::ExecutionStatus(e.code(), e.message()));
            return Status(e.code(), e.message());
        }
        catch (const std::exception & e)
        {
            DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                                 cache_key.getTableUUID(),
                                                 cache_key.getPartName(),
                                                 cache_key.getPartitionID(),
                                                 DB::VectorIndexEventLogElement::LOAD_ERROR,
                                                 DB::ExecutionStatus(DB::ErrorCodes::STD_EXCEPTION, e.what()));
            return Status(DB::ErrorCodes::STD_EXCEPTION, e.what());
        }
        DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                             cache_key.getTableUUID(),
                                             cache_key.getPartName(),
                                             cache_key.getPartitionID(),
                                             DB::VectorIndexEventLogElement::LOAD_FAILED);

        return Status(2, "Load failed");
    }
    else
    {
        LOG_DEBUG(log, "Hit cache, cache_key_str = {}", cache_key_str);
        IndexWithMeta & new_index = index_holder->value();
        index = new_index.index;
        total_vec = new_index.total_vec;
        fallback_to_flat = new_index.fallback_to_flat;
        delete_bitmap = new_index.getDeleteBitmap();

        des = new_index.des;
        if (!new_index.row_ids_map->empty())
        {
            row_ids_map = new_index.row_ids_map;
            inverted_row_ids_map = new_index.inverted_row_ids_map;
            inverted_row_sources_map = new_index.inverted_row_sources_map;
        }
        else
        {
            // very fast and frequent operations under continuous deletes.
            // When reading row ids map related files, the files may be deleted,
            // resulting in reading failure. It is necessary to ensure that
            // the load thread is aware of the status when reading fails,
            // otherwise there will be problems when using the damaged row ids map directly
            try
            {
                updateCacheValueWithRowIdsMaps(std::move(index_holder));
            }
            catch(const DB::Exception & e)
            {
                DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                                     cache_key.getTableUUID(),
                                                     cache_key.getPartName(),
                                                     cache_key.getPartitionID(),
                                                     DB::VectorIndexEventLogElement::LOAD_ERROR,
                                                     DB::ExecutionStatus(e.code(), e.message()));
                return Status(e.code(), e.message());

            }
            catch(...)
            {
                DB::VectorIndexEventLog::addEventLog(DB::Context::getGlobalContextInstance(),
                                                     cache_key.getTableUUID(),
                                                     cache_key.getPartName(),
                                                     cache_key.getPartitionID(),
                                                     DB::VectorIndexEventLogElement::LOAD_ERROR,
                                                     DB::ExecutionStatus(DB::ErrorCodes::STD_EXCEPTION, "Unknown error"));
                return Status(2, "Load failed");
            }
        }

        return Status();
    }
}

std::shared_ptr<Search::SearchResult> VectorSegmentExecutor::search(
    VectorDatasetPtr queries,
    int32_t k,
    const Search::DenseBitmapPtr & filter,
    Search::Parameters & parameters,
    bool first_stage_only)
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::search()");
    
    // Check if the index is initialized and ready for searching
    if (index == nullptr)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "Index not initialized before searching!");
    }
    if (!index->ready())
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "Index not ready before searching!");
    }

    // Check if the dimensions of the searched index and input match
    if (queries->getDimension() != static_cast<int64_t>(dimension))
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "The dimension of searched index and input doesn't match.");
    }

    LOG_DEBUG(log, "Index {} has {} vectors", this->segment_id.getFullPath(), this->total_vec);

    SearchThreadLimiter limiter(log, max_threads);

    auto merged_filter = filter;
    // Merge filter and delete_bitmap
    if (!delete_bitmap->all())
        merged_filter = Search::intersectDenseBitmaps(filter, delete_bitmap);

    if (fallback_to_flat)
        parameters.clear();

    try
    {
        std::shared_ptr<Search::SearchResult> ret;
        // Perform the actual search
        {
            DB::OpenTelemetry::SpanHolder span_search("VectorSegmentExecutor::performSearch()::search");
            auto search_queries
                = std::make_shared<Search::DataSet<float>>(queries->getData(), queries->getVectorNum(), queries->getDimension());
            ret = index->search(search_queries, k, parameters, first_stage_only, merged_filter.get());
        }

        // Transfer the results to newRowIds
        /// if (!first_stage_only) Use new row ids for decouple part
        {
            DB::OpenTelemetry::SpanHolder span_transfer_id("VectorSegmentExecutor::performSearch()::transferToNewRowIds");
            transferToNewRowIds(ret);
        }
        return ret;
    }
    catch (const SearchIndexException & e)
    {
        LOG_ERROR(log, "SearchIndexException: {}", e.what());
        throw IndexException(e.getCode(), e.what());
    }
    catch (const std::exception & e)
    {
        throw IndexException(DB::ErrorCodes::STD_EXCEPTION, e.what());
    }
}

std::shared_ptr<Search::SearchResult> VectorSegmentExecutor::computeTopDistanceSubset(
    VectorDatasetPtr queries, std::shared_ptr<Search::SearchResult> first_stage_result, int32_t top_k)
{
    auto search_queries = std::make_shared<Search::DataSet<float>>(queries->getData(), queries->getVectorNum(), queries->getDimension());
    auto ret = index->computeTopDistanceSubset(search_queries, first_stage_result, top_k);
    transferToNewRowIds(ret);
    return ret;
}

Status VectorSegmentExecutor::searchWithoutIndex(
    VectorDatasetPtr query_data, 
    VectorDatasetPtr base_data, 
    int32_t k, 
    float *& distances, 
    int64_t *& labels, 
    const Search::Metric & metric)
{
    omp_set_num_threads(1);
    auto metric2 = metric;
    if (metric == Search::Metric::Cosine)
    {
        LOG_DEBUG(&Poco::Logger::get("VectorSegmentExecutor"), "Normalize vectors for cosine similarity brute force search");
        metric2 = Search::Metric::IP;
        query_data->normalize();
        base_data->normalize();
    }
    auto status = tryBruteForceSearch(
        query_data->getData(),
        base_data->getData(),
        query_data->getDimension(),
        k,
        query_data->getVectorNum(),
        base_data->getVectorNum(),
        labels,
        distances,
        metric2);
    if (metric == Search::Metric::Cosine)
    {
        for (int64_t i = 0; i < k * query_data->getVectorNum(); i++)
        {
            distances[i] = 1 - distances[i];
        }
    }
    return status;
}

void VectorSegmentExecutor::cancelVectorIndexLoading(const CacheKey & cache_key)
{
    Poco::Logger * log = &Poco::Logger::get("VectorSegmentExecutor");
    CacheManager * mgr = CacheManager::getInstance();

    IndexWithMetaHolderPtr index_holder = mgr->get(cache_key);
    if (index_holder != nullptr)
    {
        IndexWithMeta & index = index_holder->value();
        Search::IndexStatus index_status = index.index->getStatus();
        if (index_status == Search::IndexStatus::LOADING)
        {
            LOG_DEBUG(log, "Index {} is in {}, will be aborted", cache_key.toString(), index_status);
            index.index->abort();
        }
    }
}

Status VectorSegmentExecutor::removeFromCache(const CacheKey & cache_key)
{
    Poco::Logger * log = &Poco::Logger::get("VectorSegmentExecutor");
    CacheManager * mgr = CacheManager::getInstance();
    LOG_DEBUG(log, "Num of cache items before forceExpire {} ", mgr->countItem());
    mgr->forceExpire(cache_key);
    LOG_DEBUG(log, "Num of cache items after forceExpire {} ", mgr->countItem());
    return Status();
}

int64_t VectorSegmentExecutor::getRawDataSize()
{
    return total_vec;
}

Status VectorSegmentExecutor::cancelBuild()
{
    /// TODO implement
    return Status();
}

Status VectorSegmentExecutor::removeByIds(size_t n, const size_t * ids)
{
    for (size_t i = 0; i < n; i++)
    {
        if (delete_bitmap->is_member(ids[i]))
        {
            delete_bitmap->unset(ids[i]);
        }
    }
    return Status();
}

bool VectorSegmentExecutor::writeBitMap()
{
    String bitmap_path = segment_id.getBitMapFilePath();
    auto writer = segment_id.volume->getDisk()->writeFile(bitmap_path);

    if (delete_bitmap == nullptr)
        delete_bitmap = std::make_shared<Search::DenseBitmap>(total_vec, true);
    size_t size = delete_bitmap->get_size();
    writer->write(reinterpret_cast<const char *>(&size), sizeof(size_t));

    writer->write(reinterpret_cast<const char *>(delete_bitmap->get_bitmap()), delete_bitmap->byte_size());
    writer->finalize();

    return true;
}

bool VectorSegmentExecutor::readBitMap()
{
    String read_file_path = segment_id.getBitMapFilePath();
    auto reader = segment_id.volume->getDisk()->readFile(read_file_path);

    if (!reader)
        return false;

    size_t size;
    try
    {
        reader->readStrict(reinterpret_cast<char *>(&size), sizeof(size_t));
    }
    catch (...)
    {
        LOG_ERROR(log, "Bitmap file read error.");
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "Vector index bitmap on disk is corrupted");
    }

    if (delete_bitmap == nullptr)
        delete_bitmap = std::make_shared<Search::DenseBitmap>(size);

    size_t bit_map_size = delete_bitmap->byte_size();

    if (total_vec != 0 && size != total_vec)
    {
        LOG_ERROR(log, "Bitmap file {} is corrupted: size {}, total_vec {}", read_file_path, size, total_vec);
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "Vector index bitmap on disk is corrupted");
    }

    reader->readStrict(reinterpret_cast<char *>(delete_bitmap->get_bitmap()), bit_map_size);

    return true;
}

void VectorSegmentExecutor::setCacheManagerSizeInBytes(size_t size)
{
    CacheManager::setCacheSize(size);
}

std::list<std::pair<CacheKey, Search::Parameters>> VectorSegmentExecutor::getAllCacheNames()
{
    ///from this list, we get <segment_id, vectorindex description> pair
    return CacheManager::getInstance()->getAllItems();
}

void VectorSegmentExecutor::updateBitMap(const std::vector<UInt64> & deleted_row_ids)
{
    /// Update bitmap in cache if exists
    CacheManager * mgr = CacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();

    IndexWithMetaHolderPtr index_holder = mgr->get(cache_key);
    if (segment_id.fromMergedParts() ||
        !index_holder ||
        deleted_row_ids.empty())
        return;

    delete_bitmap = std::make_shared<Search::DenseBitmap>(*index_holder->value().getDeleteBitmap());

    /// Map new deleted row ids to row ids in old part and update delete bitmap
    bool need_update = false;
    for (auto & del_row_id : deleted_row_ids)
    {
        if (delete_bitmap->is_member(del_row_id))
        {
            delete_bitmap->unset(del_row_id);

            if (!need_update)
                need_update = true;
        }
    }

    if (!need_update)
        return;

    /// Update bitmap in cache if exists
    if (index_holder)
        index_holder->value().setDeleteBitmap(delete_bitmap);
}

void VectorSegmentExecutor::updateMergedBitMap(const std::vector<UInt64> & deleted_row_ids)
{
    /// Update bitmap in cache if exists
    CacheManager * mgr = CacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();

    IndexWithMetaHolderPtr index_holder = mgr->get(cache_key);

    if (!segment_id.fromMergedParts() ||
        !index_holder ||
        deleted_row_ids.empty())
        return;

    delete_bitmap = std::make_shared<Search::DenseBitmap>(*index_holder->value().getDeleteBitmap());

    /// Call handleMergedMaps() to get inverted_row_ids_map and inverted_row_sources_map
    try
    {
        handleMergedMaps();
    }
    catch (const DB::Exception & e)
    {
        LOG_DEBUG(log, "Skip to update vector bitmap due to failure when read inverted row ids map entries, error: {}", e.what());
        return;
    }

    /// Map new deleted row ids to row ids in old part and update delete bitmap
    bool need_update = false;
    for (auto & new_row_id : deleted_row_ids)
    {
        if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
        {
            UInt64 old_row_id = (*inverted_row_ids_map)[new_row_id];
            if (delete_bitmap->is_member(old_row_id))
            {
                delete_bitmap->unset(old_row_id);

                if (!need_update)
                    need_update = true;
            }
        }
    }

    if (!need_update)
        return;

    if (index_holder)
        index_holder->value().setDeleteBitmap(delete_bitmap);
}

#ifdef ENABLE_SCANN
std::shared_ptr<Search::DiskIOManager> VectorSegmentExecutor::getDiskIOManager()
{
    static std::mutex mutex;
    static std::shared_ptr<Search::DiskIOManager> io_manager = nullptr;

    if (type != Search::IndexType::MSTG || disk_mode == 0)
        return nullptr;

    std::lock_guard<std::mutex> lock(mutex);
    if (io_manager == nullptr)
        io_manager = std::make_shared<Search::DiskIOManager>(max_threads, 64);
    return io_manager;
}
#endif

void VectorSegmentExecutor::configureDiskMode()
{
    if (type == Search::IndexType::MSTG)
    {
        disk_mode = des.getParam("disk_mode", DEFAULT_DISK_MODE);
        if (!des.contains("disk_mode"))
            des.setParam("disk_mode", disk_mode);
        if (disk_mode)
        {
            vector_index_cache_prefix = fs::path(segment_id.getVectorIndexCachePrefix()).parent_path().string() + '-' + generateUUIDv4() + '/';
            LOG_INFO(log, "vector_index_cache_prefix: {}", vector_index_cache_prefix);
        }
    }
}

const std::vector<UInt64> VectorSegmentExecutor::readDeleteBitmapAccordingSegmentId() const
{
    // Get mergedatapart information according to context
    auto local_context = DB::Context::createCopy(DB::Context::getGlobalContextInstance());
    DB::UUID table_uuid = DB::VectorIndexEventLog::parseUUID(segment_id.getCacheKey().getTableUUID());

    // Get database and table name
    if (!DB::DatabaseCatalog::instance().tryGetByUUID(table_uuid).second)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Unable to get table and database by table uuid");
    auto table_id = DB::DatabaseCatalog::instance().tryGetByUUID(table_uuid).second->getStorageID();
    if (!table_id)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Unable to get table and database by table uuid");

    // Get MergeTree Data Storage
    DB::StoragePtr table = DB::DatabaseCatalog::instance().tryGetTable({table_id.database_name, table_id.table_name}, local_context);
    DB::MergeTreeData * merge_tree = dynamic_cast<DB::MergeTreeData *>(table.get());
    if (!merge_tree)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Unable to fetch MergeTree Data Storage");

    // Get the part corresponding to the current segment, Whether to allow reading delete bitmap from part in outdated state?
    auto part = merge_tree->getPartIfExists(segment_id.current_part_name, {DB::MergeTreeDataPartState::Active, DB::MergeTreeDataPartState::Outdated});
    if (!part)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Cannot get active part according to the segment");

    LOG_INFO(log,
             "Read Delete Bitmap From Part {}, Current segment_id Part {}, Own Part {}",
             part->name,
             segment_id.current_part_name,
             segment_id.owner_part_name);
    auto row_exists_column_opt = part->readRowExistsColumn();
    std::vector<UInt64> del_row_ids; /// Store deleted row ids
    if (!row_exists_column_opt.has_value())
    {
        LOG_INFO(log, "row_exists column is empty in part {}, Delete bitmap will be all set to true", part->name);
        return del_row_ids;
    }

    const DB::ColumnUInt8 * row_exists_col = typeid_cast<const DB::ColumnUInt8 *>(row_exists_column_opt.value().get());
    if (row_exists_col == nullptr)
    {
        LOG_WARNING(log, "row_exists column type is not UInt8 in part {}", part->name);
        return del_row_ids;
    }

    const DB::ColumnUInt8::Container & vec_res = row_exists_col->getData();
    for (size_t pos = 0; pos < vec_res.size(); pos++)
    {
        if (!vec_res[pos])
            del_row_ids.push_back(static_cast<UInt64>(pos));
    }

    return del_row_ids;
}

void VectorSegmentExecutor::convertBitmap(const std::vector<UInt64> & deleted_row_ids)
{
    if (segment_id.fromMergedParts())
        if (!inverted_row_sources_map ||
            !inverted_row_ids_map)
        {
            LOG_ERROR(log, "Convert Bitmap From Merge Parts, But inverted row is empty. This is Bug!");
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Inverted Row Id Data corruption");
        }

    if (segment_id.fromMergedParts())
    {
        for (auto & new_row_id : deleted_row_ids)
        {
            if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
            {
                UInt64 old_row_id = (*inverted_row_ids_map)[new_row_id];
                if (delete_bitmap->is_member(old_row_id))
                {
                    delete_bitmap->unset(old_row_id);
                }
            }
        }
    }
    else
    {
        for (auto & del_row_id : deleted_row_ids)
        {
            if (delete_bitmap->is_member(del_row_id))
            {
                delete_bitmap->unset(del_row_id);
            }
        }
    }
}

}
