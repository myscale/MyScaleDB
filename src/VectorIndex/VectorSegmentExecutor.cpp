#include <VectorIndex/VectorSegmentExecutor.h>
#include <random>
#include <thread>
#include <omp.h>
#include <boost/algorithm/string/split.hpp>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <VectorIndex/BruteForceSearch.h>
#include <VectorIndex/CacheManager.h>
#include <VectorIndex/DiskIOReader.h>
#include <VectorIndex/DiskIOWriter.h>
#include <VectorIndex/IndexException.h>
#include <VectorIndex/MergeUtils.h>
#include <VectorIndex/VectorIndexFactory.h>
#include <VectorIndex/VectorIndexCommon.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>

#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int STD_EXCEPTION;
extern const int CORRUPTED_DATA;
}

namespace VectorIndex
{
std::once_flag once;
std::shared_mutex mu;
std::condition_variable_any cv;
int num_thread_for_vector;
std::atomic_int count;

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

static String dumpBitmap(GeneralBitMapPtr bit_map_ptr)
{
    if (bit_map_ptr == nullptr)
    {
        return "";
    }

    String r;

    const int size = bit_map_ptr->size;
    for (int i = 0; i < 10 && i < size; ++i)
    {
        if (i > 0)
        {
            r += ",";
        }
        r += (bit_map_ptr->test(i) ? "1" : "0");
    }

    return r;
}

/// TODO segment_id needs to be dynamic in the future
VectorSegmentExecutor::VectorSegmentExecutor(IndexType type_, const SegmentId & segment_id_, Parameters des_, size_t dimension_)
    : dimension(dimension_), type(type_), segment_id(segment_id_), log(&Poco::Logger::get("VectorSegmentExecutor")), des(des_)
{
    std::call_once(once, [&] {
        int num_threads = omp_get_max_threads();
        if (num_threads <= 0)
        {
            num_threads = 16;
        }
        num_thread_for_vector = num_threads;
        omp_set_num_threads(num_thread_for_vector);
        count.store(0);
        LOG_INFO(log, "set omp_num_threads to {}", num_threads);
    });
}

VectorSegmentExecutor::VectorSegmentExecutor(const SegmentId & segment_id_)
    : dimension(0)
    , type(IndexType::FLAT)
    , mode(IndexMode::CPU)
    , me(Metrics::L2)
    , segment_id(segment_id_)
    , log(&Poco::Logger::get("VectorSegmentExecutor"))
{
}

Status VectorSegmentExecutor::buildIndex(VectorDatasetPtr data_set, int64_t total_vectors_expected, bool slow_mode)
{
    Parameters para_copy = des;
    try
    {
        if (!index)
        {
            //        if(data_set->getVectorNum()< MAX_BRUTE_FORCE_SEARCH_SIZE){
            //            type = IndexType::FLAT;
            //        }
            LOG_INFO(log, "Index type actually created {}", VectorIndexFactory::typeToString(type));
            if (para_copy.contains("metric_type"))
            {
                me = VectorIndexFactory::createIndexMetrics(para_copy.at("metric_type"));
                para_copy.erase("metric_type");
            }
            if (para_copy.contains("mode"))
            {
                mode = VectorIndexFactory::createIndexMode(para_copy.at("mode"));
                para_copy.erase("mode");
            }
            if (para_copy.contains("compression_scheme"))
            {
                cmb = DB::CompressionCodecFactory::instance().get(para_copy.find("compression_scheme")->second, {})->getMethodByte();
                para_copy.erase("compression_scheme");
            }
            index = VectorIndexFactory::createIndex(type, mode, me, this->dimension, para_copy);

            {
                /// slow mode
                if (slow_mode)
                {
                    int num_procs = omp_get_num_procs();
                    /// only use half cores
                    omp_set_num_threads(std::max(1, num_procs / 4));
                    LOG_INFO(log, "build index in slow mode, set num threads to {} with omp_get_num_procs {}", std::max(1, num_procs / 4), num_procs);
                }
            }

            index->train(data_set, total_vectors_expected);
            if (delete_bitmap == nullptr)
            {
                delete_bitmap = std::make_shared<GeneralBitMap>(total_vectors_expected);
                memset(delete_bitmap->bitmap, 255, (total_vectors_expected / 8) + 1);
            }
            return Status();
        }
        else
        {
            /// maybe can never reach here
            return Status(5, "vector index already built!");
        }
    }
    catch (const IndexException & e)
    {
        LOG_ERROR(log, "IndexException: {}, {}", e.code(), e.message());
        return Status(e.code(), e.message());
    }
}

void VectorSegmentExecutor::updateCacheValueWithRowIdsMaps()
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::updateCacheValueWithRowIdsMaps");
    try
    {
        handleMergedMaps();
    }
    catch(const DB::Exception & e)
    {
        LOG_DEBUG(log, "[updateCacheValueWithRowIdsMaps]: Failed to load inverted row ids map entries, error: {}", e.what());
        return;
    }

    if (inverted_row_sources_map->empty())
    {
        return;
    }
    CacheKey cache_key = segment_id.getCacheKey();
    CacheManager * mgr = CacheManager::getInstance();
    IndexWithMetaPtr index = mgr->get(cache_key);
    if (index != nullptr)
    {
        index->row_ids_map = this->row_ids_map;
        index->inverted_row_ids_map = this->inverted_row_ids_map;
        index->inverted_row_sources_map = this->inverted_row_sources_map;
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
    if (!des.contains("type"))
    {
        des.insert(std::make_pair("type", VectorIndexFactory::typeToString(type)));
    }
    /// when cacheIndexAndMeta() is called, related files should have already been loaded.
    IndexWithMetaPtr cache_item = std::make_shared<IndexWithMeta>(index, total_vec, op_points, delete_bitmap, des,
        row_ids_map, inverted_row_ids_map, inverted_row_sources_map);

    LOG_INFO(log, "cache key: {}", segment_id.getCacheKey().toString());
    mgr->put(segment_id.getCacheKey(), cache_item);
    LOG_INFO(log, "num of item after cache {}", mgr->countItem());
    return Status();
}

Status VectorSegmentExecutor::serialize()
{
    /// Serialization contains three steps:
    /// 1. write vector_index_ready file to mark that we start writting
    /// 2. incrementally write index file
    /// 3. write vector_index_ready file to mark that we finished writting
    ///    vector_index_ready file is a binary_log which can only be appended
    ///    to but not altered
    try
    {
        int64_t binary_total_size = 0;
        bool last_part = false;
        int segment_count = 0;
        BinaryPtr index_binary;
        startWrite();
        while (!last_part)
        {
            /// Even though we set the max bytes to serialize for serialization,
            /// it could exceed this amount by accident,then we need to handle the exceeded part.
            LOG_INFO(log, "expected segment_size: {}", optimal_segment_size);
            index_binary = index->serialize(optimal_segment_size, last_part);
            if (index_binary->size <= 0)
            {
                break;
            }
            binary_total_size += index_binary->size;
            LOG_INFO(log, "binary_total_size: {}", binary_total_size);
            int64_t actual_all = index_binary->size;
            int64_t written = 0;
            while (actual_all > 0)
            {
                size_t max_allowed_per_write = compressBound(actual_all, cmb);
                bool last_sub_part = (last_part & (max_allowed_per_write == actual_all));
                /// write index loop, compress and write index in small parts
                Status stat = writePart(last_sub_part, segment_count, index_binary->data + written, max_allowed_per_write);
                if (!stat.fine())
                {
                    return stat;
                }
                segment_count++;
                actual_all -= max_allowed_per_write;
                written += max_allowed_per_write;
            }
        }

        writeBitMap();
        return finishWrite(binary_total_size);
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "serialze: failed due to {}", e.what());
        return Status(1, e.what());
    }
}

Status VectorSegmentExecutor::startWrite()
{
    DiskIOWriter ready_flag_writer;
    /// try a more elegant way
    /// String ready_file_path = segment_id.substr(0, segment_id.find("//")) + "/" + VECTOR_INDEX_READY;
    String ready_file_path = segment_id.getVectorReadyFilePath();
    String index_type = VectorIndexFactory::typeToString(type);
    String paras = "";
    String index_name = segment_id.getIndexNameWithColumn();
    /// -1 means the index is invalid in disk
    String binary_total_size_str = "-1";
    String nextline = "\n";
    
    if (!ready_flag_writer.open(ready_file_path + VECTOR_INDEX_FILE_SUFFIX, true))
    {
        if (!ready_flag_writer.open(ready_file_path, true))
        {
            LOG_ERROR(log, "fail to open {}", ready_file_path);
            return Status(5, "not able to open ready flag for write!");
        }
    }
    String all_string_together = index_type + ";" + paras + ";" + index_name + ":" + binary_total_size_str + nextline;
    LOG_INFO(log, "{}, length{}", all_string_together, all_string_together.length());
    ready_flag_writer.seekp(0, seekdir::end);
    ready_flag_writer.write((void *)all_string_together.c_str(), all_string_together.length());
    ready_flag_writer.close();
    return Status();
}

Status VectorSegmentExecutor::writePart(bool final, int segment_count, uint8_t * index_segment_offset, size_t index_segment_size)
{
    std::string part_id = segment_id.getFullPath() + "_" + ItoS(segment_count) + VECTOR_INDEX_FILE_SUFFIX;
    BinaryPtr index_binary_compressed = std::make_shared<Binary>();
    LOG_INFO(log, "Size of binary before compress: {}", index_segment_size);
    compressWithCheckSum(index_segment_offset, index_segment_size, index_binary_compressed);
    LOG_INFO(log, "Size of binary after compress: {}", index_binary_compressed->size);
    DiskIOWriter writer;
    if (!writer.open(part_id, false))
    {
        LOG_ERROR(log, "fail to open {}", part_id);
        return Status(5, "not able to open file!");
    }
    int64_t final_mark = final ? 1 : 0;
    int64_t binary_length_compressed = index_binary_compressed->size;
    int64_t binary_length_original = index_segment_size;
    ///when this is the last part to write, the mark will be 1, else 0.
    writer.write(&final_mark, sizeof(final_mark));
    ///compressed size of binaries of index
    writer.write(&binary_length_compressed, sizeof(binary_length_compressed));
    ///uncompressed size of binaries of index
    writer.write(&binary_length_original, sizeof(binary_length_original));
    ///total vector
    writer.write(&total_vec, sizeof(total_vec));
    ///compressed binaries of index
    writer.write(index_binary_compressed->data, binary_length_compressed);
    /// writer.write(index_segment_offset, index_segment_size);
    writer.close();
    ///after serializing the index, we write a ready flag to mark future
    ///TODO with checksum, we can possiblly drop this
    return Status();
}

Status VectorSegmentExecutor::finishWrite(int64_t binary_total_size)
{
    DiskIOWriter ready_flag_writer;
    String ready_file_path = segment_id.getVectorReadyFilePath();
    String index_type;
    index_type = VectorIndexFactory::typeToString(type);
    String paras;
    for (auto & s : des)
    {
        LOG_INFO(log, "{}", s.first);
        LOG_INFO(log, "{}", s.second);
        LOG_INFO(log, "{}", paras);
        paras = paras + s.first + ",";
        paras = paras + s.second + ",";
    }
    String index_name = segment_id.getIndexNameWithColumn();
    String binary_total_size_str = ItoS(binary_total_size);
    String nextline = "\n";
    ///TODO,This is hacky, as our segment_id is like store/12345/all_1_1_0//v1, the extra / before v1 gives a delimeter.
    ///need to find a better way to handle this
    if (!ready_flag_writer.open(ready_file_path + VECTOR_INDEX_FILE_SUFFIX, true))
    {
        if (!ready_flag_writer.open(ready_file_path, true))
        {
            LOG_ERROR(log, "fail to open {}", ready_file_path);
            return Status(5, "not able to open ready flag for write!");
        }
    }
    String all_string_together = index_type + ";" + paras + ";" + index_name + ":" + binary_total_size_str + nextline;
    LOG_INFO(log, "{}, length {}", all_string_together, all_string_together.length());
    ready_flag_writer.seekp(0, seekdir::end);
    ready_flag_writer.write((void *)all_string_together.c_str(), all_string_together.length());
    ready_flag_writer.close();
    return Status();
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
        auto row_ids_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(std::make_unique<DB::ReadBufferFromFile>(segment_id.getRowIdsMapFilePath()));
        auto inverted_row_ids_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(std::make_unique<DB::ReadBufferFromFile>(segment_id.getInvertedRowIdsMapFilePath()));
        auto inverted_row_sources_map_buf = std::make_unique<DB::CompressedReadBufferFromFile>(std::make_unique<DB::ReadBufferFromFile>(segment_id.getInvertedRowSourcesMapFilePath()));

        while (!inverted_row_sources_map_buf->eof())
        {
            uint8_t * row_source_pos = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->position());
            uint8_t * row_sources_end = reinterpret_cast<uint8_t *>(inverted_row_sources_map_buf->buffer().end());
            LOG_DEBUG(log, "[generateRowIdsMap]: read from rows_sources_file: size {}", row_sources_end - row_source_pos);

            while (row_source_pos < row_sources_end)
            {
                inverted_row_sources_map->push_back(*row_source_pos);
                ++row_source_pos;
            }

            inverted_row_sources_map_buf->position() = reinterpret_cast<char *>(row_source_pos);
        }

        LOG_DEBUG(log, "[VectorSegmentExecutor]: loaded {} inverted row sources map entries", inverted_row_sources_map->size());

        UInt64 row_id;

        while (!row_ids_map_buf->eof())
        {
            readIntText(row_id, *row_ids_map_buf);
            row_ids_map_buf->ignore();
            row_ids_map->push_back(row_id);
        }

        LOG_DEBUG(log, "[VectorSegmentExecutor]: loaded {} row ids map entries", row_ids_map->size());

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

    LOG_DEBUG(log, "[VectorSegmentExecutor]: loaded {} inverted row ids map entries", inverted_row_ids_map->size());
}

Status VectorSegmentExecutor::load()
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::load");
    CacheManager * mgr = CacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();
    const String cache_key_str = cache_key.toString();

    LOG_DEBUG(log, "[load] segment_id.getPathSuffix() = {}", segment_id.getPathSuffix());
    LOG_DEBUG(log, "[load] segment_id.getBitMapFilePath() = {}", segment_id.getBitMapFilePath());
    LOG_DEBUG(log, "[load] cache_key_str = {}", cache_key_str);

    IndexWithMetaPtr new_index = mgr->get(cache_key);
    if (new_index == nullptr)
    {
        LOG_DEBUG(log, "[load] miss cache, cache_key_str = {}", cache_key_str);
        ///we don't want many execution engine reading disk and preserving multiple copies of index, so we use a unique lock to
        ///ensure that only one execution engine may read from disk at any time.
        LOG_DEBUG(log, "[load] num of item before cache {}", mgr->countItem());
        mgr->startLoading(cache_key);
        std::shared_ptr<std::mutex> this_segment_mutex = mgr->getMutex(cache_key);
        if (this_segment_mutex != nullptr)
        {
            LOG_TRACE(log, "entering critical area");
            const std::lock_guard<std::mutex> lock(*this_segment_mutex);
            LOG_TRACE(log, "acquired lock");
            /// when it acquires the lock, it has to double check if the index was cached by its previous execution engine
            IndexWithMetaPtr new_index = mgr->get(cache_key);
            if (new_index != nullptr)
            {
                index = new_index->index;
                total_vec = new_index->total_vec;
                op_points = new_index->op_points;
                delete_bitmap = new_index->getDeleteBitmap();
                des = new_index->des;
                if (auto_tune && getOps().getCode() != 0)
                {
                    LOG_WARNING(log, "Index not autotuned");
                }
                return Status();
            }

            DiskIOReader reader;
            ///TODO this is really funky... have to change it later
            String ready_file_path = segment_id.getVectorReadyFilePath();
            String index_name = segment_id.getIndexNameWithColumn();
            std::vector<String> index_names{index_name};
            std::unordered_map<std::string, Parameters> params;
            std::unordered_map<String, int64_t> original_binary_sizes
                = readVectorIndexReadyFile(reader, ready_file_path, index_names, params);
            if (original_binary_sizes.find(index_name) == original_binary_sizes.end())
            {
                LOG_DEBUG(log, "[load] unable to parse the original index size {}", ready_file_path);
                return Status(5, "unable to parse the original index size " + ready_file_path);
            }
            int64_t original_binary_size = original_binary_sizes.find(index_name)->second;
            if (original_binary_size < 0)
            {
                return Status(5, "unable to parse the original index size " + ready_file_path);
            }
            des = params.at(index_name);
            BinaryPtr index_binary = std::make_shared<Binary>();
            index_binary->size = original_binary_size;
            LOG_INFO(log, "[load] original_binary_size: {}", original_binary_size);
            index_binary->data = new uint8_t[original_binary_size + COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER];

            bool next = true;
            int part_count = 0;
            int64_t current_loaded_size = 0;
            while (next)
            {
                Status stat = readPart(next, part_count, index_binary->data + current_loaded_size, current_loaded_size);
                if (!stat.fine())
                {
                    return stat;
                }
                part_count++;
            }
            LOG_DEBUG(log, "[load] after read part");

            if (current_loaded_size != original_binary_size)
            {
                LOG_ERROR(log, "vector index binary size not matching size recorded in metadata, this might be corrupted data.");
                return Status(5, "corrupted data: " + segment_id.getFullPath());
            }

            if (!readBitMap())
            {
                LOG_WARNING(log, "vector bitMap file not readable !");
                return Status(5, "corrupted data: " + segment_id.getFullPath());
            }

            if (des.contains("metric_type"))
            {
                me = VectorIndexFactory::createIndexMetrics(des.at("metric_type"));
            }
            Parameters place_holder;
            index = VectorIndexFactory::createIndex(type, mode, me, dimension, place_holder);
            LOG_INFO(log, "[load] start loading index: total_vec: {}", total_vec);
            try
            {
                index->load(index_binary, total_vec);
            }
            catch (const IndexException & e)
            {
                return Status(e.code(), e.message());
            }
            index->setTrained();
            index->parseParameter(des);
            LOG_INFO(log, "[load] finish loading index");
            if (auto_tune && getOps().getCode() != 0)
            {
                LOG_WARNING(log, "Index not autotuned");
            }

            try
            {
                /// May failed to load merged row ids map due to background index build may remove them when finished.
                handleMergedMaps();
            }
            catch(const DB::Exception & e)
            {
                LOG_DEBUG(log, "[load]: Failed to load inverted row ids map entries, error: {}", e.what());
                return Status(e.code(), e.message());
            }

            return cache();
        }
        else
        {
            return Status(4, "can't lock this segment, aborting: " + segment_id.getCacheKey().toString());
        }
    }
    else
    {
        LOG_DEBUG(log, "[load] hit cache, cache_key_str = {}", cache_key_str);
        index = new_index->index;
        total_vec = new_index->total_vec;
        op_points = new_index->op_points;
        delete_bitmap = new_index->getDeleteBitmap();

        des = new_index->des;
        if (!new_index->row_ids_map->empty())
        {
            row_ids_map = new_index->row_ids_map;
            inverted_row_ids_map = new_index->inverted_row_ids_map;
            inverted_row_sources_map = new_index->inverted_row_sources_map;
        }
        else
        {
            // very fast and frequent operations under continuous deletes
            updateCacheValueWithRowIdsMaps();
        }

        if (auto_tune && getOps().getCode() != 0)
        {
            LOG_WARNING(log, "Index not autotuned");
        }

        LOG_DEBUG(log, "[load] after load");
        return Status();
    }
}

Status VectorSegmentExecutor::readPart(bool & next, int part_count, uint8_t* index_binary, int64_t & current_loaded_size)
{
    DiskIOReader reader;
    String path = segment_id.getFullPath() + "_" + ItoS(part_count) + VECTOR_INDEX_FILE_SUFFIX;
    if (!reader.open(path))
    {
        return Status(5, "unable to open file " + path);
    }
    BinaryPtr index_binary_compressed = std::make_shared<Binary>();
    /// first 8 bytes is final mark, deciding if this is the last segment
    int64_t final_mark;
    reader.read(&final_mark, sizeof(final_mark));
    if (final_mark)
    {
        next = false;
    }

    /// second 8 bytes are meta recording compressed binary size of index
    reader.seekg(sizeof(int64_t));
    int64_t binary_length;
    reader.read(&binary_length, sizeof(binary_length));
    LOG_DEBUG(log, "[readPart] binary length in meta {}", binary_length);

    /// third 8 bytes are meta recording uncompressed binary size of index
    reader.seekg(sizeof(int64_t) * 2);
    int64_t binary_length_original;
    reader.read(&binary_length_original, sizeof(binary_length_original));
    LOG_DEBUG(log, "[readPart] binary length originally in meta {}", binary_length_original);

    /// fourth 8 bytes records total vectors stored, this is repeated many times. Could be d, or not.
    reader.seekg(sizeof(int64_t) * 3);
    int64_t total_vec_bin;
    reader.read(&total_vec_bin, sizeof(total_vec_bin));
    LOG_DEBUG(log, "[readPart] total vectors read: {}", total_vec_bin);
    total_vec = total_vec_bin;

    /// finally we have the compressed binaries
    reader.seekg(sizeof(int64_t) * 4);
    index_binary_compressed->data = new uint8_t[binary_length + COMPRESSION_ADDITIONAL_BYTES_AT_END_OF_BUFFER];
    index_binary_compressed->size = binary_length;

    reader.read(index_binary_compressed->data, binary_length);

    validateAndDecompress(index_binary_compressed, binary_length_original, index_binary);

    current_loaded_size += binary_length_original;
    LOG_DEBUG(log, "[readPart] current_loaded_size: {}", current_loaded_size);

    return Status();
}

Status VectorSegmentExecutor::addVectors(VectorDatasetPtr dataset)
{
    LOG_TRACE(log, "adding {} vectors", dataset->getVectorNum());
    index->addWithoutId(dataset);
    total_vec += dataset->getVectorNum();
    index->setTrained();
    ///index is only searchable after the first call to addVector.
    ///this bypassed some concurrency problem.
    return Status();
}

Status VectorSegmentExecutor::search(
    VectorDatasetPtr dataset, int32_t k, float *& distances, int64_t *& labels, GeneralBitMapPtr filter, Parameters parameters)
{
    DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::search");
    bool added = false;
    try
    {
        if (index == nullptr)
        {
            return Status(3, "index not initialized before searching!");
        }
        if (!index->trainStatus())
        {
            return Status(7, "index not trained before searching!");
        }
        if (dataset->getDimension() != dimension)
        {
            return Status(10, "the dimension of searched index and input doesn't match.");
        }
        LOG_DEBUG(log, "{} vectors in engine {}", this->total_vec, this->segment_id.getFullPath());
        Parameters params = parameters;
        if (op_points != nullptr)
        {
            ///TODO pass acc_req in from user
            float acc_req = 0.9;
            bool satisfied = false;
            for (auto & acc_parameters : *op_points)
            {
                LOG_DEBUG(log, "op point tested with acc {},", acc_parameters.first);
                if (acc_parameters.first >= acc_req)
                {
                    params = acc_parameters.second->op_point;
                    satisfied = true;
                    break;
                }
            }
            if (!satisfied)
            {
                LOG_WARNING(log, "index can't satisfy the required accuracy, switching to brute force search.");
                return Status(200);
            }
        }

        filter = mergeBitMap(filter, this->getDeleteBitMap());

        std::shared_lock<std::shared_mutex> lock(mu);
        cv.wait(lock, [] { return count.load() <= num_thread_for_vector; });
        count.fetch_add(1);
        added = true;
        LOG_DEBUG(log, "[search] index search, num threads: {}", omp_get_max_threads());
        /// a shared lock on a small number of concurrent threads, like 16. this is not hard limit so race is not a problem.
        {
            DB::OpenTelemetry::SpanHolder span("VectorSegmentExecutor::search::vector_index_search");
            span.addAttribute("vec_search.num_threads", omp_get_max_threads());
            index->search(dataset, k, distances, labels, params, filter);
        }

        transferToNewRowIds(labels, k * dataset->getVectorNum());
        LOG_DEBUG(log, "[search] after transfer row ids");
    }
    catch (const IndexException & e)
    {
        LOG_ERROR(log, "IndexException: {}", e.message());
        if (added)
            count.fetch_sub(1);
        cv.notify_one();
        return Status(e.code(), e.message());
    }
    if (added)
        count.fetch_sub(1);
    cv.notify_one();
    LOG_DEBUG(log, "[search] before return status");
    return Status();
}

Status VectorSegmentExecutor::searchWithoutIndex(
    VectorDatasetPtr query_data, VectorDatasetPtr base_data, int32_t k, float *& distances, int64_t *& labels, const Metrics& metrics)
{
    LOG_DEBUG(&Poco::Logger::get("VectorSegmentExecutor"), "[searchWithoutIndex] query_data {}", query_data->printVectors());
    omp_set_num_threads(1);
    return tryBruteForceSearch(
        query_data->getData(),
        base_data->getData(),
        query_data->getDimension(),
        k,
        query_data->getVectorNum(),
        base_data->getVectorNum(),
        labels,
        distances,
        metrics);
}

Status VectorSegmentExecutor::copyToCpu()
{
    //TODO
    return Status();
}

// Status VectorSegmentExecutor::copyToGpu(int32_t device_id, bool hybrid)
// {
//TODO
//     return Status();
// }

IndexType VectorSegmentExecutor::indexType()
{
    return index->indexType();
}

Status VectorSegmentExecutor::removeFromCache(const CacheKey & cache_key)
{
    CacheManager * mgr = CacheManager::getInstance();
    LOG_INFO(&Poco::Logger::get("VectorSegmentExecutor"), "[removeFromCache] num of cache items before forceExpire {} ", mgr->countItem());
    mgr->forceExpire(cache_key);
    LOG_INFO(&Poco::Logger::get("VectorSegmentExecutor"), "[removeFromCache] num of cache items after forceExpire {} ", mgr->countItem());
    return Status();
}

int64_t VectorSegmentExecutor::getRawDataSize()
{
    return total_vec;
}

void VectorSegmentExecutor::setIndexParameters(Parameters p)
{
    index->getMyParameters(p);
}


Status VectorSegmentExecutor::dispathAutoTuneTask(VectorDatasetPtr base)
{
    if (base->getDimension() != dimension)
    {
        return Status(10, "dimension of base vector no matching index");
    }
    if (base->getVectorNum() < MAX_BRUTE_FORCE_SEARCH_SIZE)
    {
        return Status(0, "base too small, no need to tune, just brute force.");
    }
    ///TODO make this user-defined
    int default_topk = 50;
    int default_query_size = 1000 < base->getVectorNum() ? 1000 : base->getVectorNum();
    Metrics default_metrics = me;
    ///These two vectors are deconstrcuted by Autotuner
    std::vector<float> * query = new std::vector<float>(default_query_size * dimension);
    /// gt_dis here is just a place holder, we don't need the data.
    std::vector<float> gt_dis(default_topk * default_query_size);
    std::vector<int64_t> * gt = new std::vector<int64_t>(default_topk * default_query_size);
    getQueryandGt(base, gt_dis.data(), gt->data(), query->data(), default_topk, default_query_size, default_metrics);
    TuningPackPtr pack = std::make_shared<TuningPack>(index, query, gt, default_topk, default_query_size, false);
    Autotuner * tuner = Autotuner::getInstance();
    tuner->addTask(segment_id.getFullPath(), pack);
    return Status();
}

/// base here is just used as query, not search base.
Status VectorSegmentExecutor::tune(VectorDatasetPtr base, std::vector<int64_t> & empty_ids, size_t current_round_start_row)
{
    if (auto * ivfflat = dynamic_cast<IVFFlatIndex *>(index.get()))
    {
        int default_query_size = std::min(base->getVectorNum(), (int64_t)2000);
        int default_topk = 50;
        int dimension = base->getDimension();
        if (default_query_size < 2000)
        {
            LOG_WARNING(log, "Not enough data points to train this datapart.");
            return Status();
        }
        std::vector<float> remove_empty;
        size_t non_empty_size = 0;
        auto empty_id = empty_ids.begin();
        /// we need to filter out empty data because there are just 0.
        for (int i = 0; i < default_query_size; i++)
        {
            if (empty_id != empty_ids.end() && i == *empty_id - current_round_start_row)
            {
                empty_id++;
                continue;
            }
            remove_empty.insert(remove_empty.end(), base->getData() + i * dimension, base->getData() + (i + 1) * dimension);
            non_empty_size++;
        }

        VectorDatasetPtr non_empty = std::make_shared<VectorDataset>(non_empty_size, dimension, remove_empty.data());
        ivfflat->tune(non_empty, default_topk);
    }
    return Status();
}


///if the autoTuning has finished, we try to get the points from the disk
///and load them into op_points
Status VectorSegmentExecutor::getOps()
{
    if (op_points == nullptr)
    {
        DiskIOReader reader;
        ///TODO this should be changed to checking hash of file
        if (reader.open(segment_id.getFullPath() + PARAMETER_PACK_NAME))
        {
            op_points = std::make_shared<std::vector<std::pair<float, OperatingPointPtr>>>();
            LOG_INFO(log, "{} successfully found the parameters pack", segment_id.getFullPath());
            int64_t bin_size;
            reader.read(&bin_size, sizeof(bin_size));
            reader.seekg(sizeof(bin_size));
            BinaryPtr ops_bin = std::make_shared<Binary>();
            ops_bin->data = new uint8_t[bin_size];
            ops_bin->size = bin_size;
            reader.read(ops_bin->data, bin_size);
            std::string params(reinterpret_cast<const char *>(ops_bin->data));
            LOG_TRACE(log, "param: {}", params);
            AccParametersPack pack = StringToAccParametersPack(params, log);
            for (auto & m : pack)
            {
                OperatingPointPtr op = std::make_shared<OperatingPoint>();
                for (auto & one_parameter : m.second)
                {
                    op->insertPoint(one_parameter.first, one_parameter.second);
                }
                LOG_INFO(log, "{} added op point {}", segment_id.getFullPath(), m.first);
                op_points->emplace_back(std::make_pair(m.first, op));
            }
            std::sort(
                op_points->begin(),
                op_points->end(),
                [](const std::pair<float, OperatingPointPtr> & a, const std::pair<float, OperatingPointPtr> & b) {
                    return a.first < b.first;
                });
        }
        else
        {
            LOG_WARNING(log, "The tuning for this index haven't finished.");
        }
    }
    else
    {
        LOG_INFO(log, "op points already exist");
    }
    return Status();
}

uint32_t VectorSegmentExecutor::compressWithCheckSum(uint8_t * source, size_t size, BinaryPtr des)
{
    //DB::WriteBuffer out(des,size);
    DB::CompressionCodecPtr codec = DB::CompressionCodecFactory::instance().get(cmb);
    size_t decompressed_size = size;
    des->data = new uint8_t[codec->getCompressedReserveSize(decompressed_size)];
    uint32_t size_compressed
        = codec->compress(reinterpret_cast<const char *>(source), decompressed_size, reinterpret_cast<char *>(des->data));
    /// although we preallocated much more memory than needed, this is the amount actually need to get
    /// serialized
    des->size = size_compressed;
    return size_compressed;
}

uint32_t VectorSegmentExecutor::validateAndDecompress(const BinaryPtr source, size_t uncompressed_size, uint8_t * des)
{
    uint8_t method = DB::ICompressionCodec::readMethod(reinterpret_cast<const char *>(source->data));
    //    if(method==static_cast<const UInt8>(DB::CompressionMethodByte::NONE)){
    //        ///if no compression,don't decompress, just point des to source
    //        des.swap(source);
    //        des->data = &des->data[DB::ICompressionCodec::getHeaderSize()];
    //        des->size-= DB::ICompressionCodec::getHeaderSize();
    //        return des->size;
    //    }
    DB::CompressionCodecPtr codec = DB::CompressionCodecFactory::instance().get(method);

    uint32_t size_decompressed
        = codec->decompress(reinterpret_cast<const char *>(source->data), source->size, reinterpret_cast<char *>(des));

    LOG_DEBUG(log, "[validateAndDecompress] decompressed size: {}", size_decompressed);
    
    if (uncompressed_size != size_decompressed)
    {
        LOG_ERROR(
            log, "The binary is corrupted, decompressed size: {}, recorded decompressed sized: {}", size_decompressed, uncompressed_size);
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "vector index on disk is corrupted");
    }
    return size_decompressed;
}

Status VectorSegmentExecutor::cancelBuild()
{
    ///TODO implement
    return Status();
}

Status VectorSegmentExecutor::removeByIds(int64_t n, int64_t * ids)
{
    LOG_INFO(log, "need to remove {} ids", n);
    int64_t removed = 0;
    for (int64_t i = 0; i < n; i++)
    {
        if (delete_bitmap->test(ids[i]))
        {
            removed++;
            delete_bitmap->unset(ids[i]);
        }
    }
    LOG_INFO(log, "removed {} ids", removed);
    if (removed == n)
    {
        return Status();
    }
    else
    {
        return Status(10, "error during remove by id, removed item num: " + ItoS(removed) + ", needed to remove num:" + ItoS(n));
    }
}

bool VectorSegmentExecutor::writeBitMap()
{
    DiskIOWriter bit_map_writer;
    String bitMap_path = segment_id.getBitMapFilePath() + VECTOR_INDEX_FILE_SUFFIX;
    bit_map_writer.open(bitMap_path, false);

    int64_t total_vec = delete_bitmap->size;
    int64_t byte_count = (total_vec >> 3) + 1;
    bit_map_writer.write(&byte_count, sizeof(int64_t));

    bit_map_writer.write(delete_bitmap->bitmap, byte_count);
    bit_map_writer.close();

    return true;
}

bool VectorSegmentExecutor::readBitMap()
{
    DiskIOReader bit_map_reader;
    String read_file_path = segment_id.getBitMapFilePath();

    if (!bit_map_reader.open(read_file_path + VECTOR_INDEX_FILE_SUFFIX))
    {
        if (!bit_map_reader.open(read_file_path))
        {
            return false;
        }
    }

    int64_t bit_map_size;
    bit_map_reader.read(&bit_map_size, sizeof(int64_t));
    if (bit_map_size != (total_vec >> 3) + 1)
    {
        LOG_ERROR(log, "bitmap file {} is corrupted: bit_map_size {}, total_vec {}", read_file_path, bit_map_size, total_vec);
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "vector index bitmap on disk is corrupted");
    }

    if (delete_bitmap == nullptr)
        delete_bitmap = std::make_shared<GeneralBitMap>(total_vec);

    bit_map_reader.seekg(sizeof(int64_t));
    bit_map_reader.read(delete_bitmap->bitmap, bit_map_size);

    return true;
}

void VectorSegmentExecutor::setCacheManagerSizeInBytes(size_t size)
{
    CacheManager::setCacheSize(size);
}

void VectorSegmentExecutor::setSerializeSegmentSize(size_t size)
{
    optimal_segment_size = size > MIN_SEGMENT_SIZE ? size : MIN_SEGMENT_SIZE;
}

bool VectorSegmentExecutor::compareVectorIndexParameters(IndexType t1, Parameters p1, IndexType t2, Parameters p2)
{
    Metrics me = L2;
    IndexMode mode = CPU;
    char cmb = static_cast<uint8_t>(DB::CompressionMethodByte::NONE);
    if (p1.contains("metric_type"))
    {
        me = VectorIndexFactory::createIndexMetrics(p1.at("metric_type"));
        p1.erase("metric_type");
    }
    if (p1.contains("mode"))
    {
        mode = VectorIndexFactory::createIndexMode(p1.at("mode"));
        p1.erase("mode");
    }
    if (p1.contains("compression_scheme"))
    {
        cmb = DB::CompressionCodecFactory::instance().get(p1.at("compression_scheme"), {})->getMethodByte();
        p1.erase("compression_scheme");
    }

    Metrics me2 = L2;
    IndexMode mode2 = CPU;
    char cmb2 = static_cast<uint8_t>(DB::CompressionMethodByte::NONE);
    if (p2.contains("metric_type"))
    {
        me2 = VectorIndexFactory::createIndexMetrics(p2.at("metric_type"));
        p2.erase("metric_type");
    }
    if (p2.contains("mode"))
    {
        mode2 = VectorIndexFactory::createIndexMode(p2.at("mode"));
        p2.erase("mode");
    }
    if (p2.contains("compression_scheme"))
    {
        cmb2 = DB::CompressionCodecFactory::instance().get(p2.at("compression_scheme"), {})->getMethodByte();
        p2.erase("compression_scheme");
    }

    if (cmb != cmb2)
    {
        return false;
    }
    int dimension = 1;
    if (VectorIndexFactory::typeToString(t1).find("PQ") != -1 || VectorIndexFactory::typeToString(t2).find("PQ") != -1)
    {
        dimension = -1;
    }
    VectorIndexPtr index1 = VectorIndexFactory::createIndex(t1, mode, me, dimension, p1);
    VectorIndexPtr index2 = VectorIndexFactory::createIndex(t2, mode2, me2, dimension, p2);
    return (index1->compare(*index2));
}

std::list<std::pair<CacheKey, Parameters>> VectorSegmentExecutor::getAllCacheNames()
{
    ///from this list, we get <segment_id, vectorindex description> pair
    return CacheManager::getInstance()->getAllItems();
}

void VectorSegmentExecutor::readTotalVec()
{
    DiskIOReader reader;
    String path = segment_id.getFullPath() + "_" + ItoS(0) + VECTOR_INDEX_FILE_SUFFIX;
    if (!reader.open(path))
    {
        LOG_ERROR(log, "failed to open {}", path);
        throw IndexException(DB::ErrorCodes::CORRUPTED_DATA, "failed to open " + path);
    }

    reader.seekg(sizeof(int64_t) * 3);
    reader.read(&total_vec, sizeof(total_vec));
    LOG_DEBUG(log, "[readTotalVec] total vectors read: {}", total_vec);
}

void VectorSegmentExecutor::updateBitMap(const std::vector<UInt64>& deleted_row_ids)
{
    if (segment_id.fromMergedParts())
        return;

    readTotalVec();

    /// Read the delete bitmap
    if (!readBitMap())
    {
        LOG_WARNING(log, "[updateBitMap] Skip to update not readable vector bitMap file part {}", segment_id.current_part_name);
        return;
    }

    /// Map new deleted row ids to row ids in old part and update delete bitmap
    bool need_update = false;
    for (auto & del_row_id : deleted_row_ids)
    {
        if (delete_bitmap->test(del_row_id))
        {
            delete_bitmap->unset(del_row_id);

            if (!need_update)
                need_update = true;
        }
    }

    if (!need_update)
        return;

    /// Flush the updated delete bitmap to disk
    writeBitMap();

    /// Update bitmap in cache if exists
    CacheManager * mgr = CacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();

    IndexWithMetaPtr cache_index = mgr->get(cache_key);
    if (cache_index)
        cache_index->setDeleteBitmap(delete_bitmap);
}

void VectorSegmentExecutor::updateMergedBitMap(const std::vector<UInt64>& deleted_row_ids)
{
    if (!segment_id.fromMergedParts())
        return;

    readTotalVec();

    /// Read the delete bitmap
    if (!readBitMap())
    {
        LOG_WARNING(log, "[updateMergedBitMap] Skip to update not readable vector bitMap file for segement: merged part {} in decouple part {}", segment_id.owner_part_name, segment_id.current_part_name);
        return;
    }

    /// Call handleMergedMaps() to get inverted_row_ids_map and inverted_row_sources_map
    try
    {
        handleMergedMaps();
    }
    catch(const DB::Exception & e)
    {
        LOG_DEBUG(log, "[updateMergedBitMap] Skip to update vector bitmap due to failure when read inverted row ids map entries, error: {}", e.what());
        return;
    }

    /// Map new deleted row ids to row ids in old part and update delete bitmap
    bool need_update = false;
    for (auto & new_row_id : deleted_row_ids)
    {
        if (segment_id.getOwnPartId() == (*inverted_row_sources_map)[new_row_id])
        {
            UInt64 old_row_id = (*inverted_row_ids_map)[new_row_id];
            if (delete_bitmap->test(old_row_id))
            {
                delete_bitmap->unset(old_row_id);

                if (!need_update)
                    need_update = true;
            }
        }
    }

    if (!need_update)
        return;

    /// Flush the updated delete bitmap to disk
    writeBitMap();

    /// Update bitmap in cache if exists
    CacheManager * mgr = CacheManager::getInstance();
    CacheKey cache_key = segment_id.getCacheKey();

    IndexWithMetaPtr cache_index = mgr->get(cache_key);
    if (cache_index)
        cache_index->setDeleteBitmap(delete_bitmap);
}


}
