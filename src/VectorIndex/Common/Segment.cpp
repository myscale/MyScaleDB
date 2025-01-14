#include <Core/ServerSettings.h>

#include <SearchIndex/IndexDataFileIO.h>
#include <VectorIndex/Common/BruteForceSearch.h>
#include <VectorIndex/Common/SegmentsMgr.h>
#include "Segment.h"
#include "TempVIHolder.h"
#include "SegmentStatus.h"

namespace VectorIndex
{
/// index properties key
const String MEMORY_USAGE_BYTES = "memory_usage_bytes";
const String DISK_USAGE_BYTES = "disk_usage_bytes";

UInt8 BaseSegment::max_threads = getNumberOfPhysicalCPUCores();
BaseSegment::Metadata BaseSegment::generateVIMetadata(const IMergeTreeDataPart & data_part, const VIDescription & vec_desc)
{
    auto metadata_snapshot = data_part.storage.getInMemoryMetadataPtr();
    auto merge_tree_setting = data_part.storage.getSettings();
    BaseSegment::Metadata meta;
    meta.total_vec = data_part.rows_count;
    meta.dimension = getVectorDimension(vec_desc.vector_search_type, *metadata_snapshot, vec_desc.column);
    checkVectorDimension(vec_desc.vector_search_type, meta.dimension);
    meta.build_params = convertPocoJsonToMap(vec_desc.parameters);
    meta.build_params.erase("metric_type");

    /// dynamic index type, when total_vec is small, build flat index
    if (meta.total_vec * getEachVectorBytes(vec_desc.vector_search_type, meta.dimension)
        < merge_tree_setting->min_bytes_to_build_vector_index)
    {
        /// fallback to flat index, need clean build params
        /// due to flat index could not support other index params
        meta.index_type = fallbackToFlat(vec_desc.vector_search_type);
        meta.fallback_to_flat = true;
        meta.build_params.clear();
    }
    else
    {
        meta.index_type = Search::getVectorIndexType(vec_desc.type, vec_desc.vector_search_type);
    }

    /// init metric type, when not set, use default metric type in merge tree settings
    String index_metric_str;
    if (vec_desc.parameters && vec_desc.parameters->has("metric_type"))
        index_metric_str = vec_desc.parameters->getValue<String>("metric_type");
    else
    {
        if (vec_desc.vector_search_type == Search::DataType::FloatVector)
            index_metric_str = merge_tree_setting->float_vector_search_metric_type;
        else
            index_metric_str = merge_tree_setting->binary_vector_search_metric_type;
    }
    meta.index_metric = Search::getMetricType(index_metric_str, vec_desc.vector_search_type);

    /// init disk mode, when not set, use default disk mode in merge tree settings
    if (meta.index_type == VIType::MSTG)
    {
        if (!meta.build_params.contains("disk_mode"))
            meta.build_params.setParam("disk_mode", merge_tree_setting->default_mstg_disk_mode);
    }

    /// set version
    meta.index_version = "1.0.0";
    return meta;
}

BaseSegment::Metadata BaseSegment::Metadata::readFromBuffer(ReadBuffer & buf)
{
    BaseSegment::Metadata meta;
    assertString("vector index metadata format version: 1\n", buf);
    assertString("num_segments: 1\n", buf);

    assertString("version: ", buf);
    readString(meta.index_version, buf);
    assertChar('\n', buf);

    assertString("type: ", buf);
    String type_str;
    readString(type_str, buf);
    Search::findEnumByName(type_str, meta.index_type);
    assertChar('\n', buf);

    assertString("metric: ", buf);
    String metric_str;
    readString(metric_str, buf);
    Search::findEnumByName(metric_str, meta.index_metric);
    assertChar('\n', buf);

    assertString("dimension: ", buf);
    readIntText(meta.dimension, buf);
    assertChar('\n', buf);

    assertString("total_vec: ", buf);
    readIntText(meta.total_vec, buf);
    assertChar('\n', buf);

    assertString("fallback_to_flat: ", buf);
    readBoolText(meta.fallback_to_flat, buf);
    assertChar('\n', buf);

    /// no use for now, keep for compatibility
    String current_part_name;
    assertString("current_part_name: ", buf);
    readString(current_part_name, buf);
    assertChar('\n', buf);

    /// no use for now, keep for compatibility
    String owner_part_name;
    assertString("owner_part_name: ", buf);
    readString(owner_part_name, buf);
    assertChar('\n', buf);

    /// no use for now, keep for compatibility
    String vector_index_name;
    assertString("vector_index_name: ", buf);
    readString(vector_index_name, buf);
    assertChar('\n', buf);

    /// no use for now, keep for compatibility
    String column_name;
    assertString("column_name: ", buf);
    readString(column_name, buf);
    assertChar('\n', buf);

    /// no use for now, keep for compatibility
    UInt8 owner_part_id;
    assertString("owner_part_id: ", buf);
    readIntText(owner_part_id, buf);
    assertChar('\n', buf);

    String key;
    String value;

    size_t num_params = 0;
    assertString("num_params: ", buf);
    readIntText(num_params, buf);
    assertChar('\n', buf);

    for (size_t i = 0; i < num_params; i++)
    {
        readBackQuotedStringWithSQLStyle(key, buf);
        assertChar(' ', buf);
        readString(value, buf);
        assertChar('\n', buf);
        meta.build_params.setParam(key, value);
    }

    size_t num_infos = 0;
    assertString("num_infos: ", buf);
    readIntText(num_infos, buf);
    assertChar('\n', buf);

    for (size_t i = 0; i < num_infos; i++)
    {
        readBackQuotedStringWithSQLStyle(key, buf);
        assertChar(' ', buf);
        readString(value, buf);
        assertChar('\n', buf);
        meta.properties[key] = value;
    }

    assertEOF(buf);
    return meta;
}

void BaseSegment::Metadata::writeToBuffer(const BaseSegment::Metadata & meta, WriteBuffer & buf)
{
    writeString("vector index metadata format version: 1\n", buf);
    writeString("num_segments: 1\n", buf);

    writeString("version: ", buf);
    writeString(meta.index_version, buf);
    writeChar('\n', buf);

    writeString("type: ", buf);
    writeString(Search::enumToString(meta.index_type), buf);
    writeChar('\n', buf);

    writeString("metric: ", buf);
    writeString(Search::enumToString(meta.index_metric), buf);
    writeChar('\n', buf);

    writeString("dimension: ", buf);
    writeIntText(meta.dimension, buf);
    writeChar('\n', buf);

    writeString("total_vec: ", buf);
    writeIntText(meta.total_vec, buf);
    writeChar('\n', buf);

    writeString("fallback_to_flat: ", buf);
    writeBoolText(meta.fallback_to_flat, buf);
    writeChar('\n', buf);

    String current_part_name = "";
    writeString("current_part_name: ", buf);
    writeString(current_part_name, buf);
    writeChar('\n', buf);

    String owner_part_name = "";
    writeString("owner_part_name: ", buf);
    writeString(owner_part_name, buf);
    writeChar('\n', buf);

    String vector_index_name = "";
    writeString("vector_index_name: ", buf);
    writeString(vector_index_name, buf);
    writeChar('\n', buf);

    String column_name = "";
    writeString("column_name: ", buf);
    writeString(column_name, buf);
    writeChar('\n', buf);

    UInt8 owner_part_id = 0;
    writeString("owner_part_id: ", buf);
    writeIntText(owner_part_id, buf);
    writeChar('\n', buf);

    writeString("num_params: ", buf);
    writeIntText(meta.build_params.size(), buf);
    writeChar('\n', buf);

    for (const auto & it : meta.build_params)
    {
        writeBackQuotedString(it.first, buf);
        writeChar(' ', buf);
        writeString(it.second, buf);
        writeChar('\n', buf);
    }

    writeString("num_infos: ", buf);
    writeIntText(meta.properties.size(), buf);
    writeChar('\n', buf);

    for (const auto & it : meta.properties)
    {
        writeBackQuotedString(it.first, buf);
        writeChar(' ', buf);
        writeString(it.second, buf);
        writeChar('\n', buf);
    }
}

bool BaseSegment::canMergeForSegs(const SegmentPtr & left_seg, const SegmentPtr & right_seg)
{
    if (!left_seg && !right_seg)
        return true;
    if (!left_seg || !right_seg)
        return false;
    if (left_seg->isDecoupled() || right_seg->isDecoupled())
        return false;
    auto left_seg_status = left_seg->getSegmentStatus()->getStatus();
    auto right_seg_status = right_seg->getSegmentStatus()->getStatus();
    if (left_seg_status == SegmentStatus::BUILDING || right_seg_status == SegmentStatus::BUILDING)
        return false;
    if (left_seg_status <= SegmentStatus::PENDING && right_seg_status <= SegmentStatus::PENDING)
        return true;
    else if (
        (left_seg_status <= SegmentStatus::PENDING && right_seg_status == SegmentStatus::ERROR)
        || (right_seg_status <= SegmentStatus::PENDING && left_seg_status == SegmentStatus::ERROR))
        return true;
    else if (left_seg_status == right_seg_status)
        return true;
    else if (
        (left_seg_status == SegmentStatus::SMALL_PART && right_seg_status == SegmentStatus::BUILT)
        || (right_seg_status == SegmentStatus::SMALL_PART && left_seg_status == SegmentStatus::BUILT))
        return true;

    return false;
}

BaseSegment::BaseSegment(
    const VIDescription & vec_desc_,
    const MergeTreeDataPartWeakPtr part,
    const MergeTreeDataPartChecksumsPtr & vi_checksums_,
    const String & owner_part_name_,
    UInt8 owner_part_id_)
    : vec_desc(vec_desc_)
    , data_part(part)
    , vi_status(std::make_shared<SegmentStatus>(vi_checksums_ ? SegmentStatus::BUILT : SegmentStatus::PENDING))
    , owner_part_name(owner_part_name_)
    , owner_part_id(owner_part_id_)
    , vi_metadata(Metadata())
    , vi_checksum(vi_checksums_)
{
    try
    {
        VECTOR_INDEX_EXCEPTION_ADAPT(
        {
            auto lock_part = part.lock();
            if (!lock_part)
                throw VIException(ErrorCodes::LOGICAL_ERROR, "Data part is expired.");
            if (lock_part->isSmallPart())
            {
                vi_status->setStatus(SegmentStatus::SMALL_PART);
            }
            else if (vi_checksums_)
            {
                /// with ready vi, init vi metadata from description file
                String desc_file_path = getVectorDescriptionFilePath();
                auto read_buf = lock_part->getDataPartStoragePtr()->readFile(desc_file_path, {}, {}, {});
                vi_metadata = Metadata::readFromBuffer(*read_buf);
                size_t memory_usage_bytes = vi_metadata.properties.find(MEMORY_USAGE_BYTES) != vi_metadata.properties.end()
                    ? std::stol(vi_metadata.properties[MEMORY_USAGE_BYTES])
                    : 0;
                vi_memory_metric_increment.changeTo(memory_usage_bytes);
            }
            else
            {
                /// with no ready vi, init vi metadata from part
                vi_metadata = generateVIMetadata(*lock_part, vec_desc);
            }
        },
        "Init Segment")
    }
    catch (const Exception e)
    {
        LOG_DEBUG(log, "Failed to init segment, error {}: {}", e.code(), e.message());
        vi_status->setStatus(SegmentStatus::ERROR, e.message());
    }
}

bool BaseSegment::validateSegments() const
{
    if (vi_status->getStatus() != SegmentStatus::BUILT && vi_checksum == nullptr)
        return true;

    if (vi_checksum == nullptr)
    {
        LOG_ERROR(log, "Vector index checksum is nullptr, but vi is in ready, part: {}", getOwnPartName());
        return false;
    }

    auto lock_part = getDataPart();
    if (!checkConsistencyForVectorIndex(lock_part->getDataPartStoragePtr(), *vi_checksum))
    {
        LOG_ERROR(log, "Vector index checksum is not consistent, part: {}", getOwnPartName());
        return false;
    }
    /// [TODO] vi other checklists

    return true;
}

const CachedSegmentKeyList BaseSegment::getCachedSegmentKeys() const
{
    if (this->vi_status->getStatus() != SegmentStatus::BUILT)
        return {};
    auto lock_part = getDataPart();
    String part_relative_path = getPartRelativePath(fs::path(lock_part->getDataPartStorage().getFullPath()).parent_path());
    return {CachedSegmentKey{
        part_relative_path,
        cutMutVer(lock_part->name),
        cutMutVer(owner_part_name.empty() ? lock_part->name : owner_part_name),
        vec_desc.name,
        vec_desc.column}};
}

const CachedSegmentholderList BaseSegment::getCachedSegmentHolders() const
{
    CachedSegmentholderList cached_segments;
    for (const auto & cache_key : getCachedSegmentKeys())
    {
        auto cached_seg = VICacheManager::getInstance()->get(cache_key);
        if (cached_seg)
            cached_segments.emplace_back(std::move(cached_seg));
    }
    return cached_segments;
}

void BaseSegment::inheritMetadata(const SegmentPtr & old_seg)
{
    this->vi_status = old_seg->vi_status;
}

SegmentPtr BaseSegment::mutation(const MergeTreeDataPartPtr new_data_part)
{
    SegmentPtr new_segment = createSegment(vec_desc, new_data_part, this->vi_checksum, this->owner_part_name, this->owner_part_id);
    new_segment->vi_status = this->vi_status;
    new_segment->vi_metadata = this->vi_metadata;
    new_segment->vi_checksum = this->vi_checksum;
    size_t memory_usage_bytes = new_segment->vi_metadata.properties.find(MEMORY_USAGE_BYTES) != new_segment->vi_metadata.properties.end()
        ? std::stol(new_segment->vi_metadata.properties[MEMORY_USAGE_BYTES])
        : 0;
    new_segment->vi_memory_metric_increment.changeTo(memory_usage_bytes);
    return new_segment;
}

bool BaseSegment::containCachedSegmentKey(const CachedSegmentKey & cache_key) const
{
    for (const auto & seg_cache_key : getCachedSegmentKeys())
    {
        if (seg_cache_key == cache_key)
            return true;
    }
    return false;
}


void BaseSegment::removeVIFiles(bool remove_checksums_file)
{
    LOG_DEBUG(log, "Remove vector index files for single part: {}", getOwnPartName());
    if (vi_checksum == nullptr)
    {
        LOG_DEBUG(log, "Vector index checksum is nullptr, no vi files need to be remove, part: {}", getOwnPartName());
        return;
    }
    const String description_file_suffix = String(VECTOR_INDEX_DESCRIPTION) + VECTOR_INDEX_FILE_SUFFIX;
    const String row_ids_map_file_suffix = String("row_ids_map") + VECTOR_INDEX_FILE_SUFFIX;
    const String inverted_row_ids_map_file_suffix = String("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX;
    const String inverted_row_source_map_file_suffix = String("merged-inverted_row_sources_map") + VECTOR_INDEX_FILE_SUFFIX;
    try
    {
        auto lock_part = getDataPart();
        /// first remove description file
        for (const auto & [file, _] : vi_checksum->files)
        {
            if (endsWith(file, description_file_suffix))
            {
                LOG_DEBUG(log, "Remove vector index description file: {}", file);
                const_cast<IDataPartStorage &>(lock_part->getDataPartStorage()).removeFileIfExists(file);
            }
        }
        /// remove other vector index files
        for (const auto & [file, _] : vi_checksum->files)
        {
            if (!endsWith(file, VECTOR_INDEX_FILE_SUFFIX) || endsWith(file, row_ids_map_file_suffix)
                || endsWith(file, inverted_row_ids_map_file_suffix) || endsWith(file, inverted_row_source_map_file_suffix))
                continue;
            LOG_DEBUG(log, "Remove vector index file: {}", file);
            const_cast<IDataPartStorage &>(lock_part->getDataPartStorage()).removeFileIfExists(file);
        }
        /// remove checksums file, if new seg onlined, we don't need to remove checksums file
        if (remove_checksums_file)
        {
            const String checksum_file_name = getVectorIndexChecksumsFileName(vec_desc.name);
            LOG_DEBUG(log, "Remove vector index checksum file: {}", checksum_file_name);
            const_cast<IDataPartStorage &>(lock_part->getDataPartStorage()).removeFileIfExists(checksum_file_name);
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to remove vi file, error {}: {}", e.code(), e.message());
    }
}

String BaseSegment::getOwnPartName() const
{
    if (owner_part_name.empty())
    {
        auto lock_part = getDataPart();
        return lock_part->name;
    }
    return owner_part_name;
}

template <Search::DataType data_type>
CachedSegmentPtr SimpleSegment<data_type>::prepareVIEntry(std::optional<Metadata> vi_metadata_)
{
    auto lock_part = data_part.lock();
    Metadata metadata = vi_metadata_.has_value() ? vi_metadata_.value() : vi_metadata;
    String vector_index_cache_prefix
        = getVectorIndexCachePrefix(lock_part->getDataPartStoragePtr()->getRelativePath(), getOwnPartName(), vec_desc.name);

    auto vi_cache_path_holder = TempVIHolder::instance().lockCachePath(vector_index_cache_prefix);

    LOG_DEBUG(
        log,
        "Create vector index {}, type: {}, metric: {}, dimension: {}, total_vec: {}, path: {}",
        vec_desc.name,
        Search::enumToString(metadata.index_type),
        Search::enumToString(metadata.index_metric),
        metadata.dimension,
        metadata.total_vec,
        vector_index_cache_prefix);
    InnerSegmentVariantPtr index = Search::createVectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, data_type>(
        vec_desc.name,
        metadata.index_type,
        metadata.index_metric,
        metadata.dimension,
        metadata.total_vec,
        metadata.build_params,
        BaseSegment::max_threads,
        vector_index_cache_prefix,
        [base_daemon = BaseDaemon::tryGetInstance()]()
        {
            if (base_daemon.has_value())
                return base_daemon.value().get().isCancelled();
            return false;
        });

    auto delete_bitmap = std::make_shared<VIBitmap>(metadata.total_vec, true);
    return std::make_shared<CachedSegment>(index, delete_bitmap, metadata.fallback_to_flat, std::make_shared<VIDescription>(vec_desc), vi_cache_path_holder);
}

template <Search::DataType data_type>
typename SimpleSegment<data_type>::VIDataReaderPtr SimpleSegment<data_type>::getVIDataReader(std::function<bool()> cancel_callback)
{
    auto lock_part = data_part.lock();
    NamesAndTypesList cols;
    auto metadate_snapshot = lock_part->storage.getInMemoryMetadataPtr();
    auto col_and_type = metadate_snapshot->getColumns().getAllPhysical().tryGetByName(vec_desc.column);
    bool enforce_fixed_array = lock_part->storage.getSettings()->enforce_fixed_vector_length_constraint;
    if (col_and_type)
    {
        cols.emplace_back(*col_and_type);
    }
    else
    {
        LOG_WARNING(log, "Found column {} in part and VIDescription, but not in metadata snapshot.", vec_desc.column);
        throw VIException(ErrorCodes::CORRUPTED_DATA, "Column was not found in the metadata snapshot.");
    }
    auto metadata_snapshot = lock_part->storage.getInMemoryMetadataPtr();
    size_t dimension = getVectorDimension(vec_desc.vector_search_type, *metadata_snapshot, vec_desc.column);
    return std::make_shared<VIPartReader<data_type>>(
        lock_part,
        cols,
        metadate_snapshot,
        lock_part->storage.getContext()->getMarkCache().get(),
        cancel_callback,
        dimension,
        enforce_fixed_array);
}

template <Search::DataType data_type>
CachedSegmentPtr
SimpleSegment<data_type>::buildVI(bool slow_mode, VIBuildMemoryUsageHelper & build_memory_lock, std::function<bool()> cancel_build_callback)
{
    auto lock_part = data_part.lock();
    OpenTelemetry::SpanHolder span("VectorExecutor::buildIndex");
    auto tmp_vi_entry = prepareVIEntry();
    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config);

    size_t add_block_size = server_settings.max_build_index_add_block_size;
    size_t train_block_size = 0;
    if constexpr (data_type == Search::DataType::FloatVector)
        train_block_size = server_settings.max_build_index_train_block_size;
    else
        train_block_size = server_settings.max_build_binary_vector_index_train_block_size;

    auto num_threads = BaseSegment::max_threads;
    if (slow_mode)
        num_threads = num_threads / 2;
    if (num_threads == 0)
        num_threads = 1;

    SimpleSegment<data_type>::VIPtr index_ptr = std::get<SimpleSegment<data_type>::VIPtr>(tmp_vi_entry->index);
    SimpleSegment<data_type>::VIDataReaderPtr part_read_ptr = getVIDataReader(cancel_build_callback);
    if (!part_read_ptr || !index_ptr)
        throw VIException(ErrorCodes::LOGICAL_ERROR, "Index ptr or part reader ptr is nullptr, it's a bug!");

    index_ptr->setTrainDataChunkSize(train_block_size);
    index_ptr->setAddDataChunkSize(add_block_size);
    build_memory_lock.checkBuildMemory(index_ptr->getResourceUsage().build_memory_usage_bytes);

    printMemoryInfo(log, "Before build");
    index_ptr->build(part_read_ptr.get(), num_threads, cancel_build_callback);
    printMemoryInfo(log, "After build");
    return tmp_vi_entry;
}

template <Search::DataType data_type>
void SimpleSegment<data_type>::serialize(
    DiskPtr disk,
    const CachedSegmentPtr vi_entry,
    const String & serialize_local_folder,
    MergeTreeDataPartChecksumsPtr & vector_index_checksum,
    VIBuildMemoryUsageHelper & /*build_memory_lock*/)
{
    auto lock_part = data_part.lock();
    auto index_serialize_folder = fs::path(serialize_local_folder) / std::string(vec_desc.name + "-");
    auto index_checksums = std::make_shared<MergeTreeDataPartChecksums>();
    LOG_INFO(log, "index serialize path: {}", index_serialize_folder);
    auto file_writer = Search::IndexDataFileWriter<VectorIndexOStream>(
        index_serialize_folder,
        [&](const std::string & name, std::ios::openmode /*mode*/)
        { return std::make_shared<VectorIndexWriter>(disk, name, index_checksums); });

    SimpleSegment<data_type>::VIPtr index_ptr = std::get<SimpleSegment<data_type>::VIPtr>(vi_entry->index);

    index_ptr->serialize(&file_writer);
    index_ptr->saveDataID(&file_writer);

    printMemoryInfo(log, "After serialization");

    String index_version = index_ptr->getVersion().toString();
    auto usage = index_ptr->getResourceUsage();
    String memory_usage = std::to_string(usage.memory_usage_bytes);
    String disk_usage = std::to_string(usage.disk_usage_bytes);

    LOG_INFO(
        log,
        "Index type: {}, version: {}, memory_usage_bytes: {}, disk_usage_bytes: {}",
        Search::enumToString(vi_metadata.index_type),
        index_version,
        memory_usage,
        disk_usage);
    std::unordered_map<std::string, std::string> properties;

    properties[MEMORY_USAGE_BYTES] = memory_usage;
    properties[DISK_USAGE_BYTES] = disk_usage;

    vi_metadata.properties = properties;
    vi_metadata.index_version = index_version;

    auto buf = disk->writeFile(index_serialize_folder.string() + VECTOR_INDEX_DESCRIPTION + VECTOR_INDEX_FILE_SUFFIX, 4096);

    Metadata::writeToBuffer(vi_metadata, *buf);
    buf->finalize();

    /// Calculate vector index checksums
    vector_index_checksum = std::make_shared<MergeTreeDataPartChecksums>(
        calculateVectorIndexChecksums(lock_part->getDataPartStoragePtr(), serialize_local_folder, index_checksums));

    String checksum_file_path = index_serialize_folder.string() + VECTOR_INDEX_CHECKSUMS + VECTOR_INDEX_FILE_SUFFIX;
    LOG_INFO(log, "Write {} checksum file {}", vec_desc.name, checksum_file_path);

    auto out_checksums = disk->writeFile(checksum_file_path, 4096);
    vector_index_checksum->write(*out_checksums);
    out_checksums->finalize();
}

template <Search::DataType data_type>
CachedSegmentHolderPtr SimpleSegment<data_type>::loadVI(const MergeIdMapsPtr & vi_merged_maps)
{
    CachedSegmentKeyList cache_keys = getCachedSegmentKeys();
    if (cache_keys.empty())
    {
        LOG_DEBUG(log, "Segment ids is empty, cannot load vector index.");
        return nullptr;
    }

    OpenTelemetry::SpanHolder span("VectorExecutor::load");
    VICacheManager * lru_cache = VICacheManager::getInstance();
    const CachedSegmentKey & cache_key = cache_keys.front();
    const String cache_key_str = cache_key.toString();
    LOG_DEBUG(log, "cache_key_str = {}", cache_key_str);
    auto index_holder = lru_cache->get(cache_key);

    if (!index_holder)
    {
        auto lock_part = data_part.lock();
        auto write_event_log = [&](VIEventLogElement::Type event, int code = 0, String msg = "")
        {
            VIEventLog::addEventLog(
                Context::getGlobalContextInstance(),
                lock_part,
                vec_desc.name,
                event,
                cache_key.part_name_no_mutation,
                ExecutionStatus(code, msg));
        };
        write_event_log(VIEventLogElement::LOAD_START);

        auto load_func = [&]() -> CachedSegmentPtr
        {
            LOG_INFO(log, "loading vector index from {}", getSegmentFullPath());
            auto disk = getDiskFromLockPart();
            if (getDataPart()->getState() != MergeTreeDataPartState::Active)
                throw Exception(ErrorCodes::INVALID_VECTOR_INDEX, "Part is inactive, will not reload index!");

            if (!disk->exists(getVectorDescriptionFilePath()))
                throw VIException(
                    ErrorCodes::CORRUPTED_DATA,
                    "Does not exists {}, Index is not in the ready state and cannot be loaded",
                    getVectorDescriptionFilePath());

            auto check_index_expired = [this, disk]() mutable -> bool { return !disk->exists(getVectorDescriptionFilePath()); };

            CachedSegmentPtr vi_cache_entry = prepareVIEntry(vi_metadata);
            auto file_reader = Search::IndexDataFileReader<VectorIndexIStream>(
                getSegmentFullPath(),
                [disk](const std::string & name, std::ios::openmode /*mode*/)
                { return std::make_shared<VectorIndexReader>(disk, name); });

            printMemoryInfo(log, "Before load");
            const auto & index_ptr = std::get<SimpleSegment<data_type>::VIPtr>(vi_cache_entry->index);
            index_ptr->load(&file_reader, check_index_expired);
            index_ptr->loadDataID(&file_reader);
            printMemoryInfo(log, "After load");

            if (vi_metadata.total_vec != static_cast<size_t>(index_ptr->numData()))
                throw VIException(
                    ErrorCodes::CORRUPTED_DATA,
                    "Index total vec is not correct, real {}, need {}",
                    index_ptr->numData(),
                    vi_metadata.total_vec);

            LOG_INFO(log, "load total_vec={}", index_ptr->numData());

            auto del_row_ids = getDataPart()->getDeleteBitmapFromRowExists();

            VIBitmapPtr delete_bitmap = vi_cache_entry->getDeleteBitmap();

            if (vi_merged_maps)
            {
                vi_merged_maps->initRealFilter(owner_part_id, delete_bitmap, del_row_ids);
            }
            else
            {
                for (const auto & deL_row_id : del_row_ids)
                {
                    if (delete_bitmap->is_member(deL_row_id))
                        delete_bitmap->unset(deL_row_id);
                }
            }

            /// rechack index valid
            if (check_index_expired())
                throw VIException(
                    ErrorCodes::CORRUPTED_DATA,
                    "Does not exists {}, Index is not in the ready state and cannot be loaded",
                    getVectorDescriptionFilePath());
            return vi_cache_entry;
        };

        try
        {
            VECTOR_INDEX_EXCEPTION_ADAPT(index_holder = lru_cache->load(cache_key, load_func), "Load Index")

            if (!index_holder)
            {
                LOG_WARNING(
                    log,
                    "Fail to load vector index {}, cache key: {}, it might be that the cache size is not enough.",
                    vec_desc.name,
                    cache_key.toString());
                write_event_log(VIEventLogElement::LOAD_FAILED);
                return nullptr;
            }
            write_event_log(VIEventLogElement::LOAD_SUCCEED);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(
                log,
                "Fail to load vector index {}, cache key: {}, error {}: {}",
                vec_desc.name,
                cache_key.toString(),
                e.code(),
                e.message());
            write_event_log(VIEventLogElement::LOAD_ERROR, e.code(), e.message());
            return nullptr;
        }
    }
    return index_holder;
}

template <Search::DataType data_type>
SearchResultPtr SimpleSegment<data_type>::searchVI(
    const VectorDatasetVariantPtr & queries,
    int32_t k,
    const VIBitmapPtr filter,
    const VIParameter & parameters,
    bool & first_stage_only,
    const MergeIdMapsPtr & vi_merged_maps_)
{
    auto vi_entry = loadVI(vi_merged_maps_);
    if (vi_entry == nullptr)
        throw VIException(ErrorCodes::VECTOR_INDEX_CACHE_LOAD_ERROR, "Index is not loaded.");
    const auto & index_ptr = std::get<SimpleSegment<data_type>::VIPtr>(vi_entry->value().index);
    first_stage_only = index_ptr->supportTwoStageSearch() && first_stage_only;
    const SimpleSegment<data_type>::DatasetPtr & query_dataset_ptr = std::get<SimpleSegment<data_type>::DatasetPtr>(queries);
    if (query_dataset_ptr->getDimension() != static_cast<int64_t>(index_ptr->dataDimension()))
        throw VIException(ErrorCodes::LOGICAL_ERROR, "The dimension of searched index and input doesn't match.");

    // Merge filter and delete_bitmap
    VIBitmapPtr delete_bitmap = vi_entry->value().getDeleteBitmap();
    VIBitmapPtr merged_filter = filter;
    if (!delete_bitmap->all())
        merged_filter = Search::DenseBitmap::intersectDenseBitmaps(merged_filter, delete_bitmap);
    VIParameter index_parameters = parameters;
    if (vi_entry->value().fallback_to_flat)
        index_parameters.clear();

    {
        OpenTelemetry::SpanHolder span_search("VectorExecutor::performSearch()::search");

        auto search_queries = std::make_shared<Search::DataSet<typename SearchIndexDataTypeMap<data_type>::IndexDatasetType>>(
            query_dataset_ptr->template getData<VectorSearchMethod::IndexSearch>(),
            query_dataset_ptr->getVectorNum(),
            query_dataset_ptr->getDimension());

        /// Limit the number of bruteforce search threads to 2 * number of physical cores
        static LimiterSharedContext vector_index_context(BaseSegment::max_threads * 2);
        ScanThreadLimiter limiter(vector_index_context, log);
        VECTOR_INDEX_EXCEPTION_ADAPT(return index_ptr->search(search_queries, k, index_parameters, first_stage_only, merged_filter.get()),
                                            "VI_Search")
    }
    UNREACHABLE();
}

template <Search::DataType data_type>
SearchResultPtr SimpleSegment<data_type>::computeTopDistanceSubset(
    const VectorDatasetVariantPtr queries, SearchResultPtr first_stage_result, int32_t top_k, const MergeIdMapsPtr & vi_merged_maps_)
{
    if constexpr (data_type != Search::DataType::FloatVector)
        throw VIException(ErrorCodes::NOT_IMPLEMENTED, "Does not implemented computeTopDistanceSubset option.");
    if (!first_stage_result)
        return nullptr;
    auto vi_entry = loadVI(vi_merged_maps_);
    if (!vi_entry)
        return first_stage_result;

    const auto & index_ptr = std::get<SimpleSegment<data_type>::VIPtr>(vi_entry->value().index);
    if (!index_ptr->supportTwoStageSearch())
        return first_stage_result;

    {
        OpenTelemetry::SpanHolder span("VectorExecutor::computeTopDistanceSubset()");
        /// Limit the number of bruteforce search threads to 2 * number of physical cores
        static LimiterSharedContext vector_index_context(BaseSegment::max_threads * 2);
        ScanThreadLimiter limiter(vector_index_context, log);

        const SimpleSegment<data_type>::DatasetPtr & query_dataset_ptr = std::get<SimpleSegment<data_type>::DatasetPtr>(queries);

        auto search_queries = std::make_shared<Search::DataSet<typename SearchIndexDataTypeMap<data_type>::IndexDatasetType>>(
            query_dataset_ptr->template getData<VectorSearchMethod::IndexSearch>(),
            query_dataset_ptr->getVectorNum(),
            query_dataset_ptr->getDimension());
        VECTOR_INDEX_EXCEPTION_ADAPT(return index_ptr->computeTopDistanceSubset(search_queries, first_stage_result, top_k),
                                            "VI_ComputeTopDistanceSubset")
    }
    UNREACHABLE();
}

template <Search::DataType data_type>
void SimpleSegment<data_type>::updateCachedBitMap(const VIBitmapPtr & bitmap)
{
    auto cache_keys = getCachedSegmentKeys();
    if (cache_keys.empty())
        return;
    auto vi_entry = VICacheManager::getInstance()->get(cache_keys.front());
    if (!vi_entry)
        return;
    auto delete_bitmap = vi_entry->value().getDeleteBitmap();
    auto real_filter = Search::DenseBitmap::intersectDenseBitmaps(bitmap, delete_bitmap);
    vi_entry->value().setDeleteBitmap(real_filter);
}

template <Search::DataType data_type>
SegmentInfoPtrList SimpleSegment<data_type>::getSegmentInfoList() const
{
    auto lock_part = getDataPart();
    return {std::make_shared<SegmentInfo>(
        vec_desc,
        *lock_part,
        this->getOwnPartName(),
        this->getOwnerPartId(),
        vi_status,
        vi_metadata.index_type,
        vi_metadata.dimension,
        vi_metadata.total_vec,
        vi_metadata.properties.find(MEMORY_USAGE_BYTES) != vi_metadata.properties.end()
            ? std::stol(vi_metadata.properties.at(MEMORY_USAGE_BYTES))
            : 0,
        vi_metadata.properties.find(DISK_USAGE_BYTES) != vi_metadata.properties.end()
            ? std::stol(vi_metadata.properties.at(DISK_USAGE_BYTES))
            : 0)};
}

/// VI Segment does not hold the vi ptr, how to check vi ptr support two stage search?
template <Search::DataType data_type>
bool SimpleSegment<data_type>::supportTwoStageSearch() const
{
    if constexpr (data_type == Search::DataType::FloatVector)
    {
        if (vi_metadata.index_type == VIType::MSTG)
            return true;
    }
    return false;
}

template <Search::DataType data_type>
void SimpleSegment<data_type>::searchWithoutIndex(
    DatasetPtr query_data, DatasetPtr base_data, int32_t k, float *& distances, int64_t *& labels, const VIMetric & metric)
{
    omp_set_num_threads(1);
    auto new_metric = metric;
    if constexpr (data_type == Search::DataType::FloatVector)
    {
        if (metric == VIMetric::Cosine)
        {
            LOG_DEBUG(getLogger("SimpleSegment"), "Normalize vectors for cosine similarity brute force search");
            new_metric = VIMetric::IP;
            query_data->normalize();
            base_data->normalize();
        }
    }
    tryBruteForceSearch<data_type>(
        query_data->template getData<VectorSearchMethod::BruteForce>(),
        base_data->template getData<VectorSearchMethod::BruteForce>(),
        query_data->getDimension(),
        k,
        query_data->getVectorNum(),
        base_data->getVectorNum(),
        labels,
        distances,
        new_metric);
    if constexpr (data_type == Search::DataType::FloatVector)
    {
        if (metric == VIMetric::Cosine)
        {
            for (int64_t i = 0; i < k * query_data->getVectorNum(); i++)
            {
                distances[i] = 1 - distances[i];
            }
        }
    }
}

template <Search::DataType data_type>
DecoupleSegment<data_type>::DecoupleSegment(
    const VIDescription & vec_desc_,
    const MergeIdMapsPtr vi_merged_map,
    const MergeTreeDataPartWeakPtr part,
    const MergeTreeDataPartChecksumsPtr & vi_checksums_)
    : SimpleSegment<data_type>(vec_desc_, part, nullptr), merged_maps(vi_merged_map)
{
    const String desc_file_suffix = getVectorIndexDescriptionFileName(vec_desc_.name);
    for (const auto & [file, _] : vi_checksums_->files)
    {
        if (!endsWith(file, desc_file_suffix))
            continue;

        auto tokens = splitString(file, '-');
        if (tokens.size() != 5)
            throw VIException(ErrorCodes::LOGICAL_ERROR, "Invalid vector index desc file name: {}", file);

        const String part_name = tokens[2];
        const UInt8 part_id = parse<UInt8>(tokens[1]);
        auto seg = std::make_shared<SimpleSegment<data_type>>(vec_desc_, part, vi_checksums_, part_name, part_id);
        segments.emplace_back(seg);
    }
}

template <Search::DataType data_type>
DecoupleSegment<data_type>::DecoupleSegment(
    const VIDescription & vec_desc_,
    const MergeIdMapsPtr vi_merged_map,
    const MergeTreeDataPartWeakPtr part,
    const std::vector<std::shared_ptr<SimpleSegment<data_type>>> & segments_)
    : SimpleSegment<data_type>(vec_desc_, part, nullptr), segments(segments_), merged_maps(vi_merged_map)
{
}

template <Search::DataType data_type>
SegmentPtr DecoupleSegment<data_type>::mutation(const MergeTreeDataPartPtr new_data_part)
{
    std::vector<std::shared_ptr<SimpleSegment<data_type>>> new_segments;
    for (const auto & old_simple_segment : this->segments)
    {
        SegmentPtr new_seg = old_simple_segment->mutation(new_data_part);
        std::shared_ptr<SimpleSegment<data_type>> new_seg_ptr = std::dynamic_pointer_cast<SimpleSegment<data_type>>(new_seg);
        if (!new_seg_ptr)
            throw VIException(ErrorCodes::LOGICAL_ERROR, "New segment is not simple segment.");
        new_segments.emplace_back(new_seg_ptr);
    }
    std::shared_ptr<DecoupleSegment<data_type>> new_decouple_seg
        = std::make_shared<DecoupleSegment<data_type>>(this->vec_desc, this->getOrInitMergeMaps(false), new_data_part, new_segments);
    new_decouple_seg->vi_status = this->vi_status;
    new_decouple_seg->vi_metadata = this->vi_metadata;
    return new_decouple_seg;
}

template <Search::DataType data_type>
void DecoupleSegment<data_type>::inheritMetadata(const SegmentPtr & old_seg)
{
    SimpleSegment<data_type>::inheritMetadata(old_seg);
    auto old_decouple_seg = std::dynamic_pointer_cast<DecoupleSegment<data_type>>(old_seg);
    if (!old_decouple_seg)
        throw VIException(ErrorCodes::LOGICAL_ERROR, "Old segment is not decouple segment.");
    this->merged_maps = old_decouple_seg->getOrInitMergeMaps(false);
}

template <Search::DataType data_type>
CachedSegmentHolderPtr DecoupleSegment<data_type>::loadVI(const MergeIdMapsPtr & /*vi_merged_maps*/)
{
    CachedSegmentHolderPtr res;
    for (const auto & segment : segments)
    {
        res = segment->loadVI(getOrInitMergeMaps());
        if (!res)
            return nullptr;
    }
    return res;
}

template <Search::DataType data_type>
SearchResultPtr DecoupleSegment<data_type>::searchVI(
    const VectorDatasetVariantPtr & queries,
    int32_t k,
    const VIBitmapPtr filter,
    const VIParameter & parameters,
    bool & first_stage_only,
    const MergeIdMapsPtr & /*vi_merged_maps_*/)
{
    std::vector<SearchResultPtr> res;
    MergeIdMapsPtr inited_merged_maps = getOrInitMergeMaps();
    bool has_first_stage_result = false;
    auto real_filters = inited_merged_maps->getRealFilters(filter);
    for (const auto & segment : segments)
    {
        bool first_stage_only_local = first_stage_only;
        VIBitmapPtr real_filter = nullptr;
        if (real_filters.contains(segment->getOwnerPartId()))
            real_filter = real_filters.at(segment->getOwnerPartId());

        /// call searchVI for each segment
        auto result = segment->searchVI(queries, k, real_filter, parameters, first_stage_only_local, inited_merged_maps);

        /// record if any segment has first stage result
        has_first_stage_result = has_first_stage_result || first_stage_only_local;
        inited_merged_maps->transferToNewRowIds(segment->getOwnerPartId(), result);
        if (result != nullptr)
            res.emplace_back(result);
    }
    /// decouple seg search result has first stage result, we don't need to sort and trim
    if (has_first_stage_result)
        return SearchResult::merge(res, this->vi_metadata.index_metric, 0, false);
    else
        return SearchResult::merge(res, this->vi_metadata.index_metric, k);
}

template <Search::DataType data_type>
SearchResultPtr DecoupleSegment<data_type>::computeTopDistanceSubset(
    VectorDatasetVariantPtr queries, SearchResultPtr first_stage_result, int32_t top_k, const MergeIdMapsPtr & /*vi_merged_maps_*/)
{
    std::vector<SearchResultPtr> res;
    MergeIdMapsPtr inited_merged_maps = getOrInitMergeMaps();
    for (const auto & segment : segments)
    {
        auto real_first_stage_result = first_stage_result;
        inited_merged_maps->transferToOldRowIds(segment->getOwnerPartId(), real_first_stage_result);
        auto result = segment->computeTopDistanceSubset(queries, real_first_stage_result, top_k, inited_merged_maps);
        inited_merged_maps->transferToNewRowIds(segment->getOwnerPartId(), result);
        if (result != nullptr)
            res.emplace_back(result);
    }
    /// Merge results and sort, because we will trim the result to top_k
    return SearchResult::merge(res, this->vi_metadata.index_metric, top_k);
}

template <Search::DataType data_type>
void DecoupleSegment<data_type>::updateCachedBitMap(const VIBitmapPtr & bitmap)
{
    if (!bitmap)
        return;
    MergeIdMapsPtr inited_merged_maps = getOrInitMergeMaps();
    auto real_filters = inited_merged_maps->getRealFilters(bitmap);
    for (const auto & segment : segments)
    {
        if (!real_filters.contains(segment->getOwnerPartId()))
            continue;
        auto real_filter = real_filters.at(segment->getOwnerPartId());
        segment->updateCachedBitMap(real_filter);
    }
}

template <Search::DataType data_type>
const CachedSegmentKeyList DecoupleSegment<data_type>::getCachedSegmentKeys() const
{
    CachedSegmentKeyList res;
    for (const auto & segment : segments)
    {
        auto cache_keys = segment->getCachedSegmentKeys();
        res.insert(res.end(), cache_keys.begin(), cache_keys.end());
    }
    return res;
}

template <Search::DataType data_type>
SegmentInfoPtrList DecoupleSegment<data_type>::getSegmentInfoList() const
{
    SegmentInfoPtrList res;
    for (const auto & segment : segments)
    {
        auto info_list = segment->getSegmentInfoList();
        res.insert(res.end(), info_list.begin(), info_list.end());
    }
    auto cur_info = SimpleSegment<data_type>::getSegmentInfoList().front();
    res.insert(res.begin(), cur_info);
    return res;
}

template class SimpleSegment<Search::DataType::FloatVector>;
template class SimpleSegment<Search::DataType::BinaryVector>;
template class DecoupleSegment<Search::DataType::FloatVector>;
template class DecoupleSegment<Search::DataType::BinaryVector>;

} // namespace VectorIndex
