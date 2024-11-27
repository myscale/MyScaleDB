#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include <sys/resource.h>

#include <Common/ErrorCodes.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/logger_useful.h>
#include <Disks/IDisk.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <IO/copyData.h>
#include <IO/HashingReadBuffer.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

#include <VectorIndex/Cache/VICacheManager.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/MergedPartNameAndId.h>
#include <VectorIndex/Interpreters/VIEventLog.h>
#include <VectorIndex/Storages/VSDescription.h>
#include <VectorIndex/Utils/VIUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_VECTOR_INDEX;
    extern const int CORRUPTED_DATA;
}

/// used to move vector indices files of one old data part
/// to new data part's path, and move vector index checksums to avoid recalculate
MergeTreeDataPartChecksums moveVectorIndexFiles(
    const bool decouple,
    const String & part_id,
    const String & part_name,
    const String & vec_index_name,
    DB::MergeTreeDataPartPtr old_data_part,
    DB::MergeTreeMutableDataPartPtr new_data_part)
{
    LOG_DEBUG(getLogger("VIUtils"), "Create hard link for vector index {} file From {} to {}.", vec_index_name, old_data_part->name, new_data_part->name);
    const auto & old_storage = old_data_part->getDataPartStorage();
    auto & new_storage = new_data_part->getDataPartStorage();

    bool both_on_disk = !old_storage.isStoredOnRemoteDisk() && !new_storage.isStoredOnRemoteDisk();
    bool same_disk = old_storage.getDiskName() == new_storage.getDiskName();

    auto old_path = old_storage.getFullPath();
    auto new_path = new_storage.getFullPath();

    DB::MergeTreeDataPartChecksums new_index_checksums;

    /// move and rename vector index files,
    /// combine vector index checksums and fill it to map
    DB::MergeTreeDataPartChecksums old_index_checksums = VectorIndex::getVectorIndexChecksums(old_data_part->getDataPartStoragePtr(), vec_index_name);

    /// No need to move when no vector index files
    if (old_index_checksums.empty())
        return new_index_checksums;
    
    const DB::DataPartStorageOnDiskBase * part_storage
        = dynamic_cast<const DB::DataPartStorageOnDiskBase *>(&new_storage);
    
    auto disk = part_storage->getDisk();

    for (const auto & [old_file_name, checksum_] : old_index_checksums.files)
    {
        /// vector index in old part doesn't exists
        if (!old_storage.exists(old_file_name))
            return new_index_checksums;

        String old_file_path = old_path + old_file_name;

        /// Use original name when not decouple (for cases when only one VPart is merged)
        String new_file_name;
        if (decouple)
            new_file_name = "merged-" + part_id + "-" + part_name + "-" + old_file_name;
        else
            new_file_name = old_file_name;

        String new_file_path = new_path + new_file_name;

        if (both_on_disk || same_disk)
        {
            /// if both saved on local disk or on same remote fs, just call fs::rename to move files
            disk->createHardLink(old_file_path, new_file_path);
        }
        else
        {
            /// different disks, we need to read from old part and write to new part
            auto read_buf = old_storage.readFile(old_file_path, /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
            auto size = read_buf->getFileSize();
            auto write_buf
                = new_storage.writeFile(new_file_path, std::min<size_t>(size, DBMS_DEFAULT_BUFFER_SIZE), /* mode */ {}, /* settings */ {});

            DB::copyData(*read_buf, *write_buf, size);
            write_buf->finalize();
        }

        new_index_checksums.addFile(new_file_name, checksum_.file_size, checksum_.file_hash);
    }

    return new_index_checksums;
}

MergeTreeDataPartChecksums calculateVectorIndexChecksums(
    const DataPartStoragePtr & part_storage,
    const String & vector_index_relative_path,
    std::shared_ptr<MergeTreeDataPartChecksums> existing_checksums)
{
    const DataPartStorageOnDiskBase * data_part_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(part_storage.get());
    if (data_part_storage == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported part storage.");

    MergeTreeDataPartChecksums index_checksums;
    auto disk = data_part_storage->getDisk();
    for (auto it = disk->iterateDirectory(vector_index_relative_path); it->isValid(); it->next())
    {
        if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX))
            continue;
        
        if (existing_checksums && existing_checksums->has(it->name()))
        {
            LOG_DEBUG(getLogger("VIUtils"), "checksum exists: {}", it->name());
            auto checksum = existing_checksums->files.at(it->name());
            index_checksums.addFile(it->name(), checksum.file_size, checksum.file_hash);
        }
        else
        {
            auto file_buf = disk->readFile(it->path());
            HashingReadBuffer hashing_buf(*file_buf);
            hashing_buf.ignoreAll();
            index_checksums.addFile(it->name(), hashing_buf.count(), hashing_buf.getHash());
        }
    }

    return index_checksums;
}

}
namespace VectorIndex
{

/// [TODO] replace with index segment ptr
DB::NameSet
getVectorIndexFileNamesInChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool need_checksums_file)
{
    String checksums_filename = VectorIndex::getVectorIndexChecksumsFileName(index_name);
    if (!part_storage->exists(checksums_filename))
        return {};

    try
    {
        DB::MergeTreeDataPartChecksums vector_index_checksums;
        auto buf = part_storage->readFile(checksums_filename, {}, std::nullopt, std::nullopt);
        if (vector_index_checksums.read(*buf))
            assertEOF(*buf);

        if (vector_index_checksums.empty())
            return {};

        DB::NameSet res;
        for (const auto & [file_name, _] : vector_index_checksums.files)
            res.emplace(file_name);

        if (need_checksums_file)
            res.emplace(checksums_filename);

        return res;
    }
    catch (const DB::Exception & e)
    {
        /// read checksum failed, return empty list
        LOG_WARNING(
            getLogger("VIUtils"),
            "getVectorIndexFileNamesInChecksums: read checksum file {} error {}: {}",
            checksums_filename,
            e.code(),
            e.message());
    }

    return {};
}

DB::MergeTreeDataPartChecksums
getVectorIndexChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name)
{
     String checksums_filename = VectorIndex::getVectorIndexChecksumsFileName(index_name);
    if (!part_storage->exists(checksums_filename))
        throw DB::Exception(
            DB::ErrorCodes::INVALID_VECTOR_INDEX,
            "Checksums file {} in part {} does not exists.",
            checksums_filename,
            part_storage->getRelativePath());

    DB::MergeTreeDataPartChecksums vector_index_checksums;
    auto buf = part_storage->readFile(checksums_filename, {}, std::nullopt, std::nullopt);
    if (vector_index_checksums.read(*buf))
        assertEOF(*buf);

    if (vector_index_checksums.empty())
        throw DB::Exception(DB::ErrorCodes::INVALID_VECTOR_INDEX, "Read empty checksums for file {}", checksums_filename);

    return vector_index_checksums;
}

DB::MergeTreeDataPartChecksums
getVectorIndexChecksums(const DB::DiskPtr disk, const String & index_name, const String & vi_file_path_prefix)
{
    String checksums_filename = VectorIndex::getVectorIndexChecksumsFileName(index_name);
    if (!disk->exists(vi_file_path_prefix + checksums_filename))
        throw DB::Exception(
            DB::ErrorCodes::INVALID_VECTOR_INDEX,
            "Checksums file {} in path {} does not exists.",
            checksums_filename,
            vi_file_path_prefix);

    DB::MergeTreeDataPartChecksums vector_index_checksums;
    auto buf = disk->readFile(vi_file_path_prefix + checksums_filename);
    if (vector_index_checksums.read(*buf))
        assertEOF(*buf);

    if (vector_index_checksums.empty())
        throw DB::Exception(DB::ErrorCodes::INVALID_VECTOR_INDEX, "Read empty checksums for file {}", checksums_filename);

    return vector_index_checksums;
}

void removeVectorIndexFilesFromFileLists(const DB::DataPartStoragePtr & part_storage, const DB::Names & index_files_list)
{
    DB::IDataPartStorage * storage = const_cast<DB::IDataPartStorage *>(part_storage.get());
    for (const auto & file_name : index_files_list)
        storage->removeFileIfExists(file_name);
}

void removeIndexFile(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool include_checksum_file)
{
    auto * mutable_part_storage = const_cast<DB::IDataPartStorage *>(part_storage.get());
    for (auto & file_name : getVectorIndexFileNamesInChecksums(part_storage, index_name, include_checksum_file))
        mutable_part_storage->removeFileIfExists(file_name);
}

DB::NameSet getAllValidVectorIndexFileNames(const DB::IMergeTreeDataPart & data_part)
{
    if (!data_part.storage.getInMemoryMetadataPtr()->hasVectorIndices())
        return {};

    DB::NameSet valid_indices_file;

    for (const auto & vec_desc : data_part.storage.getInMemoryMetadataPtr()->getVectorIndices())
    {
        auto index_file = getVectorIndexFileNamesInChecksums(data_part.getDataPartStoragePtr(), vec_desc.name, /*need checksum file*/ true);
        valid_indices_file.insert(index_file.begin(), index_file.end());
    }

    return valid_indices_file;
}

void removeIncompleteMovedVectorIndexFiles(const DB::IMergeTreeDataPart & data_part)
{
    auto exclude_index_file = getAllValidVectorIndexFileNames(data_part);
    auto & mutable_part_storage = const_cast<DB::IDataPartStorage &>(data_part.getDataPartStorage());
    auto iter_file = mutable_part_storage.iterate();
    for (auto it = mutable_part_storage.iterate(); it->isValid(); it->next())
    {
        if (!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX) || exclude_index_file.count(it->name()) != 0)
            continue;

        mutable_part_storage.removeFileIfExists(it->name());
    }
}

void dumpCheckSums(
    const DB::DataPartStoragePtr & part_storage, const String & index_name, const DB::MergeTreeDataPartChecksums & index_checksum)
{
    /// write new part decoupled vector index checksums file
    auto * mutable_part_storage = const_cast<DB::IDataPartStorage *>(part_storage.get());
    auto out_checksums = mutable_part_storage->writeFile(getVectorIndexChecksumsFileName(index_name), 4096, {});

    index_checksum.write(*out_checksums);
    out_checksums->finalize();
}

bool checkConsistencyForVectorIndex(const DB::DataPartStoragePtr & part_storage, const DB::MergeTreeDataPartChecksums & index_checksums)
{
    /// Base consistency check: existence of files recorded in checksums
    try
    {
        for (const auto & [filename, vector_index_checksum] : index_checksums.files)
            vector_index_checksum.checkSize(*part_storage, filename);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

std::vector<DB::MergedPartNameAndId>
getMergedSourcePartsFromFileName(const String & index_name, const DB::MergeTreeDataPartChecksums vector_index_checksums)
{
    std::vector<DB::MergedPartNameAndId> old_part_names;
    if (!vector_index_checksums.has(DB::toString("merged-inverted_row_ids_map") + VECTOR_INDEX_FILE_SUFFIX))
        return old_part_names;

    String description_file_name = VectorIndex::getVectorIndexDescriptionFileName(index_name);
    for (auto const & [file_name, _] : vector_index_checksums.files)
    {
        if (!endsWith(file_name, description_file_name))
            continue;

        /// Found merged files, merged-<id>-<part name>-<vector_index_name>-vector_index_ready
        DB::Strings tokens;
        boost::algorithm::split(tokens, file_name, boost::is_any_of("-"));
        if (tokens.size() != 5)
        {
            return {};
        }

        old_part_names.emplace_back(tokens[2], std::stoi(tokens[1]));
    }
    return old_part_names;
}

String getVectorIndexCachePrefix(
    const String & table_relative_path,
    const String & part_name,
    const String & index_name)
{
    auto global_context = DB::Context::getGlobalContextInstance();
    if (global_context)
    {
        return fs::path(global_context->getVectorIndexCachePath())
            / fs::path(table_relative_path).parent_path().parent_path()
            / String(cutMutVer(part_name) + "-" + index_name)
            / "";
    }
    else
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot get Vector Index Cache prefix!");
}

String generateUUIDv4() {
    return DB::toString(DB::UUIDHelpers::generateV4());
}

std::pair<String, String> getPartNameUUIDFromNvmeCachePath(const String & nvme_cache_path)
{
    std::vector<String> tokens;
    /// path uuid example: all_2778448_2786755_136-v1-bcab5ab2-0b55-41ed-b42d-c62ba844e434
    boost::split(tokens, nvme_cache_path, boost::is_any_of("-"));
    String part_name, index_name, uuid_path;
    if (tokens.size() == 7)
    {
        part_name = tokens[0];
        index_name = tokens[1];
        uuid_path = tokens[2] + "-" + tokens[3] + "-" + tokens[4] + "-" + tokens[5] + "-" + tokens[6];
    }
    return std::make_pair(String(part_name + "-" + index_name), uuid_path);

}   


void printMemoryInfo(const LoggerPtr log, std::string msg)
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

uint64_t getVectorDimension(const Search::DataType &search_type, const DB::StorageInMemoryMetadata &metadata, const String &column_name)
{
    std::optional<DB::NameAndTypePair> search_column_type = metadata.columns.getAllPhysical().tryGetByName(column_name);
    if (!search_column_type)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "vector search column name: {}, type is not exist", column_name);
    uint64_t result_dim = 0;
    switch (search_type)
    {
        case Search::DataType::FloatVector:
        {
            const DB::DataTypeArray *array_type = typeid_cast<const DB::DataTypeArray *>(search_column_type->type.get());
            if (!array_type)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector search type is FloatVector for column {}, but datatype is not Array()", column_name);
            result_dim = metadata.getConstraints().getArrayLengthByColumnName(column_name).first;
            LOG_DEBUG(getLogger("VIUtils"), "vector search type: FloatVector, search column dim: {}", result_dim);
            break;
        }
        case Search::DataType::BinaryVector:
        {
            const DB::DataTypeFixedString *fixed_string_type = typeid_cast<const DB::DataTypeFixedString *>(search_column_type->type.get());
            if (!fixed_string_type)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Vector search type is BinaryVector for column {}, but datatype is not FixedString", column_name);
            result_dim = static_cast<uint64_t>(fixed_string_type->getN() * 8);
            LOG_DEBUG(getLogger("VIUtils"), "vector search type: BinaryVector, search column dim: {}", result_dim);
            break;
        }
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported Vector search Type");
    }
    return result_dim;
}
size_t getEachVectorBytes(const Search::DataType &search_type, const size_t dimension)
{
    switch (search_type)
    {
        case Search::DataType::FloatVector:
            return dimension * sizeof(float);
        case Search::DataType::BinaryVector:
            // As for Binary Vector, each dimension is 1 bit
            return dimension / 8;
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type");
    }
}

std::vector<String> splitString(const String &str, const char &delim)
{
    std::vector<String> tokens;
    boost::split(tokens, str, boost::is_any_of(String(1, delim)));
    return tokens;
}

void convertIndexFileForUpgrade(const DB::IMergeTreeDataPart & part)
{
    auto log = getLogger("VIUtils");
    if (part.getState() >= DB::MergeTreeDataPartState::Active)
    {
        LOG_WARNING(log, "The part {} is already active, no need to convert", part.name);
        return;
    }
    /// [TODO] convert index file for upgrade
    DB::IDataPartStorage & part_storage = const_cast<DB::IDataPartStorage &>(part.getDataPartStorage());

    /// If checksums file needs to be generated.
    bool has_intact_old_version_vector_index = false;

    /// Only supports either all index versions are in V1, or all versions are in V2
    String old_vector_index_ready_v1 = DB::toString("vector_index_ready") + VECTOR_INDEX_FILE_OLD_SUFFIX;
    String old_vector_index_ready_v2 = DB::toString("vector_index_ready_v2") + VECTOR_INDEX_FILE_OLD_SUFFIX;
    String old_index_description_v2 = DB::toString(VECTOR_INDEX_DESCRIPTION) + VECTOR_INDEX_FILE_OLD_SUFFIX;

    /// Support multiple vector indices
    auto metadata_snapshot = part.storage.getInMemoryMetadataPtr();
    auto vec_indices = metadata_snapshot->getVectorIndices();

    /// Only one vector index is supported before upgrade to support multiple.
    /// Use the first vector index description.
    auto vector_index_desc = vec_indices[0];
    String current_index_description_name = DB::toString(VECTOR_INDEX_DESCRIPTION) + VECTOR_INDEX_FILE_SUFFIX;
    String current_checksums_file_name = getVectorIndexChecksumsFileName(vector_index_desc.name);
    String new_description_file_name = getVectorIndexDescriptionFileName(vector_index_desc.name);

    /// Quick check the existence of new description file with index name.
    bool from_checksums = false;
    if (part_storage.exists(current_checksums_file_name))
    {
        from_checksums = true;
        /// remove other incomplate index files based on checksums
        auto exclude_index_file = getVectorIndexFileNamesInChecksums(part.getDataPartStoragePtr(), vector_index_desc.name, true);
        for (auto it = part_storage.iterate(); it->isValid(); it->next())
        {
            if ((!endsWith(it->name(), VECTOR_INDEX_FILE_SUFFIX) && !endsWith(it->name(), VECTOR_INDEX_FILE_OLD_SUFFIX))
                || exclude_index_file.count(it->name()) != 0)
                continue;

            if (!startsWith(it->name(), vector_index_desc.name + "-"))
                continue;

            part_storage.removeFileIfExists(it->name());
        }

        if (part_storage.exists(new_description_file_name))
        {
            /// The current version file already exists locally, no need to convert
            LOG_DEBUG(log, "The current version file already exists locally, does not need to convert");
            return;
        }
    }

    /// Used for upgrade from checksums version, need update checksums with new file names.
    std::unordered_map<String, String> converted_files_map;
    for (auto it = part_storage.iterate(); it->isValid(); it->next())
    {
        String file_name = it->name();

        /// v1, v2 or checksums
        if (from_checksums)
        {
            if (!endsWith(file_name, VECTOR_INDEX_FILE_SUFFIX))
                continue;
        }
        else if (!(endsWith(file_name, VECTOR_INDEX_FILE_OLD_SUFFIX)))
            continue;

        /// vector index description file need to update name.
        bool is_description = false;

        /// Check for checksums first
        if (from_checksums)
        {
            if (endsWith(file_name, current_index_description_name))
            {
                /// Lastest desciption file name with index name
                if (endsWith(file_name, new_description_file_name))
                {
                    LOG_DEBUG(log, "The current version file already exists locally, does not need to convert");
                    return;
                }

                has_intact_old_version_vector_index = true;
                is_description = true;
            }
            else if (file_name == current_checksums_file_name)
                continue;
        }
        else if (endsWith(file_name, old_vector_index_ready_v2)) /// v2 ready file
        {
            has_intact_old_version_vector_index = true;

            LOG_DEBUG(log, "Delete ready file {}", file_name);
            part_storage.removeFile(file_name);

            continue;
        }
        else if (endsWith(file_name, old_index_description_v2))
        {
            is_description = true;
        }
        else if (endsWith(file_name, old_vector_index_ready_v1)) /// v1 ready file
        {
            has_intact_old_version_vector_index = true;
            is_description = true; /// ready will be updated to description file.
        }

        /// There are some common codes to get new description file name.
        if (is_description)
        {
            String new_file_name = file_name;
            if (endsWith(file_name, VECTOR_INDEX_FILE_OLD_SUFFIX))
                new_file_name = fs::path(file_name).replace_extension(VECTOR_INDEX_FILE_SUFFIX).string();

            /// Replace vector_index_description to <index_name>-vector_index_description
            /// Replace merged-<part_id>-<part_name>-vector_index_description to merged-<part_id>-<part_name>-<index_name>-vector_index_description
            new_file_name = std::regex_replace(new_file_name, std::regex("vector"), vector_index_desc.name + "-vector");
            converted_files_map[file_name] = new_file_name;
        }
        else
        {
            /// For other vector index files (exclude ready, checksum, or description files), update vector index file extension to latest.
            /// Support multiple vector indices feature changes the vector index file name by removing column name.
            /// e.g. <index_name>-<column_name>-id_list will be updated to <index_name>-id_list

            /// old vector index name
            String old_vector_index_column_name = vector_index_desc.name + "-" + vector_index_desc.column;
            if (file_name.find(old_vector_index_column_name) == std::string::npos)
            {
                /// Just check suffix
                if (!from_checksums && endsWith(file_name, VECTOR_INDEX_FILE_OLD_SUFFIX))
                {
                    String new_file_name = fs::path(file_name).replace_extension(VECTOR_INDEX_FILE_SUFFIX).string();
                    converted_files_map[file_name] = new_file_name;
                }
                continue;
            }

            /// Replace "<index_name>-<column_name>" to "<index_name>"
            String new_file_name = std::regex_replace(file_name, std::regex(old_vector_index_column_name), vector_index_desc.name);

            if (!from_checksums && endsWith(new_file_name, VECTOR_INDEX_FILE_OLD_SUFFIX))
                new_file_name = fs::path(new_file_name).replace_extension(VECTOR_INDEX_FILE_SUFFIX).string();

            /// for faiss index file, contain "<index_name>-<index_name>.vidx3" file, replcace "<index_name>-<index_name>.vidx3" to "<index_name>-data_bin.vidx3"
            String faiss_index_old_suffix = vector_index_desc.name + VECTOR_INDEX_FILE_SUFFIX;
            String faiss_index_new_suffix = String("data_bin") + VECTOR_INDEX_FILE_SUFFIX;
            if (new_file_name.find(faiss_index_old_suffix) != std::string::npos)
                new_file_name =  std::regex_replace(new_file_name, std::regex(faiss_index_old_suffix), faiss_index_new_suffix);

            converted_files_map[file_name] = new_file_name;
        }
    }

    /// Support multiple vector indices
    /// No vector index files in part or incomplete vector index files
    if (!has_intact_old_version_vector_index)
        return;

    /// Here we collect converted files and upgrade is needed.
    for (auto const & [old_name_, new_name_] : converted_files_map)
    {
        part_storage.moveFile(old_name_, new_name_);
        LOG_DEBUG(log, "Convert vector index file {} to {}", old_name_, new_name_);
    }

    /// Old version vector index files have ready file, will generate checksums file.
    DB::MergeTreeDataPartChecksums vector_index_checksums;
    if (!from_checksums)
        vector_index_checksums = calculateVectorIndexChecksums(part.getDataPartStoragePtr(), part_storage.getRelativePath());
    else if (!converted_files_map.empty())
    {
        if (!part_storage.exists(current_checksums_file_name))
        {
            LOG_WARNING(log, "checksums file '{}' doesn't exist in part {}, will not update it", current_checksums_file_name, part.name);
            return;
        }

        /// Multiple vector indices feature changes vector file names
        DB::MergeTreeDataPartChecksums old_checksums;
        auto buf = part_storage.readFile(current_checksums_file_name, {}, std::nullopt, std::nullopt);
        if (old_checksums.read(*buf))
            assertEOF(*buf);

        for (auto const & [name_, checksum_] : old_checksums.files)
        {
            String new_name_;
            if (converted_files_map.contains(name_))
            {
                new_name_ = converted_files_map[name_];
            }
            else
            {
                new_name_ = name_;
            }
            vector_index_checksums.addFile(new_name_, checksum_.file_size, checksum_.file_hash);
        }

        part_storage.removeFile(current_checksums_file_name);
    }

    LOG_DEBUG(log, "write vector index {} checksums file for part {}", vector_index_desc.name, part.name);
    auto out_checksums = part_storage.writeFile(current_checksums_file_name, 4096, {});
    vector_index_checksums.write(*out_checksums);
    out_checksums->finalize();
    /// Incomplete vector index files will be removed when loading checksums file.
}

}
