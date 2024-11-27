#pragma once

#include <Common/logger_useful.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Storages/VIDescriptions.h>

#include <boost/algorithm/string.hpp>

#pragma GCC diagnostic ignored "-Wunused-function"

namespace Search
{
enum class DataType;
}

namespace DB
{
struct MergedPartNameAndId;
struct SegmentMetadata;
class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

/// used to move vector indices files of one old data part
/// to new data part's path, and move vector index checksums to avoid recalculate
DB::MergeTreeDataPartChecksums moveVectorIndexFiles(
    const bool decouple,
    const String & part_id,
    const String & part_name,
    const String & vec_index_name,
    DB::MergeTreeDataPartPtr old_data_part,
    DB::MergeTreeMutableDataPartPtr new_data_part);

/// Calculate checksum for the vector index file under the specified path
MergeTreeDataPartChecksums calculateVectorIndexChecksums(
    const DataPartStoragePtr & part_storage,
    const String & vector_index_relative_path,
    std::shared_ptr<MergeTreeDataPartChecksums> existing_checksums = nullptr);

}

namespace VectorIndex
{
struct CachedSegment;

inline String cutMutVer(const String & part_name)
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

inline String cutPartitionID(const String & part_name)
{
    std::vector<String> tokens;
    boost::split(tokens, part_name, boost::is_any_of("_"));
    return tokens[0];
}

inline String getPartRelativePath(const String & table_path)
{
    /// get table relative path from data_part_path,
    /// for example: table_path: /var/lib/clickhouse/store/0e3/0e3..../all_1_1_0 or store/0e3/0e3..../,
    /// return path: store/0e3/0e3....
    auto path = fs::path(table_path).parent_path();
    return fs::path(path.parent_path().parent_path().filename()) / path.parent_path().filename() / path.filename();
}

DB::NameSet
getVectorIndexFileNamesInChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool need_checksums_file = false);

DB::MergeTreeDataPartChecksums
getVectorIndexChecksums(const DB::DataPartStoragePtr & part_storage, const String & index_name);

DB::MergeTreeDataPartChecksums
getVectorIndexChecksums(const DB::DiskPtr disk, const String & index_name, const String & vi_file_path_prefix);

void removeVectorIndexFilesFromFileLists(const DB::DataPartStoragePtr & part_storage, const DB::Names & index_files_list);

void removeIndexFile(const DB::DataPartStoragePtr & part_storage, const String & index_name, bool include_checksum_file = true);

DB::NameSet getAllValidVectorIndexFileNames(const DB::IMergeTreeDataPart & data_part);

void removeIncompleteMovedVectorIndexFiles(const DB::IMergeTreeDataPart & data_part);

void dumpCheckSums(const DB::DataPartStoragePtr & part_storage, const String & index_name, const DB::MergeTreeDataPartChecksums & index_checksum);

bool checkConsistencyForVectorIndex(const DB::DataPartStoragePtr & part_storage, const DB::MergeTreeDataPartChecksums & index_checksums);

std::vector<DB::MergedPartNameAndId>
getMergedSourcePartsFromFileName(const String & index_name, const DB::MergeTreeDataPartChecksums vector_index_checksums);

String getVectorIndexCachePrefix(
    const String & table_relative_path,
    const String & part_name,
    const String & index_name);

std::pair<String, String> getPartNameUUIDFromNvmeCachePath(const String & path_with_uuid);

String generateUUIDv4();

void printMemoryInfo(const LoggerPtr log, std::string msg);

uint64_t getVectorDimension(const Search::DataType &search_type, const DB::StorageInMemoryMetadata &metadata, const String &column_name);

size_t getEachVectorBytes(const Search::DataType &search_type, const size_t dimension);

std::vector<String> splitString(const String &str, const char &delim);

void convertIndexFileForUpgrade(const DB::IMergeTreeDataPart & part);

}
