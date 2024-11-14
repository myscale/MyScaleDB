#pragma once

#include <array>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <vector>
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
#include <Storages/MergeTree/SkipIndex/Common/CacheDirectoryHelper.h>
#include <Storages/MergeTree/SkipIndex/Common/IndexType.h>

namespace DB
{

using ChecksumPairs = std::vector<std::pair<String, MergeTreeDataPartChecksums::Checksum>>;

static constexpr auto TEMP_DISK_NAME = "_tmp_skp_idx_disk";


class IndexFilesManager
{
public:
    IndexFilesManager(
        const SkipIndexType index_type_,
        const String & index_meta_file_suffix,
        const String & index_data_file_suffix,
        const String & skp_index_name_,
        const DataPartStoragePtr storage_,
        const MutableDataPartStoragePtr storage_builder_ = nullptr);

    /// @brief get current part index directory in cache.
    /// @return example "/var/lib/clickhouse/xx_index_cache/store/20a/ - uuid - /all_1_1_1_0/skp_idx_name/"
    String getFullIndexPathInCache();

    /// @brief update current part index directory in cache.
    /// @param new_part_path_in_cache example: `/xx/.../xx_cache/store/xxx/xxx/all_1_1_0_2`
    String updateFullIndexPathInCache(const String & new_part_path_in_cache);

    /// @brief remove current part index directory in cache. forward stop at `store` or `data`.
    void removeFullIndexPathInCacheForward();

    ChecksumPairs serialize();

    void deserialize();

private:
    const SkipIndexType index_type;
    const String skp_idx_name;

    const DataPartStoragePtr storage;
    const MutableDataPartStoragePtr storage_builder;

    const String index_meta_file_name;
    const String index_data_file_name;

    const ContextPtr context;
    const Poco::Logger * log;
    const DiskPtr tmp_disk;


    /// example: "/var/lib/clickhouse/xx_index_cache/store/20a/ - uuid - /all_1_1_1_0/skp_idx_name/"
    String full_index_path_in_cache = "";

    mutable std::shared_mutex full_index_path_lock;

    /// @brief  init variable `full_index_path_in_cache`.
    void initFullIndexPathInCache();
};


}
