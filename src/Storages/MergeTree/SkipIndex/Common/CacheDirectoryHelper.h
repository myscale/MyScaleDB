#pragma once

#include <filesystem>
#include <iostream>
#include <Core/Block.h>
#include <Storages/MergeTree/SkipIndex/Common/IndexType.h>


namespace fs = std::filesystem;


namespace DB
{

static const auto TEMP_DISK_FOR_DIRECTORY_HELPER = "_tmp_disk_for_directory_helper";

class CacheDirectoryHelper
{
public:
    static void removeFullIndexPathInCacheForward(const String & full_index_path_in_cache);
    static void removeFullIndexPathInCacheForward(
        const String & part_relative_path_in_cache, const SkipIndexType index_type, const String & skp_index_name);
    static void removePartRelativePathInCacheForward(const String & part_relative_path_in_cache, const SkipIndexType index_type);

    /// @brief get part full path in cache directory.
    /// @param relative_data_part_in_cache data part relative path, example: `store/xxx/all_1_1_1_0`.
    static std::optional<fs::path>
    convertPartRelativePathToFullPathInCache(const String & relative_data_part_in_cache, const SkipIndexType index_type);

    static void removeDirectoryDirectly(const fs::path & directory);
    static void removeDirectoryIfEmpty(const fs::path & directory);
};

}
