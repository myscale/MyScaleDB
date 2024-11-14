#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/SkipIndex/Common/CacheDirectoryHelper.h>
#include <Storages/MergeTree/SkipIndex/Common/IndexFileMeta.h>

namespace DB
{

void CacheDirectoryHelper::removeFullIndexPathInCacheForward(const String & full_index_path_in_cache_)
{
    try
    {
        fs::path full_index_path_in_cache = fs::path(full_index_path_in_cache_);
        auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath(), 0);

        constexpr int required_depth = 5; // Corrected depth

        // varify index cache path.
        fs::path store_path = full_index_path_in_cache;
        for (int i = 0; i < required_depth; ++i)
        {
            if (!store_path.has_parent_path())
            {
                LOG_ERROR(
                    &Poco::Logger::get("CacheDirectoryHelper"), "Can't remove `full_index_path_in_cache`: `{}`", full_index_path_in_cache);
                return;
            }
            store_path = store_path.parent_path();
        }
        if (!store_path.has_filename() || (store_path.filename() != "store" && store_path.filename() != "data"))
        {
            LOG_ERROR(
                &Poco::Logger::get("CacheDirectoryHelper"), "Can't remove `full_index_path_in_cache`: `{}`", full_index_path_in_cache);
            return;
        }


        if (tmp_disk->isDirectory(full_index_path_in_cache))
        {
            LOG_INFO(&Poco::Logger::get("CacheDirectoryHelper"), "try remove index directory: `{}`", full_index_path_in_cache);
        }

        // index_full_path_in_cache: `/var/lib/clickhouse/sparse_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/skp_idx_test_idx/`
        CacheDirectoryHelper::removeDirectoryDirectly(full_index_path_in_cache);

        // data_part_full_path_in_cache: `/var/lib/clickhouse/sparse_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
        auto data_part_full_path_in_cache = full_index_path_in_cache.parent_path().parent_path();
        CacheDirectoryHelper::removeDirectoryIfEmpty(data_part_full_path_in_cache);

        // table_uuid_directory_in_cache: `/var/lib/clickhouse/sparse_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
        auto table_uuid_directory_in_cache = data_part_full_path_in_cache.parent_path();
        CacheDirectoryHelper::removeDirectoryIfEmpty(table_uuid_directory_in_cache);

        // table_uuid_prefix_directory_in_cache: `/var/lib/clickhouse/sparse_index_cache/store/6b0`
        auto table_uuid_prefix_directory_in_cache = table_uuid_directory_in_cache.parent_path();
        CacheDirectoryHelper::removeDirectoryIfEmpty(table_uuid_prefix_directory_in_cache);
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("CacheDirectoryHelper"), "Error happend when removing `{}` forwardly", full_index_path_in_cache_);
    }
}

void CacheDirectoryHelper::removeFullIndexPathInCacheForward(
    const String & part_relative_path_in_cache, const SkipIndexType index_type, const String & skp_index_name)
{
    try
    {
        std::optional<fs::path> res
            = CacheDirectoryHelper::convertPartRelativePathToFullPathInCache(part_relative_path_in_cache, index_type);
        if (res.has_value())
        {
            fs::path data_part_full_path_in_cache = res.value();
            fs::path index_full_path_in_cache = data_part_full_path_in_cache / skp_index_name / "";
            CacheDirectoryHelper::removeFullIndexPathInCacheForward(index_full_path_in_cache);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("CacheDirectoryHelper"),
            "Error happend when removing part: `{}` skp_idx_name:`{}` forwardly",
            part_relative_path_in_cache,
            skp_index_name);
    }
}

void CacheDirectoryHelper::removePartRelativePathInCacheForward(const String & part_relative_path_in_cache, const SkipIndexType index_type)
{
    try
    {
        std::optional<fs::path> res
            = CacheDirectoryHelper::convertPartRelativePathToFullPathInCache(part_relative_path_in_cache, index_type);
        if (res.has_value())
        {
            fs::path part_full_path_in_cache = res.value();
            auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath(), 0);

            if (tmp_disk->isDirectory(part_full_path_in_cache))
            {
                LOG_INFO(
                    &Poco::Logger::get("CacheDirectoryHelper"),
                    "[removePartRelativePathInCacheForward] try remove directory: `{}`",
                    part_full_path_in_cache);
            }

            // data_part_full_path_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
            CacheDirectoryHelper::removeDirectoryDirectly(part_full_path_in_cache);

            // table_uuid_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
            auto table_uuid_directory_in_cache = part_full_path_in_cache.parent_path().parent_path();
            CacheDirectoryHelper::removeDirectoryIfEmpty(table_uuid_directory_in_cache);

            // table_uuid_prefix_directory_in_cache: `/var/lib/clickhouse/tantivy_index_cache/store/6b0`
            auto table_uuid_prefix_directory_in_cache = table_uuid_directory_in_cache.parent_path();
            CacheDirectoryHelper::removeDirectoryIfEmpty(table_uuid_prefix_directory_in_cache);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("CacheDirectoryHelper"),
            "[removePartRelativePathInCacheForward] Error happend when removing `{}` forwardly",
            part_relative_path_in_cache);
    }
}


void CacheDirectoryHelper::removeDirectoryDirectly(const fs::path & directory)
{
    try
    {
        auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath(), 0);

        if (tmp_disk->isDirectory(directory))
        {
            tmp_disk->clearDirectory(directory);
            tmp_disk->removeDirectory(directory);
        }
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("CacheDirectoryHelper"), "error happened when removing `{}` directly", directory);
    }
}

void CacheDirectoryHelper::removeDirectoryIfEmpty(const fs::path & directory)
{
    try
    {
        auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath(), 0);

        if (tmp_disk->isDirectory(directory) && tmp_disk->isDirectoryEmpty(directory))
        {
            tmp_disk->removeRecursive(directory);
        }
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("CacheDirectoryHelper"), "error happened when removing empty `{}`", directory);
    }
}

std::optional<fs::path>
CacheDirectoryHelper::convertPartRelativePathToFullPathInCache(const String & relative_data_part_in_cache, const SkipIndexType index_type)
{
    try
    {
        auto context = Context::getGlobalContextInstance();
        fs::path skp_index_cache_prefix;
        if (index_type == SkipIndexType::SparseIndex)
        {
            skp_index_cache_prefix = context->getSparseIndexCachePath();
        }
        else if (index_type == SkipIndexType::TantivyIndex)
        {
            skp_index_cache_prefix = context->getTantivyIndexCachePath();
        }

        // example: /var/lib/clickhouse/sparse_index_cache/store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2/
        auto data_part_full_cache_path = skp_index_cache_prefix / relative_data_part_in_cache / "";

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
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("CacheDirectoryHelper"),
            "error happend when converting data part relative path to full_path_in_cache, rel_data_part: `{}`",
            relative_data_part_in_cache);
        return std::nullopt;
    }
}


}
