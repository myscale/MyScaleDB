#pragma once

#include <boost/noncopyable.hpp>
#include <unordered_set>

#include <base/types.h>
#include <base/scope_guard.h>
#include <functional>

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace VectorIndex
{

/// designed for file path lock, and singleton
class TempVIHolder : private boost::noncopyable
{
public:
    static TempVIHolder & instance()
    {
        static TempVIHolder instance;
        return instance;
    }

    scope_guard lockCachePath(const String & path)
    {
        std::lock_guard lock(mutex);
        if (!temp_vi_cache_files.insert(path).second)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "VICachePath {} is already locked", path);
        return [this, path]
        {
            removeCachePath(path);
        };
    }
private:
    TempVIHolder() = default;
    void removeCachePath(const String & path)
    {
        std::lock_guard lock(mutex);
        if (!temp_vi_cache_files.erase(path))
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "VICachePath {} does not exist", path);
    }

    std::mutex mutex;
    std::unordered_set<String> temp_vi_cache_files;

};



}
