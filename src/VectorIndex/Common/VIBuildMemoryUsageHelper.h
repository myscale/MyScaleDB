#pragma once

#include <Poco/Logger.h>

namespace VectorIndex
{
enum class BuildMemoryCheckResult
{
    OK,     /// ok to build
    LATER,  /// currently unable to build, maybe try again later
    NEVER,  /// size required greater than limit
};

struct VIBuildMemoryUsageHelper
{
    VIBuildMemoryUsageHelper() = default;

    ~VIBuildMemoryUsageHelper()
    {
        if (!build_memory_size_recorded)
            return;

        /// decrease build memory size reserved before build. see checkBuildMemory()
        {
            std::lock_guard lock(build_memory_mutex);
            current_build_memory_size -= build_memory_size_recorded;
        }

        LOG_DEBUG(
            getLogger("VIBuildMemoryUsageHelper"),
            "after build: size = {}, current_total = {}",
            build_memory_size_recorded,
            current_build_memory_size);
    }

    static void setCacheManagerSizeInBytes(size_t size);

    static void setBuildMemorySizeInBytes(size_t size);

    static std::mutex build_memory_mutex;
    /// global memory size limit for index building
    static size_t build_memory_size_limit;
    /// current total memory size reserved for index building globally
    static size_t current_build_memory_size;

    /// check if index to build will exceed build memory size limit
    static BuildMemoryCheckResult checkBuildMemorySize(size_t size);

    void checkBuildMemory(size_t size);

    size_t build_memory_size_recorded = 0;
};

using IndexBuildMemoryUsageHelperPtr = std::unique_ptr<VIBuildMemoryUsageHelper>;
}
