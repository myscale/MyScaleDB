#pragma once

#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <mutex>

#include <Common/logger_useful.h>

namespace VectorIndex
{
class LimiterSharedContext {
public:
    std::shared_mutex mutex;
    std::condition_variable_any cv;
    std::atomic_int count{0};
    int max_threads;

    LimiterSharedContext(int max_threads_) : max_threads(max_threads_) { }
};

class SearchThreadLimiter {
private:
    LimiterSharedContext& context;
    const Poco::Logger* log;

public:
    SearchThreadLimiter(LimiterSharedContext& context_, const Poco::Logger* log_) 
    : context(context_), log(log_)
    {
        std::shared_lock<std::shared_mutex> lock(context.mutex);
        context.cv.wait(lock, [&] { return context.count.load() < context.max_threads; });
        context.count.fetch_add(1);
        LOG_DEBUG(log, "Uses {}/{} threads", context.count.load(), context.max_threads);
    }

    ~SearchThreadLimiter()
    {
        context.count.fetch_sub(1);
        context.cv.notify_one();
    }
};
}
