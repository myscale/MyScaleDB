/*
 * Copyright (2024) MOQI SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
