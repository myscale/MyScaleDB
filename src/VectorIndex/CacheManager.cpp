#include <VectorIndex/CacheManager.h>
#include <memory>

#include <VectorIndex/IndexException.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace VectorIndex
{

CacheManager::CacheManager(int): log(&Poco::Logger::get("CacheManager"))
{
    while (!m)
    {
        sleep(100);
    }

    cache_ = std::make_unique<VectorIndexCache>(cache_size_in_bytes);
}

CacheManager * CacheManager::getInstance()
{
    constexpr int unused = 0;
    static CacheManager cache_mgr(unused);
    return &cache_mgr;
}

IndexWithMetaPtr CacheManager::get(const CacheKey& cache_key)
{
    if (!cache_)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }
    IndexAndMutexPtr iam_ptr = cache_->get(cache_key);
    if (iam_ptr)
    {
        return iam_ptr->index_ptr;
    }
    else
    {
        return nullptr;
    }
}

void CacheManager::put(const CacheKey& cache_key, IndexWithMetaPtr index)
{
    if (!cache_)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "cache not allocated");
    }
    LOG_INFO(log, "VectorIndexCache put cache_key={}", cache_key.toString());

    IndexAndMutexPtr iam_ptr = std::make_shared<IndexAndMutex>(index, nullptr);

    cache_->set(cache_key, iam_ptr);
}

size_t CacheManager::countItem() const
{
    return cache_->count();
}

void CacheManager::forceExpire(const CacheKey& cache_key)
{
    LOG_INFO(log, "VectorIndexCache forceExpire cache_key={}", cache_key.toString());
    return cache_->remove(cache_key);
}

void CacheManager::startLoading(const CacheKey& cache_key)
{
    if (!cache_)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "startLoading: cache not allocated");
    }
    LOG_INFO(log, "VectorIndexCache startLoading cache_key={}", cache_key.toString());
    std::shared_ptr<std::mutex> new_mutex = std::make_shared<std::mutex>();

    std::shared_ptr<IndexAndMutex> im_ptr = std::make_shared<IndexAndMutex>(nullptr, new_mutex);

    cache_->getOrSet(cache_key, [&](){
        return im_ptr;
    });
}

std::shared_ptr<std::mutex> CacheManager::getMutex(const CacheKey& cache_key)
{
    if (!cache_)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "getMutex: cache not allocated");
    }

    IndexAndMutexPtr iam_ptr = cache_->get(cache_key);
    if (iam_ptr)
    {
        return iam_ptr->mu_ptr;
    }
    else
    {
        return nullptr;
    }
}

void CacheManager::setCacheSize(size_t size_in_bytes)
{
    cache_size_in_bytes = size_in_bytes;
    m = true;
}

std::list<std::pair<CacheKey, Parameters>> CacheManager::getAllItems()
{
    std::list<std::pair<CacheKey, Parameters>> result;

    std::list<std::pair<CacheKey, std::shared_ptr<IndexAndMutex>>> cache_list = cache_->getCacheList();

    for (auto im_ptr : cache_list)
    {
        // key   --- string
        // value --- std::shared_ptr<IndexAndMutex>
        if (im_ptr.second->index_ptr)
        {
            result.emplace_back(std::make_pair(im_ptr.first, im_ptr.second->index_ptr->des));
        }
    }
    return result;
}

}
