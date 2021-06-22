#include "FlatIndex.h"
#include <Common/logger_useful.h>
#include "CacheManager.h"
#include "IndexException.h"
#include "IndexReader.h"
#include "IndexWriter.h"
#include "faiss/index_io.h"
#include <VectorIndex/VectorIndexCommon.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
}

namespace VectorIndex
{
void FlatIndex::train(VectorDatasetPtr dataset, int64_t total)
{
    reinterpret_cast<faiss::IndexFlatFilter *>(index.get())->reserve(total);
}

void FlatIndex::addWithoutId(VectorDatasetPtr dataset)
{
    if (index != nullptr)
    {
        index->add(dataset->getVectorNum(), dataset->getData());
    }
    else
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "addWithoutId: index not intialized");
    }
}

void FlatIndex::search(
    const VectorDatasetPtr dataset,
    const int32_t topK,
    float * distances,
    int64_t * result_id,
    Parameters & /*param*/,
    GeneralBitMapPtr filter)
{
    Poco::Logger * log = &Poco::Logger::get("FlatIndex");
    if (index == nullptr)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "search: index not intialized");
    }
    faiss::bitMapPtr inner_bit_map = std::shared_ptr<faiss::bitMap>();
    inner_bit_map.reset(reinterpret_cast<faiss::bitMap *>(convertInnerBitMap(filter)));

    int32_t num_query = dataset->getVectorNum();
    float * query_datas = dataset->getData();

    LOG_DEBUG(log, "[search] raw data size: {}", reinterpret_cast<faiss::IndexFlatFilter *>(index.get())->xb.size());

    reinterpret_cast<faiss::IndexFlatFilter *>(index.get())
        ->search(num_query, query_datas, topK, distances, result_id, inner_bit_map.get());
}

VectorDatasetPtr FlatIndex::getInMemVectors()
{
    VectorDatasetPtr data = std::make_shared<VectorDataset>(
        reinterpret_cast<faiss::IndexFlatFilter *>(index.get())->xb.size() / dimension,
        dimension,
        reinterpret_cast<faiss::IndexFlatFilter *>(index.get())->xb.data());
    return data;
}

void FlatIndex::getMyParameters(Parameters params)
{
    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::FLAT);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
}

bool FlatIndex::compare(const VectorIndex & other)
{
    const FlatIndex * other_p = dynamic_cast<const FlatIndex *>(&other);
    if (other_p == nullptr)
    {
        return false;
    }
    if (other_p->me != me)
    {
        return false;
    }
    if (other_p->dimension != dimension)
    {
        return false;
    }
    return true;
}
/*
void FlatIndex::remove(const int32_t * ids)
{
}
*/

}
