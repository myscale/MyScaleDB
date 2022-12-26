#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic pop

#include "HNSWIndex.h"
#include <omp.h>
#include <Common/logger_useful.h>
#include "IndexException.h"
#include <VectorIndex/VectorIndexCommon.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
extern const int EMPTY_DATA_PASSED;
}

namespace VectorIndex
{
void HNSWIndex::train(const VectorDatasetPtr dataset, int64_t total)
{
    hnswlib::SpaceInterface<float> * space;
    switch (me)
    {
        case (Metrics::L2):
            space = new hnswlib::L2Space(dimension);
            break;
        case (Metrics::IP):
            space = new hnswlib::InnerProductSpace(dimension);
            break;
        case (Metrics::Cosine):
            space = new hnswlib::CosineSpace(dimension);
    }
    //TODO configure this, dynamic max_element
    max_element = total;
    index = std::make_shared<hnswlib::HierarchicalNSW<float>>(space, max_element, neighbor, ef_c);
}

void HNSWIndex::addWithoutId(const VectorDatasetPtr dataset)
{
    Poco::Logger * log = &Poco::Logger::get("HNSW");
    if (index != nullptr)
    {
        int total_vectors = dataset->getVectorNum();
        float * __restrict data_grid = dataset->getData();
        int dim = dataset->getDimension();
        //TODO add omp resource control
        size_t current = index->cur_element_count;
        LOG_TRACE(log, "current: {}", current);
#pragma omp parallel for
        for (int i = 0; i < total_vectors; i++)
        {
            index->addPoint(data_grid + i * dim, current + i);
        }
    }
    else
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "addWithoutId: index not intialized");
    }
}

void HNSWIndex::search(
    const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & params, GeneralBitMapPtr filter)
{
    Poco::Logger * log = &Poco::Logger::get("HNSW");

    if (index == nullptr)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "search: index not intialized");
    }
    int total_vectors = dataset->getVectorNum();
    float * __restrict data_grid = dataset->getData();
    int dim = dataset->getDimension();
    hnswlib::bitMapPtr inner_bit_map = std::shared_ptr<hnswlib::bitMap>();
    inner_bit_map.reset(reinterpret_cast<hnswlib::bitMap *>(convertInnerBitMap(filter)));
    //TODO add omp resource control
    LOG_DEBUG(log, "searching in HNSW, current element count:{}, max:{}", index->cur_element_count, index->max_elements_);
    int ef_s = 50;
    if (params.contains("ef_s"))
    {
        ef_s = StoI(params.find("ef_s")->second);
        params.erase("ef_s");
    }
    if (params.contains("metric_type"))
    {
        params.erase("metric_type");
    }
    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::HNSWFLAT);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
    int num_thread = 1;
    if (total_vectors > 1)
    {
        num_thread = std::max(1, (num_thread_for_vector / count.load()));
    }
#pragma omp parallel for schedule(dynamic) num_threads(num_thread)
    for (int i = 0; i < total_vectors; ++i)
    {
        //TODO might not need to be closer first here
        auto result = index->searchKnnCloserFirst(data_grid + i * dim, topK, ef_s, inner_bit_map.get());
        // size_t missing_k = topK - result.size();
        //this part fills correct results
        for (size_t j = 0; j < result.size(); ++j)
        {
            distances[i * topK + j] = result[j].first;
            result_id[i * topK + j] = result[j].second;
        }
        //this part fills missing results if result size < kzw
        for (size_t j = result.size(); j < topK; j++)
        {
            distances[i * topK + j] = -1;
            result_id[i * topK + j] = -1;
        }
    }
}

BinaryPtr HNSWIndex::serialize(size_t max_bytes_to_serialize, bool & finished)
{
    IndexWriter writer;
    index->saveIndex(writer, max_bytes_to_serialize, finished);
    return convertStructToBinary(writer.data, writer.actual_size);
}

void HNSWIndex::load(BinaryPtr & bi, int64_t total_vec)
{
    if (bi->size == 0 || bi->data == nullptr)
    {
        throw IndexException(DB::ErrorCodes::EMPTY_DATA_PASSED, "load: failed with empty data");
    }
    hnswlib::SpaceInterface<float> * space;
    Poco::Logger * log = &Poco::Logger::get("HNSW");
    switch (me)
    {
        case (Metrics::L2):
            LOG_INFO(log, "searching in HNSW, metric type: L2");
            space = new hnswlib::L2Space(dimension);
            break;
        case (Metrics::IP):
            LOG_INFO(log, "searching in HNSW, metric type: IP");
            space = new hnswlib::InnerProductSpace(dimension);
            break;
        case (Metrics::Cosine):
            LOG_INFO(log, "searching in HNSW, metric type: Cosine");
            space = new hnswlib::CosineSpace(dimension);
            break;
    }
    setRawData(bi);
    IndexReader reader;
    reader.data = bi->data;
    reader.total = bi->size;
    index = std::make_shared<hnswlib::HierarchicalNSW<float>>(space);
    index->loadIndex(reader, space, total_vec + 1);
    index->manage_own_fields = false;
}

void * HNSWIndex::convertInnerBitMap(GeneralBitMapPtr outerBitMap)
{
    /// handle this pointer carefully! remember to deconstruct it somewhere
    hnswlib::bitMap * new_map = new hnswlib::bitMap(outerBitMap->get_size(), outerBitMap->bitmap);
    return new_map;
}

BinaryPtr HNSWIndex::convertStructToBinary(uint8_t * index_data, uint64_t written_size)
{
    BinaryPtr serial_index = std::make_shared<Binary>();
    serial_index->data = index_data;
    serial_index->size = written_size;
    return serial_index;
}

VectorDatasetPtr HNSWIndex::getInMemVectors()
{
    return nullptr;
}

AccParametersPack HNSWIndex::exploreTask(
    const float * query_data,
    const int64_t * gt,
    int topK,
    int query_size,
    bool oneRecall,
    std::mutex & m,
    std::condition_variable & cv,
    bool & go,
    Poco::Logger * log)
{
    ///TODO implement
    return AccParametersPack();
}

void HNSWIndex::getMyParameters(Parameters params)
{
    if (params.contains("m"))
    {
        neighbor = StoI(params.find("m")->second);
        params.erase("m");
    }
    if (params.contains("ef_c"))
    {
        ef_c = StoI(params.find("ef_c")->second);
        params.erase("ef_c");
    }
    if (params.contains("metric_type"))
    {
        params.erase("metric_type");
    }
    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::HNSWFLAT);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
}

int64_t HNSWIndex::removeWithIds(int64_t n, int64_t * ids)
{
#pragma omp parallel for
    for (int64_t i = 0; i < n; i++)
    {
        index->markDelete(ids[i]);
    }
    /// HNSW does mark for delete, so it will delete required number of items
    return n;
}

bool HNSWIndex::compare(const VectorIndex & other)
{
    const HNSWIndex * other_p = dynamic_cast<const HNSWIndex *>(&other);
    if (other_p == nullptr)
    {
        LOG_INFO(&Poco::Logger::get("HNSW"), "nullptr");
        return false;
    }
    if (other_p->ef_c != ef_c)
    {
        LOG_INFO(&Poco::Logger::get("HNSW"), "ef_c");
        return false;
    }
    if (other_p->me != me)
    {
        LOG_INFO(&Poco::Logger::get("HNSW"), "me");
        return false;
    }
    if (other_p->neighbor != neighbor)
    {
        LOG_INFO(&Poco::Logger::get("HNSW"), "neighbor");
        return false;
    }
    if (other_p->dimension != dimension)
    {
        LOG_INFO(&Poco::Logger::get("HNSW"), "dimension");
        return false;
    }
    return true;
}

}
