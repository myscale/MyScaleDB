#include "HNSWPQ.h"
#include <faiss/index_io.h>
#include "IndexException.h"
#include "IndexReader.h"
#include "IndexWriter.h"
#include <VectorIndex/VectorIndexCommon.h>

namespace VectorIndex
{
HNSWpq::HNSWpq(IndexType it_, IndexMode im_, Metrics me_, int dimension_, Parameters parameters) : VectorIndex(it_, im_, me_, dimension_)
{
    faiss::MetricType metrictype;

    getMyParameters(parameters);
    switch (me)
    {
        case (Metrics::L2):
            metrictype = faiss::METRIC_L2;
            break;
        case (Metrics::IP):
            metrictype = faiss::METRIC_INNER_PRODUCT;
            break;
        case (Metrics::Cosine):
            metrictype = faiss::METRIC_Cosine;
    }

    if (dimension == -1)
    {
        dimension = pq_m;
    }
    index = std::make_shared<faiss::IndexHNSWfastPQ>(dimension, pq_m, bit_size, neighbor, metrictype);
    index->hnsw.efConstruction = ef_c;
    index->own_fields = true;
}

void HNSWpq::train(const VectorDatasetPtr dataset, int64_t total)
{
    if (index != nullptr)
    {
        index->init_hnsw(total);
        index->train(dataset->getVectorNum(), dataset->getData());
    }
    else
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "train: index not intialized");
    }
}

void HNSWpq::addWithoutId(VectorDatasetPtr dataset)
{
    if (index != nullptr)
    {
        index->add(dataset->getVectorNum(), dataset->getData());
        total_vector += dataset->getVectorNum();
    }
    else
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "addWithoutId: index not intialized");
    }
}

void HNSWpq::search(
    const VectorDatasetPtr dataset,
    const int32_t topK,
    float * distances,
    int64_t * result_id,
    Parameters & params,
    GeneralBitMapPtr filter)
{
    if (index == nullptr)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "search: index not intialized");
    }

    faiss::bitMapPtr inner_bit_map = std::shared_ptr<faiss::bitMap>();
    inner_bit_map.reset(reinterpret_cast<faiss::bitMap *>(convertInnerBitMap(filter)));
    int32_t num_query = dataset->getVectorNum();
    float * query_datas = dataset->getData();

    //TODO make dynamic or user defined
    int64_t ef_s = topK;
    if (params.contains("ef_s"))
    {
        ef_s = std::max(ef_s, StoI(params.find("ef_s")->second));
        params.erase("ef_s");
    }
    if (params.contains("metric_type"))
    {
        params.erase("metric_type");
    }
    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::HNSWPQ);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
    int num_thread = 1;
    if (num_query > 1)
    {
        num_thread = std::max(1, (num_thread_for_vector / count.load()));
    }
    omp_set_num_threads(num_thread);
    index->search(num_query, query_datas, topK, distances, result_id, ef_s, inner_bit_map.get());
    //distance might not be useful in many cases
}

BinaryPtr HNSWpq::serialize(size_t max_bytes_to_serialize, bool & finished)
{
    IndexWriter writer;
    faiss::write_index_incremental(index.get(), &writer, max_bytes_to_serialize, finished);
    return convertStructToBinary(writer.data, writer.actual_size);
}

void HNSWpq::load(BinaryPtr & bi, int64_t /*total_vec*/)
{
    if (bi->size == 0 || bi->data == nullptr)
    {
        throw IndexException(DB::ErrorCodes::EMPTY_DATA_PASSED, "load: failed with empty data");
    }
    setRawData(bi);
    IndexReader reader;
    reader.data = bi->data;
    reader.total = bi->size;

    index.reset(reinterpret_cast<faiss::IndexHNSWfastPQ *>(faiss::read_index(&reader)));

    /// reinterpret_cast might seem fishy, but when they returned from read_index they initially
    /// created a child class then cast it to Index.
}

void * HNSWpq::convertInnerBitMap(GeneralBitMapPtr outerBitMap)
{
    /// handle this pointer carefully! remember to deconstruct it somewhere
    faiss::bitMap * new_map = new faiss::bitMap(outerBitMap->get_size(), outerBitMap->bitmap);
    return new_map;
}

BinaryPtr HNSWpq::convertStructToBinary(uint8_t * index_data, size_t written_size)
{
    BinaryPtr serial_index = std::make_shared<Binary>();
    serial_index->data = index_data;
    serial_index->size = written_size;
    return serial_index;
}

void HNSWpq::getMyParameters(Parameters p)
{
    if (p.contains("ef_c"))
    {
        ef_c = StoI(p.find("ef_c")->second);
        p.erase("ef_c");
    }
    if (p.contains("pq_m"))
    {
        pq_m = StoI(p.find("pq_m")->second);
        p.erase("pq_m");
    }
    if (p.contains("m"))
    {
        neighbor = StoI(p.find("m")->second);
        p.erase("m");
    }
    if (p.contains("bit_size"))
    {
        bit_size = StoI(p.find("bit_size")->second);
        p.erase("bit_size");
    }
    if (p.contains("metric_type"))
    {
        p.erase("metric_type");
    }
    if (!p.empty())
    {
        std::string message = generateUnsupportedParameters(p, IndexType::HNSWPQ);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
}
VectorDatasetPtr HNSWpq::getInMemVectors()
{
    return nullptr;
}

AccParametersPack HNSWpq::exploreTask(
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
    (void)query_data;
    (void)gt;
    (void)topK;
    (void)query_size;
    (void)oneRecall;
    (void)m;
    (void)cv;
    (void)go;
    (void)log;
    return AccParametersPack();
}

int64_t HNSWpq::removeWithIds(int64_t n, int64_t * ids)
{
    faiss::IDSelectorBatch batch_selector(n, ids);
    int64_t removed = index->remove_ids(batch_selector);
    return removed;
}

bool HNSWpq::compare(const VectorIndex & other)
{
    const HNSWpq * other_p = dynamic_cast<const HNSWpq *>(&other);
    if (other_p == nullptr)
    {
        return false;
    }
    if (other_p->bit_size != bit_size)
    {
        return false;
    }
    if (other_p->pq_m != pq_m)
    {
        return false;
    }
    if (other_p->ef_c != ef_c)
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

}
