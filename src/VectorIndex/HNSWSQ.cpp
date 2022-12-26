#include "HNSWSQ.h"
#include <faiss/index_io.h>
#include "IndexException.h"
#include "IndexReader.h"
#include "IndexWriter.h"
#include <VectorIndex/VectorIndexCommon.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
extern const int EMPTY_DATA_PASSED;
}

namespace VectorIndex
{
HNSWsq::HNSWsq(IndexType it_, IndexMode im_, Metrics me_, int dimension_, Parameters parameters) : VectorIndex(it_, im_, me_, dimension_)
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
            // metrictype = faiss::METRIC_Cosine;
            throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, "unsupported metric_type COSINE");
    }
    index = std::make_shared<faiss::IndexHNSWfastSQ>(dimension, quantizer, neighbor, metrictype);
    index->hnsw.efConstruction = ef_c;
    index->own_fields = true;
}

void HNSWsq::train(const VectorDatasetPtr dataset, int64_t total)
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

void HNSWsq::addWithoutId(VectorDatasetPtr dataset)
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

void HNSWsq::search(
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
        std::string message = generateUnsupportedParameters(params, IndexType::HNSWSQ);
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
BinaryPtr HNSWsq::serialize(size_t max_bytes_to_serialize, bool & finished)
{
    IndexWriter writer;
    faiss::write_index_incremental(index.get(), &writer, max_bytes_to_serialize, finished);
    return convertStructToBinary(writer.data, writer.actual_size);
}

void HNSWsq::load(BinaryPtr & bi, int64_t /*total_vec*/)
{
    if (bi->size == 0 || bi->data == nullptr)
    {
        throw IndexException(DB::ErrorCodes::EMPTY_DATA_PASSED, "load: failed with empty data");
    }
    IndexReader reader;
    reader.data = bi->data;
    reader.total = bi->size;

    index.reset(reinterpret_cast<faiss::IndexHNSWfastSQ *>(faiss::read_index(&reader)));
}

void * HNSWsq::convertInnerBitMap(GeneralBitMapPtr outerBitMap)
{
    /// handle this pointer carefully! remember to deconstruct it somewhere
    faiss::bitMap * new_map = new faiss::bitMap(outerBitMap->get_size(), outerBitMap->bitmap);
    return new_map;
}

BinaryPtr HNSWsq::convertStructToBinary(uint8_t * index_data, size_t written_size)
{
    BinaryPtr serial_index = std::make_shared<Binary>();
    serial_index->data = index_data;
    serial_index->size = written_size;
    return serial_index;
}

void HNSWsq::getMyParameters(Parameters p)
{
    if (p.contains("ef_c"))
    {
        ef_c = StoI(p.find("ef_c")->second);
        p.erase("ef_c");
    }
    if (p.contains("m"))
    {
        neighbor = StoI(p.find("m")->second);
        p.erase("m");
    }
    if (p.contains("bit_size"))
    {
        String bits = p.find("bit_size")->second;
        quantizer = parse_SQ_string(bits);
        p.erase("bit_size");
    }
    if (p.contains("metric_type"))
    {
        p.erase("metric_type");
    }
    if (!p.empty())
    {
        std::string message = generateUnsupportedParameters(p, IndexType::HNSWSQ);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
}
VectorDatasetPtr HNSWsq::getInMemVectors()
{
    return nullptr;
}

AccParametersPack HNSWsq::exploreTask(
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
faiss::ScalarQuantizer::QuantizerType HNSWsq::parse_SQ_string(String bits)
{
    if (bits == "8bit")
    {
        return faiss::ScalarQuantizer::QT_8bit;
    }
    else if (bits == "6bit")
    {
        return faiss::ScalarQuantizer::QT_6bit;
    }
    else if (bits == "4bit")
    {
        return faiss::ScalarQuantizer::QT_4bit;
    }
    else if (bits == "8bit_uniform")
    {
        return faiss::ScalarQuantizer::QT_8bit_uniform;
    }
    //    else if (bits == "8bit_direct")
    //    {
    //        return faiss::ScalarQuantizer::QT_8bit_direct;
    //    }
    else if (bits == "4bit_uniform")
    {
        return faiss::ScalarQuantizer::QT_4bit_uniform;
    }
    else if (bits == "QT_fp16")
    {
        return faiss::ScalarQuantizer::QT_fp16;
    }
    else
    {
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, "unsupported QT bit size in HNSWSQ: {}", bits);
    }
}

int64_t HNSWsq::removeWithIds(int64_t n, int64_t * ids)
{
    faiss::IDSelectorBatch batch_selector(n, ids);
    int64_t removed = index->remove_ids(batch_selector);
    return removed;
}

bool HNSWsq::compare(const VectorIndex & other)
{
    const HNSWsq * other_p = dynamic_cast<const HNSWsq *>(&other);
    if (other_p == nullptr)
    {
        return false;
    }
    if (other_p->quantizer != quantizer)
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
