#include "IVFPQIndex.h"
#include <omp.h>
#include "CacheManager.h"
#include "IndexException.h"
#include "IndexReader.h"
#include "IndexWriter.h"
#include "faiss/IndexIVFPQFilter.h"
#include "faiss/impl/AuxIndexStructures.h"
#include <VectorIndex/VectorIndexCommon.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
}

namespace VectorIndex
{
void IVFPQIndex::train(VectorDatasetPtr dataset, int64_t total)
{
    faiss::MetricType metrictype;
    switch (me)
    {
        case Metrics::L2:
            metrictype = faiss::METRIC_L2;
            break;
        case Metrics::IP:
            metrictype = faiss::METRIC_INNER_PRODUCT;
            break;
        case Metrics::Cosine:
            metrictype = faiss::METRIC_Cosine;
    }
    faiss::IndexFlat * coarse_quantizer = new faiss::IndexFlat(dimension, metrictype);
    int nlist = ncentroids > dataset->getVectorNum() / min_centroid_size ? dataset->getVectorNum() / min_centroid_size : ncentroids;
    nlist = std::max(1, nlist);
    index = std::make_shared<faiss::IndexIVFPQFilter>(coarse_quantizer, dimension, nlist, M, bit_size, metrictype);
    reinterpret_cast<faiss::IndexIVFPQFilter *>(index.get())->own_fields = true;
    index->train(dataset->getVectorNum(), dataset->getData());
}

void IVFPQIndex::addWithoutId(VectorDatasetPtr dataset)
{
    if (index != nullptr)
    {
        int64_t * ids = new int64_t[dataset->getVectorNum()];
        for (int i = 0; i < dataset->getVectorNum(); i++)
        {
            ids[i] = total_vector + i;
        }
        index->add_with_ids(dataset->getVectorNum(), dataset->getData(), ids);
        total_vector += dataset->getVectorNum();
        delete[] ids;
    }
    else
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "addWithoutId: index not intialized");
    }
}

void IVFPQIndex::search(
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

    int nprobe = 1;
    if (params.contains("nprobe"))
    {
        nprobe = StoI(params.find("nprobe")->second);
        params.erase("nprobe");
    }
    if (params.contains("metric_type"))
    {
        params.erase("metric_type");
    }

    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::IVFFLAT);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
    faiss::IVFSearchParameters ivf_params;
    ivf_params.nprobe = nprobe;

    /// we have two ways to optimize parallelizations for IVF.
    /// first case, when we have very few connections, each sending a vector scan request that contains a single query vector.
    /// In this case, we parallelize over centroids.
    int current_running_task = count.load(std::memory_order_relaxed);
    if (num_query <= num_thread_for_vector)
    {
        ivf_params.parallel_mode = 1;
    }
    /// otherwise, which are when we have large number of connections,
    /// we follow the default parallel mode which is parallel by query.
    else
    {
        ivf_params.parallel_mode = 0;
    }
    omp_set_num_threads(std::max(1, (num_thread_for_vector / current_running_task)));
    reinterpret_cast<faiss::IndexIVFPQFilter *>(index.get())
        ->search(num_query, query_datas, topK, distances, result_id, &ivf_params, inner_bit_map.get());
    //distance might not be useful in many cases
}


VectorDatasetPtr IVFPQIndex::getInMemVectors()
{
    return nullptr;
}


void IVFPQIndex::getMyParameters(Parameters params)
{
    if (params.contains("ncentroids"))
    {
        ncentroids = StoI(params.find("ncentroids")->second);
        params.erase("ncentroids");
    }
    if (params.contains("M"))
    {
        M = StoI(params.find("M")->second);
        params.erase("M");
    }
    if (params.contains("bit_size"))
    {
        bit_size = StoI(params.find("bit_size")->second);
        params.erase("bit_size");
    }
    if (params.contains("metric_type"))
    {
        params.erase("metric_type");
    }

    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::IVFPQ);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
}

bool IVFPQIndex::compare(const VectorIndex & other)
{
    const IVFPQIndex * other_p = dynamic_cast<const IVFPQIndex *>(&other);
    if (other_p == nullptr)
    {
        return false;
    }
    if (other_p->ncentroids != ncentroids)
    {
        return false;
    }
    if (other_p->M != M)
    {
        return false;
    }
    if (other_p->bit_size != bit_size)
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

//std::unordered_map<std::string, std::string> IVFPQIndex::exploreTask(const float * query_data, const int64_t * gt, int topK, int query_size,
//                                                                     bool oneReccall,std::mutex & m, std::condition_variable & cv, bool & go)
//{
//}


/*
void IVFPQIndex::remove(const int32_t * ids)
{
}
*/

}
