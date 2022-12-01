#include "IVFFlatIndex.h"
#include <iostream>
#include <omp.h>
#include <Common/logger_useful.h>
#include "BruteForceSearch.h"
#include "CacheManager.h"
#include "IndexException.h"
#include "IndexReader.h"
#include "IndexWriter.h"
#include "faiss/impl/AuxIndexStructures.h"
#include "faiss/index_io.h"
#include "faiss/profile.h"
#include <VectorIndex/VectorIndexCommon.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
extern const int INCORRECT_INDEX;
}

namespace VectorIndex
{

void IVFFlatIndex::train(const VectorDatasetPtr dataset, int64_t total)
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
    LOG_INFO(&Poco::Logger::get("IVFFlatIndex"), "[build] nlist: {}", nlist);
    index = std::make_shared<faiss::IndexIVFFlatFilter>(coarse_quantizer, dimension, nlist, metrictype);
    faiss::IndexIVFFlatFilter * ivfflat = reinterpret_cast<faiss::IndexIVFFlatFilter *>(index.get());
    ivfflat->own_fields = true;
    if (profiler)
    {
        ivfflat->set_tune_mode();
        ivfflat->train(dataset->getVectorNum(), dataset->getData());
        ivfflat->set_tune_off();
    }
    else
    {
        LOG_INFO(&Poco::Logger::get("IVFFlatIndex"), "[build] profiler is false, vector num: {}", dataset->getVectorNum());
        ivfflat->train(dataset->getVectorNum(), dataset->getData());
    }
}

void IVFFlatIndex::addWithoutId(VectorDatasetPtr dataset)
{
    /// we skip the first add in IVFFlat, becuase we did it already in training.
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


void IVFFlatIndex::search(
    const VectorDatasetPtr dataset, int32_t topK, float * distances, int64_t * result_id, Parameters & params, GeneralBitMapPtr filter)
{
    if (index == nullptr)
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "search: index not intialized");
    }
    auto * index_real = reinterpret_cast<faiss::IndexIVFFlatFilter *>(index.get());

    faiss::bitMapPtr inner_bit_map = std::shared_ptr<faiss::bitMap>();
    inner_bit_map.reset(reinterpret_cast<faiss::bitMap *>(convertInnerBitMap(filter)));
    int32_t num_query = dataset->getVectorNum();
    float * query_datas = dataset->getData();

    int nprobe = 1;
    float acc = -1;
    if (params.contains("nprobe"))
    {
        nprobe = StoI(params.find("nprobe")->second);
        params.erase("nprobe");
        /// TODO: we want to scale down the nprobe by the proportion of nlist scale down.
        /// nprobe = std::max(1, static_cast<int>(nprobe * (static_cast<float>(index_real->nlist) / ncentroids)));
    }
    /// we can only have one of nprobe or acc, not both.
    else if (params.contains("acc"))
    {
        acc = StoF(params.find("acc")->second);
        params.erase("acc");
        if (!index_real->tuned)
        {
            throw IndexException(DB::ErrorCodes::INCORRECT_INDEX, "autotune is off, turn on profiler and rebuild index");
        }
        if (acc < 0 || acc > 1)
        {
            throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, "invalid acc {} for autotune", acc);
        }
        if (!index_real->tuned)
        {
            LOG_WARNING(&Poco::Logger::get("IVFFlatIndex"), "the index is too small to be tuned, not using accuracy bounding.");
            nprobe = INT32_MAX; ///since the datapart is too small, we might just search its entirety.
        }
    } 
    if (params.contains("metric_type"))
    {
        /// simply ignore it
        params.erase("metric_type");
    }
    
    if (!params.empty())
    {
        std::string message = generateUnsupportedParameters(params, IndexType::IVFFLAT);
        throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, message);
    }
    faiss::IVFSearchParameters ivf_params;
    int current_running_task = count.load(std::memory_order_relaxed);
    /// when we don't have acc bounding to do, we follow normal search route.
    if (acc == -1 || !index_real->tuned)
    {
        ivf_params.nprobe = nprobe;
        /// we have two ways to optimize parallelizations for IVF.
        /// first case, when we have very few connections, each sending a vector scan request that contains a single query vector.
        /// In this case, we parallelize over centroids.
        if (num_query <= num_thread_for_vector)
        {
            ivf_params.parallel_mode = 1;
        }
        /// experimental feature, parallelize over centroids with probes grouped together to use fast blas distance.
        /// please see the implementation for detail.
        else if (num_query * nprobe >= index_real->nlist * parallel_mode_4_threadhold)
        {
            ivf_params.parallel_mode = 4;
        }
        /// otherwise, which are when we have large number of connections,
        /// we follow the default parallel mode which is parallel by query.
        else
        {
            ivf_params.parallel_mode = 0;
        }
        omp_set_num_threads(std::max(1, (num_thread_for_vector / current_running_task)));
        LOG_DEBUG(
            &Poco::Logger::get("IVFFlatIndex"),
            "[search] nprobe: {}, parallel mode: {}, num_t: {}",
            nprobe,
            ivf_params.parallel_mode,
            num_thread_for_vector);
        index_real->search(num_query, query_datas, topK, distances, result_id, &ivf_params, inner_bit_map.get());
    }
    else
    {
        /// this is acc bounded search route
        ivf_params.acc = acc;
        omp_set_num_threads(std::max(1, (num_thread_for_vector / current_running_task)));
        LOG_DEBUG(
            &Poco::Logger::get("IVFFlatIndex"),
            "[search] acc requirement: {}, parallel mode: {}, num_t: {}",
            acc,
            ivf_params.parallel_mode,
            num_thread_for_vector);
        faiss::Error_sys profiled_index(index.get());
        profiled_index.search(num_query, query_datas, topK, distances, result_id, &ivf_params, inner_bit_map.get());
    }

    //distance might not be useful in many cases
}

VectorDatasetPtr IVFFlatIndex::getInMemVectors()
{
    return nullptr;
}

void IVFFlatIndex::getMyParameters(Parameters params)
{
    if (params.contains("ncentroids"))
    {
        ncentroids = StoI(params.find("ncentroids")->second);
        params.erase("ncentroids");
    }
    if (params.contains("profiler"))
    {
        profiler = str_toupper(params.find("profiler")->second) == "TRUE";
        params.erase("profiler");
    }
    if (params.contains("std_m"))
    {
        std_m = StoF(params.find("std_m")->second);
        params.erase("std_m");
    }
    if (params.contains("multiplier"))
    {
        multiplier = StoF(params.find("multiplier")->second);
        params.erase("multiplier");
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
}

bool IVFFlatIndex::compare(const VectorIndex & other)
{
    const IVFFlatIndex * other_p = dynamic_cast<const IVFFlatIndex *>(&other);
    if (other_p == nullptr)
    {
        return false;
    }
    if (other_p->ncentroids != ncentroids)
    {
        return false;
    }
    if (other_p->std_m != std_m)
    {
        return false;
    }
    if (other_p->multiplier != multiplier)
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

void IVFFlatIndex::tune(VectorDatasetPtr base, int topK)
{
    if (!profiler)
    {
        return;
    }
    if (auto * index_real = reinterpret_cast<faiss::IndexIVFFlatFilter *>(index.get()))
    {
        int default_query_size = base->getVectorNum();
        int default_topk = topK;
        std::vector<float> query(default_query_size * dimension);
        memcpy(query.data(), base->getData(), sizeof(float) * default_query_size * default_topk);
        std::vector<float> gt_dis(default_topk * default_query_size);
        std::vector<int64_t> gt(default_topk * default_query_size);
        LOG_INFO(&Poco::Logger::get("IVFFlatIndex"), "get gt for {} queries", default_query_size);
        faiss::bitMapPtr bits = std::make_shared<faiss::bitMap>(base->getVectorNum());
        memset(bits->bitmap, 255, (base->getVectorNum() / 8) + 1);
        faiss::IVFSearchParameters param;
        param.nprobe = index_real->nlist;
        param.parallel_mode = 4;
        if(me==Metrics::Cosine){
            index_real->metric_type = faiss::METRIC_INNER_PRODUCT;
            index_real->quantizer->metric_type = faiss::METRIC_INNER_PRODUCT;
            /// the relative distance of ip and cosine should be the same
        }
        index_real->search(default_query_size, query.data(), default_topk, gt_dis.data(), gt.data(), &param, bits.get());

        faiss::Error_sys profiled_index(index_real, default_query_size, default_topk);
        LOG_INFO(&Poco::Logger::get("IVFFlatIndex"), "training profiler");
        profiled_index.set_gt(gt_dis.data(), gt.data());
        profiled_index.sys_train(default_query_size, query.data(), std_m, multiplier);
        if(me==Metrics::Cosine){
            index_real->metric_type = faiss::METRIC_Cosine;
            index_real->quantizer->metric_type = faiss::METRIC_Cosine;
        }
        LOG_INFO(&Poco::Logger::get("IVFFlatIndex"), "profiler train completed");
    }
    else
    {
        throw IndexException(DB::ErrorCodes::LOGICAL_ERROR, "IVFFlat casting failed, this is logic error.");
    }
}
}
