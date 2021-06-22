#include "VectorIndexFactory.h"
#include "FlatIndex.h"
#include "HNSWIndex.h"
#include "HNSWPQ.h"
#include "HNSWSQ.h"
#include "IVFFlatIndex.h"
#include "IVFPQIndex.h"
#include "IVFSQIndex.h"
#include "IndexException.h"

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace VectorIndex
{
VectorIndexPtr VectorIndexFactory::createIndex(IndexType it, IndexMode im, Metrics me, int dimension, Parameters parameters)
{
    if (it == IndexType::IVFFLAT)
    {
        return std::make_shared<IVFFlatIndex>(it, im, me, dimension, parameters);
    }
    else if (it == IndexType::IVFPQ)
    {
        return std::make_shared<IVFPQIndex>(it, im, me, dimension, parameters);
    }
    else if (it == IndexType::IVFSQ)
    {
        return std::make_shared<IVFSQIndex>(it, im, me, dimension, parameters);
    }
    else if (it == IndexType::FLAT)
    {
        return std::make_shared<FlatIndex>(it, im, me, dimension, parameters);
    }
    else if (it == IndexType::HNSWFLAT)
    {
        return std::make_shared<HNSWIndex>(it, im, me, dimension, parameters);
    }
    else if (it == IndexType::HNSWPQ)
    {
        return std::make_shared<HNSWpq>(it, im, me, dimension, parameters);
    }
    else if (it == IndexType::HNSWSQ)
    {
        return std::make_shared<HNSWsq>(it, im, me, dimension, parameters);
    }
    return nullptr;
}

bool VectorIndexFactory::typeExist(std::string index_type)
{
    return (
        index_type == "IVFFLAT" || index_type == "IVFPQ" || index_type == "IVFSQ" || index_type == "FLAT" || index_type == "HNSW"
        || index_type == "HNSWFLAT" || index_type == "HNSWPQ" || index_type == "HNSWSQ");
}

IndexType VectorIndexFactory::createIndexType(std::string index_type)
{
    if (index_type == "IVFFLAT")
    {
        return IndexType::IVFFLAT;
    }
    if (index_type == "IVFPQ")
    {
        return IndexType::IVFPQ;
    }
    if (index_type == "IVFSQ")
    {
        return IndexType::IVFSQ;
    }
    if (index_type == "FLAT")
    {
        return IndexType::FLAT;
    }
    if (index_type == "HNSW" || index_type == "HNSWFLAT")
    {
        return IndexType::HNSWFLAT;
    }
    if (index_type == "HNSWPQ")
    {
        return IndexType::HNSWPQ;
    }
    if (index_type == "HNSWSQ")
    {
        return IndexType::HNSWSQ;
    }
    __builtin_unreachable();
}

std::string VectorIndexFactory::typeToString(IndexType it)
{
    if (it == IndexType::IVFFLAT)
    {
        return std::string("IVFFLAT");
    }
    else if (it == IndexType::IVFPQ)
    {
        return std::string("IVFPQ");
    }
    else if (it == IndexType::IVFSQ)
    {
        return std::string("IVFSQ");
    }
    else if (it == IndexType::FLAT)
    {
        return std::string("FLAT");
    }
    else if (it == IndexType::HNSWFLAT)
    {
        return std::string("HNSWFLAT");
    }
    else if (it == IndexType::HNSWPQ)
    {
        return std::string("HNSWPQ");
    }
    else if (it == IndexType::HNSWSQ)
    {
        return std::string("HNSWSQ");
    }
    return "";
}
std::string VectorIndexFactory::MetricToString(Metrics me)
{
    if (me == Metrics::L2)
    {
        return std::string("L2");
    }
    else if (me == Metrics::IP)
    {
        return std::string("IP");
    }
    else if (me == Metrics::Cosine)
    {
        return std::string("COSINE");
    }
    return "";
}

inline std::string str_toupper(std::string s)
{
    std::transform(
        s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); } // correct
    );
    return s;
}

Metrics VectorIndexFactory::createIndexMetrics(std::string index_metric)
{
    auto upper_case = str_toupper(index_metric);
    if (upper_case == "L2")
    {
        return Metrics::L2;
    }
    if (upper_case == "IP")
    {
        return Metrics::IP;
    }
    if (upper_case == "COSINE")
    {
        return Metrics::Cosine;
    }
    throw IndexException(DB::ErrorCodes::UNSUPPORTED_PARAMETER, "unknown metric_type {}", index_metric);
}

std::string VectorIndexFactory::modeToString(IndexMode mode)
{
    if (mode == IndexMode::CPU)
    {
        return std::string("CPU");
    }
    else if (mode == IndexMode::GPU)
    {
        return std::string("GPU");
    }
    else if (mode == IndexMode::FPGA)
    {
        return std::string("FPGA");
    }
    return "";
}

IndexMode VectorIndexFactory::createIndexMode(std::string index_mode)
{
    if (index_mode == "CPU")
    {
        return IndexMode::CPU;
    }
    if (index_mode == "GPU")
    {
        return IndexMode::GPU;
    }
    if (index_mode == "FPGA")
    {
        return IndexMode::FPGA;
    }
    __builtin_unreachable();
}
}
