#pragma once
#include <cmath>
#include <iostream>
#include <string>
#include <lz4.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Common/Exception.h>

#include <VectorIndex/BruteForceSearch.h>
#include <VectorIndex/IOReader.h>
#include <VectorIndex/IOWriter.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#pragma GCC diagnostic pop

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wextra-semi-stmt"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wc++20-compat"
#pragma GCC diagnostic ignored "-Walloca"
#pragma GCC diagnostic ignored "-Wmissing-noreturn"
#pragma GCC diagnostic ignored "-Wgcc-compat"
#pragma GCC diagnostic ignored "-Wcovered-switch-default"
#pragma GCC diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#pragma GCC diagnostic ignored "-Wextra-semi"
#pragma GCC diagnostic ignored "-Wfinal-dtor-non-final-class"
#pragma GCC diagnostic ignored "-Wundef"
#pragma GCC diagnostic ignored "-Wsuggest-override"
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#pragma GCC diagnostic ignored "-Wdeprecated-copy-with-user-provided-dtor"
#pragma GCC diagnostic ignored "-Wmismatched-tags"
#pragma GCC diagnostic ignored "-Wundefined-reinterpret-cast"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wshadow-uncaptured-local"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-private-field"
#pragma GCC diagnostic ignored "-Wshadow-field"
#pragma GCC diagnostic ignored "-Wdelete-non-abstract-non-virtual-dtor"
#pragma GCC diagnostic ignored "-Wshadow"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#pragma GCC diagnostic ignored "-Wrange-loop-bind-reference"
#pragma GCC diagnostic ignored "-Wenum-compare-switch"
#pragma GCC diagnostic ignored "-Wnewline-eof"
#pragma GCC diagnostic ignored "-Wreorder-ctor"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#include <SearchIndex/SearchIndexCommon.h>
#include <SearchIndex/VectorIndex.h>
#pragma GCC diagnostic pop

#define VECTOR_INDEX_FILE_SUFFIX ".vidx2"
#define MAX_BRUTE_FORCE_SEARCH_SIZE 50000
#define MIN_SEGMENT_SIZE 1000000
#define VECTOR_INDEX_READY "vector_index_ready"
#define VECTOR_INDEX_BITMAP "vector_bitmap"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}
namespace VectorIndex
{

const int DEFAULT_TOPK = 30;

using VectorIndexPtr = std::shared_ptr<
    Search::VectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::FloatVector>>;

static inline std::string ParametersToString(const Search::Parameters & params)
{
    rapidjson::StringBuffer strBuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strBuf);
    writer.StartObject();
    for (auto & param : params)
    {
        writer.Key(param.first.c_str());
        writer.String(param.second.c_str());
    }
    writer.EndObject();
    return strBuf.GetString();
}

static inline Search::Parameters convertPocoJsonToMap(Poco::JSON::Object::Ptr json)
{
    Search::Parameters params;
    if (json)
    {
        for (Poco::JSON::Object::ConstIterator it = json->begin(); it != json->end(); it++)
        {
            params.insert(std::make_pair(it->first, it->second.toString()));
        }
    }

    return params;
}

inline Search::IndexType getIndexType(const std::string & type)
{
    auto upper = Poco::toUpper(type);
    if (upper == "IVFFLAT")
        return Search::IndexType::IVFFLAT;
    else if (upper == "IVFPQ")
        return Search::IndexType::IVFPQ;
    else if (upper == "IVFSQ")
        return Search::IndexType::IVFSQ;
    else if (upper == "FLAT")
        return Search::IndexType::FLAT;
    else if (upper == "HNSWFLAT" || upper == "HNSWFASTFLAT")
        return Search::IndexType::HNSWfastFLAT;
    else if (upper == "HNSWPQ" || upper == "HNSWFASTPQ")
        return Search::IndexType::HNSWPQ;
    else if (upper == "HNSWSQ" || upper == "HNSWFASTSQ")
        return Search::IndexType::HNSWfastSQ;
    else if (upper == "MSTG")
        return Search::IndexType::MSTG;
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown index type: {}", type);
}

inline Search::Metric getMetric(const std::string & metric)
{
    auto upper = Poco::toUpper(metric);
    if (upper == "L2")
        return Search::Metric::L2;
    else if (upper == "IP")
        return Search::Metric::IP;
    else if (upper == "COSINE")
        return Search::Metric::Cosine;
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown metric type: {}", metric);
}

}
