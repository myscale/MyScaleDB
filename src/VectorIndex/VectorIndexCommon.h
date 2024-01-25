#pragma once
#include <cmath>
#include <iostream>
#include <string>
#include <lib/lz4.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Common/Exception.h>
#include <Interpreters/VectorScanDescription.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wfloat-conversion"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#include <SearchIndex/VectorSearch.h>
#pragma clang diagnostic pop
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#pragma GCC diagnostic pop

#include <SearchIndex/SearchIndexCommon.h>
#include <SearchIndex/VectorIndex.h>

#define VECTOR_INDEX_FILE_SUFFIX ".vidx3"
#define MAX_BRUTE_FORCE_SEARCH_SIZE 50000
#define MIN_SEGMENT_SIZE 1000000
#define VECTOR_INDEX_DESCRIPTION "vector_index_description"
#define VECTOR_INDEX_CHECKSUMS "vector_index_checksums"
#define DECOUPLE_OWNER_PARTS_RESTORE_PREFIX "restore"
#define DISK_MODE_PARAM "disk_mode"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}
namespace VectorIndex
{

using SearchFloatVectorIndex = Search::VectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::FloatVector>;
using FloatVectorIndexPtr = std::shared_ptr<SearchFloatVectorIndex>;

using SearchBinaryVectorIndex = Search::VectorIndex<Search::AbstractIStream, Search::AbstractOStream, Search::DenseBitmap, Search::DataType::BinaryVector>;
using BinaryVectorIndexPtr = std::shared_ptr<SearchBinaryVectorIndex>;

using VectorIndexVariantPtr = std::variant<FloatVectorIndexPtr, BinaryVectorIndexPtr>;

/// VectorSearchTypeMap maps VectorSearchType enum values actual types
template <DB::VectorSearchType>
struct VectorSearchTypeMap;

template <>
struct VectorSearchTypeMap<DB::VectorSearchType::Float32Vector>
{
    using VectorDatasetType = float;
    using IndexDatasetType = float;
    using VectorIndexPtr = FloatVectorIndexPtr;
};

template <>
struct VectorSearchTypeMap<DB::VectorSearchType::BinaryVector>
{
    using VectorDatasetType = uint8_t;
    using IndexDatasetType = bool;
    using VectorIndexPtr = BinaryVectorIndexPtr;
};

const int DEFAULT_TOPK = 30;

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

static inline std::string getVectorIndexChecksumsFileName(const std::string & index_name)
{
    return index_name + "-" + VECTOR_INDEX_CHECKSUMS + VECTOR_INDEX_FILE_SUFFIX;
}

static inline std::string getVectorIndexDescriptionFileName(const std::string & index_name)
{
    return index_name + "-" + VECTOR_INDEX_DESCRIPTION + VECTOR_INDEX_FILE_SUFFIX;
}

static inline std::string getDecoupledVectorIndexDescriptionFileName(const std::string & index_name, const int & old_part_id, const std::string & old_part_name)
{
    return "merged-" + std::to_string(old_part_id) + "-" + old_part_name + "-" + getVectorIndexDescriptionFileName(index_name);
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
    else if (upper == "BINARYFLAT")
        return Search::IndexType::BinaryFLAT;
    else if (upper == "BINARYMSTG")
        return Search::IndexType::BinaryMSTG;
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown index type: {}", type);
}

inline Search::Metric getMetric(const std::string & metric, DB::VectorSearchType search_type)
{
    auto upper = Poco::toUpper(metric);
    switch (search_type)
    {
        case DB::VectorSearchType::Float32Vector:
            if (upper == "L2")
                return Search::Metric::L2;
            else if (upper == "IP")
                return Search::Metric::IP;
            else if (upper == "COSINE")
                return Search::Metric::Cosine;
            break;
        case DB::VectorSearchType::BinaryVector:
            if (upper == "HAMMING")
                return Search::Metric::Hamming;
            else if (upper == "JACCARD")
                return Search::Metric::Jaccard;
            break;
        default:
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported vector search type");
    }
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown metric type: {}", metric);
}

inline void verifyVectorIndexType(const String &index_type, const DB::VectorSearchType &search_type)
{
    auto search_index_type = getIndexType(index_type);
    switch (search_type)
    {
        case DB::VectorSearchType::Float32Vector:
        {
            auto types = Search::FLOAT_VECTOR_INDEX_TEST_TYPES;
            if (std::find(types.begin(), types.end(), search_index_type) == types.end())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Wrong vector index type for Float32 vector: {}", index_type);
            break;
        }
        case DB::VectorSearchType::BinaryVector:
        {
            auto types = Search::BINARY_VECTOR_INDEX_TYPES;
            if (std::find(types.begin(), types.end(), search_index_type) == types.end())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Wrong vector index type for Binary vector: {}", index_type);
            break;
        }
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type");
    }
}

}
