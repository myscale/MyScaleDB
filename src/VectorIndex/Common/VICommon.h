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
#include <VectorIndex/Storages/VSDescription.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wfloat-conversion"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#pragma clang diagnostic ignored "-Wcovered-switch-default"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-function"
#include <SearchIndex/VectorSearch.h>
#pragma clang diagnostic pop
#endif

#include <VectorIndex/Common/VIException.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#pragma GCC diagnostic pop

#include <SearchIndex/SearchIndexCommon.h>
#include <SearchIndex/VectorIndex.h>

#define VECTOR_INDEX_FILE_SUFFIX ".vidx3"
#define VECTOR_INDEX_FILE_OLD_SUFFIX ".vidx2"
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
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
}
}

/// Convert search index exception to DB::Exception
#define VECTOR_INDEX_EXCEPTION_ADAPT(callable, func_name)               \
    try                                                                 \
    {                                                                   \
        callable;                                                       \
    }                                                                   \
    catch (const DB::Exception & e)                                     \
    {                                                                   \
        throw VectorIndex::VIException(                              \
            e.code(),                                                   \
            "Error in {}, {}", func_name, e.message());                 \
    }                                                                   \
    catch (const SearchIndexException & e)                              \
    {                                                                   \
        throw VectorIndex::VIException(                              \
            e.getCode(),                                                \
            "Error in {}, {}", func_name, e.what());                    \
    }                                                                   \
    catch (const std::exception & e)                                    \
    {                                                                   \
        throw VectorIndex::VIException(                              \
            DB::ErrorCodes::STD_EXCEPTION,                              \
            "Error in {}, {}",                                          \
            func_name, e.what());                                       \
    }                                                                   \
    catch (...)                                                         \
    {                                                                   \
        throw VectorIndex::VIException(                              \
            DB::ErrorCodes::UNKNOWN_EXCEPTION,                          \
            "Unknown error in {}.", func_name);                         \
    }

namespace Search
{
class DiskIOManager;
enum class DataType;

}
namespace VectorIndex
{

using RowIds = std::vector<UInt64>;
using RowSource = std::vector<uint8_t>;

using VectorIndexIStream = Search::AbstractIStream;
using VectorIndexOStream = Search::AbstractOStream;

using VIBitmap = Search::DenseBitmap;
using VIBitmapPtr = std::shared_ptr<Search::DenseBitmap>;

using SearchResult = Search::SearchResult;
using SearchResultPtr = std::shared_ptr<SearchResult>;

using VIParameter = Search::Parameters;
using VIType = Search::IndexType;
using VIMetric = Search::Metric;
using VIDataType = Search::DataType;

using DiskIOManager = Search::DiskIOManager;

/*
using SearchVectorIndex = Search::VectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::FloatVector>;
using VectorIndexPtr = std::shared_ptr<SearchVectorIndex>;
*/

/// [TODO] Convert the smart pointer type from shared_ptr to unique_ptr to prevent potential memory problems.
/// This change requires modifying search-index simultaneously.
using FloatInnerSegment = Search::VectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::FloatVector>;
using FloatInnerSegmentPtr = std::shared_ptr<FloatInnerSegment>;

using BinaryInnerSegment = Search::VectorIndex<VectorIndexIStream, VectorIndexOStream, VIBitmap, VIDataType::BinaryVector>;
using BinaryInnerSegmentPtr = std::shared_ptr<BinaryInnerSegment>;

using InnerSegmentVariantPtr = std::variant<FloatInnerSegmentPtr, BinaryInnerSegmentPtr>;

/// SearchIndexDataTypeMap maps Search::DataType enum values actual types
template <Search::DataType>
struct SearchIndexDataTypeMap;

template <Search::DataType T>
using VISourcePartReader = Search::IndexSourceDataReader<typename SearchIndexDataTypeMap<T>::IndexDatasetType>;

template <>
struct SearchIndexDataTypeMap<Search::DataType::FloatVector>
{
    using VectorDatasetType = float;
    using IndexDatasetType = float;
    using VectorIndexPtr = FloatInnerSegmentPtr;
};

template <>
struct SearchIndexDataTypeMap<Search::DataType::BinaryVector>
{
    using VectorDatasetType = uint8_t;
    using IndexDatasetType = bool;
    using VectorIndexPtr = BinaryInnerSegmentPtr;
};

const int DEFAULT_TOPK = 30;


inline VIType fallbackToFlat(const Search::DataType &search_type)
{
    switch (search_type)
    {
        case Search::DataType::FloatVector:
            return VIType::FLAT;
        case Search::DataType::BinaryVector:
            return VIType::BinaryFLAT;
        default:
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type");
    }
}

static inline std::string ParametersToString(const VIParameter & params)
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

static inline VIParameter convertPocoJsonToMap(Poco::JSON::Object::Ptr json)
{
    VIParameter params;
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

}
