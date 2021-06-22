#pragma once
#include <cmath>
#include <iostream>
#include <string>
#include <lz4.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>

#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>

#include <VectorIndex/BruteForceSearch.h>
#include <VectorIndex/GeneralBitMap.h>
#include <VectorIndex/IOReader.h>
#include <VectorIndex/IOWriter.h>
#include <VectorIndex/VectorIndexFactory.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#pragma GCC diagnostic pop

#define VECTOR_INDEX_FILE_SUFFIX ".vidx"
#define MAX_BRUTE_FORCE_SEARCH_SIZE 50000
#define MIN_SEGMENT_SIZE 1000000
#define VECTOR_INDEX_READY "vector_index_ready"
#define VECTOR_INDEX_BITMAP "vector_bitMap"

namespace VectorIndex
{
///for now, we stick with std implementation
static inline int64_t StoI(const String& text)
{
    return std::stoll(text);
}

static inline std::string ItoS(int64_t i)
{
    return std::to_string(i);
}

static inline float StoF(const String& text)
{
    return std::stof(text);
}

static inline std::string FtoS(float f)
{
    return std::to_string(f);
}

static inline std::string ParametersToString(const Parameters& params)
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

static inline std::string AccParametersPackToString(const AccParametersPack& pack)
{
    rapidjson::StringBuffer strBuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strBuf);
    writer.StartObject();
    for (auto & pair : pack)
    {
        writer.Key(FtoS(pair.first).c_str());
        writer.StartObject();
        for (auto & para : pair.second)
        {
            writer.Key(para.first.c_str());
            writer.String(para.second.c_str());
        }
        writer.EndObject();
    }
    writer.EndObject();
    return strBuf.GetString();
}

static inline AccParametersPack StringToAccParametersPack(const String& raw, Poco::Logger * log)
{
    AccParametersPack pack;
    rapidjson::Document doc;
    doc.Parse(raw.c_str());
    for (auto & m : doc.GetObject())
    {
        LOG_TRACE(log, "{}", m.name.GetString());
        std::unordered_map<std::string, std::string> params;
        for (auto m2 = m.value.MemberBegin(); m2 != m.value.MemberEnd(); m2++)
        {
            LOG_TRACE(log, "{}", m2->name.GetString());
            LOG_TRACE(log, "{}", m2->value.GetString());
            params.insert(std::make_pair(m2->name.GetString(), m2->value.GetString()));
        }
        pack.insert(std::make_pair(StoF(m.name.GetString()), params));
    }
    return pack;
}

static inline Parameters convertPocoJsonToMap(Poco::JSON::Object::Ptr json)
{
    Parameters params;
    if (json)
    {
        for (Poco::JSON::Object::ConstIterator it = json->begin(); it != json->end(); it++)
        {
            params.insert(std::make_pair(it->first, it->second.toString()));
        }
    }

    return params;
}

static inline std::string generateUnsupportedParameters(const Parameters& params, IndexType type)
{
    std::string message = "These parameters are not supported in Index type ";
    message = message + VectorIndexFactory::typeToString(type) + " : ";
    for (auto & each : params)
    {
        message = message + each.first + " : ";
        message = message + each.second + ", ";
    }
    return message;
}

static inline std::unordered_map<String, int64_t> readVectorIndexReadyFile(
    IOReader & reader, const String& ready_file, const std::vector<String>& index_name, std::unordered_map<String, Parameters> & index_parameters)
{
    std::unordered_map<String, int64_t> pair;
    if (!reader.open(ready_file + VECTOR_INDEX_FILE_SUFFIX))
    {
        if (!reader.open(ready_file))
        {
            return pair;
        }
    }
    std::unique_ptr<char[]> chars(new char[reader.length()]);
    reader.read(chars.get(), reader.length());
    String lines(chars.get(), reader.length());
    std::stringstream stream(lines);
    std::string aline;
    while (std::getline(stream, aline))
    {
        int64_t original_index_size = -1;
        for (auto & one_index_name : index_name)
        {
            if (static_cast<int>(aline.find(one_index_name)) != -1)
            {
                std::string temp_string;
                std::string type;
                int c = 0;
                ///If there are multiple copies of one_index_name due to any reasons, use the last occurence.
                index_parameters.insert_or_assign(one_index_name, Parameters());
                std::stringstream inner_stream(aline);
                while (std::getline(inner_stream, temp_string, ';'))
                {
                    if (static_cast<int>(temp_string.find(':')) != -1)
                    {
                        break;
                    }
                    if (c == 0)
                    {
                        type = temp_string;
                        index_parameters.find(one_index_name)->second.insert(std::make_pair("type", type));
                        c++;
                    }
                    else if (c == 1)
                    {
                        std::string para_string;
                        std::string para_string2;
                        bool even = true;
                        std::stringstream innermost_stream(temp_string);
                        while (std::getline(innermost_stream, para_string, ','))
                        {
                            if (even)
                            {
                                para_string2 = para_string;
                                even = false;
                            }
                            else
                            {
                                index_parameters.find(one_index_name)->second.insert(std::make_pair(para_string2, para_string));
                                even = true;
                            }
                        }
                        c++;
                    }
                }
                std::string length_string = aline.substr(aline.find(':') + 1, aline.length()); ///index_name:1234
                original_index_size = StoI(length_string);
                pair.insert_or_assign(one_index_name, original_index_size);
                break;
            }
        }
    }
    reader.close();
    return pair;
}

static inline GeneralBitMapPtr mergeBitMap(GeneralBitMapPtr left, GeneralBitMapPtr right)
{
    DB::OpenTelemetry::SpanHolder span("mergeBitMap");
    int64_t vector_count = left->get_size();
    GeneralBitMapPtr after_merge = std::make_shared<GeneralBitMap>();
    char * bits = new char[(vector_count >> 3) + 1]; // size/8 = bytes
    char * left_bits = left->bitmap;
    char * right_bits = right->bitmap;
    size_t bit_size = 0;
    for (int64_t i = 0; i < (vector_count >> 3) + 1; ++i)
    {
        bits[i] = left_bits[i] & right_bits[i];
        if (bits[i])
        {
            ++bit_size;
        }
    }
    Poco::Logger * log = &Poco::Logger::get("mergeBitMap");
    LOG_DEBUG(log, "[mergeBitMap] bit size: {}, vector_count: {}", bit_size, vector_count);
    after_merge->bitmap = bits;
    after_merge->size = vector_count;
    return after_merge;
}

static inline size_t compressBound(int64_t raw_size, uint8_t cmb)
{
    int64_t max_precompress_size = 0;
    if (cmb == static_cast<UInt8>(DB::CompressionMethodByte::LZ4))
    {
        max_precompress_size = LZ4_MAX_INPUT_SIZE;
    }
    else if (cmb == static_cast<UInt8>(DB::CompressionMethodByte::NONE))
    {
        max_precompress_size = (1LL << 32) - 10;
    }
    return max_precompress_size < raw_size ? max_precompress_size : raw_size;
}

using rng_type = std::mt19937;
static inline void getQueryandGt(
    VectorDatasetPtr base, float * gt_dis, int64_t * gt, float * query, int default_topk, int default_query_size, Metrics default_metrics)
{
    std::vector<int64_t> query_ids;
    query_ids.reserve(default_query_size);
    ///might be fewer
    GeneralBitMapPtr bits = std::make_shared<GeneralBitMap>(base->getVectorNum());
    memset(bits->bitmap, 255, (base->getVectorNum() / 8) + 1);
    ///might be fewer

    float * base_data = base->getData();
    std::uniform_int_distribution<> udist(0, base->getVectorNum());
    rng_type rng;
    int dimension = base->getDimension();
    for (int i = 0; i < default_query_size; i++)
    {
        int index = udist(rng);
        query_ids.emplace_back(index);
        for (int k = 0; k < dimension; k++)
        {
            query[i * dimension + k] = base_data[index * dimension + k];
        }
    }
    ///we extract query_size random vectors from base, use them as query to test against base so we can produce a ground truth table.
    ///we are not using the ids of those query directly because some metric type will make a vector have greater distance with itself
    ///than others (such as IP).
    tryBruteForceSearch(query, base_data, dimension, default_topk, default_query_size, base->getVectorNum(), gt, gt_dis, default_metrics);
}

inline String str_toupper(String s)
{
    std::transform(
        s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); } // correct
    );
    return s;
}

}
