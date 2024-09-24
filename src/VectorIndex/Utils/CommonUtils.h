#pragma once

#include <base/types.h>
#include <Poco/String.h>
#include <Interpreters/Context_fwd.h>

namespace Search
{
enum class DataType;
}

namespace DB
{

const String BATCH_DISTANCE_FUNCTION = "batch_distance";
const String DISTANCE_FUNCTION = "distance";
const String TEXT_SEARCH_FUNCTION = "textsearch";
const String HYBRID_SEARCH_FUNCTION = "hybridsearch";

const String SCORE_COLUMN_NAME = "bm25_score";

/// Different search types
enum class HybridSearchFuncType
{
    UNKNOWN_FUNC = 0,
    VECTOR_SCAN,
    TEXT_SEARCH,
    HYBRID_SEARCH
};

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

inline bool isDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(DISTANCE_FUNCTION) == 0;
}

inline bool isBatchDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(BATCH_DISTANCE_FUNCTION) == 0;
}

inline bool isVectorScanFunc(const String & func)
{
    return isDistance(func) || isBatchDistance(func);
}

inline bool isTextSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(TEXT_SEARCH_FUNCTION) == 0;
}

inline bool isHybridSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(HYBRID_SEARCH_FUNCTION) == 0;
}

inline bool isHybridSearchFunc(const String & func)
{
    return isVectorScanFunc(func) || isTextSearch(func) || isHybridSearch(func);
}

inline bool isRelativeScoreFusion(const String & fusion_type)
{
    String type = Poco::toLower(fusion_type);
    return type.find("rsf") == 0;
}

inline bool isRankFusion(const String & fusion_type)
{
    String type = Poco::toLower(fusion_type);
    return type.find("rrf") == 0;
}

inline bool isScoreColumnName(const String & col_name)
{
    return col_name == SCORE_COLUMN_NAME;
}

Search::DataType getSearchIndexDataType(DataTypePtr &data_type);

void checkVectorDimension(const Search::DataType & search_type, const uint64_t & dim);

void checkTextSearchColumnDataType(DataTypePtr &data_type, bool & is_mapKeys);

#if USE_TANTIVY_SEARCH
void collectStatisticForBM25Calculation(ContextMutablePtr & context, String cluster_name, String database_name, String table_name, String query_column_name, String query_text);
void parseBM25StaisiticsInfo(const Block & block, size_t row, UInt64 & total_docs, std::map<UInt32, UInt64> & total_tokens_map, std::map<std::pair<UInt32, String>, UInt64> & terms_freq_map);
#endif

}
