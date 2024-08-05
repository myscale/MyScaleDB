#pragma once

#include <base/types.h>
#include <Poco/String.h>

namespace Search
{
enum class DataType;
}

namespace DB
{

const String SCORE_COLUMN_NAME = "bm25_score";

/// Different search types
enum class HybridSearchFuncType
{
    VECTOR_SCAN = 0,
    TEXT_SEARCH,
    HYBRID_SEARCH,
    UNKNOWN_FUNC
};

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

inline bool isDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find("distance") == 0;
}

inline bool isBatchDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find("batch_distance") == 0;
}

inline bool isVectorScanFunc(const String & func)
{
    return isDistance(func) || isBatchDistance(func);
}

inline bool isTextSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find("textsearch") == 0;
}

inline bool isHybridSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find("hybridsearch") == 0;
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

}
