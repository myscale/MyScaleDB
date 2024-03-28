#pragma once

#include <base/types.h>
#include <Poco/String.h>

namespace Search
{
enum class DataType;
}

namespace DB
{

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

Search::DataType getSearchIndexDataType(DataTypePtr &data_type);

void checkVectorDimension(const Search::DataType & search_type, const uint64_t & dim);

}
