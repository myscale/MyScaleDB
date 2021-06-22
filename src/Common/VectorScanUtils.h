#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <base/types.h>
#include <Poco/String.h>

namespace DB
{

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

}

