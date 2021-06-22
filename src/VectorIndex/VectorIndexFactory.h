#pragma once

#include "VectorIndex.h"

namespace VectorIndex
{
class VectorIndexFactory
{
public:
    static bool typeExist(std::string index_type);
    static IndexType createIndexType(std::string index_type);
    static Metrics createIndexMetrics(std::string index_metric);
    static IndexMode createIndexMode(std::string index_mode);
    static VectorIndexPtr createIndex(IndexType it, IndexMode im, Metrics me, int dimension, Parameters parameters);
    static std::string typeToString(IndexType it);
    static std::string MetricToString(Metrics me);
    static std::string modeToString(IndexMode mode);
};
}
