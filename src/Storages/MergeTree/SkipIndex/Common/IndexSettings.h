#pragma once

#include <string>
#include <vector>

namespace DB
{

struct IndexSettings
{
    std::string json_parameter = "{}";

    IndexSettings & operator=(const IndexSettings & other);

    virtual ~IndexSettings() = default;
};

struct SparseIndexSettings : public IndexSettings
{
    std::string column = "";

    SparseIndexSettings & operator=(const SparseIndexSettings & other);
};

struct TantivyIndexSettings : public IndexSettings
{
    std::vector<std::string> indexed_columns = {};

    TantivyIndexSettings & operator=(const TantivyIndexSettings & other);
};

using IndexSettingsPtr = std::shared_ptr<IndexSettings>;
using SparseIndexSettingsPtr = std::shared_ptr<SparseIndexSettings>;
using TantivyIndexSettingsPtr = std::shared_ptr<TantivyIndexSettings>;

}
