#include <Storages/MergeTree/SkipIndex/Common/IndexSettings.h>

namespace DB
{

IndexSettings & IndexSettings::operator=(const IndexSettings & other)
{
    if (this != &other)
    {
        this->json_parameter = other.json_parameter;
    }
    return *this;
}

SparseIndexSettings & SparseIndexSettings::operator=(const SparseIndexSettings & other)
{
    if (this != &other)
    {
        IndexSettings::operator=(other);
        this->column = other.column;
    }
    return *this;
}

TantivyIndexSettings & TantivyIndexSettings::operator=(const TantivyIndexSettings & other)
{
    if (this != &other)
    {
        IndexSettings::operator=(other);
        this->indexed_columns = other.indexed_columns;
    }
    return *this;
}

}
