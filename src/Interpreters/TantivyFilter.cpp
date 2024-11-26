#include <algorithm>
#include <string>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/TantivyFilter.h>
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <Storages/MergeTree/TantivyIndexStore.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

TantivyFilter::TantivyFilter(const TantivyFilterParameters & params_) : params(params_), query_terms()
{
}

void TantivyFilter::addRowRangeToTantivyFilter(UInt64 rowIDStart, UInt64 rowIDEnd)
{
    if (!rowid_ranges.empty())
    {
        TantivyRowIdRange & last_rowid_range = rowid_ranges.back();
        if (last_rowid_range.range_end + 1 == rowIDStart)
        {
            last_rowid_range.range_end = rowIDEnd;
            return;
        }
    }
    rowid_ranges.push_back({rowIDStart, rowIDEnd});
}

void TantivyFilter::addRowRangeToTantivyFilter(UInt32 rowIDStart, UInt32 rowIDEnd)
{
    addRowRangeToTantivyFilter(static_cast<UInt64>(rowIDStart), static_cast<UInt64>(rowIDEnd));
}

void TantivyFilter::clear()
{
    query_term.clear();
    query_terms.clear();
    rowid_ranges.clear();
    this->setQueryType(QueryType::UNSPECIFIC_QUERY);
}
}
