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
#include <Interpreters/SparseFilter.h>
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <Storages/MergeTree/MergeTreeIndexInverted.h>
#include <Storages/MergeTree/SkipIndex/Store/SparseIndexStore.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

SparseFilter::SparseFilter(const SparseFilterParameters & params_) : params(params_), query_sparse_vector()
{
}

void SparseFilter::addRowRangeToSparseFilter(UInt64 rowIDStart, UInt64 rowIDEnd)
{
    if (!rowid_ranges.empty())
    {
        SparseRowIdRange & last_rowid_range = rowid_ranges.back();
        if (last_rowid_range.range_end + 1 == rowIDStart)
        {
            last_rowid_range.range_end = rowIDEnd;
            return;
        }
    }
    rowid_ranges.push_back({rowIDStart, rowIDEnd});
}

void SparseFilter::addRowRangeToSparseFilter(UInt32 rowIDStart, UInt32 rowIDEnd)
{
    addRowRangeToSparseFilter(static_cast<UInt64>(rowIDStart), static_cast<UInt64>(rowIDEnd));
}

void SparseFilter::clear()
{
    query_sparse_vector.clear();
    rowid_ranges.clear();
}
}
