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
#include <Storages/MergeTree/MergeTreeIndexInverted.h>
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

/// Determine if the searched string can hit the `row_id_ranges` within the TantivyFilter.
bool TantivyFilter::contains(const TantivyFilter & filter, TantivyIndexStore & store) const
{
    bool status = false;

    for (const auto & rowid_range : this->getFilter())
    {
        switch (filter.getQueryType())
        {
            case QueryType::REGEX_QUERY:
                if (filter.getForbidRegexSearch())
                {
                    status = store.singleTermQueryWithRowIdRange(
                        filter.getQueryColumnName(), filter.getQueryString(), rowid_range.range_start, rowid_range.range_end);
                }
                else
                {
                    status = store.regexTermQueryWithRowIdRange(
                        filter.getQueryColumnName(), filter.getQueryString(), rowid_range.range_start, rowid_range.range_end);
                }
                break;
            case QueryType::SINGLE_TERM_QUERY:
                status = store.singleTermQueryWithRowIdRange(
                    filter.getQueryColumnName(), filter.getQueryString(), rowid_range.range_start, rowid_range.range_end);
                break;
            case QueryType::MULTI_TERM_QUERY:
                status = store.termsQueryWithRowIdRange(
                    filter.getQueryColumnName(), filter.getQueryTerms(), rowid_range.range_start, rowid_range.range_end);
                break;
            case QueryType::SENTENCE_QUERY:
                status = store.sentenceQueryWithRowIdRange(
                    filter.getQueryColumnName(), filter.getQueryString(), rowid_range.range_start, rowid_range.range_end);
                break;
            default:
                status = store.sentenceQueryWithRowIdRange(
                    filter.getQueryColumnName(), filter.getQueryString(), rowid_range.range_start, rowid_range.range_end);
                break;
        }
        if (status)
        {
            return true;
        }
    }

    return status;
}


}
