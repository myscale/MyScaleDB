#pragma once

#include <memory>
#include <vector>
#include <Storages/MergeTree/TantivyIndexStore.h>
#include <roaring.hh>
#include <roaring64map.hh>
namespace DB
{


static inline constexpr auto TANTIVY_INDEX_NAME = "fts";

struct TantivyFilterParameters
{
    TantivyFilterParameters(const String & index_json_parameter_) : index_json_parameter(index_json_parameter_) { }

    const String index_json_parameter;
};

struct TantivyRowIdRange
{
    /// First row ID in the range [
    UInt64 range_start;

    /// Last row ID in the range (inclusive) ]
    UInt64 range_end;
};

using TantivyRowIdRanges = std::vector<TantivyRowIdRange>;

class TantivyFilter
{
public:
    enum QueryType
    {
        REGEX_QUERY,
        SINGLE_TERM_QUERY,
        MULTI_TERM_QUERY,
        SENTENCE_QUERY,
        UNSPECIFIC_QUERY,
    };

    explicit TantivyFilter(const TantivyFilterParameters & params_);

    /// Accumulate row_ranges, then generate granule idx file.
    void addRowRangeToTantivyFilter(UInt64 rowIDStart, UInt64 rowIDEnd);

    /// Accumulate row_ranges, then generate granule idx file.
    void addRowRangeToTantivyFilter(UInt32 rowIDStart, UInt32 rowIDEnd);

    /// Clear the content
    void clear();

    template <typename RoaringType>
    RoaringType searchedRoaringTemplate(const TantivyFilter & filter, TantivyIndexStore & store) const;

    size_t getRowIdRangesSize() { return rowid_ranges.size(); }

    /// Getter
    const TantivyRowIdRanges & getFilter() const { return rowid_ranges; }
    TantivyRowIdRanges & getFilter() { return rowid_ranges; }
    const String & getQueryString() const { return query_term; }
    const String & getQueryColumnName() const { return this->column_name; }
    const std::vector<String> & getQueryTerms() const { return this->query_terms; }
    const QueryType & getQueryType() const { return this->query_type; }
    bool getForbidRegexSearch() const { return this->forbidden_regex_search; }

    /// Setter
    void setQueryString(const char * data, size_t len) { query_term = String(data, len); }
    void setQueryColumnName(const String & column_name_) { this->column_name = column_name_; }
    void addQueryTerm(const String & term) { this->query_terms.push_back(term); }
    void setQueryType(QueryType type) { this->query_type = type; }
    void forbidRegexSearch() { this->forbidden_regex_search = true; }


private:
    /// Append rowids from u8bitmap to target_bitmap
    template <typename RoaringType>
    void appendU8BitmapToRoaringTemplate(const rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap, RoaringType & target_bitmap) const;

    /// Convert u8Bitmap to Roaring type.
    template <typename RoaringType>
    RoaringType convertU8BitampToRoaringTemplate(rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap) const;

    /// Filter parameters
    const TantivyFilterParameters & params;

    /// Query string of the filter
    String query_term = "";
    std::vector<String> query_terms = {};

    /// Query column name of the filter
    String column_name = "";

    /// Filter type, each type will trigger different query strategy in tantivy_search.
    QueryType query_type = QueryType::UNSPECIFIC_QUERY;

    /// avoid regex search in map keys.
    bool forbidden_regex_search = false;

    TantivyRowIdRanges rowid_ranges;
};

using TantivyFilters = std::vector<TantivyFilter>;

template <typename RoaringType>
struct TantivyRoaringBitmapAdder
{
    // for roaring::Roaring64Map
    template <typename T = RoaringType>
    static typename std::enable_if<!std::is_same<T, roaring::Roaring>::value, void>::type add(RoaringType & target_bitmap, size_t index)
    {
        target_bitmap.add(index);
    }

    // for roaring::Roaring
    template <typename T = RoaringType>
    static typename std::enable_if<std::is_same<T, roaring::Roaring>::value, void>::type add(roaring::Roaring & target_bitmap, size_t index)
    {
        if (index > std::numeric_limits<uint32_t>::max())
        {
            throw std::overflow_error("Overflow happened when adding numbers into roaring bitmap");
        }
        target_bitmap.add(static_cast<uint32_t>(index));
    }
};

template <typename RoaringType>
void TantivyFilter::appendU8BitmapToRoaringTemplate(const rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap, RoaringType & target_bitmap) const
{
    for (size_t i = 0; i < u8bitmap.size(); i++)
    {
        if (u8bitmap[i] == 0)
        {
            continue;
        }
        std::bitset<8> temp(u8bitmap[i]);
        size_t bit = i * 8;
        for (size_t k = 0; k < temp.size(); k++)
        {
            if (temp[k])
            {
                TantivyRoaringBitmapAdder<RoaringType>::add(target_bitmap, bit + k);
            }
        }
    }
}

template <typename RoaringType>
RoaringType TantivyFilter::convertU8BitampToRoaringTemplate(rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap) const
{
    RoaringType target_bitmap;
    if (u8bitmap.empty())
    {
        return target_bitmap;
    }
    this->appendU8BitmapToRoaringTemplate<RoaringType>(u8bitmap, target_bitmap);
    return target_bitmap;
}


template <typename RoaringType>
RoaringType TantivyFilter::searchedRoaringTemplate(const TantivyFilter & filter, TantivyIndexStore & store) const
{
    rust::cxxbridge1::Vec<uint8_t> res;

    switch (filter.getQueryType())
    {
        case QueryType::REGEX_QUERY:
            if (filter.getForbidRegexSearch())
            {
                res = store.singleTermQueryBitmap(filter.getQueryColumnName(), filter.getQueryString());
            }
            else
            {
                res = store.regexTermQueryBitmap(filter.getQueryColumnName(), filter.getQueryString());
            }
            break;
        case QueryType::SINGLE_TERM_QUERY:
            res = store.singleTermQueryBitmap(filter.getQueryColumnName(), filter.getQueryString());
            break;
        case QueryType::MULTI_TERM_QUERY:
            res = store.termsQueryBitmap(filter.getQueryColumnName(), filter.getQueryTerms());
            break;
        case QueryType::SENTENCE_QUERY:
            res = store.sentenceQueryBitmap(filter.getQueryColumnName(), filter.getQueryString());
            break;
        default:
            res = store.sentenceQueryBitmap(filter.getQueryColumnName(), filter.getQueryString());
            break;
    }

    return this->convertU8BitampToRoaringTemplate<RoaringType>(res);
}
}
