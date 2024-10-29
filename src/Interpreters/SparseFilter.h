#pragma once

#include <memory>
#include <vector>
#include <Storages/MergeTree/SkipIndex/Store/SparseIndexStore.h>
#include <roaring.hh>
#include <roaring64map.hh>
namespace DB
{

// static inline constexpr auto SPARSE_INDEX_NAME = "sparse";

struct SparseFilterParameters
{
    SparseFilterParameters(const String & index_json_parameter_) : index_json_parameter(index_json_parameter_) {}

    const String index_json_parameter;
};

struct SparseRowIdRange
{
    /// First row ID in the range [
    UInt64 range_start;

    /// Last row ID in the range (inclusive) ]
    UInt64 range_end;
};

using SparseRowIdRanges = std::vector<SparseRowIdRange>;

class SparseFilter
{
public:
    explicit SparseFilter(const SparseFilterParameters & params_);

    /// Accumulate row_ranges, then generate granule idx file.
    void addRowRangeToSparseFilter(UInt64 rowIDStart, UInt64 rowIDEnd);

    /// Accumulate row_ranges, then generate granule idx file.
    void addRowRangeToSparseFilter(UInt32 rowIDStart, UInt32 rowIDEnd);

    /// Clear the content
    void clear();

    template <typename RoaringType>
    RoaringType searchedRoaringTemplate(const SparseFilter & filter, SparseIndexStore & store) const;

    size_t getRowIdRangesSize() { return rowid_ranges.size(); }

    /// Getter
    const SparseRowIdRanges & getFilter() const { return rowid_ranges; }
    SparseRowIdRanges & getFilter() { return rowid_ranges; }
    const std::unordered_map<UInt32, Float32> & getQuerySparseVector() const { return query_sparse_vector; }
    const String & getQueryColumnName() const { return this->column_name; }

    /// Setter
    // void setQueryString(const char * data, size_t len) { query_term = String(data, len); }
    void setQueryColumnName(const String & column_name_) { this->column_name = column_name_; }
    // void addQueryTerm(const String & term) { this->query_terms.push_back(term); }
    // void setQueryType(QueryType type) { this->query_type = type; }
    // void forbidRegexSearch() { this->forbidden_regex_search = true; }


private:
    /// Append rowids from u8bitmap to target_bitmap
    template <typename RoaringType>
    void appendU8BitmapToRoaringTemplate(const rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap, RoaringType & target_bitmap) const;

    /// Convert u8Bitmap to Roaring type.
    template <typename RoaringType>
    RoaringType convertU8BitampToRoaringTemplate(rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap) const;

    /// Filter parameters
    const SparseFilterParameters & params;

    /// Query sparse vector of the filter
    std::unordered_map<UInt32, Float32> query_sparse_vector;

    /// Query column name of the filter
    String column_name = "";

    SparseRowIdRanges rowid_ranges;
};

using SparseFilters = std::vector<SparseFilter>;

template <typename RoaringType>
struct SparseRoaringBitmapAdder
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
void SparseFilter::appendU8BitmapToRoaringTemplate(const rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap, RoaringType & target_bitmap) const
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
                SparseRoaringBitmapAdder<RoaringType>::add(target_bitmap, bit + k);
            }
        }
    }
}

template <typename RoaringType>
RoaringType SparseFilter::convertU8BitampToRoaringTemplate(rust::cxxbridge1::Vec<std::uint8_t> & u8bitmap) const
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
RoaringType SparseFilter::searchedRoaringTemplate(const SparseFilter & filter, SparseIndexStore & store) const
{
    rust::cxxbridge1::Vec<uint8_t> res;

    /// TODO by libaoy
    // res = store.termsQueryBitmap(filter.getQueryColumnName(), filter.getQuerySparseVector());

    (void)filter;
    (void)store;

    return this->convertU8BitampToRoaringTemplate<RoaringType>(res);
}
}
