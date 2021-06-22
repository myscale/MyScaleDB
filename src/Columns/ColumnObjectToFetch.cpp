#include <Columns/ColumnObjectToFetch.h>

#include <base/sort.h>
#include <base/range.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnCompressed.h>
#include <Core/Field.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/Serializations/SerializationInfoObjectToFetch.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
    extern const int LOGICAL_ERROR;
}


std::string ColumnObjectToFetch::getName() const
{
    WriteBufferFromOwnString res;
    res << "ObjectToFetch(";
    bool is_first = true;
    for (const auto & column : columns)
    {
        if (!is_first)
            res << ", ";
        is_first = false;
        res << column->getName();
    }
    res << ")";
    return res.str();
}

ColumnObjectToFetch::ColumnObjectToFetch(MutableColumns && mutable_columns)
{
    columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
    {
        if (isColumnConst(*column))
            throw Exception{ErrorCodes::ILLEGAL_COLUMN, "ColumnObjectToFetch cannot have ColumnConst as its element"};

        columns.push_back(std::move(column));
    }
}

ColumnObjectToFetch::Ptr ColumnObjectToFetch::create(const Columns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{ErrorCodes::ILLEGAL_COLUMN, "ColumnObjectToFetch cannot have ColumnConst as its element"};

    auto column_objecttofetch = ColumnObjectToFetch::create(MutableColumns());
    column_objecttofetch->columns.assign(columns.begin(), columns.end());

    return column_objecttofetch;
}

ColumnObjectToFetch::Ptr ColumnObjectToFetch::create(const ObjectToFetchColumns & columns)
{
    for (const auto & column : columns)
        if (isColumnConst(*column))
            throw Exception{ErrorCodes::ILLEGAL_COLUMN, "ColumnObjectToFetch cannot have ColumnConst as its element"};

    auto column_objecttofetch = ColumnObjectToFetch::create(MutableColumns());
    column_objecttofetch->columns = columns;

    return column_objecttofetch;
}

MutableColumnPtr ColumnObjectToFetch::cloneEmpty() const
{
    const size_t objecttofetch_size = columns.size();
    MutableColumns new_columns(objecttofetch_size);
    for (size_t i = 0; i < objecttofetch_size; ++i)
        new_columns[i] = columns[i]->cloneEmpty();

    return ColumnObjectToFetch::create(std::move(new_columns));
}

MutableColumnPtr ColumnObjectToFetch::cloneResized(size_t new_size) const
{
    const size_t objecttofetch_size = columns.size();
    MutableColumns new_columns(objecttofetch_size);
    for (size_t i = 0; i < objecttofetch_size; ++i)
        new_columns[i] = columns[i]->cloneResized(new_size);

    return ColumnObjectToFetch::create(std::move(new_columns));
}

Field ColumnObjectToFetch::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnObjectToFetch::get(size_t n, Field & res) const
{
    const size_t tuple_size = columns.size();

    res = ObjectToFetch();
    ObjectToFetch & res_tuple = res.get<ObjectToFetch &>();
    res_tuple.reserve(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        res_tuple.push_back((*columns[i])[n]);
}

bool ColumnObjectToFetch::isDefaultAt(size_t n) const
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        if (!columns[i]->isDefaultAt(n))
            return false;
    return true;
}

StringRef ColumnObjectToFetch::getDataAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {}", getName());
}

void ColumnObjectToFetch::insertData(const char *, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertData is not supported for {}", getName());
}

void ColumnObjectToFetch::insert(const Field & x)
{
    const auto & objecttofetch = x.get<const ObjectToFetch &>();

    const size_t objecttofetch_size = columns.size();
    if (objecttofetch.size() != objecttofetch_size)
        throw Exception(ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE, "Cannot insert value of different size into objecttofetch");

    for (size_t i = 0; i < objecttofetch_size; ++i)
        columns[i]->insert(objecttofetch[i]);
}

void ColumnObjectToFetch::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnObjectToFetch & src = assert_cast<const ColumnObjectToFetch &>(src_);

    const size_t objecttofetch_size = columns.size();
    if (src.columns.size() != objecttofetch_size)
        throw Exception(ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE, "Cannot insert value of different size into objecttofetch");

    for (size_t i = 0; i < objecttofetch_size; ++i)
        columns[i]->insertFrom(*src.columns[i], n);
}

void ColumnObjectToFetch::insertDefault()
{
    for (auto & column : columns)
        column->insertDefault();
}

void ColumnObjectToFetch::popBack(size_t n)
{
    for (auto & column : columns)
        column->popBack(n);
}

StringRef ColumnObjectToFetch::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    StringRef res(begin, 0);
    for (const auto & column : columns)
    {
        auto value_ref = column->serializeValueIntoArena(n, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}

const char * ColumnObjectToFetch::deserializeAndInsertFromArena(const char * pos)
{
    for (auto & column : columns)
        pos = column->deserializeAndInsertFromArena(pos);

    return pos;
}

const char * ColumnObjectToFetch::skipSerializedInArena(const char * pos) const
{
    for (const auto & column : columns)
        pos = column->skipSerializedInArena(pos);

    return pos;
}

void ColumnObjectToFetch::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashWithValue(n, hash);
}

void ColumnObjectToFetch::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = size();

    if (hash.getData().size() != s)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Size of WeakHash32 does not match size of column: column size is {}, hash size is {}",
            std::to_string(s),
            std::to_string(hash.getData().size()));

    for (const auto & column : columns)
        column->updateWeakHash32(hash);
}

void ColumnObjectToFetch::updateHashFast(SipHash & hash) const
{
    for (const auto & column : columns)
        column->updateHashFast(hash);
}

void ColumnObjectToFetch::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const size_t objecttofetch_size = columns.size();
    for (size_t i = 0; i < objecttofetch_size; ++i)
        columns[i]->insertRangeFrom(
            *assert_cast<const ColumnObjectToFetch &>(src).columns[i],
            start, length);
}

ColumnPtr ColumnObjectToFetch::filter(const Filter & filt, ssize_t result_size_hint) const
{
    const size_t objecttofetch_size = columns.size();
    Columns new_columns(objecttofetch_size);

    for (size_t i = 0; i < objecttofetch_size; ++i)
        new_columns[i] = columns[i]->filter(filt, result_size_hint);

    return ColumnObjectToFetch::create(new_columns);
}

void ColumnObjectToFetch::expand(const Filter & mask, bool inverted)
{
    for (auto & column : columns)
        column->expand(mask, inverted);
}

ColumnPtr ColumnObjectToFetch::permute(const Permutation & perm, size_t limit) const
{
    const size_t objecttofetch_size = columns.size();
    Columns new_columns(objecttofetch_size);

    for (size_t i = 0; i < objecttofetch_size; ++i)
        new_columns[i] = columns[i]->permute(perm, limit);

    return ColumnObjectToFetch::create(new_columns);
}

ColumnPtr ColumnObjectToFetch::index(const IColumn & indexes, size_t limit) const
{
    const size_t objecttofetch_size = columns.size();
    Columns new_columns(objecttofetch_size);

    for (size_t i = 0; i < objecttofetch_size; ++i)
        new_columns[i] = columns[i]->index(indexes, limit);

    return ColumnObjectToFetch::create(new_columns);
}

ColumnPtr ColumnObjectToFetch::replicate(const Offsets & offsets) const
{
    const size_t objecttofetch_size = columns.size();
    Columns new_columns(objecttofetch_size);

    for (size_t i = 0; i < objecttofetch_size; ++i)
        new_columns[i] = columns[i]->replicate(offsets);

    return ColumnObjectToFetch::create(new_columns);
}

MutableColumns ColumnObjectToFetch::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    const size_t objecttofetch_size = columns.size();
    std::vector<MutableColumns> scattered_objecttofetch_elements(objecttofetch_size);

    for (size_t objecttofetch_element_idx = 0; objecttofetch_element_idx < objecttofetch_size; ++objecttofetch_element_idx)
        scattered_objecttofetch_elements[objecttofetch_element_idx] = columns[objecttofetch_element_idx]->scatter(num_columns, selector);

    MutableColumns res(num_columns);

    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
    {
        MutableColumns new_columns(objecttofetch_size);
        for (size_t objecttofetch_element_idx = 0; objecttofetch_element_idx < objecttofetch_size; ++objecttofetch_element_idx)
            new_columns[objecttofetch_element_idx] = std::move(scattered_objecttofetch_elements[objecttofetch_element_idx][scattered_idx]);
        res[scattered_idx] = ColumnObjectToFetch::create(std::move(new_columns));
    }

    return res;
}

int ColumnObjectToFetch::compareAtImpl(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator * collator) const
{
    const size_t objecttofetch_size = columns.size();
    for (size_t i = 0; i < objecttofetch_size; ++i)
    {
        int res;
        if (collator && columns[i]->isCollationSupported())
            res = columns[i]->compareAtWithCollation(n, m, *assert_cast<const ColumnObjectToFetch &>(rhs).columns[i], nan_direction_hint, *collator);
        else
            res = columns[i]->compareAt(n, m, *assert_cast<const ColumnObjectToFetch &>(rhs).columns[i], nan_direction_hint);
        if (res)
            return res;
    }
    return 0;
}

int ColumnObjectToFetch::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint);
}

void ColumnObjectToFetch::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnObjectToFetch>(assert_cast<const ColumnObjectToFetch &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

int ColumnObjectToFetch::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const
{
    return compareAtImpl(n, m, rhs, nan_direction_hint, &collator);
}

bool ColumnObjectToFetch::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnObjectToFetch>();
}

template <bool positive>
struct ColumnObjectToFetch::Less
{
    ObjectToFetchColumns columns;
    int nan_direction_hint;
    const Collator * collator;

    Less(const ObjectToFetchColumns & columns_, int nan_direction_hint_, const Collator * collator_=nullptr)
        : columns(columns_), nan_direction_hint(nan_direction_hint_), collator(collator_)
    {
    }

    bool operator() (size_t a, size_t b) const
    {
        for (const auto & column : columns)
        {
            int res;
            if (collator && column->isCollationSupported())
                res = column->compareAtWithCollation(a, b, *column, nan_direction_hint, *collator);
            else
                res = column->compareAt(a, b, *column, nan_direction_hint);
            if (res < 0)
                return positive;
            else if (res > 0)
                return !positive;
        }
        return false;
    }
};

void ColumnObjectToFetch::getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator) const
{
    size_t rows = size();
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;

    if (limit >= rows)
        limit = 0;

    EqualRanges ranges;
    ranges.emplace_back(0, rows);
    updatePermutationImpl(direction, stability, limit, nan_direction_hint, res, ranges, collator);
}

void ColumnObjectToFetch::updatePermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges, const Collator * collator) const
{
    if (equal_ranges.empty())
        return;

    for (const auto & column : columns)
    {
        while (!equal_ranges.empty() && limit && limit <= equal_ranges.back().first)
            equal_ranges.pop_back();

        if (collator && column->isCollationSupported())
            column->updatePermutationWithCollation(*collator, direction, stability, limit, nan_direction_hint, res, equal_ranges);
        else
            column->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);

        if (equal_ranges.empty())
            break;
    }
}

void ColumnObjectToFetch::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, nan_direction_hint, res, nullptr);
}

void ColumnObjectToFetch::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    updatePermutationImpl(direction, stability, limit, nan_direction_hint, res, equal_ranges);
}

void ColumnObjectToFetch::getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, nan_direction_hint, res, &collator);
}

void ColumnObjectToFetch::updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    updatePermutationImpl(direction, stability, limit, nan_direction_hint, res, equal_ranges, &collator);
}

void ColumnObjectToFetch::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnObjectToFetch::reserve(size_t n)
{
    const size_t objecttofetch_size = columns.size();
    for (size_t i = 0; i < objecttofetch_size; ++i)
        getColumn(i).reserve(n);
}

void ColumnObjectToFetch::ensureOwnership()
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        getColumn(i).ensureOwnership();
}

size_t ColumnObjectToFetch::byteSize() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSize();
    return res;
}

size_t ColumnObjectToFetch::byteSizeAt(size_t n) const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSizeAt(n);
    return res;
}

size_t ColumnObjectToFetch::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();
    return res;
}

void ColumnObjectToFetch::protect()
{
    for (auto & column : columns)
        column->protect();
}

void ColumnObjectToFetch::getExtremes(Field & min, Field & max) const
{
    const size_t objecttofetch_size = columns.size();

    ObjectToFetch min_objecttofetch(objecttofetch_size);
    ObjectToFetch max_objecttofetch(objecttofetch_size);

    for (const auto i : collections::range(0, objecttofetch_size))
        columns[i]->getExtremes(min_objecttofetch[i], max_objecttofetch[i]);

    min = min_objecttofetch;
    max = max_objecttofetch;
}

void ColumnObjectToFetch::forEachSubcolumn(ColumnCallback callback) const
{
    for (auto & column : columns)
        callback(column);
}

bool ColumnObjectToFetch::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_objecttofetch = typeid_cast<const ColumnObjectToFetch *>(&rhs))
    {
        const size_t objecttofetch_size = columns.size();
        if (objecttofetch_size != rhs_objecttofetch->columns.size())
            return false;

        for (const auto i : collections::range(0, objecttofetch_size))
            if (!columns[i]->structureEquals(*rhs_objecttofetch->columns[i]))
                return false;

        return true;
    }
    else
        return false;
}

bool ColumnObjectToFetch::isCollationSupported() const
{
    for (const auto & column : columns)
    {
        if (column->isCollationSupported())
            return true;
    }
    return false;
}


ColumnPtr ColumnObjectToFetch::compress() const
{
    size_t byte_size = 0;
    Columns compressed;
    compressed.reserve(columns.size());
    for (const auto & column : columns)
    {
        auto compressed_column = column->compress();
        byte_size += compressed_column->byteSize();
        compressed.emplace_back(std::move(compressed_column));
    }

    return ColumnCompressed::create(size(), byte_size,
        [compressed = std::move(compressed)]() mutable
        {
            for (auto & column : compressed)
                column = column->decompress();
            return ColumnObjectToFetch::create(compressed);
        });
}

double ColumnObjectToFetch::getRatioOfDefaultRows(double sample_ratio) const
{
    return getRatioOfDefaultRowsImpl<ColumnObjectToFetch>(sample_ratio);
}

void ColumnObjectToFetch::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    return getIndicesOfNonDefaultRowsImpl<ColumnObjectToFetch>(indices, from, limit);
}

}
