#pragma once

#include <atomic>
#include <memory>
#include <tantivy_search.h>
#include <Interpreters/ITokenExtractor.h>
#include <Interpreters/TantivyFilter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <base/types.h>
#include <roaring.hh>
#include <roaring64map.hh>

namespace DB
{

template <typename RoaringType>
struct TemplateAddRangeClosed
{
    // for roaring::Roaring64Map
    template <typename T = RoaringType>
    static typename std::enable_if<!std::is_same<T, roaring::Roaring>::value, void>::type
    addRangeClosed(RoaringType & target_bitmap, size_t left, size_t right)
    {
        target_bitmap.addRangeClosed(left, right);
    }

    // for roaring::Roaring
    template <typename T = RoaringType>
    static typename std::enable_if<std::is_same<T, roaring::Roaring>::value, void>::type
    addRangeClosed(roaring::Roaring & target_bitmap, size_t left, size_t right)
    {
        if (left > std::numeric_limits<uint32_t>::max() || right > std::numeric_limits<uint32_t>::max())
        {
            throw std::overflow_error("Overflow happened when adding numbers into roaring bitmap");
        }
        target_bitmap.addRangeClosed(static_cast<uint32_t>(left), static_cast<uint32_t>(right));
    }
};

struct MergeTreeIndexGranuleTantivy final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleTantivy(const String & index_name_, size_t columns_number, const TantivyFilterParameters & params_);

    ~MergeTreeIndexGranuleTantivy() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    template <typename RoaringType>
    RoaringType granuleRowIdRangesToRoaring() const
    {
        RoaringType ranges;
        if (this->tantivy_filters.empty())
        {
            return ranges;
        }
        else
        {
            auto tantivy_row_id_ranges = tantivy_filters[0].getFilter();
            for (const auto & tantivy_row_id_range : tantivy_row_id_ranges)
            {
                TemplateAddRangeClosed<RoaringType>::addRangeClosed(
                    ranges, tantivy_row_id_range.range_start, tantivy_row_id_range.range_end);
            }
        }
        return ranges;
    }

    bool granuleRowIdRangesHitted(roaring::Roaring64Map & target_bitmap) const;

    bool empty() const override { return !has_elems; }

    String index_name;
    TantivyFilterParameters params;
    // A group of `TantivyFilter`, each `TantivyFilter` represent a Table column.
    TantivyFilters tantivy_filters;
    bool has_elems;
};

using MergeTreeIndexGranuleTantivyPtr = std::shared_ptr<MergeTreeIndexGranuleTantivy>;

struct MergeTreeIndexAggregatorTantivy final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorTantivy(
        TantivyIndexStorePtr store_, const Names & index_columns_, const String & index_name_, const TantivyFilterParameters & params_);
    ~MergeTreeIndexAggregatorTantivy() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    TantivyIndexStorePtr store;
    Names index_columns;
    const String index_name;
    const TantivyFilterParameters params;

    MergeTreeIndexGranuleTantivyPtr granule;
};


class MergeTreeConditionTantivy final : public IMergeTreeIndexCondition, WithContext
{
public:
    MergeTreeConditionTantivy(
        const ActionsDAG * filter_actions_dag,
        ContextPtr context,
        const Block & index_sample_block,
        const TantivyFilterParameters & params_);

    ~MergeTreeConditionTantivy() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule([[maybe_unused]] MergeTreeIndexGranulePtr idx_granule) const override
    {
        /// should call mayBeTrueOnGranuleInPart instead
        assert(false);
        return false;
    }

    template <typename RoaringType>
    RoaringType
    calculateRowIdsRoaringTemplate(MergeTreeIndexGranulePtr idx_granule, TantivyIndexStore & store, UInt64 indexed_doc_nums) const;

private:
    struct KeyTuplePositionMapping
    {
        KeyTuplePositionMapping(size_t tuple_index_, size_t key_index_) : tuple_index(tuple_index_), key_index(key_index_) { }

        size_t tuple_index;
        size_t key_index;
    };
    /// Uses RPN like KeyCondition

    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_LIKE, // 0
            FUNCTION_NOT_LIKE, // 1
            FUNCTION_EQUALS, // 2
            FUNCTION_NOT_EQUALS, // 3
            FUNCTION_HAS, // 4
            FUNCTION_IN, // 5
            FUNCTION_NOT_IN, // 6
            FUNCTION_MULTI_SEARCH, // 7
            FUNCTION_UNKNOWN, /// Can take any value.  // 8
            /// Operators of the logical expression.
            FUNCTION_NOT, // 9
            FUNCTION_AND, // 10
            FUNCTION_OR, // 11
            /// Constants
            ALWAYS_FALSE, // 12
            ALWAYS_TRUE, // 13
        };

        /// Every RPNElement has a TantivyFilter
        RPNElement( /// NOLINT
            Function function_ = FUNCTION_UNKNOWN, size_t key_column_ = 0, std::unique_ptr<TantivyFilter> && const_tantivy_filter_ = nullptr)
            : function(function_), key_column(key_column_), tantivy_filter(std::move(const_tantivy_filter_)) {}

        Function function = FUNCTION_UNKNOWN;
        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS and FUNCTION_MULTI_SEARCH
        size_t key_column;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<TantivyFilter> tantivy_filter;

        /// For FUNCTION_IN, FUNCTION_NOT_IN and FUNCTION_MULTI_SEARCH
        std::vector<TantivyFilters> set_tantivy_filters;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out);

    bool traverseASTEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_ast,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out);

    bool tryPrepareSetTantivyFilter(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, RPNElement & out);

    // static bool createFunctionEqualsCondition(
    //     RPNElement & out, const Field & value, const GinFilterParameters & params, TokenExtractorPtr token_extractor);

    const Block & header;
    TantivyFilterParameters params;
    RPN rpn;
    /// Sets from syntax analyzer.
    PreparedSetsPtr prepared_sets;
};


template <typename RoaringType>
RoaringType MergeTreeConditionTantivy::calculateRowIdsRoaringTemplate(
    MergeTreeIndexGranulePtr idx_granule, TantivyIndexStore & store, UInt64 indexed_doc_nums) const
{
    std::shared_ptr<MergeTreeIndexGranuleTantivy> granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleTantivy>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TantivyFilter index condition got a granule with the wrong type.");

    try
    {
        std::vector<RoaringType> rpn_stack;
        RoaringType full;
        TemplateAddRangeClosed<RoaringType>::addRangeClosed(full, 0, indexed_doc_nums - 1);
        for (const auto & element : rpn)
        {
            if (element.function == RPNElement::FUNCTION_UNKNOWN)
            {
                // For unknown element, assume it may hit all row ids.
                rpn_stack.emplace_back(full);
            }
            else if (
                element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS
                || element.function == RPNElement::FUNCTION_HAS)
            {
                LOG_DEBUG(
                    getLogger("MergeTreeConditionTantivy"),
                    "[calculateRowIdsRoaringTemplate] element.key_column:{}, ",
                    element.key_column);
                rpn_stack.emplace_back(
                    granule->tantivy_filters[element.key_column].searchedRoaringTemplate<RoaringType>(*element.tantivy_filter, store));

                if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                {
                    RoaringType rpn_back = rpn_stack.back();
                    rpn_stack.pop_back();
                    rpn_stack.emplace_back(full - rpn_back);
                }
            }
            else if (element.function == RPNElement::FUNCTION_LIKE || element.function == RPNElement::FUNCTION_NOT_LIKE)
            {
                rpn_stack.emplace_back(
                    granule->tantivy_filters[element.key_column].searchedRoaringTemplate<RoaringType>(*element.tantivy_filter, store));

                if (element.function == RPNElement::FUNCTION_NOT_LIKE)
                {
                    RoaringType rpn_back = rpn_stack.back();
                    rpn_stack.pop_back();
                    rpn_stack.emplace_back(full - rpn_back);
                }
            }
            else if (element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN)
            {
                RoaringType result;

                for (size_t column = 0; column < element.set_key_position.size(); ++column)
                {
                    const size_t key_idx = element.set_key_position[column];
                    const auto & tantivy_filters = element.set_tantivy_filters[column];

                    for (size_t row = 0; row < tantivy_filters.size(); ++row)
                        result |= granule->tantivy_filters[key_idx].searchedRoaringTemplate<RoaringType>(tantivy_filters[row], store);
                }

                rpn_stack.emplace_back(std::move(result));
                if (element.function == RPNElement::FUNCTION_NOT_IN)
                {
                    RoaringType rpn_back = rpn_stack.back();
                    rpn_stack.pop_back();
                    rpn_stack.emplace_back(full - rpn_back);
                }
            }
            else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
            {
                rpn_stack.emplace_back(
                    granule->tantivy_filters[element.key_column].searchedRoaringTemplate<RoaringType>(*element.tantivy_filter, store));
            }
            else if (element.function == RPNElement::FUNCTION_NOT)
            {
                RoaringType rpn_back = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.emplace_back(full - rpn_back);
            }
            else if (element.function == RPNElement::FUNCTION_AND)
            {
                auto arg1 = rpn_stack.back();
                rpn_stack.pop_back();
                auto arg2 = rpn_stack.back();
                rpn_stack.back() = arg1 & arg2;
            }
            else if (element.function == RPNElement::FUNCTION_OR)
            {
                auto arg1 = rpn_stack.back();
                rpn_stack.pop_back();
                auto arg2 = rpn_stack.back();
                rpn_stack.back() = arg1 | arg2;
            }
            else if (element.function == RPNElement::ALWAYS_FALSE)
            {
                RoaringType emptyRoaring;
                rpn_stack.emplace_back(std::move(emptyRoaring));
            }
            else if (element.function == RPNElement::ALWAYS_TRUE)
            {
                rpn_stack.emplace_back(full);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in TantivyFilterCondition::RPNElement");
        }

        if (rpn_stack.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in TantivyFilterCondition::calculateRowIdsRoaringTemplate");

        return rpn_stack[0];
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RPN Stack Error happend. {}", e.what());
    }
}

class MergeTreeIndexTantivy final : public IMergeTreeIndex
{
public:
    MergeTreeIndexTantivy(const IndexDescription & index_, const TantivyFilterParameters & params_)
        : IMergeTreeIndex(index_), params(params_)
    {
    }

    ~MergeTreeIndexTantivy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(TantivyIndexStorePtr & store, const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG * filter_actions_dag, ContextPtr context) const override;

    TantivyFilterParameters params;
    /// Function for selecting next token.
};

}
