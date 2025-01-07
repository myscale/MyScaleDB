#include <Storages/MergeTree/MergeTreeIndexTantivy.h>

#include <algorithm>
#include <regex>
#include <tantivy_search.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexUtils.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Poco/Logger.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int TANTIVY_BUILD_INDEX_INTERNAL_ERROR;
}

MergeTreeIndexGranuleTantivy::MergeTreeIndexGranuleTantivy(
    const String & index_name_, size_t columns_number, const TantivyFilterParameters & params_)
    : index_name(index_name_), params(params_), tantivy_filters(columns_number, TantivyFilter(params)), has_elems(false)
{
}

[[deprecated("Performance is worse than using bitmap.")]] bool
MergeTreeIndexGranuleTantivy::granuleRowIdRangesHitted(roaring::Roaring64Map & target_bitmap) const
{
    bool hitted = false;
    if (this->tantivy_filters.empty())
    {
        return hitted;
    }
    else
    {
        auto tantivy_row_id_ranges = tantivy_filters[0].getFilter();
        for (const auto & tantivy_row_id_range : tantivy_row_id_ranges)
        {
            for (auto row_id = tantivy_row_id_range.range_start; row_id <= tantivy_row_id_range.range_end; row_id++)
            {
                if (target_bitmap.contains(row_id))
                {
                    hitted = true;
                    break;
                }
            }

            if (hitted == true)
            {
                break;
            }
        }
    }
    return hitted;
}

/// Serializing a set of tantivy_filters actually involves serializing their internal row_id_ranges.
void MergeTreeIndexGranuleTantivy::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty fts granule idx {}.", backQuote(index_name));

    const auto & size_type = std::make_shared<DataTypeUInt32>();
    auto size_serialization = size_type->getDefaultSerialization();

    for (const auto & tantivy_filter : tantivy_filters)
    {
        size_t filter_size = tantivy_filter.getFilter().size();
        size_serialization->serializeBinary(filter_size, ostr, {});
        ostr.write(reinterpret_cast<const char *>(tantivy_filter.getFilter().data()), filter_size * sizeof(TantivyRowIdRanges::value_type));
    }
}

/// Deserialize the internal row_id_ranges of a set of tantivy_filters.
void MergeTreeIndexGranuleTantivy::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_rows;
    const auto & size_type = std::make_shared<DataTypeUInt32>();

    auto size_serialization = size_type->getDefaultSerialization();
    for (auto & tantivy_filter : tantivy_filters)
    {
        size_serialization->deserializeBinary(field_rows, istr, {});
        size_t filter_size = field_rows.safeGet<size_t>();

        if (filter_size == 0)
            continue;

        tantivy_filter.getFilter().assign(filter_size, {});
        istr.readStrict(reinterpret_cast<char *>(tantivy_filter.getFilter().data()), filter_size * sizeof(TantivyRowIdRanges::value_type));
    }
    has_elems = true;
}


MergeTreeIndexAggregatorTantivy::MergeTreeIndexAggregatorTantivy(
    TantivyIndexStorePtr store_, const Names & index_columns_, const String & index_name_, const TantivyFilterParameters & params_)
    : store(store_)
    , index_columns(index_columns_)
    , index_name(index_name_)
    , params(params_)
    , granule(std::make_shared<MergeTreeIndexGranuleTantivy>(index_name, index_columns.size(), params))
{
    /// Process relevant parameters from the Tantivy Parameter to initialize the Store.
    TantivyIndexSettings index_settings;
    // TODO: 记录下来索引的 json 配置
    index_settings.index_json_parameter = params.index_json_parameter;
    index_settings.indexed_columns = index_columns_;
    // TODO: 将索引的列名传递给 tantivy_search
    store->setTantivyIndexSettings(index_settings);
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorTantivy::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleTantivy>(index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

/// Logic for building indices and accumulating row_id_ranges.
void MergeTreeIndexAggregatorTantivy::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. "
            "Position: {}, Block rows: {}.",
            toString(*pos),
            toString(block.rows()));

    // Each column can read rows limit.
    size_t rows_read = std::min(limit, block.rows() - *pos);
    // Each column start row_id, they are same in each columns.
    auto start_row_id = store->getNextRowId(rows_read);
    // Traverse all rows that need to be read vertically.
    // Horizontal processing for each row
    for (size_t row_idx = 0; row_idx < rows_read; row_idx++)
    {
        // Record each row column_names
        std::vector<std::string> column_names;
        // Record each row docs need indexed.
        std::vector<std::string> docs;
        for (size_t col = 0; col < index_columns.size(); ++col)
        {
            const auto & column_with_type = block.getByName(index_columns[col]);
            const auto & column = column_with_type.column;
            const auto & column_name = column_with_type.name;
            const auto & column_type = column_with_type.type;
            // record column name.
            column_names.emplace_back(column_name);

            // process current row docs.
            if (isArray(column_type))
            {
                const auto & column_array = assert_cast<const ColumnArray &>(*column);
                const auto & column_offsets = column_array.getOffsets();
                const auto & column_key = column_array.getData();

                size_t element_start_row = column_offsets[*pos + row_idx - 1];
                size_t elements_size = column_offsets[*pos + row_idx] - element_start_row;

                std::string combined;

                for (size_t row_num = 0; row_num < elements_size; ++row_num)
                {
                    auto ref = column_key.getDataAt(element_start_row + row_num);
                    combined.append(ref.data, ref.size);
                    combined.push_back(' ');
                }

                docs.emplace_back(combined);
            }
            else
            {
                auto ref = column->getDataAt(*pos + row_idx);
                std::string doc(ref.data, ref.size);
                docs.emplace_back(doc);
            }
            /// Accumulate row_id_ranges to construct the granule idx file.
            granule->tantivy_filters[col].addRowRangeToTantivyFilter(start_row_id + row_idx, static_cast<UInt64>(start_row_id + row_idx));
            // LOG_DEBUG(getLogger("MergeTreeIndexAggregatorTantivy"), "[update] addRowRangeToTantivyFilter {} -- {}", start_row_id + row_idx, static_cast<UInt64>(start_row_id + row_idx));
        }
        // Index docs to tantivy-search
        if (column_names.size() != docs.size())
        {
            throw Exception(
                ErrorCodes::TANTIVY_BUILD_INDEX_INTERNAL_ERROR,
                "Generating `column_names`(size:{}) and `docs`(size:{}) may be wrong when index fts docs.",
                column_names.size(),
                docs.size());
        }
        else if (column_names.size() != 0 && docs.size() != 0)
        {
            store->indexMultiColumnDoc(start_row_id + row_idx, column_names, docs);
            // for (size_t i = 0; i < column_names.size(); i++)
            // {
            //     LOG_DEBUG(getLogger("MergeTreeIndexAggregatorTantivy"), "[update] row_id:{}, col:{}, column_name:{}, doc:{}", start_row_id+row_idx, i, column_names[i], docs[i]);
            // }
        }
    }
    granule->has_elems = true;
    *pos += rows_read;
}


MergeTreeConditionTantivy::MergeTreeConditionTantivy(
    const ActionsDAG * filter_actions_dag,
    ContextPtr context_,
    const Block & index_sample_block,
    const TantivyFilterParameters & params_)
    : WithContext(context_), header(index_sample_block),
    params(params_)
{
    if (!filter_actions_dag)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    rpn = std::move(
            RPNBuilder<RPNElement>(
                    filter_actions_dag->getOutputs().at(0), context_,
                    [&](const RPNBuilderTreeNode & node, RPNElement & out)
                    {
                        return this->traverseAtomAST(node, out);
                    }).extractRPN());
}

/// Keep in-sync with MergeTreeConditionFullText::alwaysUnknownOrTrue
bool MergeTreeConditionTantivy::alwaysUnknownOrTrue() const
{
    /// Check like in KeyCondition.
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (
            element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_LIKE || element.function == RPNElement::FUNCTION_NOT_LIKE
            || element.function == RPNElement::FUNCTION_HAS || element.function == RPNElement::FUNCTION_IN
            || element.function == RPNElement::FUNCTION_NOT_IN || element.function == RPNElement::FUNCTION_MULTI_SEARCH
            || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            // do nothing
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 && arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 || arg2;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }
    return rpn_stack[0];
}

// convert RPNBuilderTreeNode to RPNElement
bool MergeTreeConditionTantivy::traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            /// Check constant like in KeyCondition
            if (const_value.getType() == Field::Types::UInt64 || const_value.getType() == Field::Types::Int64
                || const_value.getType() == Field::Types::Float64)
            {
                /// Zero in all types is represented in memory the same way as in UInt64.
                out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;

                return true;
            }
        }
    }

    if (node.isFunction())
    {
        const auto function = node.toFunctionNode();
        // auto arguments_size = function.getArgumentsSize();
        auto function_name = function.getFunctionName();

        size_t function_arguments_size = function.getArgumentsSize();
        if (function_arguments_size != 2)
            return false;
        auto lhs_argument = function.getArgumentAt(0);
        auto rhs_argument = function.getArgumentAt(1);

        if (functionIsInOrGlobalInOperator(function_name))
        {
            if (tryPrepareSetTantivyFilter(lhs_argument, rhs_argument, out))
            {
                if (function_name == "notIn")
                {
                    out.function = RPNElement::FUNCTION_NOT_IN;
                    return true;
                }
                else if (function_name == "in")
                {
                    out.function = RPNElement::FUNCTION_IN;
                    return true;
                }
            }
        }
        else if (
            function_name == "equals" || function_name == "notEquals" || function_name == "has" || function_name == "mapContains"
            || function_name == "like" || function_name == "notLike" || function_name == "hasToken" || function_name == "hasTokenOrNull"
            || function_name == "startsWith" || function_name == "endsWith" || function_name == "multiSearchAny")
        {
            Field const_value;
            DataTypePtr const_type;
            if (rhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseASTEquals(function_name, lhs_argument, const_type, const_value, out))
                    return true;
            }
            else if (lhs_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
            {
                if (traverseASTEquals(function_name, rhs_argument, const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeConditionTantivy::traverseASTEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_ast,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    Field const_value = value_field;
    size_t key_column_num = 0;
    String col_name = key_ast.getColumnName();
    bool key_exists = header.has(key_ast.getColumnName());
    // for function mapContains
    bool map_key_exists = header.has(fmt::format("mapKeys({})", key_ast.getColumnName()));
    // for arraryElement
    bool map_key_costant = false;

    if (key_ast.isFunction())
    {
        const auto function = key_ast.toFunctionNode();
        if (function.getFunctionName() == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exists in map
              * we return default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exists.
              */
            if (value_field == value_type->getDefault())
                return false;

            auto first_argument = function.getArgumentAt(0);
            const auto map_column_name = first_argument.getColumnName();
            auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
            col_name = map_keys_index_column_name;
            auto map_values_index_column_name = fmt::format("mapValues({})", map_column_name);

            if (header.has(map_keys_index_column_name))
            {
                auto argument = function.getArgumentAt(1);
                DataTypePtr const_type;
                if (argument.tryGetConstant(const_value, const_type))
                {
                    key_column_num = header.getPositionByName(map_keys_index_column_name);
                    key_exists = true;
                    map_key_costant = true;
                }
                else
                {
                    return false;
                }
            }
            else if (header.has(map_values_index_column_name))
            {
                key_column_num = header.getPositionByName(map_values_index_column_name);
                key_exists = true;
            }
            else
            {
                return false;
            }
        }
    }

    if (!key_exists && !map_key_exists)
        return false;

    if (map_key_exists && (function_name == "has" || function_name == "mapContains"))
    {
        if (function_name == "mapContains")
        {
            col_name = fmt::format("mapKeys({})", col_name);
        }

        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::SENTENCE_QUERY);
        auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        return true;
    }
    // The array of strings in `has(arr, elem)` will be combined into a single string.
    // Here we use the sentence query.
    else if (function_name == "has")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::SENTENCE_QUERY);
        auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        return true;
    }

    if (function_name == "notEquals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::SENTENCE_QUERY);
        const auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        return true;
    }
    else if (function_name == "equals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::SENTENCE_QUERY);
        const auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        return true;
    }
    else if (function_name == "like")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_LIKE;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::REGEX_QUERY);
        const auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        if (map_key_costant)
        {
            // When use LIKE in mapKeys(''), we can't use regex search.
            out.tantivy_filter->forbidRegexSearch();
        }
        return true;
    }
    else if (function_name == "notLike")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_LIKE;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::REGEX_QUERY);
        const auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        if (map_key_costant)
        {
            out.tantivy_filter->forbidRegexSearch();
        }
        return true;
    }
    else if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::SINGLE_TERM_QUERY);
        const auto & value = const_value.safeGet<String>();
        out.tantivy_filter->setQueryString(value.data(), value.size());
        return true;
    }
    else if (function_name == "startsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::REGEX_QUERY);
        const auto & value = const_value.safeGet<String>();
        std::string pattern_string(value.data(), value.size());
        pattern_string += "%";
        out.tantivy_filter->setQueryString(pattern_string.c_str(), pattern_string.size());
        return true;
    }
    else if (function_name == "endsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.tantivy_filter = std::make_unique<TantivyFilter>(params);
        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::REGEX_QUERY);
        const auto & value = const_value.safeGet<String>();
        std::string pattern_string(value.data(), value.size());
        pattern_string = "%" + pattern_string;
        out.tantivy_filter->setQueryString(pattern_string.c_str(), pattern_string.size());
        return true;
    }
    else if (function_name == "multiSearchAny")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_MULTI_SEARCH;

        out.tantivy_filter = std::make_unique<TantivyFilter>(params);

        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;
            const auto & value = element.safeGet<String>();
            std::string term = String(value.data(), value.size());
            out.tantivy_filter->addQueryTerm(term);
        }

        out.tantivy_filter->setQueryColumnName(col_name);
        out.tantivy_filter->setQueryType(TantivyFilter::MULTI_TERM_QUERY);

        return true;
    }

    return false;
}

bool MergeTreeConditionTantivy::tryPrepareSetTantivyFilter(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, RPNElement & out)
{
    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    std::vector<String> column_names;
    DataTypes data_types;

    if (lhs.isFunction() && lhs.toFunctionNode().getFunctionName() == "tuple")
    {
        const auto function = lhs.toFunctionNode();
        auto arguments_size = function.getArgumentsSize();
        for (size_t i = 0; i < arguments_size; ++i)
        {
            if (header.has(function.getArgumentAt(i).getColumnName()))
            {
                auto key = header.getPositionByName(function.getArgumentAt(i).getColumnName());
                key_tuple_mapping.emplace_back(i, key);
                column_names.emplace_back(function.getArgumentAt(i).getColumnName());
                data_types.push_back(header.getByPosition(key).type);
            }
        }
    }
    else
    {
        if (header.has(lhs.getColumnName()))
        {
            auto key = header.getPositionByName(lhs.getColumnName());
            key_tuple_mapping.emplace_back(0, key);
            column_names.emplace_back(lhs.getColumnName());
            data_types.push_back(header.getByPosition(key).type);
        }
    }

    if (key_tuple_mapping.empty())
        return false;

    auto future_set = rhs.tryGetPreparedSet(data_types);
    if (!future_set)
        return false;
    
    auto prepared_set = future_set->buildOrderedSetInplace(rhs.getTreeContext().getQueryContext());
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & data_type : prepared_set->getDataTypes())
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString) // Only boost string type.
            return false;

    std::vector<TantivyFilters> tantivy_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    for (size_t i = 0; i < key_tuple_mapping.size(); i++)
    {
        const auto & elem = key_tuple_mapping[i];
        tantivy_filters.emplace_back();
        tantivy_filters.back().reserve(prepared_set->getTotalRowCount());
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            tantivy_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            tantivy_filters.back().back().setQueryString(ref.data, ref.size);
            tantivy_filters.back().back().setQueryColumnName(column_names[i]);
            tantivy_filters.back().back().setQueryType(TantivyFilter::SENTENCE_QUERY);
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_tantivy_filters = std::move(tantivy_filters);
    LOG_DEBUG(
        getLogger("MergeTreeIndexTantivy"),
        "[MergeTreeConditionTantivy::tryPrepareSetTantivyFilter] create tantivy_filters for RPNElement");

    return true;
}


MergeTreeIndexGranulePtr MergeTreeIndexTantivy::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleTantivy>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexTantivy::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    /// should not be called: createIndexAggregatorForPart should be used
    assert(false);
    return nullptr;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexTantivy::createIndexAggregatorForPart(TantivyIndexStorePtr & store, const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorTantivy>(store, index.column_names, index.name, params);
}

MergeTreeIndexConditionPtr MergeTreeIndexTantivy::createIndexCondition(const ActionsDAG * filter_actions_dag, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionTantivy>(filter_actions_dag, context, index.sample_block, params);
}

MergeTreeIndexPtr ftsIndexCreator(const IndexDescription & index)
{
    String tantivy_index_parameter = index.arguments.empty() ? "{}" : index.arguments[0].safeGet<String>();
    TantivyFilterParameters params(tantivy_index_parameter);

    return std::make_shared<MergeTreeIndexTantivy>(index, params);
}

std::pair<String, std::vector<String>> parseTokenizerString(const String & tokenizer_with_parameter)
{
    std::regex tokenizerRegex("([a-zA-Z_]+)(?:\\((.*?)\\))?");
    // store regex sub match str, ["tokenizer_with_parameter", "tokenizer_name", "parameter1, parameter2..."]
    std::smatch tokenizerMatch;

    if (std::regex_match(tokenizer_with_parameter, tokenizerMatch, tokenizerRegex))
    {
        String tokenizer_name = tokenizerMatch[1];
        std::vector<String> params;

        // tokenizerMatch.size() = 2: "tokenizer_with_parameter" only contain tokenizer name
        // tokenizerMatch[2].matched is false: "tokenizer_with_parameter" only contain tokenizer name
        if (tokenizerMatch.size() > 2 && tokenizerMatch[2].matched)
        {
            std::string param_string = tokenizerMatch[2];
            std::istringstream param_stream(param_string);
            String param;

            while (std::getline(param_stream, param, ','))
            {
                param.erase(0, param.find_first_not_of(" \n\r\t")); // Trim leading whitespace
                param.erase(param.find_last_not_of(" \n\r\t") + 1); // Trim trailing whitespace
                params.push_back(param);
                // verify parameter
                if (std::regex_match(param, std::regex("^[A-Za-z0-9_]+$")))
                {
                    params.push_back(param);
                }
                else
                {
                    throw Exception(ErrorCodes::INCORRECT_QUERY, "Can't parse tokenizer, invalid parameter: {}", tokenizer_with_parameter);
                }
            }
        }
        return std::make_pair(tokenizer_name, params);
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Can't parse tokenizer: {}", tokenizer_with_parameter);
    }
}

void ftsIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isArray())
        {
            const auto & gin_type = assert_cast<const DataTypeArray &>(*index_data_type);
            data_type = WhichDataType(gin_type.getNestedType());
        }
        else if (data_type.isLowCardinality())
        {
            const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index_data_type);
            data_type = WhichDataType(low_cardinality.getDictionaryType());
        }

        if (!data_type.isString() && !data_type.isFixedString())
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Fts index can be used only with `String`, `FixedString`,"
                "`LowCardinality(String)`, `LowCardinality(FixedString)` "
                "column or Array with `String` or `FixedString` values column.");
    }

    if (index.arguments.size() > 1)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Fts index must have less than one arguments.");
    }

    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::String)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Fts index argument must be string.");
    }

    /// Just validate
    String index_json_parameter = index.arguments.empty() ? "{}" : index.arguments[0].safeGet<String>();


    TANTIVY::FFIBoolResult json_status = TANTIVY::ffi_verify_index_parameter(index_json_parameter);
    if (json_status.error.is_error)
    {
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "{}", std::string(json_status.error.message));
    }

    if (!json_status.result)
    {
        LOG_ERROR(getLogger("MergeTreeIndexFts"), "[ftsIndexValidator] bad json arguments");
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Json parameter may be error");
    }

    TantivyFilterParameters params(index_json_parameter);
    LOG_DEBUG(getLogger("MergeTreeIndexFts"), "[ftsIndexValidator] OK.");
}

}
