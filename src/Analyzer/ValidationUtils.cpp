#include <Analyzer/ValidationUtils.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/WindowFunctionsUtils.h>

#include <Analyzer/SortNode.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <VectorIndex/Analyzer/SpecialSearchFunctionsUtils.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Utils/CommonUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_AN_AGGREGATE;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int ILLEGAL_PREWHERE;
    extern const int UNSUPPORTED_METHOD;
    extern const int UNEXPECTED_EXPRESSION;
}

namespace
{

void validateFilter(const QueryTreeNodePtr & filter_node, std::string_view exception_place_message, const QueryTreeNodePtr & query_node)
{
    DataTypePtr filter_node_result_type;
    try
    {
        filter_node_result_type = filter_node->getResultType();
    }
    catch (const DB::Exception &e)
    {
        if (e.code() != ErrorCodes::UNSUPPORTED_METHOD)
            e.rethrow();
    }

    if (!filter_node_result_type)
        throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                        "Unexpected expression '{}' in filter in {}. In query {}",
                        filter_node->formatASTForErrorMessage(),
                        exception_place_message,
                        query_node->formatASTForErrorMessage());

    if (!filter_node_result_type->canBeUsedInBooleanContext())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid type for filter in {}: {}. In query {}",
            exception_place_message,
            filter_node_result_type->getName(),
            query_node->formatASTForErrorMessage());
}

}

void validateFilters(const QueryTreeNodePtr & query_node)
{
    const auto & query_node_typed = query_node->as<QueryNode &>();
    if (query_node_typed.hasPrewhere())
    {
        validateFilter(query_node_typed.getPrewhere(), "PREWHERE", query_node);

        assertNoFunctionNodes(query_node_typed.getPrewhere(),
            "arrayJoin",
            ErrorCodes::ILLEGAL_PREWHERE,
            "ARRAY JOIN",
            "in PREWHERE");
    }

    if (query_node_typed.hasWhere())
        validateFilter(query_node_typed.getWhere(), "WHERE", query_node);

    if (query_node_typed.hasHaving())
        validateFilter(query_node_typed.getHaving(), "HAVING", query_node);

    if (query_node_typed.hasQualify())
        validateFilter(query_node_typed.getQualify(), "QUALIFY", query_node);
}

namespace
{

class ValidateGroupByColumnsVisitor : public ConstInDepthQueryTreeVisitor<ValidateGroupByColumnsVisitor>
{
public:
    explicit ValidateGroupByColumnsVisitor(const QueryTreeNodes & group_by_keys_nodes_, const QueryTreeNodePtr & query_node_)
        : group_by_keys_nodes(group_by_keys_nodes_)
        , query_node(query_node_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto query_tree_node_type = node->getNodeType();
        if (query_tree_node_type == QueryTreeNodeType::CONSTANT ||
            query_tree_node_type == QueryTreeNodeType::SORT ||
            query_tree_node_type == QueryTreeNodeType::INTERPOLATE)
            return;

        if (nodeIsAggregateFunctionOrInGroupByKeys(node))
            return;

        auto * function_node = node->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "grouping")
        {
            auto & grouping_function_arguments_nodes = function_node->getArguments().getNodes();
            for (auto & grouping_function_arguments_node : grouping_function_arguments_nodes)
            {
                bool found_argument_in_group_by_keys = false;

                for (const auto & group_by_key_node : group_by_keys_nodes)
                {
                    if (grouping_function_arguments_node->isEqual(*group_by_key_node))
                    {
                        found_argument_in_group_by_keys = true;
                        break;
                    }
                }

                if (!found_argument_in_group_by_keys)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "GROUPING function argument {} is not in GROUP BY keys. In query {}",
                        grouping_function_arguments_node->formatASTForErrorMessage(),
                        query_node->formatASTForErrorMessage());
            }

            return;
        }

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_node_source = column_node->getColumnSource();
        if (column_node_source->getNodeType() == QueryTreeNodeType::LAMBDA)
            return;

        throw Exception(ErrorCodes::NOT_AN_AGGREGATE,
            "Column {} is not under aggregate function and not in GROUP BY keys. In query {}",
            column_node->formatConvertedASTForErrorMessage(),
            query_node->formatASTForErrorMessage());
    }

    bool needChildVisit(const QueryTreeNodePtr & parent_node, const QueryTreeNodePtr & child_node)
    {
        if (nodeIsAggregateFunctionOrInGroupByKeys(parent_node))
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:

    static bool areColumnSourcesEqual(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
    {
        using NodePair = std::pair<const IQueryTreeNode *, const IQueryTreeNode *>;
        std::vector<NodePair> nodes_to_process;
        nodes_to_process.emplace_back(lhs.get(), rhs.get());

        while (!nodes_to_process.empty())
        {
            const auto [lhs_node, rhs_node] = nodes_to_process.back();
            nodes_to_process.pop_back();

            if (lhs_node->getNodeType() != rhs_node->getNodeType())
                return false;

            if (lhs_node->getNodeType() == QueryTreeNodeType::COLUMN)
            {
                const auto * lhs_column_node = lhs_node->as<ColumnNode>();
                const auto * rhs_column_node = rhs_node->as<ColumnNode>();
                if (!lhs_column_node->getColumnSource()->isEqual(*rhs_column_node->getColumnSource()))
                    return false;
            }

            const auto & lhs_children = lhs_node->getChildren();
            const auto & rhs_children = rhs_node->getChildren();
            if (lhs_children.size() != rhs_children.size())
                return false;

            for (size_t i = 0; i < lhs_children.size(); ++i)
            {
                const auto & lhs_child = lhs_children[i];
                const auto & rhs_child = rhs_children[i];

                if (!lhs_child && !rhs_child)
                    continue;
                else if (lhs_child && !rhs_child)
                    return false;
                else if (!lhs_child && rhs_child)
                    return false;

                nodes_to_process.emplace_back(lhs_child.get(), rhs_child.get());
            }
        }
        return true;
    }

    bool nodeIsAggregateFunctionOrInGroupByKeys(const QueryTreeNodePtr & node) const
    {
        if (auto * function_node = node->as<FunctionNode>())
            if (function_node->isAggregateFunction())
                return true;

        for (const auto & group_by_key_node : group_by_keys_nodes)
        {
            if (node->isEqual(*group_by_key_node, {.compare_aliases = false}))
            {
                /** Column sources should be compared with aliases for correct GROUP BY keys validation,
                  * otherwise t2.x and t1.x will be considered as the same column:
                  * SELECT t2.x FROM t1 JOIN t1 as t2 ON t1.x = t2.x GROUP BY t1.x;
                  */
                if (areColumnSourcesEqual(node, group_by_key_node))
                    return true;
            }
        }

        return false;
    }

    const QueryTreeNodes & group_by_keys_nodes;
    const QueryTreeNodePtr & query_node;
};

}

void validateAggregates(const QueryTreeNodePtr & query_node, AggregatesValidationParams params)
{
    const auto & query_node_typed = query_node->as<QueryNode &>();
    auto join_tree_node_type = query_node_typed.getJoinTree()->getNodeType();
    bool join_tree_is_subquery = join_tree_node_type == QueryTreeNodeType::QUERY || join_tree_node_type == QueryTreeNodeType::UNION;

    if (!join_tree_is_subquery)
    {
        assertNoAggregateFunctionNodes(query_node_typed.getJoinTree(), "in JOIN TREE");
        assertNoGroupingFunctionNodes(query_node_typed.getJoinTree(), "in JOIN TREE");
        assertNoWindowFunctionNodes(query_node_typed.getJoinTree(), "in JOIN TREE");
    }

    if (query_node_typed.hasWhere())
    {
        assertNoAggregateFunctionNodes(query_node_typed.getWhere(), "in WHERE");
        assertNoGroupingFunctionNodes(query_node_typed.getWhere(), "in WHERE");
        assertNoWindowFunctionNodes(query_node_typed.getWhere(), "in WHERE");
    }

    if (query_node_typed.hasPrewhere())
    {
        assertNoAggregateFunctionNodes(query_node_typed.getPrewhere(), "in PREWHERE");
        assertNoGroupingFunctionNodes(query_node_typed.getPrewhere(), "in PREWHERE");
        assertNoWindowFunctionNodes(query_node_typed.getPrewhere(), "in PREWHERE");
    }

    if (query_node_typed.hasHaving())
        assertNoWindowFunctionNodes(query_node_typed.getHaving(), "in HAVING");

    if (query_node_typed.hasWindow())
        assertNoWindowFunctionNodes(query_node_typed.getWindowNode(), "in WINDOW");

    QueryTreeNodes aggregate_function_nodes;
    QueryTreeNodes window_function_nodes;

    collectAggregateFunctionNodes(query_node, aggregate_function_nodes);
    collectWindowFunctionNodes(query_node, window_function_nodes);

    if (query_node_typed.hasGroupBy())
    {
        assertNoAggregateFunctionNodes(query_node_typed.getGroupByNode(), "in GROUP BY");
        assertNoGroupingFunctionNodes(query_node_typed.getGroupByNode(), "in GROUP BY");
        assertNoWindowFunctionNodes(query_node_typed.getGroupByNode(), "in GROUP BY");
    }

    for (auto & aggregate_function_node : aggregate_function_nodes)
    {
        auto & aggregate_function_node_typed = aggregate_function_node->as<FunctionNode &>();

        assertNoAggregateFunctionNodes(aggregate_function_node_typed.getArgumentsNode(), "inside another aggregate function");
        assertNoGroupingFunctionNodes(aggregate_function_node_typed.getArgumentsNode(), "inside another aggregate function");
        assertNoWindowFunctionNodes(aggregate_function_node_typed.getArgumentsNode(), "inside an aggregate function");
    }

    for (auto & window_function_node : window_function_nodes)
    {
        auto & window_function_node_typed = window_function_node->as<FunctionNode &>();
        assertNoWindowFunctionNodes(window_function_node_typed.getArgumentsNode(), "inside another window function");

        if (query_node_typed.hasWindow())
            assertNoWindowFunctionNodes(window_function_node_typed.getWindowNode(), "inside window definition");
    }

    QueryTreeNodes group_by_keys_nodes;
    group_by_keys_nodes.reserve(query_node_typed.getGroupBy().getNodes().size());

    for (const auto & node : query_node_typed.getGroupBy().getNodes())
    {
        if (query_node_typed.isGroupByWithGroupingSets())
        {
            auto & grouping_set_keys = node->as<ListNode &>();
            for (auto & grouping_set_key : grouping_set_keys.getNodes())
            {
                if (grouping_set_key->as<ConstantNode>())
                    continue;

                group_by_keys_nodes.push_back(grouping_set_key->clone());
                if (params.group_by_use_nulls)
                    group_by_keys_nodes.back()->convertToNullable();
            }
        }
        else
        {
            if (node->as<ConstantNode>())
                continue;

            group_by_keys_nodes.push_back(node->clone());
            if (params.group_by_use_nulls)
                group_by_keys_nodes.back()->convertToNullable();
        }
    }

    if (query_node_typed.getGroupBy().getNodes().empty())
    {
        if (query_node_typed.hasHaving())
            assertNoGroupingFunctionNodes(query_node_typed.getHaving(), "in HAVING without GROUP BY");

        if (query_node_typed.hasOrderBy())
            assertNoGroupingFunctionNodes(query_node_typed.getOrderByNode(), "in ORDER BY without GROUP BY");

        assertNoGroupingFunctionNodes(query_node_typed.getProjectionNode(), "in SELECT without GROUP BY");
    }

    bool has_aggregation = !query_node_typed.getGroupBy().getNodes().empty() || !aggregate_function_nodes.empty();

    if (has_aggregation)
    {
        ValidateGroupByColumnsVisitor validate_group_by_columns_visitor(group_by_keys_nodes, query_node);

        if (query_node_typed.hasHaving())
            validate_group_by_columns_visitor.visit(query_node_typed.getHaving());

        if (query_node_typed.hasQualify())
            validate_group_by_columns_visitor.visit(query_node_typed.getQualify());

        if (query_node_typed.hasOrderBy())
            validate_group_by_columns_visitor.visit(query_node_typed.getOrderByNode());

        if (query_node_typed.hasInterpolate())
            validate_group_by_columns_visitor.visit(query_node_typed.getInterpolate());

        validate_group_by_columns_visitor.visit(query_node_typed.getProjectionNode());
    }

    bool aggregation_with_rollup_or_cube_or_grouping_sets = query_node_typed.isGroupByWithRollup() ||
        query_node_typed.isGroupByWithCube() ||
        query_node_typed.isGroupByWithGroupingSets();
    if (!has_aggregation && (query_node_typed.isGroupByWithTotals() || aggregation_with_rollup_or_cube_or_grouping_sets))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS, ROLLUP, CUBE or GROUPING SETS are not supported without aggregation");
}

void validateHybridSearchFuncs(const QueryTreeNodePtr & query_node)
{
    const auto & query_node_typed = query_node->as<QueryNode &>();

    QueryTreeNodes hybrid_function_nodes;
    collectHybridSearchFunctionNodes(query_node, hybrid_function_nodes);

    if (hybrid_function_nodes.size() == 0)
        return;

    if (hybrid_function_nodes.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support more than one function among vector scan, text search, or hybrid search in one query now");

    auto * function_node = hybrid_function_nodes[0]->as<FunctionNode>();
    if (!function_node)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid hybrid search function");

    String func_name = function_node->getFunctionName();

    if (!query_node_typed.hasOrderBy())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support {} function without ORDER BY clause", func_name);

    bool is_batch = isBatchDistance(func_name);
    if (is_batch && !query_node_typed.hasLimitByLimit())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support batch {} function without LIMIT N BY clause", func_name);
    else if (!is_batch && !query_node_typed.hasLimit())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support {} function without LIMIT N clause", func_name);

    /// Further check if hybrid search function column exists in ORDER BY
    if (!hasHybridSearchFunctionNodes(query_node_typed.getOrderByNode()))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support without {} function inside ORDER BY clause", func_name);

    /// Some checks in collectForXXXSearchFunctions()
    /// Get search column from argument
    QueryTreeNodePtr vector_col_node;
    QueryTreeNodePtr text_col_node;
    bool has_vector = false, has_text = false;
    QueryTreeNodes argument_nodes = function_node->getArguments().getNodes();
    if (isTextSearch(func_name))
    {
        has_text = true;
        text_col_node = argument_nodes[0];
    }
    else if (isVectorScanFunc(func_name))
    {
        has_vector = true;
        vector_col_node = argument_nodes[0];
    }
    else if (isHybridSearch(func_name))
    {
        has_vector = true;
        has_text = true;

        vector_col_node = argument_nodes[0];
        text_col_node = argument_nodes[1];
    }

    /// Check search column data type
    if (has_text)
    {
        bool is_mapkeys = false;

        auto * text_column = text_col_node->as<ColumnNode>();
        if (!text_column)
        {
            /// Check mapKeys for text column
            if (auto * text_function_node = text_col_node->as<FunctionNode>())
            {
                if ((text_function_node->getFunctionName() == "mapKeys") && text_function_node->getArguments().getNodes().size() == 1)
                {
                    text_column = text_function_node->getArguments().getNodes()[0]->as<ColumnNode>();
                    is_mapkeys = true;
                }
            }
        }

        if (!text_column)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "The {} argument of {} function should be text column name", has_vector ? "second" : "first", func_name);

        DataTypePtr search_text_column_type = text_column->getColumnType();
        checkTextSearchColumnDataType(search_text_column_type, is_mapkeys);
    }

    /// Check sort direction of hybrid search function column
    /// reference QueryAnalyzer::resolveSortNodeList()
    SortDirection sort_direction = SortDirection::ASCENDING;
    auto & sort_node_list_typed = query_node_typed.getOrderByNode()->as<ListNode &>();
    for (auto & node : sort_node_list_typed.getNodes())
    {
        auto & sort_node = node->as<SortNode &>();
        auto * function_expression = sort_node.getExpression()->as<FunctionNode>();
        if (!function_expression || !isHybridSearchFunc(function_expression->getFunctionName()))
            continue;
        sort_direction = sort_node.getSortDirection();
    }

    if (has_text)
    {
        if (sort_direction == SortDirection::ASCENDING)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "The results returned by the {} function should be ordered by `DESC`", func_name);
    }
    else if (has_vector)
    {
        auto * vector_column = vector_col_node->as<ColumnNode>();
        if (!vector_column)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "The {} argument of {} function should be vector column name", has_text ? "second" : "first", func_name);

        /// The sort direction of vector scan function is related to metric_type
        String vec_col_name = vector_column->getColumnName();

        StorageMetadataPtr metadata_snapshot = nullptr;
        bool table_is_remote = false;

        auto vector_column_source = vector_column->getColumnSourceOrNull();
        if (vector_column_source)
        {
            auto node_type = vector_column_source->getNodeType();
            if (node_type == QueryTreeNodeType::TABLE)
            {
                if (auto * table_node = vector_column_source->as<TableNode>())
                {
                    const auto & table_storage = table_node->getStorage();
                    if (table_storage)
                    {
                        metadata_snapshot = table_storage->getInMemoryMetadataPtr();
                        table_is_remote = table_storage->isRemote();
                    }
                }
            }
        }

        if (!table_is_remote)
        {
            DataTypePtr search_vector_column_type = vector_column->getColumnType();
            auto vector_search_type = getSearchIndexDataType(search_vector_column_type);

            String vector_scan_metric_type = getMetricType(metadata_snapshot, vector_search_type, vec_col_name, query_node_typed.getContext());
            Poco::toUpperInPlace(vector_scan_metric_type);

            if (vector_scan_metric_type == "IP")
            {
                if (sort_direction == SortDirection::ASCENDING)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Use 'ORDER BY distance DESC' when the metric type is IP");
            }
            else if (sort_direction == SortDirection::DESCENDING)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Use 'ORDER BY distance ASC' when the metric type is {}", vector_scan_metric_type);
        }
    }
}

namespace
{

class ValidateFunctionNodesVisitor : public ConstInDepthQueryTreeVisitor<ValidateFunctionNodesVisitor>
{
public:
    explicit ValidateFunctionNodesVisitor(std::string_view function_name_,
        int exception_code_,
        std::string_view exception_function_name_,
        std::string_view exception_place_message_)
        : function_name(function_name_)
        , exception_code(exception_code_)
        , exception_function_name(exception_function_name_)
        , exception_place_message(exception_place_message_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == function_name)
            throw Exception(exception_code,
                "{} function {} is found {} in query",
                exception_function_name,
                function_node->formatASTForErrorMessage(),
                exception_place_message);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    std::string_view function_name;
    int exception_code = 0;
    std::string_view exception_function_name;
    std::string_view exception_place_message;
};

}

void assertNoFunctionNodes(const QueryTreeNodePtr & node,
    std::string_view function_name,
    int exception_code,
    std::string_view exception_function_name,
    std::string_view exception_place_message)
{
    ValidateFunctionNodesVisitor visitor(function_name, exception_code, exception_function_name, exception_place_message);
    visitor.visit(node);
}

void validateTreeSize(const QueryTreeNodePtr & node,
    size_t max_size,
    std::unordered_map<QueryTreeNodePtr, size_t> & node_to_tree_size)
{
    size_t tree_size = 0;
    std::vector<std::pair<QueryTreeNodePtr, bool>> nodes_to_process;
    nodes_to_process.emplace_back(node, false);

    while (!nodes_to_process.empty())
    {
        const auto [node_to_process, processed_children] = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (processed_children)
        {
            ++tree_size;

            size_t subtree_size = 1;
            for (const auto & node_to_process_child : node_to_process->getChildren())
            {
                if (!node_to_process_child)
                    continue;

                subtree_size += node_to_tree_size[node_to_process_child];
            }

            auto * constant_node = node_to_process->as<ConstantNode>();
            if (constant_node && constant_node->hasSourceExpression())
                subtree_size += node_to_tree_size[constant_node->getSourceExpression()];

            node_to_tree_size.emplace(node_to_process, subtree_size);
            continue;
        }

        auto node_to_size_it = node_to_tree_size.find(node_to_process);
        if (node_to_size_it != node_to_tree_size.end())
        {
            tree_size += node_to_size_it->second;
            continue;
        }

        nodes_to_process.emplace_back(node_to_process, true);

        for (const auto & node_to_process_child : node_to_process->getChildren())
        {
            if (!node_to_process_child)
                continue;

            nodes_to_process.emplace_back(node_to_process_child, false);
        }

        auto * constant_node = node_to_process->as<ConstantNode>();
        if (constant_node && constant_node->hasSourceExpression())
            nodes_to_process.emplace_back(constant_node->getSourceExpression(), false);
    }

    if (tree_size > max_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Query tree is too big. Maximum: {}",
            max_size);
}

}
