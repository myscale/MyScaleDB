#include <algorithm>
#include <memory>
#include <set>

#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>

#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/ComparisonTupleEliminationVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/ExpressionActions.h> /// getSmallestColumn()
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/GroupingSetsRewriterVisitor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/RewriteOrderByVisitor.hpp>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/TreeOptimizer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/replaceForPositionalArguments.h>
#include <Interpreters/replaceMissedSubcolumnsInQuery.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCreateQuery.h>

#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <IO/WriteHelpers.h>
#include <Storages/IStorage.h>
#include <Storages/StorageJoin.h>
#include "Common/Allocator.h"
#include <Common/checkStackSize.h>
#include <Storages/StorageView.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionHelpers.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Interpreters/GetHybridSearchVisitor.h>
#include <VectorIndex/Interpreters/parseVSParameters.h>

#include <Parsers/formatAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int EMPTY_NESTED_TABLE;
    extern const int EXPECTED_ALL_OR_ANY;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SYNTAX_ERROR;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int BAD_ARGUMENTS;
}

namespace
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs

void optimizeGroupingSets(ASTPtr & query)
{
    GroupingSetsRewriterVisitor::Data data;
    GroupingSetsRewriterVisitor(data).visit(query);
}

/// Select implementation of a function based on settings.
/// Important that it is done as query rewrite. It means rewritten query
///  will be sent to remote servers during distributed query execution,
///  and on all remote servers, function implementation will be same.
template <char const * func_name>
struct CustomizeFunctionsData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_name;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        if (Poco::toLower(func.name) == func_name)
        {
            func.name = customized_func_name;
        }
    }
};

char countdistinct[] = "countdistinct";
using CustomizeCountDistinctVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<countdistinct>>, true>;

char countifdistinct[] = "countifdistinct";
using CustomizeCountIfDistinctVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<countifdistinct>>, true>;

char in[] = "in";
using CustomizeInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<in>>, true>;

char notIn[] = "notin";
using CustomizeNotInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<notIn>>, true>;

char globalIn[] = "globalin";
using CustomizeGlobalInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<globalIn>>, true>;

char globalNotIn[] = "globalnotin";
using CustomizeGlobalNotInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<globalNotIn>>, true>;

template <char const * func_suffix>
struct CustomizeFunctionsSuffixData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_suffix;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        if (endsWith(Poco::toLower(func.name), func_suffix))
        {
            size_t prefix_len = func.name.length() - strlen(func_suffix);
            func.name = func.name.substr(0, prefix_len) + customized_func_suffix;
        }
    }
};

/// Swap 'if' and 'distinct' suffixes to make execution more optimal.
char ifDistinct[] = "ifdistinct";
using CustomizeIfDistinctVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsSuffixData<ifDistinct>>, true>;

/// Used to rewrite all aggregate functions to add -OrNull suffix to them if setting `aggregate_functions_null_for_empty` is set.
struct CustomizeAggregateFunctionsSuffixData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_suffix;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        const auto & instance = AggregateFunctionFactory::instance();
        if (instance.isAggregateFunctionName(func.name) && !endsWith(func.name, customized_func_suffix) && !endsWith(func.name, customized_func_suffix + "If"))
        {
            auto properties = instance.tryGetProperties(func.name);
            if (properties && !properties->returns_default_when_only_null)
            {
                func.name += customized_func_suffix;
            }
        }
    }
};

// Used to rewrite aggregate functions with -OrNull suffix in some cases, such as sumIfOrNull, we should rewrite to sumOrNullIf
struct CustomizeAggregateFunctionsMoveSuffixData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_suffix;

    String moveSuffixAhead(const String & name) const
    {
        auto prefix = name.substr(0, name.size() - customized_func_suffix.size());

        auto prefix_size = prefix.size();

        if (endsWith(prefix, "MergeState"))
            return prefix.substr(0, prefix_size - 10) + customized_func_suffix + "MergeState";

        if (endsWith(prefix, "Merge"))
            return prefix.substr(0, prefix_size - 5) + customized_func_suffix + "Merge";

        if (endsWith(prefix, "State"))
            return prefix.substr(0, prefix_size - 5) + customized_func_suffix + "State";

        if (endsWith(prefix, "If"))
            return prefix.substr(0, prefix_size - 2) + customized_func_suffix + "If";

        return name;
    }

    void visit(ASTFunction & func, ASTPtr &) const
    {
        const auto & instance = AggregateFunctionFactory::instance();
        if (instance.isAggregateFunctionName(func.name))
        {
            if (endsWith(func.name, customized_func_suffix))
            {
                auto properties = instance.tryGetProperties(func.name);
                if (properties && !properties->returns_default_when_only_null)
                {
                    func.name = moveSuffixAhead(func.name);
                }
            }
        }
    }
};

using CustomizeAggregateFunctionsOrNullVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeAggregateFunctionsSuffixData>, true>;
using CustomizeAggregateFunctionsMoveOrNullVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeAggregateFunctionsMoveSuffixData>, true>;

struct ExistsExpressionData
{
    using TypeToVisit = ASTFunction;

    static void visit(ASTFunction & func, ASTPtr)
    {
        bool exists_expression = func.name == "exists"
            && func.arguments && func.arguments->children.size() == 1
            && typeid_cast<const ASTSubquery *>(func.arguments->children[0].get());

        if (!exists_expression)
            return;

        /// EXISTS(subquery) --> 1 IN (SELECT 1 FROM subquery LIMIT 1)

        auto subquery_node = func.arguments->children[0];
        auto table_expression = std::make_shared<ASTTableExpression>();
        table_expression->subquery = std::move(subquery_node);
        table_expression->children.push_back(table_expression->subquery);

        auto tables_in_select_element = std::make_shared<ASTTablesInSelectQueryElement>();
        tables_in_select_element->table_expression = std::move(table_expression);
        tables_in_select_element->children.push_back(tables_in_select_element->table_expression);

        auto tables_in_select = std::make_shared<ASTTablesInSelectQuery>();
        tables_in_select->children.push_back(std::move(tables_in_select_element));

        auto select_expr_list = std::make_shared<ASTExpressionList>();
        select_expr_list->children.push_back(std::make_shared<ASTLiteral>(1u));

        auto select_query = std::make_shared<ASTSelectQuery>();
        select_query->children.push_back(select_expr_list);

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expr_list);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables_in_select);

        ASTPtr limit_length_ast = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length_ast));

        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
        select_with_union_query->list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

        auto new_subquery = std::make_shared<ASTSubquery>();
        new_subquery->children.push_back(select_with_union_query);

        auto function = makeASTFunction("in", std::make_shared<ASTLiteral>(1u), new_subquery);
        func = *function;
    }
};

using ExistsExpressionVisitor = InDepthNodeVisitor<OneTypeMatcher<ExistsExpressionData>, false>;

struct ReplacePositionalArgumentsData
{
    using TypeToVisit = ASTSelectQuery;

    static void visit(ASTSelectQuery & select_query, ASTPtr &)
    {
        if (select_query.groupBy())
        {
            for (auto & expr : select_query.groupBy()->children)
                replaceForPositionalArguments(expr, &select_query, ASTSelectQuery::Expression::GROUP_BY);
        }
        if (select_query.orderBy())
        {
            for (auto & expr : select_query.orderBy()->children)
            {
                auto & elem = assert_cast<ASTOrderByElement &>(*expr).children.at(0);
                replaceForPositionalArguments(elem, &select_query, ASTSelectQuery::Expression::ORDER_BY);
            }
        }
        if (select_query.limitBy())
        {
            for (auto & expr : select_query.limitBy()->children)
                replaceForPositionalArguments(expr, &select_query, ASTSelectQuery::Expression::LIMIT_BY);
        }
    }
};

using ReplacePositionalArgumentsVisitor = InDepthNodeVisitor<OneTypeMatcher<ReplacePositionalArgumentsData>, false>;

/// Translate qualified names such as db.table.column, table.column, table_alias.column to names' normal form.
/// Expand asterisks and qualified asterisks with column names.
/// There would be columns in normal form & column aliases after translation. Column & column alias would be normalized in QueryNormalizer.
void translateQualifiedNames(ASTPtr & query, const ASTSelectQuery & select_query, const NameSet & source_columns_set,
                             const TablesWithColumns & tables_with_columns, const NameToNameMap & parameter_values = {},
                             const NameToNameMap & parameter_types = {})
{
    LogAST log;
    TranslateQualifiedNamesVisitor::Data visitor_data(source_columns_set, tables_with_columns, true/* has_columns */, parameter_values, parameter_types);
    TranslateQualifiedNamesVisitor visitor(visitor_data, log.stream());
    visitor.visit(query);

    /// This may happen after expansion of COLUMNS('regexp').
    if (select_query.select()->children.empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED, "Empty list of columns in SELECT query");
}

bool hasArrayJoin(const ASTPtr & ast)
{
    if (const ASTFunction * function = ast->as<ASTFunction>())
        if (function->name == "arrayJoin")
            return true;

    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && hasArrayJoin(child))
            return true;

    return false;
}

/// Keep number of columns for 'GLOBAL IN (SELECT 1 AS a, a)'
void renameDuplicatedColumns(const ASTSelectQuery * select_query)
{
    ASTs & elements = select_query->select()->children;

    std::set<String> all_column_names;
    std::set<String> assigned_column_names;

    for (auto & expr : elements)
        all_column_names.insert(expr->getAliasOrColumnName());

    for (auto & expr : elements)
    {
        auto name = expr->getAliasOrColumnName();

        if (!assigned_column_names.insert(name).second)
        {
            size_t i = 1;
            while (all_column_names.end() != all_column_names.find(name + "_" + toString(i)))
                ++i;

            name = name + "_" + toString(i);
            expr = expr->clone();   /// Cancels fuse of the same expressions in the tree.
            expr->setAlias(name);

            all_column_names.insert(name);
            assigned_column_names.insert(name);
        }
    }
}

/// Sometimes we have to calculate more columns in SELECT clause than will be returned from query.
/// This is the case when we have DISTINCT or arrayJoin: we require more columns in SELECT even if we need less columns in result.
/// Also we have to remove duplicates in case of GLOBAL subqueries. Their results are placed into tables so duplicates are impossible.
/// Also remove all INTERPOLATE columns which are not in SELECT anymore.
void removeUnneededColumnsFromSelectClause(ASTSelectQuery * select_query, const Names & required_result_columns, bool remove_dups)
{
    ASTs & elements = select_query->select()->children;

    std::map<String, size_t> required_columns_with_duplicate_count;

    if (!required_result_columns.empty())
    {
        /// Some columns may be queried multiple times, like SELECT x, y, y FROM table.
        for (const auto & name : required_result_columns)
        {
            if (remove_dups)
                required_columns_with_duplicate_count[name] = 1;
            else
                ++required_columns_with_duplicate_count[name];
        }
    }
    else if (remove_dups)
    {
        /// Even if we have no requirements there could be duplicates cause of asterisks. SELECT *, t.*
        for (const auto & elem : elements)
            required_columns_with_duplicate_count.emplace(elem->getAliasOrColumnName(), 1);
    }
    else
        return;

    ASTs new_elements;
    new_elements.reserve(elements.size());

    NameSet remove_columns;

    for (const auto & elem : elements)
    {
        String name = elem->getAliasOrColumnName();

        auto it = required_columns_with_duplicate_count.find(name);
        if (required_columns_with_duplicate_count.end() != it && it->second)
        {
            new_elements.push_back(elem);
            --it->second;
        }
        else if (select_query->distinct || hasArrayJoin(elem))
        {
            /// ARRAY JOIN cannot be optimized out since it may change number of rows,
            /// so as DISTINCT.
            new_elements.push_back(elem);
        }
        else
        {
            remove_columns.insert(name);

            ASTFunction * func = elem->as<ASTFunction>();

            /// Never remove untuple. It's result column may be in required columns.
            /// It is not easy to analyze untuple here, because types were not calculated yet.
            if (func && func->name == "untuple")
                new_elements.push_back(elem);

            /// removing aggregation can change number of rows, so `count()` result in outer sub-query would be wrong
            if (func && !select_query->groupBy())
            {
                GetAggregatesVisitor::Data data = {};
                GetAggregatesVisitor(data).visit(elem);
                if (!data.aggregates.empty())
                    new_elements.push_back(elem);
            }

            /// Removing hybrid search related function can change number of rows.
            if (func && isHybridSearchFunc(func->name))
                new_elements.push_back(elem);
        }
    }

    if (select_query->interpolate())
    {
        auto & children = select_query->interpolate()->children;
        if (!children.empty())
        {
            for (auto * it = children.begin(); it != children.end();)
            {
                if (remove_columns.contains((*it)->as<ASTInterpolateElement>()->column))
                    it = select_query->interpolate()->children.erase(it);
                else
                    ++it;
            }

            if (children.empty())
                select_query->setExpression(ASTSelectQuery::Expression::INTERPOLATE, nullptr);
        }
    }

    elements = std::move(new_elements);
}

/// Replacing scalar subqueries with constant values.
void executeScalarSubqueries(
    ASTPtr & query, ContextPtr context, size_t subquery_depth, Scalars & scalars, Scalars & local_scalars, bool only_analyze, bool is_create_parameterized_view)
{
    LogAST log;
    ExecuteScalarSubqueriesVisitor::Data visitor_data{WithContext{context}, subquery_depth, scalars, local_scalars, only_analyze, is_create_parameterized_view};
    ExecuteScalarSubqueriesVisitor(visitor_data, log.stream()).visit(query);
}

void getArrayJoinedColumns(ASTPtr & query, TreeRewriterResult & result, const ASTSelectQuery * select_query,
                           const NamesAndTypesList & source_columns, const NameSet & source_columns_set)
{
    if (!select_query->arrayJoinExpressionList().first)
        return;

    ArrayJoinedColumnsVisitor::Data visitor_data{
        result.aliases, result.array_join_name_to_alias, result.array_join_alias_to_name, result.array_join_result_to_source};
    ArrayJoinedColumnsVisitor(visitor_data).visit(query);

    /// If the result of ARRAY JOIN is not used, it is necessary to ARRAY-JOIN any column,
    /// to get the correct number of rows.
    if (result.array_join_result_to_source.empty())
    {
        if (select_query->arrayJoinExpressionList().first->children.empty())
            throw DB::Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "ARRAY JOIN requires an argument");

        ASTPtr expr = select_query->arrayJoinExpressionList().first->children.at(0);
        String source_name = expr->getColumnName();
        String result_name = expr->getAliasOrColumnName();

        /// This is an array.
        if (!expr->as<ASTIdentifier>() || source_columns_set.contains(source_name))
        {
            result.array_join_result_to_source[result_name] = source_name;
        }
        else /// This is a nested table.
        {
            bool found = false;
            for (const auto & column : source_columns)
            {
                auto split = Nested::splitName(column.name, /*reverse=*/ true);
                if (split.first == source_name && !split.second.empty())
                {
                    result.array_join_result_to_source[Nested::concatenateName(result_name, split.second)] = column.name;
                    found = true;
                    break;
                }
            }
            if (!found)
                throw Exception(ErrorCodes::EMPTY_NESTED_TABLE, "No columns in nested table {}", source_name);
        }
    }
}

void setJoinStrictness(ASTSelectQuery & select_query, JoinStrictness join_default_strictness, bool old_any, std::shared_ptr<TableJoin> & analyzed_join)
{
    const ASTTablesInSelectQueryElement * node = select_query.join();
    if (!node)
        return;

    auto & table_join = const_cast<ASTTablesInSelectQueryElement *>(node)->table_join->as<ASTTableJoin &>();

    if (table_join.strictness == JoinStrictness::Unspecified &&
        table_join.kind != JoinKind::Cross)
    {
        if (join_default_strictness == JoinStrictness::Any)
            table_join.strictness = JoinStrictness::Any;
        else if (join_default_strictness == JoinStrictness::All)
            table_join.strictness = JoinStrictness::All;
        else
            throw Exception(DB::ErrorCodes::EXPECTED_ALL_OR_ANY,
                            "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty");
    }

    if (old_any)
    {
        if (table_join.strictness == JoinStrictness::Any &&
            table_join.kind == JoinKind::Inner)
        {
            table_join.strictness = JoinStrictness::Semi;
            table_join.kind = JoinKind::Left;
        }

        if (table_join.strictness == JoinStrictness::Any)
            table_join.strictness = JoinStrictness::RightAny;
    }
    else
    {
        if (table_join.strictness == JoinStrictness::Any && table_join.kind == JoinKind::Full)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ANY FULL JOINs are not implemented");
    }

    analyzed_join->getTableJoin() = table_join;
}

/// Evaluate expression and return boolean value if it can be interpreted as bool.
/// Only UInt8 or NULL are allowed.
/// Returns `false` for 0 or NULL values, `true` for any non-negative value.
std::optional<bool> tryEvaluateConstCondition(ASTPtr expr, ContextPtr context)
{
    if (!expr)
        return {};

    Field eval_res;
    DataTypePtr eval_res_type;
    {
        auto constant_expression_result = tryEvaluateConstantExpression(expr, context);
        if (!constant_expression_result)
            return {};
        std::tie(eval_res, eval_res_type) = std::move(constant_expression_result.value());
    }

    /// UInt8, maybe Nullable, maybe LowCardinality, and NULL are allowed
    eval_res_type = removeNullable(removeLowCardinality(eval_res_type));
    if (auto which = WhichDataType(eval_res_type); !which.isUInt8() && !which.isNothing())
        return {};

    if (eval_res.isNull())
        return false;

    UInt8 res = eval_res.template safeGet<UInt8>();
    return res > 0;
}

bool tryJoinOnConst(TableJoin & analyzed_join, const ASTPtr & on_expression, ContextPtr context)
{
    if (!analyzed_join.isEnabledAlgorithm(JoinAlgorithm::HASH))
        return false;

    if (analyzed_join.strictness() == JoinStrictness::Asof)
        return false;

    if (analyzed_join.isSpecialStorage())
        return false;

    if (auto eval_const_res = tryEvaluateConstCondition(on_expression, context))
    {
        if (eval_const_res.value())
        {
            /// JOIN ON 1 == 1
            LOG_DEBUG(&Poco::Logger::get("TreeRewriter"), "Join on constant executed as cross join");
            analyzed_join.resetToCross();
        }
        else
        {
            /// JOIN ON 1 != 1
            LOG_DEBUG(&Poco::Logger::get("TreeRewriter"), "Join on constant executed as empty join");
            analyzed_join.resetKeys();
        }
        return true;
    }
    return false;
}

/// Find the columns that are obtained by JOIN.
void collectJoinedColumns(TableJoin & analyzed_join, ASTTableJoin & table_join,
                          const TablesWithColumns & tables, const Aliases & aliases, ContextPtr context)
{
    assert(tables.size() >= 2);

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();

        analyzed_join.addDisjunct();
        for (const auto & key : keys.children)
            analyzed_join.addUsingKey(key);
    }
    else if (table_join.on_expression)
    {
        bool join_on_const_ok = tryJoinOnConst(analyzed_join, table_join.on_expression, context);
        if (join_on_const_ok)
            return;

        bool is_asof = (table_join.strictness == JoinStrictness::Asof);

        CollectJoinOnKeysVisitor::Data data{analyzed_join, tables[0], tables[1], aliases, is_asof};
        if (auto * or_func = table_join.on_expression->as<ASTFunction>(); or_func && or_func->name == "or")
        {
            for (auto & disjunct : or_func->arguments->children)
            {
                analyzed_join.addDisjunct();
                CollectJoinOnKeysVisitor(data).visit(disjunct);
            }
            assert(analyzed_join.getClauses().size() == or_func->arguments->children.size());
        }
        else
        {
            analyzed_join.addDisjunct();
            CollectJoinOnKeysVisitor(data).visit(table_join.on_expression);
            assert(analyzed_join.oneDisjunct());
        }

        auto check_keys_empty = [] (auto e) { return e.key_names_left.empty(); };
        bool any_keys_empty = std::any_of(analyzed_join.getClauses().begin(), analyzed_join.getClauses().end(), check_keys_empty);

        if (any_keys_empty)
            throw DB::Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                                "Cannot get JOIN keys from JOIN ON section: '{}', found keys: {}",
                                queryToString(table_join.on_expression), TableJoin::formatClauses(analyzed_join.getClauses()));

        if (is_asof)
        {
            if (!analyzed_join.oneDisjunct())
                throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join doesn't support multiple ORs for keys in JOIN ON section");
            data.asofToJoinKeys();
        }

        if (!analyzed_join.oneDisjunct() && !analyzed_join.isEnabledAlgorithm(JoinAlgorithm::HASH))
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `hash` join supports multiple ORs for keys in JOIN ON section");
    }
}

std::pair<bool, UInt64> recursivelyCollectMaxOrdinaryExpressions(const ASTPtr & expr, ASTExpressionList & into)
{
    checkStackSize();

    if (expr->as<ASTIdentifier>())
    {
        into.children.push_back(expr);
        return {false, 1};
    }

    auto * function = expr->as<ASTFunction>();

    if (!function)
        return {false, 0};

    if (AggregateUtils::isAggregateFunction(*function))
        return {true, 0};

    UInt64 pushed_children = 0;
    bool has_aggregate = false;

    for (const auto & child : function->arguments->children)
    {
        auto [child_has_aggregate, child_pushed_children] = recursivelyCollectMaxOrdinaryExpressions(child, into);
        has_aggregate |= child_has_aggregate;
        pushed_children += child_pushed_children;
    }

    /// The current function is not aggregate function and there is no aggregate function in its arguments,
    /// so use the current function to replace its arguments
    if (!has_aggregate)
    {
        for (UInt64 i = 0; i < pushed_children; i++)
            into.children.pop_back();

        into.children.push_back(expr);
        pushed_children = 1;
    }

    return {has_aggregate, pushed_children};
}

/** Expand GROUP BY ALL by extracting all the SELECT-ed expressions that are not aggregate functions.
  *
  * For a special case that if there is a function having both aggregate functions and other fields as its arguments,
  * the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.
  *
  * Example:
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY ALL
  * will expand as
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY substring(a, 4, 2), substring(a, 1, 2)
  */
void expandGroupByAll(ASTSelectQuery * select_query)
{
    auto group_expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & expr : select_query->select()->children)
        recursivelyCollectMaxOrdinaryExpressions(expr, *group_expression_list);

    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_expression_list);
}

std::vector<const ASTFunction *> getAggregates(ASTPtr & query, const ASTSelectQuery & select_query)
{
    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query.where())
        assertNoAggregates(select_query.where(), "in WHERE");
    if (select_query.prewhere())
        assertNoAggregates(select_query.prewhere(), "in PREWHERE");

    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);

    /// There can not be other aggregate functions within the aggregate functions.
    for (const ASTFunction * node : data.aggregates)
    {
        if (node->arguments)
        {
            for (auto & arg : node->arguments->children)
            {
                assertNoAggregates(arg, "inside another aggregate function");
                // We also can't have window functions inside aggregate functions,
                // because the window functions are calculated later.
                assertNoWindows(arg, "inside an aggregate function");
                /// Currently not support vector scan, text search and hybrid search function inside aggregate functions
                assertNoVectorScan(arg, "inside an aggregate function");
                assertNoTextSearch(arg, "inside an aggregate function");
                assertNoHybridSearch(arg, "inside an aggregate function");
            }
        }
    }
    return data.aggregates;
}

std::vector<const ASTFunction *> getWindowFunctions(ASTPtr & query, const ASTSelectQuery & select_query)
{
    /// There can not be window functions inside the WHERE, PREWHERE and HAVING
    if (select_query.having())
        assertNoWindows(select_query.having(), "in HAVING");
    if (select_query.where())
        assertNoWindows(select_query.where(), "in WHERE");
    if (select_query.prewhere())
        assertNoWindows(select_query.prewhere(), "in PREWHERE");
    if (select_query.window())
        assertNoWindows(select_query.window(), "in WINDOW");

    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);

    /// Window functions cannot be inside aggregates or other window functions.
    /// Aggregate functions can be inside window functions because they are
    /// calculated earlier.
    for (const ASTFunction * node : data.window_functions)
    {
        if (node->arguments)
        {
            for (auto & arg : node->arguments->children)
            {
                assertNoWindows(arg, "inside another window function");
            }
        }

        if (node->window_definition)
        {
            assertNoWindows(node->window_definition, "inside window definition");
        }
    }

    return data.window_functions;
}

class MarkTupleLiteralsAsLegacyData
{
public:
    struct Data
    {
    };

    static void visitLiteral(ASTLiteral & literal, ASTPtr &)
    {
        if (literal.value.getType() == Field::Types::Tuple)
            literal.use_legacy_column_name_of_tuple = true;
    }
    static void visitFunction(ASTFunction & func, ASTPtr &ast)
    {
        if (func.name == "tuple" && func.arguments && !func.arguments->children.empty())
        {
            // re-write tuple() function as literal
            if (auto literal = func.toLiteral())
            {
                ast = literal;
                visitLiteral(*typeid_cast<ASTLiteral *>(ast.get()), ast);
            }
        }
    }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * identifier = typeid_cast<ASTFunction *>(ast.get()))
            visitFunction(*identifier, ast);
        if (auto * identifier = typeid_cast<ASTLiteral *>(ast.get()))
            visitLiteral(*identifier, ast);
    }

    static bool needChildVisit(const ASTPtr & /*parent*/, const ASTPtr & /*child*/)
    {
        return true;
    }
};

using MarkTupleLiteralsAsLegacyVisitor = InDepthNodeVisitor<MarkTupleLiteralsAsLegacyData, true>;

void markTupleLiteralsAsLegacy(ASTPtr & query)
{
    MarkTupleLiteralsAsLegacyVisitor::Data data;
    MarkTupleLiteralsAsLegacyVisitor(data).visit(query);
}

/// Rewrite _shard_num -> shardNum() AS _shard_num
struct RewriteShardNum
{
    struct Data
    {
    };

    static bool needChildVisit(const ASTPtr & parent, const ASTPtr & /*child*/)
    {
        /// ON section should not be rewritten.
        return typeid_cast<ASTTableJoin  *>(parent.get()) == nullptr;
    }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * identifier = typeid_cast<ASTIdentifier *>(ast.get()))
            visit(*identifier, ast);
    }

    static void visit(ASTIdentifier & identifier, ASTPtr & ast)
    {
        if (identifier.shortName() != "_shard_num")
            return;

        String alias = identifier.tryGetAlias();
        if (alias.empty())
            alias = "_shard_num";
        ast = makeASTFunction("shardNum");
        ast->setAlias(alias);
    }
};
using RewriteShardNumVisitor = InDepthNodeVisitor<RewriteShardNum, true>;

/// Get hybrid search related functions(distance, batch_distance, TextSearch and HybridSearch), remove duplicated functions
void getHybridSearchFunctions(
    ASTPtr & query,
    const ASTSelectQuery & select_query,
    std::vector<const ASTFunction *> & hybrid_search_functions,
    HybridSearchFuncType & search_func_type)
{
    GetHybridSearchVisitor::Data data;
    GetHybridSearchVisitor(data).visit(query);

    size_t hybrid_search_func_count = data.vector_scan_funcs.size() + data.text_search_func.size() + data.hybrid_search_func.size();
    if (hybrid_search_func_count == 0)
        return ;
    else if (hybrid_search_func_count > 1)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support more than one function among vector scan, text search, or hybrid search in one query now");

    String search_func_name;
    if (data.vector_scan_funcs.size() == 1)
    {
        hybrid_search_functions = data.vector_scan_funcs;
        search_func_type = HybridSearchFuncType::VECTOR_SCAN;
        search_func_name = "distance";
    }
    else if (data.text_search_func.size() == 1)
    {
        hybrid_search_functions = data.text_search_func;
        search_func_type = HybridSearchFuncType::TEXT_SEARCH;
        search_func_name = "TextSearch";
    }
    else if (data.hybrid_search_func.size() == 1)
    {
        hybrid_search_functions = data.hybrid_search_func;
        search_func_type = HybridSearchFuncType::HYBRID_SEARCH;
        search_func_name = "HybridSearch";
    }

    if (!select_query.orderBy())
    {
        /// TODO: Will be removed when distance functions are implemented
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support {} function without ORDER BY clause", search_func_name);
    }

    bool is_batch = hybrid_search_functions.size() == 1 && isBatchDistance(hybrid_search_functions[0]->getColumnName());
    if (!is_batch && !select_query.limitLength())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support {} function without LIMIT N clause", search_func_name);
    else if (is_batch && !select_query.limitByLength())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support batch {} function without LIMIT N BY clause", search_func_name);

    if (select_query.orderBy())
    {
        GetHybridSearchVisitor::Data order_by_data;
        GetHybridSearchVisitor(order_by_data).visit(select_query.orderBy());

        auto search_func_count = order_by_data.vector_scan_funcs.size() + order_by_data.text_search_func.size() + order_by_data.hybrid_search_func.size();
        if (search_func_count != 1)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support without {} function inside ORDER BY clause", search_func_name);
    }
}

void addSearchFunctionColumnName(const String & func_col_name, NamesAndTypesList & source_columns)
{
    if (source_columns.contains(func_col_name)) /* consider second analysis round */
        return;

    /// Spencial handle for batch distance
    if (isBatchDistance(func_col_name))
    {
        auto id_type = std::make_shared<DataTypeUInt32>();
        auto distance_type = std::make_shared<DataTypeFloat32>();
        DataTypes types;
        types.emplace_back(id_type);
        types.emplace_back(distance_type);
        auto type = std::make_shared<DataTypeTuple>(types);
        NameAndTypePair new_name_pair(func_col_name, type);
        source_columns.push_back(new_name_pair);
    }
    else
    {
        NameAndTypePair new_name_pair(func_col_name, std::make_shared<DataTypeFloat32>());
        source_columns.push_back(new_name_pair);
    }
}

UInt64 getTopKFromLimit(const ASTSelectQuery * select_query, ContextPtr context, bool is_batch = false)
{
    UInt64 topk = 0;

    if (!select_query)
        return topk;

    /// topk for search is sum of length and offset in limit
    UInt64 length = 0, offset = 0;
    ASTPtr length_ast = nullptr;
    ASTPtr offset_ast = nullptr;

    if (is_batch)
    {
        /// LIMIT m OFFSET n BY expressions
        length_ast = select_query->limitByLength();
        offset_ast = select_query->limitByOffset();
    }
    else
    {
        /// LIMIT m OFFSET n
        length_ast = select_query->limitLength();
        offset_ast = select_query->limitOffset();
    }

    if (length_ast)
    {
        const auto & [field, type] = evaluateConstantExpression(length_ast, context);

        if (isNativeNumber(type))
        {
            Field converted = convertFieldToType(field, DataTypeUInt64());
            if (!converted.isNull())
                length = converted.safeGet<UInt64>();
        }
    }

    if (offset_ast)
    {
        const auto & [field, type] = evaluateConstantExpression(offset_ast, context);

        if (isNativeNumber(type))
        {
            Field converted = convertFieldToType(field, DataTypeUInt64());
            if (!converted.isNull())
                offset = converted.safeGet<UInt64>();
        }
    }

    topk = length + offset;

    if (topk > context->getSettingsRef().max_search_result_window)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sum of m and n in limit ({}) should not exceed `max_search_result_window`({})", topk, context->getSettingsRef().max_search_result_window);

    return topk;
}

}

TreeRewriterResult::TreeRewriterResult(
    const NamesAndTypesList & source_columns_,
    ConstStoragePtr storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    bool add_special)
    : storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , source_columns(source_columns_)
{
    collectSourceColumns(add_special);
    is_remote_storage = storage && storage->isRemote();
}

/// Add columns from storage to source_columns list. Deduplicate resulted list.
/// Special columns are non physical columns, for example ALIAS
void TreeRewriterResult::collectSourceColumns(bool add_special)
{
    if (storage)
    {
        auto options = GetColumnsOptions(add_special ? GetColumnsOptions::All : GetColumnsOptions::AllPhysical);
        options.withExtendedObjects();
        if (storage->supportsSubcolumns())
            options.withSubcolumns();

        auto columns_from_storage = storage_snapshot->getColumns(options);

        if (source_columns.empty())
            source_columns.swap(columns_from_storage);
        else
            source_columns.insert(source_columns.end(), columns_from_storage.begin(), columns_from_storage.end());

        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        auto metadata_column_descriptions = metadata_snapshot->getColumns();
        source_columns_ordinary = metadata_column_descriptions.getOrdinary();
    }

    source_columns_set = removeDuplicateColumns(source_columns);
}


/// Calculate which columns are required to execute the expression.
/// Then, delete all other columns from the list of available columns.
/// After execution, columns will only contain the list of columns needed to read from the table.
bool TreeRewriterResult::collectUsedColumns(const ASTPtr & query, bool is_select, bool visit_index_hint, bool no_throw)
{
    /// We calculate required_source_columns with source_columns modifications and swap them on exit
    required_source_columns = source_columns;

    RequiredSourceColumnsVisitor::Data columns_context;
    columns_context.visit_index_hint = visit_index_hint;
    RequiredSourceColumnsVisitor(columns_context).visit(query);

    NameSet source_column_names;
    for (const auto & column : source_columns)
        source_column_names.insert(column.name);

    NameSet required = columns_context.requiredColumns();
    if (columns_context.has_table_join)
    {
        NameSet available_columns;
        for (const auto & name : source_columns)
            available_columns.insert(name.name);

        /// Add columns obtained by JOIN (if needed).
        for (const auto & joined_column : analyzed_join->columnsFromJoinedTable())
        {
            const auto & name = joined_column.name;
            if (available_columns.contains(name))
                continue;

            if (required.contains(name))
            {
                /// Optimisation: do not add columns needed only in JOIN ON section.
                if (columns_context.nameInclusion(name) > analyzed_join->rightKeyInclusion(name))
                    analyzed_join->addJoinedColumn(joined_column);

                required.erase(name);
            }
            /// Add vector scan, text search and hybrid search function column name when exists in right joined table
            else if (isHybridSearchFunc(name))
                analyzed_join->addJoinedColumn(joined_column);
        }
    }

    NameSet array_join_sources;
    if (columns_context.has_array_join)
    {
        /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
        for (const auto & result_source : array_join_result_to_source)
            array_join_sources.insert(result_source.second);

        for (const auto & column_name_type : source_columns)
            if (array_join_sources.contains(column_name_type.name))
                required.insert(column_name_type.name);
    }

    /// Figure out if we're able to use the trivial count optimization.
    has_explicit_columns = !required.empty();
    if (is_select && !has_explicit_columns)
    {
        optimize_trivial_count = !columns_context.has_array_join;

        /// You need to read at least one column to find the number of rows.
        /// We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
        /// Because it is the column that is cheapest to read.
        struct ColumnSizeTuple
        {
            size_t compressed_size;
            size_t type_size;
            size_t uncompressed_size;
            String name;

            bool operator<(const ColumnSizeTuple & that) const
            {
                return std::tie(compressed_size, type_size, uncompressed_size)
                    < std::tie(that.compressed_size, that.type_size, that.uncompressed_size);
            }
        };

        std::vector<ColumnSizeTuple> columns;
        if (storage)
        {
            auto column_sizes = storage->getColumnSizes();
            for (auto & source_column : source_columns)
            {
                auto c = column_sizes.find(source_column.name);
                if (c == column_sizes.end())
                    continue;
                size_t type_size = source_column.type->haveMaximumSizeOfValue() ? source_column.type->getMaximumSizeOfValueInMemory() : 100;
                columns.emplace_back(ColumnSizeTuple{c->second.data_compressed, type_size, c->second.data_uncompressed, source_column.name});
            }
        }

        if (!columns.empty())
            required.insert(std::min_element(columns.begin(), columns.end())->name);
        else if (!source_columns.empty())
            /// If we have no information about columns sizes, choose a column of minimum size of its data type.
            required.insert(ExpressionActions::getSmallestColumn(source_columns).name);
    }
    else if (is_select && storage_snapshot && !columns_context.has_array_join)
    {
        const auto & partition_desc = storage_snapshot->metadata->getPartitionKey();
        if (partition_desc.expression)
        {
            auto partition_source_columns = partition_desc.expression->getRequiredColumns();
            partition_source_columns.push_back("_part");
            partition_source_columns.push_back("_partition_id");
            partition_source_columns.push_back("_part_uuid");
            partition_source_columns.push_back("_partition_value");
            optimize_trivial_count = true;
            for (const auto & required_column : required)
            {
                if (std::find(partition_source_columns.begin(), partition_source_columns.end(), required_column)
                    == partition_source_columns.end())
                {
                    optimize_trivial_count = false;
                    break;
                }
            }
        }
    }

    NameSet unknown_required_source_columns = required;

    for (NamesAndTypesList::iterator it = source_columns.begin(); it != source_columns.end();)
    {
        const String & column_name = it->name;
        unknown_required_source_columns.erase(column_name);

        if (!required.contains(column_name))
            it = source_columns.erase(it);
        else
            ++it;
    }

    has_virtual_shard_num = false;
    /// If there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing they are also considered.
    if (storage)
    {
        const auto storage_virtuals = storage->getVirtuals();
        for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
        {
            auto column = storage_virtuals.tryGetByName(*it);
            if (column)
            {
                source_columns.push_back(*column);
                it = unknown_required_source_columns.erase(it);
            }
            else
                ++it;
        }

        if (is_remote_storage)
        {
            for (const auto & name_type : storage_virtuals)
            {
                if (name_type.name == "_shard_num" && storage->isVirtualColumn("_shard_num", storage_snapshot->getMetadataForQuery()))
                {
                    has_virtual_shard_num = true;
                    break;
                }
            }
        }
    }

    /// insert vector scan / TextSearch / HybridSearch func columns into source columns here
    if (!hybrid_search_funcs.empty() && !hybrid_search_from_right_table)
    {
        for (auto node : hybrid_search_funcs)
        {
            const String func_column_name = node->getColumnName();
            addSearchFunctionColumnName(func_column_name, source_columns);
            unknown_required_source_columns.erase(func_column_name);
        }
    }

    /// Collect missed object subcolumns
    if (!unknown_required_source_columns.empty())
    {
        for (const NameAndTypePair & pair : source_columns_ordinary)
        {
            for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
            {
                size_t object_pos = it->find('.');
                if (object_pos != std::string::npos)
                {
                    String object_name = it->substr(0, object_pos);
                    if (pair.name == object_name && pair.type->getTypeId() == TypeIndex::Object)
                    {
                        const auto * object_type = typeid_cast<const DataTypeObject *>(pair.type.get());
                        if (object_type->getSchemaFormat() == "json" && object_type->hasNullableSubcolumns())
                        {
                            missed_subcolumns.insert(*it);
                            it = unknown_required_source_columns.erase(it);
                            continue;
                        }
                    }
                }
                ++it;
            }
        }
    }

    if (!unknown_required_source_columns.empty())
    {
        constexpr auto format_string = "Missing columns: {} while processing query: '{}', required columns:{}{}";
        WriteBufferFromOwnString ss;
        ss << "Missing columns:";
        for (const auto & name : unknown_required_source_columns)
            ss << " '" << name << "'";
        ss << " while processing query: '" << queryToString(query) << "'";

        ss << ", required columns:";
        for (const auto & name : columns_context.requiredColumns())
            ss << " '" << name << "'";

        if (storage)
        {
            std::vector<String> hint_name{};
            std::set<String> helper_hint_name{};
            for (const auto & name : columns_context.requiredColumns())
            {
                auto hints = storage->getHints(name);
                for (const auto & hint : hints)
                {
                    // We want to preserve the ordering of the hints
                    // (as they are ordered by Levenshtein distance)
                    auto [_, inserted] = helper_hint_name.insert(hint);
                    if (inserted)
                        hint_name.push_back(hint);
                }
            }

            if (!hint_name.empty())
            {
                ss << ", maybe you meant: ";
                ss << toStringWithFinalSeparator(hint_name, " or ");
            }
        }
        else
        {
            if (!source_column_names.empty())
                for (const auto & name : columns_context.requiredColumns())
                    ss << " '" << name << "'";
            else
                ss << ", no source columns";
        }

        if (columns_context.has_table_join)
        {
            ss << ", joined columns:";
            for (const auto & column : analyzed_join->columnsFromJoinedTable())
                ss << " '" << column.name << "'";
        }

        if (!array_join_sources.empty())
        {
            ss << ", arrayJoin columns:";
            for (const auto & name : array_join_sources)
                ss << " '" << name << "'";
        }

        if (no_throw)
            return false;
        throw Exception(PreformattedMessage{ss.str(), format_string}, ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    required_source_columns.swap(source_columns);
    for (const auto & column : required_source_columns)
    {
        source_column_names.insert(column.name);
    }
    return true;
}

NameSet TreeRewriterResult::getArrayJoinSourceNameSet() const
{
    NameSet forbidden_columns;
    for (const auto & elem : array_join_result_to_source)
        forbidden_columns.insert(elem.first);
    return forbidden_columns;
}

inline String getMetircType(StorageMetadataPtr & metadata_snapshot, Search::DataType & vector_search_type, String & vec_col_name, ContextPtr context)
{
    /// The default value is float_vector_search_metric_type or binary_vector_search_metric_type in MergeTree, but we cannot get it here.
    String metric_type;
    for (const auto & vector_index_desc : metadata_snapshot->getVectorIndices())
    {
        if (vector_index_desc.column == vec_col_name)
        {
            const auto index_parameter = VectorIndex::convertPocoJsonToMap(vector_index_desc.parameters);
            if (index_parameter.contains("metric_type"))
            {
                /// Get metric_type in index definition
                metric_type = index_parameter.at("metric_type");
                break;
            }
        }
    }
    if (metric_type.empty() && metadata_snapshot->hasSettingsChanges())
    {
        const auto settings_changes = metadata_snapshot->getSettingsChanges()->as<const ASTSetQuery &>().changes;
        Field change_metric;
        /// TODO: Try not to use string literals directly
        if ((vector_search_type == Search::DataType::FloatVector && settings_changes.tryGet("float_vector_search_metric_type", change_metric)) ||
            (vector_search_type == Search::DataType::BinaryVector && settings_changes.tryGet("binary_vector_search_metric_type", change_metric)))
        {
            metric_type = change_metric.safeGet<String>();
        }
    }
    if (metric_type.empty())
    {
        const auto settings = context->getMergeTreeSettings();
        if (vector_search_type == Search::DataType::FloatVector)
        {
            metric_type = settings.float_vector_search_metric_type.toString();
        }
        else if (vector_search_type == Search::DataType::BinaryVector)
        {
            metric_type = settings.binary_vector_search_metric_type.toString();
        }
    }

    return metric_type;
}

void TreeRewriterResult::collectForVectorScanFunctions(
    ASTSelectQuery * select_query,
    const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
    ContextPtr context)
{
    /// distance function exists in main query's select caluse
    if (search_func_type == HybridSearchFuncType::VECTOR_SCAN && hybrid_search_funcs.size() == 1)
    {
        /// Get topK from limit N
        bool is_batch = isBatchDistance(hybrid_search_funcs[0]->getColumnName());

        limit_length = getTopKFromLimit(select_query, context, is_batch);

        if (limit_length == 0)
        {
            if (is_batch)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support batch_distance function without LIMIT BY clause");
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support distance function without LIMIT clause");
        }

        /// There is no vector scan function, hence the checks like input paramters are put here.
        /// Check if vector column in vector scan func exists in left table or right joined table
        /// Insert distance func columns into source columns here
        const ASTFunction * node = hybrid_search_funcs[0];
        const ASTs & arguments = node->arguments ? node->arguments->children : ASTs();

        if (arguments.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong argument number in distance function");

        String vec_col_name = arguments[0]->getColumnName();
        String distance_col_name = node->getColumnName();
        StorageMetadataPtr metadata_snapshot = nullptr;
        bool table_is_remote = false; /// Mark if the storage with vector column is distributed.

        std::optional<NameAndTypePair> search_column_type = std::nullopt;

        if (storage_snapshot && storage_snapshot->metadata->getColumns().has(vec_col_name))
        {
            /// distance func column name should add to left table's source_columns
            /// Will be added inside collectUsedColumns() after erase unrequired columns.
            metadata_snapshot = storage_snapshot->metadata;
            table_is_remote = is_remote_storage;

            search_column_type = metadata_snapshot->columns.getAllPhysical().tryGetByName(vec_col_name);
        }
        else if (tables_with_columns.size() > 1)
        {
            /// Check if vector column name exists in right joined table
            const auto & right_table = tables_with_columns[1];
            String table_name = right_table.table.getQualifiedNamePrefix(false);

            /// Handle cases where left table and right table both have the same vector column.
            if (auto * identifier = arguments[0]->as<ASTIdentifier>())
                vec_col_name = identifier->shortName();

            if (!right_table.hasColumn(vec_col_name))
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}' in table '{}'", vec_col_name, table_name);
            }

            search_column_type = right_table.columns.tryGetByName(vec_col_name);
            hybrid_search_from_right_table = true;

            /// distance func column name should add to right joined table's source columns
            addSearchFunctionColumnName(distance_col_name, analyzed_join->columns_from_joined_table);

            /// Add distance func column to original_names too
            auto & original_names = analyzed_join->original_names;
            original_names[distance_col_name] = distance_col_name;

            /// Get metadata for right table
            auto table_id = context->resolveStorageID(StorageID(right_table.table.database, right_table.table.table, right_table.table.uuid));
            const auto & right_table_storage = DatabaseCatalog::instance().getTable(table_id, context);
            metadata_snapshot = right_table_storage->getInMemoryMetadataPtr();
            table_is_remote = right_table_storage->isRemote();
        }
        else if (tables_with_columns.size() == 1)
        {
            /// Left table is subquery
            const auto & left_table = tables_with_columns[0];
            String table_name = left_table.table.getQualifiedNamePrefix(false);

            if (!left_table.hasColumn(vec_col_name))
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}' in table '{}'", vec_col_name, table_name);
            }

            /// Unable get metadata for left table, because the table name and UUID are empty.
            search_column_type = left_table.columns.tryGetByName(vec_col_name);
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}'", vec_col_name);
        }

        /// Check vector column data type
        if (!search_column_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "search column name: {}, type is not exist", vec_col_name);
        vector_search_type = getSearchIndexDataType(search_column_type->type);

        /// When metric_type = IP in definition of vector index, order by must be DESC.
        /// Skip the check when table is distributed.
        if (metadata_snapshot && !table_is_remote)
        {
            /// 1 for ASC, -1 for DESC
            direction = 1;

            auto order_by = select_query->orderBy();
            if (!order_by)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support distance function without ORDER BY clause");

            /// Find the direction for distance func
            for (const auto & child : order_by->children)
            {
                auto * order_by_element = child->as<ASTOrderByElement>();
                if (!order_by_element || order_by_element->children.empty())
                    continue;
                ASTPtr order_expression = order_by_element->children.at(0);

                if (!is_batch && isDistance(order_expression->getColumnName()))
                {
                    direction = order_by_element->direction;
                    break;
                }
                else if (is_batch)
                {
                    /// order by batch_distance column name's 1 and 2, where 2 is distance column.
                    if (auto * function = order_expression->as<ASTFunction>(); function->name == "tupleElement")
                    {
                        const ASTs & func_arguments = function->arguments->as<ASTExpressionList &>().children;
                        if (func_arguments.size() >= 2 && isBatchDistance(func_arguments[0]->getColumnName()))
                        {
                            if (func_arguments[1]->getColumnName() == "2")
                            {
                                direction = order_by_element->direction;
                                break;
                            }
                        }
                    }
                }
            }

            vector_scan_metric_type = getMetircType(metadata_snapshot, vector_search_type, vec_col_name, context);

            String metric_type = vector_scan_metric_type;
            Poco::toUpperInPlace(metric_type);
            if (metric_type == "IP")
            {
                if (direction == 1)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Use 'ORDER BY distance DESC' when the metric type is IP");
            }
            else if (direction == -1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Use 'ORDER BY distance ASC' when the metric type is {}", metric_type);
        }
    }
    /// Interpreter select on the right joined table where vector column exists, insert distance func column name.
    else if (auto vector_scan_desc = context->getVecScanDescription())
    {
        /// Add vector scan func name and type to source columns
        addSearchFunctionColumnName(vector_scan_desc->column_name, source_columns);

        /// Add vector scan func name to select clauses if not exists
        const auto select_expression_list = select_query->select();
        bool found = false;
        for (const auto & elem : select_expression_list->children)
        {
            String name = elem->getAliasOrColumnName();
            if (name == vector_scan_desc->column_name)
            {
                found = true;
                break;
            }
        }

        if (!found)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(vector_scan_desc->column_name));
    }
}

void TreeRewriterResult::collectForTextSearchFunctions(
    ASTSelectQuery * select_query,
    const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
    ContextPtr context)
{
    /// text search function exists in main query's select clause
    if (search_func_type == HybridSearchFuncType::TEXT_SEARCH && hybrid_search_funcs.size() == 1)
    {
        /// Get topK from limit N
        limit_length = getTopKFromLimit(select_query, context);

        if (limit_length == 0)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support TextSearch function without LIMIT clause");

        /// There is no text search function, hence the checks like input paramters are put here.
        /// Check if text column in text search func exists in left table or right joined table
        /// Insert text search func columns into source columns here
        const ASTFunction * node = hybrid_search_funcs[0];
        const ASTs & arguments = node->arguments ? node->arguments->children : ASTs();

        if (arguments.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong argument number in TextSearch function");

        String text_col_name = arguments[0]->getColumnName();
        String function_col_name = node->getColumnName();
        StorageMetadataPtr metadata_snapshot = nullptr;
        std::optional<NameAndTypePair> search_column_type = std::nullopt;
        bool is_mapkeys = false;

        if (const ASTFunction * function = arguments[0]->as<ASTFunction>())
        {
            if (function->name == "mapKeys")
            {
                if (function->arguments)
                {
                    const auto & function_arguments_list = function->arguments->as<ASTExpressionList>()->children;
                    if (function_arguments_list.size() == 1)
                    {
                        text_col_name = function_arguments_list[0]->getColumnName();
                        is_mapkeys = true;
                    }
                }
            }
        }

        /// Mark if the storage with text column is distributed.
        bool table_is_remote = false;

        if (storage_snapshot && storage_snapshot->metadata->getColumns().has(text_col_name))
        {
            metadata_snapshot = storage_snapshot->metadata;
            table_is_remote = is_remote_storage;
            search_column_type = metadata_snapshot->columns.getAllPhysical().tryGetByName(text_col_name);
        }
        else if (tables_with_columns.size() > 1)
        {
            /// Check if text column name exists in right joined table
            const auto & right_table = tables_with_columns[1];
            String table_name = right_table.table.getQualifiedNamePrefix(false);

            /// Handle cases where left table and right table both have the same text column.
            if (auto * identifier = arguments[0]->as<ASTIdentifier>())
                text_col_name = identifier->shortName();

            if (!right_table.hasColumn(text_col_name))
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}' in table '{}'", text_col_name, table_name);
            }

            search_column_type = right_table.columns.tryGetByName(text_col_name);
            hybrid_search_from_right_table = true;

            /// text search func column name should add to right joined table's source columns
            addSearchFunctionColumnName(function_col_name, analyzed_join->columns_from_joined_table);

            /// Add text search func column to original_names too
            auto & original_names = analyzed_join->original_names;
            original_names[function_col_name] = function_col_name;

            /// Get metadata for right table
            auto table_id = context->resolveStorageID(StorageID(right_table.table.database, right_table.table.table, right_table.table.uuid));
            const auto & right_table_storage = DatabaseCatalog::instance().getTable(table_id, context);
            metadata_snapshot = right_table_storage->getInMemoryMetadataPtr();
            table_is_remote = right_table_storage->isRemote();
        }
        else if (tables_with_columns.size() == 1)
        {
            /// Left table is subquery
            const auto & left_table = tables_with_columns[0];
            String table_name = left_table.table.getQualifiedNamePrefix(false);

            if (!left_table.hasColumn(text_col_name))
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}' in table '{}'", text_col_name, table_name);
            }

            /// Unable get metadata for left table, because the table name and UUID are empty.
            search_column_type = left_table.columns.tryGetByName(text_col_name);
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}'", text_col_name);
        }

        /// Check text column data type
        if (!search_column_type)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "search column name: {}, type not exist", text_col_name);
        }
        checkTextSearchColumnDataType(search_column_type->type, is_mapkeys);

        /// Skip the check when table is distributed.
        if (metadata_snapshot && !table_is_remote)
        {
            auto order_by = select_query->orderBy();
            if (!order_by)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support TextSearch function without ORDER BY clause");

            /// 1 for ASC, -1 for DESC
            direction = 1;
            /// Find the direction for TextSearch func
            for (const auto & child : order_by->children)
            {
                auto * order_by_element = child->as<ASTOrderByElement>();
                if (!order_by_element || order_by_element->children.empty())
                    continue;
                ASTPtr order_expression = order_by_element->children.at(0);

                if (isTextSearch(order_expression->getColumnName()))
                {
                    direction = order_by_element->direction;
                    break;
                }
            }
            if (direction == 1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "The results returned by the TextSearch function should be ordered by `DESC`");
        }
    }
    /// Interpreter select on the right joined table where text column exists, insert text search func column name.
    else if (auto text_search_info = context->getTextSearchInfo())
    {
        /// Add text search func name and type to source columns
        addSearchFunctionColumnName(text_search_info->function_column_name, source_columns);

        /// Add text search func name to select clauses if not exists
        const auto select_expression_list = select_query->select();
        bool found = false;
        for (const auto & elem : select_expression_list->children)
        {
            String name = elem->getAliasOrColumnName();
            if (name == text_search_info->function_column_name)
            {
                found = true;
               break;
           }
        }

        if (!found)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(text_search_info->function_column_name));
    }
}

void TreeRewriterResult::collectForHybridSearchFunctions(
    ASTSelectQuery * select_query,
    const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
    ContextPtr context)
{
    /// hybrid search related function exists in main query's select caluse
    if (search_func_type == HybridSearchFuncType::HYBRID_SEARCH && hybrid_search_funcs.size() == 1)
    {
        /// Get topK from limit N
        limit_length = getTopKFromLimit(select_query, context);

        if (limit_length == 0)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support HybridSearch function without LIMIT clause");

        /// There is no hybrid search function, hence the checks like input paramters are put here.
        /// Check if vector and text column in hybrid search func exists in left table or right joined table
        /// Insert hybrid search func columns into source columns here
        const ASTFunction * node = hybrid_search_funcs[0];
        const ASTs & arguments = node->arguments ? node->arguments->children : ASTs();

        if (arguments.size() != 4)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong argument number in HybridSearch function");

        String function_col_name = node->getColumnName();
        StorageMetadataPtr metadata_snapshot = nullptr;

        String vector_col_name = arguments[0]->getColumnName();
        String text_col_name = arguments[1]->getColumnName();
        std::optional<NameAndTypePair> search_vector_column_type = std::nullopt;
        std::optional<NameAndTypePair> search_text_column_type = std::nullopt;
        bool is_mapkeys = false;

        if (const ASTFunction * function = arguments[1]->as<ASTFunction>())
        {
            if (function->name == "mapKeys")
            {
                if(function->arguments)
                {
                    const auto & function_arguments_list = function->arguments->as<ASTExpressionList>()->children;
                    if (function_arguments_list.size() == 1)
                    {
                        text_col_name = function_arguments_list[0]->getColumnName();
                        is_mapkeys = true;
                    }
                }
            }
        }

        /// Mark if the storage with text column is distributed.
        bool table_is_remote = false;

        if (storage_snapshot && storage_snapshot->metadata->getColumns().has(text_col_name))
        {
            metadata_snapshot = storage_snapshot->metadata;
            table_is_remote = is_remote_storage;

            search_vector_column_type = metadata_snapshot->columns.getAllPhysical().tryGetByName(vector_col_name);
            search_text_column_type = metadata_snapshot->columns.getAllPhysical().tryGetByName(text_col_name);
        }
        else if (tables_with_columns.size() > 1)
        {
            /// Check if text column name exists in right joined table
            const auto & right_table = tables_with_columns[1];
            String table_name = right_table.table.getQualifiedNamePrefix(false);

            /// Handle cases where left table and right table both have the same vector column.
            if (auto * identifier = arguments[0]->as<ASTIdentifier>())
                vector_col_name = identifier->shortName();
            if (!right_table.hasColumn(vector_col_name))
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}' in table '{}'", vector_col_name, table_name);
            search_vector_column_type = right_table.columns.tryGetByName(vector_col_name);

            /// Handle cases where left table and right table both have the same text column.
            if (auto * identifier = arguments[1]->as<ASTIdentifier>())
                text_col_name = identifier->shortName();
            if (!right_table.hasColumn(text_col_name))
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}' in table '{}'", text_col_name, table_name);
            search_text_column_type = right_table.columns.tryGetByName(text_col_name);

            hybrid_search_from_right_table = true;

            /// text search func column name should add to right joined table's source columns
            addSearchFunctionColumnName(function_col_name, analyzed_join->columns_from_joined_table);

            /// Add text search func column to original_names too
            auto & original_names = analyzed_join->original_names;
            original_names[function_col_name] = function_col_name;

            /// Get metadata for right table
            auto table_id = context->resolveStorageID(StorageID(right_table.table.database, right_table.table.table, right_table.table.uuid));
            const auto & right_table_storage = DatabaseCatalog::instance().getTable(table_id, context);
            metadata_snapshot = right_table_storage->getInMemoryMetadataPtr();
            table_is_remote = right_table_storage->isRemote();
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "There is no column '{}'", text_col_name);
        }

        /// Check vector column data type
        if (!search_vector_column_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "search vector column name: {}, type is not exist", vector_col_name);
        vector_search_type = getSearchIndexDataType(search_vector_column_type->type);

        /// Check text column data type
        if (!search_text_column_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "search text column name: {}, type is not exist", text_col_name);
        checkTextSearchColumnDataType(search_text_column_type->type, is_mapkeys);

        /// Skip the check when table is distributed.
        if (metadata_snapshot && !table_is_remote)
        {
            auto order_by = select_query->orderBy();
            if (!order_by)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Not support HybridSearch function without ORDER BY clause");

            /// 1 for ASC, -1 for DESC
            direction = 1;
            /// Find the direction for hybrid search func
            for (const auto & child : order_by->children)
            {
                auto * order_by_element = child->as<ASTOrderByElement>();
                if (!order_by_element || order_by_element->children.empty())
                    continue;
                ASTPtr order_expression = order_by_element->children.at(0);

                if (isHybridSearch(order_expression->getColumnName()))
                {
                    direction = order_by_element->direction;
                    break;
                }
            }
            if (direction == 1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "The results returned by the HybridSearch function should be ordered by `DESC`");

            vector_scan_metric_type = getMetircType(metadata_snapshot, vector_search_type, vector_col_name, context);
        }
    }
    /// Interpreter select on the right joined table where hybrid column exists, insert hybrid search func column name.
    else if (auto hybrid_search_info = context->getHybridSearchInfo())
    {
        /// Add hybrid search func name and type to source columns
        addSearchFunctionColumnName(hybrid_search_info->function_column_name, source_columns);

        /// Add hybrid search func name to select clauses if not exists
        const auto select_expression_list = select_query->select();
        bool found = false;
        for (const auto & elem : select_expression_list->children)
        {
            String name = elem->getAliasOrColumnName();
            if (name == hybrid_search_info->function_column_name)
            {
                found = true;
                break;
            }
        }

        if (!found)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(hybrid_search_info->function_column_name));
    }
}

TreeRewriterResultPtr TreeRewriter::analyzeSelect(
    ASTPtr & query,
    TreeRewriterResult && result,
    const SelectQueryOptions & select_options,
    const TablesWithColumns & tables_with_columns,
    const Names & required_result_columns,
    std::shared_ptr<TableJoin> table_join,
    bool is_parameterized_view,
    const NameToNameMap parameter_values,
    const NameToNameMap parameter_types) const
{
    DB::OpenTelemetry::SpanHolder span("TreeRewriter::analyzeSelect");
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Select analyze for not select asts.");

    size_t subquery_depth = select_options.subquery_depth;
    bool remove_duplicates = select_options.remove_duplicates;

    const auto & settings = getContext()->getSettingsRef();

    const NameSet & source_columns_set = result.source_columns_set;

    if (table_join)
    {
        result.analyzed_join = table_join;
        result.analyzed_join->resetCollected();
    }
    else /// TODO: remove. For now ExpressionAnalyzer expects some not empty object here
        result.analyzed_join = std::make_shared<TableJoin>();

    if (remove_duplicates)
        renameDuplicatedColumns(select_query);

    /// Perform it before analyzing JOINs, because it may change number of columns with names unique and break some logic inside JOINs
    if (settings.optimize_normalize_count_variants)
        TreeOptimizer::optimizeCountConstantAndSumOne(query, getContext());

    if (tables_with_columns.size() > 1)
    {
        const auto & right_table = tables_with_columns[1];
        auto columns_from_joined_table = right_table.columns;
        /// query can use materialized or aliased columns from right joined table,
        /// we want to request it for right table
        columns_from_joined_table.insert(columns_from_joined_table.end(), right_table.hidden_columns.begin(), right_table.hidden_columns.end());
        result.analyzed_join->setColumnsFromJoinedTable(std::move(columns_from_joined_table), source_columns_set, right_table.table.getQualifiedNamePrefix());
    }

    translateQualifiedNames(query, *select_query, source_columns_set, tables_with_columns, parameter_values, parameter_types);

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, tables_with_columns, settings.optimize_min_equality_disjunction_chain_length.value).perform();

    NameSet all_source_columns_set = source_columns_set;
    if (table_join)
    {
        for (const auto & [name, _] : table_join->columnsFromJoinedTable())
            all_source_columns_set.insert(name);
    }

    normalize(query, result.aliases, all_source_columns_set, select_options.ignore_alias, settings, /* allow_self_aliases = */ true, getContext(), select_options.is_create_parameterized_view);

    // expand GROUP BY ALL
    if (select_query->group_by_all)
        expandGroupByAll(select_query);

    /// Remove unneeded columns according to 'required_result_columns'.
    /// Leave all selected columns in case of DISTINCT; columns that contain arrayJoin function inside.
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    removeUnneededColumnsFromSelectClause(select_query, required_result_columns, remove_duplicates);

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries(query, getContext(), subquery_depth, result.scalars, result.local_scalars, select_options.only_analyze, select_options.is_create_parameterized_view);

    if (settings.legacy_column_name_of_tuple_literal)
        markTupleLiteralsAsLegacy(query);

    /// Push the predicate expression down to subqueries. The optimization should be applied to both initial and secondary queries.
    result.rewrite_subqueries = PredicateExpressionsOptimizer(getContext(), tables_with_columns, settings).optimize(*select_query);

    TreeOptimizer::optimizeIf(query, result.aliases, settings.optimize_if_chain_to_multiif);

    /// Only apply AST optimization for initial queries.
    const bool ast_optimizations_allowed
        = getContext()->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY && !select_options.ignore_ast_optimizations;
    if (ast_optimizations_allowed)
        TreeOptimizer::apply(query, result, tables_with_columns, getContext());

    /// array_join_alias_to_name, array_join_result_to_source.
    getArrayJoinedColumns(query, result, select_query, result.source_columns, source_columns_set);

    setJoinStrictness(
        *select_query, settings.join_default_strictness, settings.any_join_distinct_right_table_keys, result.analyzed_join);

    auto * table_join_ast = select_query->join() ? select_query->join()->table_join->as<ASTTableJoin>() : nullptr;
    if (table_join_ast && tables_with_columns.size() >= 2)
        collectJoinedColumns(*result.analyzed_join, *table_join_ast, tables_with_columns, result.aliases, getContext());

    result.aggregates = getAggregates(query, *select_query);
    result.window_function_asts = getWindowFunctions(query, *select_query);
    result.expressions_with_window_function = getExpressionsWithWindowFunctions(query);

    /// replaceQueryParameterWithValue is used for parameterized view (which are created using query parameters
    /// and SELECT is used with substitution of these query parameters )
    /// the replaced column names will be used in the next steps
    if (is_parameterized_view)
    {
        for (auto & column : result.source_columns)
            column.name = StorageView::replaceQueryParameterWithValue(column.name, parameter_values, parameter_types);
    }

    getHybridSearchFunctions(query, *select_query, result.hybrid_search_funcs, result.search_func_type);

    /// TODO: will combine three functions into one collectForHybridSearchRelatedFunctions function
    /// Special handling for vector scan, text search and hybrid search function
    result.collectForVectorScanFunctions(select_query, tables_with_columns, getContext());
    result.collectForTextSearchFunctions(select_query, tables_with_columns, getContext());
    result.collectForHybridSearchFunctions(select_query, tables_with_columns, getContext());

    result.collectUsedColumns(query, true, settings.query_plan_optimize_primary_key);

    if (!result.missed_subcolumns.empty())
    {
        for (const String & column_name : result.missed_subcolumns)
            replaceMissedSubcolumnsInQuery(query, column_name);
        result.missed_subcolumns.clear();
    }

    result.required_source_columns_before_expanding_alias_columns = result.required_source_columns.getNames();

    /// rewrite filters for select query, must go after getArrayJoinedColumns
    bool is_initiator = getContext()->getClientInfo().distributed_depth == 0;
    if (settings.optimize_respect_aliases && result.storage_snapshot && is_initiator)
    {
        std::unordered_set<IAST *> excluded_nodes;
        {
            /// Do not replace ALIASed columns in JOIN ON/USING sections
            if (table_join_ast && table_join_ast->on_expression)
                excluded_nodes.insert(table_join_ast->on_expression.get());
            if (table_join_ast && table_join_ast->using_expression_list)
                excluded_nodes.insert(table_join_ast->using_expression_list.get());
        }

        bool is_changed = replaceAliasColumnsInQuery(query, result.storage_snapshot->metadata->getColumns(),
                                                     result.array_join_result_to_source, getContext(), excluded_nodes);
        /// If query is changed, we need to redo some work to correct name resolution.
        if (is_changed)
        {
            /// We should re-apply the optimization, because an expression substituted from alias column might be a function of a group key.
            if (ast_optimizations_allowed && settings.optimize_group_by_function_keys)
                TreeOptimizer::optimizeGroupByFunctionKeys(select_query);

            result.aggregates = getAggregates(query, *select_query);
            result.window_function_asts = getWindowFunctions(query, *select_query);
            result.expressions_with_window_function = getExpressionsWithWindowFunctions(query);
            result.collectUsedColumns(query, true, settings.query_plan_optimize_primary_key);
        }
    }

    /// Rewrite _shard_num to shardNum()
    if (result.has_virtual_shard_num)
    {
        RewriteShardNumVisitor::Data data_rewrite_shard_num;
        RewriteShardNumVisitor(data_rewrite_shard_num).visit(query);
    }

    result.ast_join = select_query->join();

    if (result.optimize_trivial_count)
        result.optimize_trivial_count = settings.optimize_trivial_count_query &&
            !select_query->groupBy() && !select_query->having() &&
            !select_query->sampleSize() && !select_query->sampleOffset() && !select_query->final() &&
            (tables_with_columns.size() < 2 || isLeft(result.analyzed_join->kind()));

    // remove outer braces in order by
    RewriteOrderByVisitor::Data data;
    RewriteOrderByVisitor(data).visit(query);

    return std::make_shared<const TreeRewriterResult>(result);
}

TreeRewriterResultPtr TreeRewriter::analyze(
    ASTPtr & query,
    const NamesAndTypesList & source_columns,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    bool allow_aggregations,
    bool allow_self_aliases,
    bool execute_scalar_subqueries,
    bool is_create_parameterized_view) const
{
    if (query->as<ASTSelectQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not select analyze for select asts.");

    const auto & settings = getContext()->getSettingsRef();

    TreeRewriterResult result(source_columns, storage, storage_snapshot, false);

    normalize(query, result.aliases, result.source_columns_set, false, settings, allow_self_aliases, getContext(), is_create_parameterized_view);

    /// Executing scalar subqueries. Column defaults could be a scalar subquery.
    executeScalarSubqueries(query, getContext(), 0, result.scalars, result.local_scalars, !execute_scalar_subqueries, is_create_parameterized_view);

    if (settings.legacy_column_name_of_tuple_literal)
        markTupleLiteralsAsLegacy(query);

    TreeOptimizer::optimizeIf(query, result.aliases, settings.optimize_if_chain_to_multiif);

    if (allow_aggregations)
    {
        GetAggregatesVisitor::Data data;
        GetAggregatesVisitor(data).visit(query);

        /// There can not be other aggregate functions within the aggregate functions.
        for (const ASTFunction * node : data.aggregates)
            for (auto & arg : node->arguments->children)
                assertNoAggregates(arg, "inside another aggregate function");
        result.aggregates = data.aggregates;
    }
    else
        assertNoAggregates(query, "in wrong place");

    bool is_ok = result.collectUsedColumns(query, false, settings.query_plan_optimize_primary_key, no_throw);
    if (!is_ok)
        return {};

    if (!result.missed_subcolumns.empty())
    {
        for (const String & column_name : result.missed_subcolumns)
            replaceMissedSubcolumnsInQuery(query, column_name);
        result.missed_subcolumns.clear();
    }

    return std::make_shared<const TreeRewriterResult>(result);
}

void TreeRewriter::normalize(
    ASTPtr & query, Aliases & aliases, const NameSet & source_columns_set, bool ignore_alias, const Settings & settings, bool allow_self_aliases, ContextPtr context_, bool is_create_parameterized_view)
{
    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query);

    CustomizeCountDistinctVisitor::Data data_count_distinct{settings.count_distinct_implementation};
    CustomizeCountDistinctVisitor(data_count_distinct).visit(query);

    CustomizeCountIfDistinctVisitor::Data data_count_if_distinct{settings.count_distinct_implementation.toString() + "If"};
    CustomizeCountIfDistinctVisitor(data_count_if_distinct).visit(query);

    CustomizeIfDistinctVisitor::Data data_distinct_if{"DistinctIf"};
    CustomizeIfDistinctVisitor(data_distinct_if).visit(query);

    ExistsExpressionVisitor::Data exists;
    ExistsExpressionVisitor(exists).visit(query);

    if (context_->getSettingsRef().enable_positional_arguments)
    {
        ReplacePositionalArgumentsVisitor::Data data_replace_positional_arguments;
        ReplacePositionalArgumentsVisitor(data_replace_positional_arguments).visit(query);
    }

    if (settings.transform_null_in)
    {
        CustomizeInVisitor::Data data_null_in{"nullIn"};
        CustomizeInVisitor(data_null_in).visit(query);

        CustomizeNotInVisitor::Data data_not_null_in{"notNullIn"};
        CustomizeNotInVisitor(data_not_null_in).visit(query);

        CustomizeGlobalInVisitor::Data data_global_null_in{"globalNullIn"};
        CustomizeGlobalInVisitor(data_global_null_in).visit(query);

        CustomizeGlobalNotInVisitor::Data data_global_not_null_in{"globalNotNullIn"};
        CustomizeGlobalNotInVisitor(data_global_not_null_in).visit(query);
    }

    /// Rewrite all aggregate functions to add -OrNull suffix to them
    if (settings.aggregate_functions_null_for_empty)
    {
        CustomizeAggregateFunctionsOrNullVisitor::Data data_or_null{"OrNull"};
        CustomizeAggregateFunctionsOrNullVisitor(data_or_null).visit(query);
    }

    /// Move -OrNull suffix ahead, this should execute after add -OrNull suffix
    CustomizeAggregateFunctionsMoveOrNullVisitor::Data data_or_null{"OrNull"};
    CustomizeAggregateFunctionsMoveOrNullVisitor(data_or_null).visit(query);

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    QueryAliasesVisitor(aliases).visit(query);

    /// Mark table ASTIdentifiers with not a column marker
    MarkTableIdentifiersVisitor::Data identifiers_data{aliases};
    MarkTableIdentifiersVisitor(identifiers_data).visit(query);

    /// Rewrite function names to their canonical ones.
    /// Notice: function name normalization is disabled when it's a secondary query, because queries are either
    /// already normalized on initiator node, or not normalized and should remain unnormalized for
    /// compatibility.
    if (context_->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY && settings.normalize_function_names)
        FunctionNameNormalizer().visit(query.get());

    if (settings.optimize_move_to_prewhere)
    {
        /// Required for PREWHERE
        ComparisonTupleEliminationVisitor::Data data_comparison_tuple_elimination;
        ComparisonTupleEliminationVisitor(data_comparison_tuple_elimination).visit(query);
    }

    /// Common subexpression elimination. Rewrite rules.
    QueryNormalizer::Data normalizer_data(aliases, source_columns_set, ignore_alias, settings, allow_self_aliases, is_create_parameterized_view);
    QueryNormalizer(normalizer_data).visit(query);

    optimizeGroupingSets(query);
}

}
