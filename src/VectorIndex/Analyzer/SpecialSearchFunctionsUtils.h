#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Planner/PlannerContext.h>
#include <VectorIndex/Storages/VSDescription.h>

namespace DB
{

/// The parameters that specify vector scan in HybridSearch() all have the same prefix.
static inline constexpr auto vector_scan_parameter_prefix = "dense_";

/** Collect hybrid search function nodes in node children.
  * Do not visit subqueries.
  */
QueryTreeNodes collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node);

/** Collect hybrid search function nodes in node children and add them into result.
  * Do not visit subqueries.
  */
void collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result);

/** Returns true if there are hybrid search function nodes in node children, false otherwise.
  * Do not visit subqueries.
  */
bool hasHybridSearchFunctionNodes(const QueryTreeNodePtr & node);

/** Assert that there are no hybrid search function nodes in node children.
  * Do not visit subqueries.
  */
void assertNoHybridSearchFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_hybrids_place_message);

/// Analysis result for special searches: vector scan, text search and hybrid search
struct SpecialSearchAnalysisResult
{
    VSDescriptions vector_scan_descriptions = {};
    TextSearchInfoPtr text_search_info = nullptr;
    HybridSearchInfoPtr hybrid_search_info = nullptr;
    bool has_vector_scan = false;
    bool has_text_search = false;
    bool has_hybrid_search = false;

    QueryTreeNodeWeakPtr source_weak_pointer;
};

std::optional<SpecialSearchAnalysisResult> analyzeSpecialSearch(
    const QueryTreeNodePtr & query_tree,
    const ContextPtr & context);

}
