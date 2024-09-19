#pragma once

#include <Common/logger_useful.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/formatAST.h>

#include <VectorIndex/Utils/CommonUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_VECTOR_SCAN;
    extern const int ILLEGAL_TEXT_SEARCH;
    extern const int ILLEGAL_HYBRID_SEARCH;
}

class GetHybridSearchMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<GetHybridSearchMatcher, true>;

    /// may have multiple vector scan functions
    struct Data
    {
        const char * assert_no_vector_scan = nullptr;
        const char * assert_no_text_search = nullptr;
        const char * assert_no_hybrid_search = nullptr;
        std::unordered_set<String> uniq_names {};

        std::vector<const ASTFunction *> vector_scan_funcs {};
        std::vector<const ASTFunction *> text_search_func {};
        std::vector<const ASTFunction *> hybrid_search_func {};

        /// Save all vector scan functions including duplicated
        /// Need to set flag in ASTFunction for multiple distances cases
        std::vector<const ASTFunction *> all_multiple_vector_scan_funcs {};
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;
        if (auto * select = node->as<ASTSelectQuery>())
        {
            // We don't analysis WITH statement because it might contain useless aggregates
            if (child == select->with())
                return false;
        }
        if (auto * func = node->as<ASTFunction>())
        {
            if (isHybridSearchFunc(func->name))
            {
                return false;
            }

            // Window functions can contain aggregation results as arguments
            // to the window functions, or columns of PARTITION BY or ORDER BY
            // of the window.
        }
        return true;
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>())
            visit(*func, ast, data);
    }
private:
    static void visit(ASTFunction & node, const ASTPtr &, Data & data)
    {
        if (isVectorScanFunc(node.name))
        {
            auto full_name = getFullName(node);

            if (data.assert_no_vector_scan)
                throw Exception(ErrorCodes::ILLEGAL_VECTOR_SCAN, "Vector Scan function {} is found {} in query", full_name, String(data.assert_no_vector_scan));

            /// Save all existing distance funcs
            if (isDistance(node.name))
                data.all_multiple_vector_scan_funcs.push_back(&node);

            /// Save duplicated distance functions once
            if (data.uniq_names.count(full_name))
                return;

            data.vector_scan_funcs.push_back(&node);
            data.uniq_names.insert(full_name);
        }
        else if (isTextSearch(node.name))
        {
            auto full_name = getFullName(node);
            if (data.uniq_names.count(full_name))
                return;

            if (data.assert_no_text_search)
                throw Exception(ErrorCodes::ILLEGAL_TEXT_SEARCH, "Text Search function {} is found {} in query", full_name, String(data.assert_no_text_search));
            data.text_search_func.push_back(&node);
            data.uniq_names.insert(full_name);
        }
        else if (isHybridSearch(node.name))
        {
            auto full_name = getFullName(node);
            if (data.uniq_names.count(full_name))
                return;

            if (data.assert_no_hybrid_search)
                throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "Hybrid Search function {} is found {} in query", full_name, String(data.assert_no_hybrid_search));
            data.hybrid_search_func.push_back(&node);
            data.uniq_names.insert(full_name);
        }
    }
    static String getFullName(ASTFunction & node)
    {
        WriteBufferFromOwnString buf;
        formatAST(node, buf, false, true);
        return buf.str();
    }
};

using GetHybridSearchVisitor = GetHybridSearchMatcher::Visitor;

inline void assertNoVectorScan(const ASTPtr & ast, const char * description)
{
    GetHybridSearchVisitor::Data data{.assert_no_vector_scan = description};
    GetHybridSearchVisitor(data).visit(ast);
}

inline void assertNoTextSearch(const ASTPtr & ast, const char * description)
{
    GetHybridSearchVisitor::Data data{.assert_no_text_search = description};
    GetHybridSearchVisitor(data).visit(ast);
}

inline void assertNoHybridSearch(const ASTPtr & ast, const char * description)
{
    GetHybridSearchVisitor::Data data{.assert_no_hybrid_search = description};
    GetHybridSearchVisitor(data).visit(ast);
}

}
