#pragma once

#include <Common/logger_useful.h>
#include <Common/VectorScanUtils.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_VECTOR_SCAN;
}

class GetVectorScanMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<GetVectorScanMatcher, true>;

    /// may have multiple vector scan functions
    struct Data
    {
        const char * assert_no_vector_scan = nullptr;
        std::vector<const ASTFunction *> vector_scan_funcs;
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
            if (isVectorScanFunc(func->name))
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
        // throw Exception(ErrorCodes::ILLEGAL_VECTOR_SCAN, "Unknown function");
        /// Poco::Logger * log = &Poco::Logger::get("GetVectorScanMatcher");
        if (isVectorScanFunc(node.name))
        {
            if (data.assert_no_vector_scan)
                throw Exception(ErrorCodes::ILLEGAL_VECTOR_SCAN, "Vector Scan function {} is found {} in query", node.getColumnName(), String(data.assert_no_vector_scan));
            data.vector_scan_funcs.push_back(&node);
        }
    }
};

using GetVectorScanVisitor = GetVectorScanMatcher::Visitor;

inline void assertNoVectorScan(const ASTPtr & ast, const char * description)
{
    GetVectorScanVisitor::Data data{.assert_no_vector_scan = description};
    GetVectorScanVisitor(data).visit(ast);
}

}
