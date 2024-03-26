#pragma once

#include <base/types.h>

#include <memory>
#include <vector>
#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>

#include <VectorIndex/Storages/VSDescription.h>

namespace DB
{

/// Description of non-primary index for Storage
struct VIDescription
{
    /// Definition AST of index
    ASTPtr definition_ast;

    /// List of expressions for index calculation
    ASTPtr expression_list_ast;

    /// Index name
    String name;

    /// Index type (IVFPQ, IVFFLAT, etc.)
    String type;

    /// Prepared expressions for index calculations
    ExpressionActionsPtr expression;

    /// Index arguments, for example probability for bloom filter
    FieldVector arguments;

    /// Vector index arguments json, for example metric type for vector build index
    Poco::JSON::Object::Ptr parameters;

    /// Names of index column
    String column;

    /// Data types of index columns
    DataTypePtr data_type;

    /// Sample block with index columns. (NOTE: columns in block are empty, but not nullptr)
    Block sample_block;

    /// 0: not build; 1: building; 2: built; 3: fail to build
    int status;

    int dim = 0;

    Search::DataType vector_search_type;

    /// Parse index from definition AST
    static VIDescription getVectorIndexFromAST(
        const ASTPtr & definition_ast,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        bool check_parameter);
    static VIDescription getVectorIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);

    VIDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    VIDescription(const VIDescription & other);
    VIDescription & operator=(const VIDescription & other);
    bool operator==(const VIDescription & other) const;


    /// Recalculate index with new columns because index expression may change
    /// if something change in columns.
    void recalculateWithNewColumns(const ColumnsDescription & new_columns);

    /// Parse vector index build arguments
    String parse_arg(String & input, const String verify_json, const String index_type, int _dim, bool check_parameter);
};

/// All secondary indices in storage
struct VIDescriptions : public std::vector<VIDescription>
{
    /// Index with name exists
    bool has(const String & name) const;
    /// Index with name and type desc exists
    bool has(const VIDescription & vec_index_desc) const;
    /// Convert description to string
    String toString() const;
    /// Parse description from string
    static VIDescriptions parse(const String & str, const ColumnsDescription & columns);

    /// Return common expression for all stored indices
    ExpressionActionsPtr getSingleExpressionForVectorIndices(const ColumnsDescription & columns, ContextPtr context) const;
};
}
