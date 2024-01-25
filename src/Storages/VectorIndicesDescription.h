#pragma once

#include <base/types.h>

#include <memory>
#include <vector>
#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/VectorScanDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>
namespace DB
{

/// Description of non-primary index for Storage
struct VectorIndexDescription
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

    VectorSearchType vector_search_type = VectorSearchType::InvalidVector;

    /// Parse index from definition AST
    static VectorIndexDescription getVectorIndexFromAST(
        const ASTPtr & definition_ast,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        bool check_parameter);
    static VectorIndexDescription getVectorIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);

    VectorIndexDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    VectorIndexDescription(const VectorIndexDescription & other);
    VectorIndexDescription & operator=(const VectorIndexDescription & other);
    bool operator==(const VectorIndexDescription & other) const;


    /// Recalculate index with new columns because index expression may change
    /// if something change in columns.
    void recalculateWithNewColumns(const ColumnsDescription & new_columns);

    /// Parse vector index build arguments
    String parse_arg(String & input, const String verify_json, const String index_type, int _dim, bool check_parameter);
};

/// All secondary indices in storage
struct VectorIndicesDescription : public std::vector<VectorIndexDescription>
{
    /// Index with name exists
    bool has(const String & name) const;
    /// Index with name and type desc exists
    bool has(const VectorIndexDescription & vec_index_desc) const;
    /// Convert description to string
    String toString() const;
    /// Parse description from string
    static VectorIndicesDescription parse(const String & str, const ColumnsDescription & columns);

    /// Return common expression for all stored indices
    ExpressionActionsPtr getSingleExpressionForVectorIndices(const ColumnsDescription & columns, ContextPtr context) const;
};
}
