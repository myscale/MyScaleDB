#pragma once

#include <base/types.h>
#include <vector>
#include "config.h"

#if USE_TANTIVY_SEARCH
#    include <tantivy_search.h>
#endif

namespace DB
{

#if USE_TANTIVY_SEARCH

using RustVecDocWithFreq = rust::cxxbridge1::Vec<TANTIVY::DocWithFreq>;

/// Support tantivy index on multiple text columns
using VecTextColumnTokenNums = rust::cxxbridge1::Vec<TANTIVY::FieldTokenNums>;

struct BM25InfoInDataPart
{
    UInt64 total_docs; /// Total number of documents in a data part   
    VecTextColumnTokenNums text_cols_total_num_tokens;  /// Total number of tokens from all documents on all text columns in a data part
    RustVecDocWithFreq term_with_doc_nums;  /// vector of terms with number of documents containing it

    BM25InfoInDataPart() = default;

    BM25InfoInDataPart(
        const UInt64 & total_docs_,
        const VecTextColumnTokenNums & text_cols_total_num_tokens_,
        const RustVecDocWithFreq & term_with_doc_nums_)
        : total_docs{total_docs_}
        , text_cols_total_num_tokens{text_cols_total_num_tokens_}
        , term_with_doc_nums{term_with_doc_nums_}
    {}

    UInt64 getTotalDocsCount() const;
    VecTextColumnTokenNums getTextColsTotalNumTokens() const;
    const RustVecDocWithFreq & getTermWithDocNums() const;
};

struct BM25InfoInDataParts: public std::vector<BM25InfoInDataPart>
{
    using std::vector<BM25InfoInDataPart>::vector;

    UInt64 getTotalDocsCountAllParts() const;
    VecTextColumnTokenNums getTextColsTotalNumTokensAllParts() const;
    RustVecDocWithFreq getTermWithDocNumsAllParts() const;
};

#endif
}
