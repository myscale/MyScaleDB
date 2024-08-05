#include <VectorIndex/Common/BM25InfoInDataParts.h>

#include <Common/logger_useful.h>

namespace DB
{

#if USE_TANTIVY_SEARCH
UInt64 BM25InfoInDataPart::getTotalDocsCount() const
{
    return total_docs;
}

VecTextColumnTokenNums BM25InfoInDataPart::getTextColsTotalNumTokens() const
{
    return text_cols_total_num_tokens;
}

const RustVecDocWithFreq & BM25InfoInDataPart::getTermWithDocNums() const
{
    return term_with_doc_nums;
}


UInt64 BM25InfoInDataParts::getTotalDocsCountAllParts() const
{
    UInt64 result = 0;
    for (const auto & part : *this)
        result += part.getTotalDocsCount();
    return result;   
}

VecTextColumnTokenNums BM25InfoInDataParts::getTextColsTotalNumTokensAllParts() const
{
    /// Add total num tokens in all parts based on column name
    std::map<UInt32, UInt64> text_cols_total_tokens_map;
    for (const auto & part : *this)
    {
        const auto & cols_total_tokens_in_part = part.getTextColsTotalNumTokens();

        /// Loop through the vector of Vec<TextColumnTokenNums> in a part
        for (auto & col_total_tokens : cols_total_tokens_in_part)
            text_cols_total_tokens_map[col_total_tokens.field_id] += col_total_tokens.field_total_tokens;
    }

    VecTextColumnTokenNums result;
    result.reserve(text_cols_total_tokens_map.size());

    for (const auto & [field_id, field_total_tokens] : text_cols_total_tokens_map)
        result.push_back({field_id, field_total_tokens});

    return result;
}

RustVecDocWithFreq BM25InfoInDataParts::getTermWithDocNumsAllParts() const
{
    /// Add number of docs containing a term in all parts based on term name and column name
    using FieldIdAndTokenName = std::pair<UInt32, String>;
    std::map<FieldIdAndTokenName, UInt64> field_token_name_with_docs_map;
    for (const auto & part : *this)
    {
        auto & doc_nums_in_part = part.getTermWithDocNums();

        /// Loop through the vector of Vec<DocWithFreq> in a part
        for (auto & field_token_doc_freq : doc_nums_in_part)
        {
            FieldIdAndTokenName field_token = FieldIdAndTokenName(field_token_doc_freq.field_id, field_token_doc_freq.term_str);
            field_token_name_with_docs_map[field_token] += field_token_doc_freq.doc_freq;
        }
    }

    RustVecDocWithFreq result;
    result.reserve(field_token_name_with_docs_map.size());

    for (const auto & [col_token, doc_freq] : field_token_name_with_docs_map)
        result.push_back({col_token.second, col_token.first, doc_freq});

    return result;
}
#endif
}
