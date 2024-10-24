#pragma once

#include <tantivy_search.h>
#include <Storages/MergeTree/SkipIndex/Store/IndexStore.h>

namespace DB
{

static constexpr auto TANTIVY_INDEX_META_FILE_SUFFIX = ".meta";
static constexpr auto TANTIVY_INDEX_DATA_FILE_SUFFIX = ".data";


class TantivyIndexStore : public IndexStore
{
public:
    TantivyIndexStore(
        const String & index_name_, const DataPartStoragePtr storage_, const MutableDataPartStoragePtr storage_builder_ = nullptr);

    BoolWithMessage freeIndexReaderImpl(const String & full_index_path) override;
    BoolWithMessage freeIndexWriterImpl(const String & full_index_path) override;

    BoolWithMessage commitIndexImpl(const String & full_index_path) override;

    BoolWithMessage loadIndexReaderImpl(const String & full_index_path) override;
    BoolWithMessage loadIndexWriterImpl(const String & full_index_path) override;

    bool indexMultiColumnDoc(uint64_t row_id, std::vector<String> & column_names, std::vector<String> & docs);


    /*** BEGIN --- For Skip Index Query. ***/
    rust::cxxbridge1::Vec<std::uint8_t> singleTermQueryBitmap(String column_name, String term);
    rust::cxxbridge1::Vec<std::uint8_t> sentenceQueryBitmap(String column_name, String sentence);
    rust::cxxbridge1::Vec<std::uint8_t> regexTermQueryBitmap(String column_name, String pattern);
    rust::cxxbridge1::Vec<std::uint8_t> termsQueryBitmap(String column_name, std::vector<String> terms);
    /***  END  --- For Skip Index Query. ***/

    /*** BEGIN --- For BM25 Search and HybridSearch. ***/
    /// @brief If enable_nlq is true, use Natural Language Search. If enable_nlq is false, use Standard Search.
    rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> bm25Search(
        String sentence,
        bool enable_nlq,
        bool operator_or,
        TANTIVY::Statistics & statistics,
        size_t topk,
        std::vector<String> column_names = {});


    /// @brief execute bm25 search with row_id_bitmap.
    rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> bm25SearchWithFilter(
        String sentence,
        bool enable_nlq,
        bool operator_or,
        TANTIVY::Statistics & statistics,
        size_t topk,
        const std::vector<uint8_t> & u8_alived_bitmap,
        std::vector<String> column_names = {});


    /// @brief Get current part sentence doc_freq, sentence will be tokenized by tokenizer with each indexed column.
    rust::cxxbridge1::Vec<TANTIVY::DocWithFreq> getDocFreq(String sentence);
    /// @brief Get current part total_num_docs, each column will have same total_num_docs.
    UInt64 getTotalNumDocs();
    /// @brief Get current part total_num_tokens, each column will have it's own total_num_tokens.
    rust::cxxbridge1::Vec<TANTIVY::FieldTokenNums> getTotalNumTokens();
    /// @brief Get the number of documents stored in the index file.
    UInt64 getIndexedDocsNum();
    /***  END  --- For BM25 Search and HybridSearch. ***/
};


class TantivyIndexStoreWeightFunc
{
public:
    size_t operator()(const TantivyIndexStore & /* value */) const { return 0; }
};


using TantivyIndexStorePtr = std::shared_ptr<TantivyIndexStore>;

}
