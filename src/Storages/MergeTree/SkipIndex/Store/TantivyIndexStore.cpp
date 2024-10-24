#include <Storages/MergeTree/SkipIndex/Store/TantivyIndexStore.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TANTIVY_SEARCH_INTERNAL_ERROR;
}


TantivyIndexStore::TantivyIndexStore(
    const String & index_name_, const DataPartStoragePtr storage_, const MutableDataPartStoragePtr storage_builder_)
    : IndexStore(
        SkipIndexType::TantivyIndex,
        index_name_,
        TANTIVY_INDEX_META_FILE_SUFFIX,
        TANTIVY_INDEX_DATA_FILE_SUFFIX,
        storage_,
        storage_builder_)
{
}

bool TantivyIndexStore::indexMultiColumnDoc(uint64_t row_id, std::vector<String> & column_names, std::vector<String> & docs)
{
    if (!this->getIndexWriterStatus())
        this->loadIndexWriter();
    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();

    TANTIVY::FFIBoolResult index_status = TANTIVY::ffi_index_multi_column_docs(index_files_cache_path, row_id, column_names, docs);
    if (index_status.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(index_status.error.message));
    }

    if (!index_status.result)
    {
        LOG_ERROR(log, "[indexMultiColumnDoc] Error happend when FTS indexing doc under index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "Error happend when FTS indexing doc under index_cache:{}", index_files_cache_path);
    }

    return true;
}

BoolWithMessage TantivyIndexStore::freeIndexReaderImpl(const String & full_index_path)
{
    TANTIVY::FFIBoolResult free_status = TANTIVY::ffi_free_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage TantivyIndexStore::freeIndexWriterImpl(const String & full_index_path)
{
    TANTIVY::FFIBoolResult free_status = TANTIVY::ffi_free_index_writer(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage TantivyIndexStore::commitIndexImpl(const String & full_index_path)
{
    TANTIVY::FFIBoolResult free_status = TANTIVY::ffi_index_writer_commit(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage TantivyIndexStore::loadIndexReaderImpl(const String & full_index_path)
{
    TANTIVY::FFIBoolResult free_status = TANTIVY::ffi_load_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}

BoolWithMessage TantivyIndexStore::loadIndexWriterImpl(const String & full_index_path)
{
    TantivyIndexSettingsPtr tantivy_index_settings = std::dynamic_pointer_cast<TantivyIndexSettings>(this->index_settings);
    TANTIVY::FFIBoolResult free_status = TANTIVY::ffi_create_index_with_parameter(
        full_index_path, tantivy_index_settings->indexed_columns, tantivy_index_settings->json_parameter);
    return FFI_BOOL_CONVERT(free_status);
}

rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::singleTermQueryBitmap(String column_name, String term)
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIVecU8Result result
        = TANTIVY::ffi_query_term_bitmap(this->index_files_manager->getFullIndexPathInCache(), column_name, term);
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::sentenceQueryBitmap(String column_name, String sentence)
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIVecU8Result result
        = TANTIVY::ffi_query_sentence_bitmap(this->index_files_manager->getFullIndexPathInCache(), column_name, sentence);
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::regexTermQueryBitmap(String column_name, String pattern)
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIVecU8Result result
        = TANTIVY::ffi_regex_term_bitmap(this->index_files_manager->getFullIndexPathInCache(), column_name, pattern);
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}
rust::cxxbridge1::Vec<std::uint8_t> TantivyIndexStore::termsQueryBitmap(String column_name, std::vector<String> terms)
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIVecU8Result result
        = TANTIVY::ffi_query_terms_bitmap(this->index_files_manager->getFullIndexPathInCache(), column_name, terms);
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> TantivyIndexStore::bm25Search(
    String sentence, bool enable_nlq, bool operator_or, TANTIVY::Statistics & statistics, size_t topk, std::vector<String> column_names)
{
    DB::OpenTelemetry::SpanHolder span("TantivyIndexStore::bm25_search");
    if (!this->index_reader_status)
        this->loadIndexReader();

    std::vector<uint8_t> u8_alived_bitmap;
    TANTIVY::FFIVecRowIdWithScoreResult result = TANTIVY::ffi_bm25_search(
        this->index_files_manager->getFullIndexPathInCache(),
        sentence,
        column_names,
        static_cast<uint32_t>(topk),
        u8_alived_bitmap,
        false,
        enable_nlq,
        operator_or,
        statistics);

    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

rust::cxxbridge1::Vec<TANTIVY::RowIdWithScore> TantivyIndexStore::bm25SearchWithFilter(
    String sentence,
    bool enable_nlq,
    bool operator_or,
    TANTIVY::Statistics & statistics,
    size_t topk,
    const std::vector<uint8_t> & u8_alived_bitmap,
    std::vector<String> column_names)
{
    DB::OpenTelemetry::SpanHolder span("TantivyIndexStore::bm25_search_with_filter");
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIVecRowIdWithScoreResult result = TANTIVY::ffi_bm25_search(
        this->index_files_manager->getFullIndexPathInCache(),
        sentence,
        column_names,
        static_cast<uint32_t>(topk),
        u8_alived_bitmap,
        true,
        enable_nlq,
        operator_or,
        statistics);
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

rust::cxxbridge1::Vec<TANTIVY::DocWithFreq> TantivyIndexStore::getDocFreq(String sentence)
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIVecDocWithFreqResult result = TANTIVY::ffi_get_doc_freq(this->index_files_manager->getFullIndexPathInCache(), sentence);
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

UInt64 TantivyIndexStore::getTotalNumDocs()
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIU64Result result = TANTIVY::ffi_get_total_num_docs(this->index_files_manager->getFullIndexPathInCache());
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

rust::cxxbridge1::Vec<TANTIVY::FieldTokenNums> TantivyIndexStore::getTotalNumTokens()
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIFieldTokenNumsResult result = TANTIVY::ffi_get_total_num_tokens(this->index_files_manager->getFullIndexPathInCache());
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

UInt64 TantivyIndexStore::getIndexedDocsNum()
{
    if (!this->index_reader_status)
        this->loadIndexReader();

    TANTIVY::FFIU64Result result = TANTIVY::ffi_get_indexed_doc_counts(this->index_files_manager->getFullIndexPathInCache());
    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::TANTIVY_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

}
