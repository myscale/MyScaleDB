#include <sparse_index.h>
#include <Storages/MergeTree/SkipIndex/Store/SparseIndexStore.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SPARSE_SEARCH_INTERNAL_ERROR;
    extern const int SKIP_INDEX_BUILD_INTERNAL_ERROR;
}

SparseIndexStore::SparseIndexStore(
    const String & index_name_, const DataPartStoragePtr storage_, const MutableDataPartStoragePtr storage_builder_)
    : IndexStore(
        SkipIndexType::SparseIndex, index_name_, SPARSE_INDEX_META_FILE_SUFFIX, SPARSE_INDEX_DATA_FILE_SUFFIX, storage_, storage_builder_)
{
}

BoolWithMessage SparseIndexStore::freeIndexReaderImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_free_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::freeIndexWriterImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_free_index_writer(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::commitIndexImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_commit_index(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::loadIndexReaderImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_load_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::loadIndexWriterImpl(const String & full_index_path)
{
    // SparseIndexSettingsPtr sparse_index_settings = std::static_pointer_cast<SparseIndexSettings>(index_settings);
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_load_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}

bool SparseIndexStore::indexSparseVector(
    uint64_t row_id, const std::vector<String> & /* column_names */, const std::vector<rust::Vec<SPARSE::TupleElement>> & sparse_vectors)
{
    // SparseIndexSettingsPtr sparse_index_settings = std::static_pointer_cast<SparseIndexSettings>(index_settings);
    // TODO: This func only support one column to be indexed currently!

    if (!this->getIndexWriterStatus())
        this->loadIndexWriter();

    String index_files_cache_path = this->index_files_manager->getFullIndexPathInCache();
    SPARSE::FFIBoolResult insert_status
        = SPARSE::ffi_insert_sparse_vector(index_files_cache_path, static_cast<uint32_t>(row_id), sparse_vectors.front());

    if (insert_status.error.is_error)
    {
        throw DB::Exception(ErrorCodes::SKIP_INDEX_BUILD_INTERNAL_ERROR, "{}", std::string(insert_status.error.message));
    }

    if (!insert_status.result)
    {
        LOG_ERROR(log, "[indexOneRow] Error happend when SparseIndex indexing doc under index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::SKIP_INDEX_BUILD_INTERNAL_ERROR,
            "Error happend when SparseIndex indexing sparse_vector under index_cache:{}",
            index_files_cache_path);
    }

    return true;
}

}
