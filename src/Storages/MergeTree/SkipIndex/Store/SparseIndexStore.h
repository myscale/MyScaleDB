#pragma once

#include <sparse_index.h>
#include <Storages/MergeTree/SkipIndex/Store/IndexStore.h>

namespace DB
{

static constexpr auto SPARSE_INDEX_META_FILE_SUFFIX = ".sparse_meta";
static constexpr auto SPARSE_INDEX_DATA_FILE_SUFFIX = ".sparse_data";

class SparseIndexStore : public IndexStore
{
public:
    SparseIndexStore(
        const String & index_name_, const DataPartStoragePtr storage_, const MutableDataPartStoragePtr storage_builder_ = nullptr);

    BoolWithMessage freeIndexReaderImpl(const String & full_index_path) override;
    BoolWithMessage freeIndexWriterImpl(const String & full_index_path) override;

    BoolWithMessage commitIndexImpl(const String & full_index_path) override;

    BoolWithMessage loadIndexReaderImpl(const String & full_index_path) override;
    BoolWithMessage loadIndexWriterImpl(const String & full_index_path) override;

    bool indexSparseVector(
        uint64_t row_id, const std::vector<String> & column_names, const std::vector<rust::Vec<SPARSE::TupleElement>> & sparse_vectors);

    rust::Vec<SPARSE::ScoredPointOffset> sparseSearch(
        const std::unordered_map<uint32_t, float> & sparse_vector, uint32_t topk, const std::vector<uint8_t> & u8_alived_bitmap = {});
};


class SparseIndexStoreWeightFunc
{
public:
    size_t operator()(const SparseIndexStore & /* value */) const { return 0; }
};

using SparseIndexStorePtr = std::shared_ptr<SparseIndexStore>;

}
