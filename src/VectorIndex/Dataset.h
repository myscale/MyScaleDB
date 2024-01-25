#pragma once

#include <memory>
#include <vector>
#include <base/types.h>
#include <Interpreters/VectorScanDescription.h>
#include <VectorIndex/VectorIndexCommon.h>

namespace VectorIndex
{
template<DB::VectorSearchType T>
struct VectorDataset
{
public:
    VectorDataset(
            int64_t total_vectors_,
            int64_t dimension_,
            std::vector<typename VectorSearchTypeMap<T>::VectorDatasetType> &&data)
            : total_vectors(total_vectors_), dimension(dimension_), vec_data(std::move(data))
    {
        if constexpr (T == DB::VectorSearchType::BinaryVector)
        {
            if (dimension % 8 != 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "BinaryVector dimension must be a multiple of 8");
        }
    }

    VectorDataset(int64_t total_vectors_, int64_t dimension_, typename VectorSearchTypeMap<T>::VectorDatasetType *data_)
            : total_vectors(total_vectors_), dimension(dimension_)
    {
        if constexpr (T == DB::VectorSearchType::Float32Vector)
        {
            vec_data.insert(vec_data.end(), data_, data_ + total_vectors_ * dimension_);
        }
        else if constexpr (T == DB::VectorSearchType::BinaryVector)
        {
            if (dimension % 8 != 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "BinaryVector dimension must be a multiple of 8");
            vec_data.insert(vec_data.end(), data_, data_ + total_vectors_ * dimension / 8);
        }
        else
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported vector search type for dataset");
    }

    int64_t getVectorNum()
    { return total_vectors; }

    int64_t getDimension()
    { return dimension; }

    typename VectorSearchTypeMap<T>::VectorDatasetType *getData()
    { return vec_data.data(); }

    std::vector<typename VectorSearchTypeMap<T>::VectorDatasetType> &getRawVector()
    { return vec_data; }

    template<DB::VectorSearchType U = T>
    typename std::enable_if<U == DB::VectorSearchType::BinaryVector, bool *>::type getBoolData()
    {
        // As for Binary vector, Search-index demand a `bool*` type to be passed in, and each byte stores 8-bit binary data
        return reinterpret_cast<bool *>(&vec_data[0]);
    }

    template<DB::VectorSearchType U = T>
    typename std::enable_if<U == DB::VectorSearchType::BinaryVector, int64_t>::type getN()
    { return dimension / 8; }

    void appendVectors(typename VectorSearchTypeMap<T>::VectorDatasetType *data, int size)
    {
        if constexpr (T == DB::VectorSearchType::Float32Vector)
            vec_data.insert(vec_data.end(), data, data + size * dimension);
        else if constexpr (T == DB::VectorSearchType::BinaryVector)
            vec_data.insert(vec_data.end(), data, data + size * dimension / 8);

        total_vectors += size;
    }

    template<DB::VectorSearchType U = T>
    typename std::enable_if<U == DB::VectorSearchType::Float32Vector, void>::type normalize()
    {
        for (int idx = 0; idx < total_vectors; idx++)
        {
            float sum = 0;
            float *ptr = static_cast<float *>(getData() + idx * dimension);
            for (int d = 0; d < dimension; d++)
            {
                sum += ptr[d] * ptr[d];
            }
            if (sum < std::numeric_limits<float>::epsilon())
                continue;
            sum = std::sqrt(sum);
            for (int d = 0; d < dimension; d++)
            {
                ptr[d] /= sum;
            }
        }
    }

    String printVectors()
    {
        String result;
        if constexpr (T == DB::VectorSearchType::Float32Vector)
        {
            for (int i = 0; i < static_cast<size_t>(total_vectors); i++)
            {
                for (int j = 0; j < dimension; j++)
                {
                    result += std::to_string(vec_data[i * dimension + j]) + ",";
                }
                result += "\n";
            }
        }
        else if constexpr (T==DB::VectorSearchType::BinaryVector)
        {
            size_t fixed_N = dimension / 8;
            for (size_t i = 0; i < static_cast<size_t>(total_vectors); i++)
            {
                for (size_t j = 0; j < fixed_N; j++)
                {
                    result += std::bitset<8>(vec_data[i * fixed_N + j]).to_string() + " ";
                }
                result += ",\n";
            }
        }
        return result;
    }

private:
    int64_t total_vectors;      // size of a dataset
    int64_t dimension;          // dimension of each vector
    std::vector<typename VectorSearchTypeMap<T>::VectorDatasetType> vec_data;
};

template<DB::VectorSearchType T>
using VectorDatasetPtr = std::shared_ptr<VectorDataset<T>>;

using Float32VectorDatasetPtr = std::shared_ptr<VectorDataset<DB::VectorSearchType::Float32Vector>>;
using BinaryVectorDatasetPtr = std::shared_ptr<VectorDataset<DB::VectorSearchType::BinaryVector>>;
using VectorDatasetVariantPtr = std::variant<VectorIndex::Float32VectorDatasetPtr, VectorIndex::BinaryVectorDatasetPtr>;
}
