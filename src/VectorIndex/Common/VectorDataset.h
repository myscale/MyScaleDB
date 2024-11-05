#pragma once

#include <memory>
#include <vector>
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Storages/VSDescription.h>
#include <base/types.h>

namespace Search
{
enum class DataType;
}

namespace VectorIndex
{
enum class VectorSearchMethod
{
    IndexSearch = 1,
    BruteForce = 2,
};

template<Search::DataType T>
struct VectorDataset
{
public:
    VectorDataset(
            int64_t total_vectors_,
            int64_t dimension_,
            std::vector<typename SearchIndexDataTypeMap<T>::VectorDatasetType> &&data)
            : total_vectors(total_vectors_), dimension(dimension_), vec_data(std::move(data))
    {
        if constexpr (T == Search::DataType::BinaryVector)
        {
            if (dimension % 8 != 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "BinaryVector dimension must be a multiple of 8");
        }
    }

    VectorDataset(int64_t total_vectors_, int64_t dimension_, typename SearchIndexDataTypeMap<T>::VectorDatasetType *data_)
            : total_vectors(total_vectors_), dimension(dimension_)
    {
        if constexpr (T == Search::DataType::FloatVector)
        {
            vec_data.insert(vec_data.end(), data_, data_ + total_vectors_ * dimension_);
        }
        else if constexpr (T == Search::DataType::BinaryVector)
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

    template <VectorSearchMethod U = VectorSearchMethod::IndexSearch>
    typename std::enable_if<
        U != VectorSearchMethod::IndexSearch || T != Search::DataType::BinaryVector,
        typename SearchIndexDataTypeMap<T>::VectorDatasetType *>::type
    getData()
    {
        return vec_data.data();
    }

    template <VectorSearchMethod U>
    typename std::enable_if<
        U == VectorSearchMethod::IndexSearch && T == Search::DataType::BinaryVector,
        bool *>::type
    getData()
    {
        return reinterpret_cast<bool *>(&vec_data[0]);
    }

    std::vector<typename SearchIndexDataTypeMap<T>::VectorDatasetType> &getRawVector()
    { return vec_data; }

    template<Search::DataType U = T>
    typename std::enable_if<U == Search::DataType::BinaryVector, bool *>::type getBoolData()
    {
        // As for Binary vector, Search-index demand a `bool*` type to be passed in, and each byte stores 8-bit binary data
        return reinterpret_cast<bool *>(&vec_data[0]);
    }

    template<Search::DataType U = T>
    typename std::enable_if<U == Search::DataType::BinaryVector, int64_t>::type getN()
    { return dimension / 8; }

    void appendVectors(typename SearchIndexDataTypeMap<T>::VectorDatasetType *data, int size)
    {
        if constexpr (T == Search::DataType::FloatVector)
            vec_data.insert(vec_data.end(), data, data + size * dimension);
        else if constexpr (T == Search::DataType::BinaryVector)
            vec_data.insert(vec_data.end(), data, data + size * dimension / 8);

        total_vectors += size;
    }

    template<Search::DataType U = T>
    typename std::enable_if<U == Search::DataType::FloatVector, void>::type normalize()
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
        if constexpr (T == Search::DataType::FloatVector)
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
        else if constexpr (T==Search::DataType::BinaryVector)
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
    std::vector<typename SearchIndexDataTypeMap<T>::VectorDatasetType> vec_data;
};

template<Search::DataType T>
using VectorDatasetPtr = std::shared_ptr<VectorDataset<T>>;

using Float32VectorDatasetPtr = std::shared_ptr<VectorDataset<Search::DataType::FloatVector>>;
using BinaryVectorDatasetPtr = std::shared_ptr<VectorDataset<Search::DataType::BinaryVector>>;
using VectorDatasetVariantPtr = std::variant<VectorIndex::Float32VectorDatasetPtr, VectorIndex::BinaryVectorDatasetPtr>;
}
