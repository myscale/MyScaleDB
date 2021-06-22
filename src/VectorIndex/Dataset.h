#pragma once

#include <memory>
#include <vector>
#include <base/types.h>

namespace VectorIndex
{

struct VectorDataset
{
public:
    VectorDataset(int64_t total_vectors_, int64_t dimension_, std::vector<float> && data)
        : total_vectors(total_vectors_), dimension(dimension_), vec_data(std::move(data))
    {
    }

    VectorDataset(int64_t total_vectors_, int64_t dimension_, float * data_) : total_vectors(total_vectors_), dimension(dimension_)
    {
        vec_data.insert(vec_data.end(), data_, data_ + total_vectors_ * dimension_);
    }

    int64_t getVectorNum() { return total_vectors; }

    float * getData() { return vec_data.data(); }

    std::vector<float> & getRawVector() { return vec_data; }

    int64_t getDimension() { return dimension; }

    void appendVectors(float * data, int size)
    {
        vec_data.insert(vec_data.end(), data, data + size * dimension);
        total_vectors += size;
    }

    String printVectors()
    {
        String result;
        for (int i = 0; i < total_vectors; ++i)
        {
            for (int j = 0; j < dimension; ++j)
            {
                result += std::to_string(vec_data[i * dimension + j]) + ",";
            }
            result += "\n";
        }
        return result;
    }

    void normalize()
    {
        for (int idx = 0; idx < total_vectors; idx++)
        {
            float sum = 0;
            float * ptr = static_cast<float *>(getData() + idx * dimension);
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

private:
    int64_t total_vectors; //size of a dataset, number of vectors
    int64_t dimension; //dimension of each vector
    std::vector<float> vec_data; //total_num of floats calculated as total_vectors*dimension
};
using VectorDatasetPtr = std::shared_ptr<VectorDataset>;
}
