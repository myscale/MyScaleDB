#pragma GCC diagnostic ignored "-Wreserved-identifier"
#include "FaissIndex.h"
#include <iostream>
#include "IndexException.h"
#include "IndexReader.h"
#include "IndexWriter.h"
#include "faiss/AutoTune.h"
#include "faiss/impl/AuxIndexStructures.h"
#include "faiss/index_io.h"

namespace DB::ErrorCodes
{
extern const int EMPTY_DATA_PASSED;
extern const int STD_EXCEPTION;
}
namespace VectorIndex
{
BinaryPtr FaissIndex::serialize(size_t max_bytes, bool & finished)
{
    IndexWriter writer;
    faiss::write_index_incremental(index.get(), &writer, max_bytes, finished);
    return convertStructToBinary(writer.data, writer.actual_size);
}

void FaissIndex::load(BinaryPtr & bi, int64_t /*total_vec*/)
{
    ///FLAT is just a vector, so no bother keeping the original data.
    if (it != FLAT)
    {
        setRawData(bi);
    }
    if (bi->size == 0 || bi->data == nullptr)
    {
        throw IndexException(DB::ErrorCodes::EMPTY_DATA_PASSED, "load: failed with empty data");
    }
    IndexReader reader;
    reader.data = bi->data;
    reader.total = bi->size;
    try
    {
        index.reset(faiss::read_index(&reader));
    }
    catch (const std::runtime_error & e)
    {
        throw IndexException(DB::ErrorCodes::STD_EXCEPTION, e.what());
    }
    catch (const faiss::FaissException & e)
    {
        throw IndexException(DB::ErrorCodes::STD_EXCEPTION, e.what());
    }
    // reinterpret_cast might seem fishy, but when they returned from read_index they initially
    // created a child class then cast it to Index.
}

void * FaissIndex::convertInnerBitMap(GeneralBitMapPtr outerBitMap)
{
    /// handle this pointer carefully! remember to deconstruct it somewhere
    faiss::bitMap * new_map = new faiss::bitMap(outerBitMap->get_size(), outerBitMap->bitmap);
    return new_map;
}

BinaryPtr FaissIndex::convertStructToBinary(uint8_t * index_data, uint64_t written_size)
{
    BinaryPtr serial_index = std::make_shared<Binary>();
    serial_index->data = index_data;
    serial_index->size = written_size;
    return serial_index;
}

Parameters FaissIndex::convertParamsToMap(std::string keys)
{
    Parameters params;
    std::vector<std::string> first_process;
    std::istringstream f(keys);
    std::string s;
    std::string s2;
    while (getline(f, s, ','))
    {
        std::istringstream f2(s);
        while (getline(f2, s2, '='))
        {
            first_process.push_back(s2);
        }
        params.insert(std::make_pair(first_process[0], first_process[1]));
        first_process.clear();
    }
    return params;
}

AccParametersPack FaissIndex::exploreTask(
    const float * query_data,
    const int64_t * gt,
    int topK,
    int query_size,
    bool oneRecall,
    std::mutex & m,
    std::condition_variable & cv,
    bool & go,
    Poco::Logger * log)
{
    LOG_INFO(
        log,
        "Preparing auto-tune criterion {} at top {}"
        ", with nq={}\n",
        oneRecall ? "one Recall" : "intersection Recall",
        topK,
        query_size);

    std::shared_ptr<faiss::AutoTuneCriterion> criterion;
    if (oneRecall)
    {
        criterion = std::make_shared<faiss::OneRecallAtRCriterion>(query_size, topK);
    }
    else
    {
        criterion = std::make_shared<faiss::IntersectionCriterion>(query_size, topK);
    }
    criterion->set_groundtruth(topK, nullptr, gt);
    criterion->nnn = topK;

    faiss::ParameterSpace param_space;
    //params.min_test_duration = 1;
    param_space.initialize(index.get());
    LOG_INFO(log, "Auto-tuning over {} parameters ({} combinations)", param_space.parameter_ranges.size(), param_space.n_combinations());
    faiss::OperatingPoints ops;
    param_space.explore(index.get(), query_size, query_data, *criterion, &ops, std::ref(m), std::ref(cv), std::ref(go));
    AccParametersPack result;
    LOG_INFO(log, "found these optimal point:");
    for (size_t i = 0; i < ops.optimal_pts.size(); i++)
    {
        if (ops.optimal_pts[i].perf <= 0.1)
        {
            LOG_INFO(log, "acc too low: {}, skipped", ops.optimal_pts[i].perf);
            continue;
        }
        LOG_INFO(log, "{}, at acc {}", ops.optimal_pts[i].key, ops.optimal_pts[i].perf);
        Parameters params = convertParamsToMap(ops.optimal_pts[i].key);
        result.insert(std::make_pair(ops.optimal_pts[i].perf, params));
    }
    return result;
}

int64_t FaissIndex::removeWithIds(int64_t n, int64_t * ids)
{
    faiss::IDSelectorBatch batch_selector(n, ids);
    int64_t removed = index->remove_ids(batch_selector);
    return removed;
}

}
