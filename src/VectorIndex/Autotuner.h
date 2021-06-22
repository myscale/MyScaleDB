#pragma once
#pragma GCC diagnostic ignored "-Wshadow-field-in-constructor"
#pragma GCC diagnostic ignored "-Wreserved-identifier"

#include <string>
#include <thread>
#include <unordered_map>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <VectorIndex/Status.h>
#include <VectorIndex/VectorIndex.h>

namespace VectorIndex
{
struct TuningPack
{
    TuningPack(
        VectorIndexPtr & index_, std::vector<float> * query_, std::vector<int64_t> * gt_, int topK_, int query_size_, bool oneRecall_)
        : index(index_), query(query_), gt(gt_), topK(topK_), query_size(query_size_), oneRecall(oneRecall_)
    {
    }
    VectorIndexPtr index;
    std::vector<float> * query;
    std::vector<int64_t> * gt;
    int topK;
    int query_size;
    bool oneRecall;

    ~TuningPack()
    {
        free(query);
        free(gt);
    }
};

using TuningPackPtr = std::shared_ptr<TuningPack>;
using Autotune_queue = std::unordered_map<std::string, TuningPackPtr>;

class Autotuner
{
public:
    Autotuner() { log = &Poco::Logger::get("AutoTuner"); }
    void addTask(std::string segment_id, TuningPackPtr tuningPack);
    void resume();
    void pause();
    void start();
    void run();
    void serializeResult(std::string segment_id);
    bool removeTask(std::string segment_id);
    static Autotuner * getInstance();
    ~Autotuner();

private:
    void Execute(std::string, TuningPackPtr);

    std::unordered_map<std::string, TuningPackPtr>::iterator current;
    std::mutex pause_m;
    std::condition_variable pause_c;
    bool go;
    ///segment_id : TuningPack
    Autotune_queue tuning_task_queue;
    ///segment_id: (accuracy : parameters...)
    std::unordered_map<std::string, AccParametersPack> finished_tunes;
    Poco::Logger * log;
    ThreadFromGlobalPool thread;
    std::atomic<bool> quit{false};
};
using AutotunerPtr = std::shared_ptr<Autotuner>;
}
