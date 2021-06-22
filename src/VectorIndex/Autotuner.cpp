#include <VectorIndex/Autotuner.h>
#include <thread>
#include <faiss/AutoTune.h>
#include <VectorIndex/DiskIOWriter.h>
#include <VectorIndex/VectorIndexCommon.h>

namespace VectorIndex
{

void Autotuner::start()
{
    thread = ThreadFromGlobalPool(&Autotuner::run, this);
}

void Autotuner::run()
{
    go = true;
    LOG_INFO(log, "starting index autotuner");
    while (true)
    {
        if (quit)
        {
            return;
        }
        if (!tuning_task_queue.empty())
        {
            current = tuning_task_queue.begin();
            Execute(current->first, current->second);
            serializeResult(current->first);
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void Autotuner::addTask(std::string segment_id, TuningPackPtr tuningPack)
{
    tuning_task_queue.insert(std::make_pair(segment_id, tuningPack));
}

void Autotuner::pause()
{
    LOG_TRACE(log, "pausing autotuner");
    go = false;
}

void Autotuner::resume()
{
    LOG_TRACE(log, "resuming autotuner");
    go = true;
    pause_c.notify_one();
}

void Autotuner::Execute(std::string segment_id, TuningPackPtr tuning)
{
    int query_size = tuning->query_size;
    int topK = tuning->topK;
    int64_t * gt = tuning->gt->data();
    float * query = tuning->query->data();
    bool one_recall = tuning->oneRecall;
    LOG_INFO(log, "{} entering auto tune area", segment_id);
    AccParametersPack result
        = tuning->index->exploreTask(query, gt, topK, query_size, one_recall, std::ref(pause_m), std::ref(pause_c), std::ref(go), log);
    finished_tunes.insert(std::make_pair(segment_id, result));
}

void Autotuner::serializeResult(std::string segment_id)
{
    LOG_INFO(log, "{} write op points to persistent", segment_id);
    AccParametersPack map;
    if (finished_tunes.contains(segment_id))
    {
        map = finished_tunes[segment_id];
    }
    else
    {
        return;
    }
    if (map.empty())
    {
        LOG_INFO(log, "{} can't find any op point, finish autotune with no result", segment_id);
    }
    else
    {
        std::string to_be_written = AccParametersPackToString(map);
        char * data = to_be_written.data();
        int64_t size = to_be_written.size();
        DiskIOWriter diskwriter;
        diskwriter.open(segment_id + PARAMETER_PACK_NAME, false);
        diskwriter.write(&size, sizeof(size));
        diskwriter.write(data, size);
        diskwriter.close();
    }
    removeTask(segment_id);
}

bool Autotuner::removeTask(std::string segment_id)
{
    bool removed = false;
    if (finished_tunes.contains(segment_id))
    {
        finished_tunes.erase(segment_id);
        if (tuning_task_queue.contains(segment_id))
        {
            tuning_task_queue.erase(segment_id);
        }
        removed = true;
    }
    return removed;
}

Autotuner * Autotuner::getInstance()
{
    static Autotuner autotuner;
    return &autotuner;
}

Autotuner::~Autotuner()
{
    try
    {
        quit = true;
        if (thread.joinable())
            thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
