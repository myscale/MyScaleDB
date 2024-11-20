#pragma once

#include <mutex>
#include <magic_enum.hpp>

#include <base/types.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>


namespace VectorIndex
{

struct SegmentBuiltStatus
{
    enum Status
    {
        NO_DATA_PART = 0,       /// can not find data part
        SUCCESS = 1,            /// build index success
        BUILD_FAIL = 2,         /// build index fail
        META_ERROR = 3,         /// index meta error
        MISCONFIGURED = 4,
        BUILD_SKIPPED = 5,      /// Index already in built, No need to build vector index for this part
        BUILD_CANCEL = 6,       /// cancel build index
        BUILD_RETRY = 7,        /// Retry to move vector index files to part directory
    };
    Status getStatus() { return status; }

    String statusToString()
    {
        switch (status)
        {
            case Status::NO_DATA_PART:          return "no_data_part";
            case Status::SUCCESS:               return "success";
            case Status::BUILD_FAIL:            return "build_fail";
            case Status::META_ERROR:            return "meta_error";
            case Status::MISCONFIGURED:         return "miss_configured";
            case Status::BUILD_SKIPPED:         return "skipped";
            case Status::BUILD_CANCEL:          return "build_fail";
            case Status::BUILD_RETRY:           return "build_retry";
        }
        UNREACHABLE();
    }

    Status status;
    int err_code{0};
    String err_msg{""};
};

struct SegmentStatus
{
    enum Status
    {
        SMALL_PART,             // rows_count < min_rows_to_build_index
        PENDING,                // part without index, wait build index
        BUILDING,               // part without index, is building index 
        BUILT,                  // part with ready index(not decouple index), rename with ready
        LOADED,                 // fake status, only displayed in vector_index_segments system table
        ERROR,                  // part build index error, and will not build index more than
        CANCELLED,              // cancel all index action
        UNKNOWN                 // index in unknown status, such as index does not exist
    };

    SegmentStatus(Status status_ = Status::PENDING, String err_msg_ = "") : status(status_), err_msg(err_msg_) {}

    String statusToString() const
    {
        return std::string(magic_enum::enum_name(status));
    }
    Status getStatus() const { std::lock_guard lock(mutex); return status; }
    String getErrMsg() const { std::lock_guard lock(mutex); return err_msg; }
    void setErrMsg(const String & msg) { std::lock_guard lock(mutex); err_msg = msg; }
    UInt64 getElapsedTime() const
    {
        std::lock_guard lock(mutex);
        return static_cast<UInt64>(watch ? watch->elapsedSeconds() : elapsed_time);
    }
    void setElapsedTime(double time) { std::lock_guard lock(mutex); elapsed_time = time; }
    void setStatus(Status status_, String err_msg_ = "") 
    {
        std::lock_guard lock(mutex);
        status = status_;
        if (status == Status::BUILDING)
        {
            watch = std::make_unique<Stopwatch>();
        }
        else
        {
            if (watch)
                watch->stop();
        }
        err_msg = err_msg_;
    }

private:
    mutable std::mutex mutex;
    Status status{Status::PENDING};
    String err_msg{""};
    StopwatchUniquePtr watch{nullptr};
    double elapsed_time{0};
};

using SegmentStatusPtr = std::shared_ptr<SegmentStatus>;

}
