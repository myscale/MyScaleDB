#pragma once
#include <Common/Exception.h>

namespace VectorIndex
{
class IndexException : public DB::Exception
{
public:
    IndexException(int code_, const std::string & message_) : DB::Exception("[VectorIndex] " + message_, code_) { }

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    IndexException(int code, const std::string & fmt, Args &&... args)
        : DB::Exception(code, fmt::runtime("vector index: " + fmt), std::forward<Args>(args)...)
    {
    }
};
};
