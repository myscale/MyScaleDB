#pragma once
#include <Common/Exception.h>

namespace VectorIndex
{
class VIException : public DB::Exception
{
public:
    VIException(int code, const std::string & message) : DB::Exception(code, "VectorIndex: {}", message) { }

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    VIException(int code, const std::string & fmt, Args &&... args)
        : DB::Exception(code, fmt::runtime("VectorIndex: " + fmt), std::forward<Args>(args)...)
    {
    }
};
};
