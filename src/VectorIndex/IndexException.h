#pragma once
#include <Common/Exception.h>

namespace VectorIndex
{
class IndexException : public DB::Exception
{
public:
    IndexException(int code_, const std::string & message_) : DB::Exception("[VectorIndex] " + message_, code_) { }

    /// Just record index status code and message, and wait for further processing.
    IndexException(const std::string & message_, int code_) : status_code(code_), status_message(message_) { }

    int statusCode() const { return status_code; }
    String statusMessage() const { return status_message; }

    // Format message with fmt::format, like the logging functions.
    template <typename... Args>
    IndexException(int code, const std::string & fmt, Args &&... args)
        : DB::Exception(code, fmt::runtime("vector index: " + fmt), std::forward<Args>(args)...)
    {
    }

private:
    /// Record index status code and message
    int status_code;
    String status_message;
};
};
