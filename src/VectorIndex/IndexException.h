#pragma once
#include <Common/Exception.h>

namespace VectorIndex
{
class IndexException : public DB::Exception
{
public:
    IndexException(int code_, const std::string & message_) : DB::Exception(message_, code_) { }
};

}
