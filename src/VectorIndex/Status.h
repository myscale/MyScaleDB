#pragma once
#include <base/types.h>
namespace VectorIndex
{
struct Status
{
    Status() : code(0) { }
    Status(int c) : code(c) { }
    Status(int c, const String & msg) : code(c), message(std::move(msg)) { }

    bool fine() { return code == 0; }

    int getCode() { return code; }

    void setCode(int error) { code = error; }

    void setMessage(const String & s) { message = s; }

    String getMessage() { return message; }

private:
    int code;
    String message;
};
}
