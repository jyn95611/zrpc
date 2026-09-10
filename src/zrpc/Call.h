#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>

#include "zrpc_global.h"

namespace zrpc {
enum class ErrorCode
{
    Ok = 0,
    CallInProcess,
    Timeout,
    InvalidMessage,

    NoSuchService = 100,
    NoSuchMethod,
    MethodNotImplemented,
};

struct CallOptions
{
    int64_t timeoutMs{-1};
};

struct CallResult
{
    ErrorCode code{ErrorCode::Ok};
    std::string message;
    std::string reply;

    bool ok() const { return code == ErrorCode::Ok; }
};

class StubPrivate;

class ZRPC_EXPORT CallHandle
{
public:
    bool ready() const;
    CallResult get();
    const CallResult &result() const;

private:
    friend class StubPrivate;
    void complete(CallResult result);

    mutable std::mutex _mutex;
    std::condition_variable _cv;
    bool _ready{false};
    CallResult _result;
};

using CompletionCallback = std::function<void(CallResult)>;
}
