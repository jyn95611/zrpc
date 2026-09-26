#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "zrpc_global.h"

namespace zrpc {
enum class ErrorCode
{
    Ok = 0,
    CallInProcess,
    Timeout,
    Disconnected,
    InvalidMessage,

    NoSuchService = 100,
    NoSuchMethod,
    MethodNotImplemented,
};

using Payload = std::vector<std::string>;

struct PayloadView
{
    std::vector<std::string_view> views;
    std::shared_ptr<void> owned;
};

struct CallOptions
{
    int64_t timeoutMs{-1};
};

struct CallResult
{
    ErrorCode errorCode{ErrorCode::Ok};
    std::string errorMsg;
    PayloadView payload;

    bool ok() const { return errorCode == ErrorCode::Ok; }
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
