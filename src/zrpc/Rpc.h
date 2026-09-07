#pragma once

#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>

#include "zrpc_global.h"

namespace zrpc {
enum class StatusCode
{
    INACTIVE,
    ACTIVE,
    OK,
    CANCELLED,
    APPLICATION_ERROR,
    DEADLINE_EXCEEDED,
    TERMINATED,
};

enum class ApplicationError
{
    NO_ERROR,
    INVALID_HEADER,
    NO_SUCH_SERVICE,
    NO_SUCH_METHOD,
    INVALID_MESSAGE,
    METHOD_NOT_IMPLEMENTED,
};

class ZRPC_EXPORT Rpc
{
public:
    bool ok() { return status() == StatusCode::OK; }

    StatusCode status() const { return _statusCode; }
    void setStatus(StatusCode code) { _statusCode = code; }

    ApplicationError error() const { return _appError; }
    const std::string &errorMessage() const { return _appErrorMsg; }
    void setError(ApplicationError error, const std::string &errorMsg);

    int64_t timeout() const { return _timeoutMs; }
    void setTimeout(int64_t timeoutMs) { _timeoutMs = timeoutMs; }

    int wait();
    void signal();

private:
    StatusCode _statusCode{StatusCode::INACTIVE};
    ApplicationError _appError{ApplicationError::NO_ERROR};
    std::string _appErrorMsg;

    int64_t _timeoutMs{-1};

    bool _ready{false};
    std::mutex _mutex;
    std::condition_variable _cv;
};
}
