#include "Call.h"

namespace zrpc {
bool CallHandle::ready() const
{
    std::lock_guard<std::mutex> locker(_mutex);
    return _ready;
}

CallResult CallHandle::get()
{
    std::unique_lock<std::mutex> locker(_mutex);
    _cv.wait(locker, [this] { return _ready; });
    return _result;
}

const CallResult &CallHandle::result() const
{
    std::lock_guard<std::mutex> locker(_mutex);
    return _result;
}

void CallHandle::complete(CallResult result)
{
    {
        std::lock_guard<std::mutex> locker(_mutex);
        _result = std::move(result);
        _ready = true;
    }
    _cv.notify_all();
}
}
