#include "Rpc.h"

namespace zrpc {
void Rpc::setError(ApplicationError error, const std::string &errorMsg)
{
    setStatus(StatusCode::APPLICATION_ERROR);
    _appError = error;
    _appErrorMsg = errorMsg;
}

int Rpc::wait()
{
    if (status() != StatusCode::INACTIVE) {
        std::cout << "Request must be sent before calling wait()" << std::endl;
        return -1;
    }

    std::unique_lock<std::mutex> locker(_mutex);
    _cv.wait(locker, [this]{ return _ready; });

    return 0;
}

void Rpc::signal()
{
    std::unique_lock<std::mutex> locker(_mutex);
    _ready = true;
    locker.unlock();
    _cv.notify_all();
}
}
