#pragma once

#include "zrpc_global.h"

namespace zrpc {
class ContextPrivate;

namespace detail {
struct ContextAccess;
}

class ZRPC_EXPORT Context final
{
public:
    Context(int ioThrNum = 4, int workerThrNum = 8);
    ~Context();

    void quit();
    void wait();

private:
    friend struct detail::ContextAccess;

    ContextPrivate *_d = nullptr;
};
}
