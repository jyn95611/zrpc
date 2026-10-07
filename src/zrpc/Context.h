#pragma once

#include <memory>

#include "zrpc_global.h"

namespace zrpc {
class ContextPrivate;

namespace detail {
struct ContextAccess;
}

/**
 * @brief Owns a @c zmq::context_t, one poller thread, and a worker pool.
 *
 * Must outlive every @ref Client, @ref Server, @ref Publisher, and
 * @ref Subscriber created from it. Destroy those objects first, then the context.
 */
class ZRPC_EXPORT Context final
{
public:
    /**
     * @param ioThrNum ZeroMQ I/O threads. Values @c <= 0 become 1.
     * @param workerThrNum Threads that run methods and callbacks. Values @c <= 0 become 1.
     */
    Context(int ioThrNum = 4, int workerThrNum = 8);
    ~Context();

    /**
     * Stop the poller and workers.
     *
     * Pending RPCs are not completed. Callers blocked in @ref CallHandle::get
     * or @ref Server::wait wait forever.
     */
    void quit();

    /** Join the poller and worker threads. */
    void wait();

private:
    friend struct detail::ContextAccess;

    std::unique_ptr<ContextPrivate> _d;
};
}
