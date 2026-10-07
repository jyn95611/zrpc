#pragma once

#include <map>
#include <zmq.hpp>

namespace zrpc {
/**
 * @brief Single-threaded reactor: socket poll items, monotonic timers, deferred callbacks.
 */
class Poller
{
public:
    Poller();

    using Callback = std::function<void()>;
    void addSocket(zmq::socket_t *socket, const Callback &func);
    void removeSocket(zmq::socket_t *socket);

    /**
     * Schedule @p func at an absolute @c steady_clock millisecond timestamp.
     * RPC deadlines use @ref runCallbackAfter.
     */
    void runCallbackAt(uint64_t time, const Callback &func);
    /** Schedule @p func after @p time milliseconds on the monotonic clock. */
    void runCallbackAfter(uint64_t time, const Callback &func);
    /** Run @p func after the current poll-item callbacks return (safe to unbind sockets). */
    void postCallback(const Callback &func);

    int loop();
    void quit();

private:
    void rebuildPollitems();
    uint64_t processTimerTimeout();

private:
    bool _quit{false};
    bool _dirty{false};
    std::vector<std::pair<zmq::socket_t*, Callback>> _sockets;
    std::vector<zmq::pollitem_t> _pollitems;
    std::map<uint64_t, std::vector<Callback>> _timerCbs;
    std::vector<Callback> _postCbs;
};
}
