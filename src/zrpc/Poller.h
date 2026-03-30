#pragma once

#include <map>
#include <zmq.hpp>

namespace zrpc {
class Poller
{
public:
    Poller();

    using Callback = std::function<void()>;
    void addSocket(zmq::socket_t *socket, const Callback &func);
    void removeSocket(zmq::socket_t *socket);

    void runCallbackAt(uint64_t time, const Callback &func);
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
