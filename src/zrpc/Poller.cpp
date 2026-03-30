#include <iostream>
#include <thread>
#include "utils.h"
#include "Poller.h"

namespace zrpc {
Poller::Poller()
{
}

void Poller::addSocket(zmq::socket_t *socket, const Callback &func)
{    
    std::cout << "Poller add socket: " << socket << std::endl;
    _sockets.emplace_back(socket, func);
    _dirty = true;
}

void Poller::removeSocket(zmq::socket_t *socket)
{
    for (auto iter = _sockets.begin(); iter != _sockets.end(); ++iter) {
        if (iter->first == socket) {
            _sockets.erase(iter);
            _dirty = true;
            return;
        }
    }
}

void Poller::runCallbackAt(uint64_t time, const Callback &func)
{
    _timerCbs[time].emplace_back(func);
}

void Poller::postCallback(const Callback &func)
{
    _postCbs.emplace_back(func);
}

int Poller::loop()
{
    while (!_quit) {        
        if (_dirty) {
            rebuildPollitems();
            _dirty = false;
        }                

        const long timeout = processTimerTimeout();
        int rc = zmq_poll(&_pollitems[0], _pollitems.size(), timeout);

        if (rc == -1) {
            int zmq_err = zmq_errno();
            if (zmq_err == ETERM) {
                return -1;
            }
        }
        for (size_t i = 0; i < _pollitems.size(); ++i) {
            if (!(_pollitems[i].revents & ZMQ_POLLIN)) {
                continue;
            }
            _pollitems[i].revents = 0;
            _sockets[i].second();
        }

        if (_quit)
            break;

        std::vector<Callback> postCbs;
        postCbs.swap(_postCbs);
        for (auto &cb : postCbs) {
            cb();
        }
    }
    return 0;
}

void Poller::quit()
{
    _quit = true;
}

void Poller::rebuildPollitems()
{
    _pollitems.resize(_sockets.size());
    for (int i = 0; i < _sockets.size(); ++i) {
        _pollitems[i] = zmq::pollitem_t{_sockets[i].first->handle(), 0, ZMQ_POLLIN, 0};
    }
}

uint64_t Poller::processTimerTimeout()
{
    const uint64_t now = zclock_time();
    const auto ubIter(_timerCbs.upper_bound(now));
    for (auto iter = _timerCbs.begin(); iter != ubIter; ++iter) {
        for (auto vit = iter->second.begin(); vit != iter->second.end(); ++vit) {
            (*vit)();
        }
    }
    long pollTimeout = -1;
    if (ubIter != _timerCbs.end()) {
        pollTimeout = ubIter->first - now;
    }
    _timerCbs.erase(_timerCbs.begin(), ubIter);
    return pollTimeout;
}
}
