#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <zmq.hpp>

namespace zrpc {
/** Monotonic milliseconds for poller timers (unaffected by NTP / wall-clock jumps). */
inline uint64_t zclock_time()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        steady_clock::now().time_since_epoch()).count();
}

/** Rewrite @c localhost in a ZMQ URL to @c 127.0.0.1 so the socket stays on IPv4. */
std::string normalizeZmqAddr(const std::string &addr);

struct FrameSender
{
    FrameSender(zmq::socket_t &socket) : _socket(socket) {}

    bool error() const { return _error; }

    bool sendEmpty(int flags = ZMQ_SNDMORE);

    template <typename T, typename = typename std::enable_if<std::is_fundamental_v<T>>::type>
    bool send(T val, int flags = ZMQ_SNDMORE)
    {
        zmq::message_t msg(sizeof(val));
        memcpy(msg.data(), &val, sizeof(val));
        return sendFrame(msg, flags);
    }

    bool sendString(const std::string &str, int flags = ZMQ_SNDMORE);

    /** Post an in-process pointer (typically an @c Event*). */
    bool sendPtr(void *ptr, int flags = ZMQ_SNDMORE);

    bool sendMessage(zmq::message_t &msg, int flags = ZMQ_SNDMORE);

protected:
    bool sendFrame(zmq::message_t &msg, int flags);

    zmq::socket_t &_socket;
    bool _error{false};
};

struct DealerSender : public FrameSender
{
    DealerSender(zmq::socket_t &socket);
};

struct RouterSender : public FrameSender
{
    RouterSender(zmq::socket_t &socket, const std::string &routerId);
};

struct FrameReceiver
{
    FrameReceiver(zmq::socket_t &socket) : _iter(socket) {}

    template <typename T, typename = typename std::enable_if<std::is_fundamental_v<T>>::type>
    T recv()
    {
        zmq::message_t msg = std::move(_iter.next());
        T val;
        val = *static_cast<T*>(msg.data());
        return val;
    }

    std::string recvString();

    template <typename T>
    std::unique_ptr<T> recvPtr()
    {
        zmq::message_t msg = std::move(_iter.next());
        T *ptr = nullptr;
        memcpy(&ptr, msg.data(), sizeof(ptr));
        return std::unique_ptr<T>(ptr);
    }

    zmq::message_t recvMessage();

    bool hasMore() const { return _iter.has_more(); }

    bool error() const { return _error; }

protected:
    class Iterator {
    public:
        explicit Iterator(zmq::socket_t& socket) :
            _socket(socket), _has_more(true), _more_size(sizeof(_has_more)) { };

        Iterator(const Iterator& other) :
            _socket(other._socket),
            _has_more(other._has_more),
            _more_size(other._more_size)
        {
        }

        ~Iterator();

        bool has_more() const { return _has_more; }

        zmq::message_t& next();

    private:
        zmq::socket_t& _socket;
        zmq::message_t _message;
        int _has_more;
        size_t _more_size;

        Iterator& operator=(const Iterator&);
    };

    bool _error{false};
    Iterator _iter;
};

struct DealerReceiver : public FrameReceiver
{
    DealerReceiver(zmq::socket_t &socket);
};

struct RouterReceiver : public FrameReceiver
{
    RouterReceiver(zmq::socket_t &socket);

    std::string routerId;
};

std::string currentExceptionMessage();

void sendSocketEventAck(zmq::socket_t &backend, const std::string &routerId,
                        uint64_t socketId, const std::string &error);

uint64_t recvSocketEventAck(zmq::socket_t &frontend, const char *action);

void recvEventAck(zmq::socket_t &frontend);
}
