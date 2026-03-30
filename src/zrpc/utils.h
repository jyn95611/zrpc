#pragma once

#include <zmq.hpp>

namespace zrpc {
inline uint64_t zclock_time()
{
    using namespace std;
    return chrono::time_point_cast<chrono::milliseconds>(
               chrono::system_clock::now()).time_since_epoch().count();
}

class message_iterator {
public:
    explicit message_iterator(zmq::socket_t& socket) :
        _socket(socket), _has_more(true), _more_size(sizeof(_has_more)) { };

    message_iterator(const message_iterator& other) :
        _socket(other._socket),
        _has_more(other._has_more),
        _more_size(other._more_size)
    {
    }

    ~message_iterator()
    {
        while (has_more())
            next();
    }

    bool has_more() const { return _has_more; }

    zmq::message_t& next()
    {
        if (_socket.recv(_message, zmq::recv_flags::none)) {
            _has_more = _socket.get(zmq::sockopt::rcvmore);
        } else {
            _message = zmq::message_t();
            _has_more = false;
        }
        return _message;
    }

private:
    zmq::socket_t& _socket;
    zmq::message_t _message;
    int _has_more;
    size_t _more_size;

    message_iterator& operator=(const message_iterator&);
};

struct SocketWriter
{
    SocketWriter(zmq::socket_t &socket) : _socket(socket) {}

    bool writeEmpty(zmq::send_flags flags = zmq::send_flags::sndmore)
    {
        zmq::message_t message(0);
        return _socket.send(message, flags).has_value();
    }

    template <typename T, typename = typename std::enable_if<std::is_fundamental_v<T>>::type>
    bool write(T val, zmq::send_flags flags = zmq::send_flags::sndmore)
    {
        zmq::message_t msg(sizeof(val));
        memcpy(msg.data(), &val, sizeof(val));
        return _socket.send(msg, flags).has_value();
    }

    bool writeString(const std::string &str, zmq::send_flags flags = zmq::send_flags::sndmore)
    {
        zmq::message_t msg(str.size());
        str.copy((char*)msg.data(), str.size(), 0);
        return _socket.send(msg, flags).has_value();
    }   

    bool writePtr(void *ptr, zmq::send_flags flags = zmq::send_flags::sndmore)
    {
        zmq::message_t msg(sizeof(ptr));
        memcpy(msg.data(), &ptr, sizeof(ptr));
        return _socket.send(msg, flags).has_value();
    }

    bool writeMessage(zmq::message_t &msg, zmq::send_flags flags = zmq::send_flags::sndmore)
    {
        return _socket.send(msg, flags).has_value();
    }

protected:
    zmq::socket_t &_socket;
};

struct DealerWriter : public SocketWriter
{
    DealerWriter(zmq::socket_t &socket) : SocketWriter(socket)
    {
        writeEmpty();
    }
};

struct RouterWriter: public SocketWriter
{
    RouterWriter(zmq::socket_t &socket, const std::string &routeId) : SocketWriter(socket)
    {
        writeString(routeId);
        writeEmpty();        
    }
};

struct SocketdReader
{
    SocketdReader(zmq::socket_t &socket) : _iter(socket) {}    

    template <typename T, typename = typename std::enable_if<std::is_fundamental_v<T>>::type>
    T read()
    {
        zmq::message_t msg = std::move(_iter.next());
        T val;
        val = *static_cast<T*>(msg.data());
        return val;
    }

    std::string readString()
    {
        zmq::message_t msg = std::move(_iter.next());
        return std::string((char*)msg.data(), msg.size());
    }

    template <typename T>
    std::unique_ptr<T> readPtr()
    {
        zmq::message_t msg = std::move(_iter.next());
        T *ptr = nullptr;
        memcpy(&ptr, msg.data(), sizeof(ptr));
        return std::unique_ptr<T>(ptr);
    }

    zmq::message_t readMessage()
    {
        return std::move(_iter.next());
    }

    bool hasMore() const { return _iter.has_more(); }

    bool error() const { return _error; }

protected:
    bool _error{false};
    message_iterator _iter;
};

struct DealerReader : public SocketdReader
{
    DealerReader(zmq::socket_t &socket) : SocketdReader(socket)
    {
        if (!_iter.next().empty())
            _error = true;
    }
};

struct RouterReader : public SocketdReader
{
    RouterReader(zmq::socket_t &socket) : SocketdReader(socket)
    {
        routerId = readString();
        if (!_iter.next().empty())
            _error = true;
    }

    std::string routerId;
};
}
