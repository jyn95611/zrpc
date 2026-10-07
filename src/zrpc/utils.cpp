#include <cctype>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string_view>

#include <spdlog/spdlog.h>

#include "utils.h"

namespace zrpc {

std::string normalizeZmqAddr(const std::string &addr)
{
    constexpr std::string_view host = "localhost";
    const auto schemeEnd = addr.find("://");
    if (schemeEnd == std::string::npos)
        return addr;

    const auto hostBegin = schemeEnd + 3;
    if (hostBegin >= addr.size())
        return addr;

    const auto hostEnd = addr.find_first_of(":/", hostBegin);
    const auto hostLen = (hostEnd == std::string::npos ? addr.size() : hostEnd) - hostBegin;
    if (hostLen != host.size())
        return addr;

    for (size_t i = 0; i < host.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(addr[hostBegin + i])) != host[i])
            return addr;
    }

    std::string normalizedAddr(addr);
    normalizedAddr.replace(hostBegin, host.size(), "127.0.0.1");
    return normalizedAddr;
}

FrameReceiver::Iterator::~Iterator()
{
    while (has_more())
        next();
}

zmq::message_t& FrameReceiver::Iterator::next()
{
    if (_socket.recv(&_message, 0)) {
        _has_more = _socket.getsockopt<int>(ZMQ_RCVMORE);
    } else {
        _message = zmq::message_t();
        _has_more = false;
    }
    return _message;
}

bool FrameSender::sendEmpty(int flags)
{
    zmq::message_t message(0);
    return sendFrame(message, flags);
}

bool FrameSender::sendString(const std::string &str, int flags)
{
    zmq::message_t msg(str.size());
    str.copy((char*)msg.data(), str.size(), 0);
    return sendFrame(msg, flags);
}

bool FrameSender::sendPtr(void *ptr, int flags)
{
    zmq::message_t msg(sizeof(ptr));
    memcpy(msg.data(), &ptr, sizeof(ptr));
    return sendFrame(msg, flags);
}

bool FrameSender::sendMessage(zmq::message_t &msg, int flags)
{
    return sendFrame(msg, flags);
}

bool FrameSender::sendFrame(zmq::message_t &msg, int flags)
{
    if (_error)
        return false;
    _error = !_socket.send(msg, flags);
    return !_error;
}

DealerSender::DealerSender(zmq::socket_t &socket) : FrameSender(socket)
{
    sendEmpty();
}

RouterSender::RouterSender(zmq::socket_t &socket, const std::string &routerId) : FrameSender(socket)
{
    sendString(routerId);
    sendEmpty();
}

std::string FrameReceiver::recvString()
{
    zmq::message_t msg = std::move(_iter.next());
    return std::string((char*)msg.data(), msg.size());
}

zmq::message_t FrameReceiver::recvMessage()
{
    return std::move(_iter.next());
}

DealerReceiver::DealerReceiver(zmq::socket_t &socket) : FrameReceiver(socket)
{
    if (_iter.next().size() != 0)
        _error = true;
}

RouterReceiver::RouterReceiver(zmq::socket_t &socket) : FrameReceiver(socket)
{
    routerId = recvString();
    if (_iter.next().size() != 0)
        _error = true;
}

std::string currentExceptionMessage()
{
    try {
        throw;
    } catch (const zmq::error_t &e) {
        return e.what();
    } catch (const std::exception &e) {
        return e.what();
    } catch (...) {
        return "unknown error";
    }
}

void sendSocketEventAck(zmq::socket_t &backend, const std::string &routerId,
                        uint64_t socketId, const std::string &error)
{
    RouterSender sender(backend, routerId);
    sender.send(socketId);
    sender.sendString(error, 0);
}

uint64_t recvSocketEventAck(zmq::socket_t &frontend, const char *action)
{
    DealerReceiver receiver(frontend);
    const auto socketId = receiver.recv<uint64_t>();
    const auto error = receiver.recvString();
    if (socketId == 0) {
        const auto what = error.empty() ? std::string(action) + " failed" : error;
        spdlog::error("{} failed: {}", action, what);
        throw std::runtime_error(what);
    }
    return socketId;
}

void recvEventAck(zmq::socket_t &frontend)
{
    DealerReceiver receiver(frontend);
    receiver.recv<uint64_t>();
    receiver.recvString();
}

}
