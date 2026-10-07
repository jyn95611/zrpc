#include <cstring>

#include "binser.h"
#include "utils.h"
#include "Message.h"

namespace zrpc {
namespace {
template <class... Ts>
zmq::message_t serializeToMessage(const Ts &...xs)
{
    binser::Counter counter;
    counter(xs...);
    if (!counter.ok()) {
        return {};
    }
    zmq::message_t msg(counter.size());
    binser::Serializer ser(static_cast<char *>(msg.data()));
    ser(xs...);
    return msg;
}

template <class... Ts>
bool deserializeFromMessage(const zmq::message_t &msg, Ts &...xs)
{
    binser::Deserializer ser(static_cast<const char *>(msg.data()), msg.size());
    ser(xs...);
    return ser.exact();
}

void drainRemaining(FrameReceiver &receiver)
{
    while (receiver.hasMore()) {
        receiver.recvMessage();
    }
}

} // namespace

uint64_t peekRequestId(const zmq::message_t &header)
{
    if (header.size() < sizeof(uint64_t)) {
        return 0;
    }
    uint64_t requestId = 0;
    std::memcpy(&requestId, header.data(), sizeof(requestId));
    return requestId;
}

zmq::message_t takeMessage(std::string &&s)
{
    if (s.empty()) {
        return {};
    }
    if (s.size() <= kZeroCopyThreshold) { // Small frames memcpy to avoid the zero-copy heap hint.
        zmq::message_t msg(s.size());
        std::memcpy(msg.data(), s.data(), s.size());
        return msg;
    }
    auto *owned = new std::string(std::move(s));
    return zmq::message_t(owned->data(), owned->size(),
                          [](void *, void *hint) {
                              delete static_cast<std::string *>(hint);
                          },
                          owned);
}

std::vector<zmq::message_t> takePayload(Payload &&payload)
{
    std::vector<zmq::message_t> parts;
    parts.reserve(payload.size());
    for (auto &s : payload) {
        parts.push_back(takeMessage(std::move(s)));
    }
    return parts;
}

std::string_view viewMessage(const zmq::message_t &msg)
{
    return {static_cast<const char *>(msg.data()), msg.size()};
}

PayloadView viewPayload(const RpcMessage &msg, std::shared_ptr<void> owned)
{
    PayloadView out;
    out.owned = std::move(owned);
    out.views.reserve(msg.parts.size());
    for (const auto &part : msg.parts) {
        out.views.push_back(viewMessage(part));
    }
    return out;
}

bool sendRpcMessage(FrameSender &sender, RpcMessage &msg)
{
    const int headerFlags = msg.parts.empty() ? 0 : ZMQ_SNDMORE;
    if (!sender.sendMessage(msg.header, headerFlags))
        return false;
    for (size_t i = 0; i < msg.parts.size(); ++i) {
        const int flags = (i + 1 == msg.parts.size()) ? 0 : ZMQ_SNDMORE;
        if (!sender.sendMessage(msg.parts[i], flags))
            return false;
    }
    return true;
}

RpcMessage recvRpcMessage(FrameReceiver &receiver)
{
    RpcMessage msg;
    msg.header = receiver.recvMessage();
    if (msg.header.size() > kMaxPartBytes) {
        msg.valid = false;
        drainRemaining(receiver);
        return msg;
    }

    while (receiver.hasMore()) {
        auto part = receiver.recvMessage();
        if (!msg.valid) {
            continue;
        }
        if (part.size() > kMaxPartBytes) {
            msg.valid = false;
            msg.parts.clear();
            continue;
        }
        msg.parts.push_back(std::move(part));
    }
    return msg;
}

RpcMessage errorReply(ErrorCode code, uint64_t requestId, std::string errorMsg)
{
    RpcMessage msg;
    msg.header = RpcReplyHeader(requestId, code, std::move(errorMsg)).serialize();
    return msg;
}

zmq::message_t RpcRequestHeader::serialize() const
{
    return serializeToMessage(requestId, serviceName, methodName);
}

std::optional<RpcRequestHeader> RpcRequestHeader::deserialize(zmq::message_t &msg)
{
    uint64_t requestId = 0;
    std::string serviceName;
    std::string methodName;
    if (!deserializeFromMessage(msg, requestId, serviceName, methodName)) {
        return std::nullopt;
    }
    return RpcRequestHeader(requestId, std::move(serviceName), std::move(methodName));
}

zmq::message_t RpcReplyHeader::serialize() const
{
    return serializeToMessage(requestId, errorCode, errorMsg);
}

std::optional<RpcReplyHeader> RpcReplyHeader::deserialize(zmq::message_t &msg)
{
    uint64_t requestId = 0;
    ErrorCode errorCode = ErrorCode::Ok;
    std::string errorMsg;
    if (!deserializeFromMessage(msg, requestId, errorCode, errorMsg)) {
        return std::nullopt;
    }
    return RpcReplyHeader(requestId, errorCode, std::move(errorMsg));
}
}
