#include "Message.h"

#include <cstring>

#include "utils.h"

namespace zrpc {
namespace {
size_t encodedStringSize(const std::string &str)
{
    return sizeof(uint32_t) + str.size();
}

char *writeString(char *p, const std::string &str)
{
    const auto len = static_cast<uint32_t>(str.size());
    std::memcpy(p, &len, sizeof(len));
    p += sizeof(len);
    if (len != 0) {
        std::memcpy(p, str.data(), len);
        p += len;
    }
    return p;
}

char *writeInt(char *p, int v)
{
    std::memcpy(p, &v, sizeof(v));
    return p + sizeof(v);
}

char *writeU64(char *p, uint64_t v)
{
    std::memcpy(p, &v, sizeof(v));
    return p + sizeof(v);
}

bool readString(const char *&p, const char *end, std::string &str)
{
    if (static_cast<size_t>(end - p) < sizeof(uint32_t)) {
        return false;
    }

    uint32_t len = 0;
    std::memcpy(&len, p, sizeof(len));
    p += sizeof(len);

    if (len == 0) {
        str.clear();
        return true;
    }

    if (len > kMaxPartBytes || static_cast<size_t>(end - p) < len) {
        return false;
    }

    str.assign(p, len);
    p += len;
    return true;
}

bool readInt(const char *&p, const char *end, int &v)
{
    if (static_cast<size_t>(end - p) < sizeof(int)) {
        return false;
    }

    std::memcpy(&v, p, sizeof(v));
    p += sizeof(int);
    return true;
}

bool readU64(const char *&p, const char *end, uint64_t &v)
{
    if (static_cast<size_t>(end - p) < sizeof(uint64_t)) {
        return false;
    }

    std::memcpy(&v, p, sizeof(v));
    p += sizeof(uint64_t);
    return true;
}

template<typename Fn>
zmq::message_t serializeMessage(size_t totalSize, Fn &&writeFields)
{
    zmq::message_t msg(totalSize);
    char *p = static_cast<char *>(msg.data());
    writeFields(p);
    return msg;
}

template<typename Fn>
bool deserializeMessage(zmq::message_t &msg, Fn &&readFields)
{
    const char *p = static_cast<const char *>(msg.data());
    const char *end = p + msg.size();
    if (!readFields(p, end)) {
        return false;
    }
    return p == end;
}

void drainRemaining(SocketdReader &reader)
{
    while (reader.hasMore()) {
        reader.readMessage();
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
    if (s.size() <= kZeroCopyThreshold) {
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

void writeRpcMessage(SocketWriter &writer, RpcMessage &msg)
{
    const int headerFlags = msg.parts.empty() ? 0 : ZMQ_SNDMORE;
    writer.writeMessage(msg.header, headerFlags);
    for (size_t i = 0; i < msg.parts.size(); ++i) {
        const int flags = (i + 1 == msg.parts.size()) ? 0 : ZMQ_SNDMORE;
        writer.writeMessage(msg.parts[i], flags);
    }
}

RpcMessage readRpcMessage(SocketdReader &reader)
{
    RpcMessage msg;
    msg.header = reader.readMessage();
    if (msg.header.size() > kMaxPartBytes) {
        msg.valid = false;
        drainRemaining(reader);
        return msg;
    }

    while (reader.hasMore()) {
        auto part = reader.readMessage();
        if (!msg.valid) {
            continue;
        }
        if (msg.parts.size() >= kMaxPartCount || part.size() > kMaxPartBytes) {
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

RpcRequestHeader::RpcRequestHeader(uint64_t requestId, std::string serviceName, std::string methodName)
    : requestId(requestId),
      serviceName(std::move(serviceName)),
      methodName(std::move(methodName))
{
}

zmq::message_t RpcRequestHeader::serialize() const
{
    const size_t total = sizeof(uint64_t)
                       + encodedStringSize(serviceName)
                       + encodedStringSize(methodName);
    return serializeMessage(total, [&](char *p) {
        p = writeU64(p, requestId);
        p = writeString(p, serviceName);
        writeString(p, methodName);
    });
}

std::optional<RpcRequestHeader> RpcRequestHeader::deserialize(zmq::message_t &msg)
{
    uint64_t requestId = 0;
    std::string serviceName;
    std::string methodName;
    if (!deserializeMessage(msg, [&](const char *&p, const char *end) {
            return readU64(p, end, requestId)
                && readString(p, end, serviceName)
                && readString(p, end, methodName);
        })) {
        return std::nullopt;
    }
    return RpcRequestHeader(requestId, std::move(serviceName), std::move(methodName));
}

RpcReplyHeader::RpcReplyHeader(uint64_t requestId, ErrorCode errorCode, std::string errorMsg)
    : requestId(requestId),
      errorCode(errorCode),
      errorMsg(std::move(errorMsg))
{
}

zmq::message_t RpcReplyHeader::serialize() const
{
    const size_t total = sizeof(uint64_t) + sizeof(int) + encodedStringSize(errorMsg);
    return serializeMessage(total, [&](char *p) {
        p = writeU64(p, requestId);
        p = writeInt(p, static_cast<int>(errorCode));
        writeString(p, errorMsg);
    });
}

std::optional<RpcReplyHeader> RpcReplyHeader::deserialize(zmq::message_t &msg)
{
    uint64_t requestId = 0;
    int errorCode = 0;
    std::string errorMsg;
    if (!deserializeMessage(msg, [&](const char *&p, const char *end) {
            return readU64(p, end, requestId)
                && readInt(p, end, errorCode)
                && readString(p, end, errorMsg);
        })) {
        return std::nullopt;
    }
    return RpcReplyHeader(requestId, static_cast<ErrorCode>(errorCode), std::move(errorMsg));
}
}
