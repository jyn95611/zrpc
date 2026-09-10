#include "Message.h"

#include <cstring>

namespace zrpc {
namespace {
constexpr uint32_t kMaxWireStringLen = 64u * 1024u * 1024u;

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

    if (len > kMaxWireStringLen || static_cast<size_t>(end - p) < len) {
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

} // namespace

zmq::message_t RpcRequest::serialize()
{
    const size_t total = encodedStringSize(serviceName)
                       + encodedStringSize(methodName)
                       + encodedStringSize(data);
    return serializeMessage(total, [&](char *p) {
        p = writeString(p, serviceName);
        p = writeString(p, methodName);
        writeString(p, data);
    });
}

bool RpcRequest::deserialize(zmq::message_t &msg)
{
    return deserializeMessage(msg, [&](const char *&p, const char *end) {
        return readString(p, end, serviceName)
            && readString(p, end, methodName)
            && readString(p, end, data);
    });
}

zmq::message_t RpcReply::serialize()
{
    const size_t total = sizeof(int)
                       + encodedStringSize(errorMsg)
                       + encodedStringSize(data);
    return serializeMessage(total, [&](char *p) {
        p = writeInt(p, errorCode);
        p = writeString(p, errorMsg);
        writeString(p, data);
    });
}

bool RpcReply::deserialize(zmq::message_t &msg)
{
    return deserializeMessage(msg, [&](const char *&p, const char *end) {
        return readInt(p, end, errorCode)
            && readString(p, end, errorMsg)
            && readString(p, end, data);
    });
}

zmq::message_t RpcTopic::serialize()
{
    const size_t total = encodedStringSize(topic) + encodedStringSize(data);
    return serializeMessage(total, [&](char *p) {
        p = writeString(p, topic);
        writeString(p, data);
    });
}

bool RpcTopic::deserialize(zmq::message_t &msg)
{
    return deserializeMessage(msg, [&](const char *&p, const char *end) {
        return readString(p, end, topic) && readString(p, end, data);
    });
}
}
