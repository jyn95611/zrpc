#include "Message.h"

namespace zrpc {
Serializer &Serializer::operator<<(const std::string &str)
{
    if (str.empty()) {
        *this << 0;
        return *this;
    }

    uint32_t len = str.size();
    *this << len;
    _ss.write(str.data(), len);
    return *this;
}

Deserializer &Deserializer::operator>>(std::string &str)
{
    uint32_t len = 0;
    *this >> len;
    if (len != 0) {
        str.resize(len);
        _ss.read(str.data(), len);
    }
    return *this;
}

zmq::message_t RpcRequest::serialize()
{
    Serializer serializer;
    serializer << serviceName;
    serializer << methodName;
    serializer << data;
    const auto str = serializer.str();
    return {str.data(), str.size()};
}

bool RpcRequest::deserialize(zmq::message_t &msg)
{
    Deserializer deserializer(msg.to_string());
    deserializer >> serviceName;
    deserializer >> methodName;
    deserializer >> data;
    return true;
}

zmq::message_t RpcReply::serialize()
{
    Serializer serializer;
    serializer << errorCode;
    serializer << errorMsg;
    serializer << data;
    const auto str = serializer.str();
    return {str.data(), str.size()};
}

bool RpcReply::deserialize(zmq::message_t &msg)
{
    Deserializer deserializer(msg.to_string());
    deserializer >> errorCode;
    deserializer >> errorMsg;
    deserializer >> data;
    return true;
}

zmq::message_t RpcTopic::serialize()
{
    Serializer serializer;
    serializer << topic;
    serializer << data;
    const auto str = serializer.str();
    return {str.data(), str.size()};
}

bool RpcTopic::deserialize(zmq::message_t &msg)
{
    Deserializer deserializer(msg.to_string());
    deserializer >> topic;
    deserializer >> data;
    return true;
}
}
