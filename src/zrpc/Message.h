#pragma once

#include <sstream>
#include <zmq.hpp>

namespace zrpc {
class Serializer
{
public:
    Serializer() = default;

    std::string str() const { return _ss.str(); }

    void write(void *buf, uint32_t len)
    {
        _ss.write(static_cast<char*>(buf), len);
    }

    template<typename T, typename = typename std::enable_if<std::is_fundamental_v<T>>::type>
    Serializer &operator<<(T v)
    {
        _ss.write(reinterpret_cast<char*>(&v), sizeof(v));
        return *this;
    }
    Serializer &operator<<(const std::string &str);

private:
    std::stringstream _ss;
};

class Deserializer
{
public:
    explicit Deserializer(const std::string &str) : _ss(str) {}

    void read(void *buf, uint32_t len)
    {
        _ss.read(static_cast<char*>(buf), len);
    }

    template<typename T, typename = typename std::enable_if<std::is_fundamental_v<T>>::type>
    Deserializer &operator>>(T &v)
    {
        _ss.read(reinterpret_cast<char*>(&v), sizeof(v));
        return *this;
    }
    Deserializer &operator>>(std::string &str);

private:
    std::stringstream _ss;
};

struct RpcRequest
{
    zmq::message_t serialize();
    bool deserialize(zmq::message_t &msg);

    std::string serviceName;
    std::string methodName;
    std::string data;
};

struct RpcReply
{
    zmq::message_t serialize();
    bool deserialize(zmq::message_t &msg);

    int errorCode{0};
    std::string errorMsg;
    std::string data;
};

struct RpcTopic
{
    zmq::message_t serialize();
    bool deserialize(zmq::message_t &msg);

    std::string topic;
    std::string data;
};
}
