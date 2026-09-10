#pragma once

#include <string>
#include <zmq.hpp>

namespace zrpc {
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
