#include <iostream>

#include "Rpc.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "Message.h"
#include "Server.h"

namespace zrpc {
void Service::addMethod(const std::string &name, const Method &method)
{
    _methods.insert_or_assign(name, method);
}

const Service::Method &Service::findMethod(const std::string &name)
{
    const auto iter = _methods.find(name);
    static Method emptyMethod;
    return iter != _methods.end() ? iter->second : emptyMethod;
}

class ServerPrivate
{
public:
    explicit ServerPrivate(std::shared_ptr<Context> actx)
        : ctx(actx)
    {}
    ~ServerPrivate()
    {
        if (socketId > 0)
            ctx->d()->removeServer(socketId);
    }

    void processRequest(zmq::message_t &requestMsg, zmq::message_t &replyMsg)
    {
        RpcReply rpcReply;

        RpcRequest rpcRequest;
        if (!rpcRequest.deserialize(requestMsg)) {
            rpcReply.errorCode = int(ApplicationError::INVALID_MESSAGE);
            replyMsg = std::move(rpcReply.serialize());
            return;
        }

//        std::cout << "Server recv service name: " << rpcRequest.serviceName << std::endl;
//        std::cout << "Server recv method name: " << rpcRequest.methodName << std::endl;

        auto *service = services[rpcRequest.serviceName];
        if (service) {
            auto &method = service->findMethod(rpcRequest.methodName);
            if (method) {
                method(rpcRequest.data, rpcReply.data);
            } else {
                rpcReply.errorCode = int(ApplicationError::NO_SUCH_METHOD);
                rpcReply.errorMsg = "No such method.";
            }
        } else {
            rpcReply.errorCode = int(ApplicationError::NO_SUCH_SERVICE);
            rpcReply.errorMsg = "No such service.";
        }
        replyMsg = std::move(rpcReply.serialize());
    }

    std::shared_ptr<Context> ctx;
    std::unordered_map<std::string, Service*> services;
    uint64_t socketId{0};
};

Server::Server(const std::shared_ptr<Context> &ctx)
    : _d(new ServerPrivate(ctx))
{
}

Server::~Server()
{
    delete _d;
}

void Server::registService(Service *service)
{
    _d->services.insert_or_assign(service->name(), service);
}

void Server::bind(const std::string &addr)
{
    _d->socketId = _d->ctx->d()->addServer(addr, [this](zmq::message_t &requestMsg, zmq::message_t &replyMsg){
        _d->processRequest(requestMsg, replyMsg);
    });
}
}
