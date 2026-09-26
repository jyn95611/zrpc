#include <iostream>

#include "Call.h"
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
            detail::ContextAccess::get(ctx)->removeServer(socketId);
    }

    void processRequest(RpcMessage &request, RpcMessage &reply)
    {
        if (!request.valid) {
            reply = errorReply(ErrorCode::InvalidMessage, peekRequestId(request.header));
            return;
        }

        const auto req = RpcRequestHeader::deserialize(request.header);
        if (!req) {
            reply = errorReply(ErrorCode::InvalidMessage, peekRequestId(request.header));
            return;
        }

        auto *service = services[req->serviceName];
        if (!service) {
            reply = errorReply(ErrorCode::NoSuchService, req->requestId, "No such service.");
            return;
        }

        auto &method = service->findMethod(req->methodName);
        if (!method) {
            reply = errorReply(ErrorCode::NoSuchMethod, req->requestId, "No such method.");
            return;
        }

        Payload out;
        method(viewPayload(request), out);
        reply.header = RpcReplyHeader(req->requestId, ErrorCode::Ok, {}).serialize();
        reply.parts = takePayload(std::move(out));
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

void Server::registerService(Service *service)
{
    _d->services.insert_or_assign(service->name(), service);
}

void Server::bind(const std::string &addr)
{
    _d->socketId = detail::ContextAccess::get(_d->ctx)->addServer(addr, [this](RpcMessage &request, RpcMessage &reply){
        _d->processRequest(request, reply);
    });
}
}
