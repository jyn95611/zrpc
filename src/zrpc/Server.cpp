#include <stdexcept>
#include <spdlog/spdlog.h>

#include "utils.h"
#include "Event.h"
#include "Message.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "Server.h"

namespace zrpc {
void Service::addMethod(const std::string &name, const Method &method)
{
    _methods.insert_or_assign(name, method);
}

std::optional<Service::Method> Service::findMethod(const std::string &name) const
{
    const auto iter = _methods.find(name);
    if (iter == _methods.end()) {
        return std::nullopt;
    }
    return iter->second;
}

class ServerPrivate
{
public:
    explicit ServerPrivate(std::shared_ptr<Context> actx)
        : ctx(std::move(actx))
        , dealer(detail::ContextAccess::get(ctx)->createFrontendSocket())
    {}
    ~ServerPrivate()
    {
        close();
        wait();
        if (dealer)
            dealer->close();
    }

    void bind(const std::string &addr)
    {
        if (listening)
            throw std::runtime_error("already bound");

        wait();

        auto *event = new AddServerEvent;
        event->serverAddr = addr;
        event->serverFunc = [this](RpcMessage &request, RpcMessage &reply) {
            processRequest(request, reply);
        };
        DealerSender(*dealer).sendPtr(event, 0);
        socketId = recvSocketEventAck(*dealer, "bind");
        listening = true;
        pendingWait = false;
        spdlog::info("server bound, addr={} socketId={}", addr, socketId);
    }

    void close()
    {
        if (!listening)
            return;

        auto *event = new RemoveServerEvent;
        event->socketId = socketId;
        DealerSender(*dealer).sendPtr(event, 0);
        recvEventAck(*dealer);
        listening = false;
        pendingWait = true;
    }

    void wait()
    {
        if (listening)
            close();
        if (!pendingWait)
            return;
        
        recvEventAck(*dealer);
        pendingWait = false;
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

        const auto iter = services.find(req->serviceName); // find only; unknown names are not inserted.
        if (iter == services.end() || !iter->second) {
            reply = errorReply(ErrorCode::NoSuchService, req->requestId, "No such service.");
            return;
        }

        auto *service = iter->second.get();
        const auto method = service->findMethod(req->methodName);
        if (!method) {
            reply = errorReply(ErrorCode::NoSuchMethod, req->requestId, "No such method.");
            return;
        }

        Payload out;
        (*method)(viewPayload(request), out);
        reply.header = RpcReplyHeader(req->requestId, ErrorCode::Ok, {}).serialize();
        reply.parts = takePayload(std::move(out));
    }

    std::shared_ptr<Context> ctx;
    std::unique_ptr<zmq::socket_t> dealer;
    std::unordered_map<std::string, std::unique_ptr<Service>> services;
    uint64_t socketId{0};
    bool listening{false};
    bool pendingWait{false};
};

Server::Server(const std::shared_ptr<Context> &ctx)
    : _d(std::make_unique<ServerPrivate>(ctx))
{
}

Server::~Server() = default;

void Server::registerService(std::unique_ptr<Service> service)
{
    if (service) {
        auto name = service->name();
        _d->services.insert_or_assign(std::move(name), std::move(service));
    }
}

void Server::bind(const std::string &addr)
{
    _d->bind(addr);
}

void Server::close()
{
    _d->close();
}

void Server::wait()
{
    _d->wait();
}
}
