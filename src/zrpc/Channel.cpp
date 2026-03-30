#include "utils.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "Message.h"
#include "Channel.h"

namespace zrpc {
class ChannelPrivate
{
public:
    ChannelPrivate(const std::shared_ptr<Context> &actx) : ctx(actx) {}
    ~ChannelPrivate()
    {
        if (socketId > 0) {
            ctx->d()->removeClient(socketId);
        }
    }

    void connect(const std::string &addr)
    {
        socketId = ctx->d()->addClient(addr);
    }

    std::shared_ptr<Context> ctx;
    uint64_t socketId{0};
};

Channel::Channel(const std::shared_ptr<Context> &ctx)
    : _d(new ChannelPrivate(ctx))
{
}

Channel::~Channel()
{
    delete _d;
}

void Channel::connect(const std::string &addr)
{
    _d->connect(addr);
}

std::shared_ptr<Context> Channel::context() const { return _d->ctx; }

uint64_t Channel::socketId() const { return _d->socketId; }

class StubPrivate
{
public:
    StubPrivate(const std::shared_ptr<Channel> &achannel)
        : channel(achannel), dealer(channel->context()->d()->createFrontendSocket())
    {
    }
    ~StubPrivate()
    {
        dealer->close();
    }

    std::shared_ptr<Channel> channel;
    std::unique_ptr<zmq::socket_t> dealer;
};

Stub::Stub(const std::shared_ptr<Channel> &channel)
    : _d(new StubPrivate(channel))
{
}

Stub::~Stub()
{
    delete _d;
}

namespace {
struct RpcRequestContext
{
    Rpc *rpc;
    std::string *reply;
};

void processReply(const RpcRequestContext &rpcRequestContext, RpcRequestStatus status,
                  zmq::message_t &replyMsg)
{
    switch (status) {
    case RpcRequestStatus::DEADLINE_EXCEEDED:
        rpcRequestContext.rpc->setStatus(StatusCode::DEADLINE_EXCEEDED);
        break;
    case RpcRequestStatus::DONE:
    {
        RpcReply rpcReply;
        if (!rpcReply.deserialize(replyMsg)) {
            rpcRequestContext.rpc->setError(ApplicationError::INVALID_MESSAGE, "");
            break;
        }

        if (rpcReply.errorCode != 0) {
            rpcRequestContext.rpc->setError(ApplicationError(rpcReply.errorCode), rpcReply.errorMsg);
            break;
        }

        rpcRequestContext.rpc->setStatus(StatusCode::OK);
        *rpcRequestContext.reply = rpcReply.data;
    }
        break;
    case RpcRequestStatus::NACTIVE:
    case RpcRequestStatus::ACTIVE:
    default:
        break;
    }

    rpcRequestContext.rpc->signal();
}
}

void Stub::callMethod(const std::string &serviceName, const std::string &methodName,
                std::string &request, std::string &reply, Rpc *rpc)
{   
    RpcRequest rpcRequest;
    rpcRequest.serviceName = serviceName;
    rpcRequest.methodName = methodName;
    rpcRequest.data = request;
    auto requestMsg = rpcRequest.serialize();

    RpcRequestContext rpcRequestContext {rpc, &reply};

    auto *event = new SendRequestEvent;
    event->clinetSocketId =  _d->channel->socketId();
    event->clientFunc = [rpcRequestContext](RpcRequestStatus status,
                         zmq::message_t &replyMsg){
        processReply(rpcRequestContext, status, replyMsg);
    };
    event->timeoutMs = rpc->timeout();
    event->requestMsg = std::move(requestMsg);
    DealerWriter(*_d->dealer).writePtr(event, zmq::send_flags::none);
}
}
