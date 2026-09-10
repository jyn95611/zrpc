#include "utils.h"
#include "ContextAccess.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "Message.h"
#include "Event.h"
#include "Channel.h"

namespace zrpc {
class ChannelPrivate
{
public:
    ChannelPrivate(const std::shared_ptr<Context> &actx) : ctx(actx) {}
    ~ChannelPrivate()
    {
        if (socketId > 0) {
            detail::ContextAccess::get(ctx)->removeClient(socketId);
        }
    }

    void connect(const std::string &addr)
    {
        socketId = detail::ContextAccess::get(ctx)->addClient(addr);
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

namespace {
CallResult buildCallResult(RpcRequestStatus status, zmq::message_t &replyMsg)
{
    if (status == RpcRequestStatus::DeadlineExceeded) {
        return {ErrorCode::Timeout, "timeout", {}};
    }

    if (status != RpcRequestStatus::Done) {
        return {ErrorCode::InvalidMessage, "invalid request status", {}};
    }

    RpcReply rpcReply;
    if (!rpcReply.deserialize(replyMsg)) {
        return {ErrorCode::InvalidMessage, "", {}};
    }

    if (rpcReply.errorCode != 0) {
        return {static_cast<ErrorCode>(rpcReply.errorCode), rpcReply.errorMsg, {}};
    }

    return {ErrorCode::Ok, {}, std::move(rpcReply.data)};
}
}

class StubPrivate
{
public:
    StubPrivate(const std::shared_ptr<Channel> &achannel)
        : channel(achannel), dealer(detail::ContextAccess::get(channel->context())->createFrontendSocket())
    {
    }
    ~StubPrivate()
    {
        dealer->close();
    }

    std::shared_ptr<CallHandle> call(const std::string &serviceName,
                                     const std::string &methodName,
                                     std::string &request,
                                     const CallOptions &opts,
                                     CompletionCallback onComplete)
    {
        if (_currentCall && !_currentCall->ready()) {
            return nullptr;
        }

        RpcRequest rpcRequest;
        rpcRequest.serviceName = serviceName;
        rpcRequest.methodName = methodName;
        rpcRequest.data = std::move(request);

        auto handle = std::make_shared<CallHandle>();
        _currentCall = handle;

        auto *event = new SendRequestEvent;
        event->clientSocketId = channel->socketId();
        event->timeoutMs = opts.timeoutMs;
        event->requestMsg = rpcRequest.serialize();
        event->clientFunc = [handle, onComplete = std::move(onComplete), this](
                                RpcRequestStatus status, zmq::message_t &replyMsg) {
            CallResult result = buildCallResult(status, replyMsg);
            handle->complete(result);
            if (onComplete) {
                onComplete(result);
            }
            _currentCall.reset();
        };

        DealerWriter(*dealer).writePtr(event, zmq::send_flags::none);
        return handle;
    }

    std::shared_ptr<Channel> channel;
    std::unique_ptr<zmq::socket_t> dealer;
    std::shared_ptr<CallHandle> _currentCall;
};

Stub::Stub(const std::shared_ptr<Channel> &channel)
    : _d(new StubPrivate(channel))
{
}

Stub::~Stub()
{
    delete _d;
}

CallResult Stub::callMethod(const std::string &serviceName, const std::string &methodName,
                            std::string &request, const CallOptions &opts)
{
    auto handle = _d->call(serviceName, methodName, request, opts, {});
    if (!handle) {
        return {ErrorCode::CallInProcess, "call in progress", {}};
    }
    return handle->get();
}

std::shared_ptr<CallHandle> Stub::callMethodAsync(const std::string &serviceName,
                                                  const std::string &methodName,
                                                  std::string &request,
                                                  const CallOptions &opts,
                                                  CompletionCallback onComplete)
{
    return _d->call(serviceName, methodName, request, opts, std::move(onComplete));
}
}
