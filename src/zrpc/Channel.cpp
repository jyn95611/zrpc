#include "utils.h"
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
CallResult buildCallResult(RequestStatus status, RpcMessage &reply)
{
    if (status == RequestStatus::Timeout) {
        return {ErrorCode::Timeout, "timeout", {}};
    }

    if (status == RequestStatus::Disconnected) {
        return {ErrorCode::Disconnected, "peer disconnected", {}};
    }

    if (status != RequestStatus::Replied) {
        return {ErrorCode::InvalidMessage, "invalid request status", {}};
    }

    if (!reply.valid) {
        return {ErrorCode::InvalidMessage,
                "invalid reply message: header/part exceeds max size or too many parts", {}};
    }

    const auto header = RpcReplyHeader::deserialize(reply.header);
    if (!header) {
        return {ErrorCode::InvalidMessage, "invalid reply header", {}};
    }

    if (header->errorCode != ErrorCode::Ok) {
        return {header->errorCode, header->errorMsg, {}};
    }

    auto owned = std::make_shared<RpcMessage>(std::move(reply));
    CallResult result;
    result.errorCode = ErrorCode::Ok;
    result.payload = viewPayload(*owned, owned);
    return result;
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
                                     Payload &&request,
                                     const CallOptions &opts,
                                     CompletionCallback onComplete)
    {
        if (_currentCall && !_currentCall->ready()) {
            return nullptr;
        }

        const uint64_t requestId =
            detail::ContextAccess::get(channel->context())->nextRequestId();

        auto handle = std::make_shared<CallHandle>();
        _currentCall = handle;

        auto *event = new SendRequestEvent;
        event->clientSocketId = channel->socketId();
        event->timeoutMs = opts.timeoutMs;
        event->request.header = RpcRequestHeader(requestId, serviceName, methodName).serialize();
        event->request.parts = takePayload(std::move(request));
        event->clientFunc = [handle, onComplete = std::move(onComplete), this](
                                RequestStatus status, RpcMessage &reply) {
            CallResult result = buildCallResult(status, reply);
            handle->complete(result);
            if (onComplete) {
                onComplete(result);
            }
            _currentCall.reset();
        };

        DealerWriter(*dealer).writePtr(event, 0);
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
                            Payload &&request, const CallOptions &opts)
{
    auto handle = _d->call(serviceName, methodName, std::move(request), opts, {});
    if (!handle) {
        return {ErrorCode::CallInProcess, "call in progress", {}};
    }
    return handle->get();
}

std::shared_ptr<CallHandle> Stub::callMethodAsync(const std::string &serviceName,
                                                  const std::string &methodName,
                                                  Payload &&request,
                                                  const CallOptions &opts,
                                                  CompletionCallback onComplete)
{
    return _d->call(serviceName, methodName, std::move(request), opts, std::move(onComplete));
}
}
