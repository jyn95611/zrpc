#include <atomic>
#include <mutex>
#include <stdexcept>
#include <spdlog/spdlog.h>

#include "utils.h"
#include "Message.h"
#include "Event.h"
#include "ContextPrivate.h"
#include "Context.h"
#include "Client.h"

namespace zrpc {
namespace {
CallResult buildCallResult(RequestStatus status, RpcMessage &reply)
{
    if (status == RequestStatus::Timeout) {
        return {ErrorCode::Timeout, "timeout", {}};
    }

    if (status == RequestStatus::Disconnected) {
        return {ErrorCode::Disconnected, "peer disconnected", {}};
    }

    if (status == RequestStatus::TryAgain) {
        return {ErrorCode::TryAgain, "send queue full, try again", {}};
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

class ClientPrivate
{
public:
    ClientPrivate(const std::shared_ptr<Context> &actx)
        : ctx(actx)
        , dealer(detail::ContextAccess::get(actx)->createFrontendSocket())
    {}
    ~ClientPrivate()
    {
        disconnect();
        std::lock_guard<std::mutex> locker(_dealerMtx);
        if (dealer)
            dealer->close();
    }

    void connect(const std::string &addr)
    {
        std::lock_guard<std::mutex> locker(_dealerMtx);
        if (socketId.load(std::memory_order_acquire) > 0)
            throw std::runtime_error("already connected");

        auto *event = new AddClientEvent;
        event->serverAddr = addr;
        DealerSender(*dealer).sendPtr(event, 0);
        const auto id = recvSocketEventAck(*dealer, "connect");
        socketId.store(id, std::memory_order_release);
        spdlog::info("client connected, addr={} socketId={}", addr, id);
    }

    void disconnect()
    {
        std::lock_guard<std::mutex> locker(_dealerMtx);
        const auto id = socketId.exchange(0, std::memory_order_acq_rel);
        if (id == 0)
            return;

        auto *event = new RemoveClientEvent;
        event->socketId = id;
        DealerSender(*dealer).sendPtr(event, 0);
        recvEventAck(*dealer);
    }

    std::shared_ptr<CallHandle> call(const std::string &serviceName,
                                     const std::string &methodName,
                                     Payload &&request,
                                     const CallOptions &opts,
                                     CompletionCallback onComplete)
    {
        auto handle = std::make_shared<CallHandle>();

        {
            std::lock_guard<std::mutex> locker(_dealerMtx);
            const uint64_t id = socketId.load(std::memory_order_acquire);
            if (id != 0) {
                auto *event = new SendRequestEvent;
                event->clientSocketId = id;
                event->timeoutMs = opts.timeoutMs;
                event->request.header = RpcRequestHeader(
                    detail::ContextAccess::get(ctx)->nextRequestId(),
                    serviceName, methodName).serialize();
                event->request.parts = takePayload(std::move(request));
                /// Capture the handle and onComplete, never Client. Lifetime lives on CallHandle.
                event->clientFunc = [handle, onComplete = std::move(onComplete)](
                                        RequestStatus status, RpcMessage &reply) {
                    CallResult result = buildCallResult(status, reply);
                    handle->complete(result);
                    if (onComplete)
                        onComplete(result);
                };
                DealerSender(*dealer).sendPtr(event, 0);
                return handle;
            }
        }

        CallResult result{ErrorCode::Disconnected, "not connected", {}};
        handle->complete(result);
        if (onComplete)
            onComplete(result);
        return handle;
    }

    std::shared_ptr<Context> ctx;
    std::unique_ptr<zmq::socket_t> dealer;
    std::mutex _dealerMtx;
    std::atomic<uint64_t> socketId{0};
};

Client::Client(const std::shared_ptr<Context> &ctx)
    : _d(std::make_unique<ClientPrivate>(ctx))
{
}

Client::~Client() = default;

void Client::connect(const std::string &addr)
{
    _d->connect(addr);
}

void Client::disconnect()
{
    _d->disconnect();
}

CallResult Client::callMethod(const std::string &serviceName, const std::string &methodName,
                              Payload &&request, const CallOptions &opts)
{
    return _d->call(serviceName, methodName, std::move(request), opts, {})->get();
}

std::shared_ptr<CallHandle> Client::callMethodAsync(const std::string &serviceName,
                                                    const std::string &methodName,
                                                    Payload &&request,
                                                    const CallOptions &opts,
                                                    CompletionCallback onComplete)
{
    return _d->call(serviceName, methodName, std::move(request), opts, std::move(onComplete));
}
}
