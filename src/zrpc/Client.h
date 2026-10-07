#pragma once

#include <memory>
#include <string>

#include "Call.h"
#include "zrpc_global.h"

namespace zrpc {
class Context;
class ClientPrivate;

/**
 * @brief RPC client: one DEALER socket per instance.
 *
 * After @ref connect the same client may pipeline many calls and may be used
 * from multiple threads. @ref Context must outlive the client. Destroying the
 * client while another thread is blocked in a synchronous call is undefined.
 */
class ZRPC_EXPORT Client final
{
public:
    Client(const std::shared_ptr<Context> &ctx);
    ~Client();

    /**
     * Acknowledge that the DEALER has been created and @c connect() was issued.
     *
     * Returns immediately; TCP may still be connecting. The first RPC waits for
     * this dial: the request is written after @c CONNECTED, or fails with
     * @ref ErrorCode::Disconnected on @c CONNECT_RETRIED.
     *
     * @param addr ZMQ endpoint, e.g. @c tcp://127.0.0.1:5555
     * @throws std::runtime_error if already connected. @ref disconnect first to reopen.
     */
    void connect(const std::string &addr);

    /**
     * Fail all in-flight requests with @ref ErrorCode::Disconnected.
     *
     * After ZMQ reconnects, the same client may send new RPCs without calling
     * @ref connect again.
     */
    void disconnect();

    /**
     * Synchronous RPC: @ref callMethodAsync plus @ref CallHandle::get.
     *
     * Default @ref CallOptions::timeoutMs is @c -1 (wait forever). Production
     * callers should set a positive timeout.
     */
    CallResult callMethod(const std::string &serviceName, const std::string &methodName,
                          Payload &&request, const CallOptions &opts = {});

    /**
     * Asynchronous RPC. Always returns a non-null handle.
     *
     * If the client is not connected the handle is completed immediately with
     * @ref ErrorCode::Disconnected. @p onComplete runs on a worker: it may call
     * @ref callMethodAsync again, but must not call @ref CallHandle::get (that
     * can deadlock if the reply is dispatched to the same worker).
     */
    std::shared_ptr<CallHandle> callMethodAsync(const std::string &serviceName,
                                                const std::string &methodName,
                                                Payload &&request,
                                                const CallOptions &opts = {},
                                                CompletionCallback onComplete = {});

private:
    std::unique_ptr<ClientPrivate> _d;
};
}
