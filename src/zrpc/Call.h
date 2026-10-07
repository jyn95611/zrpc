#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "zrpc_global.h"

namespace zrpc {
/**
 * @brief RPC result codes.
 *
 * Delivery is at-most-once: the library never retransmits a request that has
 * already been written to ZMQ.
 *
 * Retry guidance:
 * - @ref TryAgain: the request never entered ZMQ; the same call may be retried immediately.
 * - @ref Disconnected: not connected, peer down, or first-dial fail-fast; issue a new call after recovery.
 * - @ref Timeout: the outcome may be unknown; writes need application-level idempotency.
 */
enum class ErrorCode
{
    Ok = 0,            ///< Success.
    CallInProcess,     ///< Reserved; current Client paths do not produce this.
    Timeout,           ///< @ref CallOptions::timeoutMs expired.
    Disconnected,      ///< Not connected, peer down, client removed, or first-dial fail-fast.
    InvalidMessage,    ///< Header could not be parsed, or a frame exceeded the size limit.
    TryAgain,          ///< Send queue full (@c SNDTIMEO=0); the request was not written.

    NoSuchService = 100, ///< Server has no service with that name.
    NoSuchMethod,        ///< Service has no method with that name.
};

/** Multipart RPC payload owned as strings. */
using Payload = std::vector<std::string>;

/**
 * @brief Non-owning view of a multipart payload.
 *
 * @c views point into @c owned (or into buffers that outlive the call). Keep
 * the @ref CallResult / @ref PayloadView alive while reading asynchronous results.
 */
struct PayloadView
{
    std::vector<std::string_view> views;
    std::shared_ptr<void> owned;
};

/**
 * @brief Per-call options.
 */
struct CallOptions
{
    /**
     * Deadline in milliseconds. @c -1 waits forever.
     *
     * Production callers should set a value @c > 0. Expiry completes the call
     * with @ref ErrorCode::Timeout via the poller timer; @ref CallHandle::get
     * does not add a second timeout.
     */
    int64_t timeoutMs{-1};
};

/**
 * @brief Outcome of one RPC.
 */
struct CallResult
{
    ErrorCode errorCode{ErrorCode::Ok};
    std::string errorMsg;
    PayloadView payload;

    /** @return true if @ref errorCode is @ref ErrorCode::Ok. */
    bool ok() const { return errorCode == ErrorCode::Ok; }
};

class ClientPrivate;

/**
 * @brief Completion handle for one RPC.
 *
 * Lifetime is independent of @ref Client: @c get() remains valid after
 * @ref Client::disconnect.
 */
class ZRPC_EXPORT CallHandle
{
public:
    /** @return true after @ref complete has run. */
    bool ready() const;

    /**
     * Block until the call completes.
     *
     * Without a positive @ref CallOptions::timeoutMs this waits indefinitely.
     */
    CallResult get();

    /**
     * Snapshot of the result.
     *
     * Only meaningful after @ref ready is true; otherwise this is a default
     * empty @ref CallResult with @ref ErrorCode::Ok.
     */
    const CallResult &result() const;

private:
    friend class ClientPrivate;
    void complete(CallResult result);

    mutable std::mutex _mutex;
    std::condition_variable _cv;
    bool _ready{false};
    CallResult _result;
};

/** Optional completion callback; invoked on a worker thread. */
using CompletionCallback = std::function<void(CallResult)>;
}
