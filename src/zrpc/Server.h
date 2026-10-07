#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "Call.h"
#include "zrpc_global.h"

namespace zrpc {
/**
 * @brief Named collection of RPC methods.
 *
 * Methods may run on several workers at once; they must be reentrant or
 * synchronized by the application. They must not throw.
 */
class ZRPC_EXPORT Service
{
public:
    Service(const std::string &name) : _name(name) {}
    virtual ~Service() = default;

    const std::string &name() { return _name; }

    using Method = std::function<void(const PayloadView&, Payload&)>;
    void addMethod(const std::string &name, const Method &method);
    std::optional<Method> findMethod(const std::string &name) const;

private:
    std::string _name;
    std::unordered_map<std::string, Method> _methods;
};

class Context;
class ServerPrivate;

/**
 * @brief RPC server bound to one endpoint.
 *
 * The destructor runs @ref close then @ref wait. @ref Context must outlive the
 * server. Methods and callbacks must not call @ref wait or destroy this server.
 */
class ZRPC_EXPORT Server final
{
public:
    Server(const std::shared_ptr<Context> &ctx);
    ~Server();

    /**
     * Take ownership of @p service. A null pointer is ignored.
     *
     * Do not call this concurrently with inbound requests after @ref bind.
     */
    void registerService(std::unique_ptr<Service> service);

    /**
     * Bind the ROUTER socket.
     *
     * @throws std::runtime_error if already bound. @ref close then @ref wait to reopen.
     */
    void bind(const std::string &addr);

    /**
     * Stop listening immediately (session @c Shutdown).
     *
     * Does not wait for in-flight methods; the second ack is received by @ref wait.
     */
    void close();

    /** Block until in-flight methods finish after @ref close. */
    void wait();

private:
    std::unique_ptr<ServerPrivate> _d;
};
}
