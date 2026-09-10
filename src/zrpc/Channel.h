#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "Call.h"
#include "zrpc_global.h"

namespace zrpc {
class Context;
class ChannelPrivate;
class ZRPC_EXPORT Channel final
{
public:
    Channel(const std::shared_ptr<Context> &ctx);
    ~Channel();

    void connect(const std::string &addr);

    std::shared_ptr<Context> context() const;
    uint64_t socketId() const;

private:
    ChannelPrivate *_d{};
};

class StubPrivate;
class ZRPC_EXPORT Stub final
{
public:
    Stub(const std::shared_ptr<Channel> &channel);
    ~Stub();

    CallResult callMethod(const std::string &serviceName, const std::string &methodName,
                          const std::string &request, CallOptions opts = {});

    std::shared_ptr<CallHandle> callMethodAsync(const std::string &serviceName,
                                                const std::string &methodName,
                                                std::string request,
                                                CallOptions opts = {},
                                                CompletionCallback onComplete = {});

private:
    StubPrivate *_d{};
};
}
