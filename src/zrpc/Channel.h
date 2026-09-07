#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "Rpc.h"
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

    void callMethod(const std::string &serviceName, const std::string &methodName,
                    std::string &request, std::string &reply, Rpc *rpc);

private:
    StubPrivate *_d{};
};
}
