#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "zrpc_global.h"

namespace zrpc {
class ZRPC_EXPORT Service
{
public:
    Service(const std::string &name) : _name(name) {}
    virtual ~Service() = default;

    const std::string &name() { return _name; }

    using Method = std::function<void(const std::string&, std::string&)>;
    void addMethod(const std::string &name, const Method &method);
    const Method &findMethod(const std::string &name);

private:
    std::string _name;
    std::unordered_map<std::string, Method> _methods;
};

class Context;
class ServerPrivate;
class ZRPC_EXPORT Server final
{
public:
    Server(const std::shared_ptr<Context> &ctx);
    ~Server();

    void registerService(Service *service);
    void bind(const std::string &addr);

private:
    ServerPrivate *_d{};
};
}
