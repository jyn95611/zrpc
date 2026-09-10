#pragma once

#include <memory>

namespace zrpc {
class Context;
class ContextPrivate;

namespace detail {
struct ContextAccess
{
    static ContextPrivate *get(Context *ctx);
    static ContextPrivate *get(const std::shared_ptr<Context> &ctx) { return get(ctx.get()); }
};
}
}
