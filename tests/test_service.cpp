#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include "zrpc/Context.h"
#include "zrpc/Server.h"

TEST_CASE("Service findMethod and addMethod")
{
    zrpc::Service service("Greeter");
    CHECK(service.name() == "Greeter");
    CHECK_FALSE(service.findMethod("sayHello").has_value());

    service.addMethod("sayHello", [](const zrpc::PayloadView &, zrpc::Payload &out) {
        out.emplace_back("hi");
    });

    auto method = service.findMethod("sayHello");
    REQUIRE(method);
    zrpc::PayloadView in;
    zrpc::Payload out;
    (*method)(in, out);
    REQUIRE(out.size() == 1);
    CHECK(out[0] == "hi");
    CHECK_FALSE(service.findMethod("missing").has_value());
}

TEST_CASE("registerService ignores null and owns the service")
{
    auto ctx = std::make_shared<zrpc::Context>(1, 1);
    zrpc::Server server(ctx);
    server.registerService(nullptr);
    server.registerService(std::make_unique<zrpc::Service>("Owned"));
}
