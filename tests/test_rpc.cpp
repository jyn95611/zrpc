#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include "test_helpers.h"

using namespace std::chrono_literals;

TEST_CASE("not connected returns Disconnected immediately")
{
    auto ctx = testContext();
    zrpc::Client client(ctx);
    auto result = client.callMethod("Echo", "echo", zrpc::Payload{"x"});
    CHECK(result.errorCode == zrpc::ErrorCode::Disconnected);
    CHECK_FALSE(result.ok());
}

TEST_CASE("echo roundtrip")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);

    zrpc::Client client(ctx);
    client.connect(addr);

    auto result = client.callMethod("Echo", "echo", zrpc::Payload{"hello"},
                                    zrpc::CallOptions{.timeoutMs = 3000});
    REQUIRE(result.ok());
    CHECK(firstPart(result) == "hello");
}

TEST_CASE("unknown service and method")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);

    zrpc::Client client(ctx);
    client.connect(addr);

    auto noSvc = client.callMethod("Nope", "echo", zrpc::Payload{"x"},
                                   zrpc::CallOptions{.timeoutMs = 3000});
    CHECK(noSvc.errorCode == zrpc::ErrorCode::NoSuchService);

    auto noMethod = client.callMethod("Echo", "nope", zrpc::Payload{"x"},
                                      zrpc::CallOptions{.timeoutMs = 3000});
    CHECK(noMethod.errorCode == zrpc::ErrorCode::NoSuchMethod);
}

TEST_CASE("timeoutMs expires and late reply is not a second complete")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);

    zrpc::Client client(ctx);
    client.connect(addr);

    auto timedOut = client.callMethod("Echo", "sleep", zrpc::Payload{},
                                      zrpc::CallOptions{.timeoutMs = 50});
    CHECK(timedOut.errorCode == zrpc::ErrorCode::Timeout);

    auto ok = client.callMethod("Echo", "echo", zrpc::Payload{"after"},
                                zrpc::CallOptions{.timeoutMs = 3000});
    REQUIRE(ok.ok());
    CHECK(firstPart(ok) == "after");
}

TEST_CASE("connect to silent peer fail-fast Disconnected")
{
    auto ctx = testContext();
    zrpc::Client client(ctx);
    client.connect(testAddr());

    auto result = client.callMethod("Echo", "echo", zrpc::Payload{"x"},
                                    zrpc::CallOptions{.timeoutMs = 3000});
    CHECK(result.errorCode == zrpc::ErrorCode::Disconnected);
}

TEST_CASE("already connected / already bound throw; reopen after close")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);
    CHECK_THROWS_AS(server.bind(addr), std::runtime_error);

    zrpc::Client client(ctx);
    client.connect(addr);
    CHECK_THROWS_AS(client.connect(addr), std::runtime_error);

    server.close();
    server.wait();
    server.bind(addr);

    client.disconnect();
    client.connect(addr);
    auto result = client.callMethod("Echo", "echo", zrpc::Payload{"reopen"},
                                    zrpc::CallOptions{.timeoutMs = 3000});
    REQUIRE(result.ok());
    CHECK(firstPart(result) == "reopen");
}

TEST_CASE("disconnect fails in-flight request")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);

    zrpc::Client client(ctx);
    client.connect(addr);

    auto handle = client.callMethodAsync("Echo", "sleep", zrpc::Payload{},
                                         zrpc::CallOptions{.timeoutMs = 5000});
    std::this_thread::sleep_for(20ms);
    client.disconnect();

    auto result = handle->get();
    CHECK(result.errorCode == zrpc::ErrorCode::Disconnected);
}

TEST_CASE("async handle and onComplete")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);

    zrpc::Client client(ctx);
    client.connect(addr);

    std::promise<zrpc::CallResult> promised;
    auto future = promised.get_future();
    auto handle = client.callMethodAsync(
        "Echo", "echo", zrpc::Payload{"async"},
        zrpc::CallOptions{.timeoutMs = 3000},
        [&](zrpc::CallResult result) { promised.set_value(std::move(result)); });

    REQUIRE(handle);
    auto fromGet = handle->get();
    REQUIRE(fromGet.ok());
    CHECK(firstPart(fromGet) == "async");
    CHECK(handle->ready());
    CHECK(handle->result().ok());

    REQUIRE(future.wait_for(2s) == std::future_status::ready);
    auto fromCb = future.get();
    CHECK(fromCb.ok());
}

TEST_CASE("one client multiple outstanding and multi-thread calls")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Server server(ctx);
    server.registerService(std::make_unique<EchoService>());
    server.bind(addr);

    zrpc::Client client(ctx);
    client.connect(addr);

    std::vector<std::shared_ptr<zrpc::CallHandle>> handles;
    for (int i = 0; i < 8; ++i)
        handles.push_back(client.callMethodAsync(
            "Echo", "echo", zrpc::Payload{std::to_string(i)},
            zrpc::CallOptions{.timeoutMs = 3000}));

    for (int i = 0; i < 8; ++i) {
        auto result = handles[i]->get();
        REQUIRE(result.ok());
        CHECK(firstPart(result) == std::to_string(i));
    }

    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < 8; ++i) {
                auto result = client.callMethod(
                    "Echo", "echo", zrpc::Payload{"mt"},
                    zrpc::CallOptions{.timeoutMs = 3000});
                if (result.ok() && firstPart(result) == "mt")
                    ++ok;
            }
        });
    }
    for (auto &th : threads)
        th.join();
    CHECK(ok == 32);
}

TEST_CASE("CallHandle result is empty Ok before ready")
{
    zrpc::CallHandle handle;
    CHECK_FALSE(handle.ready());
    CHECK(handle.result().errorCode == zrpc::ErrorCode::Ok);
    CHECK(handle.result().ok());
}
