#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "zrpc/Client.h"
#include "zrpc/Context.h"

#include "common.h"

namespace {
constexpr zrpc::CallOptions kCallOpts{5000};
}

static void callEcho(const std::string &addr)
{
    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Client client(ctx);
    client.connect(addr);
    for (int i = 0; i < 10; ++i) {
        auto result = client.callMethod(kService, kEcho,
                                        zrpc::Payload{"msg-" + std::to_string(i)},
                                        kCallOpts);
        std::cout << i << ' ' << kEcho << " -> ";
        printResult(result);
    }
}

static void callGet(const std::string &addr)
{
    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Client client(ctx);
    client.connect(addr);
    for (int i = 0; i < 10; ++i) {
        const auto start = std::chrono::steady_clock::now();
        auto result = client.callMethod(kService, kGet, zrpc::Payload{}, kCallOpts);
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        std::cout << i << ' ' << kGet << ' ' << ms << " ms, "
                  << firstPart(result).size() << " bytes";
        if (!result.ok())
            std::cout << " -> error " << static_cast<int>(result.errorCode) << ": " << result.errorMsg;
        std::cout << '\n';
    }
}

static void callPut(const std::string &addr)
{
    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Client client(ctx);
    client.connect(addr);
    for (int i = 0; i < 10; ++i) {
        const auto start = std::chrono::steady_clock::now();
        auto result = client.callMethod(kService, kPut,
                                        zrpc::Payload{std::string(kPutBytes, 'a')},
                                        kCallOpts);
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
        std::cout << i << ' ' << kPut << ' ' << ms << " ms -> ";
        printResult(result);
    }
}

static void callEchoAsync(const std::string &addr)
{
    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Client client(ctx);
    client.connect(addr);
    auto handle = client.callMethodAsync(
        kService, kEcho, zrpc::Payload{"async"}, kCallOpts,
        [](zrpc::CallResult result) {
            std::cout << "onComplete -> ";
            printResult(result);
        });
    std::cout << "get() -> ";
    printResult(handle->get());
}

int main(int argc, char *argv[])
{
    if (wantsHelp(argc, argv)) {
        std::cout << "usage: rpc_client [echo|echo-async|get|put] [addr]\n"
                  << "pair with rpc_server.\n";
        return 0;
    }

    const auto mode = argOr(argc, argv, 1, "echo");
    const auto addr = argOr(argc, argv, 2, kRpcAddr);

    if (mode == "echo")
        callEcho(addr);
    else if (mode == "echo-async")
        callEchoAsync(addr);
    else if (mode == "get")
        callGet(addr);
    else if (mode == "put")
        callPut(addr);
    else {
        std::cerr << "unknown mode: " << mode << '\n';
        return 1;
    }
    return 0;
}
