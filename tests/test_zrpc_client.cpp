#include <iostream>
#include <thread>
#include <chrono>
#include <memory>
#include <string>
#include <cstdlib>

#include "zrpc/Context.h"
#include "zrpc/Channel.h"

class StopWatch
{
public:
    StopWatch() : _start(std::chrono::steady_clock::now()) {}

    void restart()
    {
        _start = std::chrono::steady_clock::now();
    }

    double elapsed()
    {
        const auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now - _start).count();
    }

private:
    std::chrono::steady_clock::time_point _start;
};

void client_func()
{
    auto context = std::make_shared<zrpc::Context>();

    std::string serverAddr = "tcp://localhost:9981";

    auto channel = std::make_shared<zrpc::Channel>(context);
    channel->connect(serverAddr);

    zrpc::Stub stub1(channel);

    for (int i = 0; i < 10; ++i) {
        std::cout << "===================== " << i << std::endl;

        std::string serviceName;
        std::string methodName;
        std::string request = "World" + std::to_string(i);
        if (i % 2 == 0) {
            serviceName = "GreeterService1";
            methodName = "sayHello";
        } else {
            serviceName = "GreeterService2";
            methodName = "sayHi";
        }

        auto result = stub1.callMethod(serviceName, methodName, request);

        if (result.ok()) {
            std::cout << "Client recv: " << result.reply << std::endl;
        } else {
            std::cout << "Client recv error: " << static_cast<int>(result.code) << std::endl;
            std::cout << "Client recv error message: " << result.message << std::endl;
        }
    }

    std::cout << "client func to exit." << std::endl;
}

void client_func2()
{
    auto context = std::make_shared<zrpc::Context>();

    std::string serverAddr1 = "tcp://localhost:9981";
    std::string serverAddr2 = "tcp://localhost:9982";

    auto channel1 = std::make_shared<zrpc::Channel>(context);
    auto channel2 = std::make_shared<zrpc::Channel>(context);
    channel1->connect(serverAddr1);
    channel2->connect(serverAddr2);

    zrpc::Stub stub1(channel1);
    zrpc::Stub stub2(channel2);

    for (int i = 0; i < 10; ++i) {
        std::cout << "===================== " << i << std::endl;

        std::string serviceName;
        std::string methodName;
        std::string request = "World" + std::to_string(i);
        zrpc::Stub *stub = nullptr;
        if (i % 2 == 0) {
            serviceName = "GreeterService1";
            methodName = "sayHello";
            stub = &stub1;
        } else {
            serviceName = "GreeterService2";
            methodName = "sayHi";
            stub = &stub2;
        }

        auto result = stub->callMethod(serviceName, methodName, request);

        if (result.ok()) {
            std::cout << "Client recv: " << result.reply << std::endl;
        } else {
            std::cout << "Client recv error: " << static_cast<int>(result.code) << std::endl;
            std::cout << "Client recv error message: " << result.message << std::endl;
        }
    }

    std::this_thread::sleep_for(std::chrono::seconds(10));
    std::cout << "client func to exit." << std::endl;
}

void client_func3()
{
    auto context = std::make_shared<zrpc::Context>();

    const std::string serverAddr = "tcp://localhost:9981";
    auto channel = std::make_shared<zrpc::Channel>(context);
    channel->connect(serverAddr);

    zrpc::Stub stub(channel);

    std::string serviceName = "CloudService";
    std::string methodName = "getCloud";
    std::string request = "cloudType";

    for (int i = 0; i < 10; ++i) {
        StopWatch watch;
        auto result = stub.callMethod(serviceName, methodName, request);

        if (result.ok()) {
            (void)result.reply.size();
        } else {
            std::cout << "Client recv error: " << static_cast<int>(result.code) << std::endl;
            std::cout << "Client recv error message: " << result.message << std::endl;
        }
        std::cout << "Get cloud elapsed(ms): " << watch.elapsed() << std::endl;
    }
    std::cout << "client func to exit." << std::endl;
}

void client_func4()
{
    auto context = std::make_shared<zrpc::Context>();

    const std::string serverAddr = "tcp://localhost:9981";
    auto channel = std::make_shared<zrpc::Channel>(context);
    channel->connect(serverAddr);

    zrpc::Stub stub(channel);

    std::string serviceName = "CloudService";
    std::string methodName = "setCloud";
    std::string request(100 * 10000 * 24, 'a');

    for (int i = 0; i < 10; ++i) {
        StopWatch watch;
        auto result = stub.callMethod(serviceName, methodName, request);

        if (result.ok()) {
            (void)std::atoi(result.reply.c_str());
        } else {
            std::cout << "Client recv error: " << static_cast<int>(result.code) << std::endl;
            std::cout << "Client recv error message: " << result.message << std::endl;
        }
        std::cout << "Get cloud elapsed(ms): " << watch.elapsed() << std::endl;
    }
    std::cout << "client func to exit." << std::endl;
}

void client_func_async()
{
    auto context = std::make_shared<zrpc::Context>();

    auto channel = std::make_shared<zrpc::Channel>(context);
    channel->connect("tcp://localhost:9981");

    zrpc::Stub stub(channel);

    std::string request = "World";
    auto handle = stub.callMethodAsync("GreeterService1", "sayHello", request,
        {}, [](zrpc::CallResult result) {
            if (result.ok()) {
                std::cout << "Async recv: " << result.reply << std::endl;
            }
        });
    if (handle) {
        auto result = handle->get();
        if (result.ok()) {
            std::cout << "Sync wait recv: " << result.reply << std::endl;
        }
    }
}

int main(int argc, char *argv[])
{
    std::cout << "zrpc client start." << std::endl;

    client_func();
//    client_func2();
//    client_func3();
//    client_func4();
//    client_func_async();

    std::cout << "zrpc client exit." << std::endl;
    return 0;
}
