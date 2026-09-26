#include <iostream>
#include <thread>

#include "zrpc/Context.h"
#include "zrpc/Server.h"

class GreeterService1 : public zrpc::Service
{
public:
    GreeterService1() : zrpc::Service("GreeterService1")
    {
        addMethod("sayHello", [this](const zrpc::PayloadView &request, zrpc::Payload &reply){
            sayHello(request, reply);
        });
        addMethod("sayHi", [this](const zrpc::PayloadView &request, zrpc::Payload &reply){
            sayHi(request, reply);
        });
    }

    void sayHello(const zrpc::PayloadView &request, zrpc::Payload &reply)
    {
        std::cout << "GreeterService1 SayHello recv request: "
                  << (request.views.empty() ? "" : request.views[0]) << std::endl;
        reply.emplace_back("Hello");
    }

    void sayHi(const zrpc::PayloadView &request, zrpc::Payload &reply)
    {
        std::cout << "GreeterService1 SayHi recv request: "
                  << (request.views.empty() ? "" : request.views[0]) << std::endl;
        reply.emplace_back("Hi");
    }
};

class GreeterService2 : public zrpc::Service
{
public:
    GreeterService2() : zrpc::Service("GreeterService2")
    {
        addMethod("sayHello", [this](const zrpc::PayloadView &request, zrpc::Payload &reply){
            sayHello(request, reply);
        });
        addMethod("sayHi", [this](const zrpc::PayloadView &request, zrpc::Payload &reply){
            sayHi(request, reply);
        });
    }

    void sayHello(const zrpc::PayloadView &request, zrpc::Payload &reply)
    {
        std::cout << "GreeterService2 SayHello recv request: "
                  << (request.views.empty() ? "" : request.views[0]) << std::endl;
        reply.emplace_back("Hello");
    }

    void sayHi(const zrpc::PayloadView &request, zrpc::Payload &reply)
    {
        std::cout << "GreeterService2 SayHi recv request: "
                  << (request.views.empty() ? "" : request.views[0]) << std::endl;
        reply.emplace_back("Hi");
    }
};

class CloudService : public zrpc::Service
{
public:
    CloudService() : zrpc::Service("CloudService")
    {
        addMethod("getCloud", [this](const zrpc::PayloadView &request, zrpc::Payload &reply){
            getCloud(request, reply);
        });
        addMethod("setCloud", [this](const zrpc::PayloadView &request, zrpc::Payload &reply){
            setCloud(request, reply);
        });
    }

    void getCloud(const zrpc::PayloadView &request, zrpc::Payload &reply)
    {
        (void)request;
        reply.emplace_back(30 * 10000 * 24, 'a');
    }

    void setCloud(const zrpc::PayloadView &request, zrpc::Payload &reply)
    {
        const auto size = request.views.empty() ? 0 : request.views[0].size();
        reply.emplace_back(std::to_string(size));
    }
};

void server_func()
{
    auto context = std::make_shared<zrpc::Context>();

    GreeterService1 service1;
    GreeterService2 service2;

    std::string serverAddr = "tcp://localhost:9981";

    zrpc::Server server(context);
    server.registerService(&service1);
    server.registerService(&service2);
    server.bind(serverAddr);

    std::this_thread::sleep_for(std::chrono::seconds(100));
    std::cout << "server func to exit." << std::endl;
}

void server_func2()
{
    auto context = std::make_shared<zrpc::Context>();

    GreeterService1 service1;
    GreeterService2 service2;

    std::string serverAddr1 = "tcp://localhost:9981";
    std::string serverAddr2 = "tcp://localhost:9982";

    zrpc::Server server1(context);
    server1.registerService(&service1);
    server1.bind(serverAddr1);

    zrpc::Server server2(context);
    server2.registerService(&service2);
    server2.bind(serverAddr2);

    std::this_thread::sleep_for(std::chrono::seconds(100));
    std::cout << "server func to exit." << std::endl;
}

void server_func3()
{
    auto context = std::make_shared<zrpc::Context>();

    CloudService service;
   const std::string serverAddr = "tcp://localhost:9981";
//    const std::string serverAddr = "tcp://192.168.1.102:9981";
    // const std::string serverAddr = "ipc://9985.sock";

    zrpc::Server server1(context);
    server1.registerService(&service);
    server1.bind(serverAddr);

    std::this_thread::sleep_for(std::chrono::seconds(100));
    std::cout << "server func to exit." << std::endl;
}

int main(int argc, char *argv[])
{
    std::cout << "zrpc server start." << std::endl;

   server_func();
//    server_func2();
    // server_func3();

    std::cout << "zrpc server exit." << std::endl;
    return 0;
}
