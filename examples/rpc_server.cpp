#include <iostream>
#include <memory>
#include <string>

#include "zrpc/Context.h"
#include "zrpc/Server.h"

#include "common.h"

class DemoService : public zrpc::Service
{
public:
    DemoService() : Service(kService)
    {
        addMethod(kEcho, [](const zrpc::PayloadView &request, zrpc::Payload &reply) {
            const auto in = request.views.empty() ? "" : request.views[0];
            std::cout << kService << '.' << kEcho << ": " << in << '\n';
            reply.emplace_back(in);
        });
        addMethod(kGet, [](const zrpc::PayloadView &, zrpc::Payload &reply) {
            reply.emplace_back(kGetBytes, 'a');
        });
        addMethod(kPut, [](const zrpc::PayloadView &request, zrpc::Payload &reply) {
            const auto size = request.views.empty() ? 0 : request.views[0].size();
            reply.emplace_back(std::to_string(size));
        });
    }
};

int main(int argc, char *argv[])
{
    if (wantsHelp(argc, argv)) {
        std::cout << "usage: rpc_server [addr]\n"
                  << "Demo service: echo / get / put. default " << kRpcAddr << '\n';
        return 0;
    }

    const auto addr = argOr(argc, argv, 1, kRpcAddr);
    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Server server(ctx);
    server.registerService(std::make_unique<DemoService>());
    server.bind(addr);
    std::cout << "bound " << addr << " (" << kService << ")\npress Enter to stop.\n";
    std::cin.get();
    return 0;
}
