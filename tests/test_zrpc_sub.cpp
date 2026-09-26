#include <iostream>
#include <string_view>
#include <thread>

#include "zrpc/Context.h"
#include "zrpc/PubSub.h"

int main(int argc, char *argv[])
{
    std::cout << "zrpc sub start." << std::endl;

    std::string serverAddr = "tcp://localhost:9981";

    auto context = std::make_shared<zrpc::Context>();
    zrpc::Subscriber subscriber({"B"}, context);
    subscriber.setCallback([](std::string_view topic, zrpc::PayloadView payload){
        std::cout << "Sub topic: " << topic << " data: "
                  << (payload.views.empty() ? "" : payload.views[0]) << std::endl;
    });
    subscriber.connect(serverAddr);
    std::this_thread::sleep_for(std::chrono::seconds(30));

    std::cout << "zrpc sub exit." << std::endl;
    return 0;
}
