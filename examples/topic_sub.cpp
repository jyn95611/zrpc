#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "zrpc/Context.h"
#include "zrpc/PubSub.h"

#include "common.h"

int main(int argc, char *argv[])
{
    if (wantsHelp(argc, argv)) {
        std::cout << "usage: topic_sub [addr] [topic]\n"
                  << "pair with topic_pub. default addr " << kTopicAddr << ", topic B\n";
        return 0;
    }

    const auto addr = argOr(argc, argv, 1, kTopicAddr);
    const auto topic = argOr(argc, argv, 2, "B");

    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Subscriber subscriber({topic}, ctx);
    subscriber.setCallback([](std::string_view gotTopic, zrpc::PayloadView payload) {
        std::cout << "sub " << gotTopic << ": "
                  << (payload.views.empty() ? "" : payload.views[0]) << '\n';
    });
    subscriber.connect(addr);
    std::cout << "subscriber connected " << addr << " topic=" << topic
              << "\npress Enter to stop.\n";
    std::cin.get();
    return 0;
}
