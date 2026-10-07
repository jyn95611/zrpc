#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "zrpc/Context.h"
#include "zrpc/PubSub.h"

#include "common.h"

int main(int argc, char *argv[])
{
    if (wantsHelp(argc, argv)) {
        std::cout << "usage: topic_pub [addr] [count]\n"
                  << "pair with topic_sub. default addr " << kTopicAddr << "\n";
        return 0;
    }

    const auto addr = argOr(argc, argv, 1, kTopicAddr);
    const int count = std::stoi(argOr(argc, argv, 2, "20"));

    auto ctx = std::make_shared<zrpc::Context>();
    zrpc::Publisher publisher(ctx);
    publisher.bind(addr);
    std::cout << "publisher bound " << addr << '\n';
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const std::vector<std::pair<std::string, std::string>> topics{
        {"A", "data for topic A"},
        {"B", "data for topic B"},
        {"C", "data for topic C"},
    };

    std::srand(static_cast<unsigned>(std::time(nullptr)));
    for (int i = 0; i < count; ++i) {
        const auto &item = topics[static_cast<size_t>(std::rand() % topics.size())];
        publisher.pubTopic(item.first, zrpc::Payload{item.second});
        std::cout << "pub " << item.first << '\n';
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return 0;
}
