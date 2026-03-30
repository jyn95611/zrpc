#include <iostream>
#include <thread>
#include <vector>

#include "zrpc/Context.h"
#include "zrpc/PubSub.h"

int main(int argc, char *argv[])
{
    std::cout << "zrpc pub start." << std::endl;

    std::string serverAddr = "tcp://localhost:9981";

    auto context = std::make_shared<zrpc::Context>();
    zrpc::Publisher publisher(context);
    publisher.bind(serverAddr);

    const std::vector<std::pair<std::string, std::string>> topics{
        {"A", "This is the data for topic A."},
        {"B", "This is the data for topic B."},
        {"C", "This is the data for topic C."},
    };
    for (int i = 0; i < 100; ++i) {
        const auto index = arc4random() % 3;
        const auto &topic = topics[index].first;
        auto data = topics[index].second;
        publisher.pubTopic(topics[index].first, std::move(data));
        std::cout << "Pub topic: " << topic << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "zrpc pub exit." << std::endl;
    return 0;
}
