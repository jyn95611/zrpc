#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "test_helpers.h"
#include "zrpc/PubSub.h"

using namespace std::chrono_literals;

TEST_CASE("pubTopic throws when not bound; already bound throws")
{
    auto ctx = testContext();
    zrpc::Publisher publisher(ctx);
    CHECK_THROWS_AS(publisher.pubTopic("t", zrpc::Payload{"x"}), std::runtime_error);

    const auto addr = testAddr();
    publisher.bind(addr);
    CHECK_THROWS_AS(publisher.bind(addr), std::runtime_error);
}

TEST_CASE("subscriber already connected throws")
{
    auto ctx = testContext();
    const auto addr = testAddr();
    zrpc::Publisher publisher(ctx);
    publisher.bind(addr);

    zrpc::Subscriber sub({"t"}, ctx);
    sub.connect(addr);
    CHECK_THROWS_AS(sub.connect(addr), std::runtime_error);
}

TEST_CASE("matching topic is delivered; other topic is not")
{
    auto ctx = testContext();
    const auto addr = testAddr();

    zrpc::Publisher publisher(ctx);
    publisher.bind(addr);

    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::pair<std::string, std::string>> got;

    zrpc::Subscriber sub({"topic.b"}, ctx);
    sub.setCallback([&](std::string_view topic, zrpc::PayloadView payload) {
        std::lock_guard<std::mutex> lock(mutex);
        got.emplace_back(std::string(topic),
                         payload.views.empty() ? std::string{} : std::string(payload.views[0]));
        cv.notify_all();
    });
    sub.connect(addr);
    std::this_thread::sleep_for(200ms);

    for (int i = 0; i < 8; ++i) {
        publisher.pubTopic("topic.a", zrpc::Payload{"A"});
        publisher.pubTopic("topic.b", zrpc::Payload{"B"});
        std::this_thread::sleep_for(30ms);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_for(lock, 2s, [&] { return !got.empty(); });
    }

    REQUIRE_FALSE(got.empty());
    for (const auto &[topic, data] : got) {
        CHECK(topic == "topic.b");
        CHECK(data == "B");
    }
}
