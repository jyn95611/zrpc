#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "zrpc/Context.h"
#include "zrpc/PubSub.h"

// ── 辅助 ─────────────────────────────────────────────────────────────────────
namespace {

constexpr int kJoinDelayMs     = 150;   // ZMQ slow-joiner 窗口
constexpr int kDeliveryTimeout = 2000;  // 单条消息最大等待 (ms)

void sleep_ms(int ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

bool wait_count(std::atomic<int> &counter, int target, int timeoutMs = kDeliveryTimeout)
{
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (counter.load() < target && std::chrono::steady_clock::now() < deadline)
        sleep_ms(10);
    return counter.load() >= target;
}

} // namespace

// ── Fixture：每个 TEST 共享独立 Context ──────────────────────────────────────
class PubSubTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ctx = std::make_shared<zrpc::Context>();
    }

    std::shared_ptr<zrpc::Context> ctx;
};

// ── TC01: 基本端到端投递 ──────────────────────────────────────────────────────
TEST_F(PubSubTest, BasicDelivery)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc01.sock";

    std::atomic<int> recvCount{0};
    std::string       recvData;
    std::mutex        recvMtx;

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"topicA"}, ctx);
    sub.setCallback([&](const std::string &, const std::string &data) {
        std::lock_guard<std::mutex> lk(recvMtx);
        recvData = data;
        recvCount++;
    });
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    pub.pubTopic("topicA", "hello");

    ASSERT_TRUE(wait_count(recvCount, 1));
    std::lock_guard<std::mutex> lk(recvMtx);
    EXPECT_EQ(recvData, "hello");
}

// ── TC02: 应用层 topic 过滤 ── sub 只收订阅的 topic ──────────────────────────
TEST_F(PubSubTest, TopicFiltering)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc02.sock";

    std::atomic<int>  recvCount{0};
    std::atomic<bool> wrongTopic{false};

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"topicB"}, ctx);
    sub.setCallback([&](const std::string &topic, const std::string &) {
        if (topic != "topicB") wrongTopic.store(true);
        recvCount++;
    });
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    pub.pubTopic("topicA", "noise_A");
    pub.pubTopic("topicB", "data_B1");
    pub.pubTopic("topicC", "noise_C");
    pub.pubTopic("topicB", "data_B2");

    ASSERT_TRUE(wait_count(recvCount, 2));
    sleep_ms(kDeliveryTimeout / 5);   // 确认 noise 不会迟到

    EXPECT_EQ(recvCount.load(), 2);
    EXPECT_FALSE(wrongTopic.load());
}

// ── TC03: 空 topic 列表 ── 任何消息均不触发 callback ─────────────────────────
TEST_F(PubSubTest, EmptyTopicListReceivesNothing)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc03.sock";

    std::atomic<int> recvCount{0};

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({}, ctx);
    sub.setCallback([&](const std::string &, const std::string &) { recvCount++; });
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    pub.pubTopic("topicA", "data");
    pub.pubTopic("topicB", "data");

    sleep_ms(kDeliveryTimeout / 4);
    EXPECT_EQ(recvCount.load(), 0);
}

// ── TC04: 多个 Subscriber，各自订阅不同 topic，互不串扰 ──────────────────────
TEST_F(PubSubTest, MultipleSubscribersIsolated)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc04.sock";

    constexpr int kN = 5;
    std::atomic<int>  recvA{0}, recvB{0};
    std::atomic<bool> wrongA{false}, wrongB{false};

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber subA({"topicA"}, ctx);
    subA.setCallback([&](const std::string &topic, const std::string &) {
        if (topic != "topicA") wrongA.store(true);
        recvA++;
    });
    subA.connect(addr);

    zrpc::Subscriber subB({"topicB"}, ctx);
    subB.setCallback([&](const std::string &topic, const std::string &) {
        if (topic != "topicB") wrongB.store(true);
        recvB++;
    });
    subB.connect(addr);
    sleep_ms(kJoinDelayMs);

    for (int i = 0; i < kN; ++i) {
        pub.pubTopic("topicA", "a" + std::to_string(i));
        pub.pubTopic("topicB", "b" + std::to_string(i));
    }

    ASSERT_TRUE(wait_count(recvA, kN));
    ASSERT_TRUE(wait_count(recvB, kN));
    EXPECT_EQ(recvA.load(), kN);
    EXPECT_EQ(recvB.load(), kN);
    EXPECT_FALSE(wrongA.load());
    EXPECT_FALSE(wrongB.load());
}

// ── TC05: 多 Publisher 绑定不同地址，Subscriber 各自隔离 ─────────────────────
TEST_F(PubSubTest, MultiplePublishersIsolated)
{
    const std::string addr1 = "ipc:///tmp/zrpc_gtest_tc05a.sock";
    const std::string addr2 = "ipc:///tmp/zrpc_gtest_tc05b.sock";

    std::atomic<int> recv1{0}, recv2{0};

    zrpc::Publisher pub1(ctx);
    pub1.bind(addr1);
    zrpc::Publisher pub2(ctx);
    pub2.bind(addr2);

    zrpc::Subscriber sub1({"ch1"}, ctx);
    sub1.setCallback([&](const std::string &, const std::string &) { recv1++; });
    sub1.connect(addr1);

    zrpc::Subscriber sub2({"ch2"}, ctx);
    sub2.setCallback([&](const std::string &, const std::string &) { recv2++; });
    sub2.connect(addr2);
    sleep_ms(kJoinDelayMs);

    pub1.pubTopic("ch1", "from_pub1");   // sub1 收到
    pub1.pubTopic("ch2", "noise");       // 连到 addr1，sub2 连 addr2，不收
    pub2.pubTopic("ch2", "from_pub2");   // sub2 收到
    pub2.pubTopic("ch1", "noise");       // 连到 addr2，sub1 连 addr1，不收

    sleep_ms(kDeliveryTimeout / 4);
    EXPECT_EQ(recv1.load(), 1);
    EXPECT_EQ(recv2.load(), 1);
}

// ── TC06: 同一 Subscriber 订阅多个 topic，各自计数正确 ───────────────────────
TEST_F(PubSubTest, MultipleTopicsOnSameSubscriber)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc06.sock";

    std::atomic<int> recvA{0}, recvB{0};
    std::mutex mtx;

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"topicA", "topicB"}, ctx);
    sub.setCallback([&](const std::string &topic, const std::string &) {
        std::lock_guard<std::mutex> lk(mtx);
        if (topic == "topicA")      recvA++;
        else if (topic == "topicB") recvB++;
    });
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    pub.pubTopic("topicA", "a1");
    pub.pubTopic("topicB", "b1");
    pub.pubTopic("topicA", "a2");

    ASSERT_TRUE(wait_count(recvA, 2));
    ASSERT_TRUE(wait_count(recvB, 1));
    EXPECT_EQ(recvA.load(), 2);
    EXPECT_EQ(recvB.load(), 1);
}

// ── TC07: 多线程并发 pubTopic ── _mtxForDealer 保证不崩溃 ───────────────────
TEST_F(PubSubTest, ConcurrentPublishNoCrash)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc07.sock";

    constexpr int kThreads       = 6;
    constexpr int kMsgsPerThread = 20;
    std::atomic<int> recvCount{0};

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"t"}, ctx);
    sub.setCallback([&](const std::string &, const std::string &) { recvCount++; });
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&pub, i]() {
            for (int j = 0; j < kMsgsPerThread; ++j)
                pub.pubTopic("t", std::to_string(i) + "_" + std::to_string(j));
        });
    }
    for (auto &t : threads) t.join();

    ASSERT_TRUE(wait_count(recvCount, kThreads * kMsgsPerThread));
    EXPECT_EQ(recvCount.load(), kThreads * kMsgsPerThread);
}

// ── TC08: 未调用 setCallback，消息到达不崩溃 ─────────────────────────────────
// SubscriberPrivate::topicCallback 有 if (topicCb) 守卫
TEST_F(PubSubTest, NoCallbackNoCrash)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc08.sock";

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"t"}, ctx);
    // 故意不调用 setCallback
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    pub.pubTopic("t", "hello");
    sleep_ms(300);
    SUCCEED();
}

// ── TC09: 大消息（256 KB）序列化/反序列化无截断 ───────────────────────────────
TEST_F(PubSubTest, LargeMessageDelivery)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc09.sock";
    const std::string largeData(256 * 1024, 'x');

    std::atomic<int> recvCount{0};
    std::string       recvData;
    std::mutex        mtx;

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"big"}, ctx);
    sub.setCallback([&](const std::string &, const std::string &data) {
        std::lock_guard<std::mutex> lk(mtx);
        recvData = data;
        recvCount++;
    });
    sub.connect(addr);
    sleep_ms(kJoinDelayMs);

    pub.pubTopic("big", std::string(largeData));

    ASSERT_TRUE(wait_count(recvCount, 1));
    std::lock_guard<std::mutex> lk(mtx);
    EXPECT_EQ(recvData.size(), largeData.size());
    EXPECT_EQ(recvData, largeData);
}

// ── TC10: 已知 Bug — connect 后再 setCallback，topicCb 读写无锁 ──────────────
// 仅验证不崩溃；计数不做精确断言
TEST_F(PubSubTest, CallbackSetAfterConnectKnownRaceNoCrash)
{
    const std::string addr = "ipc:///tmp/zrpc_gtest_tc10.sock";

    std::atomic<bool> stop{false};
    std::atomic<int>  recvCount{0};

    zrpc::Publisher pub(ctx);
    pub.bind(addr);

    zrpc::Subscriber sub({"t"}, ctx);
    sub.connect(addr);              // 先 connect，topicCb 为空
    sleep_ms(kJoinDelayMs);

    std::thread pubThread([&]() {
        while (!stop.load())
            pub.pubTopic("t", "msg");
    });

    sleep_ms(50);
    // KNOWN RACE: topicCallback() 读 topicCb，setCallback() 写 topicCb，无同步
    sub.setCallback([&](const std::string &, const std::string &) { recvCount++; });

    sleep_ms(200);
    stop.store(true);
    pubThread.join();

    EXPECT_GE(recvCount.load(), 0);   // 不对精确值断言
}