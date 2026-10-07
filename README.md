# zrpc

基于 ZeroMQ 的 C++20 RPC / Pub-Sub 库。一个 `Context` 内：1 条 poller 独占全部 ZMQ socket，method 与回调丢到 worker 池；RPC 走 DEALER/ROUTER，按 `requestId` 多路复用。

面向同架构、可信局域网。运输层是 TCP（有重传和保序）；RPC 层是 **at-most-once**：一次调用要么成功一次，要么失败，库不自动重发已送出的请求。

## 特性

- 同步 `callMethod` / 异步 `callMethodAsync` + `CallHandle`
- 一只 `Client` 可同时多飞、`connect` 之后可多线程共用
- `Server` 持有 `Service`，method 由多个 worker 并发调用（须可重入）
- Pub/Sub 尽力而为：慢订阅者、晚到的订阅者允许丢消息
- 每调用可选 `CallOptions.timeoutMs`；默认 `-1` 表示无限等
- `connect()` 立刻返回；第一笔 RPC 等这一次拨号（fail-fast）

## 依赖

- C++20
- CMake ≥ 3.20
- [libzmq](https://github.com/zeromq/libzmq)（本机树或系统包 `libzmq3-dev`）
- `external/` 已带 cppzmq 头、spdlog、doctest

## 构建

```bash
cmake -S . -B build
cmake --build build
cmake --install build
```

默认 `Release`。库装到 `build/install/lib/{debug,release}`，头文件装到 `build/install/include/zrpc`。

libzmq 默认 `${source}/../libzmq`（可用 `-DZMQ_DIR=` 改）。可选 `-DZRPC_BUILD_TESTS=OFF`、`-DZRPC_BUILD_EXAMPLES=OFF`。

```bash
cd build && ctest --output-on-failure
```

手动联调程序在 `examples/`，不进 CTest：

```bash
# 终端 1
./build/examples/rpc_server
# 终端 2
./build/examples/rpc_client echo   # 或 echo-async / get / put

./build/examples/topic_sub
./build/examples/topic_pub
```

## 快速开始

### RPC

```cpp
#include <memory>

#include <zrpc/Context.h>
#include <zrpc/Server.h>
#include <zrpc/Client.h>

class Echo : public zrpc::Service {
public:
    Echo() : Service("Echo") {
        addMethod("echo", [](const zrpc::PayloadView &in, zrpc::Payload &out) {
            for (auto v : in.views)
                out.emplace_back(v);
        });
    }
};

auto ctx = std::make_shared<zrpc::Context>();

zrpc::Server server(ctx);
server.registerService(std::make_unique<Echo>());
server.bind("tcp://127.0.0.1:5555");

zrpc::Client client(ctx);
client.connect("tcp://127.0.0.1:5555");

auto result = client.callMethod("Echo", "echo", zrpc::Payload{"hello"},
                                zrpc::CallOptions{.timeoutMs = 3000});
if (result.ok()) {
    // result.payload.views[0] == "hello"
}
```

### Pub / Sub

```cpp
#include <memory>
#include <string_view>

#include <zrpc/Context.h>
#include <zrpc/PubSub.h>

auto ctx = std::make_shared<zrpc::Context>();

zrpc::Publisher pub(ctx);
pub.bind("tcp://127.0.0.1:5556");

zrpc::Subscriber sub({"topic.a"}, ctx);
sub.setCallback([](std::string_view topic, zrpc::PayloadView payload) {
    (void)topic;
    (void)payload;
});
sub.connect("tcp://127.0.0.1:5556");

pub.pubTopic("topic.a", zrpc::Payload{"data"});
```

## 集成须知

| 主题 | 约定 |
|------|------|
| 超时 | 默认 `timeoutMs = -1`（永不超时）。生产 / 局域网建议每次调用显式设 `timeoutMs > 0`；到期由 poller 触发 `CallResult::Timeout` |
| 重试 | 库不自动重试。`TryAgain`（未入 ZMQ 发送队列）可立刻重发；`Disconnected` 后应发起**新**调用；`Timeout` 时对端可能已执行，写操作须业务幂等 |
| 连接 | `connect()` 非阻塞，只表示 dealer 侧已 `connect`；TCP 是否就绪以 socket monitor 为准，**第一笔 RPC** 才会等待本次拨号 |
| 一对象一地址 | 每个 `Client` / `Server` / `Publisher` / `Subscriber` 只绑定一个地址；已打开再 `bind`/`connect` 会抛，`close`/`disconnect` 后可换地址再开 |
| 对象生命周期 | 先销毁 endpoint 对象，再销毁 `Context`。`Server` / `Subscriber` 析构会 `close`/`disconnect` 并 `wait()` 排空在途工作；在飞 RPC 由 `CallHandle` 持有至完成 |
| 回调线程 | method、`onComplete`、topic 回调在 worker 线程执行，**不得抛异常**；回调内不要 `wait()` 或销毁触发该回调的同一对象 |
| 订阅回调 | `Subscriber::setCallback` 须在**首次** `connect()` 之前设置，之后不可更换 |
| 字节序与安全 | wire header 按本机字节序；无 TLS / 身份认证。节点须同 CPU 架构，且处于可信局域网 |
| Pub/Sub | topic 为尽力而为，慢订阅者与晚加入者会丢消息；控制面走 RPC，勿把 topic 当可靠指令队列 |

完整线程模型、事件总线、状态机、wire 格式见 [docs/DESIGN.md](docs/DESIGN.md)。
