---
name: zrpc Code Review
overview: 剩余工作清单。P0 超时 / Pub 生命周期 / Stub API / 反序列化校验已落地；P1 Context internal 化与命名修正已完成；P2 序列化方案 A 已落地；下一步 Service owning / findMethod。
todos:
  - id: harden-serialization
    content: "P0: Message deserialize 边界校验、len 上限、失败返回 false"
    status: completed
  - id: service-owning
    content: "P1: Service owning 注册（shared_ptr）；findMethod 不用 static 空引用"
    status: pending
  - id: sub-filter
    content: "P1: 按 topic 调用 zmq subscribe，去掉 subscribe(\"\") + 应用层过滤"
    status: pending
  - id: multi-context
    content: "P1: inproc 地址改为实例唯一，支持同进程多 Context"
    status: pending
  - id: perf-serialization
    content: "P2: 日志 / ZMQ HWM / 字节序 / multipart 大消息"
    status: pending
  - id: engineering
    content: "P3: CMake 便携化、单元测试、README、send 失败传播、d-pointer unique_ptr"
    status: pending
  - id: fix-timeout
    content: "P0: 超时改用 runCallbackAfter"
    status: completed
  - id: fix-pub-lifecycle
    content: "P0: onRemovePubEvent 去掉 removeSocket"
    status: completed
  - id: refactor-stub-api
    content: "P0: 删除 Rpc，callMethod 返回 CallResult；已加 callMethodAsync"
    status: completed
  - id: context-internal
    content: "P1: Context d() 移入 ContextAccess internal header"
    status: completed
  - id: naming-fixes
    content: "P1: registService→registerService；clinetSocketId→clientSocketId；清理 NACTIVE/ACTIVE"
    status: completed
  - id: wire-serialize-opt
    content: "P2: 序列化直写 zmq::message_t，去掉 stringstream / to_string 拷贝"
    status: completed
---

# zrpc 评审待办

架构（Reactor + Worker + inproc 总线）保持不变。对外 API 已是 `stub.callMethod(...) → CallResult`。

## 进度总览

| 优先级 | 项 | 状态 |
|--------|----|------|
| P0 | RPC 超时 | **已修**：`runCallbackAfter` |
| P0 | Pub `onRemovePubEvent` | **已修**：只 `_pubSockets.erase` |
| P0 | 删除 public `Rpc`，Stub 返回 `CallResult` | **已修**：含 `callMethodAsync` / `CallHandle` |
| P0 | `deserialize()` 边界校验 | **已修**：见下方 Message 优化 |
| P1 | Context internal 化 | **已修**：`ContextAccess` |
| P1 | 命名修正 | **已修**：`registerService` / `clientSocketId` / `RpcRequestStatus` |
| P1 | Service owning / `findMethod` | 未改 |
| P1 | Sub ZMQ topic 过滤 | 未改 |
| P1 | 多 Context 唯一 inproc | 未改 |
| P2 | 序列化直写 `zmq::message_t` | **已修**：方案 A |
| P2 | 日志、ZMQ 调优、Worker 调度、大消息 multipart | 未改 |
| P3 | CMake、测试、README、错误传播 | 未改 |

**下一步：** Service owning / `findMethod` → Sub 过滤 / 多 Context。

---

## 已完成

- **超时**：`Poller::runCallbackAfter`；`onSendRequestEvent` 已改用
- **Pub**：`onAddPubEvent` 不加入 Poller（正确）；`onRemovePubEvent` 去掉 `removeSocket`
- **API**：删除 `Rpc.h` / `Rpc.cpp`；`CallResult` / `CallOptions` / `ErrorCode` / `CallHandle`；测试已切新 API
- **Context internal 化**：新增 [`ContextAccess.h`](src/zrpc/ContextAccess.h)（不 install）；公共 [`Context.h`](include/zrpc/Context.h) 去掉 8 个 `friend` 与 `d()`，仅保留 `friend struct detail::ContextAccess`；Channel/Server/PubSub 改经 `detail::ContextAccess::get(ctx)` 访问
- **命名修正**：`registService` → `registerService`；`clinetSocketId` → `clientSocketId`；`RpcRequestStatus` 精简为 `Done` / `DeadlineExceeded`（不保留旧数值）
- **Message 序列化 / 反序列化**（P0 + P2 方案 A）：
  - 移除 `Serializer` / `Deserializer`（`stringstream` 路径）
  - `serialize()`：预计算长度 → 一次分配 `zmq::message_t` → `memcpy` 直写
  - `deserialize()`：从 `msg.data()` 指针区间读取，不再 `to_string()`
  - 边界校验：剩余字节检查；单字段 `len` 上限 64MB（`kMaxWireStringLen`）；读完后要求 `p == end`；失败返回 `false`
  - Wire 格式不变（`uint32_t len + bytes` / native-endian `int`），与旧二进制兼容
  - [`Server.cpp`](src/zrpc/Server.cpp)、[`Channel.cpp`](src/zrpc/Channel.cpp)、[`PubSub.cpp`](src/zrpc/PubSub.cpp) 已有 `if (!deserialize)` 分支现可正常触发

---

## 剩余 — P1 设计改进

### 1. Service 生命周期非 owning

[`Server.cpp`](src/zrpc/Server.cpp) 存储 `Service*` 裸指针，调用方必须保证 Service 比 Server 活得久。无注销 API。

**改法：** `registerService(std::shared_ptr<Service>)` 或内部持有副本；析构时自动 unbind。

### 2. `findMethod` 返回 static 空 Method 的引用

```15:19:src/zrpc/Server.cpp
const Service::Method &Service::findMethod(const std::string &name)
{
    static Method emptyMethod;
    return iter != _methods.end() ? iter->second : emptyMethod;
}
```

当前靠 `if (method)` 判断，`std::function` 的 truthiness 是隐患。

**改法：** `bool findMethod(name, Method& out)` 或 `std::optional`。

### 3. Subscriber 订阅策略低效

[`onAddSubEvent`](src/zrpc/Context.cpp) 仍 `subscribe("")` 收全部消息，Worker 里 `std::find` 过滤（[`PubSub.cpp`](src/zrpc/PubSub.cpp)）。高吞吐多 topic 时所有订阅者都会收到全部消息再过滤。

**改法：** 构造 Subscriber 时对每个 topic 调用 `subSocket->set(zmq::sockopt::subscribe, topic)`；Pub 改 multipart（topic 帧 + data 帧），ZMQ 才能按前缀过滤；去掉应用层 `std::find`（或仅作二次校验）。动态 topic 可用 prefix 订阅。

### 4. 固定 inproc 地址，不支持多 Context

[`Context.cpp`](src/zrpc/Context.cpp) 硬编码 `inproc://context_backend`，`_frontendId` 写死为 `"context_frontend"`。同进程多个 `Context` 会冲突。

**改法：** 构造时生成唯一 id，例如 `inproc://zrpc_<pid>_<id>`。

### 5. 通过 ZMQ 传输 raw pointer（低优先级）

[`utils.h`](src/zrpc/utils.h) 的 `writePtr/readPtr` 将 `Event*` 写入 ZMQ frame。只能同进程 inproc 工作，无类型安全。

**建议：** 短期保留并在 README 写清边界；长期再改 serializable command / 内存队列。

---

## 剩余 — P2 性能

| 区域 | 现状 | 建议 |
|------|------|------|
| 序列化 | ~~`stringstream` → `string` → `zmq::message_t`~~ **已改**：直写 `zmq::message_t` | 可选 reusable buffer（高频小消息） |
| 大消息 | API 仍用 `std::string` 承载 payload；24MB 测试仍有业务层拷贝 | multipart 分离 header/payload；评估 zero-copy / 流式 |
| Worker 调度 | Round-robin + 每次拷贝 `std::function` | 按 socketId/requestId hash 保序；减少 callback 拷贝 |
| Poller | `_dirty` 时 rebuild 全部 pollitems | socket 少时可接受 |
| 日志 | 热路径大量 `std::cout`（Context/Poller） | 可插拔 logger + level；默认关闭 debug |
| ZMQ 选项 | 仅部分 socket 设了 `LINGER`；未设 HWM / TCP keepalive | 生产环境需配置，防止慢消费者堆积 |
| 字节序 | `Message.cpp` / `utils.h` 仍用 native endian 写 fundamental type | 跨架构应统一 little-endian 或 protobuf |
| 帧头 | 无 magic / version | 可选 `uint32_t magic + version` 便于协议演进 |

---

## 剩余 — P3 工程化

| 项 | 现状 | 改法 |
|----|------|------|
| CMake | `src/zrpc/CMakeLists.txt` 仍硬编码本机 `ZMQ_DIR` / `CPPZMQ_DIR`；`GLOB_RECURSE` 把头文件编进 target | `find_package(cppzmq)` / pkg-config；GLOB 仅 `.cpp` |
| 测试 | 仅手动 integration test；`test_zrpc.cpp` 为空；无 Message / 超时单测 | 补 deserialize 单元测试、RPC 超时集成测试 |
| 文档 | README 仍是 GitLab 模板 | 架构图、线程模型、Stub 不可并发、inproc 单进程限制、wire protocol |
| 错误处理 | `SocketWriter::write` 返回值未检查；send 失败静默丢失 | 失败 propagate 到 `CallResult` |
| 内存 | Channel/Server/PubSub/Context 的 d-pointer 仍手动 `new/delete` | 改 `unique_ptr` |
| 异步等待 | `CallHandle::get()` 无限 `cv.wait`，与 RPC `timeoutMs` 无联动 | 可选 `wait_for`；超时路径已能 `complete(Timeout)` |

---

## 明确不改

| 项 | 说明 |
|----|------|
| Pub socket 加入 Poller | **不需要**，add / remove 路径现已一致 |
| Stub 并发加锁 | **不做**；进行中再调返回 `CallInProcess` |
| raw pointer 传 Event | 短期保留（inproc 单进程），文档说明边界 |
| 再引入 public `Rpc` | **不做**；继续用 `CallResult` / `CallHandle` |

---

## 路线图

| 阶段 | 内容 | 预估 |
|------|------|------|
| ~~P0~~ | ~~Message deserialize 边界 / `len` 上限 / 失败返回 `false`~~ | **已完成** |
| P1（部分完成） | ~~Context internal 化、命名~~；剩余 Service owning、`findMethod`、Sub ZMQ 过滤、多 Context | ~~2-3 天~~ → 剩余 ~1.5 天 |
| P2（部分完成） | ~~序列化直写 `zmq::message_t`~~；剩余日志、ZMQ HWM、Worker 调度、大消息 multipart | 按需 |
| P3 | 单元测试、CMake 便携化、README、错误传播、d-pointer `unique_ptr` | 按需 |
