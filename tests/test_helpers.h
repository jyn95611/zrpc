#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

#include "zrpc/Call.h"
#include "zrpc/Client.h"
#include "zrpc/Context.h"
#include "zrpc/Server.h"

inline std::string testAddr()
{
    static std::atomic<int> port{23000 + static_cast<int>(::getpid() % 500) * 20};
    return "tcp://127.0.0.1:" + std::to_string(port.fetch_add(1));
}

inline std::string_view firstPart(const zrpc::CallResult &result)
{
    return result.payload.views.empty() ? std::string_view{} : result.payload.views[0];
}

inline std::shared_ptr<zrpc::Context> testContext()
{
    return std::make_shared<zrpc::Context>(1, 2);
}

class EchoService : public zrpc::Service
{
public:
    EchoService() : Service("Echo")
    {
        addMethod("echo", [](const zrpc::PayloadView &in, zrpc::Payload &out) {
            for (auto v : in.views)
                out.emplace_back(std::string(v));
        });
        addMethod("sleep", [](const zrpc::PayloadView &, zrpc::Payload &out) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            out.emplace_back("slept");
        });
    }
};
