#pragma once

#include <iostream>
#include <string>
#include <string_view>

#include "zrpc/Call.h"

inline constexpr const char *kRpcAddr = "tcp://127.0.0.1:9981";
inline constexpr const char *kTopicAddr = "tcp://127.0.0.1:9983";

inline constexpr const char *kService = "Demo";
inline constexpr const char *kEcho = "echo";
inline constexpr const char *kGet = "get";
inline constexpr const char *kPut = "put";

inline constexpr size_t kGetBytes = 30 * 10000 * 24;
inline constexpr size_t kPutBytes = 100 * 10000 * 24;

inline std::string_view firstPart(const zrpc::CallResult &result)
{
    return result.payload.views.empty() ? std::string_view{} : result.payload.views[0];
}

inline void printResult(const zrpc::CallResult &result)
{
    if (result.ok()) {
        std::cout << "ok: " << firstPart(result) << '\n';
        return;
    }
    std::cout << "error " << static_cast<int>(result.errorCode)
              << ": " << result.errorMsg << '\n';
}

inline std::string argOr(int argc, char **argv, int index, const char *fallback)
{
    return index < argc ? argv[index] : fallback;
}

inline bool wantsHelp(int argc, char **argv)
{
    if (argc < 2)
        return false;
    const std::string_view a{argv[1]};
    return a == "-h" || a == "--help";
}
