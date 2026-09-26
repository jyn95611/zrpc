#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <zmq.hpp>

#include "Call.h"

namespace zrpc {
struct SocketWriter;
struct SocketdReader;

constexpr uint32_t kMaxPartBytes = 1024u * 1024u * 1024u;
constexpr size_t kMaxPartCount = 8;
constexpr size_t kZeroCopyThreshold = 256;

struct RpcMessage
{
    zmq::message_t header;
    std::vector<zmq::message_t> parts;
    bool valid{true};
};

uint64_t peekRequestId(const zmq::message_t &header);

zmq::message_t takeMessage(std::string &&s);
std::vector<zmq::message_t> takePayload(Payload &&payload);
std::string_view viewMessage(const zmq::message_t &msg);
PayloadView viewPayload(const RpcMessage &msg, std::shared_ptr<void> owned = {});

void writeRpcMessage(SocketWriter &writer, RpcMessage &msg);
RpcMessage readRpcMessage(SocketdReader &reader);

RpcMessage errorReply(ErrorCode code, uint64_t requestId, std::string errorMsg = {});

struct RpcRequestHeader
{
    RpcRequestHeader(uint64_t requestId, std::string serviceName, std::string methodName);
    zmq::message_t serialize() const;
    static std::optional<RpcRequestHeader> deserialize(zmq::message_t &msg);

    const uint64_t requestId;
    std::string serviceName;
    std::string methodName;
};

struct RpcReplyHeader
{
    RpcReplyHeader(uint64_t requestId, ErrorCode errorCode, std::string errorMsg);
    zmq::message_t serialize() const;
    static std::optional<RpcReplyHeader> deserialize(zmq::message_t &msg);

    const uint64_t requestId;
    const ErrorCode errorCode{ErrorCode::Ok};
    std::string errorMsg;
};
}
