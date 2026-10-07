#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <zmq.hpp>

#include "Call.h"

namespace zrpc {
struct FrameSender;
struct FrameReceiver;

/** Maximum size of one payload frame (1 GiB). @c ZMQ_MAXMSGSIZE is not set. */
constexpr uint32_t kMaxPartBytes = 1024u * 1024u * 1024u;
/** Frames at or below this size are memcpy'd; larger frames use a ZMQ free callback. */
constexpr size_t kZeroCopyThreshold = 256;

/** One wire message: header frame plus payload parts. Integers are native endian. */
struct RpcMessage
{
    zmq::message_t header;
    std::vector<zmq::message_t> parts;
    bool valid{true};
};

/** Read the leading native-endian @c uint64 request id, or 0 if the frame is too short. */
uint64_t peekRequestId(const zmq::message_t &header);

zmq::message_t takeMessage(std::string &&s);
std::vector<zmq::message_t> takePayload(Payload &&payload);
std::string_view viewMessage(const zmq::message_t &msg);
PayloadView viewPayload(const RpcMessage &msg, std::shared_ptr<void> owned = {});

/**
 * Send header then parts. Stops after the first failed frame.
 * @return false if any send failed (@c SNDTIMEO=0).
 */
bool sendRpcMessage(FrameSender &sender, RpcMessage &msg);
/**
 * Receive one multipart message. Oversized frames set @c valid=false and drain the rest.
 */
RpcMessage recvRpcMessage(FrameReceiver &receiver);

RpcMessage errorReply(ErrorCode code, uint64_t requestId, std::string errorMsg = {});

/** RPC request header: requestId, service name, method name. */
struct RpcRequestHeader
{
    RpcRequestHeader(uint64_t requestId, std::string serviceName, std::string methodName)
        : requestId(requestId),
          serviceName(std::move(serviceName)),
          methodName(std::move(methodName))
    {
    }
    zmq::message_t serialize() const;
    static std::optional<RpcRequestHeader> deserialize(zmq::message_t &msg);

    const uint64_t requestId;
    std::string serviceName;
    std::string methodName;
};

/** RPC reply header: requestId, error code, optional error string. */
struct RpcReplyHeader
{
    RpcReplyHeader(uint64_t requestId, ErrorCode errorCode, std::string errorMsg)
        : requestId(requestId),
          errorCode(errorCode),
          errorMsg(std::move(errorMsg))
    {
    }
    zmq::message_t serialize() const;
    static std::optional<RpcReplyHeader> deserialize(zmq::message_t &msg);

    const uint64_t requestId;
    const ErrorCode errorCode{ErrorCode::Ok};
    std::string errorMsg;
};
}
