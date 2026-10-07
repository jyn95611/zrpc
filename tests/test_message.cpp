#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <cstring>
#include <string>

#include "zrpc/Message.h"

TEST_CASE("RpcRequestHeader roundtrip")
{
    zrpc::RpcRequestHeader src(42, "Echo", "echo");
    auto msg = src.serialize();
    CHECK(zrpc::peekRequestId(msg) == 42);

    auto got = zrpc::RpcRequestHeader::deserialize(msg);
    REQUIRE(got);
    CHECK(got->requestId == 42);
    CHECK(got->serviceName == "Echo");
    CHECK(got->methodName == "echo");
}

TEST_CASE("RpcReplyHeader roundtrip")
{
    zrpc::RpcReplyHeader src(7, zrpc::ErrorCode::NoSuchMethod, "No such method.");
    auto msg = src.serialize();
    CHECK(zrpc::peekRequestId(msg) == 7);

    auto got = zrpc::RpcReplyHeader::deserialize(msg);
    REQUIRE(got);
    CHECK(got->requestId == 7);
    CHECK(got->errorCode == zrpc::ErrorCode::NoSuchMethod);
    CHECK(got->errorMsg == "No such method.");
}

TEST_CASE("deserialize rejects truncated and trailing bytes")
{
    auto full = zrpc::RpcRequestHeader(1, "S", "m").serialize();

    zmq::message_t truncated(full.size() > 0 ? full.size() - 1 : 0);
    if (full.size() > 1)
        std::memcpy(truncated.data(), full.data(), truncated.size());
    CHECK_FALSE(zrpc::RpcRequestHeader::deserialize(truncated).has_value());

    zmq::message_t trailing(full.size() + 4);
    std::memcpy(trailing.data(), full.data(), full.size());
    CHECK_FALSE(zrpc::RpcRequestHeader::deserialize(trailing).has_value());
}

TEST_CASE("deserialize rejects oversized string length")
{
    // requestId + uint32 len way above remaining bytes
    const uint64_t requestId = 1;
    const uint32_t huge = 0xFFFFFFFFu;
    zmq::message_t msg(sizeof(requestId) + sizeof(huge));
    auto *p = static_cast<char *>(msg.data());
    std::memcpy(p, &requestId, sizeof(requestId));
    std::memcpy(p + sizeof(requestId), &huge, sizeof(huge));
    CHECK_FALSE(zrpc::RpcRequestHeader::deserialize(msg).has_value());
}

TEST_CASE("peekRequestId on short buffer is 0")
{
    zmq::message_t empty;
    CHECK(zrpc::peekRequestId(empty) == 0);
}

TEST_CASE("errorReply header is readable")
{
    auto reply = zrpc::errorReply(zrpc::ErrorCode::InvalidMessage, 99, "bad");
    auto header = zrpc::RpcReplyHeader::deserialize(reply.header);
    REQUIRE(header);
    CHECK(header->requestId == 99);
    CHECK(header->errorCode == zrpc::ErrorCode::InvalidMessage);
    CHECK(header->errorMsg == "bad");
}

TEST_CASE("takePayload and viewPayload")
{
    zrpc::Payload payload{"ab", std::string(300, 'x')};
    zrpc::RpcMessage msg;
    msg.parts = zrpc::takePayload(std::move(payload));
    REQUIRE(msg.parts.size() == 2);
    CHECK(zrpc::viewMessage(msg.parts[0]) == "ab");
    CHECK(zrpc::viewMessage(msg.parts[1]).size() == 300);

    auto view = zrpc::viewPayload(msg);
    REQUIRE(view.views.size() == 2);
    CHECK(view.views[0] == "ab");
    CHECK(view.views[1].size() == 300);
}
