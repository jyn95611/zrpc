#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include "zrpc/utils.h"

TEST_CASE("normalizeZmqAddr replaces localhost")
{
    CHECK(zrpc::normalizeZmqAddr("tcp://localhost:5555") == "tcp://127.0.0.1:5555");
    CHECK(zrpc::normalizeZmqAddr("tcp://LOCALHOST:1") == "tcp://127.0.0.1:1");
    CHECK(zrpc::normalizeZmqAddr("tcp://127.0.0.1:5555") == "tcp://127.0.0.1:5555");
    CHECK(zrpc::normalizeZmqAddr("inproc://localhost") == "inproc://127.0.0.1");
}

TEST_CASE("normalizeZmqAddr leaves non-localhost hosts")
{
    CHECK(zrpc::normalizeZmqAddr("tcp://example.com:80") == "tcp://example.com:80");
    CHECK(zrpc::normalizeZmqAddr("tcp://localhostx:80") == "tcp://localhostx:80");
    CHECK(zrpc::normalizeZmqAddr("not-an-addr") == "not-an-addr");
}

TEST_CASE("zclock_time is monotonic")
{
    const auto a = zrpc::zclock_time();
    const auto b = zrpc::zclock_time();
    CHECK(b >= a);
}
