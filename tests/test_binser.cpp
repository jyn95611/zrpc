#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "binser.h"

namespace {

struct Pose
{
    float x{0};
    float y{0};
    float z{0};
};

enum class Color : int
{
    Red = 1,
    Blue = 2,
};

template <class Ser>
void serialize(Ser &ser, Pose &pose)
{
    ser(pose.x, pose.y, pose.z);
}

} // namespace

TEST_CASE("arithmetic and enum roundtrip")
{
    const int srcI = -7;
    const std::uint64_t srcU = 42;
    const Color srcC = Color::Blue;

    std::string buf;
    REQUIRE(binser::toBinary(buf, srcI, srcU, srcC));
    CHECK(buf.size() == sizeof(int) + sizeof(std::uint64_t) + sizeof(int));

    int dstI = 0;
    std::uint64_t dstU = 0;
    Color dstC = Color::Red;
    REQUIRE(binser::fromBinary(buf, dstI, dstU, dstC));
    CHECK(dstI == srcI);
    CHECK(dstU == srcU);
    CHECK(dstC == srcC);
}

TEST_CASE("string and vector roundtrip")
{
    const std::string srcS;
    const std::string srcT = "hello";
    const std::vector<int> srcV{1, 2, 3};
    const std::vector<bool> srcB{true, false, true};
    const std::vector<std::string> srcN{"a", "", "bc"};

    std::string buf;
    REQUIRE(binser::toBinary(buf, srcS, srcT, srcV, srcB, srcN));

    std::string dstS = "x";
    std::string dstT;
    std::vector<int> dstV;
    std::vector<bool> dstB;
    std::vector<std::string> dstN;
    REQUIRE(binser::fromBinary(buf, dstS, dstT, dstV, dstB, dstN));
    CHECK(dstS.empty());
    CHECK(dstT == srcT);
    CHECK(dstV == srcV);
    CHECK(dstB.size() == 3);
    CHECK(dstB[0] == true);
    CHECK(dstB[1] == false);
    CHECK(dstB[2] == true);
    CHECK(dstN == srcN);
}

TEST_CASE("custom type via serialize(Ser&, T&)")
{
    const Pose src{1.5f, -2.f, 3.f};
    std::string buf;
    REQUIRE(binser::toBinary(buf, src));
    CHECK(buf.size() == 3 * sizeof(float));

    Pose dst;
    REQUIRE(binser::fromBinary(buf, dst));
    CHECK(dst.x == src.x);
    CHECK(dst.y == src.y);
    CHECK(dst.z == src.z);
}

TEST_CASE("vector of custom type")
{
    const std::vector<Pose> src{{1, 2, 3}, {4, 5, 6}};
    std::string buf;
    REQUIRE(binser::toBinary(buf, src));

    std::vector<Pose> dst;
    REQUIRE(binser::fromBinary(buf, dst));
    REQUIRE(dst.size() == 2);
    CHECK(dst[1].z == 6.f);
}

TEST_CASE("const object can be saved")
{
    const Pose src{9, 8, 7};
    const std::string name = "p";
    std::string buf;
    REQUIRE(binser::toBinary(buf, src, name));

    Pose dst;
    std::string dstName;
    REQUIRE(binser::fromBinary(buf, dst, dstName));
    CHECK(dst.x == 9.f);
    CHECK(dstName == "p");
}

TEST_CASE("deserialize rejects truncated and trailing bytes")
{
    Pose src{1, 2, 3};
    std::string full;
    REQUIRE(binser::toBinary(full, src));

    std::string truncated = full.substr(0, full.size() - 1);
    Pose dst;
    CHECK_FALSE(binser::fromBinary(truncated, dst));

    std::string trailing = full;
    trailing.append("xxxx");
    CHECK_FALSE(binser::fromBinary(trailing, dst));
}

TEST_CASE("deserialize rejects oversized string length")
{
    std::string buf(sizeof(std::uint32_t), '\0');
    const std::uint32_t huge = 1024;
    std::memcpy(buf.data(), &huge, sizeof(huge));
    std::string dst;
    CHECK_FALSE(binser::fromBinary(buf, dst));
}

TEST_CASE("empty buffer fails for non-empty payload")
{
    int v = 1;
    CHECK_FALSE(binser::fromBinary(std::string{}, v));
}

TEST_CASE("array pair optional map set roundtrip")
{
    const std::array<int, 3> srcA{1, 2, 3};
    const std::array<Pose, 2> srcP{{{1, 2, 3}, {4, 5, 6}}};
    const std::pair<int, std::string> srcPair{7, "hi"};
    const std::optional<int> srcNone;
    const std::optional<Pose> srcSome{Pose{9, 8, 7}};
    const std::map<std::string, int> srcMap{{"a", 1}, {"b", 2}};
    const std::unordered_map<int, std::string> srcUMap{{1, "x"}, {2, "y"}};
    const std::set<int> srcSet{3, 1, 2};
    const std::unordered_set<std::string> srcUSet{"p", "q"};

    std::string buf;
    REQUIRE(binser::toBinary(buf, srcA, srcP, srcPair, srcNone, srcSome, srcMap, srcUMap, srcSet, srcUSet));

    std::array<int, 3> dstA{};
    std::array<Pose, 2> dstP{};
    std::pair<int, std::string> dstPair;
    std::optional<int> dstNone{0};
    std::optional<Pose> dstSome;
    std::map<std::string, int> dstMap;
    std::unordered_map<int, std::string> dstUMap;
    std::set<int> dstSet;
    std::unordered_set<std::string> dstUSet;
    REQUIRE(binser::fromBinary(buf, dstA, dstP, dstPair, dstNone, dstSome, dstMap, dstUMap, dstSet, dstUSet));

    CHECK(dstA == srcA);
    CHECK(dstP[1].z == 6.f);
    CHECK(dstPair == srcPair);
    CHECK_FALSE(dstNone.has_value());
    REQUIRE(dstSome.has_value());
    CHECK(dstSome->x == 9.f);
    CHECK(dstMap == srcMap);
    CHECK(dstUMap == srcUMap);
    CHECK(dstSet == srcSet);
    CHECK(dstUSet == srcUSet);
}

TEST_CASE("empty array and nested containers")
{
    const std::array<int, 0> srcEmpty{};
    const std::map<std::string, std::vector<int>> srcNested{{"k", {1, 2}}};
    const std::optional<std::array<int, 2>> srcOptArr{std::array<int, 2>{8, 9}};

    std::string buf;
    REQUIRE(binser::toBinary(buf, srcEmpty, srcNested, srcOptArr));

    std::array<int, 0> dstEmpty{};
    std::map<std::string, std::vector<int>> dstNested;
    std::optional<std::array<int, 2>> dstOptArr;
    REQUIRE(binser::fromBinary(buf, dstEmpty, dstNested, dstOptArr));
    CHECK(dstNested == srcNested);
    REQUIRE(dstOptArr.has_value());
    CHECK((*dstOptArr)[1] == 9);
}
