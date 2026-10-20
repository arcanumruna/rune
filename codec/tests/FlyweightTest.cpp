// tests/FlyweightTest.cpp — Phase 2b Part B: rune::codec::fly load/store helpers
// (the memcpy little-endian access every generated flyweight uses).
#include <gtest/gtest.h>
#include <rune/codec/Flyweight.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

namespace fly = rune::codec::fly;

TEST(Flyweight, StoresPackedLittleEndianAtAnOddAddress) {
    uint8_t buf[16] = {};
    fly::store<uint32_t>(buf + 1, 0x11223344u);
    EXPECT_EQ(buf[1], 0x44);
    EXPECT_EQ(buf[2], 0x33);
    EXPECT_EQ(buf[3], 0x22);
    EXPECT_EQ(buf[4], 0x11);
    EXPECT_EQ(fly::load<uint32_t>(buf + 1), 0x11223344u);
    fly::store<int64_t>(buf + 3, int64_t{-2});
    EXPECT_EQ(buf[3], 0xFE);
    for (int i = 4; i < 11; ++i) EXPECT_EQ(buf[i], 0xFF) << i;
    EXPECT_EQ(fly::load<int64_t>(buf + 3), int64_t{-2});
}

TEST(Flyweight, RoundTripsEveryFixedScalarType) {
    uint8_t buf[9] = {};
    auto rt = [&](auto v) {
        using T = decltype(v);
        fly::store<T>(buf + 1, v);
        return fly::load<T>(buf + 1);
    };
    EXPECT_EQ(rt(std::numeric_limits<int32_t>::min()), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(rt(std::numeric_limits<int64_t>::min()), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(rt(uint16_t{65535}), uint16_t{65535});
    EXPECT_EQ(rt(uint32_t{4'000'000'000u}), uint32_t{4'000'000'000u});
    EXPECT_EQ(rt(std::numeric_limits<uint64_t>::max()), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(rt(-3.25), -3.25);
    EXPECT_TRUE(std::signbit(rt(-0.0)));
    EXPECT_TRUE(rt(true));
    EXPECT_FALSE(rt(false));
}

TEST(Flyweight, DoubleIsItsIeeeBitPatternLittleEndian) {
    uint8_t buf[8] = {};
    fly::store<double>(buf, 1.0);  // 0x3FF0000000000000
    const uint8_t want[8] = {0, 0, 0, 0, 0, 0, 0xF0, 0x3F};
    for (int i = 0; i < 8; ++i) EXPECT_EQ(buf[i], want[i]) << i;
}

TEST(Flyweight, BoolIsOneByteAndAnyNonZeroReadsTrue) {
    uint8_t b = 0xAA;
    fly::store<bool>(&b, true);
    EXPECT_EQ(b, 1);
    fly::store<bool>(&b, false);
    EXPECT_EQ(b, 0);
    b = 7;
    EXPECT_TRUE(fly::load<bool>(&b));
}

TEST(Flyweight, FixedStrPadsWithZeroesAndTruncates) {
    uint8_t buf[6];
    std::memset(buf, 0xEE, sizeof buf);
    fly::storeFixedStr(buf, "AB", 6);
    EXPECT_EQ(fly::loadFixedStr(buf, 6), std::string_view("AB\0\0\0\0", 6));
    fly::storeFixedStr(buf, "TOO-LONG", 6);
    EXPECT_EQ(fly::loadFixedStr(buf, 6), std::string_view("TOO-LO", 6));
    fly::storeFixedStr(buf, "", 6);
    EXPECT_EQ(fly::loadFixedStr(buf, 6), std::string_view("\0\0\0\0\0\0", 6));
}
