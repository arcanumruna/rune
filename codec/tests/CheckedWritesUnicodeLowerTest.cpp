// tests/CheckedWritesUnicodeLowerTest.cpp
#include "rune/codec/UnicodeLower.h"
#include <gtest/gtest.h>
#include <array>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

using namespace rune::codec;

namespace {

struct DataRow { uint32_t cp; uint32_t lower; };   // lower == 0: no mapping

std::vector<DataRow> loadUnicodeData() {
    std::ifstream f(RUNE_UNICODE_DATA);
    std::vector<DataRow> rows;
    std::string line;
    while (std::getline(f, line)) {
        std::vector<std::string> fld;
        std::stringstream ss(line);
        std::string x;
        while (std::getline(ss, x, ';')) fld.push_back(x);
        if (line.back() == ';') fld.emplace_back();
        if (fld.size() < 14) continue;
        const uint32_t cp = uint32_t(std::stoul(fld[0], nullptr, 16));
        const uint32_t lo = fld[13].empty() ? 0 : uint32_t(std::stoul(fld[13], nullptr, 16));
        rows.push_back({cp, lo});
    }
    return rows;
}

uint32_t utf8Len(uint32_t cp) { return cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4; }

std::vector<uint8_t> bytes(std::initializer_list<int> v) {
    std::vector<uint8_t> r;
    for (int b : v) r.push_back(uint8_t(b));
    return r;
}

LowerResult lower(const std::vector<uint8_t>& in, std::vector<uint8_t>& out) {
    return lowerUtf8(in, out);
}

} // namespace

TEST(CheckedWritesUnicodeLower, VersionIsPinnedTo16_0_0) {
    EXPECT_EQ(kUnicodeVersion, 0x00100000u);
    EXPECT_EQ(packUnicodeVersion(16, 0, 0), kUnicodeVersion);
    EXPECT_TRUE(unicodeVersionSupported(kUnicodeVersion));
    EXPECT_FALSE(unicodeVersionSupported(packUnicodeVersion(15, 1, 0)));
    EXPECT_FALSE(unicodeVersionSupported(packUnicodeVersion(17, 0, 0)));
    EXPECT_FALSE(unicodeVersionSupported(0));
}

TEST(CheckedWritesUnicodeLower, EveryCodePointMatchesUnicodeData16) {
    const auto rows = loadUnicodeData();
    ASSERT_FALSE(rows.empty());
    std::vector<uint32_t> expect(0x110000);
    for (uint32_t cp = 0; cp < 0x110000; ++cp) expect[cp] = cp;
    std::size_t mappings = 0, ascii = 0;
    for (const auto& r : rows) {
        if (r.lower == 0) continue;
        expect[r.cp] = r.lower;
        ++mappings;
        if (r.cp < 0x80) ++ascii;
    }
    EXPECT_EQ(mappings, kSimpleLowercaseMappings);
    EXPECT_EQ(ascii, 26u);
    EXPECT_EQ(mappings - ascii, 1434u);
    for (uint32_t cp = 0; cp < 0x110000; ++cp) {
        ASSERT_EQ(uint32_t(lowerCodePoint(cp)), expect[cp]) << "cp=" << std::hex << cp;
    }
}

TEST(CheckedWritesUnicodeLower, LengthTransitionsArePinned) {
    std::map<std::pair<uint32_t, uint32_t>, int> hist;
    std::vector<std::pair<uint32_t, uint32_t>> twoToThree;
    for (const auto& r : loadUnicodeData()) {
        if (r.lower == 0) continue;
        const auto key = std::make_pair(utf8Len(r.cp), utf8Len(r.lower));
        ++hist[key];
        if (key == std::make_pair(2u, 3u)) twoToThree.emplace_back(r.cp, r.lower);
    }
    const std::map<std::pair<uint32_t, uint32_t>, int> want = {
        {{1, 1}, 26}, {{2, 1}, 1}, {{2, 2}, 440}, {{2, 3}, 2}, {{3, 1}, 1}, {{3, 2}, 23}, {{3, 3}, 685}, {{4, 4}, 282}};
    EXPECT_EQ(hist, want);
    const std::vector<std::pair<uint32_t, uint32_t>> pair23 = {{0x23A, 0x2C65}, {0x23E, 0x2C66}};
    EXPECT_EQ(twoToThree, pair23);
}

TEST(CheckedWritesUnicodeLower, AsciiFastPath) {
    std::vector<uint8_t> out(8, 0xFF);
    auto r = lower({'F', 'o', 'O'}, out);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.len, 3u);
    EXPECT_EQ(out, bytes({'f', 'o', 'o', 0, 0, 0, 0, 0}));

    const std::string names = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for (char c : names) {
        std::vector<uint8_t> o(2, 0xFF);
        ASSERT_TRUE(lower({uint8_t(c)}, o).ok);
        const char want = (c >= 'A' && c <= 'Z') ? char(c + 0x20) : c;
        EXPECT_EQ(o[0], uint8_t(want));
        EXPECT_EQ(o[1], 0);
    }
}

TEST(CheckedWritesUnicodeLower, SixteenBytesOfGrowersFillTwentyFourExactly) {
    std::vector<uint8_t> in, want;
    for (int i = 0; i < 8; ++i) { in.push_back(0xC8); in.push_back(0xBA); want.insert(want.end(), {0xE2, 0xB1, 0xA5}); }
    std::vector<uint8_t> out(24, 0xFF);
    auto r = lower(in, out);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.len, 24u);
    EXPECT_EQ(out, want);
    std::vector<uint8_t> small(23);
    EXPECT_FALSE(lower(in, small).ok);
}

TEST(CheckedWritesUnicodeLower, DottedCapitalIShrinksToAsciiI) {
    std::vector<uint8_t> out(4, 0xFF);
    auto r = lower(bytes({0xC4, 0xB0}), out);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.len, 1u);
    EXPECT_EQ(out, bytes({0x69, 0, 0, 0}));

    out.assign(4, 0xFF);
    r = lower(bytes({0xE1, 0xBA, 0x9E}), out);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(r.len, 2u);
    EXPECT_EQ(out, bytes({0xC3, 0x9F, 0, 0}));

    out.assign(4, 0xFF);
    r = lower(bytes({0xE2, 0x84, 0xAA}), out);
    ASSERT_TRUE(r.ok);
    EXPECT_EQ(out, bytes({0x6B, 0, 0, 0}));

    out.assign(4, 0xFF);
    ASSERT_TRUE(lower(bytes({0x49}), out).ok);
    EXPECT_EQ(out, bytes({0x69, 0, 0, 0}));
}

TEST(CheckedWritesUnicodeLower, StopsAtTheFirstNul) {
    std::vector<uint8_t> a(8, 0xFF), b(8, 0xFF);
    ASSERT_TRUE(lower(bytes({'A', 'b', 0, 'C', 'd'}), a).ok);
    ASSERT_TRUE(lower(bytes({'A', 'b', 0, 'Z', 'z'}), b).ok);
    EXPECT_EQ(a, b);
    EXPECT_EQ(a, bytes({'a', 'b', 0, 0, 0, 0, 0, 0}));
}

TEST(CheckedWritesUnicodeLower, InvalidUtf8IsNotOk) {
    const std::vector<std::vector<uint8_t>> bad = {
        bytes({0xC0, 0x80}), bytes({0xED, 0xA0, 0x80}), bytes({0xF4, 0x90, 0x80, 0x80}), bytes({0xE2, 0xB1}), bytes({0x80})};
    for (const auto& in : bad) {
        std::vector<uint8_t> out(8);
        EXPECT_FALSE(lower(in, out).ok);
    }
}

TEST(CheckedWritesUnicodeLower, NoNormalization) {
    std::vector<uint8_t> a(8), b(8);
    ASSERT_TRUE(lower(bytes({0xC3, 0xA9}), a).ok);
    ASSERT_TRUE(lower(bytes({0x65, 0xCC, 0x81}), b).ok);
    EXPECT_NE(a, b);
}

TEST(CheckedWritesUnicodeLower, MaxLoweredWidthIsPinned) {
    const std::array<std::pair<uint32_t, uint32_t>, 9> pins = {{{1, 1}, {2, 3}, {3, 4}, {4, 6}, {5, 7}, {16, 24}, {64, 96}, {85, 127}, {128, 192}}};
    for (auto [n, w] : pins) EXPECT_EQ(maxLoweredWidth(n), w) << n;

    const uint32_t best[5] = {0, 1, 3, 3, 4};   // kMaxLoweredLenBySrcLen
    std::vector<uint32_t> dp(129, 0);
    for (uint32_t n = 1; n <= 128; ++n)
        for (uint32_t l = 1; l <= 4 && l <= n; ++l) dp[n] = std::max(dp[n], dp[n - l] + best[l]);
    for (uint32_t n = 1; n <= 128; ++n) {
        EXPECT_EQ(maxLoweredWidth(n), 3 * n / 2) << n;
        EXPECT_EQ(maxLoweredWidth(n), dp[n]) << n;
    }
}
