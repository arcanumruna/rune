// tests/Utf8Test.cpp
#include <gtest/gtest.h>
#include <rune/codec/Utf8.h>
#include <random>
#include <vector>
using namespace rune::codec;

namespace {
Utf8Result check(const std::vector<uint8_t>& b) { return validateUtf8(b); }

// Straight code-point decoder, independent of validateUtf8's table.
bool referenceValid(const std::vector<uint8_t>& s) {
    size_t i = 0;
    while (i < s.size()) {
        const uint8_t b = s[i];
        size_t len = 0;
        uint32_t cp = 0, min = 0;
        if (b < 0x80) { ++i; continue; }
        else if ((b & 0xE0) == 0xC0) { len = 2; cp = b & 0x1F; min = 0x80; }
        else if ((b & 0xF0) == 0xE0) { len = 3; cp = b & 0x0F; min = 0x800; }
        else if ((b & 0xF8) == 0xF0) { len = 4; cp = b & 0x07; min = 0x10000; }
        else return false;
        if (i + len > s.size()) return false;
        for (size_t k = 1; k < len; ++k) {
            if ((s[i + k] & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (s[i + k] & 0x3F);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        i += len;
    }
    return true;
}
} // namespace

TEST(CheckedWritesUtf8, AcceptsAsciiAndEveryWidth) {
    EXPECT_TRUE(check({}).ok());
    EXPECT_TRUE(check({'F', 'o', 'o'}).ok());
    EXPECT_TRUE(check({0xC3, 0xA9}).ok());
    EXPECT_TRUE(check({0xE2, 0x82, 0xAC}).ok());
    EXPECT_TRUE(check({0xF0, 0x9F, 0x98, 0x80}).ok());
    EXPECT_TRUE(check({0xEF, 0xBF, 0xBF}).ok());
    EXPECT_TRUE(check({0xF4, 0x8F, 0xBF, 0xBF}).ok());
    EXPECT_TRUE(check(std::vector<uint8_t>(1000, 'a')).ok());
}

TEST(CheckedWritesUtf8, RejectsEachInvalidClass) {
    struct Row { std::vector<uint8_t> bytes; Utf8Error error; size_t offset; };
    const Row rows[] = {
        {{0xC0, 0xAF}, Utf8Error::Overlong, 0},
        {{0xE0, 0x80, 0xAF}, Utf8Error::Overlong, 0},
        {{0xF0, 0x80, 0x80, 0xAF}, Utf8Error::Overlong, 0},
        {{0xED, 0xA0, 0x80}, Utf8Error::Surrogate, 0},
        {{0xED, 0xBF, 0xBF}, Utf8Error::Surrogate, 0},
        {{0xF4, 0x90, 0x80, 0x80}, Utf8Error::AboveMax, 0},
        {{0xF5, 0x80, 0x80, 0x80}, Utf8Error::InvalidLead, 0},
        {{0xFF}, Utf8Error::InvalidLead, 0},
        {{0x61, 0x80}, Utf8Error::StrayContinuation, 1},
        {{0xE2, 0x82}, Utf8Error::Truncated, 0},
        {{0x61, 0xF0, 0x9F, 0x98}, Utf8Error::Truncated, 1},
        {{0xC3, 0x41}, Utf8Error::Truncated, 0},
    };
    for (const auto& r : rows) {
        const Utf8Result got = check(r.bytes);
        EXPECT_EQ(got.error, r.error) << "first byte " << int(r.bytes[0]);
        EXPECT_EQ(got.offset, r.offset) << "first byte " << int(r.bytes[0]);
    }
}

TEST(CheckedWritesUtf8, FastPathBoundaryAtEveryOffset) {
    for (size_t k = 0; k <= 16; ++k) {
        std::vector<uint8_t> bad(k, 'a');
        bad.push_back(0x80);
        const Utf8Result r = check(bad);
        EXPECT_EQ(r.error, Utf8Error::StrayContinuation) << k;
        EXPECT_EQ(r.offset, k);
        std::vector<uint8_t> good(k, 'a');
        good.push_back(0xC3);
        good.push_back(0xA9);
        EXPECT_TRUE(check(good).ok()) << k;
    }
}

TEST(CheckedWritesUtf8Fuzz, AgreesWithTheCodePointDecoder) {
    for (uint32_t seed = 1; seed <= 32; ++seed) {
        std::mt19937 rng(seed);
        for (int iter = 0; iter < 10000; ++iter) {
            std::vector<uint8_t> s(rng() % 65);
            for (auto& b : s) b = (rng() % 3 == 0) ? uint8_t(rng()) : uint8_t(0x80 + rng() % 0x75);
            if (rng() % 2 == 0)
                for (auto& b : s) if (b >= 0xF5 && rng() % 4 != 0) b = uint8_t(0xC2 + rng() % 0x33);
            ASSERT_EQ(check(s).ok(), referenceValid(s)) << "seed " << seed << " iter " << iter;
        }
    }
}
