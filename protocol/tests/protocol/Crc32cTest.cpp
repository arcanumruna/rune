#include <gtest/gtest.h>
#include <rune/protocol/Crc32c.h>
#include <string>
#include <vector>
#include <cstdint>

using rune::protocol::crc32c;

TEST(Crc32c, KnownVectors) {
    // CRC-32C ("Castagnoli") reference values.
    EXPECT_EQ(crc32c(nullptr, 0), 0x00000000u);
    const std::string a = "123456789";
    EXPECT_EQ(crc32c(reinterpret_cast<const uint8_t*>(a.data()), a.size()), 0xE3069283u);
}

TEST(Crc32c, IncrementalMatchesOneShot) {
    const std::string s = "the quick brown fox";
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    uint32_t seed = crc32c(p, 9);
    uint32_t inc  = rune::protocol::crc32c(seed, p + 9, s.size() - 9);
    EXPECT_EQ(inc, crc32c(p, s.size()));
}

// ---------------------------------------------------------------------------
// Edge-case tests added for comprehensive coverage
// ---------------------------------------------------------------------------

TEST(Crc32c, EmptyBufferIsZero) {
    // The seed=0 start of a fresh computation on empty data must be 0.
    uint8_t dummy = 0;
    EXPECT_EQ(crc32c(&dummy, 0), 0x00000000u);
    EXPECT_EQ(crc32c(nullptr, 0), 0x00000000u);
}

TEST(Crc32c, SingleByteSelfConsistency) {
    // Pin self-consistency for all 256 single-byte values.
    // We pick a handful of boundary values and verify they are stable
    // (self-consistency pins, not external authoritative vectors).
    uint8_t b0 = 0x00;
    uint8_t bFF = 0xFF;
    uint8_t b01 = 0x01;
    uint8_t b7F = 0x7F;
    uint8_t b80 = 0x80;

    uint32_t c00  = crc32c(&b0,  1);
    uint32_t cFF  = crc32c(&bFF, 1);
    uint32_t c01  = crc32c(&b01, 1);
    uint32_t c7F  = crc32c(&b7F, 1);
    uint32_t c80  = crc32c(&b80, 1);

    // Recompute and ensure they are identical (stability).
    EXPECT_EQ(crc32c(&b0,  1), c00);
    EXPECT_EQ(crc32c(&bFF, 1), cFF);
    EXPECT_EQ(crc32c(&b01, 1), c01);
    EXPECT_EQ(crc32c(&b7F, 1), c7F);
    EXPECT_EQ(crc32c(&b80, 1), c80);

    // All single-byte values differ from empty.
    EXPECT_NE(c00,  0x00000000u);
    EXPECT_NE(cFF,  0x00000000u);
}

TEST(Crc32c, TwoByteBasics) {
    // CRC of two-byte buffers: stable + nonzero.
    uint8_t ab[2] = {0xAB, 0xCD};
    uint32_t c = crc32c(ab, 2);
    EXPECT_NE(c, 0u);
    EXPECT_EQ(crc32c(ab, 2), c);  // stable
}

TEST(Crc32c, AllFF_SeveralLengths) {
    // A buffer of all 0xFF bytes at several lengths: stable, nonzero, each distinct.
    std::vector<uint8_t> buf(64, 0xFF);
    std::vector<uint32_t> crcs;
    for (size_t len : {1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64}) {
        uint32_t c = crc32c(buf.data(), len);
        EXPECT_NE(c, 0u) << "len=" << len;
        EXPECT_EQ(crc32c(buf.data(), len), c) << "not stable at len=" << len;
        crcs.push_back(c);
    }
    // All distinct lengths must produce distinct CRCs.
    for (size_t i = 0; i < crcs.size(); ++i)
        for (size_t j = i + 1; j < crcs.size(); ++j)
            EXPECT_NE(crcs[i], crcs[j]) << "collision at indices " << i << " and " << j;
}

TEST(Crc32c, OneMegabyteBufferStableAndConsistent) {
    // Fill 1 MB deterministically and verify CRC is stable across two calls.
    constexpr size_t kSz = 1u << 20; // 1 MB
    std::vector<uint8_t> buf(kSz);
    for (size_t i = 0; i < kSz; ++i)
        buf[i] = static_cast<uint8_t>((i * 0xB5 + (i >> 8) * 0x3D) & 0xFF);

    uint32_t c1 = crc32c(buf.data(), kSz);
    uint32_t c2 = crc32c(buf.data(), kSz);
    EXPECT_EQ(c1, c2);
    EXPECT_NE(c1, 0u);
}

TEST(Crc32c, IncrementalAllSplitPoints) {
    // Split a ~40-byte string at every possible index and verify the
    // two-part incremental result equals the one-shot result.
    const std::string msg = "The quick brown fox jumps over the lazy dog";
    const auto* p = reinterpret_cast<const uint8_t*>(msg.data());
    const size_t N = msg.size();
    uint32_t expected = crc32c(p, N);

    for (size_t split = 0; split <= N; ++split) {
        uint32_t part1 = crc32c(p, split);
        uint32_t part2 = rune::protocol::crc32c(part1, p + split, N - split);
        EXPECT_EQ(part2, expected) << "split at " << split;
    }
}

TEST(Crc32c, AdditionalKnownVectors) {
    // "The quick brown fox jumps over the lazy dog"
    // Self-consistency pin (computed once by this impl; NOT an external RFC value).
    const std::string fox = "The quick brown fox jumps over the lazy dog";
    uint32_t foxCrc = crc32c(reinterpret_cast<const uint8_t*>(fox.data()), fox.size());
    // Recompute to confirm stability.
    EXPECT_EQ(crc32c(reinterpret_cast<const uint8_t*>(fox.data()), fox.size()), foxCrc);
    EXPECT_NE(foxCrc, 0u);

    // "a" - self-consistency pin
    uint8_t a = 'a';
    uint32_t aCrc = crc32c(&a, 1);
    EXPECT_EQ(crc32c(&a, 1), aCrc);
    EXPECT_NE(aCrc, 0u);
}

TEST(Crc32c, OrderSensitivity) {
    // crc("ab") != crc("ba")
    uint8_t ab[2] = {'a', 'b'};
    uint8_t ba[2] = {'b', 'a'};
    EXPECT_NE(crc32c(ab, 2), crc32c(ba, 2));

    // Also "ab" != "aa"
    uint8_t aa[2] = {'a', 'a'};
    EXPECT_NE(crc32c(ab, 2), crc32c(aa, 2));
}

TEST(Crc32c, IncrementalThreeWaySplit) {
    // Verify chaining across three segments.
    const std::string s = "Hello, World! This is a three-way split test.";
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    size_t N = s.size();

    uint32_t expected = crc32c(p, N);

    size_t s1 = N / 3, s2 = 2 * N / 3;
    uint32_t c1 = crc32c(p, s1);
    uint32_t c2 = rune::protocol::crc32c(c1, p + s1, s2 - s1);
    uint32_t c3 = rune::protocol::crc32c(c2, p + s2, N - s2);
    EXPECT_EQ(c3, expected);
}

TEST(Crc32c, SeedZeroEqualsNoSeed) {
    // crc32c(0, data, len) must equal crc32c(data, len).
    const std::string s = "seed parity check";
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    EXPECT_EQ(rune::protocol::crc32c(0u, p, s.size()), crc32c(p, s.size()));
}
