#include <gtest/gtest.h>
#include <rune/protocol/PartitionHash.h>

#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <unordered_set>
#include <vector>

using rune::protocol::fnv1a64;
using rune::protocol::partitionForKey;

namespace {

// Convert a string literal to a byte span for convenience.
static std::span<const uint8_t> asSpan(std::string_view sv) noexcept {
    return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Fixed-vector tests — pin the exact FNV-1a 64-bit outputs.
// These are the authoritative reference values; CB-2 client MUST agree.
// ---------------------------------------------------------------------------

TEST(PartitionHash, EmptyKeyVector) {
    // FNV-1a offset basis: no bytes XOR'd — returns the bare offset.
    const uint64_t h = fnv1a64(nullptr, 0);
    EXPECT_EQ(h, 0xcbf29ce484222325ULL);
}

TEST(PartitionHash, KnownVectorKey) {
    const auto sv = std::string_view{"key"};
    const uint64_t h = fnv1a64(
        reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    EXPECT_EQ(h, 0x3dc94a19365b10ecULL);
}

TEST(PartitionHash, KnownVectorInventory) {
    const auto sv = std::string_view{"inventory"};
    const uint64_t h = fnv1a64(
        reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    EXPECT_EQ(h, 0x1de86446633071dfULL);
}

TEST(PartitionHash, KnownVectorHello) {
    const auto sv = std::string_view{"hello"};
    const uint64_t h = fnv1a64(
        reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    EXPECT_EQ(h, 0xa430d84680aabd0bULL);
}

TEST(PartitionHash, KnownVectorAbc) {
    const auto sv = std::string_view{"abc"};
    const uint64_t h = fnv1a64(
        reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
    EXPECT_EQ(h, 0xe71fa2190541574bULL);
}

TEST(PartitionHash, KnownVectorSingleByteZero) {
    uint8_t b = 0x00;
    EXPECT_EQ(fnv1a64(&b, 1), 0xaf63bd4c8601b7dfULL);
}

TEST(PartitionHash, KnownVectorSingleByteFF) {
    uint8_t b = 0xFF;
    EXPECT_EQ(fnv1a64(&b, 1), 0xaf64724c8602eb6eULL);
}

// ---------------------------------------------------------------------------
// partitionForKey — derived from fixed vectors, in-range, stable
// ---------------------------------------------------------------------------

TEST(PartitionHash, PartitionForKeyInRange) {
    // "key" with 8 partitions = 0x3dc94a19365b10ec % 8 = 4
    const auto sp = asSpan("key");
    int32_t p = partitionForKey(sp, 8);
    EXPECT_EQ(p, 4);
    EXPECT_GE(p, 0);
    EXPECT_LT(p, 8);
}

TEST(PartitionHash, PartitionForKeyInventory8) {
    // "inventory" % 8 = 7
    const auto sp = asSpan("inventory");
    EXPECT_EQ(partitionForKey(sp, 8), 7);
}

TEST(PartitionHash, PartitionForKeyInventory4) {
    // "inventory" % 4 = 3
    const auto sp = asSpan("inventory");
    EXPECT_EQ(partitionForKey(sp, 4), 3);
}

TEST(PartitionHash, PartitionForKeySinglePartition) {
    // Any key with partitionCount=1 must always return 0.
    for (std::string_view sv : {"key", "inventory", "hello", "abc", "rune"}) {
        EXPECT_EQ(partitionForKey(asSpan(sv), 1), 0)
            << "key=" << sv;
    }
}

TEST(PartitionHash, PartitionIsStable) {
    // Same key + same count → same result, multiple calls.
    const auto sp = asSpan("hello");
    const int32_t first = partitionForKey(sp, 4);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(partitionForKey(sp, 4), first);
    }
}

TEST(PartitionHash, PartitionDifferentKeysCanDiffer) {
    // Distinct keys should not all hash to the same partition (they might
    // collide by chance, but with these particular values they differ).
    const auto sp1 = asSpan("key");
    const auto sp2 = asSpan("inventory");
    const auto sp3 = asSpan("hello");
    int32_t p1 = partitionForKey(sp1, 8);
    int32_t p2 = partitionForKey(sp2, 8);
    int32_t p3 = partitionForKey(sp3, 8);
    // p1=4, p2=7, p3=3 — all distinct
    EXPECT_NE(p1, p2);
    EXPECT_NE(p2, p3);
    EXPECT_NE(p1, p3);
}

// ---------------------------------------------------------------------------
// Uniformity sanity — 10 000 keys over 8 partitions; no partition empty.
// This is a smoke test, not a chi-squared test.
// ---------------------------------------------------------------------------

TEST(PartitionHash, UniformitySanity) {
    constexpr int kPartitions = 8;
    constexpr int kKeys = 10000;
    std::array<int, kPartitions> counts{};

    for (int i = 0; i < kKeys; ++i) {
        // Build a 4-byte key from the integer.
        uint8_t buf[4];
        buf[0] = static_cast<uint8_t>(i & 0xFF);
        buf[1] = static_cast<uint8_t>((i >> 8) & 0xFF);
        buf[2] = static_cast<uint8_t>((i >> 16) & 0xFF);
        buf[3] = static_cast<uint8_t>((i >> 24) & 0xFF);
        int32_t p = partitionForKey({buf, 4}, kPartitions);
        ASSERT_GE(p, 0);
        ASSERT_LT(p, kPartitions);
        ++counts[p];
    }

    // Each partition should receive at least 1% of keys (generous floor).
    const int floor = kKeys / (kPartitions * 10);
    for (int i = 0; i < kPartitions; ++i) {
        EXPECT_GE(counts[i], floor)
            << "partition " << i << " has too few keys: " << counts[i];
    }
}

// ---------------------------------------------------------------------------
// Stability across runs (no wall-clock, no ASLR involvement) — already
// guaranteed by the algorithm, but pin a multi-byte sequence explicitly.
// ---------------------------------------------------------------------------

TEST(PartitionHash, StabilityMultiByteSequence) {
    // A deterministic 32-byte key: bytes 0..31.
    uint8_t buf[32];
    for (uint8_t i = 0; i < 32; ++i) buf[i] = i;
    const uint64_t h1 = fnv1a64(buf, 32);
    const uint64_t h2 = fnv1a64(buf, 32);
    EXPECT_EQ(h1, h2);
    EXPECT_NE(h1, 0ULL);
}
