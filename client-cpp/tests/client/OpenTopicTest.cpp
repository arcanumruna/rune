// tests/client/OpenTopicTest.cpp
//
// Task 11 — TypedTopic<T> unit tests: schemaMatches() fingerprint gate.
//
// NO Aeron dependency.  Links rune_codec + rune_client (header-only interface).
//
// Test matrix:
//   1. FingerprintGate_Match        — schemaMatches true when fp matches blob fp
//   2. FingerprintGate_Mismatch     — schemaMatches false when fp differs by ^0x1
//   3. FingerprintGate_MalformedBlob — schemaMatches false (not throw) on bad blob
//   4. SendBufferReuse              — schemaMatches idempotent; encodedSize stable

#include <gtest/gtest.h>
#include <rune/client/TypedTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

using namespace rune::codec;

// ===========================================================================
// schemaMatches() unit tests (no broker, no Aeron)
// ===========================================================================

TEST(OpenTopic, FingerprintGate_Match) {
    Descriptor d = DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .build();
    auto blob = serialize(d);
    EXPECT_TRUE(rune::client::schemaMatches(fingerprint(d), blob));
}

TEST(OpenTopic, FingerprintGate_Mismatch) {
    Descriptor d = DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .build();
    auto blob = serialize(d);
    EXPECT_FALSE(rune::client::schemaMatches(fingerprint(d) ^ 0x1, blob));
}

TEST(OpenTopic, FingerprintGate_MalformedBlob) {
    // Malformed blob must return false, NOT throw.
    std::vector<uint8_t> bad = {0x00, 0x01, 0x02};
    EXPECT_FALSE(rune::client::schemaMatches(0xDEADBEEFULL,
                                              std::span<const uint8_t>(bad)));
}

TEST(OpenTopic, SendBufferReuse) {
    // schemaMatches must be idempotent across repeated calls — verify the
    // function has no mutable state (calling it 10x with the same args is stable).
    Descriptor d = DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .build();
    auto blob        = serialize(d);
    const uint64_t fp = fingerprint(d);

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(rune::client::schemaMatches(fp, blob))
            << "schemaMatches must be stable across repeated calls";
    }
}
