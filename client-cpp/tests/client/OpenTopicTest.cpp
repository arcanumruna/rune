// tests/client/OpenTopicTest.cpp
//
// Task 11 — TypedTopic<T> unit tests: schemaMatches() fingerprint gate +
//           TypedTopic buffer-reuse (RULE #4 zero-per-send-alloc).
//
// NO Aeron dependency.  Links rune_codec + rune_client (header-only interface).
//
// Test matrix:
//   1. FingerprintGate_Match        — schemaMatches true when fp matches blob fp
//   2. FingerprintGate_Mismatch     — schemaMatches false when fp differs by ^0x1
//   3. FingerprintGate_MalformedBlob — schemaMatches false (not throw) on bad blob
//   4. SchemaMatchesIdempotent       — schemaMatches has no mutable state; stable × 10
//   5. SendBufferReuse              — TypedTopic::send() buf_ capacity never shrinks
//                                    across repeated sends (proves RULE #4 zero-alloc)

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

TEST(OpenTopic, SchemaMatchesIdempotent) {
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

// ===========================================================================
// TypedTopic buffer-reuse test — proves RULE #4 (zero per-send allocation)
//
// Uses a no-op StubProducer so the test does NOT need Aeron linked.
// StubT satisfies the T contract: encodedSize() / encode(uint8_t*).
// ===========================================================================

namespace {

// ---------------------------------------------------------------------------
// StubProducer — satisfies the ProducerT contract required by TypedTopic.
// Writes call count to an external int so assertions work after move/copy.
// ---------------------------------------------------------------------------
struct StubProducer {
    explicit StubProducer(int* counter) noexcept : counter_(counter) {}

    void send(rune::client::TopicHandle /*topic*/,
              const rune::client::Key& /*key*/,
              std::span<const uint8_t> /*value*/) noexcept
    {
        if (counter_) ++(*counter_);
    }

    int* counter_ = nullptr;
};

// ---------------------------------------------------------------------------
// StubT — minimal record satisfying the T contract.
// Fixed encoded size so buf_ reaches high-water mark on the first send and
// must NOT reallocate on any subsequent send.
// ---------------------------------------------------------------------------
struct StubT {
    static constexpr uint64_t          kFingerprint = 0xCAFEBABEDEADBEEFULL;
    static constexpr std::string_view  kSchemaName  = "stub";

    uint64_t value = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 8; }

    std::size_t encode(uint8_t* dst) const noexcept {
        // Write 8 bytes little-endian.
        for (int i = 0; i < 8; ++i) {
            dst[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xFF);
        }
        return 8;
    }
};

} // anonymous namespace

TEST(OpenTopic, SendBufferReuse) {
    // Construct TypedTopic with a no-op stub producer (no Aeron required).
    // callCount is stored outside TypedTopic so assertions work after the move.
    int callCount = 0;
    rune::client::TypedTopic<StubT, StubProducer> topic(
        StubProducer{&callCount}, rune::client::TopicHandle{42, 1}, /*version=*/1);

    StubT rec1{0x0102030405060708ULL};
    StubT rec2{0x0807060504030201ULL};

    // First send — buf_ starts empty and grows to encodedSize() == 8.
    topic.send(rec1);
    ASSERT_GT(topic.bufCapacity(), 0u) << "buf_ must have capacity after first send";
    const std::size_t capAfterFirst = topic.bufCapacity();

    // Second send with a different value — buf_ must NOT reallocate (capacity
    // must remain at the high-water mark: capAfterFirst).
    topic.send(rec2);
    const std::size_t capAfterSecond = topic.bufCapacity();

    EXPECT_EQ(capAfterFirst, capAfterSecond)
        << "buf_ capacity changed between send #1 and send #2 — reallocation "
           "detected; violates RULE #4 zero-per-send-alloc contract";

    // Also verify the producer was called twice (the send path was exercised).
    EXPECT_EQ(callCount, 2)
        << "StubProducer::send must have been called exactly twice";

    // A third send must also hold capacity stable.
    topic.send(rec1);
    EXPECT_EQ(topic.bufCapacity(), capAfterFirst)
        << "buf_ capacity changed on send #3 — reallocation detected";
    EXPECT_EQ(callCount, 3);
}
