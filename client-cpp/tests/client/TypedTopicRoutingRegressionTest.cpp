// tests/client/TypedTopicRoutingRegressionTest.cpp
//
// Phase 2b Part C regression (spec §C): TypedTopic::send sent every record as
// producer.send(TopicHandle{id, 1}, Key{}, bytes) — all on partition 0, no
// record key.  It must route EXACTLY like Producer::send(TopicHandle{id, n},
// Key(<key field>), bytes): same handle (the broker-reported partition count),
// same key bytes, hence the same partition (Producer::send routes to
// key.partition(handle.partitionCount) — src/producer/Producer.cpp).
//
// Runs on the REAL generated rune::gen::Characters (tests/generated/
// characters.blob -> characters.gen.h; userId int64 is the isKey field)
// behind a recording ProducerT.  No Aeron.

#include "characters.gen.h"
#include "../support/AllocCounter.h"

#include <gtest/gtest.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>
#include <rune/protocol/PartitionHash.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <set>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace rc = rune::client;

namespace {

struct Sent {
    rc::TopicHandle                           handle;
    std::array<uint8_t, rc::Key::kMaxInline>  key{};
    std::size_t                               keyLen    = 0;
    int32_t                                   partition = -1;  // where Producer::send routes it
};

// Records exactly what TypedTopic hands the producer.  `out` must have its
// capacity reserved by the test: send() itself never allocates.
struct RecordingProducer {
    std::vector<Sent>* out = nullptr;
    void send(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t>) noexcept {
        Sent s;
        s.handle = t;
        s.keyLen = k.size();
        std::copy(k.bytes().begin(), k.bytes().end(), s.key.begin());
        s.partition = k.partition(t.partitionCount);
        out->push_back(s);
    }
};

// A schema with NO key field: the generator emits no key().
struct NoKeyRecord {
    static constexpr uint64_t         kFingerprint = 0x1111;
    static constexpr std::string_view kSchemaName  = "nokey";
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4; }
    std::size_t encode(uint8_t* b) const noexcept { b[0] = 0; b[1] = 0; b[2] = 1; b[3] = 0; return 4; }
};

// Mirrors the generator's var-len key(): a BYTE span over the content
// (rune-codec Generator.cpp, Phase 2b Part C).
struct StrKeyRecord {
    static constexpr uint64_t         kFingerprint = 0x2222;
    static constexpr std::string_view kSchemaName  = "str_keyed";
    std::string_view sku;
    [[nodiscard]] std::span<const uint8_t> key() const noexcept {
        return {reinterpret_cast<const uint8_t*>(sku.data()), sku.size()};
    }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + sku.size(); }
    std::size_t encode(uint8_t* b) const noexcept {
        b[0] = 0; b[1] = 0; b[2] = 1; b[3] = 0;
        std::memcpy(b + 4, sku.data(), sku.size());
        return 4 + sku.size();
    }
};

bool keyEquals(const Sent& s, const rc::Key& want) {
    return s.keyLen == want.size() &&
           std::equal(want.bytes().begin(), want.bytes().end(), s.key.begin());
}

} // namespace

// The spec's regression: an 8-partition typed topic distributes records by key
// exactly like Producer::send with the same Key.
TEST(TypedTopicRoutingRegression, EightPartitionTypedTopicRoutesLikeProducerSendWithSameKey) {
    static_assert(rc::HasRecordKey<rune::gen::Characters>);
    static_assert(std::is_same_v<decltype(std::declval<const rune::gen::Characters&>().key()), int64_t>);

    constexpr int32_t kParts = 8;
    std::vector<Sent> sent;
    sent.reserve(1000);
    rc::TypedTopic<rune::gen::Characters, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{5, kParts}, /*version=*/1);

    std::set<int32_t> hit, expectedHit;
    for (int64_t u = 0; u < 1000; ++u) {
        rune::gen::Characters rec;
        rec.id       = 10'000 + u;
        rec.userId   = u;
        rec.strength = 1;
        rec.name     = "n";
        topic.send(rec);

        ASSERT_EQ(sent.size(), static_cast<std::size_t>(u + 1));
        const Sent& s = sent.back();
        const rc::Key want(u);   // what a Producer::send caller passes for this record
        EXPECT_EQ(s.handle.topicId, 5);
        EXPECT_EQ(s.handle.partitionCount, kParts) << "must route over the topic's partitions, not 1";
        ASSERT_TRUE(keyEquals(s, want)) << "record key must be the userId field (8 LE bytes), not empty";
        EXPECT_EQ(s.partition, want.partition(kParts));
        hit.insert(s.partition);
        expectedHit.insert(want.partition(kParts));
    }
    EXPECT_EQ(hit, expectedHit);
    EXPECT_GT(hit.size(), 1u) << "the bug sent every record to partition 0";
}

// Spec: a typed topic whose schema has no key field keeps today's behaviour.
TEST(TypedTopicRoutingRegression, SchemaWithoutKeyFieldKeepsPartitionZeroAndEmptyKey) {
    static_assert(!rc::HasRecordKey<NoKeyRecord>);
    std::vector<Sent> sent;
    sent.reserve(4);
    rc::TypedTopic<NoKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{6, 8}, /*version=*/1);
    topic.send(NoKeyRecord{});
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].handle.partitionCount, 8);
    EXPECT_EQ(sent[0].keyLen, 0u);
    EXPECT_EQ(sent[0].partition, 0);
}

// A string key must hash its CONTENT: two records whose keys are equal but
// live in different buffers must land on the same partition with the same
// key bytes (a string_view-typed key would hash pointer+len instead).
TEST(TypedTopicRoutingRegression, StringKeyRoutesByContentBytesNotByViewObject) {
    std::vector<Sent> sent;
    sent.reserve(4);
    rc::TypedTopic<StrKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{7, 8}, /*version=*/1);
    const char bufA[] = "sku-42";
    const char bufB[] = "sku-42";
    ASSERT_NE(static_cast<const void*>(bufA), static_cast<const void*>(bufB));
    topic.send(StrKeyRecord{std::string_view(bufA, 6)});
    topic.send(StrKeyRecord{std::string_view(bufB, 6)});
    ASSERT_EQ(sent.size(), 2u);
    const std::string_view content = "sku-42";
    const rc::Key want(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(content.data()),
                                                content.size()));
    EXPECT_TRUE(keyEquals(sent[0], want));
    EXPECT_TRUE(keyEquals(sent[1], want));
    EXPECT_EQ(sent[0].partition, want.partition(8));
    EXPECT_EQ(sent[0].partition, sent[1].partition);
}

// rune::client::Key holds at most 16 bytes and silently truncates.  A typed
// topic must FAIL CLOSED instead: a truncated record key would merge distinct
// entities (compaction / views fold by key).  Nothing may be sent.
TEST(TypedTopicRoutingRegression, VarKeyLongerThan16BytesIsRejectedNotTruncated) {
    std::vector<Sent> sent;
    sent.reserve(4);
    rc::TypedTopic<StrKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{7, 8}, /*version=*/1);
    EXPECT_THROW(topic.send(StrKeyRecord{"0123456789abcdefX"}), std::invalid_argument);  // 17 bytes
    EXPECT_TRUE(sent.empty()) << "a rejected record must not reach the producer";
    EXPECT_NO_THROW(topic.send(StrKeyRecord{"0123456789abcdef"}));                        // exactly 16
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].keyLen, 16u);
}

// CLAUDE.md #4: keyed routing adds no allocation to the send hot path.
TEST(TypedTopicRoutingRegression, KeyedSendAllocatesNothingInSteadyState) {
    std::vector<Sent> sent;
    sent.reserve(2000);
    rc::TypedTopic<rune::gen::Characters, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{5, 8}, /*version=*/1);
    rune::gen::Characters rec;
    rec.id = 1; rec.userId = 1; rec.strength = 1; rec.name = "warm";
    topic.send(rec);   // warm-up: the reuse buffer grows once to its high-water mark

    alloc_counter::startThisThread();
    for (int64_t u = 0; u < 1000; ++u) {
        rec.userId = u;
        topic.send(rec);
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "TypedTopic::send must not allocate in steady state";
    EXPECT_EQ(sent.size(), 1001u);
}

// Review carry-over: a broker-reported partition count <= 0 is "not reported"
// (not only == 0) — always route over at least one partition.
TEST(TypedTopicRoutingRegression, NonPositiveReportedPartitionCountFallsBackToOne) {
    static_assert(rc::typedPartitionCount(8) == 8);
    static_assert(rc::typedPartitionCount(1) == 1);
    static_assert(rc::typedPartitionCount(0) == 1);
    static_assert(rc::typedPartitionCount(-1) == 1);
    static_assert(rc::typedPartitionCount(INT32_MIN) == 1);
    EXPECT_EQ(rc::typedPartitionCount(-7), 1);
}

// A var-len key field that is EMPTY (length 0) is the empty record key, which
// rune::protocol::partitionForKey maps to partition 0 — the same partition the
// broker resolves for that key (partitionKnown=0 paths).
TEST(TypedTopicRoutingRegression, EmptyStringKeyFieldLandsOnPartitionZero) {
    std::vector<Sent> sent;
    sent.reserve(2);
    rc::TypedTopic<StrKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{7, 8}, /*version=*/1);
    topic.send(StrKeyRecord{std::string_view{}});
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].handle.partitionCount, 8);
    EXPECT_EQ(sent[0].keyLen, 0u);
    EXPECT_EQ(sent[0].partition, 0);
    EXPECT_EQ(sent[0].partition, rune::protocol::partitionForKey(std::span<const uint8_t>{}, 8));
}
