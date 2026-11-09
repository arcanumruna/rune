// tests/client/CheckedWritesTypedTopicTest.cpp - TypedTopic insert / update / upsert / remove / removeExisting and
// WriteResult, behind a recording ProducerT (no Aeron).
#include "gen/position.gen.h"
#include "gen/short_key.gen.h"

#include <gtest/gtest.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/WriteResult.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <random>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <vector>

namespace rc = rune::client;

namespace {

struct Sent {
    rc::TopicHandle      handle;
    rc::Key              key;
    std::vector<uint8_t> value;
    rc::WriteIntent      intent = rc::WriteIntent::Upsert;
};

struct Script {
    std::vector<Sent> sent;
    rc::Completion    answer{};
    bool              refuse = false;   // sendAndWait throws std::invalid_argument
};

struct StubProducer {
    Script* s = nullptr;

    void record(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v, rc::WriteIntent i) const {
        s->sent.push_back({t, k, std::vector<uint8_t>(v.begin(), v.end()), i});
    }
    void send(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v) const {
        record(t, k, v, rc::WriteIntent::Upsert);
    }
    rc::Completion sendAndWait(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v, rc::WriteIntent i,
                               std::chrono::nanoseconds) const {
        if (s->refuse) throw std::invalid_argument("refused");
        record(t, k, v, i);
        return s->answer;
    }
};

// Follows the generated-struct contract (what `rune schema generate` emits) with a uint16 key.
struct Narrow {
    static constexpr uint64_t         kFingerprint = 0x3333;
    static constexpr std::string_view kSchemaName  = "narrow";
    uint16_t id = 0;
    [[nodiscard]] uint16_t    key() const noexcept { return id; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 6; }
    std::size_t encode(uint8_t* b) const noexcept {
        b[0] = 2; b[1] = 0; b[2] = 1; b[3] = 0; b[4] = id & 0xff; b[5] = id >> 8;
        return 6;
    }
};

// A byte-span key: longer than Key::kMaxInline it cannot be a record key.
struct SpanKeyed {
    static constexpr uint64_t         kFingerprint = 0x4444;
    static constexpr std::string_view kSchemaName  = "span_keyed";
    std::string_view sku;
    [[nodiscard]] std::span<const uint8_t> key() const noexcept {
        return {reinterpret_cast<const uint8_t*>(sku.data()), sku.size()};
    }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4; }
    std::size_t encode(uint8_t* b) const noexcept { b[0] = 0; b[1] = 0; b[2] = 1; b[3] = 0; return 4; }
};

// Encodes to nothing: an intent write of it must not turn into a delete.
struct EmptyEncoding {
    static constexpr uint64_t         kFingerprint = 0x6666;
    static constexpr std::string_view kSchemaName  = "empty_encoding";
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 0; }
    std::size_t encode(uint8_t*) const noexcept { return 0; }
};

struct Keyless {
    static constexpr uint64_t         kFingerprint = 0x5555;
    static constexpr std::string_view kSchemaName  = "keyless";
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4; }
    std::size_t encode(uint8_t* b) const noexcept { b[0] = 0; b[1] = 0; b[2] = 1; b[3] = 0; return 4; }
};

template <class Topic>
concept CanRemove = requires(Topic& t) { t.remove(0); };
template <class Topic>
concept CanRemoveExistingAndWait = requires(Topic& t) { t.removeExistingAndWait(0); };

using PositionTopic = rc::TypedTopic<rune::gen::Position, StubProducer>;

constexpr rc::TopicHandle kHandle{7, 8, rc::TopicKind::Typed};

rune::gen::Position position(int64_t userId) {
    rune::gen::Position rec;
    rec.userId = userId;
    rec.zone   = 3;
    return rec;
}

}  // namespace

TEST(CheckedWritesTypedTopic, FireAndForgetOpsSendUpsertWithTheirValue) {
    Script script;
    PositionTopic topic(StubProducer{&script}, kHandle, 2);
    const auto rec = position(41);

    topic.send(rec);
    topic.upsert(rec);
    topic.remove(rec.key());
    ASSERT_EQ(script.sent.size(), 3u);
    ASSERT_FALSE(script.sent[0].value.empty());
    EXPECT_EQ(script.sent[1].value, script.sent[0].value);
    EXPECT_TRUE(script.sent[2].value.empty());
    for (const Sent& s : script.sent) {
        EXPECT_EQ(s.intent, rc::WriteIntent::Upsert);
        EXPECT_EQ(s.handle.topicId, 7);
        EXPECT_EQ(s.key.bytes().size(), 8u);
    }
}

TEST(CheckedWritesTypedTopic, AndWaitFormsSendTheirIntent) {
    Script script;
    PositionTopic topic(StubProducer{&script}, kHandle, 2);
    const auto rec = position(5);

    (void)topic.insertAndWait(rec);
    (void)topic.updateAndWait(rec);
    (void)topic.upsertAndWait(rec);
    (void)topic.removeAndWait(rec.key());
    (void)topic.removeExistingAndWait(rec.key());
    ASSERT_EQ(script.sent.size(), 5u);
    const rc::WriteIntent want[] = {rc::WriteIntent::Insert, rc::WriteIntent::Update, rc::WriteIntent::Upsert,
                                    rc::WriteIntent::Upsert, rc::WriteIntent::Update};
    for (std::size_t i = 0; i < 5; ++i) EXPECT_EQ(script.sent[i].intent, want[i]) << i;
    EXPECT_TRUE(script.sent[2].value.size() > 0);
    EXPECT_TRUE(script.sent[3].value.empty());
    EXPECT_TRUE(script.sent[4].value.empty());
}

TEST(CheckedWritesTypedTopicRegression, RemoveBuildsTheSameKeyAsSendRegression) {
    std::mt19937_64 rng(20261102);
    const auto check = [&](auto makeRec) {
        using Rec = decltype(makeRec());
        Script script;
        rc::TypedTopic<Rec, StubProducer> topic(StubProducer{&script}, kHandle, 1);
        for (int i = 0; i < 50; ++i) {
            const Rec rec = makeRec();
            topic.send(rec);
            topic.remove(rec.key());
            const Sent& sendOp   = script.sent[script.sent.size() - 2];
            const Sent& removeOp = script.sent[script.sent.size() - 1];
            ASSERT_EQ(removeOp.key.size(), sendOp.key.size());
            ASSERT_TRUE(std::equal(removeOp.key.bytes().begin(), removeOp.key.bytes().end(), sendOp.key.bytes().begin()));
            ASSERT_EQ(removeOp.key.partition(8), sendOp.key.partition(8));
        }
    };
    check([&] { return position(static_cast<int64_t>(rng())); });
    check([&] {
        rune::gen::ShortKey rec;
        for (char& c : rec.code) c = static_cast<char>('a' + rng() % 26);
        return rec;
    });
    check([&] { return Narrow{static_cast<uint16_t>(rng())}; });
}

TEST(CheckedWritesTypedTopicRegression, OverlongKeyRefusesLikeSendRegression) {
    Script script;
    rc::TypedTopic<SpanKeyed, StubProducer> topic(StubProducer{&script}, kHandle, 1);
    const SpanKeyed rec{"eighteen-bytes-xxx"};
    ASSERT_GT(rec.sku.size(), rc::Key::kMaxInline);
    EXPECT_THROW(topic.send(rec), std::invalid_argument);
    EXPECT_THROW(topic.remove(rec.key()), std::invalid_argument);
    EXPECT_THROW((void)topic.removeExistingAndWait(rec.key()), std::invalid_argument);
    EXPECT_TRUE(script.sent.empty());
}

TEST(CheckedWritesTypedTopic, AndWaitMapsTheCompletion) {
    Script script;
    PositionTopic topic(StubProducer{&script}, kHandle, 1);
    const auto rec = position(1);

    script.answer = {};
    script.answer.status    = rc::OpStatus::BrokerError;
    script.answer.errorCode = 56;
    auto r = topic.insertAndWait(rec);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.code(), 56);
    EXPECT_EQ(r.constraintId(), 0u);
    EXPECT_EQ(r.key(), 0u);

    script.answer = {};
    script.answer.value = 41;
    r = topic.insertAndWait(rec);
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.code(), 0);
    EXPECT_EQ(r.offset(), 41);

    script.answer = {};
    script.answer.status    = rc::OpStatus::VerdictUnknown;
    script.answer.errorCode = 21;
    r = topic.upsertAndWait(rec);
    EXPECT_EQ(r.status(), rc::OpStatus::VerdictUnknown);
    EXPECT_EQ(r.code(), 0);

    script.answer = {};
    script.answer.status = rc::OpStatus::BrokerTooOld;
    r = topic.removeAndWait(rec.key());
    EXPECT_EQ(r.status(), rc::OpStatus::BrokerTooOld);
    EXPECT_FALSE(r.ok());
}

TEST(CheckedWritesTypedTopic, AndWaitThrowsOnALocalRefusal) {
    Script script;
    script.refuse = true;
    PositionTopic topic(StubProducer{&script}, kHandle, 1);
    EXPECT_THROW((void)topic.insertAndWait(position(1)), std::invalid_argument);
}

TEST(CheckedWritesTypedTopic, SendIsUnchanged) {
    Script script;
    PositionTopic topic(StubProducer{&script}, kHandle, 9);
    topic.send(position(77));
    ASSERT_EQ(script.sent.size(), 1u);
    EXPECT_EQ(script.sent[0].intent, rc::WriteIntent::Upsert);
    EXPECT_EQ(topic.boundVersion(), 9);
    ASSERT_GE(script.sent[0].value.size(), 4u);
    EXPECT_EQ(script.sent[0].value[2], 9);
    EXPECT_EQ(script.sent[0].value[3], 0);
}

TEST(CheckedWritesTypedTopic, FromSchemaMarksTheHandleTyped) {
    struct FakeClient {
        StubProducer p;
        StubProducer producer() const { return p; }
    };
    Script script;
    FakeClient client{StubProducer{&script}};
    const auto descriptor = rune::codec::serialize(rune::gen::Position::descriptor());
    rc::Completion c{};
    c.value    = 12;
    c.aux      = 3;
    c.reserved0 = 8;
    const auto topic = rc::TypedTopic<rune::gen::Position, StubProducer>::fromSchema(client, c, descriptor);
    EXPECT_EQ(topic.handle().kind, rc::TopicKind::Typed);
    EXPECT_EQ(topic.handle().topicId, 12);
}

TEST(CheckedWritesTypedTopic, KeylessTopicHasNoRemove) {
    static_assert(!CanRemove<rc::TypedTopic<Keyless, StubProducer>>);
    static_assert(!CanRemoveExistingAndWait<rc::TypedTopic<Keyless, StubProducer>>);
    static_assert(CanRemove<PositionTopic>);
    static_assert(CanRemoveExistingAndWait<PositionTopic>);
    Script script;
    rc::TypedTopic<Keyless, StubProducer> topic(StubProducer{&script}, kHandle, 1);
    (void)topic.insertAndWait(Keyless{});
    ASSERT_EQ(script.sent.size(), 1u);
    EXPECT_EQ(script.sent[0].key.size(), 0u);
}

TEST(CheckedWritesTypedTopic, AnEmptyEncodingIsRefusedByTheIntentWrites) {
    Script script;
    rc::TypedTopic<EmptyEncoding, StubProducer> topic(StubProducer{&script}, kHandle, 1);
    EXPECT_THROW((void)topic.insertAndWait(EmptyEncoding{}), std::invalid_argument);
    EXPECT_THROW((void)topic.updateAndWait(EmptyEncoding{}), std::invalid_argument);
    EXPECT_THROW((void)topic.upsertAndWait(EmptyEncoding{}), std::invalid_argument);
    EXPECT_TRUE(script.sent.empty());
}

// ── GetTopicSchema completion helpers (lease topics) ─────────────────────────

namespace {
struct SchemaRow {
    const char* name;
    int64_t     aux;
    std::size_t blobLen, withKf, withLease;   // the payload: blob, then 14 (key block) and 39 (lease block) bytes
    std::size_t truncatedTo;                  // 0 = the payload as built; else cut to this many bytes
    bool        expectKf, expectLease;
    std::size_t expectBlob;
};
}  // namespace

TEST(CheckedWritesTypedTopic, SchemaCompletionHelpersRegression) {
    using rune::protocol::kLeaseSpecBytes;
    using rune::protocol::kTopicKeyFeaturesWireLen;
    rune::protocol::TopicKeyFeatures kf;
    kf.keyIndex             = true;
    kf.autoIncrement        = true;
    kf.partitioner          = rune::protocol::Partitioner::Modulo;
    kf.autoIncrementMax     = 123456;
    kf.autoIncrementReserve = 77;
    rune::protocol::LeaseSpec lease;
    lease.ttlMs = 4242;
    lease.maxKeys = 9;
    lease.checkpoint = 0;
    constexpr std::size_t B = 20, K = kTopicKeyFeaturesWireLen, L = rc::kSchemaLeaseBlockBytes;
    const int64_t none = rc::schemaAuxOf(5, false), k = rc::schemaAuxOf(5, true), kl = rc::schemaAuxOf(5, true, true),
                  l = rc::schemaAuxOf(5, false, true);
    ASSERT_EQ(l >> 17, 1);
    const SchemaRow rows[] = {
        // aux none: nothing after the blob is interpreted
        {"none/blob", none, B, 0, 0, 0, false, false, B},
        {"none/blob+14", none, B, K, 0, 0, false, false, B + K},
        {"none/blob+14+39", none, B, K, L, 0, false, false, B + K + L},
        // bit 16
        {"16/blob+14", k, B, K, 0, 0, true, false, B},
        // bits 16+17
        {"16+17/blob+14+39", kl, B, K, L, 0, true, true, B},
        // bit 17 alone is no lease and no key block: the whole payload is the blob
        {"17/blob", l, B, 0, 0, 0, false, false, B},
        {"17/blob+14+39", l, B, K, L, 0, false, false, B + K + L},
        // payload shorter than the tail the aux promises: the whole payload is the blob, nothing is decoded
        {"16/short", k, B, K, 0, 10, false, false, 10},
        {"16+17/short", kl, B, K, L, 10, false, false, 10},
        {"16+17/shorter than 53", kl, B, K, L, K + L - 1, false, false, K + L - 1},
    };
    for (const SchemaRow& row : rows) {
        SCOPED_TRACE(row.name);
        std::vector<uint8_t> payload(B, 0xAB);
        if (row.withKf) { payload.resize(B + K); rune::protocol::putTopicKeyFeatures(payload.data() + B, kf); }
        if (row.withLease) { payload.resize(B + K + L); rune::protocol::encodeLeaseSpec(lease, payload.data() + B + K); }
        if (row.truncatedTo != 0) payload.resize(row.truncatedTo);
        rc::Completion c{};
        c.aux = row.aux;
        EXPECT_EQ(rc::schemaBlobOf(c, payload).size(), row.expectBlob);
        EXPECT_EQ(rc::schemaKeyFeaturesOf(c, payload), row.expectKf ? kf : rune::protocol::TopicKeyFeatures{});
        const auto got = rc::schemaLeaseOf(c, payload);
        ASSERT_EQ(got.has_value(), row.expectLease);
        if (row.expectLease) EXPECT_EQ(*got, lease);
    }
    {   // ResultTooLarge: no payload bytes at all (value = the real size)
        SCOPED_TRACE("ResultTooLarge");
        rc::Completion c{};
        c.status = rc::OpStatus::ResultTooLarge;
        c.aux    = kl;
        const std::span<const uint8_t> empty;
        EXPECT_EQ(rc::schemaBlobOf(c, empty).size(), 0u);
        EXPECT_EQ(rc::schemaKeyFeaturesOf(c, empty), rune::protocol::TopicKeyFeatures{});
        EXPECT_FALSE(rc::schemaLeaseOf(c, empty).has_value());
    }
}

TEST(CheckedWritesTypedTopic, FromSchemaOnALeaseCompletionKeepsTheDescriptorUnchangedRegression) {
    struct FakeClient {
        StubProducer p;
        StubProducer producer() const { return p; }
    };
    Script script;
    FakeClient client{StubProducer{&script}};
    auto payload = rune::codec::serialize(rune::gen::Position::descriptor());
    const std::size_t blobLen = payload.size();
    payload.resize(blobLen + rune::protocol::kTopicKeyFeaturesWireLen + rune::protocol::kLeaseSpecBytes + 4);   // + the i32 lease-view topic id (Task 14a)
    rune::protocol::putTopicKeyFeatures(payload.data() + blobLen, rune::protocol::TopicKeyFeatures{});
    rune::protocol::encodeLeaseSpec(rune::protocol::LeaseSpec{},
                                    payload.data() + blobLen + rune::protocol::kTopicKeyFeaturesWireLen);
    rc::Completion c{};
    c.value     = 12;
    c.aux       = rc::schemaAuxOf(3, true, true);
    c.reserved0 = 8;
    const auto topic = rc::TypedTopic<rune::gen::Position, StubProducer>::fromSchema(client, c, payload);
    EXPECT_EQ(topic.handle().topicId, 12);
    EXPECT_EQ(topic.boundVersion(), 3);
    const auto blob = rc::schemaBlobOf(c, payload);
    ASSERT_EQ(blob.size(), blobLen) << "no trailer bytes in the descriptor blob";
    EXPECT_EQ(std::vector<uint8_t>(blob.begin(), blob.end()),
              rune::codec::serialize(rune::gen::Position::descriptor()));
}
