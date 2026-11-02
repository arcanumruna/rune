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
#include <cstddef>
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
    void send(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v, rc::Completions&, rc::Token,
              rc::WriteIntent i) const {
        record(t, k, v, i);
    }
    rc::Completion sendAndWait(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v, rc::WriteIntent i,
                               std::chrono::nanoseconds) const {
        if (s->refuse) throw std::invalid_argument("refused");
        record(t, k, v, i);
        return s->answer;
    }
};

// A queue the stub never touches: a real one needs the client library (Aeron), which this target does not link.
rc::Completions& unusedQueue() {
    alignas(std::max_align_t) static unsigned char storage[1];
    return *reinterpret_cast<rc::Completions*>(storage);
}

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

TEST(CheckedWritesTypedTopic, EachOpSendsItsIntentAndValue) {
    Script script;
    PositionTopic topic(StubProducer{&script}, kHandle, 2);
    const auto rec = position(41);
    rc::Completions& cq = unusedQueue();

    topic.send(rec);
    topic.insert(rec, cq, 1);
    topic.update(rec, cq, 2);
    topic.upsert(rec);
    topic.upsert(rec, cq, 3);
    topic.remove(rec.key());
    topic.remove(rec.key(), cq, 4);
    topic.removeExisting(rec.key(), cq, 5);
    ASSERT_EQ(script.sent.size(), 8u);

    const auto& sent = script.sent;
    EXPECT_EQ(sent[0].intent, rc::WriteIntent::Upsert);
    ASSERT_FALSE(sent[0].value.empty());
    EXPECT_EQ(sent[1].intent, rc::WriteIntent::Insert);
    EXPECT_EQ(sent[1].value, sent[0].value);
    EXPECT_EQ(sent[2].intent, rc::WriteIntent::Update);
    EXPECT_EQ(sent[2].value, sent[0].value);
    EXPECT_EQ(sent[3].intent, rc::WriteIntent::Upsert);
    EXPECT_EQ(sent[3].value, sent[0].value);
    EXPECT_EQ(sent[4].intent, rc::WriteIntent::Upsert);
    EXPECT_EQ(sent[4].value, sent[0].value);
    EXPECT_EQ(sent[5].intent, rc::WriteIntent::Upsert);
    EXPECT_TRUE(sent[5].value.empty());
    EXPECT_EQ(sent[6].intent, rc::WriteIntent::Upsert);
    EXPECT_TRUE(sent[6].value.empty());
    EXPECT_EQ(sent[7].intent, rc::WriteIntent::Update);
    EXPECT_TRUE(sent[7].value.empty());
    for (const Sent& s : sent) {
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
    const auto check = [&](auto makeRec, auto topicTag) {
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
        (void)topicTag;
    };
    check([&] { return position(static_cast<int64_t>(rng())); }, 0);
    check([&] {
        rune::gen::ShortKey rec;
        for (char& c : rec.code) c = static_cast<char>('a' + rng() % 26);
        return rec;
    }, 0);
    check([&] { return Narrow{static_cast<uint16_t>(rng())}; }, 0);
}

TEST(CheckedWritesTypedTopicRegression, OverlongKeyRefusesLikeSendRegression) {
    Script script;
    rc::TypedTopic<SpanKeyed, StubProducer> topic(StubProducer{&script}, kHandle, 1);
    const SpanKeyed rec{"seventeen-bytes-xx"};
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
    Script script;
    rc::TypedTopic<Keyless, StubProducer> topic(StubProducer{&script}, kHandle, 1);
    (void)topic.insertAndWait(Keyless{});
    ASSERT_EQ(script.sent.size(), 1u);
    EXPECT_EQ(script.sent[0].key.size(), 0u);
}
