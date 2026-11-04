// tests/client/CheckedWritesTypedTopicCqTest.cpp - TypedTopic's completion-queue write forms (they take a real
// Completions, so this target links the client), and TypedTopic<T, ProducerRef>.
#include "../support/FrameCapture.h"
#include "gen/position.gen.h"

#include <rune/client/Completions.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/ProducerRef.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace rc = rune::client;

namespace {

struct Sent {
    rc::Key              key;
    std::vector<uint8_t> value;
    rc::WriteIntent      intent = rc::WriteIntent::Upsert;
};

struct StubProducer {
    std::vector<Sent>* sent = nullptr;

    void send(rc::TopicHandle, const rc::Key& k, std::span<const uint8_t> v, rc::Completions&, rc::Token,
              rc::WriteIntent i) const {
        sent->push_back({k, std::vector<uint8_t>(v.begin(), v.end()), i});
    }
};

struct EmptyEncoding {
    static constexpr uint64_t         kFingerprint = 0x6666;
    static constexpr std::string_view kSchemaName  = "empty_encoding";
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 0; }
    std::size_t encode(uint8_t*) const noexcept { return 0; }
};

using PositionTopic = rc::TypedTopic<rune::gen::Position, StubProducer>;

rune::gen::Position position(int64_t userId) {
    rune::gen::Position rec;
    rec.userId = userId;
    rec.zone   = 3;
    return rec;
}

}  // namespace

TEST(CheckedWritesTypedTopicCq, EachOpSendsItsIntentAndValue) {
    std::vector<Sent> sent;
    rc::Completions cq;
    PositionTopic topic(StubProducer{&sent}, rc::TopicHandle{7, 8, rc::TopicKind::Typed}, 2);
    const auto rec = position(41);

    topic.insert(rec, cq, 1);
    topic.update(rec, cq, 2);
    topic.upsert(rec, cq, 3);
    topic.remove(rec.key(), cq, 4);
    topic.removeExisting(rec.key(), cq, 5);
    ASSERT_EQ(sent.size(), 5u);

    const rc::WriteIntent want[] = {rc::WriteIntent::Insert, rc::WriteIntent::Update, rc::WriteIntent::Upsert,
                                    rc::WriteIntent::Upsert, rc::WriteIntent::Update};
    for (std::size_t i = 0; i < 5; ++i) EXPECT_EQ(sent[i].intent, want[i]) << i;
    ASSERT_FALSE(sent[0].value.empty());
    EXPECT_EQ(sent[1].value, sent[0].value);
    EXPECT_EQ(sent[2].value, sent[0].value);
    EXPECT_TRUE(sent[3].value.empty());
    EXPECT_TRUE(sent[4].value.empty());
    for (const Sent& s : sent) EXPECT_EQ(s.key.bytes().size(), 8u);
}

TEST(CheckedWritesTypedTopicCq, AnEmptyEncodingIsRefusedByTheIntentWrites) {
    std::vector<Sent> sent;
    rc::Completions cq;
    rc::TypedTopic<EmptyEncoding, StubProducer> topic(StubProducer{&sent}, rc::TopicHandle{7, 8}, 1);
    EXPECT_THROW(topic.insert(EmptyEncoding{}, cq, 1), std::invalid_argument);
    EXPECT_THROW(topic.update(EmptyEncoding{}, cq, 2), std::invalid_argument);
    EXPECT_THROW(topic.upsert(EmptyEncoding{}, cq, 3), std::invalid_argument);
    EXPECT_TRUE(sent.empty());
}

// TypedTopic<T, ProducerRef> forwards the intent writes to the borrowed Producer.
TEST(CheckedWritesTypedTopicCq, ProducerRefForwardsTheIntentWrites) {
    rc::ProducerConfig cfg;
    cfg.acks        = rc::Acks::None;
    cfg.lingerUs    = 0;
    cfg.batchBytes  = 16 * 1024;
    cfg.maxInFlight = 8;
    rc::Producer producer{new rc::producer::ProducerImpl(cfg, 1 << 20)};
    rc::Completions cq;
    rc::TypedTopic<rune::gen::Position, rc::ProducerRef> topic(rc::ProducerRef{producer}, rc::TopicHandle{1, 8}, 1);
    const auto rec = position(5);

    // A standalone queue is not registered with a client: the borrowed Producer refuses it, so the call reached it.
    EXPECT_THROW(topic.remove(rec.key(), cq, 1), std::invalid_argument);
    EXPECT_THROW(topic.removeExisting(rec.key(), cq, 2), std::invalid_argument);
    // Never called (it would wait for a broker): the AndWait forms must still compile against ProducerRef.
    [[maybe_unused]] const auto andWait = [&] {
        (void)topic.insertAndWait(rec);
        (void)topic.removeExistingAndWait(rec.key());
    };
}
