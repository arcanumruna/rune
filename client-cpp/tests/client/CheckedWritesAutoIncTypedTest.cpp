// tests/client/CheckedWritesAutoIncTypedTest.cpp - checked writes Part 06: the broker-assigned id of a typed create
// (WriteResult::key()) and StoredIdUnknown, behind a scripted ProducerT (no Aeron).
#include "gen/position.gen.h"

#include <gtest/gtest.h>
#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/WriteResult.h>

#include <chrono>
#include <span>

namespace rc = rune::client;

namespace {

struct StubProducer {
    rc::Completion* answer = nullptr;
    rc::Completion sendAndWait(rc::TopicHandle, const rc::Key&, std::span<const uint8_t>, rc::WriteIntent,
                               std::chrono::nanoseconds) const {
        return *answer;
    }
};

using PositionTopic = rc::TypedTopic<rune::gen::Position, StubProducer>;

constexpr rc::TopicHandle kModulo{7, 4, rc::TopicKind::Typed, rune::protocol::Partitioner::Modulo};

}  // namespace

TEST(CheckedWritesAutoIncTyped, InsertAndWaitKeyIsTheAssignedId) {
    rc::Completion answer;
    answer.status = rc::OpStatus::Ok;
    answer.value  = 7;
    answer.aux    = 42;
    PositionTopic topic(StubProducer{&answer}, kModulo, 2);
    rune::gen::Position rec;
    rec.userId = 0;
    const rc::WriteResult r = topic.insertAndWait(rec);
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.key(), 42u);
    EXPECT_EQ(r.offset(), 7);
}

TEST(CheckedWritesAutoIncTyped, StoredIdUnknownSurfacesAsStatus) {
    rc::Completion answer;
    answer.status = rc::OpStatus::StoredIdUnknown;
    answer.value  = -1;
    PositionTopic topic(StubProducer{&answer}, kModulo, 2);
    rune::gen::Position rec;
    rec.userId = 0;
    const rc::WriteResult r = topic.insertAndWait(rec);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status(), rc::OpStatus::StoredIdUnknown);
    EXPECT_EQ(r.key(), 0u);
}
