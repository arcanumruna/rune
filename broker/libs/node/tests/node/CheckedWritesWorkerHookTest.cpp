// CheckedWritesWorkerHookTest.cpp - checked writes Part 03 Task 7: the hook and the frame checks inside Worker::produceCore_,
// on a real Worker over an Aeron IPC driver (CheckedWritesAckRig.h).
#include "support/CheckedWritesAckRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;

namespace {
constexpr int16_t kOk = 0;
constexpr int16_t kInvalidRequest = static_cast<int16_t>(ErrorCode::InvalidRequest);
constexpr int16_t kLevelBelow = 4, kLevelCheckedWrites = 5;
constexpr uint8_t kAllView = 3;
}  // namespace

TEST(CheckedWritesWorkerHook, IntentOnATypedTopicWithoutKeyIndexIsInvalidRequest) {
    AckRig r;
    r.typed();
    r.setLevel(kLevelCheckedWrites);
    AckRig::Write w;
    w.corr = 1; w.acks = kAllView; w.pid = 11; w.seq = 0; w.attributes = {1};
    r.produce(w);
    EXPECT_EQ(r.answer(1), kInvalidRequest);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::NoKeyIndex), 1u);
    EXPECT_EQ(r.log->logEndOffset(), 0);
}

TEST(CheckedWritesWorkerHook, ViewBelowTheLevelIsInvalidRequestOnEveryTopic) {
    for (const bool typed : {true, false}) {
        AckRig r;
        if (typed) r.typed();
        r.setLevel(kLevelBelow);
        AckRig::Write w;
        w.corr = 1; w.acks = kAllView;
        r.produce(w);
        EXPECT_EQ(r.answer(1), kInvalidRequest) << "typed " << typed;
        EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::LevelTooLow), 1u);
        EXPECT_EQ(r.log->logEndOffset(), 0);
        r.setLevel(kLevelCheckedWrites);
        w.corr = 2;
        r.produce(w);
        EXPECT_EQ(r.answer(2), kOk) << "typed " << typed;
        EXPECT_EQ(r.log->logEndOffset(), 1);
    }
}

TEST(CheckedWritesWorkerHook, ViewWithoutAWorkerSideAckSinkIsInvalidRequest) {
    AckRig r({.ackSink = false});
    r.setLevel(kLevelCheckedWrites);
    AckRig::Write w;
    w.corr = 1; w.acks = kAllView;
    r.produce(w);
    EXPECT_EQ(r.answer(1), kInvalidRequest);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::LegacyMode), 1u);
    EXPECT_EQ(r.log->logEndOffset(), 0);
}

TEST(CheckedWritesWorkerHook, PlainWritesNeedNothingInLegacyMode) {
    AckRig r({.ackSink = false});
    r.typed();
    for (const uint8_t acks : {uint8_t{0}, uint8_t{1}}) {
        AckRig::Write w;
        w.corr = acks + 1; w.acks = acks;
        r.produce(w);
        EXPECT_EQ(r.answer(w.corr), kOk) << int(acks);
    }
    EXPECT_EQ(r.log->logEndOffset(), 2);
}

TEST(CheckedWritesWorkerHook, TransactionalUnconstrainedBatchWorksAsToday) {
    AckRig r;
    r.typed();
    r.setLevel(kLevelCheckedWrites);
    AckRig::Write w;
    w.corr = 1; w.acks = 1; w.pid = 77; w.seq = 0; w.txn = true;
    r.produce(w);
    EXPECT_EQ(r.answer(1), kOk);
}

TEST(CheckedWritesViewAckRegression, ViewBitOnATransactionalBatchIsIgnoredRegression) {
    // Below the level a View write is refused; a transactional batch's View bit is cleared before that check.
    AckRig r;
    r.setLevel(kLevelBelow);
    AckRig::Write w;
    w.corr = 1; w.acks = kAllView; w.pid = 77; w.seq = 0; w.txn = true;
    r.produce(w);
    EXPECT_EQ(r.answer(1), kOk);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::LevelTooLow), 0u);
}
