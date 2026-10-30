// CheckedWritesViewAckTest.cpp - checked writes Part 03 Task 9: View waiters.  An ack with the View bit waits for the HW,
// (for All) the ISR, and every view's fold past the batch under the current leadership; All acks never queue behind it.
#include "support/CheckedWritesAckRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;

namespace {
constexpr int16_t kOk = 0;
constexpr int16_t kNotLeader = static_cast<int16_t>(ErrorCode::NotLeader);
constexpr int16_t kInternal = static_cast<int16_t>(ErrorCode::Internal);
constexpr int32_t kView = 0x40000001, kView2 = 0x40000002;
constexpr int16_t kLevel = 5;
constexpr uint8_t kLeaderView = 2, kAllView = 3, kAll = 1;

AckRig::Options twoNodes(int minIsr = 1) {
    AckRig::Options o;
    o.replicas = {1, 2};
    o.minIsr = minIsr;
    return o;
}
AckRig::Write write(int64_t corr, uint8_t acks, int64_t pid = -1, int32_t seq = -1, bool txn = false) {
    AckRig::Write w;
    w.corr = corr; w.acks = acks; w.pid = pid; w.seq = seq; w.txn = txn;
    return w;
}
bool absent(AckRig& r, int64_t corr, int n = 2000) {
    r.steps(n);
    return r.acks.count(corr) == 0;
}
}  // namespace

TEST(CheckedWritesViewAck, AllViewWaitsForTheFoldThenAcks) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(1, kAllView));
    ASSERT_TRUE(absent(r, 1));
    r.followerFetch();
    ASSERT_EQ(r.answer(1), kOk);
    EXPECT_GT(r.foldCursor(kView), r.acks[1].baseOffset);
    EXPECT_EQ(r.foldLeadership(kView), r.replica->leadershipGeneration());
    EXPECT_EQ(r.ackLen[1], 54u);
}

TEST(CheckedWritesViewAck, LeaderViewWaitsForCommitLikeAllView) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(1, kLeaderView));
    ASSERT_TRUE(absent(r, 1));
    r.followerFetch();
    EXPECT_EQ(r.answer(1), kOk);
}

TEST(CheckedWritesViewAck, NoViewsAddsNothing) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel);
    r.produce(write(1, kAllView));
    r.produce(write(2, kAll));
    ASSERT_TRUE(absent(r, 1));
    r.followerFetch();
    ASSERT_EQ(r.answer(1), kOk);
    ASSERT_EQ(r.answer(2), kOk);
    EXPECT_EQ(r.ackStep[1], r.ackStep[2]) << "with no views the View ack fires in the pass an All ack does";
}

TEST(CheckedWritesViewAckRegression, AllAcksNeverQueueBehindViewWaitersRegression) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(100, kAll, 77, 0, /*txn=*/true));
    r.steps(50);
    r.produce(write(1, kAllView));
    r.produce(write(2, kAll));
    r.steps(50);
    r.followerFetch();
    ASSERT_EQ(r.answer(2), kOk);
    r.steps(500);
    EXPECT_EQ(r.acks.count(1), 0u) << "the open transaction holds the fold";
    r.marker(77, true);
    r.followerFetch();
    EXPECT_EQ(r.answer(1), kOk);
}

TEST(CheckedWritesViewAck, BehindAnAbortedTransactionAcksAfterTheAbort) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(100, kAll, 77, 0, /*txn=*/true));
    r.steps(50);
    r.produce(write(1, kAllView));
    r.followerFetch();
    r.steps(500);
    ASSERT_EQ(r.acks.count(1), 0u);
    r.marker(77, false);
    r.followerFetch();
    EXPECT_EQ(r.answer(1), kOk);
}

TEST(CheckedWritesViewAck, LeaderLossWhileParkedIsNotLeader) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(1, kAllView));
    ASSERT_TRUE(absent(r, 1, 200));
    r.demote();
    EXPECT_EQ(r.answer(1), kNotLeader);
    EXPECT_LT(r.ackNow[1], r.appendedAtNs + 50'000'000) << "before the deadline";
}

TEST(CheckedWritesViewAck, DeadlineIsInternal) {
    AckRig r(twoNodes(/*minIsr=*/2));
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(1, kAllView));
    ASSERT_EQ(r.answer(1), kInternal);
    const int64_t deadline = r.appendedAtNs + 50'000'000;
    EXPECT_GE(r.ackNow[1], deadline);
    EXPECT_LE(r.ackNow[1], deadline + 30'000);
    EXPECT_EQ(r.ackCount[1], 1);
}

TEST(CheckedWritesViewAckRegression, StaleLeadershipFoldCursorNeverReleasesAViewAckRegression) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(1, kAllView, 11, 0));
    r.steps(50);
    r.followerFetch();
    ASSERT_EQ(r.answer(1), kOk);
    const uint64_t oldLeadership = r.foldLeadership(kView);
    ASSERT_GT(r.foldCursor(kView), 0);

    r.demote();
    r.promote();
    ASSERT_NE(r.replica->leadershipGeneration(), oldLeadership);
    r.produce(write(2, kAll, 77, 0, /*txn=*/true));   // an open transaction after the promote holds the fold
    r.produce(write(3, kAllView, 11, 0));              // the idempotent retry of the offset-0 batch
    for (int i = 0; i < 4000; ++i) {
        if (i % 50 == 0) r.followerFetch();
        r.step();
        if (r.acks.count(3) != 0) break;
    }
    if (r.acks.count(3) != 0) {
        EXPECT_EQ(r.foldLeadership(kView), r.replica->leadershipGeneration())
            << "the retry was acked on a fold from an earlier leadership";
    }
    r.marker(77, true);
    r.followerFetch();
    ASSERT_EQ(r.answer(3), kOk);
    EXPECT_EQ(r.foldLeadership(kView), r.replica->leadershipGeneration());
}

TEST(CheckedWritesViewAckRegression, StrictTierViewWaitsForFsyncHwAndFoldRegression) {
    auto o = twoNodes();
    o.durabilityTier = 2;
    AckRig r(o);
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(100, kAll, 77, 0, /*txn=*/true));
    r.steps(50);
    r.produce(write(1, kAllView));
    ASSERT_TRUE(absent(r, 1)) << "durable locally but not on the HW: no ack on the fsync alone";
    r.followerFetch();
    r.steps(500);
    EXPECT_EQ(r.acks.count(1), 0u) << "on the HW but not folded: no ack";
    r.marker(77, true);
    r.followerFetch();
    ASSERT_EQ(r.answer(1), kOk);
    EXPECT_GT(r.foldCursor(kView), r.acks[1].baseOffset);
}

TEST(CheckedWritesViewAckRegression, RoutedViewProduceWithoutAReservationIsRefusedNotParkedRegression) {
    auto o = twoNodes(/*minIsr=*/2);
    o.ackTimeoutNs = 60'000'000'000LL;
    o.ringBytes = 1 << 22;
    AckRig r(o);
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.worker->setMaxOwedReturns(Worker::kMinOwedReturns);
    const int64_t before = r.worker->reservedAckSlotsForTest();
    r.produce(write(1, kAllView));
    r.steps(20);
    ASSERT_EQ(r.acks.count(1), 0u);
    EXPECT_EQ(r.worker->reservedAckSlotsForTest(), before + 1) << "a parked View waiter owes one return";

    for (int64_t c = 2; r.worker->reservedAckSlotsForTest() < static_cast<int64_t>(Worker::kMinOwedReturns); ++c) {
        r.produce(write(c, kAll));
        r.steps(3);
        ASSERT_LT(c, 5000);
    }
    const int64_t leo = r.leo();
    r.send_routed(write(9000, kAllView));
    EXPECT_EQ(r.answer(9000), kNotLeader);
    EXPECT_EQ(r.leo(), leo);
}

TEST(CheckedWritesViewAck, DuplicateBeforeAndAfterTheFoldIsAnsweredOnceEach) {
    AckRig r(twoNodes());
    r.typed(); r.setLevel(kLevel); r.installView(kView);
    r.produce(write(1, kAllView, 11, 0));
    r.steps(50);
    r.produce(write(2, kAllView, 11, 0));
    ASSERT_TRUE(absent(r, 2, 500));
    r.followerFetch();
    ASSERT_EQ(r.answer(1), kOk);
    ASSERT_EQ(r.answer(2), kOk);
    EXPECT_EQ(r.acks[2].baseOffset, r.acks[1].baseOffset);
    r.produce(write(3, kAllView, 11, 0));
    ASSERT_EQ(r.answer(3), kOk);
    EXPECT_EQ(r.acks[3].baseOffset, r.acks[1].baseOffset);
    EXPECT_EQ(r.leo(), 1);
    for (int64_t c : {1, 2, 3}) EXPECT_EQ(r.ackCount[c], 1) << c;
}
