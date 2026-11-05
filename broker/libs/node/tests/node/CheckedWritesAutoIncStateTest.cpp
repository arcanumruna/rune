// CheckedWritesAutoIncStateTest.cpp - checked writes Part 06 Task 7: the autoincrement counter, its plan and the
// assigned-base side window.
#include <gtest/gtest.h>

#include <rune/node/AutoInc.h>

using namespace rune::node;

namespace {
constexpr uint64_t kMax53 = (uint64_t{1} << 53) - 1;

AutoIncState readyAt(int32_t partition, int32_t partitions, uint64_t max, uint32_t reserve, uint64_t reservedThroughN) {
    AutoIncState s;
    s.configure(partition, partitions, max, reserve);
    s.beginScan();
    s.onReservation(reservedThroughN);
    s.onAssignedThrough(reservedThroughN);
    EXPECT_EQ(s.finishScan(), 0u);
    return s;
}

/// A fresh Ready{1, 0} moved up by the commits.
AutoIncState freshAt(int32_t partition, int32_t partitions, uint64_t max, uint32_t reserve, uint64_t nextN, uint64_t reservedThroughN) {
    AutoIncState s;
    s.configure(partition, partitions, max, reserve);
    s.beginScan();
    EXPECT_EQ(s.finishScan(), 0u);
    s.commitReservation(reservedThroughN);
    s.commitNext(nextN);
    return s;
}
}  // namespace

TEST(CheckedWritesAutoIncState, FreshTopicStartsAtOne) {
    AutoIncState s;
    s.configure(0, 1, kMax53, 4096);
    s.beginScan();
    EXPECT_EQ(s.finishScan(), 0u);
    EXPECT_EQ(s.nextN(), 1u);
    EXPECT_EQ(s.reservedThroughN(), 0u);
    const AutoIncPlan p = s.plan(1, 0);
    EXPECT_EQ(p.firstN, 1u);
    EXPECT_EQ(p.count, 1u);
    EXPECT_EQ(p.newNextN, 2u);
    EXPECT_TRUE(p.reserve);
    EXPECT_EQ(p.reserveThroughN, 4096u);
    EXPECT_FALSE(p.exhausted);
}

TEST(CheckedWritesAutoIncState, ResumesAboveTheLatestReservation) {
    AutoIncState s;
    s.configure(0, 1, kMax53, 4096);
    s.beginScan();
    s.onReservation(4096);
    s.onReservation(8192);
    s.onAssignedThrough(5000);
    EXPECT_EQ(s.finishScan(), 3192u);
    EXPECT_EQ(s.nextN(), 8193u);
    EXPECT_EQ(s.reservedThroughN(), 8192u);
}

TEST(CheckedWritesAutoIncState, CrossingTheReservationReservesAFullBlock) {
    const AutoIncState s = freshAt(0, 1, kMax53, 8, 7, 8);
    const AutoIncPlan p = s.plan(8, 0);
    EXPECT_EQ(p.firstN, 7u);
    EXPECT_EQ(p.count, 8u);
    EXPECT_EQ(p.newNextN, 15u);
    EXPECT_TRUE(p.reserve);
    EXPECT_EQ(p.reserveThroughN, 16u);
}

TEST(CheckedWritesAutoIncState, IdsFollowTheStride) {
    EXPECT_EQ(autoIncId(1, 4, 2), 6u);
    EXPECT_EQ(autoIncId(2, 4, 2), 10u);
    EXPECT_EQ(autoIncId(1, 1, 0), 1u);
    EXPECT_EQ(autoIncId(2, 1, 0), 2u);
    EXPECT_EQ(autoIncId(3, 1, 0), 3u);
}

TEST(CheckedWritesAutoIncState, ExhaustionIsExactAtTheCap) {
    AutoIncState a = readyAt(0, 1, 10, 4096, 7);
    EXPECT_EQ(a.nextN(), 8u);
    EXPECT_FALSE(a.plan(3, 0).exhausted);
    EXPECT_TRUE(a.plan(4, 0).exhausted);
    EXPECT_FALSE(a.plan(4, 0).reserve);
    EXPECT_EQ(a.plan(3, 0).reserveThroughN, 10u);   // the reservation never exceeds nMax

    AutoIncState b;
    b.configure(3, 4, 10, 4096);
    EXPECT_EQ(b.nMax(), 1u);

    AutoIncState c;
    c.configure(3, 4, 2, 4096);
    EXPECT_EQ(c.nMax(), 0u);
    c.beginScan();
    EXPECT_EQ(c.finishScan(), 0u);
    EXPECT_TRUE(c.plan(1, 0).exhausted);
}

TEST(CheckedWritesAutoIncState, ImportMovesTheCounterPastIt) {
    const AutoIncState t = freshAt(1, 2, kMax53, 4096, 5, 4096);
    const AutoIncPlan low = t.plan(0, 50);
    EXPECT_EQ(low.newNextN, 51u);
    EXPECT_FALSE(low.reserve);
    const AutoIncPlan high = t.plan(0, 5000);
    EXPECT_EQ(high.newNextN, 5001u);
    EXPECT_TRUE(high.reserve);
    EXPECT_EQ(high.reserveThroughN, 8192u);
    EXPECT_EQ(t.plan(0, 3).newNextN, 5u);
}

TEST(CheckedWritesAutoIncState, ImportAboveTheCapReservesNothing) {
    AutoIncState s = readyAt(0, 1, 10, 4096, 10);
    const AutoIncPlan p = s.plan(0, 50);
    EXPECT_FALSE(p.reserve);
    EXPECT_EQ(p.reserveThroughN, 10u);
}

TEST(CheckedWritesAutoIncState, CommitsNeverMoveBackwards) {
    AutoIncState s = readyAt(0, 1, kMax53, 4096, 100);
    s.commitNext(50);
    s.commitReservation(40);
    EXPECT_EQ(s.nextN(), 101u);
    EXPECT_EQ(s.reservedThroughN(), 100u);
    s.commitNext(120);
    s.commitReservation(200);
    EXPECT_EQ(s.nextN(), 120u);
    EXPECT_EQ(s.reservedThroughN(), 200u);
}

TEST(CheckedWritesAutoIncState, NotReadyOutsideReady) {
    AutoIncState s;
    s.configure(0, 1, kMax53, 4096);
    EXPECT_FALSE(s.ready());
    s.beginScan();
    EXPECT_FALSE(s.ready());
    EXPECT_EQ(s.finishScan(), 0u);
    EXPECT_TRUE(s.ready());
    s.deactivate();
    EXPECT_FALSE(s.ready());
}

TEST(CheckedWritesAutoIncState, UInt32KeyCapsTheMax) {
    EXPECT_EQ(effectiveAutoIncMax(rune::codec::PrimType::UInt32, kMax53), uint64_t{UINT32_MAX});
    EXPECT_EQ(effectiveAutoIncMax(rune::codec::PrimType::UInt32, 100), 100u);
    EXPECT_EQ(effectiveAutoIncMax(rune::codec::PrimType::UInt64, kMax53), kMax53);
}

TEST(CheckedWritesAutoIncWindow, RecordAndFind) {
    AssignedBaseWindow w;
    w.reserve(16);
    for (int64_t i = 0; i < 34; ++i) w.record(7, i * 10, 1000 + static_cast<uint64_t>(i));
    EXPECT_EQ(w.find(7, 330), 1033u);
    EXPECT_EQ(w.find(7, 0), 0u);
    EXPECT_EQ(w.find(7, 20), 1002u);
    EXPECT_EQ(w.find(8, 10), 0u);
    EXPECT_TRUE(w.isKeyZeroBatch(7, 330));
    EXPECT_FALSE(w.isKeyZeroBatch(8, 330));
}

TEST(CheckedWritesAutoIncWindow, ScannedEntriesAreNotKeyZeroAndGrow) {
    AssignedBaseWindow w;
    w.reserve(2);
    for (int64_t pid = 0; pid < 5; ++pid) w.recordScanned(pid, 40, 500 + static_cast<uint64_t>(pid));
    EXPECT_GE(w.capacity(), 5u);
    EXPECT_EQ(w.producers(), 5u);
    EXPECT_EQ(w.find(4, 40), 504u);
    EXPECT_FALSE(w.isKeyZeroBatch(4, 40));
    w.clear();
    EXPECT_EQ(w.producers(), 0u);
    EXPECT_EQ(w.find(4, 40), 0u);
}

TEST(CheckedWritesAutoIncWindow, CanAcceptAtCapacity) {
    AssignedBaseWindow w;
    w.reserve(4);
    ASSERT_EQ(w.capacity(), 4u);
    for (int64_t pid = 1; pid <= 4; ++pid) {
        ASSERT_TRUE(w.canAccept(pid));
        w.record(pid, 0, 100 + static_cast<uint64_t>(pid));
    }
    EXPECT_FALSE(w.canAccept(5));
    EXPECT_TRUE(w.canAccept(3));
    w.grow(8);
    EXPECT_TRUE(w.canAccept(5));
    w.record(5, 0, 105);
    for (int64_t pid = 1; pid <= 5; ++pid) EXPECT_EQ(w.find(pid, 0), 100 + static_cast<uint64_t>(pid));
}
