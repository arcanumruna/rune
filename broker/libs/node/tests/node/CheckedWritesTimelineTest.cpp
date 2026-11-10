// Part 04 Task 5: the constraint-set timeline and the marker ledger.
#include <gtest/gtest.h>

#include <rune/node/VerdictState.h>

using namespace rune::node;

namespace {
ConstraintSet setOf(uint32_t id, std::initializer_list<uint64_t> objectIds) {
    ConstraintSet s;
    s.id = id;
    for (const uint64_t o : objectIds) s.members[s.count++] = {o, rune::view::VerdictViewKind::TestReject, 0};
    return s;
}
}  // namespace

TEST(CheckedWritesTimeline, SetAtTakesTheLatestChangeBelow) {
    ConstraintSetTimeline t;
    t.onChange(10, setOf(1, {7}));
    t.onChange(20, setOf(2, {7, 8}));
    EXPECT_EQ(t.setAt(10)->id, 0u);
    EXPECT_TRUE(t.setAt(10)->empty());
    EXPECT_EQ(t.setAt(11)->id, 1u);
    EXPECT_EQ(t.setAt(20)->id, 1u);
    EXPECT_EQ(t.setAt(21)->id, 2u);
    EXPECT_EQ(t.nextChangeAtOrAfter(11), 20);
    EXPECT_EQ(t.nextChangeAtOrAfter(21), INT64_MAX);
    EXPECT_EQ(t.latest().id, 2u);
}

TEST(CheckedWritesTimeline, OnChangeIsIdempotentAndTruncates) {
    ConstraintSetTimeline t;
    t.onChange(10, setOf(1, {7}));
    t.onChange(10, setOf(1, {7}));
    EXPECT_EQ(t.size(), 1u);
    t.truncateFrom(10);
    EXPECT_EQ(t.size(), 0u);
    EXPECT_TRUE(t.latest().empty());
}

TEST(CheckedWritesTimeline, MarkerLedgerPromotesOnHw) {
    MarkerLedger m;
    EXPECT_TRUE(m.onMarker(15, 12, true));
    EXPECT_TRUE(m.onMarker(30, 25, true));
    m.onHw(16);
    EXPECT_EQ(m.committedThrough(), 12);
    EXPECT_EQ(m.committedMarkerEnd(), 16);
    EXPECT_EQ(m.appendedThrough(), 25);
    m.onHw(31);
    EXPECT_EQ(m.committedThrough(), 25);
    EXPECT_EQ(m.committedMarkerEnd(), 31);
}

TEST(CheckedWritesTimeline, MarkerLedgerTruncateDropsTheUncommittedTail) {
    MarkerLedger m;
    EXPECT_TRUE(m.onMarker(15, 12, true));
    EXPECT_TRUE(m.onMarker(30, 25, true));
    m.onHw(16);
    m.truncateFrom(20);
    m.onHw(31);
    EXPECT_EQ(m.committedThrough(), 12);
    EXPECT_EQ(m.appendedThrough(), 12);
}

TEST(CheckedWritesTimeline, MarkerLedgerIsIdempotentAndFullRefuses) {
    MarkerLedger m;
    EXPECT_TRUE(m.onMarker(15, 12, true));
    EXPECT_TRUE(m.onMarker(15, 12, true));
    EXPECT_EQ(m.appendedThrough(), 12) << "the duplicate changes nothing";
    for (int64_t i = 1; i < static_cast<int64_t>(MarkerLedger::kCapacity); ++i) EXPECT_TRUE(m.onMarker(15 + i, 12 + i, true));
    EXPECT_TRUE(m.full());
    constexpr int64_t kRingThrough = 12 + static_cast<int64_t>(MarkerLedger::kCapacity) - 1;
    EXPECT_EQ(m.appendedThrough(), kRingThrough);
    EXPECT_FALSE(m.onMarker(100000, 1, true));
    EXPECT_EQ(m.appendedThrough(), kRingThrough) << "a left-out marker below the coverage does not lower it";
    EXPECT_TRUE(m.onMarker(100000, 1, true)) << "the left-out marker is remembered";
    EXPECT_EQ(m.appendedThrough(), kRingThrough);
}

// A marker the full ring leaves out is in the log all the same: appendedThrough counts it until a cut removes it.
TEST(CheckedWritesTimelineRegression, MarkerLedgerCountsAMarkerLeftOutWhenFullRegression) {
    MarkerLedger m;
    for (int64_t i = 1; i <= static_cast<int64_t>(MarkerLedger::kCapacity); ++i) ASSERT_TRUE(m.onMarker(i, i, true));
    EXPECT_FALSE(m.onMarker(2000, 3000, true));
    EXPECT_EQ(m.appendedThrough(), 3000);
    m.truncateFrom(2001);
    EXPECT_EQ(m.appendedThrough(), 3000) << "the cut is above it";
    m.truncateFrom(2000);
    EXPECT_EQ(m.appendedThrough(), static_cast<int64_t>(MarkerLedger::kCapacity));
}

// Fix pass 2 (B): a scan re-feed of a left-out marker below the last one must not move left_ back.  RED: left_ became {2000, 4000},
// so a cut that removed the marker at 2001 kept its coverage and appendedThrough came out too high (4000).  The cut forgets every
// left-out marker (left_ holds the last one only), so the value is the ring's coverage: too low only re-marks more.
TEST(CheckedWritesTimelineRegression, MarkerLedgerReFeedBelowTheLastLeftOutMarkerIsIgnoredRegression) {
    MarkerLedger m;
    for (int64_t i = 0; i < static_cast<int64_t>(MarkerLedger::kCapacity); ++i) ASSERT_TRUE(m.onMarker(i, i, true));
    EXPECT_FALSE(m.onMarker(2000, 3000, true));
    EXPECT_FALSE(m.onMarker(2001, 4000, true));
    EXPECT_TRUE(m.onMarker(2000, 3000, true)) << "already seen";
    m.truncateFrom(2001);
    EXPECT_LE(m.appendedThrough(), 3000) << "never above the coverage still in the log";
    EXPECT_EQ(m.appendedThrough(), static_cast<int64_t>(MarkerLedger::kCapacity) - 1);
}

// REGRESSION (#974): a full marker may end inside a transaction span: W stays at the last closing marker, and the chain is open until
// a closing marker follows.
TEST(CheckedWritesTimelineRegression, MarkerLedgerFullMarkerHoldsCommittedCoverageRegression) {
    MarkerLedger m;
    EXPECT_TRUE(m.onMarker(15, 12, true));
    EXPECT_TRUE(m.onMarker(30, 25, false));
    EXPECT_TRUE(m.chainOpen());
    EXPECT_EQ(m.appendedThrough(), 25);
    EXPECT_EQ(m.appendedClosedThrough(), 12);
    m.onHw(31);
    EXPECT_EQ(m.committedThrough(), 12);
    EXPECT_EQ(m.committedMarkerEnd(), 31);
    EXPECT_TRUE(m.onMarker(45, 40, true));
    EXPECT_FALSE(m.chainOpen());
    m.onHw(46);
    EXPECT_EQ(m.committedThrough(), 40);
}

TEST(CheckedWritesTimelineRegression, MarkerLedgerLeftOutFullMarkerKeepsTheChainOpenRegression) {
    MarkerLedger m;
    for (int64_t i = 1; i <= static_cast<int64_t>(MarkerLedger::kCapacity); ++i) ASSERT_TRUE(m.onMarker(i, i, true));
    EXPECT_FALSE(m.onMarker(2000, 3000, false));
    EXPECT_TRUE(m.chainOpen());
    m.truncateFrom(2001);
    EXPECT_TRUE(m.chainOpen()) << "the left-out full marker below the cut stays";
    EXPECT_EQ(m.appendedThrough(), 3000);
    EXPECT_EQ(m.appendedClosedThrough(), static_cast<int64_t>(MarkerLedger::kCapacity));
    EXPECT_FALSE(m.onMarker(2002, 3500, true));
    EXPECT_FALSE(m.chainOpen());
}

// REGRESSION (slow-bc review 1): a committed full marker leaves the ring at the HW, and the leadership scan's truncate at the LEO rebuilt
// appendedThrough without it: the new leader re-marked a marked range, and the chain read closed with no closing marker yet.
TEST(CheckedWritesTimelineRegression, MarkerLedgerTruncateKeepsACommittedFullMarkersEndRegression) {
    MarkerLedger m;
    EXPECT_TRUE(m.onMarker(5, 100, true));
    EXPECT_TRUE(m.onMarker(200, 9000, false));
    m.onHw(201);
    m.truncateFrom(10000);
    EXPECT_EQ(m.appendedThrough(), 9000);
    EXPECT_EQ(m.appendedClosedThrough(), 100);
    EXPECT_TRUE(m.chainOpen()) << "no closing marker follows the committed full one";
}
