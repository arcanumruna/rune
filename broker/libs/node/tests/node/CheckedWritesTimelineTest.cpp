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
    EXPECT_TRUE(m.onMarker(15, 12));
    EXPECT_TRUE(m.onMarker(30, 25));
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
    EXPECT_TRUE(m.onMarker(15, 12));
    EXPECT_TRUE(m.onMarker(30, 25));
    m.onHw(16);
    m.truncateFrom(20);
    m.onHw(31);
    EXPECT_EQ(m.committedThrough(), 12);
    EXPECT_EQ(m.appendedThrough(), 12);
}

TEST(CheckedWritesTimeline, MarkerLedgerIsIdempotentAndFullRefuses) {
    MarkerLedger m;
    EXPECT_TRUE(m.onMarker(15, 12));
    EXPECT_TRUE(m.onMarker(15, 12));
    for (int64_t i = 1; i < static_cast<int64_t>(MarkerLedger::kCapacity); ++i) EXPECT_TRUE(m.onMarker(15 + i, 12 + i));
    EXPECT_TRUE(m.full());
    EXPECT_FALSE(m.onMarker(100000, 1));
}
