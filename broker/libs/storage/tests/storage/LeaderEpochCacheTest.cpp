#include <gtest/gtest.h>
#include <rune/storage/LeaderEpochCache.h>
using rune::storage::LeaderEpochCache;

// Base test from brief
TEST(LeaderEpochCache, TracksEpochStartsAndTruncation) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(1, 100);
    c.assign(2, 250);
    EXPECT_EQ(c.latestEpoch(), 2);
    EXPECT_EQ(c.startOffsetFor(1), 100);
    // A follower on epoch 1 must truncate to the start of the next epoch (250).
    EXPECT_EQ(c.truncateOffsetForEpoch(1), 250);
}

// Empty cache edge case
TEST(LeaderEpochCache, EmptyCache) {
    LeaderEpochCache c;
    EXPECT_EQ(c.latestEpoch(), -1);
    EXPECT_EQ(c.startOffsetFor(0), -1);
    EXPECT_EQ(c.truncateOffsetForEpoch(0), -1);
}

// Single epoch
TEST(LeaderEpochCache, SingleEpoch) {
    LeaderEpochCache c;
    c.assign(5, 1000);
    EXPECT_EQ(c.latestEpoch(), 5);
    EXPECT_EQ(c.startOffsetFor(5), 1000);
    EXPECT_EQ(c.truncateOffsetForEpoch(5), -1);
}

// Truncate on latest epoch returns -1
TEST(LeaderEpochCache, TruncateLatestEpochReturnsMinusOne) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(1, 100);
    c.assign(2, 250);
    EXPECT_EQ(c.truncateOffsetForEpoch(2), -1);
}

// startOffsetFor unknown epoch returns -1
TEST(LeaderEpochCache, UnknownEpochReturnsMinusOne) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(2, 250);
    EXPECT_EQ(c.startOffsetFor(1), -1);
    EXPECT_EQ(c.startOffsetFor(999), -1);
}

// Non-contiguous epoch numbers
TEST(LeaderEpochCache, NonContiguousEpochs) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(3, 150);
    c.assign(7, 500);
    EXPECT_EQ(c.latestEpoch(), 7);
    EXPECT_EQ(c.startOffsetFor(0), 0);
    EXPECT_EQ(c.startOffsetFor(3), 150);
    EXPECT_EQ(c.startOffsetFor(7), 500);
    EXPECT_EQ(c.startOffsetFor(1), -1);
    EXPECT_EQ(c.truncateOffsetForEpoch(0), 150);  // next after 0 is 3
    EXPECT_EQ(c.truncateOffsetForEpoch(3), 500);  // next after 3 is 7
    EXPECT_EQ(c.truncateOffsetForEpoch(7), -1);   // no epoch after 7
}

// A5: Re-assigning the same epoch is a no-op (monotonic enforcement).
// In Kafka, epoch numbers are strictly increasing; re-using an epoch
// number with a different startOffset is ignored to preserve monotonicity.
// The first assignment wins.
TEST(LeaderEpochCache, OverwriteEpochIsNoOp) {
    LeaderEpochCache c;
    c.assign(1, 100);
    EXPECT_EQ(c.startOffsetFor(1), 100);
    // Same epoch, higher startOffset: ignored (epoch not strictly greater).
    c.assign(1, 200);
    EXPECT_EQ(c.startOffsetFor(1), 100) << "Re-assign of same epoch must be a no-op";
}

// ---------------------------------------------------------------------------
// B24 / A5 — monotonic epoch enforcement and truncateFromEnd coupling
// ---------------------------------------------------------------------------

// B24a: Non-monotonic epoch (lower epoch after higher) is silently ignored.
TEST(LeaderEpochCache, NonMonotonicEpochIsIgnored) {
    LeaderEpochCache c;
    c.assign(3, 300);
    c.assign(5, 500);
    // Attempt to assign a lower epoch — must be a no-op.
    c.assign(2, 200);
    EXPECT_EQ(c.latestEpoch(), 5) << "latestEpoch must not regress after non-monotonic assign";
    EXPECT_EQ(c.startOffsetFor(2), -1) << "Lower epoch must not be present in cache";
    EXPECT_EQ(c.startOffsetFor(5), 500);
}

// B24b: Non-monotonic startOffset (lower startOffset than current latest)
// with a higher epoch is ignored (offset regression is also disallowed).
TEST(LeaderEpochCache, NonMonotonicOffsetIsIgnored) {
    LeaderEpochCache c;
    c.assign(1, 500);
    // epoch 2 > epoch 1, but startOffset 100 < 500 — reject.
    c.assign(2, 100);
    EXPECT_EQ(c.latestEpoch(), 1) << "Epoch with regressing startOffset must be ignored";
    EXPECT_EQ(c.startOffsetFor(2), -1);
}

// B24c: truncateFromEnd prunes epochs whose startOffset >= truncateOffset.
TEST(LeaderEpochCache, TruncateFromEndPrunesEpochs) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(1, 100);
    c.assign(2, 250);
    c.assign(3, 400);

    // Truncate to offset 250: epochs with startOffset >= 250 (epochs 2 and 3) must go.
    c.truncateFromEnd(250);

    EXPECT_EQ(c.latestEpoch(), 1) << "Epochs at/past truncateOffset must be pruned";
    EXPECT_EQ(c.startOffsetFor(0), 0);
    EXPECT_EQ(c.startOffsetFor(1), 100);
    EXPECT_EQ(c.startOffsetFor(2), -1) << "Epoch 2 starts at 250 >= 250, must be pruned";
    EXPECT_EQ(c.startOffsetFor(3), -1) << "Epoch 3 starts at 400 >= 250, must be pruned";
}

// B24d: truncateFromEnd to 0 removes all epochs.
TEST(LeaderEpochCache, TruncateFromEndToZeroRemovesAll) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(1, 100);
    c.truncateFromEnd(0);
    EXPECT_EQ(c.latestEpoch(), -1) << "All epochs must be pruned when truncating to 0";
}

// B24e: After truncateFromEnd, new monotonic assigns still work.
TEST(LeaderEpochCache, AssignAfterTruncateFromEnd) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(1, 100);
    c.assign(2, 200);
    c.truncateFromEnd(100); // prunes epochs 1 and 2
    EXPECT_EQ(c.latestEpoch(), 0);
    // Now assign epoch 3 with a monotonically valid startOffset.
    c.assign(3, 50);        // offset 50 >= startOffset of epoch 0 (0) — valid
    EXPECT_EQ(c.latestEpoch(), 3);
    EXPECT_EQ(c.startOffsetFor(3), 50);
}
