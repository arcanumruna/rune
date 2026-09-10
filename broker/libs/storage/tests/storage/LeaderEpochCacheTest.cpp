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

// Overwrite existing epoch
TEST(LeaderEpochCache, OverwriteEpoch) {
    LeaderEpochCache c;
    c.assign(1, 100);
    EXPECT_EQ(c.startOffsetFor(1), 100);
    c.assign(1, 200);
    EXPECT_EQ(c.startOffsetFor(1), 200);
}
