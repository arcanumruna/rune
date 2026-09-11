// ---------------------------------------------------------------------------
// EpochFencingTest.cpp — Unit tests for rune::replication epoch-fencing helpers
// ---------------------------------------------------------------------------
#include <gtest/gtest.h>
#include <rune/replication/EpochFencing.h>
#include <rune/storage/LeaderEpochCache.h>

using namespace rune::replication;
using rune::storage::LeaderEpochCache;

// ---------------------------------------------------------------------------
// isStaleEpoch
// ---------------------------------------------------------------------------

TEST(EpochFencingTest, IsStaleEpochTrueWhenRequestLessThanLocal) {
    EXPECT_TRUE(isStaleEpoch(0, 1));
    EXPECT_TRUE(isStaleEpoch(1, 5));
    EXPECT_TRUE(isStaleEpoch(9, 10));
}

TEST(EpochFencingTest, IsStaleEpochFalseWhenEqual) {
    EXPECT_FALSE(isStaleEpoch(0, 0));
    EXPECT_FALSE(isStaleEpoch(5, 5));
    EXPECT_FALSE(isStaleEpoch(100, 100));
}

TEST(EpochFencingTest, IsStaleEpochFalseWhenRequestGreaterThanLocal) {
    EXPECT_FALSE(isStaleEpoch(5, 1));
    EXPECT_FALSE(isStaleEpoch(10, 9));
    EXPECT_FALSE(isStaleEpoch(100, 0));
}

TEST(EpochFencingTest, IsStaleEpochNegativeEpochsWork) {
    // Epoch -1 is "unset" — anything positive is newer
    EXPECT_TRUE(isStaleEpoch(-1, 0));
    EXPECT_FALSE(isStaleEpoch(0, -1));
}

// ---------------------------------------------------------------------------
// mustStepDown
// ---------------------------------------------------------------------------

TEST(EpochFencingTest, MustStepDownTrueWhenObservedGreaterThanLocal) {
    EXPECT_TRUE(mustStepDown(5, 4));
    EXPECT_TRUE(mustStepDown(10, 1));
    EXPECT_TRUE(mustStepDown(1, 0));
}

TEST(EpochFencingTest, MustStepDownFalseWhenEqual) {
    EXPECT_FALSE(mustStepDown(0, 0));
    EXPECT_FALSE(mustStepDown(7, 7));
}

TEST(EpochFencingTest, MustStepDownFalseWhenObservedLessThanLocal) {
    EXPECT_FALSE(mustStepDown(1, 5));
    EXPECT_FALSE(mustStepDown(0, 10));
}

TEST(EpochFencingTest, MustStepDownComplementsIsStaleEpoch) {
    // mustStepDown(obs, local) <=> isStaleEpoch(local, obs)
    for (Epoch obs = 0; obs <= 5; ++obs) {
        for (Epoch loc = 0; loc <= 5; ++loc) {
            EXPECT_EQ(mustStepDown(obs, loc), isStaleEpoch(loc, obs))
                << "obs=" << obs << " loc=" << loc;
        }
    }
}

// ---------------------------------------------------------------------------
// truncationOffsetForEpochChange
// ---------------------------------------------------------------------------

// Build a follower cache with epochs {0→0, 1→100, 2→250}:
//   Epoch 0 starts at offset 0
//   Epoch 1 starts at offset 100
//   Epoch 2 starts at offset 250
static LeaderEpochCache makeFollowerCache() {
    LeaderEpochCache c;
    c.assign(0,   0);
    c.assign(1, 100);
    c.assign(2, 250);
    return c;
}

TEST(EpochFencingTest, TruncationForEpoch2GivesStart250) {
    // newLeaderEpoch=2: follower should truncate entries from epochs >= 2.
    // truncateOffsetForEpoch(2-1=1) → startOffset of first epoch > 1 = epoch 2 start = 250.
    auto cache = makeFollowerCache();
    Offset off = truncationOffsetForEpochChange(cache, /*newLeaderEpoch=*/2);
    EXPECT_EQ(off, 250);
}

TEST(EpochFencingTest, TruncationForEpoch1GivesStart100) {
    // newLeaderEpoch=1: truncate entries from epochs >= 1.
    // truncateOffsetForEpoch(0) → startOffset of first epoch > 0 = epoch 1 start = 100.
    auto cache = makeFollowerCache();
    Offset off = truncationOffsetForEpochChange(cache, /*newLeaderEpoch=*/1);
    EXPECT_EQ(off, 100);
}

TEST(EpochFencingTest, TruncationForEpochBeyondCacheReturnsNoTruncation) {
    // newLeaderEpoch=10: follower has no epoch >= 10 → no truncation needed.
    // truncateOffsetForEpoch(9) → no epoch > 9 in the cache → returns -1.
    auto cache = makeFollowerCache();
    Offset off = truncationOffsetForEpochChange(cache, /*newLeaderEpoch=*/10);
    EXPECT_EQ(off, -1);
}

TEST(EpochFencingTest, TruncationForEpoch3BeyondCacheReturnsNoTruncation) {
    // Follower has epochs {0,1,2}; newLeaderEpoch=3 → no epoch >= 3 → -1.
    auto cache = makeFollowerCache();
    Offset off = truncationOffsetForEpochChange(cache, 3);
    EXPECT_EQ(off, -1);
}

TEST(EpochFencingTest, TruncationForEpoch0ReturnsMinusOne) {
    // newLeaderEpoch=0: our guard returns -1 (no epoch predates 0).
    auto cache = makeFollowerCache();
    Offset off = truncationOffsetForEpochChange(cache, 0);
    EXPECT_EQ(off, -1);
}

TEST(EpochFencingTest, TruncationEmptyCacheAlwaysReturnsMinusOne) {
    LeaderEpochCache empty;
    EXPECT_EQ(truncationOffsetForEpochChange(empty, 1), -1);
    EXPECT_EQ(truncationOffsetForEpochChange(empty, 5), -1);
    EXPECT_EQ(truncationOffsetForEpochChange(empty, 0), -1);
}

TEST(EpochFencingTest, TruncationSingleEpochCacheEpoch0) {
    // Cache has only epoch 0 at offset 0.
    // newLeaderEpoch=1: truncateOffsetForEpoch(0) → first epoch > 0 = none → -1.
    LeaderEpochCache c;
    c.assign(0, 0);
    EXPECT_EQ(truncationOffsetForEpochChange(c, 1), -1);
}

TEST(EpochFencingTest, TruncationSingleEpochCacheForceTruncation) {
    // Cache has epoch 5 at offset 500.
    // newLeaderEpoch=5: truncateOffsetForEpoch(4) → first epoch > 4 = epoch 5 at 500.
    LeaderEpochCache c;
    c.assign(5, 500);
    EXPECT_EQ(truncationOffsetForEpochChange(c, 5), 500);
}

TEST(EpochFencingTest, TruncationMultipleEpochsMidPoint) {
    // Epochs: {0@0, 1@100, 2@200, 3@300, 4@400}
    // newLeaderEpoch=3: truncateOffsetForEpoch(2) → first epoch > 2 = epoch 3 at 300.
    LeaderEpochCache c;
    c.assign(0,   0);
    c.assign(1, 100);
    c.assign(2, 200);
    c.assign(3, 300);
    c.assign(4, 400);
    EXPECT_EQ(truncationOffsetForEpochChange(c, 3), 300);
}

TEST(EpochFencingTest, TruncationConcreteFollowerLearnsEpoch2) {
    // Spec §8 example: follower had epochs {0,1,2}; new leader has epoch 2.
    // Follower should truncate at 250 (start of epoch 2) — uncommitted entries
    // from the stale epoch 2 leader begin there.
    auto cache = makeFollowerCache(); // {0@0, 1@100, 2@250}
    EXPECT_EQ(truncationOffsetForEpochChange(cache, 2), 250);
}

TEST(EpochFencingTest, FencingAndStepDownConsistency) {
    // A node with localEpoch=3 receiving a request from epoch 1 should fence it
    // (isStaleEpoch returns true), and the sender should step down
    // (mustStepDown from local=1, observed=3 perspective).
    Epoch local    = 3;
    Epoch incoming = 1;
    EXPECT_TRUE(isStaleEpoch(incoming, local));      // incoming is stale: reject
    EXPECT_TRUE(mustStepDown(local, incoming));       // sender (epoch 1) would step down if it saw 3
}
