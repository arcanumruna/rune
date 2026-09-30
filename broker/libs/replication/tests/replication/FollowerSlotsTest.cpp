// FollowerSlotsTest.cpp — pure unit tests for the flat per-follower leader
// bookkeeping (fetch conflation + progress-based no-dup-fetch gate, spec §3.5).
#include <gtest/gtest.h>
#include <rune/replication/FollowerSlots.h>

using namespace rune::replication;

namespace {
constexpr int64_t kMs = 1'000'000;
}

TEST(FollowerSlots, InitAssignsSlotsInReplicaOrderAndFindsById) {
    FollowerSlots s;
    ASSERT_TRUE(s.init({1, 2, 3}));
    EXPECT_EQ(s.size(), 3u);
    ASSERT_NE(s.find(2), nullptr);
    EXPECT_EQ(s.find(2)->id, 2);
    EXPECT_EQ(s.find(9), nullptr);
}

TEST(FollowerSlotsRegression, InitRefusesMoreThanCapacity) {
    FollowerSlots s;
    std::vector<NodeId> big;
    for (NodeId i = 0; i < static_cast<NodeId>(FollowerSlots::kMaxReplicas) + 1; ++i) big.push_back(i);
    EXPECT_FALSE(s.init(big));
}

TEST(FollowerSlots, IdenticalRequestInsideWindowIsConflated) {
    FollowerSlot f; f.id = 2;
    EXPECT_EQ(decideFetchServe(f, 0, 1024, 10, true, 1 * kMs, kFetchResendNs), FetchServe::Data);
    EXPECT_EQ(decideFetchServe(f, 0, 1024, 10, true, 5 * kMs, kFetchResendNs), FetchServe::Conflate);
    EXPECT_EQ(decideFetchServe(f, 0, 1024, 11, true, 6 * kMs, kFetchResendNs), FetchServe::Data)
        << "leader LEO changed → not identical";
}

TEST(FollowerSlots, CoveredAndAdvancingIsAckOnly) {
    FollowerSlot f; f.id = 2; f.pushedThrough = 100;
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs, kFetchResendNs), FetchServe::AckOnly);
    EXPECT_EQ(decideFetchServe(f, 20, 1024, 100, true, 2 * kMs, kFetchResendNs), FetchServe::AckOnly);
}

TEST(FollowerSlots, StuckForResendWindowIsServedData) {
    FollowerSlot f; f.id = 2; f.pushedThrough = 100;
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs, kFetchResendNs), FetchServe::AckOnly);
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs + kFetchResendNs, kFetchResendNs),
              FetchServe::Data);
}

TEST(FollowerSlots, NotPushedOrPushDisabledIsServedData) {
    FollowerSlot f; f.id = 2;                       // pushedThrough = 0
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs, kFetchResendNs), FetchServe::Data);
    FollowerSlot g; g.id = 3; g.pushedThrough = 100;
    EXPECT_EQ(decideFetchServe(g, 10, 1024, 100, /*pushOnAppend=*/false, 1 * kMs, kFetchResendNs),
              FetchServe::Data);
}

TEST(FollowerSlots, PureFastAckIsAckOnly) {
    FollowerSlot f; f.id = 2;
    EXPECT_EQ(decideFetchServe(f, 10, 0, 100, true, 1 * kMs, kFetchResendNs), FetchServe::AckOnly);
}

TEST(FollowerSlots, ResetLeadershipClearsCoverageKeepsIds) {
    FollowerSlots s;
    ASSERT_TRUE(s.init({1, 2}));
    s.find(2)->pushedThrough = 50;
    s.find(2)->askOffset = 7;
    s.resetLeadership();
    EXPECT_EQ(s.find(2)->pushedThrough, 0);
    EXPECT_EQ(s.find(2)->askOffset, -1);
    EXPECT_EQ(s.find(2)->id, 2);
}
