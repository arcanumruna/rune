// MembershipStateTest.cpp — TDD tests for MembershipState (Task 3).
//
// Covers:
//   - Idempotent re-registration (same incarnationId)
//   - Zombie guard: different incarnation while session still valid → RejectedZombie
//   - Expiry math with session timeout + clock-rollover guard
//   - Overload valve: setState suppressed while overloaded
//   - markCatchUpComplete / unfence-after-catch-up
//   - liveNodes filtering
//   - §8 eligibleNodes gate: joined but not caught-up is live but not eligible
//   - New incarnation resets eligibility until re-catch-up

#include <rune/metadata/MembershipState.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

TEST(MembershipState, SameIncarnationReRegisterIsIdempotentAmend) {
    MembershipState m;
    EXPECT_EQ(m.onJoin(1, "h1", /*inc*/100, /*bepoch*/10, /*now*/0), MembershipState::JoinResult::Accepted);
    EXPECT_EQ(m.onJoin(1, "h1b", 100, 10, 5), MembershipState::JoinResult::IdempotentAmend);
}

TEST(MembershipState, DifferentIncarnationWhileSessionValidIsRejectedZombie) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.onHeartbeat(1, 5); // session valid
    EXPECT_EQ(m.onJoin(1, "h1", /*newInc*/200, 10, 6), MembershipState::JoinResult::RejectedZombie);
}

TEST(MembershipState, ExpiryUsesSessionTimeoutAndIsClockRolloverGuarded) {
    MembershipState m; m.onJoin(1, "h1", 100, 10, 0); m.onHeartbeat(1, 1'000);
    const int64_t timeout = 500;
    EXPECT_FALSE(m.isExpired(1, 1'400, timeout));
    EXPECT_TRUE (m.isExpired(1, 1'600, timeout));
    // clock rollover: now < lastHeartbeat must NOT read as expired
    EXPECT_FALSE(m.isExpired(1, 900, timeout));
}

TEST(MembershipState, OverloadValveSuppressesStateTransitions) {
    MembershipState m; m.onJoin(1, "h1", 100, 10, 0);
    m.setOverloaded(true);
    m.setState(1, NodeControlState::Fenced); // suppressed
    EXPECT_NE(m.liveNodes(0, 500).empty(), true); // still considered present
}

TEST(MembershipState, FreshJoinReturnsAcceptedAndNodeIsLive) {
    MembershipState m;
    auto result = m.onJoin(7, "host7:9000", 42, 1, 0);
    EXPECT_EQ(result, MembershipState::JoinResult::Accepted);
    auto live = m.liveNodes(0, 10'000);
    ASSERT_EQ(live.size(), 1u);
    EXPECT_EQ(live[0], 7);
}

TEST(MembershipState, HeartbeatUpdatesLastSeen) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.onHeartbeat(1, 5'000);
    // Should not be expired at now=5'200, timeout=500
    EXPECT_FALSE(m.isExpired(1, 5'200, 500));
    // Should be expired at now=5'600, timeout=500
    EXPECT_TRUE(m.isExpired(1, 5'600, 500));
}

TEST(MembershipState, ExpiredSessionAllowsNewIncarnation) {
    // Use a session timeout of 500 ns so that heartbeat at t=100, re-join at t=1000
    // (diff = 900 > 500) is treated as expired.
    MembershipState m{500};
    m.onJoin(1, "h1", 100, 10, 0);
    m.onHeartbeat(1, 100);
    // Now = 1000, timeout = 500 → session expired (1000 - 100 = 900 > 500)
    auto result = m.onJoin(1, "h1-new", /*newInc*/999, 11, 1000);
    EXPECT_EQ(result, MembershipState::JoinResult::Accepted);
}

TEST(MembershipState, OnLeaveRemovesNode) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.onLeave(1);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_TRUE(live.empty());
}

TEST(MembershipState, FencedNodeNotInLiveNodes) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.setState(1, NodeControlState::Fenced);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_TRUE(live.empty());
}

TEST(MembershipState, UnfencedAfterMarkCatchUpComplete) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.setState(1, NodeControlState::Fenced);
    // Initially not live
    EXPECT_TRUE(m.liveNodes(0, 10'000).empty());
    // Mark catch-up complete → unfence
    bool changed = m.markCatchUpComplete(1);
    EXPECT_TRUE(changed);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_EQ(live.size(), 1u);
}

TEST(MembershipState, OverloadedFlagGetSet) {
    MembershipState m;
    EXPECT_FALSE(m.overloaded());
    m.setOverloaded(true);
    EXPECT_TRUE(m.overloaded());
    m.setOverloaded(false);
    EXPECT_FALSE(m.overloaded());
}

TEST(MembershipState, OverloadValveAllowsStateTransitionWhenOff) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.setOverloaded(false);
    m.setState(1, NodeControlState::Fenced);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_TRUE(live.empty()); // Fenced → not live
}

TEST(MembershipState, MultipleNodesTrackedIndependently) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.onJoin(2, "h2", 200, 20, 0);
    m.onJoin(3, "h3", 300, 30, 0);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_EQ(live.size(), 3u);
    // Fence node 2
    m.setState(2, NodeControlState::Fenced);
    live = m.liveNodes(0, 10'000);
    EXPECT_EQ(live.size(), 2u);
}

TEST(MembershipState, ShutdownNowNodeNotInLiveNodes) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.setState(1, NodeControlState::ShutdownNow);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_TRUE(live.empty());
}

TEST(MembershipState, ControlledShutdownNodeIsInLiveNodes) {
    // ControlledShutdown is not Fenced/ShutdownNow so stays live
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.setState(1, NodeControlState::ControlledShutdown);
    auto live = m.liveNodes(0, 10'000);
    EXPECT_EQ(live.size(), 1u);
}

TEST(MembershipState, IdempotentAmendUpdatesEndpoint) {
    MembershipState m;
    m.onJoin(1, "h1-old", 100, 10, 0);
    auto result = m.onJoin(1, "h1-new", 100, 10, 5);
    EXPECT_EQ(result, MembershipState::JoinResult::IdempotentAmend);
    // Node should still be live
    auto live = m.liveNodes(0, 10'000);
    EXPECT_EQ(live.size(), 1u);
}

// ---------------------------------------------------------------------------
// §8 parity: eligibleNodes gate
// ---------------------------------------------------------------------------

TEST(MembershipState, JustJoinedNodeNotEligibleUntilCaughtUp) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);

    // Node appears in liveNodes immediately after join (Unfenced, session valid)
    auto live = m.liveNodes(0, 10'000);
    ASSERT_EQ(live.size(), 1u);
    EXPECT_EQ(live[0], 1);

    // But NOT in eligibleNodes — caughtUp is still false
    auto eligible = m.eligibleNodes(0, 10'000);
    EXPECT_TRUE(eligible.empty());

    // After markCatchUpComplete the node becomes eligible
    EXPECT_TRUE(m.markCatchUpComplete(1));
    eligible = m.eligibleNodes(0, 10'000);
    ASSERT_EQ(eligible.size(), 1u);
    EXPECT_EQ(eligible[0], 1);
}

TEST(MembershipState, NewIncarnationDropsEligibilityUntilRecatchUp) {
    // Use a short session timeout (500 ns) so the incumbent expires quickly.
    MembershipState m{500};

    // First incarnation joins, catches up, becomes eligible.
    m.onJoin(1, "h1", 100, 10, 0);
    m.markCatchUpComplete(1);
    ASSERT_EQ(m.eligibleNodes(0, 10'000).size(), 1u);

    // Wait for session to expire (heartbeat at t=100, re-join at t=1000 → 900 > 500).
    m.onHeartbeat(1, 100);
    auto result = m.onJoin(1, "h1-new", /*newInc*/200, 11, 1000);
    EXPECT_EQ(result, MembershipState::JoinResult::Accepted);

    // New incarnation: in liveNodes but NOT yet eligible (caughtUp reset to false).
    auto live = m.liveNodes(1000, 10'000);
    ASSERT_EQ(live.size(), 1u);
    auto eligible = m.eligibleNodes(1000, 10'000);
    EXPECT_TRUE(eligible.empty());

    // Re-catch-up restores eligibility.
    m.markCatchUpComplete(1);
    eligible = m.eligibleNodes(1000, 10'000);
    ASSERT_EQ(eligible.size(), 1u);
    EXPECT_EQ(eligible[0], 1);
}
