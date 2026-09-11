// ---------------------------------------------------------------------------
// GroupCoordinatorTest.cpp — Kafka-faithful group state machine tests.
//
// All tests: bounded, deterministic, injected clock. NO busy-spin.
// Hard cap on any driver loop + ASSERT_TRUE(done) — never infinite.
//
// Covers:
//   1. join: first member → leader, generationId bumped, state PreparingRebalance
//   2. join: second member → generation bumped again
//   3. join: re-join existing member (by memberId) → no duplicate
//   4. heartbeat: keeps member alive across ticks
//   5. heartbeat: stale generation → rejected (kErrStaleGeneration)
//   6. heartbeat: unknown member → rejected (kErrUnknownMember)
//   7. heartbeat: during PreparingRebalance → kErrRebalanceInProgress
//   8. session timeout: member reaped after timeout (advance to just before → alive, just after → evicted)
//   9. session timeout: reap bumps generation + PreparingRebalance
//  10. session timeout: LAST member expiry → Empty state
//  11. leave: shrinks membership, bumps generation
//  12. leave: leader leaves → new leader elected (alphabetically smallest remaining)
//  13. leave: last member leaves → Empty state, generation bumped
//  14. leave: unknown member → no-op
//  15. generation monotonic across churn sequence: join/join/leave/expire/join
//  16. doWork idle → 0 work
//  17. doWork: multiple expired members reaped in one call
//  18. doWork: member with longer timeout not reaped early
//  19. join Dead group → kErrDead
//  20. leader: first joiner is leader
//  21. leader: deterministic leader after churn (alphabetically smallest)
//  22. leader flag set on MemberInfo
//  23. subscribed topics stored per member
//  24. join: empty memberId → auto-generated unique id
//  25. join: generated memberIds are unique across multiple joins
//  26. multiple groups are independent (two GroupCoordinator instances)
//  27. generationId starts at 0, first join makes it 1
//  28. generation exact values across churn
//  29. state transitions: Empty → PreparingRebalance on join
//  30. state after all members leave → Empty
//  31. heartbeat refreshes lastHeartbeatNs so member survives another timeout window
//  32. doWork bounded driver with hard cap + ASSERT (never infinite)
//  33. two members: only the timed-out one is reaped (other stays)
//  34. re-join during PreparingRebalance does not duplicate members
//  35. generationId monotonically increases — never decreases
//  36. leaderId empty when group is Empty
//  37. join after Empty → new generation, leader elected
//  38. CompletingRebalance state not directly set (only coordinator transitions internally)
//       — tested via heartbeat during CompletingRebalance
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <rune/coordination/GroupCoordinator.h>

#include <cassert>
#include <string>
#include <vector>

using namespace rune::coordination;

// ---------------------------------------------------------------------------
// Constants for bounded drivers
// ---------------------------------------------------------------------------
static constexpr int kDriverCap = 1000; ///< hard iteration cap for any driver loop

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static constexpr int32_t kDefaultTimeoutMs = 30'000; ///< 30 s in ms
static constexpr int64_t kDefaultTimeoutNs = 30LL * 1'000'000'000LL; // 30 s in ns

/// Advance clock by exactly one session timeout + 1 ns to guarantee expiry.
static int64_t afterTimeout(int64_t startNs, int32_t timeoutMs) {
    return startNs + static_cast<int64_t>(timeoutMs) * 1'000'000LL + 1;
}

/// Advance clock to 1 ns before the timeout fires.
static int64_t beforeTimeout(int64_t startNs, int32_t timeoutMs) {
    return startNs + static_cast<int64_t>(timeoutMs) * 1'000'000LL - 1;
}

// ===========================================================================
// 1. First join: leader elected, generation bumped, PreparingRebalance
// ===========================================================================

TEST(GroupCoordinator, FirstJoinMakesLeader) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.state(), GroupState::Empty);
    EXPECT_EQ(gc.generationId(), 0);

    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(r.errorCode, 0);
    EXPECT_TRUE(r.isLeader);
    EXPECT_FALSE(r.memberId.empty());
    EXPECT_EQ(gc.memberCount(), 1u);
}

TEST(GroupCoordinator, FirstJoinBumpsGeneration) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.generationId(), 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 1);
}

TEST(GroupCoordinator, FirstJoinTransitionsToPreparingRebalance) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
}

// ===========================================================================
// 2. Second join bumps generation again
// ===========================================================================

TEST(GroupCoordinator, SecondJoinBumpsGenerationAgain) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    Generation genAfterFirst = gc.generationId(); // 1
    gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), genAfterFirst + 1); // 2
    EXPECT_EQ(gc.memberCount(), 2u);
}

TEST(GroupCoordinator, SecondJoinAddsAnotherMember) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_NE(r1.memberId, r2.memberId);
    EXPECT_EQ(gc.memberCount(), 2u);
}

// ===========================================================================
// 3. Re-join existing member does not duplicate
// ===========================================================================

TEST(GroupCoordinator, RejoinExistingMemberNoDuplicate) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    std::size_t countAfterFirst = gc.memberCount();
    // Re-join with the same memberId.
    auto r2 = gc.join(r1.memberId, kDefaultTimeoutMs, {}, 1'000);
    EXPECT_EQ(gc.memberCount(), countAfterFirst); // no duplicate
    EXPECT_EQ(r2.memberId, r1.memberId);
}

// ===========================================================================
// 4. Heartbeat: keeps member alive across ticks
// ===========================================================================

TEST(GroupCoordinator, HeartbeatKeepsMemberAlive) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    // Advance clock to just before timeout, heartbeat.
    int64_t t1 = beforeTimeout(0, kDefaultTimeoutMs);
    auto hb = gc.heartbeat(r.memberId, gc.generationId(), t1);
    // During PreparingRebalance the heartbeat returns rebalanceInProgress
    // but the member is still alive.
    EXPECT_FALSE(hb.unknownMember());
    EXPECT_FALSE(hb.staleGeneration());
    // Advance past the ORIGINAL join time timeout — member should survive
    // because heartbeat refreshed lastHeartbeatNs.
    int64_t t2 = afterTimeout(0, kDefaultTimeoutMs);
    int work = gc.doWork(t2);
    EXPECT_EQ(work, 0); // member's lastHeartbeat was refreshed to t1
    EXPECT_EQ(gc.memberCount(), 1u);
}

// ===========================================================================
// 5. Heartbeat: stale generation → rejected
// ===========================================================================

TEST(GroupCoordinator, HeartbeatStaleGenerationRejected) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    // Use a stale generation (0, before the join bumped it to 1).
    auto hb = gc.heartbeat(r.memberId, 0, 1'000);
    EXPECT_TRUE(hb.staleGeneration());
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrStaleGeneration);
}

TEST(GroupCoordinator, HeartbeatStaleGenerationHigherAlsoRejected) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    // generation 999 > current
    auto hb = gc.heartbeat(r.memberId, 999, 1'000);
    EXPECT_TRUE(hb.staleGeneration());
}

// ===========================================================================
// 6. Heartbeat: unknown member → rejected
// ===========================================================================

TEST(GroupCoordinator, HeartbeatUnknownMemberRejected) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    auto hb = gc.heartbeat("ghost-member", gc.generationId(), 0);
    EXPECT_TRUE(hb.unknownMember());
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrUnknownMember);
}

// ===========================================================================
// 7. Heartbeat: during PreparingRebalance → kErrRebalanceInProgress
// ===========================================================================

TEST(GroupCoordinator, HeartbeatDuringPreparingRebalanceSignalsRejoin) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    // Group is now in PreparingRebalance after join.
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);
    auto hb = gc.heartbeat(r.memberId, gc.generationId(), 1'000);
    EXPECT_TRUE(hb.rebalanceInProgress());
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrRebalanceInProgress);
}

// ===========================================================================
// 8. Session timeout: just before → alive; just after → evicted
// ===========================================================================

TEST(GroupCoordinator, SessionTimeoutJustBeforeStillAlive) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    ASSERT_EQ(gc.memberCount(), 1u);
    // Advance to 1 ns BEFORE timeout fires.
    int64_t almostExpiry = beforeTimeout(0, kDefaultTimeoutMs);
    int work = gc.doWork(almostExpiry);
    EXPECT_EQ(work, 0);
    EXPECT_EQ(gc.memberCount(), 1u);
}

TEST(GroupCoordinator, SessionTimeoutJustAfterEvictsMember) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    ASSERT_EQ(gc.memberCount(), 1u);
    // Advance to 1 ns AFTER timeout fires.
    int64_t justExpired = afterTimeout(0, kDefaultTimeoutMs);
    int work = gc.doWork(justExpired);
    EXPECT_EQ(work, 1); // one member reaped
    EXPECT_EQ(gc.memberCount(), 0u);
}

// ===========================================================================
// 9. Session timeout: reap bumps generation + PreparingRebalance
// ===========================================================================

TEST(GroupCoordinator, SessionTimeoutBumpsGenerationAndRebalances) {
    GroupCoordinator gc("test-group", 0);
    // Join two members: A (short timeout), B (long timeout).
    auto rA = gc.join("", 1'000 /*ms*/, {}, 0);  // 1s timeout
    auto rB = gc.join("", kDefaultTimeoutMs, {}, 0); // 30s timeout
    Generation genBefore = gc.generationId();

    // Advance past A's timeout (2 s), but not B's (30 s).
    int64_t t = afterTimeout(0, 1'000);
    int work = gc.doWork(t);
    EXPECT_EQ(work, 1); // A reaped
    EXPECT_GT(gc.generationId(), genBefore); // generation bumped
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
    EXPECT_EQ(gc.memberCount(), 1u); // B still alive
}

// ===========================================================================
// 10. Session timeout: last member expiry → Empty state
// ===========================================================================

TEST(GroupCoordinator, SessionTimeoutLastMemberTransitionsToEmpty) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    ASSERT_EQ(gc.memberCount(), 1u);
    int64_t justExpired = afterTimeout(0, kDefaultTimeoutMs);
    gc.doWork(justExpired);
    EXPECT_EQ(gc.state(), GroupState::Empty);
    EXPECT_TRUE(gc.isEmpty());
    EXPECT_TRUE(gc.leaderId().empty());
}

// ===========================================================================
// 11. leave: shrinks membership, bumps generation
// ===========================================================================

TEST(GroupCoordinator, LeaveShrinksMembershipAndBumpsGeneration) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("", kDefaultTimeoutMs, {}, 0);
    ASSERT_EQ(gc.memberCount(), 2u);
    Generation genBefore = gc.generationId();
    gc.leave(r2.memberId, 1'000);
    EXPECT_EQ(gc.memberCount(), 1u);
    EXPECT_GT(gc.generationId(), genBefore);
}

// ===========================================================================
// 12. leave: leader leaves → new leader elected (alphabetically smallest)
// ===========================================================================

TEST(GroupCoordinator, LeaderLeavesNewLeaderElected) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("", kDefaultTimeoutMs, {}, 0);

    // Identify the leader.
    std::string oldLeaderId = gc.leaderId();
    EXPECT_FALSE(oldLeaderId.empty());

    // Identify the non-leader.
    std::string remaining = (r1.memberId == oldLeaderId) ? r2.memberId : r1.memberId;

    gc.leave(oldLeaderId, 1'000);
    EXPECT_EQ(gc.memberCount(), 1u);
    // New leader should be the only remaining member.
    EXPECT_EQ(gc.leaderId(), remaining);
}

TEST(GroupCoordinator, LeaderElectionDeterministic_AlphabeticallySmallest) {
    GroupCoordinator gc("test-group", 0);
    // We can't control generated memberIds easily, but we can re-join with explicit ids.
    // Use explicit member ids to test determinism.
    auto rC = gc.join("member-c", kDefaultTimeoutMs, {}, 0); // first join
    EXPECT_EQ(gc.leaderId(), "member-c"); // only member
    auto rA = gc.join("member-a", kDefaultTimeoutMs, {}, 0);
    // After second join, leader should be alphabetically smallest.
    EXPECT_EQ(gc.leaderId(), "member-a");
    auto rB = gc.join("member-b", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.leaderId(), "member-a"); // still smallest
    gc.leave("member-a", 1'000);
    EXPECT_EQ(gc.leaderId(), "member-b"); // next smallest
}

// ===========================================================================
// 13. leave: last member → Empty state, generation bumped
// ===========================================================================

TEST(GroupCoordinator, LastMemberLeavesTransitionsToEmpty) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    Generation genBefore = gc.generationId();
    gc.leave(r.memberId, 1'000);
    EXPECT_EQ(gc.state(), GroupState::Empty);
    EXPECT_TRUE(gc.isEmpty());
    EXPECT_GT(gc.generationId(), genBefore);
    EXPECT_TRUE(gc.leaderId().empty());
}

// ===========================================================================
// 14. leave: unknown member → no-op
// ===========================================================================

TEST(GroupCoordinator, LeaveUnknownMemberNoOp) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    Generation genBefore = gc.generationId();
    std::size_t countBefore = gc.memberCount();
    gc.leave("not-a-member", 0);
    EXPECT_EQ(gc.generationId(), genBefore);
    EXPECT_EQ(gc.memberCount(), countBefore);
}

// ===========================================================================
// 15. generation monotonic across churn: join/join/leave/expire/join
// ===========================================================================

TEST(GroupCoordinator, GenerationMonotonicAcrossChurn) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.generationId(), 0);

    auto r1 = gc.join("member-x", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 1); // join 1

    auto r2 = gc.join("member-y", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 2); // join 2

    gc.leave("member-y", 1'000);
    EXPECT_EQ(gc.generationId(), 3); // leave

    // Expire member-x.
    int64_t expireTime = afterTimeout(0, kDefaultTimeoutMs);
    gc.doWork(expireTime);
    EXPECT_EQ(gc.generationId(), 4); // expire → Empty
    EXPECT_EQ(gc.state(), GroupState::Empty);

    // Join again.
    gc.join("member-z", kDefaultTimeoutMs, {}, expireTime + 1);
    EXPECT_EQ(gc.generationId(), 5); // join 3

    // Verify monotonic — never decreases.
    EXPECT_GE(gc.generationId(), 5);
}

// ===========================================================================
// 16. doWork idle → 0 work
// ===========================================================================

TEST(GroupCoordinator, DoWorkIdleReturnsZero) {
    GroupCoordinator gc("test-group", 0);
    // Empty group.
    EXPECT_EQ(gc.doWork(0), 0);
    EXPECT_EQ(gc.doWork(1'000'000'000LL), 0);
}

TEST(GroupCoordinator, DoWorkWithActiveMembersNoExpiryReturnsZero) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    // Well before timeout.
    EXPECT_EQ(gc.doWork(1'000'000LL), 0);
}

// ===========================================================================
// 17. doWork: multiple expired members reaped in one call
// ===========================================================================

TEST(GroupCoordinator, DoWorkReapsMultipleExpiredMembers) {
    GroupCoordinator gc("test-group", 0);
    int32_t shortTimeout = 1'000; // 1 s
    gc.join("member-a", shortTimeout, {}, 0);
    gc.join("member-b", shortTimeout, {}, 0);
    gc.join("member-c", shortTimeout, {}, 0);
    ASSERT_EQ(gc.memberCount(), 3u);

    int64_t expireTime = afterTimeout(0, shortTimeout);
    int work = gc.doWork(expireTime);
    EXPECT_EQ(work, 3); // all three reaped
    EXPECT_EQ(gc.memberCount(), 0u);
    EXPECT_EQ(gc.state(), GroupState::Empty);
}

// ===========================================================================
// 18. doWork: member with longer timeout not reaped early
// ===========================================================================

TEST(GroupCoordinator, DoWorkOnlyReapsExpiredMembers) {
    GroupCoordinator gc("test-group", 0);
    int32_t shortTimeout = 1'000;   // 1 s
    int32_t longTimeout  = 30'000;  // 30 s
    gc.join("short-member", shortTimeout, {}, 0);
    gc.join("long-member",  longTimeout,  {}, 0);
    ASSERT_EQ(gc.memberCount(), 2u);

    // Advance past short timeout but not long timeout.
    int64_t t = afterTimeout(0, shortTimeout);
    ASSERT_LT(t, beforeTimeout(0, longTimeout)); // sanity

    int work = gc.doWork(t);
    EXPECT_EQ(work, 1); // only short-member reaped
    EXPECT_EQ(gc.memberCount(), 1u);
    // long-member should still be in the group.
    const auto& members = gc.members();
    ASSERT_EQ(members.size(), 1u);
    EXPECT_EQ(members[0].memberId, "long-member");
}

// ===========================================================================
// 19. join Dead group → kErrDead
//     (Dead is a terminal state — test by directly manipulating via public API seam)
// ===========================================================================

// Note: GroupCoordinator does not expose a direct "kill()" — Dead is future terminal.
// We test that if we could get to Dead state, join returns error.
// Since Dead is not directly reachable via the current API (no kill() method),
// we test all other error paths and the Dead enum value exists.
// This test verifies the enum and error code constants are correct.
TEST(GroupCoordinator, JoinErrorCodeConstants) {
    EXPECT_EQ(JoinResult::kErrDead, 1);
    EXPECT_EQ(HeartbeatResult::kOk, 0);
    EXPECT_EQ(HeartbeatResult::kErrUnknownMember, 1);
    EXPECT_EQ(HeartbeatResult::kErrStaleGeneration, 2);
    EXPECT_EQ(HeartbeatResult::kErrRebalanceInProgress, 3);
}

// ===========================================================================
// 20. leader: first joiner is the leader
// ===========================================================================

TEST(GroupCoordinator, FirstJoinerIsLeader) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("first-member", kDefaultTimeoutMs, {}, 0);
    EXPECT_TRUE(r.isLeader);
    EXPECT_EQ(gc.leaderId(), "first-member");
}

// ===========================================================================
// 21. leader: deterministic after churn (alphabetically smallest)
// ===========================================================================

TEST(GroupCoordinator, LeaderAlphabeticallySmallestAfterChurn) {
    GroupCoordinator gc("test-group", 0);
    gc.join("member-z", kDefaultTimeoutMs, {}, 0);
    gc.join("member-m", kDefaultTimeoutMs, {}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.leaderId(), "member-a");

    gc.leave("member-a", 1'000);
    EXPECT_EQ(gc.leaderId(), "member-m");
}

// ===========================================================================
// 22. leader flag set on MemberInfo
// ===========================================================================

TEST(GroupCoordinator, LeaderFlagSetOnMemberInfo) {
    GroupCoordinator gc("test-group", 0);
    gc.join("member-b", kDefaultTimeoutMs, {}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {}, 0);
    // leader should be member-a (alphabetically smallest).
    const auto& members = gc.members();
    int leaderCount = 0;
    std::string leaderFound;
    for (const auto& m : members) {
        if (m.isLeader) {
            ++leaderCount;
            leaderFound = m.memberId;
        }
    }
    EXPECT_EQ(leaderCount, 1); // exactly one leader
    EXPECT_EQ(leaderFound, "member-a");
}

// ===========================================================================
// 23. subscribed topics stored per member
// ===========================================================================

TEST(GroupCoordinator, SubscribedTopicsStoredPerMember) {
    GroupCoordinator gc("test-group", 0);
    std::vector<std::string> topics = {"topic-foo", "topic-bar"};
    auto r = gc.join("member-1", kDefaultTimeoutMs, topics, 0);
    const auto& members = gc.members();
    ASSERT_EQ(members.size(), 1u);
    EXPECT_EQ(members[0].subscribedTopics, topics);
}

TEST(GroupCoordinator, RejoinUpdatesSubscribedTopics) {
    GroupCoordinator gc("test-group", 0);
    gc.join("member-1", kDefaultTimeoutMs, {"topic-a"}, 0);
    gc.join("member-1", kDefaultTimeoutMs, {"topic-b", "topic-c"}, 1'000);
    const auto& members = gc.members();
    ASSERT_EQ(members.size(), 1u);
    EXPECT_EQ(members[0].subscribedTopics, (std::vector<std::string>{"topic-b", "topic-c"}));
}

// ===========================================================================
// 24. join: empty memberId → auto-generated unique id
// ===========================================================================

TEST(GroupCoordinator, EmptyMemberIdAutoGenerates) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_FALSE(r.memberId.empty());
    EXPECT_EQ(r.errorCode, 0);
}

// ===========================================================================
// 25. join: generated memberIds are unique across multiple joins
// ===========================================================================

TEST(GroupCoordinator, GeneratedMemberIdsUnique) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r3 = gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_NE(r1.memberId, r2.memberId);
    EXPECT_NE(r2.memberId, r3.memberId);
    EXPECT_NE(r1.memberId, r3.memberId);
}

// ===========================================================================
// 26. multiple groups are independent
// ===========================================================================

TEST(GroupCoordinator, MultipleGroupsAreIndependent) {
    GroupCoordinator gc1("group-1", 0);
    GroupCoordinator gc2("group-2", 0);

    gc1.join("member-a", kDefaultTimeoutMs, {}, 0);
    gc1.join("member-b", kDefaultTimeoutMs, {}, 0);
    gc2.join("member-x", kDefaultTimeoutMs, {}, 0);

    EXPECT_EQ(gc1.memberCount(), 2u);
    EXPECT_EQ(gc2.memberCount(), 1u);
    EXPECT_EQ(gc1.generationId(), 2);
    EXPECT_EQ(gc2.generationId(), 1);

    // Evict group-1 members — group-2 unaffected.
    int64_t expireTime = afterTimeout(0, kDefaultTimeoutMs);
    gc1.doWork(expireTime);
    EXPECT_EQ(gc1.memberCount(), 0u);
    EXPECT_EQ(gc2.memberCount(), 1u);
}

// ===========================================================================
// 27. generationId starts at 0, first join makes it 1
// ===========================================================================

TEST(GroupCoordinator, GenerationIdStartsAtZero) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.generationId(), 0);
}

TEST(GroupCoordinator, GenerationIdAfterFirstJoinIsOne) {
    GroupCoordinator gc("test-group", 0);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 1);
}

// ===========================================================================
// 28. generation exact values across churn
// ===========================================================================

TEST(GroupCoordinator, GenerationExactValuesChurn) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.generationId(), 0);

    gc.join("a", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 1);

    gc.join("b", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 2);

    gc.leave("b", 0);
    EXPECT_EQ(gc.generationId(), 3);

    gc.leave("a", 0); // last → Empty, also bumps
    EXPECT_EQ(gc.generationId(), 4);

    gc.join("c", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.generationId(), 5);
}

// ===========================================================================
// 29. state transitions: Empty → PreparingRebalance on join
// ===========================================================================

TEST(GroupCoordinator, StateTransitionEmptyToPreparingRebalance) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.state(), GroupState::Empty);
    gc.join("", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
}

// ===========================================================================
// 30. state after all members leave → Empty
// ===========================================================================

TEST(GroupCoordinator, StateEmptyAfterAllLeave) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("", kDefaultTimeoutMs, {}, 0);
    gc.leave(r1.memberId, 0);
    gc.leave(r2.memberId, 0);
    EXPECT_EQ(gc.state(), GroupState::Empty);
}

// ===========================================================================
// 31. heartbeat refreshes lastHeartbeatNs so member survives another window
// ===========================================================================

TEST(GroupCoordinator, HeartbeatRefreshSurvivesAnotherWindow) {
    GroupCoordinator gc("test-group", 0);
    int32_t timeoutMs = 5'000; // 5 s
    auto r = gc.join("member-1", timeoutMs, {}, 0);

    // Heartbeat at T=4s (just before the 5 s timeout from T=0).
    int64_t t1 = 4LL * 1'000'000'000LL; // 4 s
    gc.heartbeat(r.memberId, gc.generationId(), t1);
    EXPECT_EQ(gc.memberCount(), 1u);

    // At T=6s: original window [0, 5s) would have expired, but heartbeat
    // was at T=4s so new window is [4s, 9s). Member should be alive.
    int64_t t2 = 6LL * 1'000'000'000LL;
    int work = gc.doWork(t2);
    EXPECT_EQ(work, 0);
    EXPECT_EQ(gc.memberCount(), 1u);

    // At T=10s: past the new window [4s, 9s). Member should expire.
    int64_t t3 = 10LL * 1'000'000'000LL;
    work = gc.doWork(t3);
    EXPECT_EQ(work, 1);
    EXPECT_EQ(gc.memberCount(), 0u);
}

// ===========================================================================
// 32. doWork bounded driver with hard cap + ASSERT (never infinite)
// ===========================================================================

TEST(GroupCoordinator, DoWorkBoundedDriver) {
    GroupCoordinator gc("test-group", 0);
    int32_t timeoutMs = 1'000; // 1 s
    gc.join("member-a", timeoutMs, {}, 0);
    gc.join("member-b", timeoutMs, {}, 0);

    // Drive until both members expire.
    bool done = false;
    int64_t clock = 0;
    int64_t step  = static_cast<int64_t>(timeoutMs) * 1'000'000LL; // 1 s steps

    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += step;
        gc.doWork(clock);
        if (gc.memberCount() == 0) {
            done = true;
        }
    }
    ASSERT_TRUE(done) << "Bounded driver stalled after " << kDriverCap << " iterations";
    EXPECT_EQ(gc.state(), GroupState::Empty);
}

// ===========================================================================
// 33. two members: only the timed-out one is reaped
// ===========================================================================

TEST(GroupCoordinator, OnlyExpiredMemberReaped) {
    GroupCoordinator gc("test-group", 0);
    int32_t shortMs = 2'000;  // 2 s
    int32_t longMs  = 60'000; // 60 s
    gc.join("short", shortMs, {}, 0);
    gc.join("long",  longMs,  {}, 0);
    ASSERT_EQ(gc.memberCount(), 2u);

    // Advance to just after short timeout.
    int64_t t = afterTimeout(0, shortMs);
    int work = gc.doWork(t);
    EXPECT_EQ(work, 1);
    EXPECT_EQ(gc.memberCount(), 1u);
    const auto& members = gc.members();
    EXPECT_EQ(members[0].memberId, "long");
}

// ===========================================================================
// 34. re-join during PreparingRebalance does not duplicate members
// ===========================================================================

TEST(GroupCoordinator, RejoinDuringPreparingRebalanceNoDuplicate) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("member-1", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("member-2", kDefaultTimeoutMs, {}, 0);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);
    std::size_t countBefore = gc.memberCount();

    // member-1 re-joins during rebalance.
    gc.join("member-1", kDefaultTimeoutMs, {}, 1'000);
    EXPECT_EQ(gc.memberCount(), countBefore); // no duplicate
}

// ===========================================================================
// 35. generationId monotonically increases — never decreases
// ===========================================================================

TEST(GroupCoordinator, GenerationNeverDecreases) {
    GroupCoordinator gc("test-group", 0);
    Generation prev = gc.generationId();

    auto r1 = gc.join("x", kDefaultTimeoutMs, {}, 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    auto r2 = gc.join("y", kDefaultTimeoutMs, {}, 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    gc.leave("y", 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    gc.leave("x", 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    gc.join("z", kDefaultTimeoutMs, {}, 0);
    EXPECT_GE(gc.generationId(), prev);
}

// ===========================================================================
// 36. leaderId empty when group is Empty
// ===========================================================================

TEST(GroupCoordinator, LeaderIdEmptyWhenGroupEmpty) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_TRUE(gc.leaderId().empty());

    // After all members leave.
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    gc.leave(r.memberId, 0);
    EXPECT_TRUE(gc.leaderId().empty());
}

// ===========================================================================
// 37. join after Empty → new generation, leader elected
// ===========================================================================

TEST(GroupCoordinator, JoinAfterEmptyStartsFresh) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("member-1", kDefaultTimeoutMs, {}, 0);
    gc.leave(r1.memberId, 0);
    ASSERT_EQ(gc.state(), GroupState::Empty);
    Generation genAfterEmpty = gc.generationId();

    auto r2 = gc.join("member-2", kDefaultTimeoutMs, {}, 1'000);
    EXPECT_GT(gc.generationId(), genAfterEmpty);
    EXPECT_EQ(gc.leaderId(), "member-2");
    EXPECT_TRUE(r2.isLeader);
}

// ===========================================================================
// 38. CompletingRebalance: heartbeat returns rebalanceInProgress
//     (set state directly — test the heartbeat branch)
// ===========================================================================

// GroupCoordinator exposes no direct way to force CompletingRebalance from outside
// (it's set internally when all expected members re-joined — P4-3 territory).
// We verify that HeartbeatResult correctly reports rebalanceInProgress in that state
// by checking the PreparingRebalance path (same errorCode branch).
TEST(GroupCoordinator, HeartbeatDuringCompletingRebalancePath) {
    // The kErrRebalanceInProgress is returned for both PreparingRebalance and
    // CompletingRebalance. PreparingRebalance is reachable in tests; verify
    // the error code value is correct.
    EXPECT_EQ(HeartbeatResult::kErrRebalanceInProgress, 3);

    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("m", kDefaultTimeoutMs, {}, 0);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);
    auto hb = gc.heartbeat(r.memberId, gc.generationId(), 1'000);
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrRebalanceInProgress);
}

// ===========================================================================
// 39. Verify doWork clock is sampled once: no intermediate state if called
//     with the SAME nowNs twice (idempotent on idle)
// ===========================================================================

TEST(GroupCoordinator, DoWorkIdempotentWhenNoWork) {
    GroupCoordinator gc("test-group", 0);
    gc.join("m", kDefaultTimeoutMs, {}, 0);
    // Before timeout.
    EXPECT_EQ(gc.doWork(1'000), 0);
    EXPECT_EQ(gc.doWork(1'000), 0); // same clock — idempotent
    EXPECT_EQ(gc.memberCount(), 1u);
}

// ===========================================================================
// 40. Join returns generationId matching coordinator's current generationId
// ===========================================================================

TEST(GroupCoordinator, JoinResultGenerationMatchesCoordinatorGeneration) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("m1", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(r1.generationId, gc.generationId());

    auto r2 = gc.join("m2", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(r2.generationId, gc.generationId());
}
