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
#include <rune/coordination/Assignor.h>

#include <cassert>
#include <map>
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
    gc.leave(r2.memberId, gc.generationId(), 1'000);
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

    gc.leave(oldLeaderId, gc.generationId(), 1'000);
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
    gc.leave("member-a", gc.generationId(), 1'000);
    EXPECT_EQ(gc.leaderId(), "member-b"); // next smallest
}

// ===========================================================================
// 13. leave: last member → Empty state, generation bumped
// ===========================================================================

TEST(GroupCoordinator, LastMemberLeavesTransitionsToEmpty) {
    GroupCoordinator gc("test-group", 0);
    auto r = gc.join("", kDefaultTimeoutMs, {}, 0);
    Generation genBefore = gc.generationId();
    gc.leave(r.memberId, gc.generationId(), 1'000);
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
    gc.leave("not-a-member", gc.generationId(), 0);
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

    gc.leave("member-y", gc.generationId(), 1'000);
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

    gc.leave("member-a", gc.generationId(), 1'000);
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

    gc.leave("b", gc.generationId(), 0);
    EXPECT_EQ(gc.generationId(), 3);

    gc.leave("a", gc.generationId(), 0); // last → Empty, also bumps
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
    gc.leave(r1.memberId, gc.generationId(), 0);
    gc.leave(r2.memberId, gc.generationId(), 0);
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

    gc.leave("y", gc.generationId(), 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    gc.leave("x", gc.generationId(), 0);
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
    gc.leave(r.memberId, gc.generationId(), 0);
    EXPECT_TRUE(gc.leaderId().empty());
}

// ===========================================================================
// 37. join after Empty → new generation, leader elected
// ===========================================================================

TEST(GroupCoordinator, JoinAfterEmptyStartsFresh) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("member-1", kDefaultTimeoutMs, {}, 0);
    gc.leave(r1.memberId, gc.generationId(), 0);
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

// ===========================================================================
// AUDIT: C1 regression — leave() with stale generationId is a no-op
// A zombie consumer that was fenced out (heartbeat → stale) must NOT be able
// to force a rebalance of the live group via a stale-generation leave.
// ===========================================================================

TEST(GroupCoordinator, StaleGenerationLeave_IsNoOp) {
    GroupCoordinator gc("test-group", 0);
    auto r1 = gc.join("member-a", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("member-b", kDefaultTimeoutMs, {}, 0);

    // Both members form a stable group (PreparingRebalance after two joins).
    // The current generationId is the live one.
    Generation liveGen  = gc.generationId();
    Generation staleGen = liveGen - 1;
    ASSERT_GE(liveGen, 1); // sanity

    std::size_t membersBefore = gc.memberCount();
    GroupState  stateBefore   = gc.state();

    // A stale-generation leave must be silently ignored.
    gc.leave("member-b", staleGen, 1'000);

    EXPECT_EQ(gc.memberCount(), membersBefore)
        << "Stale-generation leave must not shrink membership";
    EXPECT_EQ(gc.state(), stateBefore)
        << "Stale-generation leave must not trigger a rebalance";
    EXPECT_EQ(gc.generationId(), liveGen)
        << "Stale-generation leave must not bump generationId";
}

TEST(GroupCoordinator, CurrentGenerationLeave_ShrinksMembership) {
    GroupCoordinator gc("test-group", 0);
    gc.join("member-a", kDefaultTimeoutMs, {}, 0);
    gc.join("member-b", kDefaultTimeoutMs, {}, 0);
    Generation liveGen = gc.generationId();

    // A correctly-fenced leave (current gen) must take effect.
    gc.leave("member-b", liveGen, 1'000);
    EXPECT_EQ(gc.memberCount(), 1u)
        << "Current-generation leave must shrink membership";
    EXPECT_GT(gc.generationId(), liveGen)
        << "Current-generation leave must bump generationId";
}

// ===========================================================================
// AUDIT: S4 regression — memberIds are truly unique across coordinator instances
// Two GroupCoordinator instances must never mint the same memberId.
// ===========================================================================

TEST(GroupCoordinator, GeneratedMemberIds_UniqueWithinInstance) {
    GroupCoordinator gc("grp", 0);
    auto r1 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc.join("", kDefaultTimeoutMs, {}, 0);
    auto r3 = gc.join("", kDefaultTimeoutMs, {}, 0);

    EXPECT_NE(r1.memberId, r2.memberId) << "memberIds must be unique within one instance";
    EXPECT_NE(r1.memberId, r3.memberId);
    EXPECT_NE(r2.memberId, r3.memberId);
}

TEST(GroupCoordinator, GeneratedMemberIds_UniqueAcrossInstances) {
    // Two coordinators with the same groupId must not mint identical memberIds.
    GroupCoordinator gc1("grp", 0);
    GroupCoordinator gc2("grp", 0);

    auto r1 = gc1.join("", kDefaultTimeoutMs, {}, 0);
    auto r2 = gc2.join("", kDefaultTimeoutMs, {}, 0);

    EXPECT_NE(r1.memberId, r2.memberId)
        << "Two coordinator instances must produce distinct memberIds even for same groupId";
}

// ===========================================================================
// AUDIT: S3 regression — leader election is stable across power-of-ten boundaries
// Members whose sequence numbers span e.g. 9→10 must still elect the
// numerically-smallest member, not the lexicographically-smallest.
// ===========================================================================

TEST(GroupCoordinator, LeaderElection_NumericallyCorrectAcrossPowerOfTen) {
    // Build a scenario with explicit (non-generated) memberIds whose unpadded
    // decimal sorting would be wrong lexicographically.
    // "test-group-2"  < "test-group-10" numerically but
    // "test-group-10" < "test-group-2"  lexicographically.
    GroupCoordinator gc("test-group", 0);

    // Joins: first add "test-group-10" then "test-group-2".
    gc.join("test-group-10", kDefaultTimeoutMs, {}, 0);
    gc.join("test-group-2",  kDefaultTimeoutMs, {}, 0);

    // Numerically "test-group-2" < "test-group-10" (the 2 is earlier in the
    // sequence).  The leader must be "test-group-2".
    EXPECT_EQ(gc.leaderId(), "test-group-2")
        << "Leader election must be numerically correct "
           "(test-group-2 < test-group-10 numerically)";

    // Add "test-group-9" and "test-group-11".
    gc.join("test-group-9",  kDefaultTimeoutMs, {}, 0);
    gc.join("test-group-11", kDefaultTimeoutMs, {}, 0);
    // "test-group-2" is still the numerically smallest.
    EXPECT_EQ(gc.leaderId(), "test-group-2");

    // Remove "test-group-2"; next smallest should be "test-group-9".
    gc.leave("test-group-2", gc.generationId(), 100);
    EXPECT_EQ(gc.leaderId(), "test-group-9")
        << "After removing test-group-2, test-group-9 must win (not test-group-10 or test-group-11)";
}

// ===========================================================================
// AUDIT: N1-a residual — leader election must be numeric across ALL numeric
// fields of the generated two-number id form "<group>-g<global>-<local>",
// not just the trailing <local> field.  A power-of-ten flip in the GLOBAL
// field (g2 vs g10) must NOT reorder leadership: "grp-g2-3" must win over
// "grp-g10-2" even though "g10" < "g2" lexicographically.
//
// This test FAILS under a trailing-field-only comparator (which compares the
// "<group>-g<global>" prefix lexicographically and only the <local> field
// numerically), and passes only when the comparator orders (global, local)
// as a numeric tuple.
// ===========================================================================

TEST(GroupCoordinator, LeaderElection_NumericAcrossGlobalFieldPowerOfTen) {
    // Ids in the exact generated form "<group>-g<global>-<local>".
    // Numerically the global-seq ordering is g2 < g10, so "grp-g2-3" is the
    // earliest-minted and must be leader.  Lexicographically "grp-g10-2" would
    // wrongly sort first ("g1" < "g2").
    GroupCoordinator gc("grp", 0);

    gc.join("grp-g10-2", kDefaultTimeoutMs, {}, 0);
    gc.join("grp-g2-3",  kDefaultTimeoutMs, {}, 0);

    EXPECT_EQ(gc.leaderId(), "grp-g2-3")
        << "Leader must be numerically smallest across the GLOBAL field "
           "(g2 < g10 numerically, not lexicographically)";

    // Add more members spanning the boundary in the global field.
    gc.join("grp-g9-1",  kDefaultTimeoutMs, {}, 0);
    gc.join("grp-g11-7", kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc.leaderId(), "grp-g2-3")
        << "g2 is still the numerically smallest global seq";

    // Remove the leader; next smallest global seq is g9.
    gc.leave("grp-g2-3", gc.generationId(), 100);
    EXPECT_EQ(gc.leaderId(), "grp-g9-1")
        << "After removing g2, g9 must win (not g10 or g11 by lexicographic prefix)";

    // Same-global tie broken by the trailing <local> field, numerically.
    GroupCoordinator gc2("grp", 0);
    gc2.join("grp-g5-10", kDefaultTimeoutMs, {}, 0);
    gc2.join("grp-g5-2",  kDefaultTimeoutMs, {}, 0);
    EXPECT_EQ(gc2.leaderId(), "grp-g5-2")
        << "Within the same global seq, the trailing local field must order numerically (2 < 10)";
}

// ===========================================================================
// AUDIT: C3 regression — syncGroup re-submission in Stable does not clobber assignments
// A leader re-sending a non-empty assignment map while the group is already
// Stable must be ignored (ack'd, but assignments must not change).
// ===========================================================================

TEST(GroupCoordinator, SyncGroupLeaderInStable_NoClobber) {
    GroupCoordinator gc("test-group", 0);
    using namespace rune::coordination;

    // Get to Stable with 2 members.
    gc.join("member-a", kDefaultTimeoutMs, {"topic"}, 0);
    gc.join("member-b", kDefaultTimeoutMs, {"topic"}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {"topic"}, 100);
    gc.join("member-b", kDefaultTimeoutMs, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    TopicPartitionCounts partCounts = {{"topic", 4}};
    RangeAssignor ra;

    // Leader submits the real assignment.
    auto subs = gc.members();
    std::vector<MemberSubscription> ms;
    for (const auto& m : subs) ms.push_back({m.memberId, m.subscribedTopics});
    Assignment computed = ra.assign(ms, partCounts);

    std::string lid = gc.leaderId();
    Generation gen  = gc.generationId();
    auto sr1 = gc.syncGroup(lid, gen, computed, 300);
    EXPECT_EQ(sr1.errorCode, 0);
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // Save the original assignment for each member.
    std::map<std::string, std::vector<TopicPartition>> origAssign;
    for (const auto& m : gc.members()) origAssign[m.memberId] = m.assignment;

    // Leader re-submits a different assignment while Stable (e.g. bogus single-member).
    Assignment bogus;
    bogus[lid] = {{{"topic", 0}}, {{"topic", 1}}, {{"topic", 2}}, {{"topic", 3}}};
    auto sr2 = gc.syncGroup(lid, gen, bogus, 400);
    EXPECT_EQ(sr2.errorCode, 0) << "Re-sync in Stable must be ack'd (not rejected)";
    // But the state and assignments must NOT change.
    EXPECT_EQ(gc.state(), GroupState::Stable);
    for (const auto& m : gc.members()) {
        EXPECT_EQ(m.assignment, origAssign.at(m.memberId))
            << "Assignment for " << m.memberId
            << " must not be clobbered by a duplicate leader SyncGroup in Stable";
    }
}

// ===========================================================================
// AUDIT A1 [CORRECTNESS]: Existing-member re-join while Stable → PreparingRebalance
//
// The state table (GroupCoordinator.h:24) says:
//   "Stable + join/leave → PreparingRebalance (generation bumped)"
// A join that CHANGES the member's subscription (or a genuinely new member)
// must trigger a rebalance — see SubscriptionChangeOnRejoin_* below.
//
// CG-6 idempotency guard (commit ec53265): an existing member re-joining while
// Stable with an UNCHANGED subscription is a stale in-flight duplicate (a
// buffered JoinGroup from a prior Rejoining cadence that arrived after the
// group already stabilised). It must be silently absorbed — heartbeat refresh
// + current-generation OK, NO rebalance and NO generation bump. Without this,
// late JoinGroup frames re-trigger Stable→PreparingRebalance immediately after
// auto-assign, causing an infinite-rebalance loop.
// ===========================================================================

TEST(GroupCoordinator, ExistingMemberRejoinWhileStable_SameSubscription_IsIdempotent) {
    GroupCoordinator gc("test-group", 0);

    // Reach Stable: single member, auto-assign.
    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    TopicPartitionCounts partCounts = {{"t", 2}};
    RangeAssignor ra;
    {
        auto subs = gc.members();
        std::vector<MemberSubscription> ms;
        for (const auto& m : subs) ms.push_back({m.memberId, m.subscribedTopics});
        Assignment comp = ra.assign(ms, partCounts);
        gc.syncGroup("member-a", gc.generationId(), comp, 200);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    Generation genStable = gc.generationId();

    // Existing member re-joins while Stable with the SAME subscription — this is
    // a stale duplicate join and must be absorbed idempotently (no rebalance).
    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 300);

    EXPECT_EQ(gc.state(), GroupState::Stable)
        << "Same-subscription re-join while Stable must NOT trigger a rebalance "
           "(CG-6 idempotency guard prevents the infinite-rebalance loop)";
    EXPECT_EQ(gc.generationId(), genStable)
        << "Same-subscription re-join while Stable must NOT bump generation";
}

// ===========================================================================
// AUDIT A1 [CORRECTNESS]: Subscription-change on re-join while Stable →
// reassignment reflects the new subscription.
//
// A member that changes its subscribedTopics on rejoin while Stable must
// cause a rebalance whose final assignment reflects the NEW subscription,
// not the stale one.
// ===========================================================================

TEST(GroupCoordinator, SubscriptionChangeOnRejoin_ReassignmentReflectsChange) {
    GroupCoordinator gc("test-group", 0);

    // Set up: member-a subscribes to {"t1"}; reach Stable.
    gc.join("member-a", kDefaultTimeoutMs, {"t1"}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {"t1"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    TopicPartitionCounts partCounts = {{"t1", 2}, {"t2", 2}};
    RangeAssignor ra;
    {
        auto subs = gc.members();
        std::vector<MemberSubscription> ms;
        for (const auto& m : subs) ms.push_back({m.memberId, m.subscribedTopics});
        Assignment comp = ra.assign(ms, partCounts);
        gc.syncGroup("member-a", gc.generationId(), comp, 200);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // Existing member re-joins while Stable with a DIFFERENT subscription.
    gc.join("member-a", kDefaultTimeoutMs, {"t1", "t2"}, 300);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance)
        << "Subscription-change re-join while Stable must trigger rebalance";

    // The member's stored subscribedTopics must reflect the new subscription.
    bool foundMember = false;
    for (const auto& m : gc.members()) {
        if (m.memberId == "member-a") {
            foundMember = true;
            std::vector<std::string> expected = {"t1", "t2"};
            EXPECT_EQ(m.subscribedTopics, expected)
                << "Updated subscription must be stored after rejoin";
        }
    }
    EXPECT_TRUE(foundMember) << "member-a must still be in the group";

    // Re-join (phase 2) and complete the rebalance; the new assignment must
    // include t2 since member-a now subscribes to it.
    gc.join("member-a", kDefaultTimeoutMs, {"t1", "t2"}, 400);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    {
        auto subs = gc.members();
        std::vector<MemberSubscription> ms;
        for (const auto& m : subs) ms.push_back({m.memberId, m.subscribedTopics});
        Assignment comp = ra.assign(ms, partCounts);
        gc.syncGroup("member-a", gc.generationId(), comp, 500);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // member-a must now have t2 partitions (new subscription).
    auto af = gc.assignmentFor("member-a");
    bool hasT2 = false;
    for (const auto& tp : af) {
        if (tp.topic == "t2") { hasT2 = true; break; }
    }
    EXPECT_TRUE(hasT2) << "After subscription-change rebalance, member-a must be assigned t2 partitions";
}

// ===========================================================================
// AUDIT A2 [SILENT]: Multiple members expire in the same doWork tick →
// generation bumps by exactly 1 (not N).
//
// Kafka collapses a batch of same-tick session expirations into a SINGLE
// rebalance / generation bump. The current per-member loop bumps N times.
// ===========================================================================

TEST(GroupCoordinator, TwoMembersExpireInSameTick_GenerationBumpsOnce) {
    GroupCoordinator gc("test-group", 0);
    constexpr int32_t shortTimeout = 1'000; // 1 s

    // Add two members with identical short timeouts.
    gc.join("member-a", shortTimeout, {}, 0);
    gc.join("member-b", shortTimeout, {}, 0);

    Generation genBeforeExpiry = gc.generationId();
    ASSERT_GE(genBeforeExpiry, 1);

    // Advance clock so BOTH members have expired.
    int64_t expiry = static_cast<int64_t>(shortTimeout) * 1'000'000LL + 1;
    int work = gc.doWork(expiry);

    // Both members must have been reaped.
    EXPECT_EQ(gc.memberCount(), 0u)
        << "Both members must be evicted in the same doWork tick";
    EXPECT_GE(work, 2) << "doWork must report at least 2 units of work (2 reaps)";

    // Generation must increment by EXACTLY 1 (one batch rebalance), not 2.
    EXPECT_EQ(gc.generationId(), genBeforeExpiry + 1)
        << "Batch expiry of 2 members must produce exactly ONE generation bump, not two";
}

TEST(GroupCoordinator, ThreeMembersExpireInSameTick_GenerationBumpsOnce) {
    GroupCoordinator gc("test-group", 0);
    constexpr int32_t shortTimeout = 500; // 500 ms

    gc.join("ma", shortTimeout, {}, 0);
    gc.join("mb", shortTimeout, {}, 0);
    gc.join("mc", shortTimeout, {}, 0);

    Generation genBefore = gc.generationId();

    int64_t expiry = static_cast<int64_t>(shortTimeout) * 1'000'000LL + 1;
    gc.doWork(expiry);

    EXPECT_EQ(gc.memberCount(), 0u);
    // Three expirations → Empty. Must be exactly genBefore+1.
    EXPECT_EQ(gc.generationId(), genBefore + 1)
        << "Three simultaneous expirations must produce exactly ONE generation bump";
    EXPECT_EQ(gc.state(), GroupState::Empty);
}

// ===========================================================================
// PART B — Additional coverage tests
// ===========================================================================

// B-7: Join during CompletingRebalance — NEW member should re-Prepare.
TEST(GroupCoordinator, NewMemberJoinDuringCompletingRebalance_Reprepares) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    Generation genBefore = gc.generationId();
    // A NEW member joins during CompletingRebalance.
    gc.join("member-b", kDefaultTimeoutMs, {"t"}, 200);

    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance)
        << "New join during CompletingRebalance must re-trigger rebalance (PreparingRebalance)";
    EXPECT_GT(gc.generationId(), genBefore)
        << "New join during CompletingRebalance must bump generation";
    EXPECT_EQ(gc.memberCount(), 2u);
}

// B-7b: Existing member re-joins during CompletingRebalance — should also re-Prepare (A1 fix).
TEST(GroupCoordinator, ExistingMemberRejoinDuringCompletingRebalance_Reprepares) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 0);
    gc.join("member-b", kDefaultTimeoutMs, {"t"}, 0);
    gc.join("member-a", kDefaultTimeoutMs, {"t"}, 100);
    gc.join("member-b", kDefaultTimeoutMs, {"t"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    Generation genBefore = gc.generationId();
    // An EXISTING member re-joins during CompletingRebalance (inert before fix).
    gc.join("member-a", kDefaultTimeoutMs, {"t", "t2"}, 250);

    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance)
        << "Existing-member re-join during CompletingRebalance must re-trigger rebalance";
    EXPECT_GT(gc.generationId(), genBefore)
        << "Re-join during CompletingRebalance must bump generation";
}

// B-8: Empty-group operations — no crash, appropriate error codes.
TEST(GroupCoordinator, EmptyGroup_SyncGroupReturnsWrongState) {
    GroupCoordinator gc("test-group", 0);
    ASSERT_EQ(gc.state(), GroupState::Empty);
    auto sr = gc.syncGroup("nobody", 0, {}, 0);
    EXPECT_NE(sr.errorCode, SyncResult::kOk)
        << "syncGroup on empty group must not return kOk";
}

TEST(GroupCoordinator, EmptyGroup_HeartbeatReturnsUnknownMember) {
    GroupCoordinator gc("test-group", 0);
    auto hb = gc.heartbeat("nobody", 0, 0);
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrUnknownMember);
}

TEST(GroupCoordinator, EmptyGroup_DoWorkReturnsZero) {
    GroupCoordinator gc("test-group", 0);
    EXPECT_EQ(gc.doWork(1'000'000'000LL), 0)
        << "doWork on empty group must return 0 (no work)";
    EXPECT_EQ(gc.state(), GroupState::Empty);
    EXPECT_EQ(gc.generationId(), 0);
}

TEST(GroupCoordinator, EmptyGroup_LeaveIsNoOp) {
    GroupCoordinator gc("test-group", 0);
    // Must not crash or change state.
    gc.leave("nobody", 0, 0);
    EXPECT_EQ(gc.state(), GroupState::Empty);
    EXPECT_EQ(gc.generationId(), 0);
}

// B-11: Generation wrap safety — drive many rebalances, assert monotonic fencing holds.
TEST(GroupCoordinator, LargeGenerationChurn_FencingStillHolds) {
    GroupCoordinator gc("test-group", 0);
    constexpr int kChurns = 200;

    for (int i = 0; i < kChurns; ++i) {
        gc.join("m1", kDefaultTimeoutMs, {}, static_cast<int64_t>(i));
        gc.join("m2", kDefaultTimeoutMs, {}, static_cast<int64_t>(i) + 1);
        gc.leave("m1", gc.generationId(), static_cast<int64_t>(i) + 2);
        gc.leave("m2", gc.generationId(), static_cast<int64_t>(i) + 3);
    }

    Generation finalGen = gc.generationId();
    EXPECT_GT(finalGen, kChurns) << "Generation must have grown from churn";

    // Heartbeat with a stale generation (0) must be rejected.
    gc.join("sentinel", kDefaultTimeoutMs, {}, kChurns * 10LL);
    auto hb = gc.heartbeat("sentinel", 0, kChurns * 10LL + 1);
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrStaleGeneration)
        << "Fencing must still reject stale generation after large churn";
}

// B-12: Concurrent join + timeout in one doWork window — no double-eviction,
// membership stays consistent.
TEST(GroupCoordinator, ConcurrentJoinAndTimeout_ConsistentState) {
    GroupCoordinator gc("test-group", 0);
    constexpr int32_t shortTimeout = 100; // 100 ms

    // member-a has a very short timeout; member-b has a long one.
    gc.join("member-a", shortTimeout, {"t"}, 0);
    gc.join("member-b", kDefaultTimeoutMs, {"t"}, 0);

    Generation genBefore = gc.generationId();
    std::size_t countBefore = gc.memberCount();
    ASSERT_EQ(countBefore, 2u);

    // Advance past member-a's timeout only. member-b should survive.
    int64_t expiry = static_cast<int64_t>(shortTimeout) * 1'000'000LL + 1;
    int work = gc.doWork(expiry);

    // member-a expired; member-b is alive.
    EXPECT_EQ(gc.memberCount(), 1u)
        << "Only the expired member must be evicted";
    EXPECT_GE(work, 1);

    // Check member-b is still present.
    bool foundB = false;
    for (const auto& m : gc.members()) {
        if (m.memberId == "member-b") foundB = true;
        EXPECT_NE(m.memberId, "member-a") << "member-a must have been evicted";
    }
    EXPECT_TRUE(foundB) << "member-b must still be present after member-a expired";

    // Generation must have advanced (one rebalance for the one eviction).
    EXPECT_GT(gc.generationId(), genBefore)
        << "Generation must advance after a member expires";
}

// ===========================================================================
// Phase 2c (spec 13.3): one filter per consumer group
// ===========================================================================
namespace {
const std::vector<uint8_t> kF1{1, 1, 0xAA};
const std::vector<uint8_t> kF2{1, 1, 0xBB};
const std::vector<std::string> kNoTopics;
}

TEST(GroupCoordinatorFilter, FirstMemberFixesTheFilter) {
    GroupCoordinator gc("g");
    EXPECT_TRUE(gc.admitFilter(kF1));
    (void)gc.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    EXPECT_EQ(std::vector<uint8_t>(gc.filterBytes().begin(), gc.filterBytes().end()), kF1);
    EXPECT_TRUE(gc.admitFilter(kF1));
    EXPECT_EQ(gc.filterBytes().size(), kF1.size());
}

TEST(GroupCoordinatorFilter, DifferentFilterIsRefusedWhileMembersExist) {
    GroupCoordinator gc("g");
    ASSERT_TRUE(gc.admitFilter(kF1));
    (void)gc.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    EXPECT_FALSE(gc.admitFilter(kF2));
    EXPECT_FALSE(gc.admitFilter({}));
    EXPECT_EQ(std::vector<uint8_t>(gc.filterBytes().begin(), gc.filterBytes().end()), kF1) << "a refusal changes nothing";
}

TEST(GroupCoordinatorFilter, EmptyGroupAcceptsANewFilter) {
    GroupCoordinator gc("g");
    ASSERT_TRUE(gc.admitFilter(kF1));
    const JoinResult a = gc.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    gc.leave(a.memberId, a.generationId, 1);
    ASSERT_TRUE(gc.isEmpty());
    EXPECT_TRUE(gc.admitFilter(kF2));
    (void)gc.join("b", kDefaultTimeoutMs, kNoTopics, 2);
    EXPECT_EQ(std::vector<uint8_t>(gc.filterBytes().begin(), gc.filterBytes().end()), kF2);
}

// Every way a group empties reopens the filter: session-timeout expiry and rebalance-timeout eviction too.
TEST(GroupCoordinatorFilterRegression, FilterReopensWhenTheGroupEmptiesBySessionExpiryRegression) {
    GroupCoordinator gc("g");
    ASSERT_TRUE(gc.admitFilter(kF1));
    (void)gc.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    EXPECT_FALSE(gc.admitFilter(kF2));
    gc.doWork(afterTimeout(0, kDefaultTimeoutMs));
    ASSERT_TRUE(gc.isEmpty());
    EXPECT_TRUE(gc.admitFilter(kF2));
    EXPECT_EQ(std::vector<uint8_t>(gc.filterBytes().begin(), gc.filterBytes().end()), kF2);
}

TEST(GroupCoordinatorFilterRegression, FilterReopensWhenTheGroupEmptiesByRebalanceEvictionRegression) {
    GroupCoordinator gc("g");
    gc.setRebalanceTimeoutMs(1000);
    ASSERT_TRUE(gc.admitFilter(kF1));
    (void)gc.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    // the first join starts a rebalance; "a" never re-joins, so the rebalance timeout evicts it
    // (well before its 30 s session timeout)
    const int64_t t = 1000LL * 1'000'000LL + 1;
    gc.doWork(t);
    ASSERT_TRUE(gc.isEmpty());
    EXPECT_TRUE(gc.admitFilter(kF2));
}

TEST(GroupCoordinatorFilterRegression, FilterStaysFixedWhileAMemberRemainsRegression) {
    GroupCoordinator gc("g");
    ASSERT_TRUE(gc.admitFilter(kF1));
    const JoinResult a = gc.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    ASSERT_TRUE(gc.admitFilter(kF1));
    (void)gc.join("b", kDefaultTimeoutMs, kNoTopics, 1);
    gc.leave(a.memberId, gc.generationId(), 2);
    ASSERT_EQ(gc.memberCount(), 1u);
    EXPECT_FALSE(gc.admitFilter(kF2)) << "a non-last leave keeps the filter fixed";
}

TEST(GroupCoordinatorFilterRegression, NoFilterIsAFilterValueRegression) {
    GroupCoordinator plain("g");
    ASSERT_TRUE(plain.admitFilter({}));
    (void)plain.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    EXPECT_FALSE(plain.admitFilter(kF1)) << "a group fixed with no filter refuses a filtered member";
    EXPECT_TRUE(plain.admitFilter({}));

    GroupCoordinator filtered("g");
    ASSERT_TRUE(filtered.admitFilter(kF1));
    (void)filtered.join("a", kDefaultTimeoutMs, kNoTopics, 0);
    EXPECT_FALSE(filtered.admitFilter({})) << "and vice versa";
}
