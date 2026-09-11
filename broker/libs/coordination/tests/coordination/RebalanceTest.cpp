// ---------------------------------------------------------------------------
// RebalanceTest.cpp — JoinGroup/SyncGroup rebalance cycle tests.
//
// All tests: bounded, deterministic, injected clock. NO busy-spin.
// Hard cap on any driver loop + ASSERT_TRUE(done).
//
// API contract (GroupCoordinator extensions for P4-3):
//   join() — unchanged. While in PreparingRebalance, each member re-joins.
//            When all expected members have re-joined, the coordinator
//            transitions PreparingRebalance→CompletingRebalance automatically.
//   syncGroup(memberId, generationId, assignmentsPerMember, nowNs) → SyncResult
//            Leader submits the full assignment map; followers call with empty map.
//            Generation-fenced (stale gen rejected).
//            When all members have called syncGroup (leader's assignment stored),
//            transitions CompletingRebalance→Stable and stores each member's
//            assignment in MemberInfo::assignment.
//   assignmentFor(memberId) → vector<TopicPartition>   (empty if unknown)
//   autoAssignAndSync(partCounts, assignorType, nowNs) → SyncResult
//            Convenience: runs the chosen assignor internally, stores the
//            result, transitions to Stable.
//   setRebalanceTimeoutMs(ms)  — inject join timeout for doWork-driven expiry.
//   doWork(nowNs) — also handles join-timeout: if all expected members haven't
//                   re-joined within rebalanceTimeoutMs, transition to
//                   CompletingRebalance with whoever has joined so far.
//
// SyncResult:  errorCode = 0 (ok), kErrStaleGeneration, kErrUnknownMember,
//              kErrWrongState.
//
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/Assignor.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

using namespace rune::coordination;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int kDriverCap    = 500;
static constexpr int32_t kTimeout  = 30'000; // 30s session timeout

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static int64_t afterTimeout(int64_t startNs, int32_t timeoutMs) {
    return startNs + static_cast<int64_t>(timeoutMs) * 1'000'000LL + 1;
}

/// Build MemberSubscription list from coordinator members.
static std::vector<MemberSubscription> buildSubscriptions(const GroupCoordinator& gc) {
    std::vector<MemberSubscription> subs;
    for (const auto& m : gc.members()) {
        subs.push_back({m.memberId, m.subscribedTopics});
    }
    return subs;
}

/// Collect all TopicPartitions across all members of an Assignment.
static std::set<TopicPartition> unionOfAssignment(const Assignment& a) {
    std::set<TopicPartition> s;
    for (const auto& [mid, tps] : a) s.insert(tps.begin(), tps.end());
    return s;
}

/// Check all assignments are disjoint.
static bool disjoint(const Assignment& a) {
    std::vector<TopicPartition> all;
    for (const auto& [mid, tps] : a) all.insert(all.end(), tps.begin(), tps.end());
    std::set<TopicPartition> s(all.begin(), all.end());
    return s.size() == all.size();
}

/// All partitions for a topic (0..count-1).
static std::set<TopicPartition> allPartitions(const std::string& topic, int32_t count) {
    std::set<TopicPartition> s;
    for (int32_t i = 0; i < count; ++i) s.insert({topic, i});
    return s;
}

/// Drive coordinator to CompletingRebalance by having each member re-join.
/// Returns the generationId at that state.
static Generation driveToCompleting(GroupCoordinator& gc,
                                     const std::vector<std::string>& memberIds,
                                     const std::vector<std::string>& topics,
                                     int64_t startNs)
{
    int64_t t = startNs;
    for (const auto& mid : memberIds) {
        gc.join(mid, kTimeout, topics, t);
        t += 100;
    }
    return gc.generationId();
}

// ===========================================================================
// Test 1: Initial join → PreparingRebalance; after all re-join → CompletingRebalance
// ===========================================================================
TEST(Rebalance, AllMembersRejoin_TransitionToCompletingRebalance) {
    GroupCoordinator gc("test-group", 0);

    // Initial joins — trigger PreparingRebalance.
    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);

    // All re-join during PreparingRebalance.
    gc.join("member-a", kTimeout, {"topic"}, 100);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance); // still waiting for b

    gc.join("member-b", kTimeout, {"topic"}, 200);
    EXPECT_EQ(gc.state(), GroupState::CompletingRebalance);
}

// ===========================================================================
// Test 2: Full cycle: join → CompletingRebalance → leader syncs → Stable
// ===========================================================================
TEST(Rebalance, FullCycleJoinSyncStable) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"topic", 4}};

    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);

    // All re-join → CompletingRebalance.
    gc.join("member-a", kTimeout, {"topic"}, 100);
    gc.join("member-b", kTimeout, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    // Leader computes and syncs.
    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto computed = ra.assign(subs, partCounts);

    std::string lid = gc.leaderId();
    Generation gen = gc.generationId();

    auto sr = gc.syncGroup(lid, gen, computed, 300);
    EXPECT_EQ(sr.errorCode, 0);
    // State transitions to Stable once all members have synced.
    // (Leader sync stores the assignment; other members can sync too.)
    // After leader + follower both sync → Stable.
    std::string fid = (gc.members()[0].memberId == lid)
                       ? gc.members()[1].memberId
                       : gc.members()[0].memberId;
    auto fsr = gc.syncGroup(fid, gen, {}, 310);
    EXPECT_EQ(fsr.errorCode, 0);
    EXPECT_EQ(gc.state(), GroupState::Stable);
}

// ===========================================================================
// Test 3: Union of assignments == all partitions; disjoint
// ===========================================================================
TEST(Rebalance, AssignmentUnionEqualsAllPartitionsDisjoint) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"topic", 6}};

    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);
    gc.join("member-a", kTimeout, {"topic"}, 100);
    gc.join("member-b", kTimeout, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto computed = ra.assign(subs, partCounts);
    std::string lid = gc.leaderId();
    Generation gen = gc.generationId();
    gc.syncGroup(lid, gen, computed, 300);
    for (const auto& m : gc.members()) {
        if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 310);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    Assignment stable;
    for (const auto& m : gc.members()) stable[m.memberId] = m.assignment;

    auto got = unionOfAssignment(stable);
    auto expected = allPartitions("topic", 6);
    EXPECT_EQ(got, expected);
    EXPECT_TRUE(disjoint(stable));
}

// ===========================================================================
// Test 4: Churn — new member joins while Stable → generation bumps, rebalance
// ===========================================================================
TEST(Rebalance, NewMemberWhileStable_RebalanceTriggered) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"topic", 4}};

    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);
    gc.join("member-a", kTimeout, {"topic"}, 100);
    gc.join("member-b", kTimeout, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto computed = ra.assign(subs, partCounts);
    std::string lid = gc.leaderId();
    Generation gen = gc.generationId();
    gc.syncGroup(lid, gen, computed, 300);
    for (const auto& m : gc.members()) {
        if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 310);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    Generation genStable = gc.generationId();

    // New member joins → PreparingRebalance, generation bumped.
    gc.join("member-c", kTimeout, {"topic"}, 400);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
    EXPECT_GT(gc.generationId(), genStable);
}

// ===========================================================================
// Test 5: Churn — member leaves while Stable → partitions reassigned
// ===========================================================================
TEST(Rebalance, MemberLeavesWhileStable_PartitionsReassigned) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"topic", 4}};

    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);
    gc.join("member-a", kTimeout, {"topic"}, 100);
    gc.join("member-b", kTimeout, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    {
        auto subs = buildSubscriptions(gc);
        auto comp = ra.assign(subs, partCounts);
        std::string lid = gc.leaderId();
        Generation gen = gc.generationId();
        gc.syncGroup(lid, gen, comp, 300);
        for (const auto& m : gc.members()) {
            if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 310);
        }
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // member-b leaves → rebalance.
    Generation genBefore = gc.generationId();
    gc.leave("member-b", gc.generationId(), 400);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
    EXPECT_GT(gc.generationId(), genBefore);

    // member-a re-joins alone → CompletingRebalance.
    gc.join("member-a", kTimeout, {"topic"}, 500);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    // Leader (only member-a) gets all partitions.
    {
        auto subs = buildSubscriptions(gc);
        auto newAssignment = ra.assign(subs, partCounts);
        std::string lid = gc.leaderId();
        Generation gen = gc.generationId();
        auto sr = gc.syncGroup(lid, gen, newAssignment, 600);
        EXPECT_EQ(sr.errorCode, 0);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // member-a has all 4 partitions.
    EXPECT_EQ(gc.memberCount(), 1u);
    EXPECT_EQ(static_cast<int32_t>(gc.members()[0].assignment.size()), 4);
}

// ===========================================================================
// Test 6: Generation fencing — SyncGroup with stale generationId rejected
// ===========================================================================
TEST(Rebalance, SyncGroupStaleGeneration_Rejected) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);
    gc.join("member-a", kTimeout, {"topic"}, 100);
    gc.join("member-b", kTimeout, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    Generation staleGen = gc.generationId() - 1;
    auto sr = gc.syncGroup("member-a", staleGen, {}, 300);
    EXPECT_NE(sr.errorCode, 0);
    EXPECT_EQ(sr.errorCode, SyncResult::kErrStaleGeneration);
}

// ===========================================================================
// Test 7: SyncGroup with future generationId also rejected
// ===========================================================================
TEST(Rebalance, SyncGroupFutureGeneration_Rejected) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    auto sr = gc.syncGroup("member-a", gc.generationId() + 99, {}, 200);
    EXPECT_EQ(sr.errorCode, SyncResult::kErrStaleGeneration);
}

// ===========================================================================
// Test 8: Follower syncGroup before leader completes — gets retry signal
// ===========================================================================
TEST(Rebalance, FollowerSyncBeforeLeader_GetsRetrySignal) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    gc.join("member-b", kTimeout, {"t"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    std::string lid = gc.leaderId();
    std::string fid;
    for (const auto& m : gc.members()) {
        if (m.memberId != lid) { fid = m.memberId; break; }
    }

    Generation gen = gc.generationId();

    // Follower syncs before leader: must receive the retry signal.
    auto fsr = gc.syncGroup(fid, gen, {}, 250);
    EXPECT_EQ(fsr.errorCode, SyncResult::kErrRebalanceInProgress)
        << "Follower syncing before leader must receive kErrRebalanceInProgress (retry)";
    // State stays CompletingRebalance (leader hasn't submitted yet).
    EXPECT_EQ(gc.state(), GroupState::CompletingRebalance);

    // Now leader submits — group transitions to Stable.
    TopicPartitionCounts partCounts = {{"t", 2}};
    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto computed = ra.assign(subs, partCounts);
    auto lsr = gc.syncGroup(lid, gen, computed, 300);
    EXPECT_EQ(lsr.errorCode, SyncResult::kOk);
    EXPECT_EQ(gc.state(), GroupState::Stable);

    // Follower retries after leader — now succeeds.
    auto fsr2 = gc.syncGroup(fid, gen, {}, 350);
    EXPECT_EQ(fsr2.errorCode, SyncResult::kOk);
}

// ===========================================================================
// Test 9: Auto-assign convenience path (RangeAssignor)
// ===========================================================================
TEST(Rebalance, AutoAssignConveniencePath_Range) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"topic", 4}};

    gc.join("member-a", kTimeout, {"topic"}, 0);
    gc.join("member-b", kTimeout, {"topic"}, 0);
    gc.join("member-a", kTimeout, {"topic"}, 100);
    gc.join("member-b", kTimeout, {"topic"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    auto sr = gc.autoAssignAndSync(partCounts, AssignorType::Range, 300);
    EXPECT_EQ(sr.errorCode, 0);
    EXPECT_EQ(gc.state(), GroupState::Stable);

    // Verify union and disjoint.
    Assignment stable;
    for (const auto& m : gc.members()) stable[m.memberId] = m.assignment;
    EXPECT_TRUE(disjoint(stable));
    EXPECT_EQ(unionOfAssignment(stable), allPartitions("topic", 4));
}

// ===========================================================================
// Test 10: doWork join-timeout: not all members re-joined → CompletingRebalance
// ===========================================================================
TEST(Rebalance, JoinTimeout_TransitionsToCompletingRebalance) {
    GroupCoordinator gc("test-group", 0);
    gc.setRebalanceTimeoutMs(1'000); // 1s join timeout

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);

    // Only member-a re-joins.
    gc.join("member-a", kTimeout, {"t"}, 100);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);

    // Advance past join timeout without member-b re-joining.
    bool done = false;
    int64_t clock = 100;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 500'000'000LL; // 500ms steps
        gc.doWork(clock);
        if (gc.state() == GroupState::CompletingRebalance) done = true;
    }
    ASSERT_TRUE(done) << "Join timeout did not transition to CompletingRebalance";
    // Only member-a (who re-joined) should be in the group.
    EXPECT_EQ(gc.memberCount(), 1u);
}

// ===========================================================================
// Test 11: Stable state heartbeat → OK
// ===========================================================================
TEST(Rebalance, StableHeartbeat_OK) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 2}};

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto comp = ra.assign(subs, partCounts);
    gc.syncGroup("member-a", gc.generationId(), comp, 200);
    ASSERT_EQ(gc.state(), GroupState::Stable);

    auto hb = gc.heartbeat("member-a", gc.generationId(), 300);
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kOk);
}

// ===========================================================================
// Test 12: 3-member full cycle with RoundRobin → Stable → assignmentFor exact
// ===========================================================================
TEST(Rebalance, ThreeMember_RoundRobin_Stable_ExactAssignmentFor) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 6}};

    gc.join("m1", kTimeout, {"t"}, 0);
    gc.join("m2", kTimeout, {"t"}, 0);
    gc.join("m3", kTimeout, {"t"}, 0);

    gc.join("m1", kTimeout, {"t"}, 100);
    gc.join("m2", kTimeout, {"t"}, 200);
    gc.join("m3", kTimeout, {"t"}, 300);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RoundRobinAssignor rr;
    auto subs = buildSubscriptions(gc);
    auto computed = rr.assign(subs, partCounts);
    std::string lid = gc.leaderId();
    Generation gen = gc.generationId();
    gc.syncGroup(lid, gen, computed, 400);
    for (const auto& m : gc.members()) {
        if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 410);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // assignmentFor matches MemberInfo::assignment.
    for (const auto& m : gc.members()) {
        auto af = gc.assignmentFor(m.memberId);
        EXPECT_EQ(af, m.assignment);
        EXPECT_EQ(static_cast<int32_t>(af.size()), 2); // 6 partitions / 3 members
    }

    // Union disjoint.
    Assignment stable;
    for (const auto& m : gc.members()) stable[m.memberId] = m.assignment;
    EXPECT_TRUE(disjoint(stable));
    EXPECT_EQ(unionOfAssignment(stable), allPartitions("t", 6));
}

// ===========================================================================
// Test 13: generationId monotonic across full rebalance+churn sequence
// ===========================================================================
TEST(Rebalance, GenerationMonotonicAcrossRebalanceChurn) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 4}};
    Generation prev = 0;

    gc.join("ma", kTimeout, {"t"}, 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    gc.join("mb", kTimeout, {"t"}, 0);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    gc.join("ma", kTimeout, {"t"}, 100);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    gc.join("mb", kTimeout, {"t"}, 200);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    {
        RangeAssignor ra;
        auto subs = buildSubscriptions(gc);
        auto comp = ra.assign(subs, partCounts);
        std::string lid = gc.leaderId();
        Generation gen = gc.generationId();
        gc.syncGroup(lid, gen, comp, 300);
        for (const auto& m : gc.members()) {
            if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 310);
        }
    }
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    ASSERT_EQ(gc.state(), GroupState::Stable);

    // Churn: mc joins.
    gc.join("mc", kTimeout, {"t"}, 400);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();

    gc.join("ma", kTimeout, {"t"}, 500);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    gc.join("mb", kTimeout, {"t"}, 600);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    gc.join("mc", kTimeout, {"t"}, 700);
    EXPECT_GE(gc.generationId(), prev); prev = gc.generationId();
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    {
        RangeAssignor ra;
        auto subs = buildSubscriptions(gc);
        auto comp = ra.assign(subs, partCounts);
        std::string lid = gc.leaderId();
        Generation gen = gc.generationId();
        gc.syncGroup(lid, gen, comp, 800);
        for (const auto& m : gc.members()) {
            if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 810);
        }
    }
    EXPECT_GE(gc.generationId(), prev);
}

// ===========================================================================
// Test 14: Re-join during PreparingRebalance does not duplicate members
// ===========================================================================
TEST(Rebalance, RejoinDuringPreparingNoDuplicate) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);
    std::size_t countBefore = gc.memberCount();

    gc.join("member-a", kTimeout, {"t"}, 100);
    gc.join("member-b", kTimeout, {"t"}, 200);
    EXPECT_EQ(gc.memberCount(), countBefore);
}

// ===========================================================================
// Test 15: After leader leaves, new leader elected for next sync
// ===========================================================================
TEST(Rebalance, AfterLeaderLeaves_NewLeaderElected) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 4}};

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    gc.join("member-c", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    gc.join("member-b", kTimeout, {"t"}, 200);
    gc.join("member-c", kTimeout, {"t"}, 300);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    {
        RangeAssignor ra;
        auto subs = buildSubscriptions(gc);
        auto comp = ra.assign(subs, partCounts);
        std::string lid = gc.leaderId();
        Generation gen = gc.generationId();
        gc.syncGroup(lid, gen, comp, 400);
        for (const auto& m : gc.members()) {
            if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 410);
        }
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    std::string oldLeader = gc.leaderId();
    gc.leave(oldLeader, gc.generationId(), 500);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);

    EXPECT_FALSE(gc.leaderId().empty());
    EXPECT_NE(gc.leaderId(), oldLeader);
}

// ===========================================================================
// Test 16: doWork bounded driver — hard cap + ASSERT(done)
// ===========================================================================
TEST(Rebalance, DoWorkBoundedDriver) {
    GroupCoordinator gc("test-group", 0);
    gc.setRebalanceTimeoutMs(1'000);

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);

    bool done = false;
    int64_t clock = 100;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 250'000'000LL;
        gc.doWork(clock);
        if (gc.state() == GroupState::CompletingRebalance) done = true;
    }
    ASSERT_TRUE(done) << "Bounded driver stalled after " << kDriverCap << " iterations";
}

// ===========================================================================
// Test 17: assignmentFor unknown member → empty
// ===========================================================================
TEST(Rebalance, AssignmentForUnknownMember_Empty) {
    GroupCoordinator gc("test-group", 0);
    auto af = gc.assignmentFor("nonexistent");
    EXPECT_TRUE(af.empty());
}

// ===========================================================================
// Test 18: syncGroup unknown member → kErrUnknownMember
// ===========================================================================
TEST(Rebalance, SyncGroupUnknownMember_Rejected) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    auto sr = gc.syncGroup("ghost", gc.generationId(), {}, 200);
    EXPECT_EQ(sr.errorCode, SyncResult::kErrUnknownMember);
}

// ===========================================================================
// Test 19: Stable → new join resets to PreparingRebalance
// ===========================================================================
TEST(Rebalance, StableNewJoin_BackToPreparingRebalance) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 2}};

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto comp = ra.assign(subs, partCounts);
    gc.syncGroup("member-a", gc.generationId(), comp, 200);
    ASSERT_EQ(gc.state(), GroupState::Stable);

    Generation genStable = gc.generationId();
    gc.join("member-b", kTimeout, {"t"}, 300);
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
    EXPECT_GT(gc.generationId(), genStable);
}

// ===========================================================================
// Test 20: Full cycle — 2 members, 4 partitions, each gets 2 (range), stable
// ===========================================================================
TEST(Rebalance, TwoMembersFourPartitions_EachGetsTwoStable) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 4}};

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    gc.join("member-b", kTimeout, {"t"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto comp = ra.assign(subs, partCounts);
    std::string lid = gc.leaderId();
    Generation gen = gc.generationId();
    gc.syncGroup(lid, gen, comp, 300);
    for (const auto& m : gc.members()) {
        if (m.memberId != lid) gc.syncGroup(m.memberId, gen, {}, 310);
    }
    ASSERT_EQ(gc.state(), GroupState::Stable);

    for (const auto& m : gc.members()) {
        EXPECT_EQ(static_cast<int32_t>(m.assignment.size()), 2)
            << "Member " << m.memberId << " has wrong assignment size";
    }

    Assignment stable;
    for (const auto& m : gc.members()) stable[m.memberId] = m.assignment;
    EXPECT_TRUE(disjoint(stable));
    EXPECT_EQ(unionOfAssignment(stable), allPartitions("t", 4));
}

// ===========================================================================
// Test 21: autoAssignAndSync with RoundRobin
// ===========================================================================
TEST(Rebalance, AutoAssignRoundRobin_Stable) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 6}};

    gc.join("member-a", kTimeout, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    gc.join("member-a", kTimeout, {"t"}, 100);
    gc.join("member-b", kTimeout, {"t"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    auto sr = gc.autoAssignAndSync(partCounts, AssignorType::RoundRobin, 300);
    EXPECT_EQ(sr.errorCode, 0);
    EXPECT_EQ(gc.state(), GroupState::Stable);

    Assignment stable;
    for (const auto& m : gc.members()) stable[m.memberId] = m.assignment;
    EXPECT_TRUE(disjoint(stable));
    EXPECT_EQ(unionOfAssignment(stable), allPartitions("t", 6));
}

// ===========================================================================
// Test 22: syncGroup wrong state (not CompletingRebalance) → kErrWrongState
// ===========================================================================
TEST(Rebalance, SyncGroupWrongState_Rejected) {
    GroupCoordinator gc("test-group", 0);

    gc.join("member-a", kTimeout, {"t"}, 0);
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);

    // Can't sync while in PreparingRebalance.
    auto sr = gc.syncGroup("member-a", gc.generationId(), {}, 100);
    EXPECT_EQ(sr.errorCode, SyncResult::kErrWrongState);
}

// ===========================================================================
// Test 23: Session timeout during CompletingRebalance — member reaped, rebalance
// ===========================================================================
TEST(Rebalance, SessionTimeoutDuringCompletingRebalance_Rebalance) {
    GroupCoordinator gc("test-group", 0);
    gc.join("member-a", 1'000 /*1s*/, {"t"}, 0);
    gc.join("member-b", kTimeout, {"t"}, 0);
    gc.join("member-a", 1'000, {"t"}, 100);
    gc.join("member-b", kTimeout, {"t"}, 200);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    // Advance past member-a's session timeout without syncing.
    int64_t expiry = afterTimeout(100, 1'000); // member-a last heartbeat at 100
    int work = gc.doWork(expiry);
    EXPECT_GE(work, 1); // member-a reaped
    // Group should rebalance again (since member was lost).
    EXPECT_EQ(gc.state(), GroupState::PreparingRebalance);
}

// ===========================================================================
// Test 24: Single member full cycle — alone, gets all partitions
// ===========================================================================
TEST(Rebalance, SingleMemberGetsAllPartitions) {
    GroupCoordinator gc("test-group", 0);
    TopicPartitionCounts partCounts = {{"t", 5}};

    gc.join("solo", kTimeout, {"t"}, 0);
    gc.join("solo", kTimeout, {"t"}, 100);
    ASSERT_EQ(gc.state(), GroupState::CompletingRebalance);

    RangeAssignor ra;
    auto subs = buildSubscriptions(gc);
    auto comp = ra.assign(subs, partCounts);
    auto sr = gc.syncGroup("solo", gc.generationId(), comp, 200);
    EXPECT_EQ(sr.errorCode, 0);
    ASSERT_EQ(gc.state(), GroupState::Stable);

    auto af = gc.assignmentFor("solo");
    EXPECT_EQ(static_cast<int32_t>(af.size()), 5);
    EXPECT_EQ(unionOfAssignment({{"solo", af}}), allPartitions("t", 5));
}
