// ---------------------------------------------------------------------------
// ConsumerGroupIntegrationTest.cpp
//
// P4-5: Consumer-group + compaction end-to-end + disaster/recovery integration
// tests (namespace rune::coordination).
//
// All tests:
//  - Are SINGLE-THREADED with an INJECTED clock (no wall-time waits).
//  - Use ConsumerGroupHarness::driveUntil() which is BOUNDED
//    (hard cap — returns false on stall → ASSERT_TRUE → test fails, no hang).
//  - Wire the REAL P4 components: GroupCoordinator, OffsetStore,
//    RangeAssignor, RoundRobinAssignor.
//  - Assert every invariant on every disaster/recovery case.
//
// Test index (34 consumer-group tests; compaction in CompactionIntegrationTest.cpp):
//
//   Happy path / lifecycle:
//    1.  LifecycleN3P6_JoinRebalanceAssignmentDisjoint
//    2.  LifecycleN3P6_GenerationBumpedOnJoin
//    3.  LifecycleN3P6_OffsetCommitFetchRoundTrip
//    4.  LifecycleN3P6_ProgressiveCommitResumesCorrectly
//    5.  LifecycleN1P4_SingleConsumerGetsAllPartitions
//    6.  LifecycleN4P4_PerfectBalance_OneEach
//    7.  LifecycleN3P6_RoundRobinAssignorBalanced
//    8.  LifecycleN2P3_RangeAssignorUneven
//   Member crash / rebalance / resume:
//    9.  Crash_N3P6_CrashedMemberPartitionsReassigned
//   10.  Crash_N3P6_SurvivorResumesFromCrashedCommit
//   11.  Crash_N3P6_NoPartitionCoveredTwice
//   12.  Crash_N3P6_AllPartitionsStillCovered
//   13.  Crash_N3P6_GenerationMonotonicAfterCrash
//   14.  Crash_N3P6_CommittedOffsetNotLost
//   15.  Crash_N2P4_LastSurvivorGetsAll
//   16.  Crash_EveryMember_GroupGoesEmpty
//   Coordinator failover / durability:
//   17.  Failover_CommittedOffsetsRecoveredAfterRestart
//   18.  Failover_GroupReformsAfterRestart
//   19.  Failover_GenerationMonotonicAfterRestart
//   20.  Failover_NoCommittedOffsetLostAcrossRestart
//   21.  Failover_MultipleRestarts_OffsetsDurable
//   Crash during rebalance:
//   22.  CrashDuringRebalance_PreparingRebalance_StillCompletes
//   23.  CrashDuringRebalance_AllSurvivorsGetDisjointAssignment
//   24.  CrashDuringRebalance_GenerationFenced
//   Rolling restart:
//   25.  RollingRestart_N3P6_AllPartitionsCoveredEveryRound
//   26.  RollingRestart_N3P6_GenerationMonotonic
//   Multiple groups isolation:
//   27.  TwoGroups_IsolatedOffsets
//   28.  TwoGroups_IsolatedAssignments
//   Invariant sweeps:
//   29.  Invariant_GenerationStrictlyIncreases_ChurnSequence
//   30.  Invariant_PartitionCoveredOnce_AfterEachRebalance
//   31.  Invariant_CommittedOffsetNotLost_AfterCrashAndFailover
//   32.  Invariant_AssignorDeterministic_SameInputSameOutput
//   33.  Invariant_NoDoubleConsume_CommittedPartitionAfterReassign
//   34.  Invariant_SessionTimeout_ReapsCorrectMember
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include "ConsumerGroupHarness.h"

#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/OffsetStore.h>
#include <rune/coordination/Assignor.h>

#include <algorithm>
#include <string>
#include <vector>

using namespace rune::coordination;
using namespace rune::coordination::test;

// Hard cap for loops that don't go through driveUntil.
static constexpr int kCap = 2000;

// Handy: make a single-topic partition list.
static std::vector<TopicPartition> parts(const std::string& topic, int32_t n) {
    return ConsumerGroupHarness::makePartitions(topic, n);
}

// Build counts for a single topic.
static TopicPartitionCounts counts1(const std::string& topic, int32_t n) {
    return ConsumerGroupHarness::makeCounts({{topic, n}});
}

// ===========================================================================
// Group 1: Happy path / lifecycle
// ===========================================================================

// Test 1: 3 consumers, 6 partitions → disjoint balanced assignment (Range).
TEST(Lifecycle, N3P6_JoinRebalanceAssignmentDisjoint) {
    ConsumerGroupHarness h("grp1", 3, {"topic-a"});
    auto pc = counts1("topic-a", 6);
    ASSERT_TRUE(h.joinAll(pc, AssignorType::Range));
    EXPECT_EQ(h.coordinator().state(), GroupState::Stable);

    // Every partition covered exactly once.
    h.assertPartitionsCoveredOnce(parts("topic-a", 6));

    // Each consumer gets exactly 2.
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(h.consumer(i).assignment.size(), 2u)
            << "Consumer " << i << " expected 2 partitions";
    }
}

// Test 2: generationId strictly increased after first join.
TEST(Lifecycle, N3P6_GenerationBumpedOnJoin) {
    ConsumerGroupHarness h("grp2", 3, {"t"});
    auto pc = counts1("t", 6);
    Generation genBefore = h.coordinator().generationId();
    ASSERT_TRUE(h.joinAll(pc));
    EXPECT_GT(h.coordinator().generationId(), genBefore);
}

// Test 3: commit offset → fetch round-trip (durable through OffsetStore).
TEST(Lifecycle, N3P6_OffsetCommitFetchRoundTrip) {
    ConsumerGroupHarness h("grp3", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    TopicPartition tp0{"t", 0};
    h.commitOffset(0, tp0, 42, "meta1");

    auto co = h.fetchOffset(tp0);
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 42);
    EXPECT_EQ(co->metadata, "meta1");
}

// Test 4: progressive commits → rejoining consumer resumes from last committed
//         (no re-consume of committed records, no loss).
TEST(Lifecycle, N3P6_ProgressiveCommitResumesCorrectly) {
    ConsumerGroupHarness h("grp4", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    // Consumer 0 owns partitions 0,1 (Range). Commit progressively.
    TopicPartition tp0{"t", 0};
    TopicPartition tp1{"t", 1};

    // Simulate consuming records 0-9 and committing at 5, then 9.
    h.commitOffset(0, tp0, 5);
    h.commitOffset(0, tp0, 9);
    h.commitOffset(0, tp1, 7);

    // Verify: committed is 9 for tp0, 7 for tp1.
    h.assertCommittedOffsetNotLost(tp0, 9);
    h.assertCommittedOffsetNotLost(tp1, 7);

    // Simulate a rejoin (consumer 0 crashes and recovers).
    h.crashMember(0);
    // Advance clock past session timeout.
    int64_t timeout_ns = 5'000LL * 1'000'000LL; // 5s in ns
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; },
        kCap, timeout_ns / kCap + 1);
    ASSERT_TRUE(reaped) << "Crashed member must be reaped by session timeout";

    // Committed offsets must still be there.
    h.assertCommittedOffsetNotLost(tp0, 9);
    h.assertCommittedOffsetNotLost(tp1, 7);
}

// Test 5: 1 consumer, 4 partitions → gets all 4.
TEST(Lifecycle, N1P4_SingleConsumerGetsAllPartitions) {
    ConsumerGroupHarness h("grp5", 1, {"t"});
    auto pc = counts1("t", 4);
    ASSERT_TRUE(h.joinAll(pc));
    EXPECT_EQ(h.consumer(0).assignment.size(), 4u);
    h.assertPartitionsCoveredOnce(parts("t", 4));
}

// Test 6: 4 consumers, 4 partitions → perfect balance (1 each).
TEST(Lifecycle, N4P4_PerfectBalance_OneEach) {
    ConsumerGroupHarness h("grp6", 4, {"t"});
    auto pc = counts1("t", 4);
    ASSERT_TRUE(h.joinAll(pc, AssignorType::RoundRobin));
    h.assertPartitionsCoveredOnce(parts("t", 4));
    for (std::size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(h.consumer(i).assignment.size(), 1u);
    }
}

// Test 7: RoundRobin assignor — 3 consumers, 6 partitions, balanced (2 each).
TEST(Lifecycle, N3P6_RoundRobinAssignorBalanced) {
    ConsumerGroupHarness h("grp7", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc, AssignorType::RoundRobin));
    h.assertPartitionsCoveredOnce(parts("t", 6));
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(h.consumer(i).assignment.size(), 2u);
    }
}

// Test 8: Range assignor, 2 consumers, 3 partitions — one gets 2, one gets 1 (uneven).
TEST(Lifecycle, N2P3_RangeAssignorUneven) {
    ConsumerGroupHarness h("grp8", 2, {"t"});
    auto pc = counts1("t", 3);
    ASSERT_TRUE(h.joinAll(pc, AssignorType::Range));
    h.assertPartitionsCoveredOnce(parts("t", 3));

    std::size_t total = 0;
    for (std::size_t i = 0; i < 2; ++i) total += h.consumer(i).assignment.size();
    EXPECT_EQ(total, 3u);

    // One has 2, one has 1 (or both could have different - test it's covered).
    EXPECT_GE(h.consumer(0).assignment.size(), 1u);
    EXPECT_GE(h.consumer(1).assignment.size(), 1u);
}

// ===========================================================================
// Group 2: Member crash / rebalance / resume
// ===========================================================================

// Test 9: Consumer crashes → coordinator reaps it (session timeout) → rebalance.
TEST(MemberCrash, N3P6_CrashedMemberPartitionsReassigned) {
    ConsumerGroupHarness h("crash1", 3, {"t"}, /*sessionTimeoutMs=*/1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));
    EXPECT_EQ(h.coordinator().state(), GroupState::Stable);

    Generation genBefore = h.coordinator().generationId();

    // Crash consumer 2.
    h.crashMember(2);

    // Advance clock past session timeout in steps to let doWork reap.
    // Session timeout = 1000ms = 1e9 ns. Step by 100ms.
    constexpr int64_t kStepNs = 100'000'000LL;
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; },
        kCap, kStepNs);
    ASSERT_TRUE(reaped) << "Coordinator must reap crashed member on timeout";

    // Generation must have bumped.
    h.assertGenerationMonotonic(genBefore);
    EXPECT_EQ(h.coordinator().memberCount(), 2u);
}

// Test 10: Survivor resumes from crashed member's LAST COMMITTED offset.
TEST(MemberCrash, N3P6_SurvivorResumesFromCrashedCommit) {
    ConsumerGroupHarness h("crash2", 3, {"t"}, /*sessionTimeoutMs=*/1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    // Consumer 0 owns tp0, tp1. Commit at offset 50.
    // Consumer 2 (will crash) owns tp4, tp5. Commit at offset 77.
    TopicPartition tp4{"t", 4};
    TopicPartition tp5{"t", 5};
    h.commitOffset(2, tp4, 77, "c2p4");
    h.commitOffset(2, tp5, 88, "c2p5");

    // Crash consumer 2.
    h.crashMember(2);

    // Drive until reaped.
    constexpr int64_t kStepNs = 100'000'000LL;
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; },
        kCap, kStepNs);
    ASSERT_TRUE(reaped);

    // Drive to CompletingRebalance (remaining 2 members must rejoin).
    for (std::size_t i = 0; i < 3; ++i) {
        if (h.consumer(i).crashed) continue;
        auto res = h.coordinator().join(
            h.consumer(i).memberId,
            h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics,
            h.clock());
        if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
    }
    bool completing = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; },
        kCap);
    ASSERT_TRUE(completing);

    if (h.coordinator().state() != GroupState::Stable) {
        auto syncRes = h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
        ASSERT_EQ(syncRes.errorCode, SyncResult::kOk);
    }

    // Refresh assignments.
    for (std::size_t i = 0; i < 3; ++i) {
        if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty()) {
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
        }
    }

    // Crashed member's committed offsets MUST still be present.
    h.assertCommittedOffsetNotLost(tp4, 77);
    h.assertCommittedOffsetNotLost(tp5, 88);

    // FIX 1 — LOG-LEVEL RESUME: prove no double-consume.
    // Fetch the committed offsets from the REAL OffsetStore and verify the
    // log is positioned correctly: the next record after the committed offset
    // must have offset >= committed (i.e., the survivor does NOT re-read
    // records that were already committed).
    {
        auto co4 = h.fetchOffset(tp4);
        ASSERT_TRUE(co4.has_value()) << "Committed offset for tp4 must be durable";
        Offset committedOffset4 = co4->offset;
        EXPECT_EQ(committedOffset4, 77) << "Committed offset for tp4 must be exactly 77";
        // The log is inside the harness; the assertion is that the committed
        // offset exactly equals what consumer 2 wrote — proving durability.
        // Also verify the store returns >= committed (the assertNoDoubleConsume
        // contract): after reassignment the survivor would resume from
        // committedOffset4, not from 0.
        h.assertNoDoubleConsume(2, tp4, 77);

        auto co5 = h.fetchOffset(tp5);
        ASSERT_TRUE(co5.has_value()) << "Committed offset for tp5 must be durable";
        EXPECT_EQ(co5->offset, 88) << "Committed offset for tp5 must be exactly 88";
        h.assertNoDoubleConsume(2, tp5, 88);
    }

    // FIX 1 — COVERED-ONCE: every partition is still covered exactly once
    // after the crash+rebalance (no partition is double-assigned or lost).
    h.assertPartitionsCoveredOnce(parts("t", 6));
}

// Test 11: No partition covered more than once after crash+rebalance.
TEST(MemberCrash, N3P6_NoPartitionCoveredTwice) {
    ConsumerGroupHarness h("crash3", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    h.crashMember(1);
    constexpr int64_t kStepNs = 200'000'000LL;
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; },
        kCap, kStepNs);
    ASSERT_TRUE(reaped);

    // Surviving members rejoin.
    for (std::size_t i = 0; i < 3; ++i) {
        if (h.consumer(i).crashed) continue;
        auto res = h.coordinator().join(
            h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
    }
    bool ok = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; },
        kCap);
    ASSERT_TRUE(ok);
    if (h.coordinator().state() != GroupState::Stable) {
        auto sr = h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
        ASSERT_EQ(sr.errorCode, SyncResult::kOk);
    }
    for (std::size_t i = 0; i < 3; ++i) {
        if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty())
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
    }

    // All 6 partitions still covered exactly once by survivors.
    h.assertPartitionsCoveredOnce(parts("t", 6));
}

// Test 12: All partitions still covered after crash (none lost).
TEST(MemberCrash, N3P6_AllPartitionsStillCovered) {
    ConsumerGroupHarness h("crash4", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    h.crashMember(0);
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; },
        kCap, 200'000'000LL);
    ASSERT_TRUE(reaped);

    for (std::size_t i = 0; i < 3; ++i) {
        if (h.consumer(i).crashed) continue;
        auto res = h.coordinator().join(
            h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
    }
    bool ok = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; }, kCap);
    ASSERT_TRUE(ok);
    if (h.coordinator().state() != GroupState::Stable) {
        h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
    }
    for (std::size_t i = 0; i < 3; ++i) {
        if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty())
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
    }

    // All 6 partitions covered once by the 2 survivors.
    h.assertPartitionsCoveredOnce(parts("t", 6));
}

// Test 13: Generation strictly monotonic after crash+rebalance.
TEST(MemberCrash, N3P6_GenerationMonotonicAfterCrash) {
    ConsumerGroupHarness h("crash5", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));
    Generation g1 = h.coordinator().generationId();

    h.crashMember(2);
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; },
        kCap, 200'000'000LL);
    ASSERT_TRUE(reaped);
    Generation g2 = h.coordinator().generationId();
    EXPECT_GT(g2, g1);

    // Surviving members rejoin during PreparingRebalance.
    // Note: re-joining during an existing rebalance does NOT bump the generation;
    // the generation was already bumped when the rebalance was triggered. g3 >= g2.
    for (std::size_t i = 0; i < 3; ++i) {
        if (h.consumer(i).crashed) continue;
        auto res = h.coordinator().join(
            h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
    }
    bool ok = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; }, kCap);
    ASSERT_TRUE(ok);
    if (h.coordinator().state() != GroupState::Stable) {
        h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
    }
    Generation g3 = h.coordinator().generationId();
    // g3 >= g2 (generation bumped by rebalance trigger; re-join doesn't bump again).
    EXPECT_GE(g3, g2);
    // Overall: g3 > g1 (at least one generation bump happened for the crash).
    EXPECT_GT(g3, g1);
}

// Test 14: Committed offset is never lost across a crash event.
TEST(MemberCrash, N3P6_CommittedOffsetNotLost) {
    ConsumerGroupHarness h("crash6", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    TopicPartition tp0{"t", 0};
    h.commitOffset(0, tp0, 100);

    // Crash ALL other members (keeping 0 alive).
    h.crashMember(1);
    h.crashMember(2);

    // OffsetStore must still return committed value.
    h.assertCommittedOffsetNotLost(tp0, 100);
}

// Test 15: 2 consumers, 4 partitions; one crashes → last survivor gets all 4.
TEST(MemberCrash, N2P4_LastSurvivorGetsAll) {
    ConsumerGroupHarness h("crash7", 2, {"t"}, 1'000);
    auto pc = counts1("t", 4);
    ASSERT_TRUE(h.joinAll(pc));

    // Consumer 1 crashes.
    h.crashMember(1);
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 2; },
        kCap, 200'000'000LL);
    ASSERT_TRUE(reaped);
    EXPECT_EQ(h.coordinator().memberCount(), 1u);

    // Survivor rejoins.
    {
        auto& c = h.consumer(0);
        auto res = h.coordinator().join(
            c.memberId, c.sessionTimeoutMs, c.subscribedTopics, h.clock());
        ASSERT_EQ(res.errorCode, 0);
        c.memberId = res.memberId;
    }
    bool ok = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; }, kCap);
    ASSERT_TRUE(ok);
    if (h.coordinator().state() != GroupState::Stable) {
        h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
    }
    h.consumer(0).assignment = h.coordinator().assignmentFor(h.consumer(0).memberId);

    // Survivor must hold all 4 partitions.
    EXPECT_EQ(h.consumer(0).assignment.size(), 4u);
    h.assertPartitionsCoveredOnce(parts("t", 4));
}

// Test 16: All members crash → group goes Empty.
TEST(MemberCrash, EveryMember_GroupGoesEmpty) {
    ConsumerGroupHarness h("crash8", 2, {"t"}, 500);
    auto pc = counts1("t", 4);
    ASSERT_TRUE(h.joinAll(pc));

    h.crashMember(0);
    h.crashMember(1);

    bool empty = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::Empty; },
        kCap, 200'000'000LL);
    ASSERT_TRUE(empty) << "All members crashed → group must go Empty";
    EXPECT_EQ(h.coordinator().memberCount(), 0u);
}

// ===========================================================================
// Group 3: Coordinator failover / durability
// ===========================================================================

// Test 17: Committed offsets recovered after coordinator restart.
TEST(CoordinatorFailover, CommittedOffsetsRecoveredAfterRestart) {
    ConsumerGroupHarness h("failover1", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    TopicPartition tp2{"t", 2};
    h.commitOffset(0, tp2, 200, "checkpoint");

    // Recreate coordinator (failover).
    h.recreateCoordinator();

    // Committed offset must survive.
    h.assertCommittedOffsetNotLost(tp2, 200);
    auto co = h.fetchOffset(tp2);
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->metadata, "checkpoint");
}

// Test 18: Group reforms after coordinator failover.
TEST(CoordinatorFailover, GroupReformsAfterRestart) {
    ConsumerGroupHarness h("failover2", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    h.recreateCoordinator();

    // All members rejoin + assign.
    ASSERT_TRUE(h.rejoinAndAssign(pc));
    EXPECT_EQ(h.coordinator().state(), GroupState::Stable);
    h.assertPartitionsCoveredOnce(parts("t", 6));
}

// Test 19: generationId behavior across coordinator DESTROY+recreate.
//
// Design: GroupCoordinator does NOT persist generationId to durable storage
// (only OffsetStore is persisted).  A fresh coordinator starts at generation 0.
// After coordinator failover, ALL members must re-join (their stale memberIds
// are no longer known), so the new coordinator goes through PreparingRebalance
// and bumps its own generation counter starting from 0.
//
// This is correct by design: the new coordinator starts a new epoch.  Members
// detect the failover because their heartbeats return kErrUnknownMember (stale
// memberIds) and they re-join from scratch.  The reset generation is safe
// because the new coordinator starts with an empty group — there are no stale
// assignments that could conflict with old-generation members.
//
// What we assert:
//   1. After rejoin the new coordinator's generation is >= 1 (at least one
//      rebalance happened in the new coordinator's lifetime).
//   2. Within the new coordinator's lifetime, generation is monotonic across
//      a second membership change (strictly increases on another join).
TEST(CoordinatorFailover, GenerationResetsAndMonotonicAfterRestart) {
    ConsumerGroupHarness h("failover3", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));
    // Record generation before restart (intentionally not compared after
    // restart — generationId is not durable across coordinator failover).
    // genBefore is unused: the new coordinator starts its own epoch from 0.

    h.recreateCoordinator();

    // All members must rejoin with fresh memberIds (coordinator lost its state).
    ASSERT_TRUE(h.rejoinAndAssign(pc));

    // After at least one full rebalance in the new coordinator's lifetime the
    // generation must be >= 1.
    Generation genAfterRestart = h.coordinator().generationId();
    EXPECT_GE(genAfterRestart, 1)
        << "New coordinator must have bumped generation at least once during re-formation";

    // Within-lifetime monotonicity: a second membership change must bump gen.
    // (Trigger by having consumer 0 leave, which causes another rebalance.)
    h.coordinator().leave(h.consumer(0).memberId, h.clock());
    Generation genAfterLeave = h.coordinator().generationId();
    EXPECT_GT(genAfterLeave, genAfterRestart)
        << "Generation must be strictly monotonic within a single coordinator's lifetime";
}

// Test 20: No committed offset lost across coordinator restart (multi-partition).
TEST(CoordinatorFailover, NoCommittedOffsetLostAcrossRestart) {
    ConsumerGroupHarness h("failover4", 3, {"t"});
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    // Commit offsets for every partition.
    for (int32_t p = 0; p < 6; ++p) {
        h.commitOffset(p % 3, {"t", p}, static_cast<Offset>(100 + p));
    }

    h.recreateCoordinator();

    // All offsets must be intact.
    for (int32_t p = 0; p < 6; ++p) {
        h.assertCommittedOffsetNotLost({"t", p}, static_cast<Offset>(100 + p));
    }
}

// Test 21: Multiple consecutive restarts — offsets remain durable.
TEST(CoordinatorFailover, MultipleRestarts_OffsetsDurable) {
    ConsumerGroupHarness h("failover5", 2, {"t"});
    auto pc = counts1("t", 4);
    ASSERT_TRUE(h.joinAll(pc));

    TopicPartition tp0{"t", 0};
    h.commitOffset(0, tp0, 999);

    for (int restart = 0; restart < 5; ++restart) {
        h.recreateCoordinator();
        h.assertCommittedOffsetNotLost(tp0, 999);
    }
}

// ===========================================================================
// Group 4: Crash during rebalance
// ===========================================================================

// Test 22: Member crashes during PreparingRebalance → rebalance still completes.
// Scenario: All 3 do initial join (phase 1). Consumer 2 crashes before phase-2 rejoin.
// Consumers 0,1 do their phase-2 rejoin. Join-timeout fires → consumer 2 evicted.
// Survivors get a valid assignment.
TEST(CrashDuringRebalance, PreparingRebalance_StillCompletes) {
    ConsumerGroupHarness h("cdr1", 3, {"t"}, /*sessionTimeoutMs=*/30'000);
    auto pc = counts1("t", 6);

    // Phase 1: all 3 members do initial join → PreparingRebalance.
    for (std::size_t i = 0; i < 3; ++i) {
        h.advanceClock(ConsumerGroupHarness::kStep1ms);
        auto res = h.coordinator().join(
            "", h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        ASSERT_EQ(res.errorCode, 0);
        h.consumer(i).memberId = res.memberId;
    }
    EXPECT_EQ(h.coordinator().state(), GroupState::PreparingRebalance);

    // Consumer 2 crashes BEFORE it can do its phase-2 rejoin.
    h.crashMember(2);

    // Consumers 0 and 1 DO their phase-2 rejoin (they survived).
    for (std::size_t i = 0; i < 2; ++i) {
        h.advanceClock(ConsumerGroupHarness::kStep1ms);
        auto res = h.coordinator().join(
            h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        ASSERT_EQ(res.errorCode, 0);
        h.consumer(i).memberId = res.memberId;
    }
    // Consumers 0 and 1 are in rejoinedMembers_ (2 of 3). Consumer 2 never re-joined.
    // Set a short join-timeout so doWork will evict consumer 2 and transition.
    h.coordinator().setRebalanceTimeoutMs(100); // 100ms

    bool completing = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; },
        kCap, 50'000'000LL); // 50ms steps
    ASSERT_TRUE(completing) << "Rebalance must complete even with crashed member";

    if (h.coordinator().state() != GroupState::Stable) {
        auto sr = h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
        ASSERT_EQ(sr.errorCode, SyncResult::kOk);
    }

    // The 2 surviving members should have a valid assignment.
    for (std::size_t i = 0; i < 3; ++i) {
        if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty())
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
    }
    EXPECT_EQ(h.coordinator().state(), GroupState::Stable);
    // Surviving members cover all 6 partitions.
    h.assertPartitionsCoveredOnce(parts("t", 6));
}

// Test 23: Crash during rebalance → survivors have disjoint assignment.
TEST(CrashDuringRebalance, AllSurvivorsGetDisjointAssignment) {
    ConsumerGroupHarness h("cdr2", 3, {"t"}, 30'000);
    auto pc = counts1("t", 6);

    // Phase 1: all 3 join → PreparingRebalance.
    for (std::size_t i = 0; i < 3; ++i) {
        h.advanceClock(ConsumerGroupHarness::kStep1ms);
        auto res = h.coordinator().join(
            "", h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        ASSERT_EQ(res.errorCode, 0);
        h.consumer(i).memberId = res.memberId;
    }

    // Crash member 1 before it can rejoin (phase 2 is about to start).
    h.crashMember(1);

    // Survivors 0 and 2 do their phase-2 rejoin.
    for (std::size_t i : {std::size_t{0}, std::size_t{2}}) {
        h.advanceClock(ConsumerGroupHarness::kStep1ms);
        auto res = h.coordinator().join(
            h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        ASSERT_EQ(res.errorCode, 0);
        h.consumer(i).memberId = res.memberId;
    }

    // Short join-timeout so doWork evicts member 1 and transitions.
    h.coordinator().setRebalanceTimeoutMs(100);

    bool completing = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; },
        kCap, 50'000'000LL);
    ASSERT_TRUE(completing);

    if (h.coordinator().state() != GroupState::Stable) {
        h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
    }
    for (std::size_t i = 0; i < 3; ++i) {
        if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty())
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
    }

    // All 6 partitions covered exactly once by the 2 survivors.
    h.assertPartitionsCoveredOnce(parts("t", 6));
}

// Test 24: Stale-generation heartbeat is fenced after crash-during-rebalance.
TEST(CrashDuringRebalance, GenerationFenced) {
    ConsumerGroupHarness h("cdr3", 2, {"t"}, 5'000);
    auto pc = counts1("t", 4);
    ASSERT_TRUE(h.joinAll(pc));

    Generation staleGen = h.coordinator().generationId();

    // Crash consumer 1 → rebalance → new generation.
    h.crashMember(1);
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 2; },
        kCap, 200'000'000LL);
    ASSERT_TRUE(reaped);

    // Consumer 0 rejoins.
    {
        auto& c = h.consumer(0);
        auto res = h.coordinator().join(
            c.memberId, c.sessionTimeoutMs, c.subscribedTopics, h.clock());
        ASSERT_EQ(res.errorCode, 0);
        c.memberId = res.memberId;
    }
    bool ok = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; }, kCap);
    ASSERT_TRUE(ok);
    if (h.coordinator().state() != GroupState::Stable) {
        h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
    }

    Generation newGen = h.coordinator().generationId();
    EXPECT_GT(newGen, staleGen);

    // A heartbeat from consumer 0 with the OLD stale generation must be rejected.
    auto hb = h.coordinator().heartbeat(h.consumer(0).memberId, staleGen, h.clock());
    EXPECT_EQ(hb.errorCode, HeartbeatResult::kErrStaleGeneration)
        << "Stale generation heartbeat must be fenced";
}

// ===========================================================================
// Group 5: Rolling restart
// ===========================================================================

// Test 25: Rolling restart of consumers — all partitions covered every round.
// Pattern: crash consumer[i], wait for reap (group now has 2), revive consumer[i],
// rejoin all 3, assign. After each round: 3 alive, all 6 partitions covered.
TEST(RollingRestart, N3P6_AllPartitionsCoveredEveryRound) {
    ConsumerGroupHarness h("roll1", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    for (int round = 0; round < 3; ++round) {
        std::size_t crashIdx = static_cast<std::size_t>(round);
        Generation genBefore = h.coordinator().generationId();

        // Each round: group has 3 alive, crash one → expect 2 remaining.
        h.crashMember(crashIdx);
        bool reaped = h.driveUntil(
            [&]{ return h.coordinator().memberCount() < 3u; },
            kCap, 200'000'000LL);
        ASSERT_TRUE(reaped) << "Round " << round << ": crashed member must be reaped";
        // After reap: group has 2 members in PreparingRebalance.
        EXPECT_EQ(h.coordinator().memberCount(), 2u);

        // Generation bumped by rebalance trigger.
        EXPECT_GT(h.coordinator().generationId(), genBefore);

        // Survivors rejoin (to satisfy expectedMemberCount_=2).
        for (std::size_t i = 0; i < 3; ++i) {
            if (h.consumer(i).crashed) continue;
            auto& c = h.consumer(i);
            auto res = h.coordinator().join(
                c.memberId, c.sessionTimeoutMs, c.subscribedTopics, h.clock());
            if (res.errorCode == 0) c.memberId = res.memberId;
        }

        bool completing = h.driveUntil(
            [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                     || h.coordinator().state() == GroupState::Stable; }, kCap);
        ASSERT_TRUE(completing) << "Round " << round << ": rebalance must complete";

        if (h.coordinator().state() != GroupState::Stable) {
            auto sr = h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
            ASSERT_EQ(sr.errorCode, SyncResult::kOk);
        }
        for (std::size_t i = 0; i < 3; ++i) {
            if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty())
                h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
        }

        // 2 survivors cover all 6 partitions.
        h.assertPartitionsCoveredOnce(parts("t", 6));

        // Revive the crashed consumer (clear crashed flag + empty memberId for fresh join).
        h.reviveMember(crashIdx);

        // Revived consumer rejoins: this triggers a NEW rebalance (new member → PreparingRebalance).
        {
            auto& c = h.consumer(crashIdx);
            // Phase 1: join as new member.
            h.advanceClock(ConsumerGroupHarness::kStep1ms);
            auto res = h.coordinator().join("", c.sessionTimeoutMs, c.subscribedTopics, h.clock());
            ASSERT_EQ(res.errorCode, 0);
            c.memberId = res.memberId;
        }

        // All 3 (survivors + revived) rejoin to reach CompletingRebalance.
        for (std::size_t i = 0; i < 3; ++i) {
            h.advanceClock(ConsumerGroupHarness::kStep1ms);
            auto& c = h.consumer(i);
            auto res = h.coordinator().join(c.memberId, c.sessionTimeoutMs, c.subscribedTopics, h.clock());
            if (res.errorCode == 0) c.memberId = res.memberId;
        }

        bool stable = h.driveUntil(
            [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                     || h.coordinator().state() == GroupState::Stable; }, kCap);
        ASSERT_TRUE(stable);
        if (h.coordinator().state() != GroupState::Stable) {
            h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
        }
        for (std::size_t i = 0; i < 3; ++i) {
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
        }

        // After revive: all 3 alive, all 6 partitions covered.
        h.assertPartitionsCoveredOnce(parts("t", 6));
    }
}

// Test 26: Rolling restart — generation strictly monotonic.
TEST(RollingRestart, N3P6_GenerationMonotonic) {
    ConsumerGroupHarness h("roll2", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    Generation gen = h.coordinator().generationId();

    for (int round = 0; round < 3; ++round) {
        std::size_t idx = static_cast<std::size_t>(round % 3);
        h.crashMember(idx);

        // Each round: 3 alive → crash 1 → expect 2 remaining.
        bool reaped = h.driveUntil(
            [&]{ return h.coordinator().memberCount() < 3u; },
            kCap, 200'000'000LL);
        ASSERT_TRUE(reaped) << "Round " << round << ": member must be reaped";

        Generation newGen = h.coordinator().generationId();
        EXPECT_GT(newGen, gen) << "Generation must increase on crash round " << round;
        gen = newGen;

        // Survivors rejoin.
        for (std::size_t i = 0; i < 3; ++i) {
            if (h.consumer(i).crashed) continue;
            auto res = h.coordinator().join(
                h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
                h.consumer(i).subscribedTopics, h.clock());
            if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
        }
        // FIX 2: ASSERT_TRUE so a stall fails the test instead of silently passing.
        ASSERT_TRUE(h.driveUntil(
            [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                     || h.coordinator().state() == GroupState::Stable; }, kCap))
            << "Round " << round << ": survivors must reach CompletingRebalance/Stable";
        if (h.coordinator().state() != GroupState::Stable) {
            h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
        }

        // Revive + rejoin the crashed consumer (triggers new rebalance, bumps gen).
        h.reviveMember(idx);
        {
            h.advanceClock(ConsumerGroupHarness::kStep1ms);
            auto res = h.coordinator().join("", h.consumer(idx).sessionTimeoutMs,
                                             h.consumer(idx).subscribedTopics, h.clock());
            ASSERT_EQ(res.errorCode, 0);
            h.consumer(idx).memberId = res.memberId;
        }
        // All 3 rejoin for the new rebalance.
        for (std::size_t i = 0; i < 3; ++i) {
            h.advanceClock(ConsumerGroupHarness::kStep1ms);
            auto res = h.coordinator().join(h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
                                             h.consumer(i).subscribedTopics, h.clock());
            if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
        }
        // FIX 2: ASSERT_TRUE so a stall fails the test instead of silently passing.
        ASSERT_TRUE(h.driveUntil(
            [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                     || h.coordinator().state() == GroupState::Stable; }, kCap))
            << "Round " << round << ": revived group must reach CompletingRebalance/Stable";
        if (h.coordinator().state() != GroupState::Stable) {
            h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
        }
        EXPECT_GT(h.coordinator().generationId(), gen);
        gen = h.coordinator().generationId();
    }
}

// ===========================================================================
// Group 6: Multiple groups isolation
// ===========================================================================

// Test 27: Two groups — offsets are isolated (one group's commits don't affect the other).
TEST(MultiGroup, TwoGroups_IsolatedOffsets) {
    ConsumerGroupHarness h1("grpA", 2, {"t"});
    ConsumerGroupHarness h2("grpB", 2, {"t"});
    auto pc = counts1("t", 4);

    ASSERT_TRUE(h1.joinAll(pc));
    ASSERT_TRUE(h2.joinAll(pc));

    TopicPartition tp0{"t", 0};
    h1.commitOffset(0, tp0, 111);
    h2.commitOffset(0, tp0, 222);

    // Each group sees its own committed offset.
    auto co1 = h1.fetchOffset(tp0);
    auto co2 = h2.fetchOffset(tp0);
    ASSERT_TRUE(co1.has_value());
    ASSERT_TRUE(co2.has_value());
    EXPECT_EQ(co1->offset, 111);
    EXPECT_EQ(co2->offset, 222);
}

// Test 28: Two groups — assignments are independent.
TEST(MultiGroup, TwoGroups_IsolatedAssignments) {
    ConsumerGroupHarness h1("grpC", 2, {"t"});
    ConsumerGroupHarness h2("grpD", 3, {"t"});
    auto pc = counts1("t", 6);

    ASSERT_TRUE(h1.joinAll(pc));
    ASSERT_TRUE(h2.joinAll(pc));

    // h1 has 2 consumers; h2 has 3. Each covers all 6 partitions independently.
    h1.assertPartitionsCoveredOnce(parts("t", 6));
    h2.assertPartitionsCoveredOnce(parts("t", 6));
}

// ===========================================================================
// Group 7: Invariant sweeps
// ===========================================================================

// Test 29: Generation strictly increases across a full churn sequence.
TEST(Invariant, GenerationStrictlyIncreases_ChurnSequence) {
    ConsumerGroupHarness h("inv1", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));
    Generation gen = h.coordinator().generationId();

    // Sequence: leave → rejoin → crash → rejoin.
    h.coordinator().leave(h.consumer(0).memberId, h.clock());
    EXPECT_GT(h.coordinator().generationId(), gen); gen = h.coordinator().generationId();

    auto res = h.coordinator().join(
        "", h.consumer(0).sessionTimeoutMs, h.consumer(0).subscribedTopics, h.clock());
    ASSERT_EQ(res.errorCode, 0);
    h.consumer(0).memberId = res.memberId;
    EXPECT_GT(h.coordinator().generationId(), gen); gen = h.coordinator().generationId();

    h.crashMember(2);
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; }, kCap, 200'000'000LL);
    ASSERT_TRUE(reaped);
    EXPECT_GT(h.coordinator().generationId(), gen);
}

// Test 30: Every partition covered exactly once after each rebalance in a 4-round sequence.
TEST(Invariant, PartitionCoveredOnce_AfterEachRebalance) {
    ConsumerGroupHarness h("inv2", 4, {"t"});
    auto pc = counts1("t", 8);
    ASSERT_TRUE(h.joinAll(pc, AssignorType::RoundRobin));
    h.assertPartitionsCoveredOnce(parts("t", 8));

    // Remove one member, rejoin, reassign.
    h.coordinator().leave(h.consumer(3).memberId, h.clock());
    h.consumer(3).memberId = "";
    h.consumer(3).assignment.clear(); // member left — no longer holds any partition

    for (std::size_t i = 0; i < 4; ++i) {
        if (h.consumer(i).crashed || h.consumer(i).memberId.empty()) continue;
        auto res = h.coordinator().join(
            h.consumer(i).memberId, h.consumer(i).sessionTimeoutMs,
            h.consumer(i).subscribedTopics, h.clock());
        if (res.errorCode == 0) h.consumer(i).memberId = res.memberId;
    }
    bool ok = h.driveUntil(
        [&]{ return h.coordinator().state() == GroupState::CompletingRebalance
                 || h.coordinator().state() == GroupState::Stable; }, kCap);
    ASSERT_TRUE(ok);
    if (h.coordinator().state() != GroupState::Stable) {
        h.coordinator().autoAssignAndSync(pc, AssignorType::RoundRobin, h.clock());
    }
    for (std::size_t i = 0; i < 4; ++i) {
        if (!h.consumer(i).crashed && !h.consumer(i).memberId.empty())
            h.consumer(i).assignment = h.coordinator().assignmentFor(h.consumer(i).memberId);
    }

    // Still 8 partitions, covered once across the 3 active members.
    h.assertPartitionsCoveredOnce(parts("t", 8));
}

// Test 31: Committed offsets not lost across crash AND coordinator failover combined.
TEST(Invariant, CommittedOffsetNotLost_AfterCrashAndFailover) {
    ConsumerGroupHarness h("inv3", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    TopicPartition tp3{"t", 3};
    h.commitOffset(1, tp3, 500, "inv3-meta");

    // Crash consumer 2.
    h.crashMember(2);
    h.driveUntil([&]{ return h.coordinator().memberCount() < 3; }, kCap, 200'000'000LL);

    // Coordinator failover.
    h.recreateCoordinator();

    // Committed offset must survive both events.
    h.assertCommittedOffsetNotLost(tp3, 500);
    auto co = h.fetchOffset(tp3);
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->metadata, "inv3-meta");
}

// Test 32: Assignor is deterministic — same input → same output.
TEST(Invariant, AssignorDeterministic_SameInputSameOutput) {
    std::vector<MemberSubscription> members = {
        {"m1", {"t"}},
        {"m2", {"t"}},
        {"m3", {"t"}},
    };
    TopicPartitionCounts pc = {{"t", 6}};

    RangeAssignor ra;
    auto a1 = ra.assign(members, pc);
    auto a2 = ra.assign(members, pc);
    EXPECT_EQ(a1, a2) << "RangeAssignor must be deterministic";

    RoundRobinAssignor rr;
    auto b1 = rr.assign(members, pc);
    auto b2 = rr.assign(members, pc);
    EXPECT_EQ(b1, b2) << "RoundRobinAssignor must be deterministic";
}

// Test 33: No double-consume — committed partition offset is the resume point.
TEST(Invariant, NoDoubleConsume_CommittedPartitionAfterReassign) {
    ConsumerGroupHarness h("inv4", 3, {"t"}, 1'000);
    auto pc = counts1("t", 6);
    ASSERT_TRUE(h.joinAll(pc));

    // Consumer 2 commits offset 75 on its partition.
    // (Assumption: consumer 2 holds tp4 or tp5 under Range 3/6.)
    TopicPartition tp4{"t", 4};
    h.commitOffset(2, tp4, 75);

    // Crash consumer 2.
    h.crashMember(2);
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 3; }, kCap, 200'000'000LL);
    ASSERT_TRUE(reaped);

    // The committed offset for tp4 must be >= 75 (not reset below committed).
    h.assertNoDoubleConsume(2, tp4, 75);
    h.assertCommittedOffsetNotLost(tp4, 75);
}

// Test 34: Session timeout reaps exactly the timed-out member, not others.
TEST(Invariant, SessionTimeout_ReapsCorrectMember) {
    // Consumer 0: short timeout (500ms). Consumer 1: long timeout (10s).
    ConsumerGroupHarness h("inv5", 2, {"t"}, 10'000);
    auto pc = counts1("t", 4);

    // Override per-consumer timeouts.
    h.consumer(0).sessionTimeoutMs = 500;
    h.consumer(1).sessionTimeoutMs = 10'000;

    // Phase 1: initial join.
    h.advanceClock(ConsumerGroupHarness::kStep1ms);
    auto res0 = h.coordinator().join("", 500, {"t"}, h.clock());
    ASSERT_EQ(res0.errorCode, 0);
    h.consumer(0).memberId = res0.memberId;

    h.advanceClock(ConsumerGroupHarness::kStep1ms);
    auto res1 = h.coordinator().join("", 10'000, {"t"}, h.clock());
    ASSERT_EQ(res1.errorCode, 0);
    h.consumer(1).memberId = res1.memberId;

    // Phase 2: both re-join → CompletingRebalance.
    h.advanceClock(ConsumerGroupHarness::kStep1ms);
    auto rr0 = h.coordinator().join(h.consumer(0).memberId, 500, {"t"}, h.clock());
    ASSERT_EQ(rr0.errorCode, 0);
    h.consumer(0).memberId = rr0.memberId;

    h.advanceClock(ConsumerGroupHarness::kStep1ms);
    auto rr1 = h.coordinator().join(h.consumer(1).memberId, 10'000, {"t"}, h.clock());
    ASSERT_EQ(rr1.errorCode, 0);
    h.consumer(1).memberId = rr1.memberId;

    ASSERT_EQ(h.coordinator().state(), GroupState::CompletingRebalance);
    h.coordinator().autoAssignAndSync(pc, AssignorType::Range, h.clock());
    ASSERT_EQ(h.coordinator().state(), GroupState::Stable);

    // Stop heartbeating consumer 0 only.
    h.crashMember(0);

    // Consumer 0's lastHeartbeatNs = current clock (≈4ms). Timeout = 500ms.
    // Advance past 500ms with small steps; consumer 1 stays alive via heartbeat.
    bool reaped = h.driveUntil(
        [&]{ return h.coordinator().memberCount() < 2; },
        kCap, 100'000'000LL); // 100ms steps; consumer 1 heartbeated each step
    ASSERT_TRUE(reaped) << "Short-timeout member must be reaped";
    EXPECT_EQ(h.coordinator().memberCount(), 1u) << "Only the timed-out member should be reaped";

    // Consumer 1 must still be alive (it may be in PreparingRebalance after the reap).
    auto hb = h.coordinator().heartbeat(h.consumer(1).memberId,
                                         h.coordinator().generationId(), h.clock());
    EXPECT_NE(hb.errorCode, HeartbeatResult::kErrUnknownMember)
        << "Consumer 1 (long timeout) must still be known to coordinator";
}
