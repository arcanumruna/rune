// TDD: exhaustive bump-rule tests for PartitionAssignment / mergeChange.
// Written BEFORE implementation — all tests should FAIL until the header is filled in.

#include "rune/metadata/PartitionAssignment.h"

#include <gtest/gtest.h>
#include <vector>
#include <algorithm>

using namespace rune::metadata;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

static PartitionAssignment makeAssignment(
    int32_t leaderNode,
    std::vector<int32_t> isr,
    std::vector<int32_t> replicas,
    int32_t leaderEpoch   = 1,
    int32_t partitionEpoch = 1)
{
    return PartitionAssignment{ leaderNode, std::move(isr), std::move(replicas),
                                leaderEpoch, partitionEpoch };
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Leader change ⟹ both epochs bump
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, LeaderChange_BothEpochsBump)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2, 3}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader  = 2;               // different leader
    change.newIsr     = {2, 1, 3};       // same members, different order
    change.newReplicas = NO_REPLICA_CHANGE; // replicas unchanged

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderNode,      2);
    EXPECT_EQ(next.leaderEpoch,     cur.leaderEpoch     + 1);
    EXPECT_EQ(next.partitionEpoch,  cur.partitionEpoch  + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Replica removed ⟹ both epochs bump (forced leaderEpoch bump,
//    leader itself unchanged — uses NO_LEADER_CHANGE sentinel)
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, ReplicaRemoved_BothEpochsBump)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;  // leader field unchanged
    change.newIsr      = {1, 2};            // ISR unchanged
    change.newReplicas = {1, 2};            // removed replica 3

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderNode,      cur.leaderNode);   // leader identity preserved
    EXPECT_EQ(next.leaderEpoch,     cur.leaderEpoch     + 1);
    EXPECT_EQ(next.partitionEpoch,  cur.partitionEpoch  + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Go leaderless ⟹ both epochs bump
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, GoLeaderless_BothEpochsBump)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER;         // -1 → leaderless
    change.newIsr      = {};
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderNode,      NO_LEADER);
    EXPECT_EQ(next.leaderEpoch,     cur.leaderEpoch     + 1);
    EXPECT_EQ(next.partitionEpoch,  cur.partitionEpoch  + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Plain ISR shrink ⟹ only partitionEpoch bumps
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, IsrShrink_OnlyPartitionEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2, 3}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;
    change.newIsr      = {1, 3};            // removed 2 from ISR only
    change.newReplicas = NO_REPLICA_CHANGE; // replicas unchanged

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);          // unchanged
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch + 1);   // bumped
    EXPECT_EQ(next.isr,            (std::vector<int32_t>{1, 3}));
    EXPECT_EQ(next.replicas,       cur.replicas);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Plain ISR expand ⟹ only partitionEpoch bumps
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, IsrExpand_OnlyPartitionEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;
    change.newIsr      = {1, 2, 3};         // 3 rejoined ISR
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch + 1);
    EXPECT_EQ(next.isr,            (std::vector<int32_t>{1, 2, 3}));
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. No-op change (leader sentinel + same ISR + no replica change)
//    ⟹ neither epoch bumps
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, NoOp_NeitherEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;
    change.newIsr      = cur.isr;           // identical ISR
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Leader + ISR change simultaneously ⟹ both epochs bump
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, LeaderAndIsrChange_BothEpochsBump)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2, 3}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = 3;
    change.newIsr      = {3, 2};            // also shrunk
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderNode,      3);
    EXPECT_EQ(next.leaderEpoch,     cur.leaderEpoch     + 1);
    EXPECT_EQ(next.partitionEpoch,  cur.partitionEpoch  + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Leader unchanged (same value, not sentinel) ⟹ only ISR change bumps
//    partitionEpoch; leaderEpoch stays
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, LeaderSameValue_IsrChange_OnlyPartitionEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2, 3}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;  // explicitly no leader change
    change.newIsr      = {1, 2};
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Replica added (set grows) ⟹ only partitionEpoch bumps
//    (no forced leaderEpoch; that's only for removals)
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, ReplicaAdded_OnlyPartitionEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2}, {1, 2});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;
    change.newIsr      = {1, 2};
    change.newReplicas = {1, 2, 3};         // added replica 3

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch + 1);
    EXPECT_EQ(next.replicas,       (std::vector<int32_t>{1, 2, 3}));
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. Property: sequence of merges — leaderEpoch monotonic,
//     leaderEpoch never exceeds partitionEpoch
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, Property_MonotonicEpochsAfterSequence)
{
    PartitionAssignment state = makeAssignment(1, {1, 2, 3}, {1, 2, 3}, 0, 0);

    struct Step {
        int32_t newLeader;
        std::vector<int32_t> newIsr;
        std::vector<int32_t> newReplicas; // empty = NO_REPLICA_CHANGE
    };

    std::vector<Step> steps = {
        { NO_LEADER_CHANGE, {1, 2},    {}       },  // ISR shrink → only PE
        { 2,                {2, 1},    {}       },  // leader change → both
        { NO_LEADER_CHANGE, {2, 1, 3}, {}       },  // ISR expand → only PE
        { NO_LEADER,        {},        {}       },  // go leaderless → both
        { 1,                {1},       {1, 2, 3}},  // new leader, ISR, reps same → both (leader change)
        { NO_LEADER_CHANGE, {1},       {}       },  // no-op ISR → no-op
        { NO_LEADER_CHANGE, {1},       {1, 2}   },  // replica removed → both
    };

    int32_t prevLE = state.leaderEpoch;
    int32_t prevPE = state.partitionEpoch;

    for (auto& step : steps)
    {
        AssignmentChange change;
        change.newLeader   = step.newLeader;
        change.newIsr      = step.newIsr;
        change.newReplicas = step.newReplicas; // empty vector used as NO_REPLICA_CHANGE

        state = mergeChange(state, change);

        EXPECT_GE(state.leaderEpoch,    prevLE)  << "leaderEpoch went backwards";
        EXPECT_GE(state.partitionEpoch, prevPE)  << "partitionEpoch went backwards";
        EXPECT_LE(state.leaderEpoch,    state.partitionEpoch) << "leaderEpoch > partitionEpoch";

        prevLE = state.leaderEpoch;
        prevPE = state.partitionEpoch;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. leaderEpoch never decreases across any single merge
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, LeaderEpoch_NeverDecreases)
{
    auto cur = makeAssignment(1, {1, 2}, {1, 2, 3}, /*le=*/5, /*pe=*/10);
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;
    change.newIsr      = cur.isr;
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);
    EXPECT_GE(next.leaderEpoch, cur.leaderEpoch);
}
