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
// 8. Leader sentinel (NO_LEADER_CHANGE) + ISR change ⟹ only partitionEpoch bumps;
//    leaderEpoch stays.  This is the sentinel/unchanged path.
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, LeaderSentinel_IsrChange_OnlyPartitionEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2, 3}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = NO_LEADER_CHANGE;  // sentinel: leave leader field untouched
    change.newIsr      = {1, 2};
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch + 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8b. Leader explicit same value (NOT sentinel) + ISR change ⟹
//     only partitionEpoch bumps; leaderEpoch stays.
//     Re-asserting the same leader is not a leader *change* — no leaderEpoch bump.
//     This test targets the branch: change.newLeader != NO_LEADER_CHANGE
//       but change.newLeader == cur.leaderNode.
// ─────────────────────────────────────────────────────────────────────────────
TEST(PartitionAssignmentBumpRules, LeaderExplicitSameValue_IsrChange_OnlyPartitionEpochBumps)
{
    auto cur = makeAssignment(/*leader=*/1, {1, 2, 3}, {1, 2, 3});
    AssignmentChange change;
    change.newLeader   = 1;                 // explicit equal value, NOT NO_LEADER_CHANGE (-2)
    change.newIsr      = {1, 2};            // ISR change — ensures partitionEpoch bumps
    change.newReplicas = NO_REPLICA_CHANGE;

    auto next = mergeChange(cur, change);

    // Re-asserting the same leader must NOT bump leaderEpoch.
    EXPECT_EQ(next.leaderNode,     1);
    EXPECT_EQ(next.leaderEpoch,    cur.leaderEpoch);          // NO bump
    EXPECT_EQ(next.partitionEpoch, cur.partitionEpoch + 1);   // ISR change bumps PE
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

// ─────────────────────────────────────────────────────────────────────────────
// Authentication Task 18 (seeded stress, seed 0): a DDL-churned AssignmentMap probed forever.  remove() left tombstones
// that never counted toward the load factor, so create / drop churn over distinct topic ids ate every EMPTY slot and
// get() of an absent key then looped for good on the control thread (every DDL answer stalled behind it).
// ─────────────────────────────────────────────────────────────────────────────
#include "rune/metadata/AssignmentMap.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

TEST(AssignmentMapRegression, ChurnOverDistinctTopicIdsNeverExhaustsEmptySlots) {
    auto done = std::make_shared<std::atomic<bool>>(false);
    std::thread([done] {
        AssignmentMap m;
        // a handful of live entries; thousands of create-then-drop cycles over fresh ids (DDL churn)
        for (int32_t id = 1; id <= 5000; ++id) {
            m.put(id, 0, PartitionAssignment{});
            if (id > 3) m.remove(id - 3, 0);
        }
        EXPECT_EQ(m.size(), 3);
        for (int32_t id = 6000; id < 6100; ++id) EXPECT_EQ(m.get(id, 0), nullptr);   // absent keys must terminate
        for (int32_t id = 4998; id <= 5000; ++id) EXPECT_NE(m.get(id, 0), nullptr);
        done->store(true);
    }).detach();
    for (int i = 0; i < 100 && !done->load(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(done->load()) << "AssignmentMap::get() of an absent key never returned (tombstones exhausted every EMPTY slot)";
}

TEST(AssignmentMapRegression, RemoveKeepsEveryOtherEntryReachable) {
    AssignmentMap m;
    for (int32_t id = 1; id <= 200; ++id) m.put(id, 0, PartitionAssignment{});
    for (int32_t id = 1; id <= 200; id += 2) m.remove(id, 0);
    for (int32_t id = 1; id <= 200; ++id) EXPECT_EQ(m.get(id, 0) != nullptr, id % 2 == 0) << id;
    int n = 0;
    m.forEach([&](int32_t, int32_t, const PartitionAssignment&) { ++n; });
    EXPECT_EQ(n, 100);
}

#include <map>
#include <random>

TEST(AssignmentMapRegression, RandomPutRemoveMatchesAStdMap) {
    std::mt19937 rng(7);
    AssignmentMap m;
    std::map<std::pair<int32_t, int32_t>, int32_t> model;
    for (int step = 0; step < 20000; ++step) {
        const int32_t t = static_cast<int32_t>(rng() % 300), p = static_cast<int32_t>(rng() % 3);
        if (rng() % 2) { PartitionAssignment pa; pa.leaderNode = step; m.put(t, p, pa); model[{t, p}] = step; }
        else { m.remove(t, p); model.erase({t, p}); }
        if (step % 97 == 0) {
            ASSERT_EQ(m.size(), static_cast<int>(model.size()));
            for (int32_t tt = 0; tt < 300; ++tt) for (int32_t pp = 0; pp < 3; ++pp) {
                const auto* got = m.get(tt, pp);
                const auto it = model.find({tt, pp});
                ASSERT_EQ(got != nullptr, it != model.end()) << step << " " << tt << "," << pp;
                if (got) ASSERT_EQ(got->leaderNode, it->second);
            }
        }
    }
}
