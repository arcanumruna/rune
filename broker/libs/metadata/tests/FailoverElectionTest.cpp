#include <rune/metadata/FailoverElection.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

static PartitionAssignment mk(int leader, std::vector<int> isr, std::vector<int> rep, int le, int pe){
    PartitionAssignment a; a.leaderNode=leader; a.isr={isr.begin(),isr.end()};
    a.replicas={rep.begin(),rep.end()}; a.leaderEpoch=le; a.partitionEpoch=pe; return a;
}

TEST(FailoverElection, KeepsCurrentLeaderWhenStillLive) {
    ReplicaPositionTable pos;
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {1,2,3}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_FALSE(out.leaderChanged);
}

TEST(FailoverElection, ElectsMostCompleteSurvivorByCompareLog) {
    ReplicaPositionTable pos;
    pos.report({7,0,2, /*epoch*/5, /*leo*/120});
    pos.report({7,0,3, /*epoch*/5, /*leo*/100});
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);       // leader 1 dies
    ElectionInputs in{cur, {2,3}, &pos, false};      // only 2,3 live
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.leaderChanged);
    EXPECT_EQ(out.change.newLeader, 2) << "node 2 has higher LEO on same epoch";
    // leader always in ISR:
    ASSERT_FALSE(out.change.newIsr.empty());
    EXPECT_EQ(out.change.newIsr.front(), 2);
}

TEST(FailoverElection, NoIsrSurvivorGoesNoLeader) {
    ReplicaPositionTable pos;
    auto cur = mk(1, {1}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {2,3}, &pos, false};      // ISR={1}, 1 is dead
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.wentNoLeader);
    EXPECT_EQ(out.change.newLeader, NO_LEADER);
    // last-known replicas preserved:
    auto next = mergeChange(cur, out.change);
    EXPECT_EQ(next.replicas.size(), 3u);
    EXPECT_GT(next.leaderEpoch, cur.leaderEpoch) << "NO_LEADER bumps leaderEpoch";
    // RECOVERY FIX: the ISR lineage must be RETAINED (not cleared) so a returning
    // in-sync replica can restore leadership without unclean election.
    ASSERT_EQ(next.isr.size(), 1u) << "NO_LEADER retains the last ISR lineage";
    EXPECT_EQ(next.isr[0], 1);
}

// REGRESSION (dr3 rolling leader-kill: partition permanently NO_LEADER):
// A partition whose sole ISR survivor died goes NO_LEADER but RETAINS its ISR
// lineage.  When that former-ISR member rejoins (returns to liveNodes) it must be
// elected cleanly on the very next pass — proving the empty-ISR deadlock is gone.
TEST(FailoverElectionRegression, NoLeaderRecoversWhenIsrMemberRejoins) {
    ReplicaPositionTable pos;
    // Partition led by node 3 with ISR collapsed to {3}; node 3 then dies.
    auto cur = mk(3, {3}, {1,2,3}, 5, 9);
    ElectionInputs down{cur, {1,2}, &pos, false};   // 3 dead, only 1,2 live (not in ISR)
    auto gone = FailoverElection::elect(1, 2, down);
    ASSERT_TRUE(gone.wentNoLeader);
    auto offline = mergeChange(cur, gone.change);
    ASSERT_EQ(offline.leaderNode, NO_LEADER);
    ASSERT_EQ(offline.isr.size(), 1u) << "ISR lineage retained while offline";
    EXPECT_EQ(offline.isr[0], 3);

    // Node 3 rejoins (back in liveNodes, caught up). Next election must elect it.
    ElectionInputs back{offline, {1,2,3}, &pos, false};
    auto recovered = FailoverElection::elect(1, 2, back);
    EXPECT_TRUE(recovered.leaderChanged);
    EXPECT_FALSE(recovered.wentNoLeader);
    EXPECT_EQ(recovered.change.newLeader, 3) << "returning former-ISR member is re-elected (clean)";
    auto online = mergeChange(offline, recovered.change);
    EXPECT_EQ(online.leaderNode, 3);
}

TEST(FailoverElection, ReRunsOverProposedIsrHigherEpochWins) {
    ReplicaPositionTable pos;
    pos.report({7,0,2, /*epoch*/6, /*leo*/50});   // higher epoch beats higher LEO
    pos.report({7,0,3, /*epoch*/5, /*leo*/200});
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {2,3}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_EQ(out.change.newLeader, 2) << "epoch 6 > epoch 5 dominates LEO";
}

// ─── Additional edge-case coverage required by the brief ─────────────────────

// Fence-one-at-a-time: when the current leader is the ONLY ISR survivor,
// it stays leader (no-churn) even though one of the two live nodes is not
// in the ISR.
TEST(FailoverElection, KeepsCurrentLeaderWhenOnlySurvivorInIsr) {
    ReplicaPositionTable pos;
    // ISR = {1,2}; node 2 dies; node 3 is live but NOT in ISR.
    auto cur = mk(1, {1,2}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {1,3}, &pos, false};  // 2 down; 3 live but not in ISR
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_FALSE(out.leaderChanged);
    EXPECT_FALSE(out.wentNoLeader);
}

// Re-run over proposed ISR rejects an ineligible proposed member:
// a node that is live but NOT in cur.isr must not become leader.
TEST(FailoverElection, ReRunOverProposedIsrRejectsNonIsrMember) {
    ReplicaPositionTable pos;
    pos.report({7,0,4, /*epoch*/9, /*leo*/500});  // node 4 is live with great position
    // ISR = {1,2}; leader 1 dies; node 4 live but NOT in ISR.
    auto cur = mk(1, {1,2}, {1,2,3,4}, 5, 9);
    ElectionInputs in{cur, {2,4}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.leaderChanged);
    EXPECT_EQ(out.change.newLeader, 2)
        << "node 4 must not be elected — it is not in the proposed ISR (cur.isr ∩ liveNodes)";
}

// Tie-break on lowest nodeId when epoch and LEO are equal.
TEST(FailoverElection, TieBreaksOnLowestNodeId) {
    ReplicaPositionTable pos;
    pos.report({7,0,2, 5, 120});
    pos.report({7,0,3, 5, 120});  // same epoch, same LEO
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {2,3}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_EQ(out.change.newLeader, 2) << "lowest nodeId wins on tie";
}

// No-position sentinel: a member with no report is least-complete.
TEST(FailoverElection, NoPositionReportIsLeastComplete) {
    ReplicaPositionTable pos;
    // Node 3 has a position; node 2 has none → node 3 wins.
    pos.report({7,0,3, 5, 1});   // tiny position but at least has a report
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {2,3}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_EQ(out.change.newLeader, 3) << "node with a position report beats one with no report";
}

TEST(FailoverElection, UncleanOptInElectsOutOfIsrAndFlagsRecovering) {
    ReplicaPositionTable pos;
    pos.report({7,0,3, /*epoch*/4, /*leo*/70}); // 3 not in ISR but live + most complete
    auto cur = mk(1, {1}, {1,2,3}, 5, 9);         // ISR={1}, 1 dead
    ElectionInputs in{cur, {2,3}, &pos, /*allowUnclean*/true};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.recovering);
    EXPECT_EQ(out.change.newLeader, 3);
    ASSERT_EQ(out.change.newIsr.size(), 1u);
    EXPECT_EQ(out.change.newIsr[0], 3);
}
TEST(FailoverElection, UncleanRefusedWhenLeaderAlreadyExists) {
    ReplicaPositionTable pos;
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {1,2,3}, &pos, /*allowUnclean*/true};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_FALSE(out.leaderChanged); // ELECTION_NOT_NEEDED
    EXPECT_FALSE(out.recovering);
}

// ISR shrink without leader change must NOT bump leaderEpoch.
TEST(FailoverElection, IsrShrinkWithoutLeaderChangeNoLeaderEpochBump) {
    ReplicaPositionTable pos;
    pos.report({7,0,1, 5, 200});  // current leader still has highest position
    // Leader 1 is still live; node 3 is down.
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {1,2}, &pos, false};  // node 3 down
    auto out = FailoverElection::elect(7, 0, in);
    // Leader stays (1 is live and in ISR), so leaderChanged==false.
    // ISR shrinks from {1,2,3} to {1,2} — this is handled by the controller
    // at a higher level; elect() itself just reports no leader change.
    EXPECT_FALSE(out.leaderChanged);
    EXPECT_FALSE(out.wentNoLeader);
    // Applying the resulting (no-op) change must not bump leaderEpoch.
    auto next = mergeChange(cur, out.change);
    EXPECT_EQ(next.leaderEpoch, cur.leaderEpoch);
}
