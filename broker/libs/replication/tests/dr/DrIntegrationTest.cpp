// ---------------------------------------------------------------------------
// DrIntegrationTest.cpp — E2 / DUR-1 disaster-recovery integration suite.
//
// Deterministic, in-process, seconds per suite (see DrHarness.h for what is
// real vs modelled).  Periodic fsync is OFF (A3 default) unless a test says
// otherwise: durability comes from replication, so the invariant every test
// checks is "an acks=all-ACKED record is never lost while a majority of
// replicas keeps its page cache" plus convergence (every live replica holds
// the leader's records, in order, no duplicates).
//
// Suite names: DrIntegration (scenarios); DrIntegrationRegression (a scenario
// that pinned a bug — see the test comment for the fix it guards).
// ---------------------------------------------------------------------------
#include "DrHarness.h"

using namespace rune::replication;
using namespace rune::replication::dr;

namespace {

constexpr int kCap = 3000;   // hard cap for every drive loop (steps of 1 ms)

void expectAllConvergedAndAcked(DrCluster& c, const char* where) {
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap))
        << where << ": no convergence\n" << c.dump();
    for (NodeId id = 1; id <= 3; ++id) {
        if (!c.up(id)) continue;
        c.expectAckedPresent(id, where);
        c.expectNoDuplicates(id);
    }
}

} // namespace

// Sanity: the harness replicates, commits and converges.
TEST(DrIntegration, HappyPath_ProduceAckConverge) {
    DrCluster c(3);
    c.start();
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).isLeaderComplete(); }));
    EXPECT_EQ(c.produceAcked(25), 25);
    expectAllConvergedAndAcked(c, "happy");
    EXPECT_EQ(c.ids(1).size(), 25u);
}

// Unclean shutdown (power loss) of a follower: its never-fsynced tail is gone
// on restart (recovery truncates to what is on disk) and it re-fetches it.
TEST(DrIntegration, UncleanShutdown_FollowerPowerLoss_TailRefetched) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));

    c.crash(3, CrashKind::PowerLoss);
    c.restart(3);
    EXPECT_LT(c.rep(3).logEndOffset(), 30) << "nothing was fsynced: the tail must be lost";
    c.applyMetadata(3);
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "follower power loss");
}

// A torn / partial batch at the tail (power loss mid-write): recovery drops
// it (CRC / length check) and keeps every whole durable batch before it.
TEST(DrIntegration, TornBatchAtTail_RecoveryDropsIt) {
    DrConfig cfg;
    cfg.followerFlushIntervalNs = 100 * kStepNs; // some of the follower's log is fsynced
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(150);                                  // the follower flush makes those 20 durable
    ASSERT_EQ(c.produceAcked(10), 10);           // acked well inside the next flush window
    c.crash(2, CrashKind::PowerLossTorn, /*tornBytes=*/37);
    c.restart(2);
    const Offset leo = c.rep(2).logEndOffset();
    EXPECT_GE(leo, 20) << "fsynced batches must survive";
    EXPECT_LT(leo, 30) << "unsynced batches must be gone";
    EXPECT_EQ(c.ids(2).size(), static_cast<std::size_t>(leo)) << "torn tail must be dropped whole";
    c.applyMetadata(2);
    expectAllConvergedAndAcked(c, "torn tail");
}

// Leader power loss with an unflushed tail: every acked record is on the
// surviving majority's page cache; the new leader serves it and the restarted
// old leader re-fetches what it lost.
TEST(DrIntegration, LeaderCrash_UnflushedTail_PowerLoss_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(40), 40);
    c.crash(1, CrashKind::PowerLoss);
    const NodeId nl = c.failover();
    ASSERT_GT(nl, 1);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));
    EXPECT_EQ(c.produceAcked(15), 15);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "leader power loss");
}

// Leader crash (process kill) holding an UNACKED tail of its own epoch: on
// rejoin as a follower the KIP-101 probe truncates exactly that tail.
TEST(DrIntegration, LeaderCrash_UnackedTail_TruncatedOnRejoin) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(20), 20);
    c.isolate(1);
    ASSERT_EQ(c.produce(5), 5);                  // appended, never replicated → never acked
    c.run(3);
    c.crash(1, CrashKind::ProcessKill);
    EXPECT_EQ(c.acked().size(), 20u);
    const NodeId nl = c.failover();
    ASSERT_GT(nl, 1);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));
    EXPECT_EQ(c.produceAcked(10), 10);
    c.restart(1);
    c.healAll();
    ASSERT_EQ(c.rep(1).logEndOffset(), 25) << "process kill keeps the page cache";
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "unacked tail");
    EXPECT_EQ(c.ids(1).size(), 30u);
}

// ISR shrink / expand under faults: a lagging follower leaves the ISR, the
// partition keeps committing with min-ISR; below min-ISR acks=all is refused;
// healed followers re-enter the ISR only once caught up.
TEST(DrIntegration, IsrShrinkExpand_UnderFaults_MinIsrGates) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_TRUE(c.driveUntil([&] { return !c.leaderIsrContains(3); }, kCap)) << "ISR never shrank";
    EXPECT_EQ(c.produceAcked(10), 10) << "ISR {1,2} >= minIsr 2 still commits";
    c.isolate(2);
    ASSERT_TRUE(c.driveUntil([&] { return !c.leaderIsrContains(2); }, kCap));
    EXPECT_EQ(c.produce(1), 0) << "below min-ISR an acks=all produce is refused";
    c.healAll();
    c.dropNext(Kind::FetchResp, 7);              // lossy catch-up
    c.dropNext(Kind::FetchReq, 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.leaderIsrContains(2) && c.leaderIsrContains(3); }, kCap))
        << "healed followers never re-entered the ISR";
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "isr shrink/expand");
}

// KIP-101 epoch truncation across two failovers with a divergent unacked
// tail on BOTH deposed leaders (epochs 1 and 2).
TEST(DrIntegration, Kip101_DivergentTailsAcrossTwoFailovers) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);

    c.isolate(1);
    ASSERT_EQ(c.produce(3), 3);                  // epoch-1 tail on node 1 only
    c.run(2);
    c.crash(1, CrashKind::ProcessKill);
    ASSERT_EQ(c.failover(), 2);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).isLeaderComplete(); }, kCap));
    c.restart(1);
    c.healAll();
    c.applyMetadata(1);
    EXPECT_EQ(c.produceAcked(5), 5);

    c.isolate(2);
    ASSERT_EQ(c.produce(2), 2);                  // epoch-2 tail on node 2 only
    c.run(2);
    c.crash(2, CrashKind::ProcessKill);
    const NodeId nl = c.failover();
    ASSERT_TRUE(nl == 1 || nl == 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(2);
    c.healAll();
    c.applyMetadata(2);
    expectAllConvergedAndAcked(c, "kip-101 two failovers");
    EXPECT_EQ(c.ids(nl).size(), 20u) << "both divergent tails must be truncated";
}

// Producer-state rebuild after a crash: the new leader rebuilds the idempotent
// producer's state from ITS log.  A retry of a stored batch is a Duplicate
// (never an OutOfOrderSequence); a retry of a lost (unacked) batch is appended
// once; nothing is ever stored twice.
TEST(DrIntegration, ProducerStateRebuildAfterCrash_NoDupNoOos) {
    DrCluster c(3);
    c.start();
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).isLeaderComplete(); }));
    constexpr int64_t kPid = 4242;
    uint64_t id = 1'000'000;
    for (int32_t s = 0; s < 20; ++s)
        ASSERT_EQ(c.produceIdempotent(kPid, 0, s, id++).status, IdemResult::Status::Appended);
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0; }, kCap));
    ASSERT_EQ(c.acked().size(), 20u);

    c.isolate(1);
    for (int32_t s = 20; s < 24; ++s)            // stored on node 1 only, never acked
        ASSERT_EQ(c.produceIdempotent(kPid, 0, s, 2'000'000 + s).status, IdemResult::Status::Appended);
    c.run(2);
    c.crash(1, CrashKind::ProcessKill);
    const NodeId nl = c.failover();
    ASSERT_GT(nl, 1);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));

    // The client retries its whole in-flight window (seq 16..23) in order.
    for (int32_t s = 16; s < 20; ++s) {
        const auto r = c.produceIdempotent(kPid, 0, s, 1'000'000 + s);
        EXPECT_EQ(r.status, IdemResult::Status::Duplicate) << "stored seq " << s;
    }
    for (int32_t s = 20; s < 24; ++s) {
        const auto r = c.produceIdempotent(kPid, 0, s, 2'000'000 + s);
        EXPECT_EQ(r.status, IdemResult::Status::Appended) << "lost seq " << s;
    }
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0; }, kCap));

    // Old leader returns (power loss this time on the NEW leader's follower
    // set is not needed): rejoin, then leadership moves back to it.  Its
    // rebuilt producer state must still answer stored retries as Duplicate.
    c.restart(1);
    c.healAll();
    c.applyMetadata(1);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.moveLeader(1);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).isLeaderComplete(); }, kCap)) << c.dump();
    for (int32_t s = 18; s < 24; ++s) {
        const auto r = c.produceIdempotent(kPid, 0, s, 9);
        EXPECT_EQ(r.status, IdemResult::Status::Duplicate) << "after leader move, seq " << s;
    }
    EXPECT_EQ(c.produceIdempotent(kPid, 0, 24, 2'000'024).status, IdemResult::Status::Appended);
    expectAllConvergedAndAcked(c, "psm rebuild");
    EXPECT_EQ(c.ids(1).size(), 25u) << "every sequence stored exactly once";
}

// Stale-leader fencing (split brain): an isolated old leader keeps believing
// it leads epoch 1.  Nothing it appends is ever acked; once it learns the new
// epoch it becomes a follower and its stale tail is truncated.
TEST(DrIntegration, StaleLeaderFencing_SplitBrain_NoStaleAck) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(1);
    // The controller lost node 1's heartbeat: it elects from the majority side
    // and its metadata never reaches node 1, which still leads epoch 1.
    ASSERT_EQ(c.failover({1}), 2);
    ASSERT_EQ(c.rep(1).role(), Role::Leader);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).isLeaderComplete(); }, kCap));
    EXPECT_EQ(c.produceAcked(10), 10);

    // Writes into the stale leader (a client that has not refreshed metadata).
    const std::size_t ackedBefore = c.acked().size();
    rune::protocol::RecordBatchBuilder b(0, 0);
    const uint64_t staleId = 777777;
    char v[8]; std::memcpy(v, &staleId, 8);
    b.append({}, std::string_view(v, 8), 0);
    const auto bytes = b.build();
    Offset base = -1;
    c.node(1).log->appendNoSyncRestamp(bytes.data(), bytes.size(), 0, base);
    c.rep(1).onLeaderLogAppended();
    c.run(200);
    EXPECT_LE(c.rep(1).highWatermark(), base) << "a stale leader must never commit its write";
    EXPECT_EQ(c.acked().size(), ackedBefore);

    c.healAll();
    c.applyMetadata(1);                          // node 1 learns epoch 2
    expectAllConvergedAndAcked(c, "split brain");
    const auto ids = c.ids(1);
    EXPECT_EQ(std::count(ids.begin(), ids.end(), staleId), 0) << "stale write must be truncated";
}

// Kill-ALL (process kill of every node, page caches survive) with acks=all:
// no acknowledged record is lost.  In-flight (unacked) produces may be lost.
TEST(DrIntegration, KillAll_ProcessKill_AcksAll_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(50), 50);
    ASSERT_EQ(c.produce(5), 5);                  // in flight
    c.step();
    for (NodeId id = 1; id <= 3; ++id) c.crash(id, CrashKind::ProcessKill);
    for (NodeId id = 1; id <= 3; ++id) c.restart(id);
    const NodeId nl = c.failover();
    ASSERT_GT(nl, 0);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "kill-all");
}

// Kill-all where a MINORITY lost power (its page cache is gone) and the rest
// were process-killed: still no acked loss — a majority's page cache survives.
TEST(DrIntegration, KillAll_MinorityPowerLoss_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(50), 50);
    c.crash(1, CrashKind::PowerLoss);
    c.crash(2, CrashKind::ProcessKill);
    c.crash(3, CrashKind::ProcessKill);
    for (NodeId id = 1; id <= 3; ++id) c.restart(id);
    const NodeId nl = c.failover();
    ASSERT_GT(nl, 0);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "kill-all minority power loss");
}

// Outside the guarantee: a simultaneous power loss of EVERY node with
// periodic fsync off loses whatever was not yet sealed/fsynced.  What must
// still hold: recovery yields a consistent cluster (converged, gap-free, no
// duplicates) that accepts and commits new writes.
TEST(DrIntegration, TotalPowerLoss_OutOfGuarantee_StillConsistent) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    for (NodeId id = 1; id <= 3; ++id) c.crash(id, CrashKind::PowerLoss);
    for (NodeId id = 1; id <= 3; ++id) c.restart(id);
    const NodeId nl = c.failover();
    ASSERT_GT(nl, 0);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).isLeaderComplete(); }, kCap));
    ASSERT_EQ(c.produce(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0 && c.converged(); }, kCap));
    for (NodeId id = 1; id <= 3; ++id) c.expectNoDuplicates(id);
}

// ---------------------------------------------------------------------------
// Leadership handed to a replica that is BEHIND the committed frontier while
// still in the controller ISR (the controller's ISR only changes on a leader
// change, so a follower that lagged stays in it).  Kafka parity: a leader never
// takes records it did not hold when elected.  The deficient leader must never
// complete (no write, no truncation directive: the survivors keep the committed
// records); once the controller moves leadership to a complete replica the
// cluster converges with no acked loss.  (Before the redesign it reverse-
// reconciled the survivors' records — the heuristic layer this replaces.)
// ---------------------------------------------------------------------------
TEST(DrIntegrationRegression, LeaderMovedToLaggingIsrReplica_StaysIncompleteUntilReelected) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);                                // 3 lags; controller ISR still {1,2,3}
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    ASSERT_EQ(c.rep(2).highWatermark(), 30);
    c.moveLeader(3);                             // preferred-leader move (3 is in the ISR)
    c.healAll();
    c.run(300);
    EXPECT_FALSE(c.rep(3).acceptsProduce()) << "a deficient leader completed\n" << c.dump();
    EXPECT_EQ(c.produce(5), 0);
    EXPECT_EQ(c.rep(3).logEndOffset(), 10);
    EXPECT_EQ(c.rep(1).logEndOffset(), 30) << "a survivor truncated committed records";
    EXPECT_EQ(c.rep(2).logEndOffset(), 30) << "a survivor truncated committed records";
    c.moveLeader(1);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "lagging leader");
}

// KIP-966: a replica restarted after an UNCLEAN shutdown (power loss: its
// unflushed tail — acked records included — is gone) stays in the controller
// ISR.  Leadership handed to it must not lose the acked records the others
// still hold: it never completes (their fetches are ahead of its lineage), and
// the controller's next move to a complete replica loses nothing.
TEST(DrIntegrationRegression, UncleanRestartedReplicaMadeLeader_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.run(20);
    ASSERT_EQ(c.rep(2).highWatermark(), 30);
    c.crash(3, CrashKind::PowerLoss);
    c.restart(3);
    c.applyMetadata(3);
    c.moveLeader(3);
    c.run(300);
    EXPECT_FALSE(c.rep(3).acceptsProduce()) << c.dump();
    EXPECT_EQ(c.rep(1).logEndOffset(), 30);
    EXPECT_EQ(c.rep(2).logEndOffset(), 30);
    c.moveLeader(2);
    ASSERT_TRUE(c.waitWritable(kCap)) << "leader never became writable";
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "unclean restarted leader");
}

// Power loss of the LEADER after an epoch change: its leader-epoch checkpoint
// (tmp+fsync+rename) survives but the log tail it describes does not.  On
// restart the epoch cache must be cut back to the recovered log end — a stale
// entry past the LEO makes the next becomeLeader's epoch assignment a silent
// no-op (non-monotonic start), corrupting KIP-101 answers.
TEST(DrIntegrationRegression, PowerLoss_EpochCacheBeyondRecoveredLeo_TruncatedOnRecovery) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.moveLeader(2);                              // epoch 2 starts at 10 on node 2
    ASSERT_EQ(c.produceAcked(10), 10);
    c.crash(2, CrashKind::PowerLoss);            // checkpoint {1:0, 2:10} survives, log does not
    c.restart(2);
    const Offset leo = c.rep(2).logEndOffset();
    ASSERT_LT(leo, 10);
    const auto& cache = c.rep(2).leaderEpochCacheForTest();
    EXPECT_LE(cache.startOffsetFor(cache.latestEpoch()), leo)
        << "epoch cache holds an epoch starting past the recovered log end";
    const NodeId nl = c.failover();
    ASSERT_TRUE(nl == 1 || nl == 3);
    c.applyMetadata(2);
    ASSERT_TRUE(c.waitWritable(kCap));
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "epoch cache past leo");
    c.moveLeader(2);
    ASSERT_TRUE(c.waitWritable(kCap));
    EXPECT_EQ(c.rep(2).leaderEpochCacheForTest().latestEpoch(), static_cast<int32_t>(c.epoch()))
        << "becomeLeader's epoch must be recorded";
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "epoch cache past leo, leader again");
}

// KIP-966 (data plane): a follower restarted after an UNCLEAN shutdown lost
// records it had acknowledged (they counted toward the HW).  It must leave the
// ISR at once — it no longer holds the committed prefix, so it can neither
// vouch for the HW nor satisfy min-ISR — and re-enter only once caught up.
// Without the fix it stayed in the ISR forever (it keeps fetching, and the
// offset lag stays far below replicaLagMaxOffset), so min-ISR kept accepting
// acks=all writes while a single replica held the committed prefix.
TEST(DrIntegrationRegression, FollowerRestartAfterUncleanShutdown_OutOfIsrUntilCaughtUp) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    c.run(20);
    ASSERT_TRUE(c.converged());
    c.crash(2, CrashKind::PowerLoss);
    c.restart(2);
    c.cut(1, 2);                                 // 2's fetches reach the leader; nothing comes back
    c.applyMetadata(2);
    c.run(100);
    EXPECT_LT(c.rep(2).logEndOffset(), 30);
    EXPECT_FALSE(c.leaderIsrContains(2)) << "unclean-restarted follower still in the ISR\n" << c.dump();
    c.isolate(3);
    ASSERT_TRUE(c.driveUntil([&] { return !c.leaderIsrContains(3); }, kCap));
    EXPECT_EQ(c.produce(1), 0) << "only the leader holds the committed prefix: min-ISR must refuse";
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.leaderIsrContains(2) && c.leaderIsrContains(3); }, kCap))
        << "caught-up followers never re-entered the ISR\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "unclean follower isr");
}

// KIP-966 ELR double fault (controller side): the only replicas holding a
// committed range are the leader and a follower that then loses power; the
// third replica lagged (out of the data-plane ISR, still in the controller
// ISR — the controller ISR only changes on a leader change).  The leader is
// then killed (process kill: its page cache survives).  Kafka's ELR keeps the
// partition leaderless until the old leader returns; Rune elects a survivor
// that lacks the range.  (The elected survivor was in the controller ISR but
// does not hold the committed prefix; the data plane never counts a replica it
// has not validated, and a deficient leader never completes while a validated
// quorum is missing — but only the controller can avoid electing it.  E1b also
// covers the review's M2: an
// unclean follower carried across a leadership change (the new leader has no
// prior position for it, so the regression eviction cannot fire), and RF >= 4
// with an even ISR, where the completeness quorum and the commit quorum need
// not overlap.)
TEST(DrIntegration, DISABLED_Kip966_DoubleFault_UncleanFollowerPlusLeaderKill) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);           // on {1,2} only
    c.run(20);
    c.crash(2, CrashKind::PowerLoss);
    c.restart(2);
    c.cut(1, 2);
    c.applyMetadata(2);
    c.run(10);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    (void)c.failover();
    (void)c.waitWritable(kCap);
    (void)c.produce(5);
    c.run(50);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "kip-966 double fault");
}

// The deficient-leader case under a lossy, delayed network: the deficient
// leader stays incomplete; after the controller moves leadership, dropped
// fetch answers / fetches and DivergingEpoch rounds are re-driven by the fetch
// cadence and the owed-answer path — no acked loss, no divergence.
TEST(DrIntegration, LeaderMovedToLaggingIsrReplica_LossyDelayedLinks) {
    DrConfig cfg;
    cfg.deliveryDelaySteps = 2;
    cfg.replicaLagMaxMs    = 500;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(25), 25);
    c.run(30);
    c.moveLeader(3);
    c.healAll();
    c.dropNext(Kind::FetchResp, 4);
    c.dropNext(Kind::FetchReq, 3);
    c.run(300);
    EXPECT_FALSE(c.rep(3).acceptsProduce());
    EXPECT_EQ(c.rep(1).logEndOffset(), 35);
    c.moveLeader(2);
    c.dropNext(Kind::FetchResp, 4);
    c.dropNext(Kind::FetchReq, 2);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "lossy lagging leader");
    EXPECT_GT(c.framesLost(), 0u);
}

// Power loss across segment rolls: a roll seals (fsyncs) the outgoing
// segment, so every sealed segment survives; only the active segment's
// unflushed tail is lost — never a gap in the offsets.
TEST(DrIntegration, PowerLoss_AcrossSegmentRolls_SealedSegmentsSurvive) {
    DrConfig cfg;
    cfg.segmentRollBytes = 2048;                 // ~25 single-record batches per segment
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(120), 120);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    const auto bases = c.node(2).log->segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "test needs several rolled segments";
    const Offset activeBase = bases.back();
    c.crash(2, CrashKind::PowerLoss);
    c.restart(2);
    EXPECT_GE(c.rep(2).logEndOffset(), activeBase) << "a sealed (fsynced) segment was lost";
    EXPECT_LT(c.rep(2).logEndOffset(), 120);
    EXPECT_EQ(c.ids(2).size(), static_cast<std::size_t>(c.rep(2).logEndOffset())) << "offset gap";
    c.applyMetadata(2);
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "segment rolls");
}

// ===========================================================================
// Fix round 1 (review) — each pinned by a reproduction first.
// ===========================================================================

// B1: an INCOMPLETE leader (completeness gate still holding: it has not yet
// proven it holds the committed frontier) must refuse EVERY append — plain
// (pid -1) produce, idempotent produce, control batch (txn marker), acks=all
// internal-store append — with a retriable NotLeader.  The worker gate used to
// check role() only, so a pid -1 batch landed on the deficient leader at
// [10,15) and the reconciled survivors' [15,30) was grafted on top (divergence
// + acked loss).  Under the Kafka-parity redesign the deficient leader never
// completes at all; the controller moves leadership on.  The harness produces through the REAL gate
// (PartitionReplica::acceptsProduce, the predicate Worker / ClientGateway use).
TEST(DrIntegrationRegression, IncompleteDeficientLeaderRefusesEveryAppend) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);
    c.healAll();
    ASSERT_FALSE(c.rep(3).isLeaderComplete());
    EXPECT_FALSE(c.rep(3).acceptsProduce());
    EXPECT_EQ(c.produce(5), 0) << "pid -1 produce accepted by an incomplete leader";
    EXPECT_EQ(c.produceIdempotent(77, 0, 0, 5000).status, IdemResult::Status::Refused);
    rune::protocol::RecordBatchBuilder mb(0, 0);
    mb.setControl(true);
    mb.setTransactional(true);
    mb.setProducerId(77);
    mb.append(std::string_view("\0\0\0\0", 4), std::string_view("\0\0\0\0\0\0", 6), 0);
    const auto marker = mb.build();
    EXPECT_EQ(c.rep(3).appendControlBatch(marker), PartitionReplica::kAppendNotLeader);
    const uint8_t k[1] = {1}, v[1] = {2};
    EXPECT_EQ(c.rep(3).appendKeyed(k, v, /*acksAll=*/true), PartitionReplica::kAppendNotLeader);
    EXPECT_EQ(c.rep(3).logEndOffset(), 10) << "nothing may land before completion";
    c.run(300);                                  // it never completes (deficient)
    EXPECT_FALSE(c.rep(3).acceptsProduce());
    c.moveLeader(1);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "incomplete leader gate");
}




// Review: "probe-fallback plain fetch defeats lineage checks" / OOR <-> probe
// loops.  Divergence is resolved ON THE FETCH PATH (lastFetchedEpoch +
// DivergingEpoch): no replica ever sends an OffsetsForLeaderEpoch probe or a
// reverse-reconcile pull, across a KIP-279 two-epoch divergence.
TEST(DrIntegrationRegression, NoSideProbes_DivergenceResolvedOnFetchPath) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(8), 8);
    c.isolate(1);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(2);
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);
    c.run(2);
    c.moveLeader(1);
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "no side probes");
    for (NodeId id = 1; id <= 3; ++id) {
        EXPECT_EQ(c.sentBy(id, Kind::EpochReq), 0u) << "node " << id;
        EXPECT_EQ(c.sentBy(id, Kind::ReconReq), 0u) << "node " << id;
    }
}

// L2: production seals asynchronously (Log::setDeferSealSync — the worker's io
// backend fsyncs the sealed segment later).  A power loss before that fsync
// completes loses the sealed segment's unflushed bytes too; recovery must
// still yield a GAP-FREE log (never a later segment's base as the LEO over
// missing offsets) and the replica must re-fetch and converge.
// KNOWN BUG (E1d, not fixed in round 1): Log recovery keeps every segment
// file, so after this power loss the replica reports LEO = the last segment's
// base (108) while holding 0 records — it claims offsets it does not have.
// A structural "inter-segment gap => truncate" rule is NOT safe: compaction
// legitimately removes a sealed segment's last records (gap to the next base),
// and the cleaner checkpoint can trail its renames.  Needs a durable recovery
// point (or a cleaner that always preserves each segment's end offset).
TEST(DrIntegrationRegression, DISABLED_PowerLoss_AsyncSealNotCompleted_NoOffsetGap) {
    DrConfig cfg;
    cfg.segmentRollBytes = 2048;
    cfg.deferSealSync    = true;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(120), 120);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    ASSERT_GE(c.node(2).log->segmentBaseOffsets().size(), 3u);
    c.crash(2, CrashKind::PowerLoss);
    c.restart(2);
    EXPECT_EQ(c.ids(2).size(), static_cast<std::size_t>(c.rep(2).logEndOffset()))
        << "offset gap after recovery: LEO " << c.rep(2).logEndOffset() << " but "
        << c.ids(2).size() << " records";
    c.applyMetadata(2);
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "async seal power loss");
}

// ===========================================================================
// Kafka-parity divergence redesign (review round 2) — every review repro
// (Repro.cpp, rev2-Repro2.cpp, rev2-Repro3.cpp) pinned first, then fixed.
//
// Model under test: every batch carries its real partitionLeaderEpoch; a
// follower fetches with lastFetchedEpoch and the leader validates EVERY fetch
// against its own epoch lineage (DivergingEpoch → KIP-279 truncation loop); a
// fetch is credited (ISR / HW / completeness) only once it passed validation.
// A leader never takes records it did not hold when elected: a deficient
// leader (one that lacks committed records an ISR member holds) never becomes
// complete — it refuses writes and never directs a truncation — until the
// controller hands leadership to a complete replica (E1b covers making the
// controller avoid such an election).
// ===========================================================================
namespace {

std::size_t positionsDiffering(const std::vector<uint64_t>& a, const std::vector<uint64_t>& b) {
    const std::size_t n = std::min(a.size(), b.size());
    std::size_t diff = 0;
    for (std::size_t i = 0; i < n; ++i) if (a[i] != b[i]) ++diff;
    return diff;
}

} // namespace

// Repro.cpp #1: the committed range the moved-to replica lacks was written
// under an epoch it never held.  It must stay incomplete (no write, no
// truncation directive — the survivors keep every committed record) instead
// of adopting the survivors' prefix; once the controller moves leadership to a
// complete replica everything converges with no acked loss.
TEST(DrIntegrationRegression, DeficientLeaderMissingWholeEpoch_StaysIncompleteNoTruncation) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    c.moveLeader(2);                             // epoch 2 on {1,2}; 3 never sees it
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);                             // lagging ISR replica, epoch 3
    c.healAll();
    c.run(300);
    EXPECT_FALSE(c.rep(3).acceptsProduce()) << "a deficient leader must never complete\n" << c.dump();
    EXPECT_EQ(c.produce(5), 0) << "an incomplete leader accepted a write";
    EXPECT_EQ(c.rep(3).logEndOffset(), 10) << "the leader took records it did not hold when elected";
    EXPECT_EQ(c.rep(1).logEndOffset(), 30) << "a survivor truncated committed records";
    EXPECT_EQ(c.rep(2).logEndOffset(), 30) << "a survivor truncated committed records";
    c.moveLeader(1);                             // controller: a complete replica leads
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(25), 25);
    expectAllConvergedAndAcked(c, "missing whole epoch");
}

// Repro.cpp #2: followers ahead of the deficient leader by different amounts
// (one holds an extra uncommitted tail).  Same contract.
TEST(DrIntegrationRegression, DeficientLeaderTwoFollowersAheadDifferentAmounts_NoLossNoDivergence) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.cut(1, 2);
    (void)c.produce(5);                          // uncommitted tail on node 1 only
    c.run(5);
    c.moveLeader(3);
    c.healAll();
    c.run(300);
    EXPECT_FALSE(c.rep(3).acceptsProduce()) << c.dump();
    EXPECT_EQ(c.rep(1).logEndOffset(), 35);
    EXPECT_EQ(c.rep(2).logEndOffset(), 30);
    c.moveLeader(1);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap)) << c.dump();
    for (NodeId id = 1; id <= 3; ++id) { c.expectAckedPresent(id, "two ahead"); c.expectNoDuplicates(id); }
}

// Repro.cpp #3 / the pid -1 produce of #4: the controller elects node 3 while
// node 2 (holding the committed [10,30)) is unreachable; node 2 learns the
// assignment late.  Losing [10,30) here is the controller-side E1b gap (a
// replica lacking committed records was elected while a clean one existed —
// see DISABLED_E1b_MisElectionWithCleanHolderUnreachable_NoAckedLoss).  What
// the DATA PLANE must guarantee: node 2 is never counted as a copy of records
// it does not hold (it truncates to the leader's lineage via the validated
// fetch path and re-fetches), no silent divergence, every record acked AFTER
// the election survives, and the cluster converges.
TEST(DrIntegrationRegression, LateFollowerAheadOfElectedLeader_ValidatedTruncationNoDivergence) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover({2}), 3);
    c.applyMetadata(2);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    const std::size_t before = c.acked().size();
    ASSERT_EQ(c.produceAcked(25), 25);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap)) << c.dump();
    EXPECT_EQ(positionsDiffering(c.ids(2), c.ids(3)), 0u) << "silent divergence";
    c.expectNoDuplicates(2);
    c.expectNoDuplicates(3);
    std::size_t idx = 0;
    const auto ids2 = c.ids(2);
    for (uint64_t a : c.acked()) {
        if (idx++ < before) continue;            // acked after the election only
        EXPECT_NE(std::find(ids2.begin(), ids2.end(), a), ids2.end()) << "acked id " << a;
    }
}

// E1b acceptance (controller side): the same scenario must not lose the
// committed [10,30) — the controller must not elect a replica lacking committed
// records while a clean ISR member exists (KIP-966 ELR / clean-member gate).
TEST(DrIntegration, DISABLED_E1b_MisElectionWithCleanHolderUnreachable_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    (void)c.failover({2});
    c.applyMetadata(2);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(25), 25);
    expectAllConvergedAndAcked(c, "e1b mis-election");
}

// Repro.cpp #4: a non-idempotent (pid -1) produce reaching the incomplete
// deficient leader through the production gate is refused; nothing lands;
// leadership moves to a complete replica and nothing is lost.
TEST(DrIntegrationRegression, NonIdempotentProduceOnIncompleteDeficientLeader_Refused) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);
    c.healAll();
    for (int i = 0; i < 50; ++i) { EXPECT_EQ(c.produce(1), 0); c.step(); }
    EXPECT_EQ(c.rep(3).logEndOffset(), 10);
    c.moveLeader(2);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "pid -1 on incomplete leader");
}

// rev2-Repro2 #1 (clean elections, every replica in the controller ISR):
// A(1) epoch 1 [0,8) committed + [8,10) on A only; B(2) epoch 2 writes [8,12)
// on B only; A re-elected (epoch 3, LEO 10).  B's tail is DIVERGENT (epoch 2
// never existed on A): it must be truncated, never held as a candidate that
// stalls the leader.  Before: the leader never completed (adoption candidate).
TEST(DrIntegrationRegression, Kip279_DivergentEpochTail_LeaderCompletesAndFollowerTruncates) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(8), 8);
    c.isolate(1);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(2);
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);
    c.run(2);
    c.moveLeader(1);
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << "leader never completed\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "kip-279 divergent epoch tail");
    EXPECT_EQ(c.ids(2).size(), 15u);
}

// rev2-Repro2 #2: same, but B's divergent epoch-2 tail is exactly as long as
// A's uncommitted epoch-1 tail (both LEO 10).  KIP-279: B truncates to
// min(leader's end of epoch 1 = 10, B's OWN end of epoch 1 = 8) = 8, then
// re-fetches [8,10) from A.  Before: B truncated to 10 and kept divergent
// [8,10) as a "copy".
TEST(DrIntegrationRegression, Kip279_SameLengthDivergentTail_TruncatedToOwnEpochEnd) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(8), 8);
    c.isolate(1);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(2);
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(1);
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.run(200);
    EXPECT_EQ(positionsDiffering(c.ids(1), c.ids(2)), 0u) << "silent divergence\n" << c.dump();
    expectAllConvergedAndAcked(c, "kip-279 same length");
}

// rev2-Repro2 #3: a same-epoch re-assert (a partitionEpoch-only ISR change
// re-applies becomeLeader / becomeFollower with the SAME leaderEpoch) must only
// apply the ISR — never reset the leader's completeness / follower slots nor
// the follower's state.  Before: it re-armed the completeness gate and cleared
// the diverged/wedge state, re-admitting a divergent follower (silent
// divergence).
TEST(DrIntegrationRegression, SameEpochReassert_OnlyAppliesIsr_NoDivergence) {
    DrConfig cfg;
    cfg.minIsr = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover({2}), 3);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    const std::size_t before = c.acked().size();
    ASSERT_EQ(c.produceAcked(5), 5);
    c.applyMetadata(2);
    c.run(300);
    ASSERT_EQ(c.produceAcked(20), 20);
    ASSERT_TRUE(c.rep(3).isLeaderComplete());
    const Offset tracked2 = c.rep(3).followerLeoForTest(2);
    // partitionEpoch-only bump: the same leaderEpoch re-applied everywhere.
    c.applyMetadata(3);
    c.applyMetadata(2);
    EXPECT_TRUE(c.rep(3).isLeaderComplete()) << "same-epoch re-assert re-armed the completeness gate";
    EXPECT_TRUE(c.rep(3).acceptsProduce());
    EXPECT_EQ(c.rep(3).followerLeoForTest(2), tracked2) << "same-epoch re-assert reset follower state";
    c.run(300);
    ASSERT_TRUE(c.waitWritable(kCap));
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap)) << c.dump();
    EXPECT_EQ(positionsDiffering(c.ids(3), c.ids(2)), 0u) << "silent divergence";
    std::size_t idx = 0;
    const auto ids2 = c.ids(2);
    for (uint64_t a : c.acked()) {
        if (idx++ < before) continue;
        EXPECT_NE(std::find(ids2.begin(), ids2.end(), a), ids2.end()) << "acked id " << a;
    }
}

// rev2-Repro3: every fetch answer carries exactly ONE batch (maxFetchBytes = 1
// — the regime of any batch >= maxFetchBytes).  Before: the LEO-1 reconcile
// pull spent the whole budget on the boundary batch → livelock.  Now there is
// no reverse pull at all: the deficient leader stays incomplete without a
// frame storm, the controller moves leadership, and catch-up + the KIP-279
// truncation loop make progress one batch per answer.
TEST(DrIntegrationRegression, OneBatchPerAnswer_NoLivelock_ConvergesBatchByBatch) {
    DrConfig cfg;
    cfg.maxFetchBytes = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);
    c.healAll();
    c.run(300);
    EXPECT_FALSE(c.rep(3).acceptsProduce());
    EXPECT_EQ(c.rep(1).logEndOffset(), 30);
    c.moveLeader(1);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(10), 10);
    expectAllConvergedAndAcked(c, "one batch per answer");
}

// rev2-Repro3 × rev2-Repro2 #1: the KIP-279 multi-round truncation loop with a
// one-batch budget (each DivergingEpoch round + every re-fetch carries at most
// one batch).
TEST(DrIntegrationRegression, Kip279_DivergentTail_OneBatchPerAnswer_Converges) {
    DrConfig cfg;
    cfg.maxFetchBytes = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(8), 8);
    c.isolate(1);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    c.moveLeader(2);
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 300)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);
    c.run(2);
    c.moveLeader(1);
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "kip-279 one batch");
}

// Point 1: every batch carries the REAL partitionLeaderEpoch of the leader
// that appended it, on the leader and (verbatim) on every follower, and each
// replica's epoch cache is exactly the (epoch, first offset) of those stamps.
TEST(DrIntegrationRegression, BatchesCarryRealLeaderEpoch_FollowerCacheBuiltFromStamps) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.moveLeader(2);
    ASSERT_TRUE(c.waitWritable(kCap));
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.moveLeader(3);
    ASSERT_TRUE(c.waitWritable(kCap));
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    for (NodeId id = 1; id <= 3; ++id) {
        DrNode& n = c.node(id);
        const auto bytes = n.log->read(0, 1 << 20);
        std::size_t pos = 0;
        int i = 0;
        while (pos + rune::protocol::batch::kRecordBatchOverhead <= bytes.size()) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            const int32_t want = r.baseOffset() < 5 ? 1 : (r.baseOffset() < 10 ? 2 : 3);
            EXPECT_EQ(r.partitionLeaderEpoch(), want) << "node " << id << " batch " << i;
            pos += static_cast<std::size_t>(r.totalLength());
            ++i;
        }
        EXPECT_EQ(i, 15);
        const auto& k = c.rep(id).leaderEpochCacheForTest();
        EXPECT_EQ(k.startOffsetFor(1), 0) << "node " << id;
        EXPECT_EQ(k.startOffsetFor(2), 5) << "node " << id;
        EXPECT_EQ(k.startOffsetFor(3), 10) << "node " << id;
    }
}

// Review (compaction branch): an OffsetsForLeaderEpoch probe carried the
// follower's PRE-truncation (possibly divergent) LEO and the leader credited it
// to the follower's fetch position + ISR liveness — the HW advanced over
// offsets where that follower holds different bytes (or nothing), i.e. acked
// loss on a failover to it.  Only a fetch that passed lineage validation is
// credited; a probe answer credits NOTHING.
TEST(DrIntegrationRegression, EpochProbeCreditsNothing_HwNeverAdvancesOnAProbe) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.isolate(2);
    c.isolate(3);
    ASSERT_EQ(c.produce(5), 5);                  // leader only: uncommitted [10,15)
    c.run(5);
    ASSERT_EQ(c.rep(1).highWatermark(), 10);
    const Offset tracked = c.rep(1).followerLeoForTest(2);
    OffsetsForLeaderEpochReqMsg probe{};
    probe.currentEpoch   = c.rep(1).epoch();
    probe.partition      = kPartId;
    probe.topicId        = kTopicId;
    probe.followerId     = 2;
    probe.requestedEpoch = 1;
    probe.followerLeo    = 15;                   // claims [10,15) it does not hold
    c.rep(1).onOffsetsForLeaderEpochRequest(probe);
    EXPECT_EQ(c.rep(1).followerLeoForTest(2), tracked) << "a probe was credited as a fetch position";
    EXPECT_EQ(c.rep(1).highWatermark(), 10) << "the HW advanced over records only the leader holds";
    c.run(5);
    EXPECT_EQ(c.rep(1).highWatermark(), 10);
    EXPECT_EQ(c.acked().size(), 10u);
    c.healAll();
    expectAllConvergedAndAcked(c, "probe credits nothing");
}

// Review: "TCS/GCS keep stale in-memory state when a complete leader re-enters
// the completeness gate" — the old leader re-armed the gate (and later appended
// FOREIGN reconciled batches, bumping logGeneration) when a follower's probe
// claimed a tail past its LEO, so state the coordinators had loaded for that
// leadership silently went stale.  Kafka parity: completeness is decided once
// per leadership; an ahead / diverging follower is answered on the fetch path
// and never changes the leader's log or completeness.
TEST(DrIntegrationRegression, CompleteLeaderNeverReentersGateNorChangesItsLog) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    ASSERT_TRUE(c.rep(1).isLeaderComplete());
    const uint64_t gen = c.rep(1).logGeneration();
    const uint64_t lead = c.rep(1).leadershipGeneration();
    OffsetsForLeaderEpochReqMsg probe{};
    probe.currentEpoch   = c.rep(1).epoch();
    probe.partition      = kPartId;
    probe.topicId        = kTopicId;
    probe.followerId     = 2;
    probe.requestedEpoch = 1;                    // the shared, still-open epoch
    probe.followerLeo    = 25;                   // "ahead" of the leader
    c.rep(1).onOffsetsForLeaderEpochRequest(probe);
    EXPECT_TRUE(c.rep(1).acceptsProduce()) << "a probe made a complete leader re-enter the gate";
    FetchRequestMsg ahead = c.rep(2).nextFetch();
    ahead.fetchOffset = 25;                      // a diverging fetch past the LEO
    (void)c.rep(1).onFetchRequest(ahead, c.clock());
    c.run(50);
    EXPECT_TRUE(c.rep(1).isLeaderComplete()) << "a complete leader re-entered the gate";
    EXPECT_TRUE(c.rep(1).acceptsProduce());
    EXPECT_EQ(c.rep(1).logGeneration(), gen) << "the leader's log content changed";
    EXPECT_EQ(c.rep(1).leadershipGeneration(), lead);
    EXPECT_EQ(c.rep(1).logEndOffset(), 10);
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "gate never re-entered");
}
