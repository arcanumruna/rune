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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).isLeaderComplete(); }, kCap));
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
// change, so a follower that lagged — or restarted — stays in it).  The new
// leader must recover the committed records from the ahead survivors before
// it serves or accepts writes: no acked loss, no divergence.
// ---------------------------------------------------------------------------
TEST(DrIntegrationRegression, LeaderMovedToLaggingIsrReplica_ReconcilesFromSurvivors) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);                                // 3 lags; controller ISR still {1,2,3}
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);                                   // followers learn HW == LEO: no uncommitted
                                                 // tail, so no KIP-101 probe on the move
    ASSERT_EQ(c.rep(2).highWatermark(), 30);
    c.moveLeader(3);                             // preferred-leader move (3 is in the ISR)
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << "lagging leader never became writable";
    EXPECT_GE(c.rep(3).logEndOffset(), 30) << "leader serves before holding the committed prefix";
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "lagging leader");
}

// KIP-966: a replica restarted after an UNCLEAN shutdown (power loss: its
// unflushed tail — acked records included — is gone) stays in the controller
// ISR.  Leadership handed to it must not lose the acked records the others
// still hold.
TEST(DrIntegrationRegression, UncleanRestartedReplicaMadeLeader_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.run(20);                                   // followers learn HW == LEO (no probe on the move)
    ASSERT_EQ(c.rep(2).highWatermark(), 30);
    c.crash(3, CrashKind::PowerLoss);
    c.restart(3);
    c.applyMetadata(3);
    c.moveLeader(3);
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
// that lacks the range.  (Precondition shared with B2: the elected survivor
// was in the controller ISR but not holding the committed prefix — the data
// plane now refuses to COUNT such a follower as a copy (B2 wedge/diverged) and
// adopts a survivor's committed prefix when it still can (H1), but only the
// controller can avoid electing it.  E1b also covers the review's M2: an
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

// The deficient-leader recovery (OutOfRange → KIP-101 probe → reverse
// reconcile) under a lossy, delayed network: dropped probe answers, reconcile
// answers and fetch answers are re-driven (owed re-sends / reconcile re-drive)
// — still no acked loss, no divergence.
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
    c.dropNext(Kind::EpochResp, 2);
    c.dropNext(Kind::ReconResp, 2);
    c.dropNext(Kind::FetchResp, 4);
    c.dropNext(Kind::EpochReq, 1);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 35);
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
// + acked loss).  The harness produces through the REAL gate
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
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "incomplete leader gate");
}

// H1: the committed range a lagging replica misses was written under an epoch
// it NEVER held (one extra leader move while it lagged).  Handed leadership it
// must still recover that range (adopting the survivors' committed prefix with
// its epochs) and complete — before the fix it stayed incomplete forever
// (partition unwritable): the per-batch lineage gate rejected the unknown
// epoch, and the followers' caches claimed the new epoch from an EMPTY push.
TEST(DrIntegrationRegression, DeficientLeaderMissingWholeEpoch_AdoptsCommittedPrefix) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    c.moveLeader(2);                             // epoch 2 on {1,2}; 3 never sees it
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);                             // lagging ISR replica, epoch 3
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << "deficient leader never completed\n" << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 30);
    EXPECT_EQ(c.rep(3).leaderEpochCacheForTest().startOffsetFor(2), 10)
        << "adopted epoch-2 range must be recorded in the leader's epoch cache";
    EXPECT_EQ(c.produceAcked(25), 25);
    expectAllConvergedAndAcked(c, "missing whole epoch");
}

// B2 (review repro, min-ISR 2): node 2 holds committed [10,30) the elected
// leader 3 lacks; it was unreachable to the controller, so it is outside the
// new ISR and learns the assignment late.  Before the fix its probe was
// answered "truncate to 10", its floor (30) kept the records, it rejoined the
// ISR on the clamped tracked LEO and then counted as a COPY of the leader's new
// [10,35): 20 divergent offsets, bogus acks=all with one real copy.  Now the
// leader (not yet past the shared point) ADOPTS node 2's committed prefix (H1):
// no acked loss at all.
TEST(DrIntegrationRegression, FloorClampedFollowerNeverCountedAsCopy_LeaderAdopts) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);           // committed on {1,2}
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover({2}), 3);               // 2 alive but unreachable to the controller
    c.applyMetadata(2);                          // 2 learns the assignment late
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(25), 25);
    expectAllConvergedAndAcked(c, "floor clamp, adopted");
}

// B2 (wedge path, min-ISR 1): the elected leader already wrote past the shared
// point before node 2 showed up, so node 2's committed [10,30) can no longer
// be adopted.  Node 2 must WEDGE (keep its records, send nothing, CRITICAL
// once) and the leader must never count it in the ISR / as a copy.
TEST(DrIntegrationRegression, FloorClampedDivergedFollowerWedges_NeverCountedAsCopy) {
    DrConfig cfg;
    cfg.minIsr = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);           // committed on {1,2}
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover({2}), 3);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    const std::size_t ackedBefore = c.acked().size();
    ASSERT_EQ(c.produceAcked(5), 5);             // leader 3 moves past the shared point (10)
    c.applyMetadata(2);
    c.run(300);
    EXPECT_GE(c.rep(2).wedgedAtOffset(), 0) << "diverged follower must wedge\n" << c.dump();
    EXPECT_EQ(c.rep(2).logEndOffset(), 30) << "its committed records are kept, not truncated";
    EXPECT_FALSE(c.leaderIsrContains(2)) << "diverged follower counted in the ISR\n" << c.dump();
    EXPECT_EQ(c.acked().size(), ackedBefore + 5);
    EXPECT_EQ(c.produceAcked(5), 5);             // leader-only ISR (min-ISR 1) keeps committing
    EXPECT_FALSE(c.leaderIsrContains(2));
    // Every record acked AFTER the failover is on the leader; none of them was
    // ever credited to node 2 (it never received or acknowledged any).
    const auto ids3 = c.ids(3);
    const auto ids2 = c.ids(2);
    for (uint64_t a : c.acked()) {
        if (a <= 30) continue;                   // pre-failover range: lost to the mis-election (E1b)
        EXPECT_NE(std::find(ids3.begin(), ids3.end(), a), ids3.end()) << "acked id " << a;
        EXPECT_EQ(std::find(ids2.begin(), ids2.end(), a), ids2.end()) << "node 2 counted for " << a;
    }
}

// M1: an OutOfRange answer re-arms the KIP-101 probe; if the probe resolves to
// "fetch again" and the leader answers OutOfRange again, the follower used to
// loop OOR <-> probe at round-trip rate (both sites zeroed the cadence).  The
// probe is now bounded to the normal fetch cadence (and a CRITICAL is logged
// once).
TEST(DrIntegrationRegression, OutOfRangeProbeLoop_BoundedToFetchCadence) {
    DrConfig cfg;
    cfg.fetchIntervalNs = 10 * kStepNs;
    cfg.replicaLagMaxMs = 1000;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.run(30);
    const uint64_t before = c.sentBy(2, Kind::EpochReq);
    FetchResponseMsg oor{};
    oor.partition = kPartId;
    oor.topicId   = kTopicId;
    oor.errorCode = kErrOutOfRange;
    for (int i = 0; i < 100; ++i) {
        oor.leaderEpoch   = c.rep(2).epoch();
        oor.highWatermark = c.rep(1).highWatermark();
        c.rep(2).applyReplicated(oor);           // the leader keeps answering OutOfRange
        c.step();
    }
    const uint64_t probes = c.sentBy(2, Kind::EpochReq) - before;
    EXPECT_GT(probes, 0u);
    EXPECT_LE(probes, 12u) << "OOR <-> probe loop not bounded to the fetch cadence";
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
TEST(DrIntegrationRegression, PowerLoss_AsyncSealNotCompleted_NoOffsetGap) {
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
