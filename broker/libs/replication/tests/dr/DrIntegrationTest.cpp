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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).acceptsProduce(); }));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).acceptsProduce(); }));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));

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
    // Kafka: a controlled move only goes to a member of the committed ISR — wait
    // for the leader's AlterIsr re-admitting node 1 to be committed.
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(1); }, kCap)) << c.dump();
    ASSERT_TRUE(c.moveLeader(1));
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).acceptsProduce(); }, kCap)) << c.dump();
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));
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
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(nl).acceptsProduce(); }, kCap));
    ASSERT_EQ(c.produce(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0 && c.converged(); }, kCap));
    for (NodeId id = 1; id <= 3; ++id) c.expectNoDuplicates(id);
}

// ---------------------------------------------------------------------------
// Kafka model (fix round 1): HW = min LEO over the ISR, ISR changes go through
// the controller (AlterIsr).  A follower that lags past replicaLagMaxMs is
// shrunk out of the controller's COMMITTED ISR before the HW passes its LEO —
// so a lagging replica is never electable: a preferred / controlled move to it
// is refused, and the committed records stay on every electable replica.
// (Replaces the deficient-leader scenarios of the heuristic and completeness
// eras: a deficient user-partition leader cannot be elected any more.)
// ---------------------------------------------------------------------------
TEST(DrIntegrationRegression, LaggingReplicaShrunkFromControllerIsr_NeverElectable) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);                                // 3 lags
    ASSERT_EQ(c.produceAcked(20), 20) << "commits once 3 is shrunk out (AlterIsr committed)";
    EXPECT_FALSE(c.controllerIsrContains(3)) << "the controller still elects from an ISR holding 3";
    EXPECT_GE(c.alterIsrCommitted() + c.fenceCommitted(), 1u);   // leader shrink or controller fencing
    c.run(20);
    EXPECT_FALSE(c.moveLeader(3)) << "a controlled move to a replica outside the committed ISR";
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap))
        << "the caught-up replica was never re-admitted\n" << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 30) << "re-admitted before it held the committed prefix";
    ASSERT_TRUE(c.moveLeader(3)) << "re-admitted: a move to it is allowed again";
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "lagging replica");
}

// KIP-966 (data plane): a replica restarted after an UNCLEAN shutdown (power
// loss: acked records gone) is shrunk out of the controller ISR as soon as its
// regressed fetch reaches the leader (AlterIsr) — a controlled move to it is
// refused until it caught up again (validated, at the HW) and was re-admitted.
// (The window between the restart and its first fetch is the controller-side
// KIP-966 gap: see DISABLED_E1b_UncleanRestartMovedBeforeFirstFetch.)
TEST(DrIntegrationRegression, UncleanRestartedReplicaMadeLeader_NoAckedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.crash(3, CrashKind::PowerLoss);
    c.restart(3);
    c.cut(1, 3);                                 // its fetches reach the leader, nothing comes back
    c.applyMetadata(3);
    ASSERT_TRUE(c.driveUntil([&] { return !c.controllerIsrContains(3); }, kCap)) << c.dump();
    EXPECT_FALSE(c.moveLeader(3)) << "an unclean-restarted replica is not electable";
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap)) << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 30);
    ASSERT_TRUE(c.moveLeader(3));
    ASSERT_TRUE(c.waitWritable(kCap)) << "leader never became writable";
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "unclean restarted leader");
}

// E1b (controller side, KIP-966 clean-shutdown signal): moved BEFORE its first
// fetch, the unclean replica is still in the committed ISR and gets elected
// with a log missing acked records.  The data-plane half (review 2026-10-04
// MEDIUM-4: KIP-903 fetch incarnation + "an ISR member fetching below the HW
// lost committed records") evicts it at its FIRST fetch; electing it before
// that needs the controller to learn of the unclean restart (broker
// registration carrying the incarnation + a clean-shutdown marker) — open.
TEST(DrIntegration, DISABLED_E1b_UncleanRestartMovedBeforeFirstFetch) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.crash(3, CrashKind::PowerLoss);
    c.restart(3);
    c.applyMetadata(3);
    (void)c.moveLeader(3);
    (void)c.waitWritable(kCap);
    (void)c.produceAcked(5);
    expectAllConvergedAndAcked(c, "unclean moved before first fetch");
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();                              // epoch 2 starts at 10 on node 2
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();
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

// KIP-966 double fault, now closed by the Kafka model: the committed range
// lives on {1,2}; node 3 lagged (shrunk out of the committed ISR), node 2
// loses power (its regressed fetch shrinks it out too: committed ISR {1}),
// then leader 1 is process-killed.  No clean candidate → LEADERLESS (never an
// unclean election of 2 or 3); when node 1 returns it is elected and every
// acked record survives.
TEST(DrIntegrationRegression, Kip966_DoubleFault_UncleanFollowerPlusLeaderKill_LeaderlessThenNoLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);           // on {1,2} only; 3 shrunk out
    c.run(20);
    c.crash(2, CrashKind::PowerLoss);
    c.restart(2);
    c.cut(1, 2);
    c.applyMetadata(2);
    ASSERT_TRUE(c.driveUntil([&] { return !c.controllerIsrContains(2); }, kCap)) << c.dump();
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    EXPECT_EQ(c.failover(), -1) << "an unclean / lagging replica was elected";
    EXPECT_EQ(c.produce(5), 0);
    c.run(50);
    c.restart(1);
    ASSERT_EQ(c.failover(), 1) << "the last clean ISR member must be re-elected";
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "kip-966 double fault");
}

// The lagging-replica case under a lossy, delayed network: dropped fetches /
// answers / AlterIsr frames are re-driven (fetch cadence, owed answers, the
// AlterIsr resend) — the lagging replica leaves and re-enters the controller
// ISR, no acked loss, no divergence.
TEST(DrIntegration, LeaderMovedToLaggingIsrReplica_LossyDelayedLinks) {
    DrConfig cfg;
    cfg.deliveryDelaySteps = 2;
    cfg.replicaLagMaxMs    = 100;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    c.dropNext(Kind::AlterIsr, 1);
    ASSERT_EQ(c.produceAcked(25), 25);
    EXPECT_FALSE(c.controllerIsrContains(3));
    EXPECT_FALSE(c.moveLeader(3));
    c.healAll();
    c.dropNext(Kind::FetchResp, 4);
    c.dropNext(Kind::FetchReq, 3);
    c.dropNext(Kind::AlterIsr, 1);
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap)) << c.dump();
    ASSERT_TRUE(c.moveLeader(3));
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

// E1b, consensus side (was DISABLED: the gap the review's MAJOR-2 closed).  An
// INTERNAL partition is led by the consensus leader.  Node 3 was dead (fenced
// out of the internal ISR — committed on {1,2}'s durable knowledge) while
// [10,30) committed; it rejoins and, before its first fetch, the consensus is
// asked to move to it.  It is outside the newest durable internal ISR: node 2
// never votes for it, so it cannot win — the consensus stays on an ISR member,
// nothing is truncated, and node 3 is re-admitted once caught up.
TEST(DrIntegrationRegression, E1b_InternalFencedMemberNotElectableBeforeCatchUp_NoAckedLoss) {
    DrConfig cfg;
    cfg.topicId = -1;                            // __consumer_offsets-like internal partition
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);                                // consensus-dead: fenced
    ASSERT_EQ(c.produceAcked(20), 20);
    c.healAll();
    EXPECT_FALSE(c.moveLeader(3)) << "the consensus elected a member missing committed records";
    ASSERT_TRUE(c.waitWritable(kCap));
    EXPECT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.leaderIsrContains(3); }, kCap))
        << "the caught-up member was never re-admitted\n" << c.dump();
    ASSERT_TRUE(c.driveUntil([&] { return c.moveLeader(3); }, kCap)) << "re-admitted: electable again";
    ASSERT_TRUE(c.waitWritable(kCap));
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "e1b internal fenced member");
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);
    c.run(2);
    ASSERT_TRUE(c.moveLeader(1)) << c.dump();
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

// Repro.cpp #1 under the Kafka model: the replica that missed a whole epoch
// is outside the committed ISR — never electable; failovers pick a holder.
// The internal-partition variant (consensus-led) stays INCOMPLETE instead.
TEST(DrIntegrationRegression, DeficientLeaderMissingWholeEpoch_NotElectable) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_TRUE(c.moveLeader(2));                // epoch 2 on {1,2}; 3 never sees it
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    EXPECT_FALSE(c.controllerIsrContains(3));
    EXPECT_FALSE(c.moveLeader(3));
    c.healAll();
    c.crash(2, CrashKind::ProcessKill);
    const NodeId nl = c.failover();
    EXPECT_EQ(nl, 1) << "only an ISR member may be elected";
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(25), 25);
    c.restart(2);
    c.applyMetadata(2);
    expectAllConvergedAndAcked(c, "missing whole epoch");
}

// INTERNAL partition (__cluster_metadata-like), Repro.cpp #1 shape: node 3 is
// dead while leadership moves to 2 (epoch 2) and [10,30) commits on {1,2} —
// node 3 is FENCED out of the internal ISR (consensus-dead), so the commits
// never wait for it and never freeze.  It rejoins, catches up (validated, at
// the HW), is re-admitted, and only then does the consensus move to it: it is
// writable at once and nothing is lost.
TEST(DrIntegrationRegression, InternalDeadMemberFencedThenReadmitted_NoFreezeNoLoss) {
    DrConfig cfg;
    cfg.topicId = -3;                            // __cluster_metadata-like
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_TRUE(c.moveLeader(2));
    ASSERT_EQ(c.produceAcked(20), 20) << "internal commits froze on a dead member";
    const auto& isr2 = c.rep(2).isr();
    EXPECT_EQ(std::find(isr2.begin(), isr2.end(), 3), isr2.end()) << "a dead member stayed in the ISR";
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] {
        const auto& i = c.rep(2).isr();
        return std::find(i.begin(), i.end(), 3) != i.end();
    }, kCap)) << "the caught-up member was never re-admitted\n" << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 30);
    ASSERT_TRUE(c.moveLeader(3));
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(25), 25);
    expectAllConvergedAndAcked(c, "internal fenced then readmitted");
}

// CRITICAL-2 (review of 0ff9322): an INTERNAL partition whose consensus leader
// fails while the surviving follower holds an UNCOMMITTED tail.  The consensus
// picks the other (live, in-ISR) member: with HW = min over the ISR it holds
// every committed record — it is writable at once (under the majority HW +
// completeness gate it could never prove completeness: control plane frozen),
// truncates the uncommitted tail on the follower (KIP-279) and loses nothing.
TEST(DrIntegrationRegression, Critical2_InternalLeaderFailover_UncommittedTailOnSurvivor_WritableNoLoss) {
    DrConfig cfg;
    cfg.topicId = -1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cut(1, 3);                                 // data plane only: 3 stays consensus-live
    ASSERT_EQ(c.produce(5), 5);
    c.run(5);                                    // inside replicaLagMaxMs: 3 still in the ISR
    EXPECT_EQ(c.acked().size(), 10u) << "committed past a live internal ISR member";
    EXPECT_EQ(c.rep(2).logEndOffset(), 15);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_TRUE(c.moveLeader(3));                // the consensus picks node 3
    ASSERT_TRUE(c.waitWritable(kCap)) << "the new internal leader froze\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "critical-2 internal failover");
}

// CRITICAL-2 + whole-cluster kill -9: an INTERNAL partition, node 3 lagging on
// the data plane (consensus-live) when every node is process-killed.  After the
// restart the consensus elects node 3: it holds every committed record (the HW
// waited for it), is writable at once and nothing acked is lost.
TEST(DrIntegrationRegression, Critical2_InternalPartition_WholeClusterKill9_LaggingConsensusLeader_NoLoss) {
    DrConfig cfg;
    cfg.topicId = -1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.cut(1, 3);
    ASSERT_EQ(c.produce(20), 20);                // on {1,2} only: uncommitted
    c.run(5);
    for (NodeId id = 1; id <= 3; ++id) c.crash(id, CrashKind::ProcessKill);
    for (NodeId id = 1; id <= 3; ++id) c.restart(id);
    c.healAll();
    ASSERT_TRUE(c.moveLeader(3));                // consensus picks the lagging node
    ASSERT_TRUE(c.waitWritable(kCap)) << "a restarted internal leader froze\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "internal kill -9");
}
// Repro.cpp #2: followers ahead by different amounts (one with an extra
// uncommitted tail).  The lagging replica is not electable; when the leader
// dies, the controller elects from the committed ISR; the uncommitted tail is
// truncated by the validated fetch path; no acked loss.
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
    EXPECT_FALSE(c.moveLeader(3));
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover(), 2);
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap)) << c.dump();
    for (NodeId id = 1; id <= 3; ++id) { c.expectAckedPresent(id, "two ahead"); c.expectNoDuplicates(id); }
}

// Repro.cpp #3 (and the former E1b mis-election): the committed range lives on
// {1,2}; node 3 lagged and was shrunk out of the committed ISR; leader 1 dies
// while node 2 is unreachable to the controller.  Kafka: no clean candidate →
// the partition stays LEADERLESS (never an unclean election of 3); once node 2
// is reachable it is elected and nothing is lost.
TEST(DrIntegrationRegression, MisElectionPrevented_LeaderlessUntilCleanHolderReachable) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    ASSERT_FALSE(c.controllerIsrContains(3));
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    EXPECT_EQ(c.failover({2}), -1) << "elected a replica outside the committed ISR";
    c.run(50);
    ASSERT_EQ(c.failover(), 2) << "the clean holder must be elected once reachable";
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(25), 25);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "mis-election prevented");
}


// Repro.cpp #4: a non-idempotent (pid -1) produce on a deficient leader.  User
// partition: the deficient replica is not electable (move refused) — the
// produce goes to the real leader.  (Internal-partition variant:
// IncompleteDeficientLeaderRefusesEveryAppend.)
TEST(DrIntegrationRegression, NonIdempotentProduceOnIncompleteDeficientLeader_Refused) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    EXPECT_FALSE(c.moveLeader(3));
    EXPECT_EQ(c.rep(3).role(), Role::Follower);
    c.healAll();
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "pid -1 never on a deficient leader");
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);
    c.run(2);
    ASSERT_TRUE(c.moveLeader(1)) << c.dump();
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 200)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(2), 2);
    c.run(2);
    ASSERT_TRUE(c.moveLeader(1)) << c.dump();
    c.healAll();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.run(200);
    EXPECT_EQ(positionsDiffering(c.ids(1), c.ids(2)), 0u) << "silent divergence\n" << c.dump();
    expectAllConvergedAndAcked(c, "kip-279 same length");
}

// rev2-Repro2 #3: a same-epoch re-assert (partitionEpoch-only ISR change, e.g.
// a committed AlterIsr) must only apply the ISR — never reset the leader's
// follower slots / completeness nor the follower's state — and never cause a
// silent divergence.
TEST(DrIntegrationRegression, SameEpochReassert_OnlyAppliesIsr_NoDivergence) {
    DrConfig cfg;
    cfg.minIsr = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);           // 3 shrunk out (committed ISR {1,2})
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap)) << c.dump();
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.rep(1).acceptsProduce());
    const Offset tracked2 = c.rep(1).followerLeoForTest(2);
    // partitionEpoch-only re-assert: the same leaderEpoch re-applied everywhere.
    c.applyMetadata(1);
    c.applyMetadata(2);
    c.applyMetadata(3);
    EXPECT_TRUE(c.rep(1).acceptsProduce());
    EXPECT_TRUE(c.rep(1).acceptsProduce());
    EXPECT_EQ(c.rep(1).followerLeoForTest(2), tracked2) << "same-epoch re-assert reset follower state";
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap)) << c.dump();
    EXPECT_EQ(positionsDiffering(c.ids(1), c.ids(2)), 0u) << "silent divergence";
    EXPECT_EQ(positionsDiffering(c.ids(1), c.ids(3)), 0u) << "silent divergence";
}

// rev2-Repro3: every fetch answer carries exactly ONE batch (maxFetchBytes = 1
// — the regime of any batch >= maxFetchBytes).  No reverse pull exists; the
// lagging replica catches up batch by batch, is re-admitted and can lead.
TEST(DrIntegrationRegression, OneBatchPerAnswer_NoLivelock_ConvergesBatchByBatch) {
    DrConfig cfg;
    cfg.maxFetchBytes = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    EXPECT_FALSE(c.moveLeader(3));
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap)) << c.dump();
    ASSERT_TRUE(c.moveLeader(3));
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();
    c.heal(2, 3);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).acceptsProduce(); }, 300)) << c.dump();
    c.isolate(2);
    ASSERT_EQ(c.produce(4), 4);
    c.run(2);
    ASSERT_TRUE(c.moveLeader(1)) << c.dump();
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
    ASSERT_TRUE(c.moveLeader(2)) << c.dump();
    ASSERT_TRUE(c.waitWritable(kCap));
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    ASSERT_TRUE(c.moveLeader(3)) << c.dump();
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
    ASSERT_TRUE(c.rep(1).acceptsProduce());
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
    EXPECT_TRUE(c.rep(1).acceptsProduce()) << "a probe changed the leader's write state";
    FetchRequestMsg ahead = c.rep(2).nextFetch();
    ahead.fetchOffset = 25;                      // a diverging fetch past the LEO
    (void)c.rep(1).onFetchRequest(ahead, c.clock());
    c.run(50);
    EXPECT_TRUE(c.rep(1).acceptsProduce()) << "a diverging fetch changed the leader's write state";
    EXPECT_EQ(c.rep(1).logGeneration(), gen) << "the leader's log content changed";
    EXPECT_EQ(c.rep(1).leadershipGeneration(), lead);
    EXPECT_EQ(c.rep(1).logEndOffset(), 10);
    EXPECT_EQ(c.produceAcked(5), 5);
    expectAllConvergedAndAcked(c, "gate never re-entered");
}

// ===========================================================================
// Fix round 1 (Opus review of 0ff9322): Kafka model for acks=all — HW = min LEO
// over the (maximal) ISR, ISR changes committed by the controller (AlterIsr),
// elections only from the committed ISR.  CRITICAL-1/2, MAJOR-3 scenarios.
// ===========================================================================

// CRITICAL-1: RF=3, the controller sits on node 3 (it knows only its own
// position), node 3 is one fetch round behind, leader 1 dies.  Whoever the
// controller picks from the ISR (here node 3) is a COMPLETE, writable leader —
// with HW = min over the ISR nothing past node 3's LEO was ever committed — and
// no acked record is lost.  (Under the majority HW node 3 was a deficient
// leader that never completed.)
TEST(DrIntegrationRegression, Critical1_ControllerPicksLaggingIsrMember_WritableNoCommittedLoss) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cut(1, 3);                                 // node 3 misses the next round
    ASSERT_EQ(c.produce(5), 5);
    c.run(3);
    EXPECT_EQ(c.acked().size(), 10u) << "committed past an ISR member's LEO";
    EXPECT_TRUE(c.controllerIsrContains(3));
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_EQ(c.failover({}, /*positionsFrom=*/3), 3) << "the controller on node 3 elects itself";
    ASSERT_TRUE(c.waitWritable(kCap)) << "the elected ISR member must be writable\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "critical-1");
}

// KIP-497/841 maximal ISR: while the controller has not committed a shrink,
// the member still holds the HW back (it is still electable) — the leader may
// not commit past its LEO.  Once the AlterIsr is committed, the HW advances.
TEST(DrIntegrationRegression, PendingShrink_HwWaitsForTheMemberUntilCommitted) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cutBoth(1, 3);                             // 3 lags
    c.cutBoth(1, kControllerAddr);               // the leader's AlterIsr is lost
    ASSERT_EQ(c.produce(5), 5);
    c.run(300);                                  // far past replicaLagMaxMs
    EXPECT_TRUE(c.rep(1).isrProposalPendingForTest()) << "the shrink was never proposed";
    EXPECT_TRUE(c.controllerIsrContains(3));
    EXPECT_LE(c.rep(1).highWatermark(), 10) << "HW passed a member the controller could still elect";
    EXPECT_EQ(c.acked().size(), 10u);
    c.heal(1, kControllerAddr);                  // the AlterIsr re-send gets through
    ASSERT_TRUE(c.driveUntil([&] { return !c.controllerIsrContains(3); }, kCap)) << c.dump();
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0; }, kCap));
    EXPECT_EQ(c.acked().size(), 15u) << "committed once the shrink was committed";
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap)) << c.dump();
    expectAllConvergedAndAcked(c, "pending shrink");
}

// Kafka shrink vs expand: a follower whose fetches still ARRIVE but whose
// answers are lost (or applied too slowly) sits exactly at the HW it pins.  It
// must be shrunk (not caught up to the log end within replicaLagMaxMs) and must
// NOT be re-admitted in the same ISR check just because its fetch offset equals
// the old HW — before the fix it was shrunk and re-expanded on every check, the
// HW stayed pinned at its LEO and acks=all stalled forever.
TEST(DrIntegrationRegression, FetchingButLaggingFollowerShrunk_HwAdvances) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cut(1, 3);                                 // 3's fetches reach the leader; nothing comes back
    ASSERT_EQ(c.produceAcked(5), 5) << "acks=all stalled on a lagging-but-fetching follower\n" << c.dump();
    EXPECT_FALSE(c.controllerIsrContains(3));
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap)) << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 15);
    expectAllConvergedAndAcked(c, "fetching but lagging");
}

// A pending expansion always resolves: the leader proposed re-admitting node 3
// (caught up, validated) but the AlterIsr cannot reach the controller; node 3
// then dies.  Node 3 is in the maximal ISR meanwhile (the HW waits for it — it
// may be committed into the ISR at any moment).  Once the controller is
// reachable the pending proposal commits (node 3 held every record up to the
// HW), the lag check shrinks the dead node 3 out, and the HW moves again — no
// permanent pin, no acked loss.
TEST(DrIntegrationRegression, PendingExpansionOfAMemberThatDies_ResolvesHwNotPinned) {
    DrConfig cfg;
    cfg.minIsr = 1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(5), 5);             // 3 shrunk out
    ASSERT_FALSE(c.controllerIsrContains(3));
    c.healAll();
    c.cutBoth(1, kControllerAddr);               // the re-admission proposal is stuck
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).isrProposalPendingForTest(); }, kCap)) << c.dump();
    const auto& max = c.rep(1).maximalIsrForTest();
    ASSERT_NE(std::find(max.begin(), max.end(), 3), max.end()) << "the proposed member must count for the HW";
    c.crash(3, CrashKind::ProcessKill);
    ASSERT_EQ(c.produce(3), 3);
    c.run(100);
    EXPECT_EQ(c.acked().size(), 15u) << "committed past a member the controller may still admit";
    c.heal(1, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0; }, kCap))
        << "the HW stayed pinned by a dead member of a pending proposal\n" << c.dump();
    EXPECT_FALSE(c.controllerIsrContains(3));
    c.restart(3);
    c.applyMetadata(3);
    expectAllConvergedAndAcked(c, "pending expansion resolves");
}

// A re-admitted member is caught up to the HW (validated fetch) before it counts:
// it never enters the committed ISR — nor the HW computation — below the HW.
TEST(DrIntegrationRegression, ReadmittedMemberCaughtUpBeforeItCounts) {
    DrConfig cfg;
    cfg.maxFetchBytes = 1;                       // slow catch-up: one batch per answer
    DrCluster c(3, cfg);
    c.start();
    ASSERT_EQ(c.produceAcked(5), 5);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(40), 40);
    ASSERT_FALSE(c.controllerIsrContains(3));
    c.healAll();
    bool admitted = false;
    for (int i = 0; i < kCap && !admitted; ++i) {
        c.step();
        const auto& max = c.rep(1).maximalIsrForTest();
        const bool counted = std::find(max.begin(), max.end(), 3) != max.end();
        if (counted) {
            ASSERT_GE(c.rep(3).logEndOffset(), c.rep(1).highWatermark())
                << "counted in the HW before it held the committed prefix (step " << i << ")";
        }
        admitted = c.controllerIsrContains(3);
    }
    ASSERT_TRUE(admitted) << c.dump();
    EXPECT_GE(c.rep(3).logEndOffset(), 45);
    expectAllConvergedAndAcked(c, "re-admission");
}

// CRITICAL-2: consensus failovers of an INTERNAL partition while offset commits
// (acks=all appends) keep coming: the consensus leadership moves (to a lagging
// node while it is still electable) and back; acked commits are never lost,
// uncommitted ones are truncated (never acked), and every replica converges.
TEST(DrIntegrationRegression, Critical2_InternalPartition_ConsensusFailoverUnderCommitLoad_NoLoss) {
    DrConfig cfg;
    cfg.topicId = -1;
    DrCluster c(3, cfg);
    c.start();
    ASSERT_TRUE(c.waitWritable(kCap));
    auto load = [&](int steps) {
        for (int i = 0; i < steps; ++i) { (void)c.produce(1); c.step(); }
    };
    load(40);
    c.cut(1, 3);                                 // node 3 falls behind under load
    load(40);
    // Consensus failovers under load (MAJOR-2/3 rule: the election picks node 3
    // only while it is in the newest durable internal ISR — a lagging node the
    // leader shrank out is passed over).
    ASSERT_GE(c.consensusElect(/*preferred=*/3), 0) << c.dump();
    c.heal(1, 3);
    load(60);
    ASSERT_GE(c.consensusElect(/*preferred=*/1), 0) << c.dump();   // and back
    load(60);
    ASSERT_GE(c.consensusElect(/*preferred=*/2), 0) << c.dump();
    load(60);
    ASSERT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0; }, kCap));
    EXPECT_GT(c.acked().size(), 100u);
    expectAllConvergedAndAcked(c, "internal commit load");
}

// ---- Ports of the 0ff9322 review findings that run on the 0ff9322 harness ---
// (RED there: CRITICAL-1 lost 5 acked records on every node; CRITICAL-2 froze —
// the elected ISR member never completed.)
// CRITICAL-1: majority-of-ISR HW + position-blind ISR election.  Node 3 misses
// a round; the leader dies; the controller (which cannot reach node 2) elects
// node 3 from the ISR.
TEST(DrIntegrationRegression, Review0ff9322Port_Critical1_LaggingIsrMemberElected_NoAckedLossAndWritable) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cut(1, 3);
    (void)c.produce(5);
    c.run(3);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    const NodeId nl = c.failover({2});
    ASSERT_EQ(nl, 3);
    c.applyMetadata(2);
    ASSERT_TRUE(c.waitWritable(kCap)) << "the elected ISR member never became writable\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "critical-1 (red port)");
}

// CRITICAL-2 shape: the leader dies while the surviving follower holds a tail
// the new leader lacks; the new leader (in the ISR) must become writable.
TEST(DrIntegrationRegression, Review0ff9322Port_Critical2_NewLeaderWithUncommittedTailOnSurvivor_Writable) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cut(1, 3);
    (void)c.produce(5);
    c.run(3);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    ASSERT_TRUE(c.moveLeader(3));
    ASSERT_TRUE(c.waitWritable(kCap)) << "the new leader froze\n" << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "critical-2 (red port)");
}

// Review 2026-10-04 (rev3) CRITICAL-1, end to end (ReviewRepro R1b + step 5):
// the leader's per-follower LEO map survived across leaderships.  Epoch 1
// (leader 1): node 2 fetches to 20, node 3 stalls at 5.  Epoch 2 (leader 3):
// nodes 1 and 2 truncate to 5, [5,8) commits, node 2 goes slow (inside
// replicaLagMaxMs, still in the ISR) while [8,12) reaches node 1.  Epoch 3: a
// preferred move back to node 1 — it counted node 2 at its epoch-1 position 20
// and acked [12,15) that node 2 lacks, and published (epoch 3, 20) for node 2 to
// the controller.  Node 1 dies; the controller elects node 2 off those
// positions; node 3 truncates to 8: acked records lost.  Fixed: a new
// leadership forgets every follower position (Kafka resetReplicaState) and
// publishes only followers validated in it.
TEST(DrIntegrationRegression, Rev3Critical1_StaleFollowerLeoAcrossLeaderships_NoAckedLoss) {
    DrConfig cfg;
    cfg.replicaLagMaxMs = 200;                   // node 2 stays "in sync" through the scenario
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(5), 5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cutBoth(1, 3);                             // node 3 stalls at 5 (data plane)
    ASSERT_EQ(c.produce(15), 15);
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).followerLeoForTest(2) == 20; }, kCap)) << c.dump();
    ASSERT_EQ(c.rep(1).highWatermark(), 5);
    // Epoch 2: node 3 (ISR member, holds the committed prefix) leads.
    c.healAll();
    ASSERT_TRUE(c.moveLeader(3));
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    ASSERT_EQ(c.produceAcked(3), 3);             // [5,8) on every node
    c.cutBoth(3, 2);                             // node 2 slow from here
    c.cutBoth(1, 2);
    ASSERT_EQ(c.produce(4), 4);                  // [8,12): node 1 gets it, HW waits for node 2
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(1).logEndOffset() == 12; }, kCap)) << c.dump();
    // Epoch 3: preferred move back to node 1 (committed-ISR member).
    ASSERT_TRUE(c.moveLeader(1));
    ASSERT_EQ(c.produce(3), 3);                  // [12,15)
    c.run(20);
    c.controllerLearnPositionsFrom(1);           // the controller ingests the leader's snapshot
    // Node 1 dies; the controller fails over off the positions it learned.
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    const NodeId nl = c.failover();
    ASSERT_GE(nl, 0) << c.dump();
    (void)c.waitWritable(kCap);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "rev3 critical-1");
}

// Review 2026-10-04 MEDIUM-2 (KIP-841): an ASYMMETRIC partition — the
// controller misses node 3's heartbeats (it fences node 3 out of the committed
// ISR) while the leader keeps receiving node 3's fetches (it re-proposes node 3,
// validated at the HW).  Without "never add a fenced replica" + an explicit
// AlterIsr reject, fencing and re-admission alternate every cycle (two metadata
// records per partition per cycle, unbounded metadata-log growth).
TEST(DrIntegrationRegression, AsymmetricPartition_FencedReplicaNotReaddedNoFlapping) {
    DrCluster c(3);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.cut(3, kControllerAddr);                   // controller-side only: 3 still fetches
    c.cut(kControllerAddr, 3);
    c.run(300);
    const uint64_t records = c.alterIsrCommitted() + c.fenceCommitted();
    EXPECT_LE(records, 2u) << "fencing / re-admission flapped: " << c.alterIsrCommitted()
                           << " AlterIsr commits + " << c.fenceCommitted() << " fences in 300 steps";
    EXPECT_FALSE(c.controllerIsrContains(3)) << "a fenced replica sits in the committed ISR";
    EXPECT_EQ(c.produceAcked(5), 5) << "acks=all stalled behind a pinned proposal";
    c.healAll();
    ASSERT_TRUE(c.driveUntil([&] { return c.controllerIsrContains(3); }, kCap))
        << "node 3 never re-admitted once the controller saw it again\n" << c.dump();
    expectAllConvergedAndAcked(c, "asymmetric partition");
}

// Review 2026-10-04 MEDIUM-4 (KIP-903 / KIP-966, data plane): a follower that
// lost acked records in an UNCLEAN restart (power loss, fsync off) and came
// back faster than failure detection (never fenced) kept its committed-ISR
// seat when its leader's follower slot had been reset meanwhile (here: a
// same-leader epoch bump) — the per-leadership regression check saw no
// previous position.  It stayed electable while lacking acked records: the
// controller (which sees only the restarted node's position) then elected it.
TEST(DrIntegrationRegression, Rev3Medium4_UncleanRestartSlotReset_LosesIsrSeat_NoAckedLoss) {
    DrCluster c(3);
    c.start(1);
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    c.crash(3, CrashKind::PowerLoss);            // fsync off: node 3's log is gone
    c.restart(3);                                // back before any failure detection
    ASSERT_TRUE(c.moveLeader(1));                // same leader, new epoch: follower slots reset
    c.cut(1, 3);                                 // node 3's fetches arrive; answers are lost
    c.run(10);                                   // well inside replicaLagMaxMs
    EXPECT_FALSE(c.controllerIsrContains(3)) << "an unclean follower kept its committed-ISR seat\n" << c.dump();
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    (void)c.failover({}, /*positionsFrom=*/3);   // the controller sees node 3's position only
    (void)c.waitWritable(kCap);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "rev3 medium-4 slot reset");
}

// MEDIUM-4, leader changed meanwhile: the unclean follower's first fetch goes
// to a NEW leader (no history of it).  A committed-ISR member's validated fetch
// below the leader's HW proves it lost committed records (every ISR member
// holds everything below the HW) — it loses its seat.  (Residual, controller
// side — KIP-966 registration: a new leader whose HW view is still below the
// restarted member's position cannot tell; see the report.)
TEST(DrIntegrationRegression, Rev3Medium4_UncleanRestartLeaderChanged_LosesIsrSeat_NoAckedLoss) {
    DrCluster c(3);
    c.start(1);
    ASSERT_EQ(c.produceAcked(30), 30);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap));
    // The followers learn the committed offset (HW) from their next answers.
    ASSERT_TRUE(c.driveUntil([&] { return c.rep(2).highWatermark() == 30; }, kCap)) << c.dump();
    c.crash(3, CrashKind::PowerLoss);
    c.restart(3);
    ASSERT_TRUE(c.moveLeader(2));                // preferred move while node 3 restarts
    c.cut(2, 3);
    c.run(10);
    EXPECT_FALSE(c.controllerIsrContains(3)) << "an unclean follower kept its committed-ISR seat\n" << c.dump();
    c.crash(2, CrashKind::ProcessKill);
    c.healAll();
    (void)c.failover({}, /*positionsFrom=*/3);
    (void)c.waitWritable(kCap);
    c.restart(2);
    c.applyMetadata(2);
    expectAllConvergedAndAcked(c, "rev3 medium-4 leader changed");
}

// Review 2026-10-04 MAJOR-2: an ordinary ROLLING RESTART truncated committed
// internal records.  Node 3 restarts and is fenced out of the internal ISR;
// [10,30) commits on {1,2}.  Node 3 rejoins but is still catching up when the
// operator restarts node 1 (the consensus leader).  The data-blind consensus
// election could pick node 3: it led every internal partition with a short log
// and node 2 truncated the committed records (offsets, txn state, metadata).
// Fixed: internal ISR changes commit on a majority's durable knowledge and a
// node outside the newest internal ISR it knows is never voted for.
TEST(DrIntegrationRegression, Rev3Major2_InternalRollingRestart_LaggingNodeNotElected_NoCommittedLoss) {
    DrConfig cfg;
    cfg.topicId = -3;                            // __cluster_metadata-like
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    c.crash(3, CrashKind::ProcessKill);          // rolling restart: node 3 first
    ASSERT_EQ(c.produceAcked(20), 20) << "internal commits froze on a restarting node";
    c.restart(3);
    c.applyMetadata(3);
    c.cut(1, 3);                                 // node 3 still catching up (slow)
    c.run(5);
    c.crash(1, CrashKind::ProcessKill);          // ... then the consensus leader
    c.healAll();
    const NodeId nl = c.consensusElect(/*preferred=*/3);
    ASSERT_GE(nl, 0) << "no electable consensus leader\n" << c.dump();
    EXPECT_NE(nl, 3) << "the lagging node 3 won the consensus election";
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "rev3 major-2 rolling restart");
}

// Review 2026-10-04 MAJOR-3: one consensus-live but data-plane-stuck node froze
// every internal commit (no lag shrink for internal partitions, HW = min over
// an ISR that contains it) — and with it every controller decision.  Fixed:
// the normal lag shrink, committed on a majority's durable knowledge (the
// stuck node can then no longer be elected).
TEST(DrIntegrationRegression, Rev3Major3_StuckLiveInternalReplicaShrunk_ControlPlaneNotFrozen) {
    DrConfig cfg;
    cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    c.cutBoth(1, 3);                             // replication plane only: 3 stays consensus-live
    c.cutBoth(2, 3);
    ASSERT_EQ(c.produce(5), 5);
    EXPECT_TRUE(c.driveUntil([&] { return c.pendingCount() == 0; }, 1000))
        << "internal commits froze behind a stuck live replica\n" << c.dump();
    EXPECT_EQ(c.acked().size(), 15u);
    EXPECT_FALSE(c.leaderIsrContains(3)) << "the stuck replica stayed in the internal ISR";
    c.crash(1, CrashKind::ProcessKill);
    const NodeId nl = c.consensusElect(/*preferred=*/3);
    EXPECT_NE(nl, 3) << "the stuck replica (missing committed records) won the consensus election";
    c.healAll();
    ASSERT_GE(nl, 0) << c.dump();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "rev3 major-3 stuck internal replica");
}

// Re-review 2026-10-04 (fix round 2) MAJOR-A: stale self-knowledge + the
// self-candidacy gate left the consensus permanently headless.  Node 3 is
// fenced out (v2 = {1,2}, which it receives), re-admitted while node 2 is
// fenced (v4 = {1,3}, committed on {1,2}) — but node 3 misses v4.  Node 1 dies:
// node 2 grants only {1,3} and node 3 (believing v2) refused to stand itself,
// so nobody ever nominated although 2 of 3 are up and node 3 is caught up.
// Fixed: only VOTERS apply the candidacy rule (a node always may stand; any
// grant majority intersects the commit majority), and the internal leader
// keeps re-sending the committed state to replicas that have not acked it.
TEST(DrIntegrationRegression, StaleSelfKnowledge_ConsensusNotHeadless) {
    DrConfig cfg;
    cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).internalSeedPendingForTest(); }, kCap));
    // 3 consensus-dead (data plane fine): fenced out; it RECEIVES that state.
    c.cut(3, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] {
        return !c.rep(1).isrProposalPendingForTest() && !c.leaderIsrContains(3);
    }, kCap));
    c.run(20);
    // 3's link from the leader drops (its fetches still reach 1); 3 live again,
    // 2 now consensus-dead: 3 re-admitted, 2 removed — 3 never hears of it.
    c.cut(1, 3);
    c.heal(3, kControllerAddr);
    c.cut(2, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] {
        return !c.rep(1).isrProposalPendingForTest() && c.leaderIsrContains(3) &&
               !c.leaderIsrContains(2);
    }, kCap)) << c.dump();
    c.run(50);
    const auto* k3 = c.node(3).isrKnowledge->latest(-3, kPartId);
    ASSERT_NE(k3, nullptr);
    ASSERT_FALSE(k3->contains(3)) << "precondition: node 3's newest durable state excludes itself";
    ASSERT_EQ(c.rep(3).logEndOffset(), c.rep(1).logEndOffset()) << "precondition: node 3 caught up";
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    const NodeId nl = c.consensusElect(3);
    ASSERT_EQ(nl, 3) << "headless: nobody stands although 2 of 3 are up and node 3 is caught up\n"
                     << c.dump();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(5), 5);
    c.restart(1);
    c.applyMetadata(1);
    expectAllConvergedAndAcked(c, "re-review2 major-a stale self-knowledge");
}

// Re-review 2026-10-04 (fix round 2) MAJOR-A, anti-entropy: a replica that
// missed the frame of a COMMITTED internal ISR state (one lost frame) was never
// told again — the leader re-sent only while the change was pending.  Fixed:
// the leader keeps re-sending the committed state to every replica that has not
// acked it, so knowledge converges without depending on a lucky frame.
TEST(DrIntegrationRegression, CommittedInternalIsrResentUntilAcked_KnowledgeConverges) {
    DrConfig cfg;
    cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] { return !c.rep(1).internalSeedPendingForTest(); }, kCap));
    // Fence node 3 out; the InternalIsr frames to node 3 are lost on the wire
    // while the change commits on {1,2}.
    c.cut(1, 3);
    c.cut(3, kControllerAddr);
    ASSERT_TRUE(c.driveUntil([&] {
        return !c.rep(1).isrProposalPendingForTest() && !c.leaderIsrContains(3);
    }, kCap)) << c.dump();
    const auto* k1 = c.node(1).isrKnowledge->latest(-3, kPartId);
    ASSERT_NE(k1, nullptr);
    const int64_t e = k1->leaderEpoch, v = k1->version;
    const auto* k3 = c.node(3).isrKnowledge->latest(-3, kPartId);
    ASSERT_TRUE(k3 == nullptr || !(k3->leaderEpoch == e && k3->version == v))
        << "precondition: node 3 missed the committed state";
    // The link heals; no further ISR change happens.
    c.heal(1, 3);
    EXPECT_TRUE(c.driveUntil([&] {
        const auto* s = c.node(3).isrKnowledge->latest(-3, kPartId);
        return s != nullptr && !c.rep(1).isrProposalPendingForTest() &&
               s->leaderEpoch == c.node(1).isrKnowledge->latest(-3, kPartId)->leaderEpoch &&
               s->version == c.node(1).isrKnowledge->latest(-3, kPartId)->version;
    }, 500)) << "node 3 never learned the committed internal ISR\n" << c.dump();
}

// Re-review 2026-10-04 (fix round 2) MEDIUM-A: zombie-leader ack window.  L (1)
// leads at term e; Y (3) loses its links to L and lags.  X (2) grants Y's
// RequestVote for e+1 — X's internal replica still follows L at e until the
// bridge sees NewLeadershipTerm.  In that gap L lag-shrinks Y: X persisted and
// acked (e, {1,2}), the change committed on {1,2}, L's HW passed Y's LEO and L
// acked client writes; Y then became leader at e+1 and X truncated them —
// ACKED internal records lost.  Fixed (Raft's term rule applied to the ack): a
// node refuses to persist or ack any internal ISR state whose leaderEpoch is
// older than its current consensus term.
TEST(DrIntegrationRegression, ZombieLeaderInternalIsrAckRefusedAfterVote_NoAckedLoss) {
    DrConfig cfg;
    cfg.topicId = -3;
    DrCluster c(3, cfg);
    c.start(1);
    ASSERT_EQ(c.produceAcked(10), 10);
    ASSERT_TRUE(c.driveUntil([&] {
        return !c.rep(1).internalSeedPendingForTest() && !c.rep(1).isrProposalPendingForTest();
    }, kCap));
    const auto* k2 = c.node(2).isrKnowledge->latest(-3, kPartId);
    ASSERT_NE(k2, nullptr);
    const int64_t e0 = k2->leaderEpoch, v0 = k2->version;
    ASSERT_TRUE(k2->contains(3));
    // Y (3) is cut from L on both planes of the replication link.
    c.cutBoth(1, 3);
    // X (2) grants Y's RequestVote for e+1 (3 votes for itself) — the bridges
    // have not applied a new leadership yet: everyone's replica still at e.
    c.consensusVote(2, c.epoch() + 1);
    c.consensusVote(3, c.epoch() + 1);
    ASSERT_EQ(c.produce(5), 5);                   // writes L would ack past Y's LEO
    c.run(300);                                   // far past replicaLagMaxMs: L shrinks Y
    const auto* k2b = c.node(2).isrKnowledge->latest(-3, kPartId);
    ASSERT_NE(k2b, nullptr);
    EXPECT_TRUE(k2b->leaderEpoch == e0 && k2b->version == v0)
        << "X persisted a state of term " << k2b->leaderEpoch << " after voting in term "
        << c.epoch() + 1;
    EXPECT_TRUE(k2b->contains(3)) << "X's knowledge dropped Y after voting for Y";
    EXPECT_TRUE(c.rep(1).isrProposalPendingForTest() || c.leaderIsrContains(3))
        << "the zombie leader committed an internal ISR change after its acker voted for a newer term\n"
        << c.dump();
    // Y wins the election at e+1 (legitimately: X's knowledge still names it).
    c.healAll();
    const NodeId nl = c.consensusElect(3);
    ASSERT_EQ(nl, 3) << c.dump();
    ASSERT_TRUE(c.waitWritable(kCap)) << c.dump();
    EXPECT_EQ(c.produceAcked(3), 3);
    expectAllConvergedAndAcked(c, "re-review2 medium-a zombie ack window");
}
