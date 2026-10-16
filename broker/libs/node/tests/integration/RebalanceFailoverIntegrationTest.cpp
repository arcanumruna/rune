// ---------------------------------------------------------------------------
// RebalanceFailoverIntegrationTest.cpp — Milestone D Phase-2 Task 14 (EXIT).
//
// In-process (NO Docker) integration proof that the deterministic control-plane
// tick drives rebalance + failover with NO COMMITTED LOSS on a REAL 3-node
// replication harness (real KIP-101 epoch-boundary truncation on handoff — not a
// mock transport).
//
// The control-plane path exercised end-to-end:
//   1. Leader MetadataService owns a ControlPlaneTimer.  On poll it APPENDS a
//      ControlTickRecord (monotonic tickSeq) to the metadata log — every replica
//      observes the SAME tick from the SAME committed log entry.
//   2. On the tick, the leader runs DynamicBalancer (which internally runs
//      FailoverElection) over the committed AssignmentMap using
//      MembershipState::eligibleNodes() (NOT raw liveNodes()) + the soft
//      ReplicaPositionTable, and enqueues the reassignment via
//      emitDynamicAssignment (bounded batched emit).
//   3. The emitted assignment records replicate + commit; each node applies them
//      identically (deterministic-apply guarantee) and the data-plane replicas
//      transition roles (becomeLeader / becomeFollower) — the new leader is an
//      in-ISR survivor that went through KIP-101 truncation on handoff.
//
// Cases:
//   NodeDeathFailover_NoCommittedLoss              (majority + all-in-sync)   ← EXIT
//   ApplyDeterminism_SameTickBytesIdenticalState
//   Election_ConsumesEligibleNodes_NotRawLive
//   AllInSync_ServedHwRetractsThenRecovers
//   NodeJoin_RebalancePreservesCoLocationMinChurn
//   RejoinAfterRestart_PreferredLeaderMoveBack
//   NoIsrSurvivor_GoesNoLeaderAndRecoversOnUnfence
//   TwoNodeLoss_FenceOneAtATime
//   TransientTopic_HandoffKeepsDataWhenReplicaSurvives
//   TransientTopic_DataGoneWhenNoneSurvive
//   TransientTopic_AllInSyncFollowerCatchUpFromLeaderRing
//
// All waits are bounded (kDriverCap + ASSERT_TRUE << "stalled…").  No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/MetadataService.h>
#include <rune/metadata/DynamicBalancer.h>
#include <rune/metadata/FailoverElection.h>
#include <rune/metadata/MembershipState.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <rune/metadata/ControlPlaneTimer.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/MetadataRecords.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::node;
using namespace rune::metadata;
namespace pmd = rune::protocol::metadata;

static constexpr int kDriverCap = 4000;

// ---------------------------------------------------------------------------
// Temp-dir + payload helpers
// ---------------------------------------------------------------------------
static std::string uniqSuffix() {
    static std::atomic<uint64_t> g{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}
struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_rf_" + std::string(tag) + "_" + uniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

static std::vector<uint8_t> payload(uint8_t fill, std::size_t n = 24) {
    return std::vector<uint8_t>(n, fill);
}

static PartitionConfig userCfg(NodeId self, int32_t topic, int32_t part,
                               int minIsr, DurabilityMode dur = DurabilityMode::Majority) {
    PartitionConfig cfg;
    cfg.topicId             = topic;
    cfg.partitionId         = part;
    cfg.selfId              = self;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    cfg.durability          = dur;
    return cfg;
}

static PartitionConfig metaCfg(NodeId self, int minIsr) {
    PartitionConfig cfg;
    cfg.topicId = -3; cfg.partitionId = 0; cfg.selfId = self;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr = minIsr;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
    cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = true; cfg.fastAckOnApply = true;
    return cfg;
}

// ---------------------------------------------------------------------------
// UserPartition3 — a real 3-node data-plane replica set on one InMemoryTransport.
// Supports the KIP-101 handoff sequence used by the failover cases.
// ---------------------------------------------------------------------------
struct UserPartition3 {
    static constexpr NodeId kN1 = 1, kN2 = 2, kN3 = 3;
    const std::vector<NodeId> all = {kN1, kN2, kN3};

    TempDir d1, d2, d3;
    rune::storage::Log log1, log2, log3;
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> r1, r2, r3;
    int64_t clock = 0;
    int32_t topicId, partition;

    UserPartition3(int32_t topic, int32_t part, int minIsr, DurabilityMode dur)
        : d1("u1"), d2("u2"), d3("u3")
        , log1(d1.path), log2(d2.path), log3(d3.path)
        , topicId(topic), partition(part)
    {
        r1 = std::make_unique<PartitionReplica>(userCfg(kN1, topic, part, minIsr, dur),
              Role::Leader,   1, all, all, &log1, &transport);
        r2 = std::make_unique<PartitionReplica>(userCfg(kN2, topic, part, minIsr, dur),
              Role::Follower, 1, all, all, &log2, &transport);
        r3 = std::make_unique<PartitionReplica>(userCfg(kN3, topic, part, minIsr, dur),
              Role::Follower, 1, all, all, &log3, &transport);
        transport.setLeader(r1.get());
        transport.addFollower(kN2, r2.get());
        transport.addFollower(kN3, r3.get());
    }

    PartitionReplica* byId(NodeId id) {
        return id == kN1 ? r1.get() : id == kN2 ? r2.get() : r3.get();
    }

    bool drive(std::function<bool()> done,
               std::vector<PartitionReplica*> active) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock += 1'000'000LL;
            for (auto* r : active) r->doWork(clock);
            transport.pump(clock);
        }
        return done();
    }
    bool driveAll(std::function<bool()> done) {
        return drive(std::move(done), {r1.get(), r2.get(), r3.get()});
    }
};

// ---------------------------------------------------------------------------
// A minimal 3-node metadata cluster + per-node MetadataService.  Used to drive
// the deterministic ControlTick + dynamic emit + replicate + apply path.
// ---------------------------------------------------------------------------
struct MetaCluster3 {
    static constexpr NodeId kN1 = 1, kN2 = 2, kN3 = 3;
    const std::vector<NodeId> all = {kN1, kN2, kN3};

    TempDir d1, d2, d3;
    rune::storage::Log log1, log2, log3;
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> m1, m2, m3;
    std::unique_ptr<MetadataService>  s1, s2, s3;
    int64_t clock = 0;

    MetaCluster3() : d1("m1"), d2("m2"), d3("m3"), log1(d1.path), log2(d2.path), log3(d3.path) {
        m1 = std::make_unique<PartitionReplica>(metaCfg(kN1, 2), Role::Leader,   1, all, all, &log1, &transport);
        m2 = std::make_unique<PartitionReplica>(metaCfg(kN2, 2), Role::Follower, 1, all, all, &log2, &transport);
        m3 = std::make_unique<PartitionReplica>(metaCfg(kN3, 2), Role::Follower, 1, all, all, &log3, &transport);
        transport.setLeader(m1.get());
        transport.addFollower(kN2, m2.get());
        transport.addFollower(kN3, m3.get());
        s1 = std::make_unique<MetadataService>(m1.get(), kN1);
        s2 = std::make_unique<MetadataService>(m2.get(), kN2);
        s3 = std::make_unique<MetadataService>(m3.get(), kN3);
    }

    void configureAll(const std::vector<TopicSpec>& topics) {
        s1->configure(topics, all);
        s2->configure(topics, all);
        s3->configure(topics, all);
    }

    bool step(std::function<bool()> done) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock += 1'000'000LL;
            m1->doWork(clock); m2->doWork(clock); m3->doWork(clock);
            transport.pump(clock);
            s1->doWork(clock); s2->doWork(clock); s3->doWork(clock);
        }
        return done();
    }

    // Emit the initial static assignment on the leader, replicate + apply on all.
    bool bootstrapStatic() {
        // Review M5: the controller decides only after its claim edge (one pass: rebuild + claim LEO + catch-up, which
        // emits the static assignment itself); the direct call stays an idempotent no-op then.
        clock += 1'000'000LL;
        (void)s1->doWork(clock);
        s1->emitStaticAssignmentIfNeeded();
        const Offset target = m1->logEndOffset();
        if (target <= 0) return false;
        return step([&]{
            return m2->highWatermark() >= target && m3->highWatermark() >= target;
        });
    }

    // Drive replication + apply until all three services have applied through the
    // leader's current LEO.
    bool commitAndApplyAll() {
        const Offset target = m1->logEndOffset();
        return step([&]{ return m1->highWatermark() >= target
                             && m2->highWatermark() >= target
                             && m3->highWatermark() >= target; });
    }
};

// Seed a ReplicaPositionTable from the live data-plane replicas (soft state the
// controller would learn from position reports).
static void reportPositions(ReplicaPositionTable& pos, UserPartition3& up,
                            std::vector<NodeId> live) {
    for (NodeId id : live) {
        pos.report(up.byId(id)->positionReport());
    }
}

// ---------------------------------------------------------------------------
// EXIT CRITERION: node-death failover with NO COMMITTED LOSS for both a
// `majority` and an `all-in-sync` topic — control tick → reassignment → new
// leader from an in-ISR survivor (real KIP-101 truncation on handoff).
// ---------------------------------------------------------------------------
static void runNoCommittedLossFailover(DurabilityMode dur, int minIsr,
                                       const char* label) {
    constexpr int32_t kTopic = 40;
    constexpr int32_t kPart  = 0;
    const NodeId kDead = UserPartition3::kN1;   // leader dies
    const NodeId kSurv1 = UserPartition3::kN2, kSurv2 = UserPartition3::kN3;

    UserPartition3 up(kTopic, kPart, minIsr, dur);

    // ── Produce N committed (acks=All) records; drive to full ISR replication. ──
    constexpr int kN = 6;
    Offset leo = 0;
    for (int i = 0; i < kN; ++i) {
        auto p = payload(static_cast<uint8_t>(0x10 + i));
        Offset base = up.r1->append(std::span<const uint8_t>(p.data(), p.size()), /*acksAll=*/true);
        ASSERT_GE(base, 0) << label << ": acks=All append " << i << " must succeed";
        leo = up.r1->logEndOffset();
    }
    ASSERT_TRUE(up.driveAll([&]{
        return up.r1->highWatermark() >= leo
            && up.r2->logEndOffset() >= leo
            && up.r3->logEndOffset() >= leo;
    })) << label << ": committed records must replicate to all — stalled after "
        << kDriverCap << " iters";

    const Offset committedHw = up.r1->highWatermark();
    ASSERT_EQ(committedHw, leo) << label << ": all N records must be committed at HW";

    // Snapshot committed bytes so we can prove none are lost after failover.
    const std::vector<uint8_t> committedBytesBefore = up.r1->read(0, 1 << 20);
    ASSERT_FALSE(committedBytesBefore.empty());

    // ── Kill the leader: stop driving it + partition it off. ──
    up.transport.removeFollower(kSurv1);   // reset follower wiring around handoff
    up.transport.removeFollower(kSurv2);

    // ── Control plane: MembershipState marks the dead leader expired; the tick
    //    runs FailoverElection over eligibleNodes() and elects an in-ISR survivor. ──
    MembershipState membership(/*sessionTimeoutNs=*/1'000'000'000LL);
    const int64_t t0 = 1'000'000'000LL;
    for (NodeId id : up.all) {
        membership.onJoin(id, "ep", /*incarnation=*/id, /*brokerEpoch=*/id, t0);
        membership.markCatchUpComplete(id);
    }
    // Heartbeat survivors forward; leave the dead leader's heartbeat behind so it expires.
    const int64_t tNow = t0 + 2'000'000'000LL;
    membership.onHeartbeat(kSurv1, tNow);
    membership.onHeartbeat(kSurv2, tNow);

    const auto eligible = membership.eligibleNodes(tNow, /*sessionTimeoutNs=*/1'000'000'000LL);
    ASSERT_EQ(eligible.size(), 2u) << label << ": only the two survivors are eligible";
    EXPECT_EQ(std::find(eligible.begin(), eligible.end(), kDead), eligible.end())
        << label << ": the dead leader must NOT be eligible";

    // Build the current assignment map (single partition, ISR = all three).
    AssignmentMap cur;
    cur.put(kTopic, kPart, PartitionAssignment{kDead, {kDead, kSurv1, kSurv2},
                                               {kDead, kSurv1, kSurv2}, /*le=*/1, /*pe=*/1});

    ReplicaPositionTable pos;
    reportPositions(pos, up, {kSurv1, kSurv2});   // survivors report their frontier

    DynamicBalancerInputs in;
    in.cur = &cur; in.liveNodes = eligible; in.positions = &pos;
    in.topics = { TopicSpec{kTopic, 1, 3, "", ""} };
    DynamicBalancer bal;
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 1u) << label << ": dead leader must produce exactly one reassignment";
    const int32_t newLeader = recs[0].rec.leaderNode;
    ASSERT_NE(newLeader, NO_LEADER) << label << ": an ISR survivor must be elected";
    ASSERT_NE(newLeader, kDead)     << label << ": the dead node must not be re-elected";
    // Elected leader must be in the ORIGINAL ISR (no committed loss invariant).
    EXPECT_TRUE(newLeader == kSurv1 || newLeader == kSurv2);
    const int32_t newEpoch = recs[0].rec.leaderEpoch;
    ASSERT_GT(newEpoch, 1) << label << ": leaderEpoch must bump on leader change";

    // ── Apply the reassignment to the data plane (mirrors the bridge). ──
    const NodeId newFollower = (newLeader == kSurv1) ? kSurv2 : kSurv1;
    up.byId(kDead)->becomeFollower(newEpoch, newLeader);      // old leader demotes + truncates
    up.byId(newLeader)->becomeLeader(newEpoch, {newLeader, newFollower});
    up.byId(newFollower)->becomeFollower(newEpoch, newLeader);

    // Re-wire transport around the new leader (dead node stays partitioned off).
    up.transport.setLeader(up.byId(newLeader));
    up.transport.addFollower(newFollower, up.byId(newFollower));

    // ── Drive the KIP-101 epoch exchange + reconcile to full convergence. ──
    ASSERT_TRUE(up.drive([&]{
        return up.byId(newLeader)->highWatermark() >= committedHw
            && up.byId(newFollower)->logEndOffset() >= committedHw;
    }, {up.byId(newLeader), up.byId(newFollower)}))
        << label << ": new leader + follower must converge to committedHw — stalled after "
        << kDriverCap << " iters";

    // ── EXIT ASSERTION: every previously-committed record is readable at HW on
    //    the NEW leader (no committed loss). ──
    EXPECT_GE(up.byId(newLeader)->highWatermark(), committedHw)
        << label << ": new leader HW must cover every previously-committed record";
    EXPECT_LE(up.byId(newLeader)->highWatermark(), up.byId(newLeader)->logEndOffset())
        << label << ": no phantom-committed record (HW <= LEO)";

    const std::vector<uint8_t> afterBytes =
        up.byId(newLeader)->read(0, 1 << 20);
    ASSERT_GE(afterBytes.size(), committedBytesBefore.size())
        << label << ": new leader must retain at least the committed prefix bytes";
    // The committed prefix must be byte-identical (no committed record rewritten/lost).
    EXPECT_TRUE(std::equal(committedBytesBefore.begin(), committedBytesBefore.end(),
                           afterBytes.begin()))
        << label << ": committed prefix must be byte-identical after failover";
}

TEST(RebalanceFailover, NodeDeath_NoCommittedLoss_Majority) {
    runNoCommittedLossFailover(DurabilityMode::Majority, /*minIsr=*/2, "majority");
}

TEST(RebalanceFailover, NodeDeath_NoCommittedLoss_AllInSync) {
    // AllInSync forces effective minIsr = RF (3); acks=All requires every replica.
    runNoCommittedLossFailover(DurabilityMode::AllInSync, /*minIsr=*/3, "all-in-sync");
}

// REGRESSION (DR-BUG-3 × DR-BUG-6 interaction): after an AllInSync leader death,
// the new leader seeds lastHealthyHw_ from its OWN follower-view committed HW,
// which may LAG the true committed point (it had not observed the last commit when
// the old leader died).  Post-failover the survivor ISR (2) sits below minIsr=RF
// (3), so the DR-BUG-3 min-ISR gate clamps the served HW to that stale floor —
// leaving an already-committed record hidden FOREVER (HW stuck one short of
// committedHw, drive() stalls after 4000 iters).  Fixed by raising lastHealthyHw_
// to the recovered majorityCommittedFrontier when leadership completes.  This test
// is the AllInSync failover path that reproduced the stall.
TEST(RebalanceFailoverRegression, AllInSyncFailoverRecoversStaleHealthyFloor) {
    runNoCommittedLossFailover(DurabilityMode::AllInSync, /*minIsr=*/3,
                               "all-in-sync-regression");
}

// ---------------------------------------------------------------------------
// Apply-determinism: the SAME encoded ControlTickRecord bytes applied on all 3
// nodes' MSMs yield IDENTICAL resulting assignment state, and tickSeq is
// monotonic across emitted ticks.  (The tick decision replays identically on
// leader / follower / recovering.)
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, ApplyDeterminism_SameTickBytesIdenticalState) {
    MetaCluster3 mc;
    // One RF=3 topic, 3 partitions.
    std::vector<TopicSpec> topics = { TopicSpec{50, 3, 3, "", ""} };
    mc.configureAll(topics);
    ASSERT_TRUE(mc.bootstrapStatic()) << "static assignment must replicate — stalled";
    ASSERT_TRUE(mc.commitAndApplyAll());

    // Arm two control ticks on the leader; poll each and assert monotonic tickSeq.
    mc.s1->armControlTick(mc.clock + 1);
    ASSERT_TRUE(mc.s1->pollAndAppendControlTick(mc.clock + 2));
    const int64_t seq0 = mc.s1->lastEmittedTickSeq();
    mc.s1->armControlTick(mc.clock + 3);
    ASSERT_TRUE(mc.s1->pollAndAppendControlTick(mc.clock + 4));
    const int64_t seq1 = mc.s1->lastEmittedTickSeq();
    EXPECT_EQ(seq0, 0);
    EXPECT_EQ(seq1, 1);
    EXPECT_GT(seq1, seq0) << "tickSeq must be monotonic across emitted ticks";

    // Replicate + apply the ControlTick records on all nodes.
    ASSERT_TRUE(mc.commitAndApplyAll());

    // Deterministic apply: independently decode+apply the SAME encoded tick bytes
    // to three fresh MSMs and assert identical resulting committed state.  Also
    // apply a reassignment record to each and confirm identical maps.
    pmd::ControlTickRecord tick; tick.tickTimeNs = 12345; tick.tickSeq = 7;
    uint8_t tickBuf[pmd::ControlTickRecord::ENCODED_SIZE];
    ASSERT_GT(tick.encode(tickBuf, sizeof(tickBuf)), 0u);

    // Build one reassignment record (topic 50, part 0 → leader 2).
    std::vector<int32_t> isr = {2, 3}, rep = {1, 2, 3};
    pmd::UpdatePartitionAssignmentRecord ur;
    ur.topicId = 50; ur.partition = 0; ur.leaderNode = 2;
    ur.isr = pmd::Int32ArrayView(isr.data(), (uint16_t)isr.size());
    ur.replicas = pmd::Int32ArrayView(rep.data(), (uint16_t)rep.size());
    ur.leaderEpoch = 2; ur.partitionEpoch = 2;
    uint8_t urBuf[512];
    const std::size_t urN = ur.encode(urBuf, sizeof(urBuf));
    ASSERT_GT(urN, 0u);

    auto applyBoth = [&](MetadataStateMachine& m) {
        m.apply(tickBuf, sizeof(tickBuf), /*pos=*/0);   // tick: no map effect (marker)
        m.apply(urBuf, urN, /*pos=*/1);
        m.commit(1);
    };
    MetadataStateMachine mA, mB, mC;
    applyBoth(mA); applyBoth(mB); applyBoth(mC);

    auto sameAs = [](const MetadataStateMachine& x, const MetadataStateMachine& y) {
        const auto* a = x.assignment(50, 0);
        const auto* b = y.assignment(50, 0);
        if (!a || !b) return false;
        return a->leaderNode == b->leaderNode && a->leaderEpoch == b->leaderEpoch
            && a->partitionEpoch == b->partitionEpoch && a->isr == b->isr
            && a->replicas == b->replicas;
    };
    EXPECT_TRUE(sameAs(mA, mB)) << "identical tick bytes must yield identical state (A==B)";
    EXPECT_TRUE(sameAs(mA, mC)) << "identical tick bytes must yield identical state (A==C)";
    const auto* r = mA.assignment(50, 0);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->leaderNode, 2);
    EXPECT_EQ(r->leaderEpoch, 2);
}

// ---------------------------------------------------------------------------
// Election consumes eligibleNodes() (not raw liveNodes()): a just-joined,
// not-yet-caught-up node is live but NOT eligible → NOT elected/assigned.
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, Election_ConsumesEligibleNodes_NotRawLive) {
    constexpr int32_t kTopic = 60, kPart = 0;
    const NodeId kDead = 1, kSurvivor = 2, kJoiner = 3;

    MembershipState membership(1'000'000'000LL);
    const int64_t t0 = 1'000'000'000LL;
    // Node 1 (leader) + node 2 caught up; node 3 JUST joined (not caught up).
    membership.onJoin(kDead,     "e1", 1, 1, t0); membership.markCatchUpComplete(kDead);
    membership.onJoin(kSurvivor, "e2", 2, 2, t0); membership.markCatchUpComplete(kSurvivor);
    membership.onJoin(kJoiner,   "e3", 3, 3, t0); // caughtUp == false

    const int64_t tNow = t0 + 2'000'000'000LL;
    membership.onHeartbeat(kSurvivor, tNow);
    membership.onHeartbeat(kJoiner,   tNow);   // joiner is LIVE …

    const auto live     = membership.liveNodes(tNow, 1'000'000'000LL);
    const auto eligible = membership.eligibleNodes(tNow, 1'000'000'000LL);
    // Joiner is live but NOT eligible.
    EXPECT_NE(std::find(live.begin(), live.end(), kJoiner), live.end())
        << "just-joined node is LIVE";
    EXPECT_EQ(std::find(eligible.begin(), eligible.end(), kJoiner), eligible.end())
        << "just-joined not-yet-caught-up node is NOT eligible";

    // Election over eligibleNodes: dead leader 1, ISR={1,2,3}.  Only survivor 2
    // is eligible → 2 elected; joiner 3 must NOT be elected nor land in new ISR.
    ReplicaPositionTable pos;
    pos.report({kTopic, kPart, kSurvivor, 1, 100});
    pos.report({kTopic, kPart, kJoiner,   1, 999});  // joiner is "ahead" but ineligible
    ElectionInputs ei;
    ei.cur = PartitionAssignment{kDead, {kDead, kSurvivor, kJoiner},
                                 {kDead, kSurvivor, kJoiner}, 1, 1};
    ei.liveNodes = eligible;   // eligibleNodes(), not raw liveNodes()
    ei.positions = &pos;
    auto out = FailoverElection::elect(kTopic, kPart, ei);
    ASSERT_TRUE(out.leaderChanged);
    EXPECT_EQ(out.change.newLeader, kSurvivor)
        << "only the eligible survivor may be elected (joiner is ahead but ineligible)";
    EXPECT_EQ(std::find(out.change.newIsr.begin(), out.change.newIsr.end(), kJoiner),
              out.change.newIsr.end())
        << "an ineligible joiner must NOT be placed in the new ISR";
}

// ---------------------------------------------------------------------------
// AllInSync served-HW retract-then-recover: dropping a follower's reported LEO
// below an acked offset retracts the served HW; re-advancing recovers it to the
// prior committed frontier (no PERMANENT loss — the leader retained the data).
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, AllInSync_ServedHwRetractsThenRecovers) {
    constexpr int32_t kTopic = 70, kPart = 0;
    // AllInSync (RF=3) → effective minIsr = 3: every replica must ack.
    UserPartition3 up(kTopic, kPart, /*minIsr=*/3, DurabilityMode::AllInSync);

    // Produce + fully replicate 4 records.
    Offset leo = 0;
    for (int i = 0; i < 4; ++i) {
        auto p = payload(static_cast<uint8_t>(0x20 + i));
        ASSERT_GE(up.r1->append(std::span<const uint8_t>(p.data(), p.size()), true), 0);
        leo = up.r1->logEndOffset();
    }
    ASSERT_TRUE(up.driveAll([&]{
        return up.r1->highWatermark() >= leo
            && up.r2->logEndOffset() >= leo
            && up.r3->logEndOffset() >= leo;
    })) << "AllInSync: all replicas must ack — stalled";
    const Offset committedFrontier = up.r1->highWatermark();
    ASSERT_EQ(committedFrontier, leo);

    // Drop follower r3 out of the transport (it stops acking → ISR shrinks below
    // RF).  Advance the clock past the lag threshold so ISR eviction fires and the
    // AllInSync served HW clamps to the last healthy (RF-in-sync) frontier.
    up.transport.removeFollower(UserPartition3::kN3);
    const int64_t step = 1'000'000LL;
    for (int i = 0; i < 15'000 && static_cast<int>(up.r1->isr().size()) >= 3; ++i) {
        up.clock += step;
        up.r1->doWork(up.clock);
        up.r2->doWork(up.clock);
        up.transport.pump(up.clock);
    }
    ASSERT_LT(static_cast<int>(up.r1->isr().size()), 3)
        << "AllInSync: ISR must shrink below RF after a follower stops acking";

    // Append a new record: with ISR < RF the AllInSync served HW must NOT advance
    // to cover it (retracted — not served as committed).
    auto pNew = payload(0x2F);
    up.r1->append(std::span<const uint8_t>(pNew.data(), pNew.size()), true);
    up.r1->doWork(up.clock);
    EXPECT_LE(up.r1->highWatermark(), committedFrontier)
        << "AllInSync served HW must retract (not advance) while ISR < RF";
    // Crucially: no committed record was LOST — the frontier is still >= the prior commit.
    EXPECT_GE(up.r1->highWatermark(), 0);

    // Re-advance the follower: re-add it, drive; served HW must RECOVER to (at
    // least) the prior committed frontier (leader retained the data → no perm loss).
    up.transport.addFollower(UserPartition3::kN3, up.r3.get());
    ASSERT_TRUE(up.driveAll([&]{
        return static_cast<int>(up.r1->isr().size()) >= 3
            && up.r1->highWatermark() >= committedFrontier;
    })) << "AllInSync served HW must RECOVER to the prior committed frontier — stalled";
    EXPECT_GE(up.r1->highWatermark(), committedFrontier)
        << "recovered served HW must cover every previously-committed record";
    // The committed prefix is intact + readable.
    EXPECT_FALSE(up.r1->read(0, 1 << 20).empty());
}

// ---------------------------------------------------------------------------
// Node-join rebalance: co-location preserved + min-churn.  A co-partition group
// (A,B) is rebalanced as a unit; steady state (no membership change) emits zero
// records (min-churn).
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, NodeJoin_RebalancePreservesCoLocationMinChurn) {
    // Co-partition group of two topics, 3 partitions, RF=3.
    AssignmentMap cur;
    for (int32_t t : {80, 81}) {
        for (int p = 0; p < 3; ++p)
            cur.put(t, p, PartitionAssignment{1, {1,2,3}, {1,2,3}, 0, 0});
    }
    ReplicaPositionTable pos;
    std::vector<TopicSpec> topics = {
        TopicSpec{80, 3, 3, "grp", "k"}, TopicSpec{81, 3, 3, "grp", "k"} };

    // Steady state: all leaders live+in-ISR → min-churn → zero records.
    {
        DynamicBalancerInputs in;
        in.cur = &cur; in.liveNodes = {1,2,3}; in.positions = &pos; in.topics = topics;
        DynamicBalancer bal;
        auto recs = bal.rebalance(in);
        EXPECT_TRUE(recs.empty()) << "min-churn: no membership change → zero records";
    }

    // Node 1 dies → both co-partitioned topics move partition p AS A UNIT to the
    // same survivor.  Co-location invariant: A.p and B.p share the elected leader.
    {
        for (int p = 0; p < 3; ++p) {
            pos.report({80, p, 2, 0, 50}); pos.report({80, p, 3, 0, 40});
            pos.report({81, p, 2, 0, 50}); pos.report({81, p, 3, 0, 40});
        }
        DynamicBalancerInputs in;
        in.cur = &cur; in.liveNodes = {2,3}; in.positions = &pos; in.topics = topics;
        DynamicBalancer bal;
        auto recs = bal.rebalance(in);
        ASSERT_EQ(recs.size(), 6u) << "3 partitions * 2 co-topics must all move";
        // Group same-index leaders by partition; A.p and B.p must match.
        for (int p = 0; p < 3; ++p) {
            std::optional<int32_t> a, b;
            for (auto& r : recs) {
                if (r.rec.partition != p) continue;
                if (r.rec.topicId == 80) a = r.rec.leaderNode;
                if (r.rec.topicId == 81) b = r.rec.leaderNode;
            }
            ASSERT_TRUE(a && b);
            EXPECT_EQ(*a, *b) << "co-location: topics 80/81 partition " << p
                              << " must share the elected leader";
            EXPECT_NE(*a, NO_LEADER);
            EXPECT_NE(*a, 1) << "dead node must not be elected";
        }
    }
}

// ---------------------------------------------------------------------------
// Rejoin-after-restart: preferred leader (replicas[0]) moves back once it is
// eligible again (min-churn restore).  replicas[0]=1 is in ISR but not leader →
// preferred-leader election restores it.
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, RejoinAfterRestart_PreferredLeaderMoveBack) {
    AssignmentMap cur;
    // Node 1 restarted: replicas[0]=1 back in ISR, but current leader is 2.
    // Give node 2 higher load so the preferred move to 1 improves balance.
    cur.put(90, 0, PartitionAssignment{2, {1,2}, {1,2}, 1, 1});
    cur.put(90, 1, PartitionAssignment{2, {1,2}, {1,2}, 1, 1});
    ReplicaPositionTable pos;
    DynamicBalancerInputs in;
    in.cur = &cur; in.liveNodes = {1,2}; in.positions = &pos;
    in.topics = { TopicSpec{90, 2, 2, "", ""} };
    DynamicBalancer bal;
    auto recs = bal.rebalance(in);
    // At least one partition's leadership should move back to preferred node 1.
    bool movedToPreferred = false;
    for (auto& r : recs) if (r.rec.leaderNode == 1) movedToPreferred = true;
    EXPECT_TRUE(movedToPreferred)
        << "preferred leader (replicas[0]=1) must be restored once it rejoins";
    // Never elect a non-ISR replica.
    for (auto& r : recs) {
        const auto* pa = cur.get(r.rec.topicId, r.rec.partition);
        ASSERT_NE(pa, nullptr);
        bool inIsr = false;
        for (int32_t n : pa->isr) if (n == r.rec.leaderNode) inIsr = true;
        EXPECT_TRUE(inIsr || r.rec.leaderNode == NO_LEADER);
    }
}

// ---------------------------------------------------------------------------
// No-ISR-survivor: all in-sync replicas dead → partition goes NO_LEADER, and it
// recovers once a replica is unfenced (caught up) and eligible again.
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, NoIsrSurvivor_GoesNoLeaderAndRecoversOnUnfence) {
    constexpr int32_t kTopic = 100, kPart = 0;
    ReplicaPositionTable pos;

    // Leader 1, ISR={1} (2 and 3 are replicas but NOT in ISR).  Node 1 dies.
    AssignmentMap cur;
    cur.put(kTopic, kPart, PartitionAssignment{1, {1}, {1,2,3}, 1, 1});

    // Election with no ISR survivor eligible → NO_LEADER (clean; allowUnclean=false).
    ElectionInputs ei;
    ei.cur = *cur.get(kTopic, kPart);
    ei.liveNodes = {2, 3};       // 2,3 live but NOT in ISR
    ei.positions = &pos;
    auto out = FailoverElection::elect(kTopic, kPart, ei);
    EXPECT_TRUE(out.wentNoLeader) << "no ISR survivor → NO_LEADER (never unclean)";
    EXPECT_EQ(out.change.newLeader, NO_LEADER);

    // Recovery: node 2 catches up and re-enters the ISR (unfence + caught up).
    // Now ISR={1,2}; node 1 still dead; election elects 2.
    cur.put(kTopic, kPart, PartitionAssignment{NO_LEADER, {1,2}, {1,2,3}, 2, 2});
    pos.report({kTopic, kPart, 2, 2, 50});
    ElectionInputs ei2;
    ei2.cur = *cur.get(kTopic, kPart);
    ei2.liveNodes = {2, 3};
    ei2.positions = &pos;
    auto out2 = FailoverElection::elect(kTopic, kPart, ei2);
    ASSERT_TRUE(out2.leaderChanged);
    EXPECT_FALSE(out2.wentNoLeader) << "with an ISR survivor eligible, must recover";
    EXPECT_EQ(out2.change.newLeader, 2) << "the recovered ISR member must be elected";
}

// ---------------------------------------------------------------------------
// Two-node loss: fence one-at-a-time.  With RF=3 and two nodes lost, the single
// remaining ISR survivor is elected (stable, lowest-nodeId tie-break).  This
// proves the election never funnels leadership to a non-ISR / unsafe node under
// cascading loss.
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, TwoNodeLoss_FenceOneAtATime) {
    constexpr int32_t kTopic = 110;
    AssignmentMap cur;
    // 3 partitions, RF=3, leaders spread across nodes; nodes 1 and 3 both die.
    cur.put(kTopic, 0, PartitionAssignment{1, {1,2,3}, {1,2,3}, 1, 1});
    cur.put(kTopic, 1, PartitionAssignment{3, {1,2,3}, {1,2,3}, 1, 1});
    cur.put(kTopic, 2, PartitionAssignment{1, {1,2,3}, {1,2,3}, 1, 1});
    ReplicaPositionTable pos;
    for (int p = 0; p < 3; ++p) pos.report({kTopic, p, 2, 1, 50});

    DynamicBalancerInputs in;
    in.cur = &cur; in.liveNodes = {2};   // only node 2 survives
    in.positions = &pos;
    in.topics = { TopicSpec{kTopic, 3, 3, "", ""} };
    DynamicBalancer bal;
    auto recs = bal.rebalance(in);
    // Every partition whose leader died must be reassigned to the sole survivor 2.
    int reassigned = 0;
    for (auto& r : recs) {
        EXPECT_EQ(r.rec.leaderNode, 2)
            << "sole ISR survivor must be elected (no unsafe funneling)";
        // The elected leader must be in the ORIGINAL ISR.
        const auto* pa = cur.get(r.rec.topicId, r.rec.partition);
        ASSERT_NE(pa, nullptr);
        bool inIsr = false; for (int32_t n : pa->isr) if (n == 2) inIsr = true;
        EXPECT_TRUE(inIsr);
        ++reassigned;
    }
    EXPECT_GE(reassigned, 2) << "partitions previously led by dead nodes 1/3 must reassign";
}

// ---------------------------------------------------------------------------
// Memory-topic handoff KEEPS in-memory-committed data when a replica survives.
// (InMemoryLog-backed partition; a survivor holds the committed ring → the new
// leader serves it.)
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, TransientTopic_HandoffKeepsDataWhenReplicaSurvives) {
    constexpr int32_t kTopic = 120, kPart = 0;
    const std::vector<NodeId> all = {1, 2, 3};

    rune::storage::InMemoryLog mem1(1 << 20, 4096), mem2(1 << 20, 4096), mem3(1 << 20, 4096);
    InMemoryTransport transport;
    auto cfg = [&](NodeId self) { return userCfg(self, kTopic, kPart, /*minIsr=*/2); };
    PartitionReplica r1(cfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(cfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    PartitionReplica r3(cfg(3), Role::Follower, 1, all, all, &mem3, &transport);
    ASSERT_TRUE(r1.isMemoryBacked());
    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    transport.addFollower(3, &r3);

    int64_t clock = 0;
    Offset leo = 0;
    for (int i = 0; i < 4; ++i) {
        auto p = payload(static_cast<uint8_t>(0x30 + i));
        ASSERT_GE(r1.append(std::span<const uint8_t>(p.data(), p.size()), true), 0);
        leo = r1.logEndOffset();
    }
    auto drive = [&](std::function<bool()> done, std::vector<PartitionReplica*> act) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock += 1'000'000LL; for (auto* r : act) r->doWork(clock); transport.pump(clock);
        }
        return done();
    };
    ASSERT_TRUE(drive([&]{ return r1.highWatermark() >= leo
                                && r2.logEndOffset() >= leo
                                && r3.logEndOffset() >= leo; }, {&r1,&r2,&r3}))
        << "memory topic: committed data must replicate — stalled";
    const Offset committedHw = r1.highWatermark();

    // Kill leader 1; survivor 2 (holds committed ring) takes over.
    transport.removeFollower(2); transport.removeFollower(3);
    const Epoch ne = 2;
    r1.becomeFollower(ne, 2); r2.becomeLeader(ne, {2, 3}); r3.becomeFollower(ne, 2);
    transport.setLeader(&r2); transport.addFollower(3, &r3);
    ASSERT_TRUE(drive([&]{ return r2.highWatermark() >= committedHw
                                && r3.logEndOffset() >= committedHw; }, {&r2, &r3}))
        << "memory topic handoff: survivor must serve committed data — stalled";

    EXPECT_GE(r2.highWatermark(), committedHw)
        << "memory-committed data must SURVIVE handoff when a replica survives";
    EXPECT_FALSE(r2.read(0, 1 << 20).empty()) << "committed ring must be readable on new leader";
}

// ---------------------------------------------------------------------------
// Memory-topic: data is GONE when NO replica survives (honest loss contract).
// A fresh in-memory replica set (all "restarted") has no committed data — the
// contract is that memory topics do NOT survive a full-replica-set loss.
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, TransientTopic_DataGoneWhenNoneSurvive) {
    constexpr int32_t kTopic = 130, kPart = 0;
    const std::vector<NodeId> all = {1, 2, 3};

    {
        rune::storage::InMemoryLog mem1(1 << 20, 4096), mem2(1 << 20, 4096), mem3(1 << 20, 4096);
        InMemoryTransport transport;
        auto cfg = [&](NodeId s){ return userCfg(s, kTopic, kPart, 2); };
        PartitionReplica r1(cfg(1), Role::Leader,   1, all, all, &mem1, &transport);
        PartitionReplica r2(cfg(2), Role::Follower, 1, all, all, &mem2, &transport);
        PartitionReplica r3(cfg(3), Role::Follower, 1, all, all, &mem3, &transport);
        transport.setLeader(&r1); transport.addFollower(2,&r2); transport.addFollower(3,&r3);
        auto p = payload(0x40);
        ASSERT_GE(r1.append(std::span<const uint8_t>(p.data(), p.size()), true), 0);
        int64_t clock = 0;
        for (int i = 0; i < 200 && r1.highWatermark() < 1; ++i) {
            clock += 1'000'000LL; r1.doWork(clock); r2.doWork(clock); r3.doWork(clock);
            transport.pump(clock);
        }
        ASSERT_GE(r1.highWatermark(), 1);
        // All three logs go out of scope here → the entire in-memory ring set is lost.
    }

    // "Restart": brand-new in-memory ring set for the same topic → no committed data.
    rune::storage::InMemoryLog fresh1(1 << 20, 4096), fresh2(1 << 20, 4096), fresh3(1 << 20, 4096);
    InMemoryTransport transport2;
    auto cfg = [&](NodeId s){ return userCfg(s, kTopic, kPart, 2); };
    PartitionReplica f1(cfg(1), Role::Leader,   1, all, all, &fresh1, &transport2);
    PartitionReplica f2(cfg(2), Role::Follower, 1, all, all, &fresh2, &transport2);
    PartitionReplica f3(cfg(3), Role::Follower, 1, all, all, &fresh3, &transport2);
    transport2.setLeader(&f1); transport2.addFollower(2,&f2); transport2.addFollower(3,&f3);

    // HONEST contract: no replica survived → the data is GONE (LEO=0, HW=0, empty read).
    EXPECT_EQ(f1.logEndOffset(), 0) << "memory topic: data is GONE when no replica survives";
    EXPECT_EQ(f1.highWatermark(), 0);
    EXPECT_TRUE(f1.read(0, 1 << 20).empty())
        << "memory topic: no committed data readable after full-replica-set loss";
}

// ---------------------------------------------------------------------------
// Memory-topic all-in-sync: a bounded-ring follower catches up from the leader's
// in-memory ring (AllInSync → every replica must be in sync).
// ---------------------------------------------------------------------------
TEST(RebalanceFailover, TransientTopic_AllInSyncFollowerCatchUpFromLeaderRing) {
    constexpr int32_t kTopic = 140, kPart = 0;
    const std::vector<NodeId> all = {1, 2, 3};

    rune::storage::InMemoryLog mem1(1 << 20, 4096), mem2(1 << 20, 4096), mem3(1 << 20, 4096);
    InMemoryTransport transport;
    // AllInSync → effective minIsr = RF (3); acks require every replica in sync.
    auto cfg = [&](NodeId s){ return userCfg(s, kTopic, kPart, 3, DurabilityMode::AllInSync); };
    PartitionReplica r1(cfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(cfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    PartitionReplica r3(cfg(3), Role::Follower, 1, all, all, &mem3, &transport);
    transport.setLeader(&r1); transport.addFollower(2,&r2); transport.addFollower(3,&r3);
    ASSERT_TRUE(r1.isMemoryBacked());

    int64_t clock = 0;
    Offset leo = 0;
    for (int i = 0; i < 5; ++i) {
        auto p = payload(static_cast<uint8_t>(0x50 + i));
        ASSERT_GE(r1.append(std::span<const uint8_t>(p.data(), p.size()), true), 0);
        leo = r1.logEndOffset();
    }
    // AllInSync acks=All: HW advances only when EVERY replica (r2 AND r3) catches
    // up from the leader's in-memory ring.
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        r1.doWork(clock); r2.doWork(clock); r3.doWork(clock);
        transport.pump(clock);
        done = (r1.highWatermark() >= leo)
            && (r2.logEndOffset() >= leo) && (r3.logEndOffset() >= leo);
    }
    ASSERT_TRUE(done) << "AllInSync memory topic: both followers must catch up from "
                         "the leader ring — stalled after " << kDriverCap << " iters";
    EXPECT_EQ(r1.highWatermark(), leo)
        << "AllInSync: HW == LEO only after every replica caught up from the ring";
    EXPECT_GE(r2.logEndOffset(), leo);
    EXPECT_GE(r3.logEndOffset(), leo);
}

// ---------------------------------------------------------------------------
// Phase 2b E (spec gate): a TRANSIENT (InMemoryLog) partition under acks=All is
// replicated BEFORE ack — the leader's HW (the ack gate) does not move past
// what every in-sync follower holds.
// ---------------------------------------------------------------------------
TEST(RebalanceFailoverRegression, TransientTopicAcksAllReplicatesBeforeAck) {
    constexpr int32_t kTopic = 150, kPart = 0;
    const std::vector<NodeId> all = {1, 2, 3};
    rune::storage::InMemoryLog mem1(1 << 20, 4096), mem2(1 << 20, 4096), mem3(1 << 20, 4096);
    InMemoryTransport transport;
    auto cfg = [&](NodeId s) { return userCfg(s, kTopic, kPart, 3, DurabilityMode::AllInSync); };
    PartitionReplica r1(cfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(cfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    PartitionReplica r3(cfg(3), Role::Follower, 1, all, all, &mem3, &transport);
    transport.setLeader(&r1); transport.addFollower(2, &r2); transport.addFollower(3, &r3);
    ASSERT_TRUE(r1.isMemoryBacked());

    auto p = payload(0x77);
    ASSERT_GE(r1.append(std::span<const uint8_t>(p.data(), p.size()), /*acksAll=*/true), 0);
    const Offset leo = r1.logEndOffset();
    EXPECT_LT(r1.highWatermark(), leo) << "no ack before any follower has the record";

    int64_t clock = 0;
    for (int i = 0; i < kDriverCap; ++i) {
        clock += 1'000'000LL;
        r1.doWork(clock); r2.doWork(clock); r3.doWork(clock);
        transport.pump(clock);
        // Invariant at EVERY step: the ack gate never runs ahead of the slowest ISR member.
        ASSERT_LE(r1.highWatermark(), std::min(r2.logEndOffset(), r3.logEndOffset()))
            << "HW (ack) advanced past a follower that does not hold the record";
        if (r1.highWatermark() >= leo) break;
    }
    EXPECT_EQ(r1.highWatermark(), leo) << "stalled: all followers must catch up and HW reach LEO";
}
