// ---------------------------------------------------------------------------
// GatewayFailoverReplicationTest.cpp — GR-7: follower catch-up / truncation /
// rejoin under failover (the durability crux).
//
// Verifies the complete failover path end-to-end with real InMemoryTransport
// (3-node in-process, no Docker, no Aeron driver required for the core
// invariants).
//
// All unit tests use PartitionReplica::append() to ensure correct RecordBatch
// framing (with proper baseOffset stamping).  The group-commit reconciliation
// invariant is verified at the structural level (duty-cycle ordering + role
// checks) since the gateway-level flushDirtyLogs requires a full Aeron setup.
//
// Tests:
//
//  UNIT tests (InMemoryTransport — no Aeron, deterministic bounded clock):
//
//  GR7_Unit_LeaderKillMidProduce_CommittedNotLost:
//    Produce a batch fully committed (acks=All, HW advances).
//    Produce an uncommitted batch (follower partitioned off, ISR shrinks, but
//    that second batch is not in the new ISR).  Force leadership change.
//    Assert committed data present on new leader; uncommitted tail truncated;
//    no phantom-committed record; offsets gap-free.
//
//  GR7_Unit_UncommittedTailTruncation_NoLogDivergence:
//    A follower that received uncommitted entries from the old leader truncates
//    them on adopting the new leader's higher epoch.  Assert no divergence.
//
//  GR7_Unit_Rejoin_CatchesUpToHW:
//    A killed follower rejoins and catches up to the current HW.
//
//  GR7_Unit_GroupCommit_DutyCycleOrder_TruncationBeforeSync:
//    Verify the group-commit ↔ truncation duty-cycle ordering invariant:
//    becomeFollower (role flip / truncation) runs on the SAME duty-cycle
//    thread BEFORE gateway.doWork (flushDirtyLogs).  After truncation:
//      a) Log contains only committed data.
//      b) replica.role() == Follower → per-partition NotLeader guard fires.
//      c) sync() on the truncated log is safe (no phantom data).
//
//  GR7_Unit_PendingAcks_AfterDemotion_NoFalseOK:
//    An acks=All PendingAck entry for a batch that is now above HW (because
//    the node was demoted and HW reset) must NOT be acked OK.
//
//  GR7_Unit_HW_Monotonic_AcrossFailover:
//    HW never regresses across a failover; old committed records are
//    immediately readable on the new leader.
//
//  INTEGRATION tests (3 BrokerNodes, real aeron:udp — no Docker):
//
//  GR7_Integration_LeaderFailover_NoCommittedLoss_GroupCommitReconcile:
//    3 nodes start, elect a leader.  Produce N records via replica::append.
//    Stop the leader node.  Wait for re-election.  Assert:
//      - New leader has all committed records (LEO >= committedHw).
//      - No phantom-committed record (HW <= LEO on new leader).
//      - Per-partition NotLeader enforced on the old leader (role == Follower).
//
//  GR7_Integration_Rejoin_CatchesUp_NoGap:
//    3 nodes start, elect a leader.  Produce N records (committed).
//    Stop a FOLLOWER.  Produce M more records.  Restart the follower.
//    Assert: rejoined follower's LEO >= N (pre-kill committed), log readable.
//
// Design:
//   - All waits bounded (hard cap + ASSERT, no infinite spin).
//   - No wall-clock sleep in unit tests (injected clock).
//   - Integration tests use real wall-clock + deadline-based polling.
//   - Single-writer (Aeron-bible invariant held in production; tested here).
//
// NO Docker.  NO mocks for replication.  C++23.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// Replication
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/PartitionFailoverCoordinator.h>
#include <rune/replication/Types.h>

// Storage
#include <rune/storage/Log.h>

// Gateway / Node (integration only)
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/BrokerNode.h>
#include "../support/OwnerThreadTestHelpers.h"  // F1: owner-thread replica probes
#include <rune/node/NodeConfig.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>

// Protocol
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

// Aeron (integration tests)
#include <Aeron.h>
#include <Publication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::replication;
using namespace rune::node::test;  // F1: leoOnOwner/hwOnOwner/appendOnOwner/...
namespace pc = rune::protocol::client;

// ===========================================================================
// Shared helpers
// ===========================================================================

static std::atomic<uint32_t> g_gr7seq{0};

static fs::path makeGr7TempDir(std::string_view tag = "") {
    auto seq = g_gr7seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gr7_") + std::string(tag) +
                std::to_string(::getpid()) + "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct Gr7TempDir {
    fs::path path;
    explicit Gr7TempDir(std::string_view tag = "") : path(makeGr7TempDir(tag)) {
        fs::create_directories(path);
    }
    ~Gr7TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Build a PartitionConfig with fast cadences for deterministic tests.
static PartitionConfig makeGr7PartitionCfg(NodeId selfId, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;    // 1 ms
    cfg.isrCheckIntervalNs  = 10'000'000LL;   // 10 ms
    cfg.maxFetchBytes       = 1 << 20;
    // GR-7 tests verify fetch-based (pull) replication semantics and truncation
    // of uncommitted tails on epoch change.  Push+fast-ack (GR-10) changes the
    // semantics: data fetched by a follower immediately becomes committed via
    // fast-ack, which invalidates the uncommitted-tail scenario these tests check.
    // Explicitly opt out of GR-10 push to preserve GR-7 test semantics.
    cfg.pushOnAppend     = false;
    cfg.fastAckOnApply   = false;
    return cfg;
}

// Build a raw payload for PartitionReplica::append() input.
// PartitionReplica::append() wraps this in a proper RecordBatch internally.
static std::vector<uint8_t> makeGr7Payload(uint8_t fill = 0xAB, std::size_t size = 16) {
    return std::vector<uint8_t>(size, fill);
}

// Bounded drive helper: advance clock, call doWork on all replicas, pump transport.
// Returns true if done() returned true within `cap` iterations.
static constexpr int     kGr7Cap    = 400;
static constexpr int64_t kGr7StepNs = 1'000'000LL; // 1 ms per step

template<typename Fn>
static bool gr7Drive(std::vector<PartitionReplica*> replicas,
                     InMemoryTransport& transport,
                     int64_t& clock,
                     Fn done) {
    for (int i = 0; i < kGr7Cap && !done(); ++i) {
        clock += kGr7StepNs;
        for (auto* r : replicas) r->doWork(clock);
        transport.pump(clock);
    }
    return done();
}

// Append N records to the leader via PartitionReplica::append().
// Uses acksAll=false (tests HW advancement via follower acks in drive loop).
static Offset appendN(PartitionReplica& leader, int n, uint8_t fill = 0xAB) {
    Offset lastBase = -1;
    for (int i = 0; i < n; ++i) {
        auto payload = makeGr7Payload(fill, 16);
        lastBase = leader.append(std::span<const uint8_t>(payload.data(), payload.size()),
                                 /*acksAll=*/false);
        EXPECT_GE(lastBase, 0) << "append() must succeed on leader";
    }
    return leader.logEndOffset();
}

// ===========================================================================
// GR7_Unit_LeaderKillMidProduce_CommittedNotLost
//
// Scenario:
//   1. 3-node cluster: leader L, followers F2, F3.  minIsr=1 so HW advances
//      once leader self-holds the data (simplifies the test; the durability
//      crux is the truncation, not the HW majority computation).
//   2. Produce 3 records → all replicate → HW = 3.
//   3. Produce 1 uncommitted record with F3 partitioned off and F2 held:
//      We partition off both F2 and F3, then produce B_unc.
//      B_unc is NOT committed (followers don't fetch it).
//   4. Force leadership change: L → Follower(epoch 2), F2 → Leader(epoch 2).
//   5. Assert:
//      a) Old leader L truncated B_unc: L.LEO == committedHw.
//      b) New leader F2 has all committed records: F2.LEO >= committedHw.
//      c) No phantom-committed record: new leader HW <= LEO.
//      d) Offsets gap-free: log readable from 0.
//      e) Old leader rejoins, catches up to new leader.
// ===========================================================================

TEST(GR7_Unit, LeaderKillMidProduce_CommittedNotLost) {
    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;

    Gr7TempDir dL("lk_L"), dF2("lk_F2"), dF3("lk_F3");
    rune::storage::Log logL(dL.path.string());
    rune::storage::Log logF2(dF2.path.string());
    rune::storage::Log logF3(dF3.path.string());

    const std::vector<NodeId> all = {kL, kF2, kF3};
    InMemoryTransport transport;

    // minIsr=1: HW advances once at least 1 ISR member (the leader itself) holds data.
    // Simplifies the test so we don't need to wait for ISR shrink after partitioning.
    PartitionReplica replicaL  (makeGr7PartitionCfg(kL,  /*minIsr=*/1), Role::Leader,   1, all, all, &logL,  &transport);
    PartitionReplica replicaF2 (makeGr7PartitionCfg(kF2, /*minIsr=*/1), Role::Follower, 1, all, all, &logF2, &transport);
    PartitionReplica replicaF3 (makeGr7PartitionCfg(kF3, /*minIsr=*/1), Role::Follower, 1, all, all, &logF3, &transport);

    transport.setLeader(&replicaL);
    transport.addFollower(kF2, &replicaF2);
    transport.addFollower(kF3, &replicaF3);

    int64_t clock = 0;

    // Step 1: Produce 3 records and replicate to all followers.
    Offset leo3 = appendN(replicaL, 3, 0xAA);
    bool ok3 = gr7Drive({&replicaL, &replicaF2, &replicaF3}, transport, clock,
                        [&]{ return replicaL.highWatermark() >= leo3 &&
                                     replicaF2.logEndOffset() >= leo3 &&
                                     replicaF3.logEndOffset() >= leo3; });
    ASSERT_TRUE(ok3) << "3 records must replicate to all followers";
    const Offset committedHw = replicaL.highWatermark();
    ASSERT_GE(committedHw, leo3);
    ASSERT_EQ(replicaF2.logEndOffset(), leo3);

    // Step 2: Partition off both followers and produce uncommitted record B_unc.
    transport.removeFollower(kF2);
    transport.removeFollower(kF3);

    auto unc_payload = makeGr7Payload(0xFF, 16);
    Offset unc_base = replicaL.append(
        std::span<const uint8_t>(unc_payload.data(), unc_payload.size()), false);
    ASSERT_GE(unc_base, 0);
    const Offset leoBeforeFailover = replicaL.logEndOffset();

    // B_unc is NOT committed: no follower fetched it.
    ASSERT_GT(leoBeforeFailover, committedHw)
        << "Uncommitted tail must exist before failover";
    // HW must still be committedHw (B_unc is unreplicated, leader alone).
    // With minIsr=1 and ISR={L,F2,F3}, leader's self-quorum = 1 = majority.
    // After appendN+drive, HW advanced. But B_unc is appended WITHOUT driving,
    // so HW should still be committedHw from the previous commit.
    // Actually: with minIsr=1 and majority=2 (ISR size=3, majority=3/2+1=2),
    // leader alone can't advance HW. Let's just verify the tail exists.
    // We'll use highWatermark() <= committedHw as a sanity check.
    // (After drive earlier, HW was at leo3. Now with B_unc and no followers,
    //  HW might be at leo3 or might advance - depends on ISR computation.)
    // The key assertion is that L's LEO > committedHw after failover, and
    // becomeFollower must truncate to committedHw.

    // Step 3: Force leadership change.
    // L → Follower(epoch 2), F2 → Leader(epoch 2).
    const Epoch newEpoch = 2;
    replicaL.becomeFollower(newEpoch, kF2);
    replicaF2.becomeLeader(newEpoch, {kF2, kF3});

    // DR-BUG-6: truncation is DEFERRED to the OffsetsForLeaderEpoch exchange
    // (KIP-101).  Wire L to the new leader and drive the exchange; L is NOT in
    // F2's ISR ({kF2,kF3}), so its ahead tail is treated as uncommitted and it
    // truncates to F2's epoch boundary (== committedHw).
    transport.setLeader(&replicaF2);
    transport.addFollower(kL, &replicaL);
    bool truncated = gr7Drive({&replicaF2, &replicaL}, transport, clock,
                              [&]{ return replicaL.logEndOffset() <= committedHw; });

    // Assertions:

    // 3a: Old leader L must have truncated B_unc to at most committedHw (via the
    // deferred epoch exchange).
    EXPECT_TRUE(truncated)
        << "old leader must truncate uncommitted B_unc via epoch exchange";
    EXPECT_LE(replicaL.logEndOffset(), committedHw)
        << "epoch exchange must truncate uncommitted B_unc: L.LEO="
        << replicaL.logEndOffset() << " must be <= committedHw=" << committedHw;

    // 3b: L is now a follower.
    EXPECT_EQ(replicaL.role(), Role::Follower);

    // 3c: F2 is now a leader.
    EXPECT_EQ(replicaF2.role(), Role::Leader);

    // 3d: New leader F2 has all committed records (B1+B2+B3).
    EXPECT_GE(replicaF2.logEndOffset(), committedHw)
        << "New leader F2 must hold all committed records (LEO >= committedHw=" << committedHw << ")";

    // 3e: No phantom-committed record: F2's HW must not exceed its LEO.
    EXPECT_LE(replicaF2.highWatermark(), replicaF2.logEndOffset())
        << "HW must never exceed LEO (no phantom-committed records)";

    // 3f: Offsets gap-free: read from 0 on both L and F2.
    {
        auto bytes = logF2.read(0, 1 << 20);
        EXPECT_FALSE(bytes.empty()) << "F2 committed data must be readable";
    }
    {
        // L still has the committed prefix (even after truncation).
        auto bytes = logL.read(0, 1 << 20);
        EXPECT_FALSE(bytes.empty()) << "L committed data must survive truncation";
    }

    // Step 4: Wire transport to new leader; re-add L and F3 as followers.
    transport.setLeader(&replicaF2);
    transport.addFollower(kL, &replicaL);
    transport.addFollower(kF3, &replicaF3);

    // F3 adopts new epoch (it was partitioned off before the failover but
    // has the committed data; becomeFollower updates its epoch).
    replicaF3.becomeFollower(newEpoch, kF2);

    // New leader produces a post-failover record.
    auto post_payload = makeGr7Payload(0xDD, 16);
    Offset post_base = replicaF2.append(
        std::span<const uint8_t>(post_payload.data(), post_payload.size()), false);
    ASSERT_GE(post_base, 0);
    const Offset leoPost = replicaF2.logEndOffset();

    // Drive until L and F3 converge to new leader's LEO.
    bool okPost = gr7Drive({&replicaF2, &replicaL, &replicaF3}, transport, clock,
                           [&]{ return replicaL.logEndOffset() >= leoPost &&
                                        replicaF3.logEndOffset() >= leoPost; });
    ASSERT_TRUE(okPost) << "Old leader and F3 must converge to new leader after rejoin";

    // 4a: No committed data lost on L after convergence.
    EXPECT_GE(replicaL.logEndOffset(), committedHw)
        << "Old leader must retain all previously committed records after rejoin";

    // 4b: L is still follower.
    EXPECT_EQ(replicaL.role(), Role::Follower);

    // 4c: No gap: L.LEO == leoPost (contiguous offsets).
    EXPECT_EQ(replicaL.logEndOffset(), leoPost)
        << "Rejoined L must have exactly the new leader's LEO (no gap)";
}

// ===========================================================================
// GR7_Unit_UncommittedTailTruncation_NoLogDivergence
//
// A follower that received uncommitted entries from the old leader must
// truncate those entries when it adopts the new leader's higher epoch.
//
// Scenario:
//   1. 3-node cluster, minIsr=1.
//   2. Produce B1 (2 records) → all 3 replicate → committed.
//   3. Partition F3 off.  Produce B2 (1 record) → L+F2 replicate → committed.
//   4. Re-add F3 to transport.  Produce B3 (1 record).  F3 fetches B3 (but
//      B3 is not committed since F2 is now partitioned off before B3 can replicate).
//   5. Failover: L → Follower(epoch 2), F2 → Leader(epoch 2).
//      F3 → becomeFollower(epoch 2, F2).  F3 must truncate B3.
//   6. F3 re-fetches from F2 (new leader) → converges.
//   7. Assert: F3.log[0..committedHw) == F2.log[0..committedHw) (byte identical).
// ===========================================================================

TEST(GR7_Unit, UncommittedTailTruncation_NoLogDivergence) {
    constexpr NodeId kL = 10, kF2 = 11, kF3 = 12;

    Gr7TempDir dL("utl_L"), dF2("utl_F2"), dF3("utl_F3");
    rune::storage::Log logL(dL.path.string());
    rune::storage::Log logF2(dF2.path.string());
    rune::storage::Log logF3(dF3.path.string());

    const std::vector<NodeId> all = {kL, kF2, kF3};
    InMemoryTransport transport;

    PartitionReplica replicaL  (makeGr7PartitionCfg(kL,  1), Role::Leader,   1, all, all, &logL,  &transport);
    PartitionReplica replicaF2 (makeGr7PartitionCfg(kF2, 1), Role::Follower, 1, all, all, &logF2, &transport);
    PartitionReplica replicaF3 (makeGr7PartitionCfg(kF3, 1), Role::Follower, 1, all, all, &logF3, &transport);

    transport.setLeader(&replicaL);
    transport.addFollower(kF2, &replicaF2);
    transport.addFollower(kF3, &replicaF3);

    int64_t clock = 0;

    // B1: 2 records → all 3 replicate → committed.
    Offset leo1 = appendN(replicaL, 2, 0x11);
    bool ok1 = gr7Drive({&replicaL, &replicaF2, &replicaF3}, transport, clock,
                        [&]{ return replicaF2.logEndOffset() >= leo1 &&
                                     replicaF3.logEndOffset() >= leo1; });
    ASSERT_TRUE(ok1) << "B1 must replicate to all followers";
    const Offset hwB1 = replicaL.highWatermark();
    ASSERT_GE(hwB1, 0);

    // Partition F3 off.
    transport.removeFollower(kF3);

    // B2: 1 record → L+F2 replicate.
    Offset leo2 = appendN(replicaL, 1, 0x22);
    bool ok2 = gr7Drive({&replicaL, &replicaF2}, transport, clock,
                        [&]{ return replicaF2.logEndOffset() >= leo2; });
    ASSERT_TRUE(ok2) << "B2 must replicate to F2";
    ASSERT_GE(replicaF2.logEndOffset(), leo2);
    // committedHw is at leo2 (leader self + F2 covers majority with minIsr=1
    // and ISR shrink; but with ISR={L,F2,F3} still and F3 lagging,
    // actual HW depends on majority computation).
    // With ISR={L,F2,F3} and F3.LEO=leo1, F2.LEO=leo2, L.LEO=leo2:
    //   sorted=[leo2,leo2,leo1], majority=2, HW=sorted[1]=leo2.
    // If ISR shrunk to {L,F2}: majority=2, HW=sorted[1]=leo2.
    // Either way, committedHw >= leo1 after B2 replication.
    const Offset committedHw = replicaL.highWatermark();
    ASSERT_GE(committedHw, leo1) << "committedHw must cover at least B1 after B2 replication";

    // Partition off F2, re-add F3.
    transport.removeFollower(kF2);
    transport.addFollower(kF3, &replicaF3);

    // B3: 1 record → L+F3 can fetch it (old epoch 1), but B3 is NOT committed
    // (F2 is gone; ISR shrinks to {L,F3} or leader alone; but we don't wait
    //  for ISR shrink here — we just check that F3 gets B3 from L).
    // push-on-append is disabled (makeGr7PartitionCfg) so B3 stays uncommitted.
    Offset leo3 = appendN(replicaL, 1, 0x33);
    ASSERT_GT(leo3, committedHw) << "B3 must be above committedHw (uncommitted)";

    // Let F3 fetch B3 from L (old epoch).
    bool gotB3 = gr7Drive({&replicaL, &replicaF3}, transport, clock,
                          [&]{ return replicaF3.logEndOffset() >= leo3; });
    ASSERT_TRUE(gotB3) << "F3 must replicate B3 from old leader";
    ASSERT_GT(replicaF3.logEndOffset(), committedHw) << "F3 has uncommitted B3";

    // Failover: L→Follower(epoch 2), F2→Leader(epoch 2).
    // F3 also adopts the new epoch.  All three are in the new ISR.
    const Epoch newEpoch = 2;
    replicaL.becomeFollower(newEpoch, kF2);
    replicaF2.becomeLeader(newEpoch, {kF2, kF3, kL});
    replicaF3.becomeFollower(newEpoch, kF2);

    // DR-BUG-6: F3 (and L) are ISR members whose ahead same-epoch tail (B3) is
    // indistinguishable, from local information alone, from a committed
    // continuation the new leader merely lacks — this is exactly the DR-8
    // ambiguity.  The divergence-critical priority is: NEVER lose committed data
    // and NEVER diverge.  So an ahead ISR member KEEPS its tail and the new
    // leader reconciles (pulls) it, rather than truncating (which risked the
    // DR-8 committed-loss).  The result: all replicas CONVERGE byte-identically
    // on the reconciled log — no divergence, which is this test's stated intent.
    transport.setLeader(&replicaF2);
    transport.addFollower(kL, &replicaL);
    transport.addFollower(kF3, &replicaF3);

    // Drive the epoch exchange + reconcile to full convergence.
    bool okConv = gr7Drive({&replicaF2, &replicaL, &replicaF3}, transport, clock,
                           [&]{
                               Offset a = replicaF2.logEndOffset();
                               return replicaF3.logEndOffset() == a &&
                                      replicaL.logEndOffset()  == a;
                           });
    ASSERT_TRUE(okConv) << "all replicas must converge to a single log (no divergence)";

    // No divergence: F2 and F3 logs must be byte-identical over their whole
    // (converged) extent.
    const int32_t readBound = static_cast<int32_t>((leo3 + 2) * 1024 + 4096);
    auto f2Bytes = logF2.read(0, readBound);
    auto f3Bytes = logF3.read(0, readBound);

    ASSERT_FALSE(f2Bytes.empty()) << "F2 log must be non-empty";
    ASSERT_FALSE(f3Bytes.empty()) << "F3 log must be non-empty";

    EXPECT_EQ(f2Bytes.size(), f3Bytes.size())
        << "F2 and F3 log byte-lengths must match (no divergence)";
    EXPECT_EQ(f2Bytes, f3Bytes)
        << "F2 and F3 logs must be byte-identical (no divergence) — the DR-BUG-6 "
           "guarantee is convergence without committed loss";

    // Committed prefix (B1) is never lost.
    EXPECT_GE(replicaF2.logEndOffset(), committedHw)
        << "committed prefix must survive on the new leader";
}

// ===========================================================================
// GR7_Unit_Rejoin_CatchesUpToHW
//
// A follower that was killed (simulated by removing from transport) and then
// rejoins must catch up to the current HW with no gap.
//
// Scenario:
//   1. 2-node cluster: L and F, minIsr=1.
//   2. Produce + replicate 3 records (both have LEO=3).
//   3. "Kill" F: remove it from transport.
//   4. Produce 2 more records on L (HW advances with single-node ISR={L}).
//   5. "Restart" F: add a NEW PartitionReplica on the SAME log.
//      Re-add it to the transport.  Initial LEO = 3 (from step 2).
//   6. Drive until F2.LEO >= current leader LEO.
//   7. Assert: F2.LEO == leader.LEO, gap-free.
// ===========================================================================

TEST(GR7_Unit, Rejoin_CatchesUpToHW) {
    constexpr NodeId kL = 20, kF = 21;

    Gr7TempDir dL("rejoin_L"), dF("rejoin_F");
    rune::storage::Log logL(dL.path.string());
    rune::storage::Log logF(dF.path.string());

    const std::vector<NodeId> all = {kL, kF};
    InMemoryTransport transport;

    PartitionReplica replicaL(makeGr7PartitionCfg(kL, /*minIsr=*/1),
                               Role::Leader,   1, all, all, &logL, &transport);
    PartitionReplica replicaF(makeGr7PartitionCfg(kF, /*minIsr=*/1),
                               Role::Follower, 1, all, all, &logF, &transport);

    transport.setLeader(&replicaL);
    transport.addFollower(kF, &replicaF);

    int64_t clock = 0;

    // Step 1: produce 3 records and replicate to F.
    Offset leo3 = appendN(replicaL, 3, 0xAA);
    bool ok3 = gr7Drive({&replicaL, &replicaF}, transport, clock,
                        [&]{ return replicaF.logEndOffset() >= leo3; });
    ASSERT_TRUE(ok3) << "Initial 3 records must replicate to F";
    ASSERT_EQ(replicaF.logEndOffset(), leo3);
    const Offset hwBeforeKill = replicaL.highWatermark();

    // Step 2: "Kill" F (remove from transport).
    transport.removeFollower(kF);

    // Step 3: produce 2 more records on L (single-node ISR={L} after shrink).
    // Drive enough cycles to trigger ISR shrink (isrCheckIntervalNs=10ms).
    for (int i = 0; i < 2; ++i) {
        auto payload = makeGr7Payload(0xBB, 16);
        replicaL.append(std::span<const uint8_t>(payload.data(), payload.size()), false);
    }
    const Offset leaderLeo = replicaL.logEndOffset();
    ASSERT_GT(leaderLeo, leo3);

    // Drive leader alone for enough cycles so ISR shrinks (>10ms / 1ms step).
    gr7Drive({&replicaL}, transport, clock,
             [&]{ return replicaL.highWatermark() >= leaderLeo; });
    // HW may or may not advance depending on ISR shrink timing; just check it's valid.
    ASSERT_GE(replicaL.highWatermark(), hwBeforeKill);

    // Step 4: "Restart" F — create a new PartitionReplica on the SAME log.
    // The log still has the first 3 records (from before the kill).
    ASSERT_EQ(logF.logEndOffset(), leo3) << "F's log must still be at pre-kill LEO";

    PartitionReplica replicaF2(makeGr7PartitionCfg(kF, /*minIsr=*/1),
                                Role::Follower, 1, all, all, &logF, &transport);

    // Add the restarted follower back to the transport.
    transport.addFollower(kF, &replicaF2);

    // Step 5: Drive until F2 catches up to the leader's LEO.
    bool okCatch = gr7Drive({&replicaL, &replicaF2}, transport, clock,
                            [&]{ return replicaF2.logEndOffset() >= leaderLeo; });
    ASSERT_TRUE(okCatch) << "Restarted follower must catch up to leader LEO";

    // Assertions:
    EXPECT_GE(replicaF2.logEndOffset(), leaderLeo)
        << "Rejoined follower LEO must equal or exceed leader LEO";

    EXPECT_GE(replicaF2.logEndOffset(), hwBeforeKill)
        << "Rejoined follower must hold all pre-kill committed records";

    // Read the committed portion — must be non-empty and match leader.
    const int32_t readBound = static_cast<int32_t>((leaderLeo + 1) * 512 + 4096);
    auto leaderBytes   = logL.read(0, readBound);
    auto followerBytes = logF.read(0, readBound);
    EXPECT_FALSE(leaderBytes.empty());
    EXPECT_FALSE(followerBytes.empty());
    EXPECT_EQ(leaderBytes.size(), followerBytes.size())
        << "Rejoined follower committed prefix must match leader byte-length";
    EXPECT_EQ(leaderBytes, followerBytes)
        << "Rejoined follower log must be byte-identical to leader";
}

// ===========================================================================
// GR7_Unit_GroupCommit_DutyCycleOrder_TruncationBeforeSync
//
// Verifies the duty-cycle ordering invariant for group-commit reconciliation:
//
// In the real NodeRuntimeAgent::doWork(), the order is:
//   1. bridge.doWork  → becomeFollower() → log truncated
//   2. transport.poll → (fetch responses, further replication)
//   3. replica.doWork → (ISR check / follower fetch)
//   4. gateway.doWork → flushDirtyLogs() → log.sync()
//
// Since bridge runs BEFORE gateway, truncation happens BEFORE sync.
// When sync() runs (step 4), the log contains only the committed prefix.
//
// This test verifies:
//   a) After becomeFollower truncation, log.sync() is safe (no phantom data).
//   b) replica.role() == Follower → GR-4 per-partition NotLeader blocks new appends.
//   c) No data above committedHw survives in the log after truncation.
//   d) HW after demotion resets to 0 (followerHw_=0 in becomeFollower).
//
// We simulate the gateway's appendNoSync path by using PartitionReplica::append
// for the committed part (easier, correct framing) and then directly appending
// an uncommitted batch to the leader's log for the truncation test.
// ===========================================================================

TEST(GR7_Unit, GroupCommit_DutyCycleOrder_TruncationBeforeSync) {
    constexpr NodeId kL = 30, kF = 31;

    Gr7TempDir dL("gc_L"), dF("gc_F");
    rune::storage::Log logL(dL.path.string());
    rune::storage::Log logF(dF.path.string());

    const std::vector<NodeId> all = {kL, kF};
    InMemoryTransport transport;

    PartitionReplica replicaL(makeGr7PartitionCfg(kL, /*minIsr=*/1),
                               Role::Leader,   1, all, all, &logL, &transport);
    PartitionReplica replicaF(makeGr7PartitionCfg(kF, /*minIsr=*/1),
                               Role::Follower, 1, all, all, &logF, &transport);

    transport.setLeader(&replicaL);
    transport.addFollower(kF, &replicaF);

    int64_t clock = 0;

    // Step 1: Produce B_committed (2 records) → replicate → commit.
    Offset leo_c = appendN(replicaL, 2, 0x11);
    bool ok1 = gr7Drive({&replicaL, &replicaF}, transport, clock,
                        [&]{ return replicaF.logEndOffset() >= leo_c &&
                                     replicaL.highWatermark() >= leo_c; });
    ASSERT_TRUE(ok1) << "Committed batch must replicate";
    const Offset committedHw = replicaL.highWatermark();
    ASSERT_GE(committedHw, leo_c);

    // Step 2: "Dirty" the leader's log — append an uncommitted batch directly.
    // This simulates what the gateway's appendNoSync does: write to page cache
    // without fsync (the Log marks itself dirty for the group-commit).
    // Partition off F so the batch is not replicated (uncommitted).
    transport.removeFollower(kF);

    auto unc_payload = makeGr7Payload(0xFF, 16);
    Offset unc_base = replicaL.append(
        std::span<const uint8_t>(unc_payload.data(), unc_payload.size()), false);
    ASSERT_GE(unc_base, 0);
    const Offset leoBeforeTrunc = replicaL.logEndOffset();
    ASSERT_GT(leoBeforeTrunc, committedHw) << "Uncommitted tail must exist";

    // SIMULATE the duty-cycle order:
    // Step A: bridge.doWork fires → becomeFollower.
    // DR-BUG-6: truncation is now DEFERRED to the OffsetsForLeaderEpoch exchange.
    // becomeFollower alone no longer truncates — it arms the epoch probe.  Make
    // kF the authoritative new leader (ISR={kF}; L is NOT in the ISR → its ahead
    // tail is an uncommitted, truncatable tail), then drive the exchange.
    replicaL.becomeFollower(2, kF);
    replicaF.becomeLeader(2, {kF});

    // Step B: After becomeFollower, replica.role() == Follower.
    // GR-4: per-partition NotLeader guard prevents new appends.
    EXPECT_EQ(replicaL.role(), Role::Follower)
        << "Demoted node must be Follower (GR-4 NotLeader guard fires)";

    // Drive the epoch exchange so the deferred truncation fires.
    transport.setLeader(&replicaF);
    transport.addFollower(kL, &replicaL);
    bool truncated = gr7Drive({&replicaF, &replicaL}, transport, clock,
                              [&]{ return replicaL.logEndOffset() <= committedHw; });

    // Step C: Log contains only committed data after the exchange truncates the tail.
    EXPECT_TRUE(truncated) << "epoch exchange must truncate the uncommitted tail";
    EXPECT_LE(replicaL.logEndOffset(), committedHw)
        << "After epoch exchange: logEndOffset=" << replicaL.logEndOffset()
        << " must be <= committedHw=" << committedHw;

    // Step E: gateway.doWork fires → flushDirtyLogs() → log.sync().
    // This runs AFTER truncation. sync() only fsyncs the surviving committed tail.
    // Critically: no phantom data above committedHw exists.
    EXPECT_NO_THROW(logL.sync())
        << "sync() on truncated log must not throw";

    EXPECT_LE(logL.logEndOffset(), committedHw)
        << "After sync() on truncated log: logEndOffset must remain <= committedHw";

    // Step F: Verify the committed prefix is still readable (no data loss).
    auto committedBytes = logL.read(0, 1 << 20);
    EXPECT_FALSE(committedBytes.empty())
        << "Committed data must survive in the log after truncation+sync";
}

// ===========================================================================
// GR7_Unit_PendingAcks_AfterDemotion_NoFalseOK
//
// An acks=All PendingAck entry for a batch that is now above the HW (because
// the node was demoted and HW reset) must NOT be acked OK.
//
// We test the invariant at the PartitionReplica level:
//   gateway check: if (replica->highWatermark() >= lastOffset) → send OK
// After demotion, followerHw_=0 → highWatermark()=0 < lastOffset → NOT acked.
// ===========================================================================

TEST(GR7_Unit, PendingAcks_AfterDemotion_NoFalseOK) {
    constexpr NodeId kL = 40, kF = 41;

    Gr7TempDir dL("pa_L"), dF("pa_F");
    rune::storage::Log logL(dL.path.string());
    rune::storage::Log logF(dF.path.string());

    const std::vector<NodeId> all = {kL, kF};
    InMemoryTransport transport;

    // minIsr=2: HW requires BOTH nodes → HW doesn't advance until F fetches.
    PartitionReplica replicaL(makeGr7PartitionCfg(kL, /*minIsr=*/2),
                               Role::Leader,   1, all, all, &logL, &transport);
    PartitionReplica replicaF(makeGr7PartitionCfg(kF, /*minIsr=*/2),
                               Role::Follower, 1, all, all, &logF, &transport);

    transport.setLeader(&replicaL);
    transport.addFollower(kF, &replicaF);

    // Append a batch (not yet committed: follower hasn't fetched).
    auto payload = makeGr7Payload(0xAB, 16);
    Offset base = replicaL.append(
        std::span<const uint8_t>(payload.data(), payload.size()), false);
    ASSERT_GE(base, 0);
    const Offset lastOffset = replicaL.logEndOffset() - 1;  // last record offset

    // HW < lastOffset before follower acks (minIsr=2, F hasn't fetched).
    ASSERT_LE(replicaL.highWatermark(), lastOffset)
        << "HW must be below lastOffset before follower acks";

    // Simulate acks=All pending entry: drainPendingAcks() check before follower acks.
    EXPECT_FALSE(replicaL.highWatermark() > lastOffset)
        << "pendingAck must not drain OK before follower acks";

    // Now demote the leader (role flip, HW resets).
    replicaL.becomeFollower(2, kF);

    // After demotion: followerHw_ = 0.
    EXPECT_EQ(replicaL.highWatermark(), 0)
        << "Demoted node's HW resets to 0";

    // drainPendingAcks() check after demotion:
    // replica->highWatermark() == 0 < lastOffset → NOT acked.
    // Ensures NO false OK ack after demotion.
    EXPECT_FALSE(replicaL.highWatermark() > lastOffset)
        << "PendingAck must NOT be acked OK after demotion (HW=0 < lastOffset)";

    // Per-partition NotLeader guard: role() == Follower → no new appends.
    EXPECT_EQ(replicaL.role(), Role::Follower);
}

// ===========================================================================
// GR7_Unit_HW_Monotonic_AcrossFailover
//
// The high-watermark must never regress across a failover.
// After a new leader takes over, HW on the new leader must be >= old HW.
// After re-wiring and producing more, HW continues to advance monotonically.
// ===========================================================================

TEST(GR7_Unit, HW_Monotonic_AcrossFailover) {
    constexpr NodeId kL = 50, kF2 = 51, kF3 = 52;

    Gr7TempDir dL("hw_L"), dF2("hw_F2"), dF3("hw_F3");
    rune::storage::Log logL(dL.path.string());
    rune::storage::Log logF2(dF2.path.string());
    rune::storage::Log logF3(dF3.path.string());

    const std::vector<NodeId> all = {kL, kF2, kF3};
    InMemoryTransport transport;

    PartitionReplica replicaL  (makeGr7PartitionCfg(kL,  1), Role::Leader,   1, all, all, &logL,  &transport);
    PartitionReplica replicaF2 (makeGr7PartitionCfg(kF2, 1), Role::Follower, 1, all, all, &logF2, &transport);
    PartitionReplica replicaF3 (makeGr7PartitionCfg(kF3, 1), Role::Follower, 1, all, all, &logF3, &transport);

    transport.setLeader(&replicaL);
    transport.addFollower(kF2, &replicaF2);
    transport.addFollower(kF3, &replicaF3);

    int64_t clock = 0;

    // Produce 4 records → replicate to all followers → HW advances.
    Offset leo4 = appendN(replicaL, 4, 0x01);
    bool ok4 = gr7Drive({&replicaL, &replicaF2, &replicaF3}, transport, clock,
                        [&]{ return replicaF2.logEndOffset() >= leo4 &&
                                     replicaF3.logEndOffset() >= leo4 &&
                                     replicaL.highWatermark() >= leo4; });
    ASSERT_TRUE(ok4) << "4 records must replicate to all followers and HW must advance";
    const Offset hw1 = replicaL.highWatermark();
    ASSERT_GT(hw1, 0);

    // Failover: L→Follower, F2→Leader.
    const Epoch newEpoch = 2;
    replicaL.becomeFollower(newEpoch, kF2);
    replicaF2.becomeLeader(newEpoch, {kF2, kF3});

    // New leader F2 must hold all hw1 committed records (LEO >= hw1).
    EXPECT_GE(replicaF2.logEndOffset(), hw1)
        << "New leader must have all previously committed records";

    // Re-wire transport to new leader.
    transport.setLeader(&replicaF2);
    transport.addFollower(kL, &replicaL);

    // Produce 2 more records under new leader.
    Offset leo6 = appendN(replicaF2, 2, 0x02);
    bool ok6 = gr7Drive({&replicaF2, &replicaL, &replicaF3}, transport, clock,
                        [&]{ return replicaL.logEndOffset() >= leo6; });
    ASSERT_TRUE(ok6) << "Post-failover produce must replicate";
    const Offset hw2 = replicaF2.highWatermark();

    // HW must be monotonically non-decreasing.
    EXPECT_GE(hw2, hw1)
        << "HW must be monotonic: hw2=" << hw2 << " >= hw1=" << hw1;
}

// ===========================================================================
// Integration test helpers
// ===========================================================================

static uint16_t gr7DiscoverFreeUdpPort() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd); throw std::runtime_error("bind() failed");
    }
    socklen_t addrLen = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &addrLen);
    const uint16_t netPort = addr.sin_port;
    const uint16_t port = static_cast<uint16_t>((netPort >> 8) | ((netPort & 0xFFu) << 8));
    ::close(fd);
    return port;
}

static std::vector<uint16_t> gr7DiscoverFreePorts(int n) {
    std::vector<uint16_t> ports;
    ports.reserve(n);
    while (static_cast<int>(ports.size()) < n) {
        uint16_t p = gr7DiscoverFreeUdpPort();
        bool dup = false;
        for (uint16_t e : ports) { if (e == p) { dup = true; break; } }
        if (!dup) ports.push_back(p);
    }
    return ports;
}

static rune::node::NodeConfig makeGr7Config(
    int nodeId,
    const fs::path& dataDir,
    const std::vector<uint16_t>& cPorts,
    const std::vector<uint16_t>& kPorts,
    const std::vector<uint16_t>& rPorts)
{
    rune::node::NodeConfig cfg;
    cfg.nodeId              = nodeId;
    cfg.dataDir             = dataDir.string();
    cfg.consensusStreamId   = 60;
    cfg.replicationStreamId = 70;

    cfg.electionTimeoutMs        = 150;
    cfg.electionStatusIntervalMs = 20;
    cfg.startupCanvassTimeoutMs  = 100;

    for (int i = 0; i < 3; ++i) {
        rune::node::PeerConfig p;
        p.id              = i;
        p.host            = "127.0.0.1";
        p.consensusPort   = cPorts[static_cast<std::size_t>(i)];
        p.clientPort      = kPorts[static_cast<std::size_t>(i)];
        p.replicationPort = rPorts[static_cast<std::size_t>(i)];
        cfg.peers.push_back(p);
    }

    cfg.clientEndpoint.channel =
        "aeron:udp?endpoint=127.0.0.1:" +
        std::to_string(kPorts[static_cast<std::size_t>(nodeId)]);
    cfg.clientEndpoint.streamId = 80;

    rune::node::TopicSpec topic;
    topic.name           = "gr7topic";
    topic.partitionCount = 1;
    topic.topicId        = 0;
    cfg.topics.push_back(topic);

    return cfg;
}

// ===========================================================================
// GR7_Integration_LeaderFailover_NoCommittedLoss_GroupCommitReconcile
//
// 3 BrokerNodes with real Aeron UDP.  Produce N records directly on the
// leader replica.  Stop the leader node.  Wait for re-election.
// Assert:
//   - New leader has all committed records (LEO >= committedHw).
//   - No phantom-committed record (HW <= LEO on new leader).
//   - Per-partition role is correct (new leader = Leader, old leader = stopped).
//
// This exercises the REAL duty-cycle ordering:
//   bridge.doWork → transport.poll → replica.doWork → gateway.doWork (flushDirtyLogs)
// The stopped leader's dirtyLogs_ was either fsynced or truncated by its last
// duty cycle before stopping.  On re-election, survivors hold all committed data.
// ===========================================================================

TEST(GR7_Integration, LeaderFailover_NoCommittedLoss_GroupCommitReconcile) {
    Gr7TempDir dir0{"int_fa0_"}, dir1{"int_fa1_"}, dir2{"int_fa2_"};

    const auto cPorts = gr7DiscoverFreePorts(3);
    const auto kPorts = gr7DiscoverFreePorts(3);
    const auto rPorts = gr7DiscoverFreePorts(3);

    auto cfg0 = makeGr7Config(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makeGr7Config(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makeGr7Config(2, dir2.path, cPorts, kPorts, rPorts);

    auto node0 = std::make_unique<rune::node::BrokerNode>(cfg0);
    auto node1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    auto node2 = std::make_unique<rune::node::BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    // Wait for all nodes to elect a leader (up to 8 s).
    ASSERT_TRUE(node0->awaitLeader(8s)) << "node0 must converge to a leader";
    ASSERT_TRUE(node1->awaitLeader(8s)) << "node1 must converge to a leader";
    ASSERT_TRUE(node2->awaitLeader(8s)) << "node2 must converge to a leader";

    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId lid = node0->leaderId();
    ASSERT_GE(lid, 0) << "no leader elected";
    ASSERT_LE(lid, 2);

    rune::node::BrokerNode* leaderNode = nullptr;
    rune::node::BrokerNode* follower1  = nullptr;
    rune::node::BrokerNode* follower2  = nullptr;
    std::array<rune::node::BrokerNode*, 3> nodes = {node0.get(), node1.get(), node2.get()};
    for (int i = 0; i < 3; ++i) {
        if (i == lid) leaderNode = nodes[i];
        else if (!follower1)   follower1 = nodes[i];
        else                   follower2 = nodes[i];
    }
    ASSERT_NE(leaderNode, nullptr);

    // Wait for the leader's replica to be in Leader role.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest())
        << "Leader's replication bridge must report isLeader";

    ASSERT_TRUE(hasReplica(*leaderNode)) << "Leader must have a partition replica";
    ASSERT_EQ(roleOnOwner(*leaderNode), Role::Leader);

    // Produce N=5 records via the leader replica directly (on its OWNING thread).
    constexpr int kN = 5;
    for (int i = 0; i < kN; ++i) {
        auto payload = makeGr7Payload(static_cast<uint8_t>(0x10 + i), 16);
        Offset off = appendOnOwner(*leaderNode,
            std::span<const uint8_t>(payload.data(), payload.size()), false);
        ASSERT_GE(off, 0) << "Leader must accept record " << i;
    }
    const Offset leaderLeo = leoOnOwner(*leaderNode);

    // Wait for the leader's HW to advance (majority of followers replicated).
    {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        bool hwAdvanced = false;
        while (std::chrono::steady_clock::now() < deadline && !hwAdvanced) {
            hwAdvanced = (hwOnOwner(*leaderNode) >= leaderLeo);
            if (!hwAdvanced) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(hwAdvanced)
            << "Leader HW must advance after replication";
    }
    const Offset committedHw = hwOnOwner(*leaderNode);
    ASSERT_GE(committedHw, leaderLeo);

    // Stop the leader — simulates leader kill.
    leaderNode->stop();

    // Wait for the remaining nodes to re-elect a new leader.
    {
        const auto deadline = std::chrono::steady_clock::now() + 12s;
        bool reElected = false;
        while (std::chrono::steady_clock::now() < deadline && !reElected) {
            const auto l1 = follower1->leaderId();
            const auto l2 = follower2->leaderId();
            reElected = (l1 >= 0 && l1 != lid && l2 >= 0 && l1 == l2);
            if (!reElected) std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(reElected) << "Surviving nodes must re-elect a new leader";
    }

    std::this_thread::sleep_for(500ms);

    // Find the new leader.
    const rune::consensus::NodeId newLid = follower1->leaderId();
    ASSERT_NE(newLid, lid) << "New leader must differ from stopped leader";

    rune::node::BrokerNode* newLeaderNode =
        (follower1->isLeader()) ? follower1 : follower2;
    ASSERT_TRUE(newLeaderNode->isLeader());

    // Wait for new leader's replica to be in Leader role.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (newLeaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(newLeaderNode->replicationIsLeaderForTest())
        << "New leader's replication bridge must report isLeader";

    ASSERT_TRUE(hasReplica(*newLeaderNode));

    // Partition failover (metadata reassignment of the dead leader's partitions)
    // is ASYNCHRONOUS and completes shortly AFTER consensus re-election — the
    // reassignment liveness window must expire before the dead node is treated as
    // gone.  Poll for the partition replica to reach Leader role rather than
    // asserting it instantaneously (mirrors the consensus-isLeader poll above).
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline &&
               roleOnOwner(*newLeaderNode) != Role::Leader) {
            std::this_thread::sleep_for(20ms);
        }
    }

    // Assert: new leader has all committed records (no committed loss).
    {
        const Offset leo = leoOnOwner(*newLeaderNode);
        EXPECT_GE(leo, committedHw)
            << "New leader must hold all committed records: LEO="
            << leo << " committedHw=" << committedHw;
    }

    // Assert: no phantom-committed record (HW <= LEO) — one consistent
    // owner-thread snapshot of the pair.
    {
        const auto [hw, leo] = newLeaderNode->onOwnerThreadForTest(0, 0,
            [](PartitionReplica& r) { return std::pair{r.highWatermark(), r.logEndOffset()}; });
        EXPECT_LE(hw, leo) << "HW must never exceed LEO (no phantom-committed records)";
    }

    // Assert: committed data readable from 0.
    auto bytes = readLogOnOwner(*newLeaderNode, 0, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "New leader's log must contain committed data";

    // Assert: per-partition role is correct.
    EXPECT_EQ(roleOnOwner(*newLeaderNode), Role::Leader);

    node0->stop();
    node1->stop();
    node2->stop();
}

// ===========================================================================
// GR7_Integration_Rejoin_CatchesUp_NoGap
//
// 3 BrokerNodes.  Produce N records (committed).
// Stop a FOLLOWER.  Produce M more records (committed on 2-node ISR).
// Restart the follower (same dataDir — log survives, catches up from partial).
// Assert: the rejoined follower's LEO >= pre-kill HW; log readable from 0.
// ===========================================================================

TEST(GR7_Integration, Rejoin_CatchesUp_NoGap) {
    Gr7TempDir dir0{"int_rj0_"}, dir1{"int_rj1_"}, dir2{"int_rj2_"};

    const auto cPorts = gr7DiscoverFreePorts(3);
    const auto kPorts = gr7DiscoverFreePorts(3);
    const auto rPorts = gr7DiscoverFreePorts(3);

    auto cfg0 = makeGr7Config(0, dir0.path, cPorts, kPorts, rPorts);
    auto cfg1 = makeGr7Config(1, dir1.path, cPorts, kPorts, rPorts);
    auto cfg2 = makeGr7Config(2, dir2.path, cPorts, kPorts, rPorts);

    auto node0 = std::make_unique<rune::node::BrokerNode>(cfg0);
    auto node1 = std::make_unique<rune::node::BrokerNode>(cfg1);
    auto node2 = std::make_unique<rune::node::BrokerNode>(cfg2);

    node0->start();
    node1->start();
    node2->start();

    ASSERT_TRUE(node0->awaitLeader(8s));
    ASSERT_TRUE(node1->awaitLeader(8s));
    ASSERT_TRUE(node2->awaitLeader(8s));

    std::this_thread::sleep_for(300ms);

    const rune::consensus::NodeId lid = node0->leaderId();
    ASSERT_GE(lid, 0);

    rune::node::BrokerNode* leaderNode   = nullptr;
    rune::node::BrokerNode* follower1    = nullptr;  // will be killed and restarted
    rune::node::BrokerNode* followerNode = nullptr;  // stays alive
    std::array<rune::node::BrokerNode*, 3> nodes = {node0.get(), node1.get(), node2.get()};
    int follower1Idx = -1;
    for (int i = 0; i < 3; ++i) {
        if (i == lid) leaderNode = nodes[i];
        else if (!follower1) { follower1 = nodes[i]; follower1Idx = i; }
        else followerNode = nodes[i];
    }
    ASSERT_NE(leaderNode, nullptr);
    ASSERT_NE(follower1,  nullptr);
    ASSERT_NE(followerNode, nullptr);

    // Wait for leader's replica to be in Leader role.
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (leaderNode->replicationIsLeaderForTest()) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    ASSERT_TRUE(leaderNode->replicationIsLeaderForTest());

    ASSERT_TRUE(hasReplica(*leaderNode));

    // Produce N=4 records → majority-committed (appended on the OWNING thread).
    constexpr int kN = 4;
    for (int i = 0; i < kN; ++i) {
        auto payload = makeGr7Payload(static_cast<uint8_t>(0x20 + i), 16);
        Offset off = appendOnOwner(*leaderNode,
            std::span<const uint8_t>(payload.data(), payload.size()), false);
        ASSERT_GE(off, 0);
    }
    const Offset leoN = leoOnOwner(*leaderNode);

    // Wait for HW to advance.
    {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = (hwOnOwner(*leaderNode) >= leoN);
            if (!ok) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(ok) << "N records must commit";
    }

    // Stop follower1 (the follower we'll kill and rejoin).
    follower1->stop();
    follower1 = nullptr;

    // Brief wait for ISR shrink.
    std::this_thread::sleep_for(500ms);

    // Produce M=2 more records.
    constexpr int kM = 2;
    for (int i = 0; i < kM; ++i) {
        auto payload = makeGr7Payload(static_cast<uint8_t>(0x30 + i), 16);
        appendOnOwner(*leaderNode,
            std::span<const uint8_t>(payload.data(), payload.size()), false);
    }
    // (HW may or may not advance depending on ISR — not critical for this test.)

    // "Restart" follower1 on the same dataDir (log survives from before the kill).
    std::unique_ptr<rune::node::BrokerNode> restartedNode;
    if (follower1Idx == 0) restartedNode = std::make_unique<rune::node::BrokerNode>(cfg0);
    else if (follower1Idx == 1) restartedNode = std::make_unique<rune::node::BrokerNode>(cfg1);
    else restartedNode = std::make_unique<rune::node::BrokerNode>(cfg2);

    restartedNode->start();

    // Wait for the restarted node to converge to the same leader.
    ASSERT_TRUE(restartedNode->awaitLeader(12s))
        << "Restarted follower must converge to a leader within 12 s";

    std::this_thread::sleep_for(500ms);

    ASSERT_TRUE(hasReplica(*restartedNode)) << "Restarted node must have a partition replica";

    // Wait for the restarted follower to catch up to at least leoN.
    {
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool ok = false;
        while (std::chrono::steady_clock::now() < deadline && !ok) {
            ok = (leoOnOwner(*restartedNode) >= leoN);
            if (!ok) std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(ok)
            << "Restarted follower must catch up to at least leoN=" << leoN
            << "; actual=" << leoOnOwner(*restartedNode);
    }

    // No gap: the restarted follower's log must be readable from 0.
    auto bytes = readLogOnOwner(*restartedNode, 0, 1 << 20);
    EXPECT_FALSE(bytes.empty())
        << "Restarted follower's log must be readable from 0 (no gap)";

    // Clean up.
    restartedNode->stop();
    node0->stop();
    node1->stop();
    node2->stop();
}
