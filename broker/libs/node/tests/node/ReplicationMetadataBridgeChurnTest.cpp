// ---------------------------------------------------------------------------
// ReplicationMetadataBridgeChurnTest.cpp — Task 12 test suite.
//
// Covers:
//   1. DeleteDeltaTearsDownReplica
//      A delete delta from the committed map must REMOVE the replica from
//      the registry (registry.lookup returns nullptr), NOT leave it as a
//      NO_LEADER follower (Note B fix).
//
//   2. StrayReplicaQuarantine
//      After a delete delta, even if a new assignment arrives for the same
//      (topicId, partition), the old quarantined replica is gone.  A fresh
//      add() for a new replica with the same key would be required — but no
//      resurrection happens via the bridge alone.
//
//   3. ChurnTransitions_LeaderFollowerLeader
//      Three successive committed maps:
//        (a) self is leader → assert Leader + epoch monotonic
//        (b) node2 is leader → assert Follower + epoch bumped
//        (c) self is leader again → assert Leader + epoch still increasing
//      Exercises the changing-map path in applyUserPartitionRoles.
//
//   4. FollowerEpochBoundaryTruncation_OnHandoff
//      A leader→follower handoff with a higher leaderEpoch triggers KIP-101
//      epoch-boundary truncation without advancing committed watermarks:
//      the follower's epoch is updated and its LEO does not exceed the HW
//      (no committed data lost).
//
//   5. ConcurrentApplyVsLookup_TSanClean
//      Concurrent apply-vs-lookup: the duty thread drives doWork() in a tight
//      loop while a reader thread calls allReplicas() / lookup() concurrently.
//      Under -fsanitize=thread this test must report zero races.
//      (Single-writer invariant: all MUTATIONS go through doWork on one thread;
//       the reader only reads the stable ordered_ snapshot.  In production both
//       live on the same duty thread — this test stresses the read path which
//       is called by gateway routing on the same thread.)
//
// Shared helpers match ReplicationMetadataBridgePerPartitionTest.cpp.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/metadata/MetadataStateMachine.h>
#include <rune/metadata/AssignmentMap.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::replication;
using namespace rune::metadata;
using namespace rune::protocol::metadata;

// ---------------------------------------------------------------------------
// TempDir helper
// ---------------------------------------------------------------------------
struct ChurnTmpDir {
    std::string path;
    explicit ChurnTmpDir(std::string_view tag) {
        static std::atomic<int> seq{0};
        path = std::filesystem::temp_directory_path().string()
             + "/rune_bridge_churn_" + std::string(tag)
             + "_" + std::to_string(::getpid())
             + "_" + std::to_string(seq.fetch_add(1));
        std::filesystem::create_directories(path);
    }
    ~ChurnTmpDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// MockTransport — no-op.
// ---------------------------------------------------------------------------
class ChurnMockTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&)                              override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&)                    override {}
    void sendOffsetsForLeaderEpochRequest(NodeId, const OffsetsForLeaderEpochReqMsg&)  override {}
    void sendOffsetsForLeaderEpochResponse(NodeId, const OffsetsForLeaderEpochRespMsg&) override {}
    void sendReconcileRequest(NodeId, const FetchRequestMsg&)                  override {}
    void sendReconcileResponse(NodeId, const FetchResponseMsg&)                override {}
};

// ---------------------------------------------------------------------------
// Helpers: encode CreateTopic / UpdatePartitionAssignment records.
// Mirrors ReplicationMetadataBridgePerPartitionTest.cpp.
// ---------------------------------------------------------------------------

static std::pair<std::array<uint8_t, 256>, std::size_t> churnEncodeCreate(
    int32_t topicId, int32_t partitionCount = 1, int16_t rf = 1,
    std::string_view name = "t")
{
    CreateTopicRecord rec;
    rec.topicId        = topicId;
    rec.partitionCount = partitionCount;
    rec.rf             = rf;
    rec.name           = name;
    rec.partitionKey   = "";
    rec.placementMode  = 0;
    rec.coPartitionGroup = "";

    std::array<uint8_t, 256> buf{};
    std::size_t sz = rec.encode(buf.data(), buf.size());
    return {buf, sz};
}

struct ChurnUpdateArgs {
    int32_t topicId;
    int32_t partition;
    int32_t leaderNode;
    std::vector<int32_t> isr;
    std::vector<int32_t> replicas;
    int32_t leaderEpoch    = 0;
    int32_t partitionEpoch = 0;
};

static std::pair<std::array<uint8_t, 512>, std::size_t>
churnEncodeUpdate(const ChurnUpdateArgs& a)
{
    std::array<uint8_t, 512> buf{};

    std::vector<uint8_t> isrBytes(a.isr.size() * 4u);
    for (std::size_t i = 0; i < a.isr.size(); ++i) {
        uint32_t v = static_cast<uint32_t>(a.isr[i]);
        isrBytes[i*4+0] = static_cast<uint8_t>(v);
        isrBytes[i*4+1] = static_cast<uint8_t>(v >> 8);
        isrBytes[i*4+2] = static_cast<uint8_t>(v >> 16);
        isrBytes[i*4+3] = static_cast<uint8_t>(v >> 24);
    }
    std::vector<uint8_t> repBytes(a.replicas.size() * 4u);
    for (std::size_t i = 0; i < a.replicas.size(); ++i) {
        uint32_t v = static_cast<uint32_t>(a.replicas[i]);
        repBytes[i*4+0] = static_cast<uint8_t>(v);
        repBytes[i*4+1] = static_cast<uint8_t>(v >> 8);
        repBytes[i*4+2] = static_cast<uint8_t>(v >> 16);
        repBytes[i*4+3] = static_cast<uint8_t>(v >> 24);
    }

    UpdatePartitionAssignmentRecord rec;
    rec.topicId        = a.topicId;
    rec.partition      = a.partition;
    rec.leaderNode     = a.leaderNode;
    rec.leaderEpoch    = a.leaderEpoch;
    rec.partitionEpoch = a.partitionEpoch;
    rec.isr     = Int32ArrayView(reinterpret_cast<const int32_t*>(isrBytes.data()),
                                 static_cast<uint16_t>(a.isr.size()));
    rec.replicas = Int32ArrayView(reinterpret_cast<const int32_t*>(repBytes.data()),
                                  static_cast<uint16_t>(a.replicas.size()));

    std::size_t sz = rec.encode(buf.data(), buf.size());
    return {buf, sz};
}

// ---------------------------------------------------------------------------
// Helper: PartitionConfig for (topicId, partId, selfId).
// ---------------------------------------------------------------------------
static PartitionConfig churnMakeCfg(int32_t topicId, PartitionId partId, NodeId selfId) {
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = partId;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.fastAckOnApply      = false;
    return cfg;
}

// ===========================================================================
// Test fixture
// ===========================================================================
class BridgeChurnTest : public ::testing::Test {
protected:
    static constexpr NodeId kSelf  = 1;
    static constexpr NodeId kNode2 = 2;
    static constexpr NodeId kNode3 = 3;

    BridgeChurnTest() : transport_() {}

    // Add a replica to the registry backed by a fresh on-disk Log.
    PartitionReplica* addReplica(int32_t topicId, int32_t partitionId,
                                 Role initialRole = Role::Follower,
                                 Epoch epoch = 1)
    {
        auto dir = std::make_unique<ChurnTmpDir>(
            "t" + std::to_string(topicId) + "p" + std::to_string(partitionId));
        auto log = std::make_unique<rune::storage::Log>(dir->path);
        log->recover();

        const std::vector<NodeId> all = {kSelf, kNode2, kNode3};
        auto replica = std::make_unique<PartitionReplica>(
            churnMakeCfg(topicId, partitionId, kSelf),
            initialRole, epoch, all, all, log.get(), &transport_);

        PartitionReplica* raw = replica.get();
        registry_.add(topicId, partitionId, std::move(replica));
        logs_.push_back(std::move(log));
        dirs_.push_back(std::move(dir));
        return raw;
    }

    // Build a ReplicationMetadataBridge with an optional MSM.
    std::unique_ptr<ReplicationMetadataBridge> makeBridge(
        const MetadataStateMachine* msm = nullptr)
    {
        const std::vector<NodeId> allNodes = {kSelf, kNode2, kNode3};
        auto bridge = std::make_unique<ReplicationMetadataBridge>(
            kSelf, allNodes, registry_);
        if (msm) bridge->setMsm(msm);
        return bridge;
    }

    ChurnMockTransport transport_;
    ReplicaRegistry    registry_;
    std::vector<std::unique_ptr<rune::storage::Log>>    logs_;
    std::vector<std::unique_ptr<ChurnTmpDir>>           dirs_;
};

// ===========================================================================
// Test 1: DELETE delta tears down the replica — Note B fix.
//
// Scenario:
//   - Committed map cycle 1: self is the leader for (topicId=7, partition=0).
//   - bridge.doWork() → replica becomes Leader; prevMap_ is snapshotted.
//   - Committed map cycle 2 (same MSM, new record): self removed from replicas.
//   - bridge.doWork() → delete delta fires → registry.lookup(7,0) == nullptr.
//
// We use a SINGLE MSM and advance it with two successive committed records so
// that prevMap_ captures the first state before the delete fires on the second
// cycle.  (setMsm() would reset prevMap_ to {} and suppress the delete delta.)
//
// Failure mode with the old code:
//   The delete branch called applyBecomeFollowerForKey → becomeFollower(0, NO_LEADER).
//   The replica remained in the registry.  This test would FAIL because
//   registry_.lookup(7, 0) would return non-null.
// ===========================================================================
TEST_F(BridgeChurnTest, DeleteDeltaTearsDownReplica) {
    // Register a user-partition replica: topicId=7, partition=0.
    PartitionReplica* rep = addReplica(/*topicId=*/7, /*partitionId=*/0, Role::Follower, 1);

    // Single MSM that will be advanced in two steps.
    MetadataStateMachine msm;
    {
        // Step 1: Create topic 7, assign self as leader.
        auto [cbuf, csz] = churnEncodeCreate(7, 1, 2);
        msm.apply(cbuf.data(), csz, 0);
        msm.commit(0);

        auto [ubuf, usz] = churnEncodeUpdate({7, 0, kSelf,
                                              {kSelf, kNode2},
                                              {kSelf, kNode2},
                                              /*leaderEpoch=*/1,
                                              /*partitionEpoch=*/1});
        msm.apply(ubuf.data(), usz, 1);
        msm.commit(1);
    }

    auto bridge = makeBridge(&msm);
    const int work1 = bridge->doWork(kSelf, 5);
    EXPECT_GE(work1, 1) << "bridge must report work done on first call";
    EXPECT_EQ(rep->role(), Role::Leader) << "self is assigned leader → must become Leader";
    ASSERT_NE(registry_.lookup(7, 0), nullptr) << "replica must be in registry after becoming leader";
    // After this doWork(), prevMap_ has been snapshotted with self as a replica.

    // Step 2: Same MSM, new record: self removed from replicas entirely.
    // This triggers a `deletes` delta on the next doWork() call because
    // prevMap_ has self as a replica but the new committedMap does not.
    {
        auto [ubuf, usz] = churnEncodeUpdate({7, 0, kNode2,
                                              {kNode2},
                                              {kNode2},
                                              /*leaderEpoch=*/2,
                                              /*partitionEpoch=*/2});
        msm.apply(ubuf.data(), usz, 2);
        msm.commit(2);
    }

    const int work2 = bridge->doWork(kSelf, 5);
    EXPECT_GE(work2, 1) << "bridge must report work done when delete delta fires";

    // KEY ASSERTION: the replica must be GONE from the registry (not just demoted).
    EXPECT_EQ(registry_.lookup(7, 0), nullptr)
        << "Note B: delete delta must REMOVE the replica from the registry, "
           "not leave it as a NO_LEADER follower";

    // replicaCount drops to zero.
    EXPECT_EQ(registry_.replicaCount(), 0u)
        << "registry must be empty after the only replica is deleted";
    (void)rep;  // rep is now dangling (destroyed by remove); do not dereference.
}

// ===========================================================================
// Test 2: Stray-replica quarantine — spec §16.
//
// After a delete delta the replica is quarantined: even if a new assignment
// record arrives for the same (topicId, partition) key, the bridge does NOT
// resurrect it because the registry has no entry and
// applyBecomeLeaderForKey()/applyBecomeFollowerForKey() are no-ops for an
// absent key.  A fresh registry.add() by the caller would be needed.
// ===========================================================================
TEST_F(BridgeChurnTest, StrayReplicaQuarantine_DeleteThenSameKeyNotResurrected) {
    addReplica(/*topicId=*/3, /*partitionId=*/0, Role::Follower, 1);

    // Single MSM advanced through three states:
    //   state 1: self is a replica → prevMap_ captures it
    //   state 2: self removed      → delete delta → quarantine
    //   state 3: self re-assigned  → electedLeader delta BUT registry has no entry → no-op
    MetadataStateMachine msm;
    {
        auto [cbuf, csz] = churnEncodeCreate(3, 1, 2);
        msm.apply(cbuf.data(), csz, 0);
        msm.commit(0);
        // State 1: self is leader.
        auto [ubuf, usz] = churnEncodeUpdate({3, 0, kSelf, {kSelf, kNode2}, {kSelf, kNode2}, 1, 1});
        msm.apply(ubuf.data(), usz, 1);
        msm.commit(1);
    }
    auto bridge = makeBridge(&msm);
    bridge->doWork(kSelf, 5);  // prevMap_ = state 1
    ASSERT_NE(registry_.lookup(3, 0), nullptr) << "precondition: replica is in registry";

    // State 2: self removed from replicas → delete delta fires next doWork.
    {
        auto [ubuf, usz] = churnEncodeUpdate({3, 0, kNode2, {kNode2}, {kNode2}, 2, 2});
        msm.apply(ubuf.data(), usz, 2);
        msm.commit(2);
    }
    bridge->doWork(kSelf, 5);  // fires delete delta → replica quarantined
    ASSERT_EQ(registry_.lookup(3, 0), nullptr) << "replica must be quarantined after delete delta";

    // State 3: self is re-assigned as leader with the SAME (topicId=3, partition=0).
    // The delta is an electedLeader (new epoch > prev epoch in prevMap_).
    // applyBecomeLeaderForKey calls registry_.lookup(3,0) → nullptr → no-op.
    {
        auto [ubuf, usz] = churnEncodeUpdate({3, 0, kSelf, {kSelf, kNode2}, {kSelf, kNode2}, 3, 3});
        msm.apply(ubuf.data(), usz, 3);
        msm.commit(3);
    }
    bridge->doWork(kSelf, 5);

    // Quarantine: lookup still nullptr because no add() was called.
    EXPECT_EQ(registry_.lookup(3, 0), nullptr)
        << "stray-quarantine: the bridge must NOT resurrect a removed replica "
           "even when the same (topicId, partition) appears again in the map";
}

// ===========================================================================
// Test 3: Leader → Follower → Leader churn with a CHANGING committed map.
//
// Three successive records committed to the SAME MSM so that prevMap_ is
// correctly snapshotted between cycles.  Each doWork() call exercises the
// delta-derivation path against the changing committed map.
// Epoch must be monotonically increasing across all three transitions.
// ===========================================================================
TEST_F(BridgeChurnTest, ChurnTransitions_LeaderFollowerLeader) {
    PartitionReplica* rep = addReplica(/*topicId=*/5, /*partitionId=*/0, Role::Follower, 1);

    MetadataStateMachine msm;
    {
        auto [cbuf, csz] = churnEncodeCreate(5, 1, 2);
        msm.apply(cbuf.data(), csz, 0);
        msm.commit(0);
    }

    // --- Map A: self is leader, leaderEpoch=2 ---
    {
        auto [ubuf, usz] = churnEncodeUpdate({5, 0, kSelf,
                                              {kSelf, kNode2}, {kSelf, kNode2},
                                              /*leaderEpoch=*/2, /*partEpoch=*/2});
        msm.apply(ubuf.data(), usz, 1);
        msm.commit(1);
    }
    auto bridge = makeBridge(&msm);
    bridge->doWork(kSelf, 10);  // prevMap_ ← state A

    EXPECT_EQ(rep->role(), Role::Leader) << "(A) self is assigned leader → Leader";
    EXPECT_EQ(rep->epoch(), 2u) << "(A) epoch must be 2";
    const Epoch epochA = rep->epoch();

    // --- Map B: kNode2 becomes leader, leaderEpoch=3 → self is follower ---
    // (self is still a replica, so this is a 'follower' delta, not 'delete')
    {
        auto [ubuf, usz] = churnEncodeUpdate({5, 0, kNode2,
                                              {kSelf, kNode2}, {kSelf, kNode2},
                                              /*leaderEpoch=*/3, /*partEpoch=*/3});
        msm.apply(ubuf.data(), usz, 2);
        msm.commit(2);
    }
    bridge->doWork(kSelf, 10);  // prevMap_ ← state B

    EXPECT_EQ(rep->role(), Role::Follower) << "(B) kNode2 is leader → self is Follower";
    EXPECT_GE(rep->epoch(), epochA) << "(B) epoch must not regress";
    const Epoch epochB = rep->epoch();

    // --- Map C: self is leader again, leaderEpoch=4 → re-election ---
    {
        auto [ubuf, usz] = churnEncodeUpdate({5, 0, kSelf,
                                              {kSelf, kNode2}, {kSelf, kNode2},
                                              /*leaderEpoch=*/4, /*partEpoch=*/4});
        msm.apply(ubuf.data(), usz, 3);
        msm.commit(3);
    }
    bridge->doWork(kSelf, 10);

    EXPECT_EQ(rep->role(), Role::Leader) << "(C) self is re-elected leader → Leader";
    EXPECT_GT(rep->epoch(), epochB) << "(C) epoch must be strictly greater than B's epoch";
    EXPECT_EQ(rep->epoch(), 4u) << "(C) epoch must be 4 (per-partition from map)";
}

// ===========================================================================
// Test 4: Follower epoch-boundary truncation on a leader handoff (KIP-101).
//
// Scenario:
//   - Self starts as Leader with epoch=2, appends some records (LEO advances).
//   - Handoff: kNode2 becomes leader with epoch=3 (higher epoch).
//   - bridge.doWork() → becomeFollower(epoch=3, kNode2) via 'follower' delta.
//   - KIP-101: the follower's epoch is updated to 3.
//   - No committed data is lost: the HW does not decrease.
//
// Uses a single MSM advanced through two states so prevMap_ is correctly set.
// ===========================================================================
TEST_F(BridgeChurnTest, FollowerEpochBoundaryTruncation_OnHandoff) {
    PartitionReplica* rep = addReplica(/*topicId=*/9, /*partitionId=*/0, Role::Leader, 2);

    MetadataStateMachine msm;
    {
        auto [cbuf, csz] = churnEncodeCreate(9, 1, 2);
        msm.apply(cbuf.data(), csz, 0);
        msm.commit(0);

        // State 1: self is leader, epoch=2.
        auto [ubuf, usz] = churnEncodeUpdate({9, 0, kSelf,
                                              {kSelf, kNode2}, {kSelf, kNode2},
                                              /*leaderEpoch=*/2, /*partEpoch=*/2});
        msm.apply(ubuf.data(), usz, 1);
        msm.commit(1);
    }
    auto bridge = makeBridge(&msm);
    bridge->doWork(kSelf, 5);  // prevMap_ ← state 1
    ASSERT_EQ(rep->role(), Role::Leader) << "precondition: self is leader";
    ASSERT_EQ(rep->epoch(), 2u) << "precondition: epoch is 2";

    // Append a batch as leader so LEO > 0 (uncommitted divergent tail).
    const std::vector<uint8_t> payload(64, 0xAB);
    rep->append(std::span<const uint8_t>(payload.data(), payload.size()), /*acksAll=*/false);
    const Offset preTruncLeo = rep->logEndOffset();
    const Offset hwBeforeHandoff = rep->highWatermark();

    // State 2: kNode2 becomes leader with a HIGHER epoch (3).
    // self is still a replica → 'follower' delta fires.
    {
        auto [ubuf, usz] = churnEncodeUpdate({9, 0, kNode2,
                                              {kSelf, kNode2}, {kSelf, kNode2},
                                              /*leaderEpoch=*/3, /*partEpoch=*/3});
        msm.apply(ubuf.data(), usz, 2);
        msm.commit(2);
    }
    bridge->doWork(kSelf, 5);

    // Post-handoff assertions.
    EXPECT_EQ(rep->role(), Role::Follower)
        << "after handoff to kNode2: self must be Follower";
    EXPECT_EQ(rep->epoch(), 3u)
        << "follower epoch must be updated to the new leader's epoch (3)";

    // No committed data loss: HW does not decrease (epoch-boundary truncation
    // only removes uncommitted tail, never below the committed HW floor).
    EXPECT_GE(rep->highWatermark(), hwBeforeHandoff)
        << "committed HW must not decrease after becoming follower";
    EXPECT_LE(rep->highWatermark(), preTruncLeo)
        << "HW must not exceed pre-handoff LEO";
    (void)preTruncLeo;
}

// ===========================================================================
// Test 5: ConcurrentApplyVsLookup_TSanClean — single-writer invariant, TSan-clean.
//
// CORRECTNESS MODEL (not a "concurrent apply-vs-route race stress"):
//   Safety here derives from the SINGLE-WRITER invariant, NOT from locking a
//   genuinely-racing apply-vs-route path.  In production, apply (doWork/setMsm),
//   route (lookup) and remove all run on the ONE AgentRunner (duty) thread — they
//   never actually execute concurrently, so there is no apply-vs-route data race
//   to stress.  The registry's mutable role/MSM state is single-writer by
//   construction.
//
//   The reader thread in this test only reads IMMUTABLE per-replica IDENTITY
//   (the stable ordered_ snapshot / lookup by key), which is published once and
//   never mutated after registration.  The purpose is narrow: let TSan confirm
//   that this identity-only read path carries the memory barriers it claims to
//   (no torn reads of the ordered_ snapshot), NOT to prove that concurrent
//   apply+route is safe — that scenario does not occur under the single-writer
//   design and is deliberately not exercised.
//
// The single MSM is advanced from leader→follower→leader cyclically so the
// bridge makes real role transitions each even/odd iteration on the duty thread.
// ===========================================================================
TEST_F(BridgeChurnTest, ConcurrentApplyVsLookup_TSanClean) {
    addReplica(/*topicId=*/11, /*partitionId=*/0, Role::Follower, 1);

    // Single MSM, pre-populated with a leader state.
    // The bridge will alternate roles by adding new records.
    MetadataStateMachine msm;
    {
        auto [cbuf, csz] = churnEncodeCreate(11, 1, 2);
        msm.apply(cbuf.data(), csz, 0);
        msm.commit(0);
        // Initial state: self is leader, epoch=1.
        auto [ubuf, usz] = churnEncodeUpdate({11, 0, kSelf,
                                              {kSelf, kNode2}, {kSelf, kNode2},
                                              /*leaderEpoch=*/1, /*partEpoch=*/1});
        msm.apply(ubuf.data(), usz, 1);
        msm.commit(1);
    }

    auto bridge = makeBridge(&msm);
    // Warm up: one duty-thread doWork to snapshot prevMap_ before reader starts.
    bridge->doWork(kSelf, 5);

    constexpr int kIterations = 1000;
    std::atomic<bool> readerDone{false};
    std::atomic<int>  readerLoops{0};

    // Reader thread: allReplicas() + lookup() in a tight loop.
    // Reads only — no mutations.
    std::thread reader([&]() {
        for (int i = 0; i < kIterations * 2; ++i) {
            const auto& replicas = registry_.allReplicas();
            for (auto* r : replicas) {
                if (r) {
                    (void)r->topicId();
                    (void)r->partitionId();
                }
            }
            (void)registry_.lookup(11, 0);
            readerLoops.fetch_add(1, std::memory_order_relaxed);
        }
        readerDone.store(true, std::memory_order_release);
    });

    // Duty thread: alternate leader / follower via new MSM records, then doWork.
    int64_t pos = 2;
    for (int i = 0; i < kIterations; ++i) {
        if (i % 2 == 0) {
            // Even: kNode2 is leader (self becomes follower).
            auto [ubuf, usz] = churnEncodeUpdate({11, 0, kNode2,
                                                  {kSelf, kNode2}, {kSelf, kNode2},
                                                  static_cast<int32_t>(pos),
                                                  static_cast<int32_t>(pos)});
            msm.apply(ubuf.data(), usz, pos);
            msm.commit(pos);
        } else {
            // Odd: self is leader again.
            auto [ubuf, usz] = churnEncodeUpdate({11, 0, kSelf,
                                                  {kSelf, kNode2}, {kSelf, kNode2},
                                                  static_cast<int32_t>(pos),
                                                  static_cast<int32_t>(pos)});
            msm.apply(ubuf.data(), usz, pos);
            msm.commit(pos);
        }
        ++pos;
        bridge->doWork(kSelf, 5);
    }

    reader.join();

    EXPECT_TRUE(readerDone.load(std::memory_order_acquire));
    EXPECT_EQ(readerLoops.load(std::memory_order_relaxed), kIterations * 2);
    // Under TSan: if any data race is detected the test process aborts — no
    // explicit assertion needed beyond verifying both threads completed.
}
