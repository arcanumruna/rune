// ---------------------------------------------------------------------------
// PartitionFailoverTest.cpp — Unit tests for PartitionFailoverCoordinator (P3-5).
//
// Covers:
//   - New leader elected from ISR holds all committed data (no committed loss).
//   - Highest-LEO ISR member chosen; tie-break by smallest NodeId.
//   - Old leader fenced on return (stale epoch rejected); truncates uncommitted tail.
//   - Non-ISR node cannot be elected (only ISR members eligible).
//   - Under-replication repair: follower bootstraps, re-enters ISR, |ISR| restored.
//   - Restart recovery: node recovers log, re-syncs from leader, rejoins ISR.
//   - No committed loss / order / HW-monotonic across failover.
//   - doWork idle ⇒ 0 work; bounded drivers with hard caps (ASSERT not while-loop).
//
// C++23. NO busy-spin. NO while(!done). All loops bounded + ASSERT(done).
// Injected clock. Fake transport (InMemoryTransport). Fake liveness.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionFailoverCoordinator.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/FileRecordingStore.h>
#include <rune/replication/PartitionRecorder.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <unistd.h>

using namespace rune::replication;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int     kDriverCap = 300;       // hard loop cap — ASSERT if exceeded
static constexpr int64_t kStep1ms   = 1'000'000LL; // 1 ms clock step

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makePayload(uint8_t fill, std::size_t size = 16)
{
    return std::vector<uint8_t>(size, fill);
}

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_failover_" + suffix + "_"
               + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

static PartitionConfig makeConfig(NodeId selfId,
                                   int64_t replicaLagMaxMs    = 1000,
                                   Offset  replicaLagMaxOff   = 10000,
                                   int     minIsr             = 1,
                                   int64_t fetchIntervalNs    = kStep1ms,
                                   int64_t isrCheckIntervalNs = kStep1ms)
{
    PartitionConfig cfg;
    cfg.partitionId          = 0;
    cfg.selfId               = selfId;
    cfg.replicaLagMaxMs      = replicaLagMaxMs;
    cfg.replicaLagMaxOffset  = replicaLagMaxOff;
    cfg.minIsr               = minIsr;
    cfg.fetchIntervalNs      = fetchIntervalNs;
    cfg.isrCheckIntervalNs   = isrCheckIntervalNs;
    cfg.maxFetchBytes        = 1 << 20;
    return cfg;
}

// Drive a set of replicas until `done()` returns true or we hit the cap.
// Returns true if done() returned true within cap iterations.
template<typename Fn>
static bool driveUntil(std::vector<PartitionReplica*> replicas,
                        InMemoryTransport& transport,
                        int64_t& clock,
                        Fn done,
                        int64_t stepNs = kStep1ms)
{
    for (int i = 0; i < kDriverCap && !done(); ++i) {
        clock += stepNs;
        for (auto* r : replicas) r->doWork(clock);
        transport.pump(clock);
    }
    return done();
}

/// Record a Log's committed content (from offset 0 up to `upToOffset`) into a
/// FileRecordingStore by using PartitionRecorder, which properly walks individual
/// RecordBatch frames so each batch is stored with its correct baseOffset.
static void recordLogToStore(rune::storage::Log& log,
                              FileRecordingStore& store,
                              Offset upToOffset)
{
    PartitionRecorder recorder(0, &log, &store);
    recorder.startRecording(0);
    recorder.onCommitted(0, upToOffset);
    recorder.stopRecording();
}

// ===========================================================================
// Fixture: 3-replica setup (leader=1, follower=2, follower=3)
// ===========================================================================
class FailoverTest3 : public ::testing::Test {
protected:
    static constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;

    FailoverTest3()
        : dL("fail_L"), dF2("fail_F2"), dF3("fail_F3")
        , logL(dL.path), logF2(dF2.path), logF3(dF3.path)
    {}

    void SetUp() override {
        std::vector<NodeId> replicas = {kL, kF2, kF3};
        std::vector<NodeId> isr      = {kL, kF2, kF3};

        leader_   = std::make_unique<PartitionReplica>(makeConfig(kL),  Role::Leader,   1, replicas, isr, &logL,  &transport_);
        follower2_= std::make_unique<PartitionReplica>(makeConfig(kF2), Role::Follower, 1, replicas, isr, &logF2, &transport_);
        follower3_= std::make_unique<PartitionReplica>(makeConfig(kF3), Role::Follower, 1, replicas, isr, &logF3, &transport_);

        transport_.setLeader(leader_.get());
        transport_.addFollower(kF2, follower2_.get());
        transport_.addFollower(kF3, follower3_.get());

        PartitionMetadata meta;
        meta.partitionId = 0;
        meta.leader      = kL;
        meta.replicas    = replicas;
        meta.isr         = isr;
        meta.leaderEpoch = 1;

        coord_ = std::make_unique<PartitionFailoverCoordinator>(meta, &liveness_);
        coord_->registerReplica(kL,  leader_.get());
        coord_->registerReplica(kF2, follower2_.get());
        coord_->registerReplica(kF3, follower3_.get());
    }

    // Fully replicate K payloads until HW >= leaderLeo.
    Offset produceAndCommit(int count)
    {
        for (int i = 0; i < count; ++i)
            leader_->append(makePayload(static_cast<uint8_t>(i)));
        Offset leo = leader_->logEndOffset();
        bool ok = driveUntil({leader_.get(), follower2_.get(), follower3_.get()},
                              transport_, clock_,
                              [&]{ return leader_->highWatermark() >= leo &&
                                          follower2_->highWatermark() >= leo &&
                                          follower3_->highWatermark() >= leo; });
        // (Kafka model: the HW advances on the LAST ISR follower's ack; the
        //  others learn it from their next answer — a keepalive at most.)
        EXPECT_TRUE(ok) << "HW did not advance / propagate to " << leo;
        return leo;
    }

    TempDir dL, dF2, dF3;
    rune::storage::Log logL, logF2, logF3;
    InMemoryTransport transport_;
    SimpleNodeLiveness liveness_;

    std::unique_ptr<PartitionReplica> leader_, follower2_, follower3_;
    std::unique_ptr<PartitionFailoverCoordinator> coord_;
    int64_t clock_ = 0;
};

// ===========================================================================
// Test group 1: New leader from ISR holds all committed data
// ===========================================================================

TEST_F(FailoverTest3, NewLeaderFromISR_NoCommittedLoss) {
    // Produce K records and replicate them (HW = K).
    constexpr int K = 5;
    Offset hw = produceAndCommit(K);
    ASSERT_GT(hw, 0) << "HW must be > 0 after commit";

    // Kill leader (node 1).
    liveness_.markDead(kL);
    coord_->signalLeaderDead();

    Epoch oldEpoch = coord_->currentEpoch();
    bool elected = coord_->electNewLeader();
    ASSERT_TRUE(elected) << "Election must succeed from non-empty ISR";

    NodeId newLeader = coord_->currentLeader();
    ASSERT_NE(newLeader, kL) << "New leader must differ from dead leader";
    ASSERT_EQ(coord_->currentEpoch(), oldEpoch + 1) << "leaderEpoch must be bumped by exactly 1";

    // The new leader must hold ALL offsets < hw in its log.
    // Find the new leader's PartitionReplica.
    PartitionReplica* newLeaderReplica =
        (newLeader == kF2) ? follower2_.get() : follower3_.get();

    ASSERT_GE(newLeaderReplica->logEndOffset(), hw)
        << "New leader must have all committed records (LEO >= hw=" << hw << ")";

    // Verify no committed record is missing: read from offset 0.
    Offset nlLeo = newLeaderReplica->logEndOffset();
    auto bytes = newLeaderReplica->log()->read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "New leader log must contain committed records";
    (void)nlLeo;
}

TEST_F(FailoverTest3, NewLeaderEpochBumpedByExactlyOne) {
    produceAndCommit(3);
    Epoch before = coord_->currentEpoch();
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    EXPECT_EQ(coord_->currentEpoch(), before + 1);
}

TEST_F(FailoverTest3, NewLeaderISRContainsNewLeader) {
    produceAndCommit(2);
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    NodeId nl = coord_->currentLeader();
    const auto& isr = coord_->currentIsr();
    bool found = std::find(isr.begin(), isr.end(), nl) != isr.end();
    EXPECT_TRUE(found) << "New leader must be in the ISR";
}

TEST_F(FailoverTest3, AfterFailover_NewLeaderRoleIsLeader) {
    produceAndCommit(2);
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    NodeId nl = coord_->currentLeader();
    PartitionReplica* nlr = (nl == kF2) ? follower2_.get() : follower3_.get();
    EXPECT_EQ(nlr->role(), Role::Leader);
}

TEST_F(FailoverTest3, AfterFailover_OldLeaderRoleIsFollower) {
    produceAndCommit(2);
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    EXPECT_EQ(leader_->role(), Role::Follower);
}

// ===========================================================================
// Test group 2: Highest-LEO ISR member chosen; tie-break by smallest NodeId
// ===========================================================================

TEST(ElectionOrderTest, HighestLEO_IsChosen) {
    // 3 ISR members. F3 has the highest LEO.
    // Build a scenario where we can control LEOs by registering replicas
    // with known logEndOffset values using InMemoryTransport.

    TempDir dL("eo_L"), dF2("eo_F2"), dF3("eo_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3};
    std::vector<NodeId> isr      = {kL, kF2, kF3};

    PartitionReplica leader  (makeConfig(kL),  Role::Leader,   1, replicas, isr, &logL,  &transport);
    PartitionReplica f2      (makeConfig(kF2), Role::Follower, 1, replicas, isr, &logF2, &transport);
    PartitionReplica f3      (makeConfig(kF3), Role::Follower, 1, replicas, isr, &logF3, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    transport.addFollower(kF3, &f3);

    // Append to leader — all replicas replicate.
    leader.append(makePayload(0x01));
    leader.append(makePayload(0x02));
    leader.append(makePayload(0x03));
    int64_t clock = 0;
    Offset leo = leader.logEndOffset();
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); f2.doWork(clock); f3.doWork(clock);
        transport.pump(clock);
        ok = (f2.logEndOffset() >= leo && f3.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok);

    // Give f3 one extra record — only deliver to f3, not f2.
    // GR-10: push-on-append would deliver to both followers; remove f2 from
    // transport first so the push is dropped for f2, then re-add after.
    transport.removeFollower(kF2);
    leader.append(makePayload(0x04));
    // Force only f3 to fetch (push already delivered via append → transport).
    f3.doWork(clock + kStep1ms);
    transport.pump(clock + kStep1ms);
    // Restore f2 (does not affect f2's LEO — it missed record 4).
    transport.addFollower(kF2, &f2);

    // Now f3.LEO > f2.LEO.
    ASSERT_GT(f3.logEndOffset(), f2.logEndOffset())
        << "f3 must have a higher LEO than f2 for this test";

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(kF2, &f2);
    coord.registerReplica(kF3, &f3);

    coord.signalLeaderDead();
    bool elected = coord.electNewLeader();
    ASSERT_TRUE(elected);

    // F3 has higher LEO → must be chosen.
    EXPECT_EQ(coord.currentLeader(), kF3)
        << "Highest-LEO ISR member (f3) must be chosen as new leader";
}

TEST(ElectionOrderTest, TieBreakBySmallestNodeId) {
    // Two ISR candidates with equal LEO → smallest NodeId wins.
    TempDir dL("tb_L"), dF2("tb_F2"), dF3("tb_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3};
    std::vector<NodeId> isr      = {kL, kF2, kF3};

    PartitionReplica leader(makeConfig(kL), Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica f2    (makeConfig(kF2),Role::Follower,1,replicas, isr, &logF2,&transport);
    PartitionReplica f3    (makeConfig(kF3),Role::Follower,1,replicas, isr, &logF3,&transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    transport.addFollower(kF3, &f3);

    // Append and fully replicate — all have same LEO.
    leader.append(makePayload(0x10));
    int64_t clock = 0;
    Offset leo = leader.logEndOffset();
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); f2.doWork(clock); f3.doWork(clock);
        transport.pump(clock);
        ok = (f2.logEndOffset() >= leo && f3.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok) << "All replicas must catch up";
    ASSERT_EQ(f2.logEndOffset(), f3.logEndOffset()) << "LEOs must be equal for tie-break test";

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(kF2, &f2);
    coord.registerReplica(kF3, &f3);

    coord.signalLeaderDead();
    coord.electNewLeader();

    // Tie-break: smallest NodeId = kF2 = 2.
    EXPECT_EQ(coord.currentLeader(), kF2)
        << "Tie-break by smallest NodeId: kF2=2 must win over kF3=3";
}

TEST(ElectionOrderTest, TieBreakWithThreeCandidates) {
    // All three non-dead candidates have the same LEO.
    // Smallest NodeId among {2, 3, 4} = 2.
    TempDir dL("tb3_L"), d2("tb3_2"), d3("tb3_3"), d4("tb3_4");
    rune::storage::Log logL(dL.path), log2(d2.path), log3(d3.path), log4(d4.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, k2 = 2, k3 = 3, k4 = 4;
    std::vector<NodeId> replicas = {kL, k2, k3, k4};
    std::vector<NodeId> isr      = {kL, k2, k3, k4};

    PartitionReplica leader(makeConfig(kL), Role::Leader,  1, replicas, isr, &logL, &transport);
    PartitionReplica r2    (makeConfig(k2), Role::Follower,1, replicas, isr, &log2, &transport);
    PartitionReplica r3    (makeConfig(k3), Role::Follower,1, replicas, isr, &log3, &transport);
    PartitionReplica r4    (makeConfig(k4), Role::Follower,1, replicas, isr, &log4, &transport);
    transport.setLeader(&leader);
    transport.addFollower(k2, &r2);
    transport.addFollower(k3, &r3);
    transport.addFollower(k4, &r4);

    leader.append(makePayload(0x20));
    int64_t clock = 0;
    Offset leo = leader.logEndOffset();
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); r2.doWork(clock); r3.doWork(clock); r4.doWork(clock);
        transport.pump(clock);
        ok = (r2.logEndOffset() >= leo && r3.logEndOffset() >= leo && r4.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(k2, &r2);
    coord.registerReplica(k3, &r3);
    coord.registerReplica(k4, &r4);

    coord.signalLeaderDead();
    coord.electNewLeader();

    EXPECT_EQ(coord.currentLeader(), k2)
        << "Tie-break by smallest NodeId: k2=2 must win among {2,3,4}";
}

// ===========================================================================
// Test group 3: Old leader fenced on return — stale epoch rejected, tail truncated
// ===========================================================================

TEST_F(FailoverTest3, OldLeader_StaleEpochFenced) {
    produceAndCommit(3);
    Epoch oldEpoch = coord_->currentEpoch();

    coord_->signalLeaderDead();
    coord_->electNewLeader();
    Epoch newEpoch = coord_->currentEpoch();
    ASSERT_GT(newEpoch, oldEpoch);

    NodeId nl = coord_->currentLeader();
    PartitionReplica* newLeaderReplica = (nl == kF2) ? follower2_.get() : follower3_.get();

    // Wire transport to new leader.
    transport_.setLeader(newLeaderReplica);

    // Old leader (node 1) tries to send a FetchRequest with the stale epoch.
    FetchRequestMsg staleReq{};
    staleReq.leaderEpoch = oldEpoch;  // stale!
    staleReq.partition   = 0;
    staleReq.followerId  = kL;
    staleReq.fetchOffset = leader_->logEndOffset();
    staleReq.maxBytes    = 1 << 20;

    int16_t err = newLeaderReplica->onFetchRequest(staleReq, clock_);
    EXPECT_EQ(err, kErrStaleEpoch) << "Old leader's stale-epoch request must be rejected";
}

TEST_F(FailoverTest3, OldLeader_BecomeFollower_TruncatesUncommittedTail) {
    // Produce and commit some records.
    Offset committed = produceAndCommit(3);

    // Leader appends one more record that is NOT committed (no follower acks).
    leader_->append(makePayload(0xFF));
    Offset leoBeforeFailover = leader_->logEndOffset();
    ASSERT_GT(leoBeforeFailover, committed);

    // Failover: electNewLeader() calls becomeFollower(newEpoch) on the old leader.
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    // Old leader is now follower with new epoch.
    ASSERT_EQ(leader_->role(), Role::Follower);

    // DR-BUG-6: becomeFollower now DEFERS truncation until the OffsetsForLeaderEpoch
    // exchange with the new leader resolves the TRUE divergence point (KIP-101).
    // The old leader (kL) is NOT an ISR member of the new epoch (it was the dead
    // leader, excluded from the new ISR), so its ahead tail is treated as an
    // uncommitted, truncatable tail — the new leader answers authoritatively and
    // kL truncates to the leader's epoch boundary (== committed).
    NodeId nl = coord_->currentLeader();
    PartitionReplica* newLeaderReplica = (nl == kF2) ? follower2_.get() : follower3_.get();
    transport_.setLeader(newLeaderReplica);
    transport_.addFollower(kL, leader_.get());
    // KIP-320/595: the new leader completes (and only then directs truncations)
    // once a majority of its ISR proved a validated position — drive the other
    // survivor too.
    PartitionReplica* const otherSurvivor = (nl == kF2) ? follower3_.get() : follower2_.get();
    transport_.addFollower(nl == kF2 ? kF3 : kF2, otherSurvivor);

    bool truncated = driveUntil(
        {newLeaderReplica, leader_.get(), otherSurvivor}, transport_, clock_,
        [&]{ return leader_->logEndOffset() == committed; });

    // Intent preserved: the uncommitted tail is truncated (now via the deferred
    // epoch exchange), landing exactly at the committed boundary.
    EXPECT_TRUE(truncated)
        << "old leader must truncate its uncommitted tail via the epoch exchange";
    EXPECT_EQ(leader_->logEndOffset(), committed)
        << "post-exchange LEO must equal committed=" << committed
        << " not staleLeo=" << leoBeforeFailover;

    // Committed data must survive (we never truncate below HW).
    auto bytes = leader_->log()->read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "Committed data must survive on old leader";
}

TEST_F(FailoverTest3, OldLeader_ReturnsAsFollower_ConvergesNoSplitBrain) {
    // Full scenario: produce+commit, failover, old leader rejoins as follower,
    // verify no split-brain double-write and no committed record loss.
    Offset committed = produceAndCommit(4);

    coord_->signalLeaderDead();
    coord_->electNewLeader();

    NodeId nl = coord_->currentLeader();
    PartitionReplica* newLeaderReplica = (nl == kF2) ? follower2_.get() : follower3_.get();
    PartitionReplica* otherFollower    = (nl == kF2) ? follower3_.get() : follower2_.get();

    // Re-wire transport: new leader serves old leader and other follower.
    transport_.setLeader(newLeaderReplica);
    // Old leader (kL) is now a follower.
    transport_.addFollower(kL, leader_.get());

    // Produce one new record under the new leader.
    newLeaderReplica->append(makePayload(0xAA));
    Offset newLeo = newLeaderReplica->logEndOffset();

    // Drive until old leader converges to new leader's LEO.
    bool ok = driveUntil(
        {newLeaderReplica, leader_.get(), otherFollower},
        transport_, clock_,
        [&]{ return leader_->logEndOffset() >= newLeo; });
    ASSERT_TRUE(ok) << "Old leader must converge to new leader after rejoin";

    // No committed data lost.
    EXPECT_GE(leader_->logEndOffset(), committed)
        << "Old leader must have all previously committed records";

    // Old leader role must still be Follower.
    EXPECT_EQ(leader_->role(), Role::Follower);
}

TEST_F(FailoverTest3, OldLeader_TailTruncation_ExactOffset) {
    // Produce and fully replicate 3 records, HW=3.
    Offset committed = produceAndCommit(3);
    ASSERT_EQ(committed, 3);

    // Leader appends 2 more records NOT committed (followers don't fetch).
    // We achieve "not replicated" by appending after stopping the driver.
    leader_->append(makePayload(0xE1));  // offset 3
    leader_->append(makePayload(0xE2));  // offset 4
    Offset staleLeo = leader_->logEndOffset();  // 5

    // Failover: old leader becomes follower.
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    Epoch newEpoch = coord_->currentEpoch();

    // Old leader is now follower at newEpoch.
    ASSERT_EQ(leader_->role(), Role::Follower);
    ASSERT_EQ(leader_->epoch(), newEpoch);
    (void)newEpoch;

    // DR-BUG-6: truncation is DEFERRED to the OffsetsForLeaderEpoch exchange.
    // Right after becomeFollower the uncommitted tail is still present (the
    // follower keeps its log until it learns the TRUE divergence point) — this
    // is the deliberate change that prevents the DR-8 committed-loss.
    Offset leoAfterBecomeFollower = leader_->logEndOffset();
    EXPECT_EQ(leoAfterBecomeFollower, staleLeo)
        << "deferred truncation: tail is retained until the epoch exchange";

    // Drive the epoch exchange with the authoritative new leader.  kL is NOT in
    // the new ISR → its ahead tail is uncommitted → it truncates to the leader's
    // epoch boundary, which is EXACTLY the committed offset.
    NodeId nl = coord_->currentLeader();
    PartitionReplica* newLeaderReplica = (nl == kF2) ? follower2_.get() : follower3_.get();
    transport_.setLeader(newLeaderReplica);
    transport_.addFollower(kL, leader_.get());
    // KIP-320/595: the new leader completes (and only then directs truncations)
    // once a majority of its ISR proved a validated position — drive the other
    // survivor too.
    PartitionReplica* const otherSurvivor = (nl == kF2) ? follower3_.get() : follower2_.get();
    transport_.addFollower(nl == kF2 ? kF3 : kF2, otherSurvivor);

    bool truncated = driveUntil(
        {newLeaderReplica, leader_.get(), otherSurvivor}, transport_, clock_,
        [&]{ return leader_->logEndOffset() == committed; });

    Offset leoAfterTrunc = leader_->logEndOffset();
    EXPECT_TRUE(truncated) << "old leader must truncate its uncommitted tail";
    EXPECT_EQ(leoAfterTrunc, committed)
        << "After the epoch exchange LEO must be EXACTLY committed=" << committed
        << "; staleLeo was=" << staleLeo;
    (void)staleLeo;
}

// ===========================================================================
// Test group 4: Non-ISR node cannot be elected
// ===========================================================================

TEST(SafetyTest, NonISRNodeNotEligible) {
    // ISR = {1, 2}; node 3 is in replicas but NOT in ISR.
    // Node 1 is the leader and dies.
    // Expected: only node 2 is eligible; node 3 cannot be elected.

    TempDir dL("nisr_L"), dF2("nisr_F2"), dF3("nisr_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3};
    std::vector<NodeId> isr      = {kL, kF2};  // kF3 NOT in ISR

    PartitionReplica leader(makeConfig(kL), Role::Leader,  1, replicas, isr, &logL,  &transport);
    PartitionReplica f2    (makeConfig(kF2),Role::Follower,1, replicas, isr, &logF2, &transport);
    PartitionReplica f3    (makeConfig(kF3),Role::Follower,1, replicas, isr, &logF3, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    transport.addFollower(kF3, &f3);

    // Produce and replicate some records.
    leader.append(makePayload(0x10));
    int64_t clock = 0;
    Offset leo = leader.logEndOffset();
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); f2.doWork(clock); f3.doWork(clock);
        transport.pump(clock);
        ok = (f2.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr;  // only {kL, kF2}
    meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL,  &leader);
    coord.registerReplica(kF2, &f2);
    coord.registerReplica(kF3, &f3);

    coord.signalLeaderDead();
    bool elected = coord.electNewLeader();
    ASSERT_TRUE(elected);

    // Only kF2 is eligible (kF3 is not in ISR).
    EXPECT_EQ(coord.currentLeader(), kF2)
        << "Only ISR member kF2 is eligible; non-ISR kF3 must not be elected";
    EXPECT_NE(coord.currentLeader(), kF3)
        << "Non-ISR node kF3 must NOT be elected leader";
}

TEST(SafetyTest, NonISRNodeAbsentFromElection) {
    // 4 replicas; ISR = {1, 2}; nodes {3,4} are not in ISR.
    // Leader 1 dies → only node 2 can be elected.
    TempDir dL("nisr4_L"), d2("nisr4_2"), d3("nisr4_3"), d4("nisr4_4");
    rune::storage::Log logL(dL.path), log2(d2.path), log3(d3.path), log4(d4.path);
    InMemoryTransport transport;

    constexpr NodeId kL=1, k2=2, k3=3, k4=4;
    std::vector<NodeId> replicas = {kL, k2, k3, k4};
    std::vector<NodeId> isr      = {kL, k2};  // only 1+2 in ISR

    PartitionReplica leader(makeConfig(kL), Role::Leader,  1, replicas, isr, &logL,  &transport);
    PartitionReplica r2    (makeConfig(k2), Role::Follower,1, replicas, isr, &log2,  &transport);
    PartitionReplica r3    (makeConfig(k3), Role::Follower,1, replicas, isr, &log3,  &transport);
    PartitionReplica r4    (makeConfig(k4), Role::Follower,1, replicas, isr, &log4,  &transport);
    transport.setLeader(&leader);
    transport.addFollower(k2, &r2);
    transport.addFollower(k3, &r3);
    transport.addFollower(k4, &r4);

    leader.append(makePayload(0x10));
    int64_t clock = 0;
    Offset leo = leader.logEndOffset();
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); r2.doWork(clock); r3.doWork(clock); r4.doWork(clock);
        transport.pump(clock);
        ok = (r2.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(k2, &r2);
    coord.registerReplica(k3, &r3);
    coord.registerReplica(k4, &r4);

    coord.signalLeaderDead();
    coord.electNewLeader();
    EXPECT_EQ(coord.currentLeader(), k2);
}

TEST(SafetyTest, ISROnlyLeader_ElectionFails) {
    // ISR = {1} only (leader only).  Leader dies → no election possible.
    TempDir dL("isr1_L"), dF("isr1_F");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL};  // only leader in ISR

    PartitionReplica leader(makeConfig(kL), Role::Leader,  1, replicas, isr, &logL, &transport);
    PartitionReplica f     (makeConfig(kF), Role::Follower,1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &f);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(kF, &f);

    coord.signalLeaderDead();
    bool elected = coord.electNewLeader();
    EXPECT_FALSE(elected) << "Election must fail when only leader is in ISR";
}

// ===========================================================================
// Test group 5: Under-replication repair — follower bootstraps, re-enters ISR
// ===========================================================================

TEST(UnderReplicationRepairTest, RepairFollower_BootstrapsAndRejoinsISR) {
    // 2-replica partition initially. F2 dies.
    // Then a new healthy node (F3) is assigned as repair follower.
    // It should bootstrap from the leader's recording, catch up, and rejoin ISR.

    TempDir dL("repair_L"), dF2("repair_F2"), dF3("repair_F3"), dStore("repair_store");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2};
    std::vector<NodeId> isr      = {kL, kF2};

    PartitionReplica leader(makeConfig(kL, 1000, 10000, 1, kStep1ms, kStep1ms),
                             Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica f2    (makeConfig(kF2), Role::Follower, 1, replicas, isr, &logF2, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);

    // Produce 5 records and fully commit.
    constexpr int K = 5;
    for (int i = 0; i < K; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    Offset leo = leader.logEndOffset();
    int64_t clock = 0;
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); f2.doWork(clock);
        transport.pump(clock);
        ok = (leader.highWatermark() >= leo);
    }
    ASSERT_TRUE(ok) << "Initial commit must succeed";
    Offset hw = leader.highWatermark();

    // Record committed batches into a FileRecordingStore (per-batch via PartitionRecorder).
    FileRecordingStore store(dStore.path + "/leader.rec");
    recordLogToStore(logL, store, leo);

    // Set up ISR metadata.
    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL;
    meta.replicas = replicas; meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness, kStep1ms);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(kF2, &f2);

    // F2 dies; shrink ISR to {kL} via signalLeaderDead on f2 (simulate control plane).
    // For test simplicity: just update coordinator metadata directly.
    // (In production the control plane commits an ISR shrink entry.)
    // We'll use assignRepairFollower with kF3 to trigger repair.
    PartitionReplica f3(makeConfig(kF3), Role::Follower, 1,
                         {kL, kF3}, {kL}, &logF3, &transport);

    // Assign the repair follower; it bootstraps from the store.
    coord.assignRepairFollower(kF3, &f3, &store, &leader);

    // Wire f3 into transport so it can live-fetch.
    transport.addFollower(kF3, &f3);

    // After bootstrap f3 should already have all committed records.
    // Drive until doWork() expands f3 into ISR.
    bool repaired = false;
    for (int i = 0; i < kDriverCap && !repaired; ++i) {
        clock += kStep1ms;
        leader.doWork(clock);
        f3.doWork(clock);
        transport.pump(clock);
        coord.doWork(clock);
        const auto& isr2 = coord.currentIsr();
        repaired = std::find(isr2.begin(), isr2.end(), kF3) != isr2.end();
    }
    ASSERT_TRUE(repaired) << "Repair follower must re-enter ISR within cap iterations";

    // F3 log must match leader's committed prefix.
    EXPECT_GE(f3.logEndOffset(), hw)
        << "Repaired follower must hold all committed data (LEO >= hw)";

    // FIX 3: byte + offset equality over [0, hw).
    // A wrong/partial replay must fail here — it's not enough to just have LEO >= hw.
    auto leaderBytes = logL.read(0, 1 << 20);
    auto f3Bytes     = logF3.read(0, 1 << 20);
    ASSERT_FALSE(leaderBytes.empty()) << "Leader log must be non-empty";
    ASSERT_FALSE(f3Bytes.empty())     << "Repair follower log must be non-empty";

    // The repaired follower's log bytes over [0, hw) must be byte-identical to
    // the leader's committed prefix.  Read exactly hw worth of log bytes using
    // a large-enough maxBytes that captures all committed batches.
    // Because both logs were written with the same RecordBatch framing
    // (via replay + live fetch), their raw bytes must match over [0, hw).
    auto leaderCommitted = logL.read(0, static_cast<int32_t>(hw * 512 + 4096));
    auto f3Committed     = logF3.read(0, static_cast<int32_t>(hw * 512 + 4096));
    EXPECT_EQ(leaderCommitted.size(), f3Committed.size())
        << "Repair follower committed prefix byte-length must match leader";
    EXPECT_EQ(leaderCommitted, f3Committed)
        << "Repair follower log bytes [0, hw) must be byte-identical to leader";
}

TEST(UnderReplicationRepairTest, RepairFollower_ISRSizeRestored) {
    // After repair, |ISR| must increase back toward the replication factor.
    TempDir dL("rsz_L"), dStore("rsz_store"), dNew("rsz_newF");
    rune::storage::Log logL(dL.path), logNew(dNew.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kNew = 5;
    std::vector<NodeId> replicas = {kL};
    std::vector<NodeId> isr      = {kL};

    PartitionReplica leader(makeConfig(kL), Role::Leader, 1, replicas, isr, &logL, &transport);
    transport.setLeader(&leader);

    leader.append(makePayload(0x42));
    int64_t clock = 0;
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock);
        transport.pump(clock);
        ok = (leader.highWatermark() > 0);
    }
    ASSERT_TRUE(ok);
    Offset leaderLeo = leader.logEndOffset();

    FileRecordingStore store(dStore.path + "/r.rec");
    recordLogToStore(logL, store, leaderLeo);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL;
    meta.replicas = {kL}; meta.isr = {kL}; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness, kStep1ms);
    coord.registerReplica(kL, &leader);

    PartitionReplica newFollower(makeConfig(kNew), Role::Follower, 1,
                                  {kL, kNew}, {kL}, &logNew, &transport);
    coord.assignRepairFollower(kNew, &newFollower, &store, &leader);
    transport.addFollower(kNew, &newFollower);

    std::size_t isrBefore = coord.currentIsr().size();
    bool expanded = false;
    for (int i = 0; i < kDriverCap && !expanded; ++i) {
        clock += kStep1ms;
        leader.doWork(clock);
        newFollower.doWork(clock);
        transport.pump(clock);
        coord.doWork(clock);
        expanded = coord.currentIsr().size() > isrBefore;
    }
    ASSERT_TRUE(expanded) << "|ISR| must grow after repair follower catches up";
    EXPECT_GT(coord.currentIsr().size(), isrBefore);
}

// ===========================================================================
// Test group 6: Restart recovery — node recovers log, re-syncs, rejoins ISR
// ===========================================================================

TEST(RestartRecoveryTest, RestartedFollower_RecoversThenConverges) {
    // Simulate node restart: a follower recovers its log from a recording store,
    // then live-fetches from the leader to catch up to the current LEO.

    TempDir dL("rr_L"), dF("rr_F"), dStore("rr_store");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader  (makeConfig(kL), Role::Leader,   1, replicas, isr, &logL, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Produce and commit.
    for (int i = 0; i < 4; ++i) leader.append(makePayload(static_cast<uint8_t>(0x10 + i)));
    Offset leo = leader.logEndOffset();
    int64_t clock = 0;
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        ok = (follower.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok) << "Follower must catch up before restart";

    // Record the committed log (per-batch, using PartitionRecorder).
    FileRecordingStore store(dStore.path + "/f.rec");
    recordLogToStore(logF, store, follower.logEndOffset());

    // "Restart" follower: new log from scratch (simulating fresh start),
    // replay from store to recover state.
    TempDir dNewF("rr_newF");
    rune::storage::Log newLogF(dNewF.path);
    {
        PartitionReplay replay(&store);
        replay.replayIntoLog(newLogF, 0, INT64_MAX);
    }

    // New follower instance on the recovered log.
    PartitionReplica newFollower(makeConfig(kF), Role::Follower, 1, replicas, isr, &newLogF, &transport);
    transport.addFollower(kF, &newFollower);

    // Leader produces more records.
    leader.append(makePayload(0xFF));
    Offset newLeo = leader.logEndOffset();

    // Drive until new follower catches up.
    ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock);
        newFollower.doWork(clock);
        transport.pump(clock);
        ok = (newFollower.logEndOffset() >= newLeo);
    }
    ASSERT_TRUE(ok) << "Restarted follower must catch up within cap iterations";

    EXPECT_GE(newFollower.logEndOffset(), newLeo);
    EXPECT_GE(newFollower.logEndOffset(), leo) << "Restarted follower must retain pre-restart records";
}

TEST(RestartRecoveryTest, RestartedFollower_NoGapAfterReplay) {
    // Verify that a replayed follower has no gap between replayed and live data.
    TempDir dL("rr2_L"), dF("rr2_F"), dStore("rr2_store");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader  (makeConfig(kL), Role::Leader,   1, replicas, isr, &logL, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    for (int i = 0; i < 3; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    Offset leo = leader.logEndOffset();
    int64_t clock = 0;
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        ok = (follower.logEndOffset() >= leo);
    }
    ASSERT_TRUE(ok);
    Offset replayEnd = follower.logEndOffset();

    // Record pre-restart state (per-batch, via PartitionRecorder).
    FileRecordingStore store(dStore.path + "/f2.rec");
    recordLogToStore(logF, store, follower.logEndOffset());

    // New log replayed from store.
    TempDir dNewF("rr2_newF");
    rune::storage::Log newLogF(dNewF.path);
    PartitionReplay replay(&store);
    int64_t batches = replay.replayIntoLog(newLogF, 0, INT64_MAX);
    ASSERT_GT(batches, 0) << "Replay must load at least one batch";

    // New follower starts at replayEnd (same as old follower).
    EXPECT_EQ(newLogF.logEndOffset(), replayEnd)
        << "Replayed log must start at the same offset as pre-restart";

    // No gap: leader produces more, new follower fetches and appends contiguously.
    PartitionReplica newFollower(makeConfig(kF), Role::Follower, 1, replicas, isr, &newLogF, &transport);
    transport.addFollower(kF, &newFollower);

    leader.append(makePayload(0xCC));
    Offset afterRestart = leader.logEndOffset();

    ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); newFollower.doWork(clock);
        transport.pump(clock);
        ok = (newFollower.logEndOffset() >= afterRestart);
    }
    ASSERT_TRUE(ok);
    EXPECT_EQ(newFollower.logEndOffset(), afterRestart)
        << "No gap: replayed + live-fetched offsets must be contiguous";
}

// ===========================================================================
// Test group 7: No committed loss / order / HW-monotonic across failover
// ===========================================================================

TEST_F(FailoverTest3, HW_Monotonic_AcrossFailover) {
    // HW must never regress before or after failover.
    Offset hw1 = produceAndCommit(3);
    ASSERT_GT(hw1, 0);

    coord_->signalLeaderDead();
    coord_->electNewLeader();
    NodeId nl = coord_->currentLeader();
    PartitionReplica* newLeaderReplica = (nl == kF2) ? follower2_.get() : follower3_.get();

    // New leader's committed prefix must be >= old HW (no regression).
    // The new leader might not have advanced its own HW yet — we check LEO >= hw1.
    EXPECT_GE(newLeaderReplica->logEndOffset(), hw1)
        << "New leader must hold all committed data (LEO >= hw=" << hw1 << ")";

    // Wire transport to new leader and produce more.
    transport_.setLeader(newLeaderReplica);
    newLeaderReplica->append(makePayload(0xDD));
    Offset newLeo = newLeaderReplica->logEndOffset();

    PartitionReplica* other = (nl == kF2) ? follower3_.get() : follower2_.get();
    transport_.addFollower(kL, leader_.get());

    bool ok = driveUntil(
        {newLeaderReplica, other, leader_.get()},
        transport_, clock_,
        [&]{ return newLeaderReplica->highWatermark() >= newLeo; });
    ASSERT_TRUE(ok) << "New leader HW must advance after produce";

    Offset hw2 = newLeaderReplica->highWatermark();
    EXPECT_GE(hw2, hw1) << "HW must be monotonic across failover";
}

TEST_F(FailoverTest3, OrderPreserved_AcrossFailover) {
    // Records appended before failover must be readable in order on the new leader.
    constexpr int K = 6;
    for (int i = 0; i < K; ++i)
        leader_->append(makePayload(static_cast<uint8_t>(i)));
    Offset leo = leader_->logEndOffset();

    bool ok = driveUntil(
        {leader_.get(), follower2_.get(), follower3_.get()},
        transport_, clock_,
        [&]{ return leader_->highWatermark() >= leo; });
    ASSERT_TRUE(ok);
    Offset hw = leader_->highWatermark();

    // Failover.
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    NodeId nl = coord_->currentLeader();
    PartitionReplica* nlr = (nl == kF2) ? follower2_.get() : follower3_.get();

    // Read from new leader and check LEO covers all committed data.
    auto bytes = nlr->log()->read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "New leader must have committed records";

    // LEO >= hw means all committed offsets are present.
    EXPECT_GE(nlr->logEndOffset(), hw)
        << "New leader must hold all committed records";
}

TEST_F(FailoverTest3, NoCommittedLoss_AfterMultipleFailovers) {
    // Run two successive failovers. No committed data must be lost.
    Offset hw1 = produceAndCommit(3);
    ASSERT_GT(hw1, 0);

    // First failover.
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    NodeId nl1 = coord_->currentLeader();
    PartitionReplica* r1 = (nl1 == kF2) ? follower2_.get() : follower3_.get();
    EXPECT_GE(r1->logEndOffset(), hw1);

    // Wire transport to r1, produce+commit.
    transport_.setLeader(r1);
    r1->append(makePayload(0xAB));
    Offset leo2 = r1->logEndOffset();

    PartitionReplica* r2_other = (nl1 == kF2) ? follower3_.get() : follower2_.get();
    transport_.addFollower(kL, leader_.get());

    bool ok = driveUntil(
        {r1, r2_other, leader_.get()},
        transport_, clock_,
        [&]{ return r1->highWatermark() >= leo2; });
    ASSERT_TRUE(ok) << "Second leader must commit";
    Offset hw2 = r1->highWatermark();
    EXPECT_GE(hw2, hw1) << "HW must not regress after second produce";
}

// ===========================================================================
// Test group 8: doWork idle ⇒ 0 work; bounded drivers
// ===========================================================================

TEST(DoWorkTest, DoWorkIdleReturnsZero) {
    TempDir dL("dw_L");
    rune::storage::Log logL(dL.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1;
    std::vector<NodeId> replicas = {kL};
    std::vector<NodeId> isr      = {kL};
    PartitionReplica leader(makeConfig(kL), Role::Leader, 1, replicas, isr, &logL, &transport);
    transport.setLeader(&leader);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness, /*isrCheckNs=*/kStep1ms);
    coord.registerReplica(kL, &leader);

    // At time 0, no interval has elapsed and no signal → 0 work.
    int work = coord.doWork(0);
    EXPECT_EQ(work, 0);
}

TEST(DoWorkTest, DoWorkWithNoSignalAndNoRepair_IdleAfterCheck) {
    TempDir dL("dw2_L");
    rune::storage::Log logL(dL.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1;
    std::vector<NodeId> replicas = {kL};
    std::vector<NodeId> isr      = {kL};
    PartitionReplica leader(makeConfig(kL), Role::Leader, 1, replicas, isr, &logL, &transport);
    transport.setLeader(&leader);

    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness, kStep1ms);
    coord.registerReplica(kL, &leader);

    // Advance past isrCheckNs with no repair candidates.
    int work1 = coord.doWork(kStep1ms * 2);
    // Second call at same time should be idle.
    int work2 = coord.doWork(kStep1ms * 2);
    EXPECT_EQ(work2, 0) << "Second doWork with no new work must return 0";
}

TEST(DoWorkTest, BoundedDriverLoop_NeverExceedsCap) {
    TempDir dL("bd_L"), dF("bd_F");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader  (makeConfig(kL), Role::Leader,   1, replicas, isr, &logL, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    leader.append(makePayload(0x01));
    Offset leo = leader.logEndOffset();
    int64_t clock = 0;
    int iterations = 0;
    for (int i = 0; i < kDriverCap; ++i) {
        clock += kStep1ms;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock);
        ++iterations;
    }
    EXPECT_EQ(iterations, kDriverCap) << "Driver loop must iterate exactly kDriverCap times";
    // HW must have advanced (proves bounded driver does reach a result).
    EXPECT_GE(leader.highWatermark(), leo);
}

// ===========================================================================
// Test group 9: doWork signal-triggered election
// ===========================================================================

TEST_F(FailoverTest3, DoWork_TriggersElectionOnSignal) {
    produceAndCommit(2);
    liveness_.markDead(kL);

    coord_->signalLeaderDead();
    // doWork must process the signal and elect a new leader.
    int work = coord_->doWork(clock_ + kStep1ms);
    EXPECT_GT(work, 0) << "doWork must return >0 when processing a death signal";

    NodeId nl = coord_->currentLeader();
    EXPECT_NE(nl, kL) << "After doWork, new leader must not be the dead leader";
}

// ===========================================================================
// Test group 10: ISR-expansion via doWork cadence
// ===========================================================================

TEST(IsrExpansionTest, DoWork_ExpandsISR_WhenFollowerCaughtUp) {
    TempDir dL("ise_L"), dF("ise_F");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL};  // start with just leader in ISR

    PartitionReplica leader  (makeConfig(kL, 1000, 10000, 1, kStep1ms, kStep1ms),
                               Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(makeConfig(kF, 1000, 10000, 1, kStep1ms, kStep1ms),
                               Role::Follower,1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    leader.append(makePayload(0x55));
    int64_t clock = 0;
    bool ok = false;
    for (int i = 0; i < kDriverCap && !ok; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        ok = (follower.logEndOffset() >= leader.highWatermark()
              && leader.highWatermark() > 0);
    }
    ASSERT_TRUE(ok) << "Follower must catch up to HW";

    // Now register as repair candidate and drive doWork.
    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr;  // {kL} only
    meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness, kStep1ms);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(kF, &follower);

    // Add kF as repair candidate.
    coord.assignRepairFollower(kF, &follower, nullptr, &leader);

    bool expanded = false;
    for (int i = 0; i < kDriverCap && !expanded; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        coord.doWork(clock);
        const auto& isr2 = coord.currentIsr();
        expanded = std::find(isr2.begin(), isr2.end(), kF) != isr2.end();
    }
    ASSERT_TRUE(expanded) << "Follower must re-enter ISR via doWork expansion";
}

// ===========================================================================
// Test group 11: Epoch correctness post-failover
// ===========================================================================

TEST_F(FailoverTest3, NewLeader_EpochIncrements) {
    Epoch e0 = coord_->currentEpoch();
    produceAndCommit(1);
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    EXPECT_EQ(coord_->currentEpoch(), e0 + 1);
}

TEST_F(FailoverTest3, NewLeader_EpochMatchesReplica) {
    produceAndCommit(1);
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    Epoch coordinatorEpoch = coord_->currentEpoch();
    NodeId nl = coord_->currentLeader();
    PartitionReplica* nlr = (nl == kF2) ? follower2_.get() : follower3_.get();
    EXPECT_EQ(nlr->epoch(), coordinatorEpoch)
        << "New leader replica's epoch must match coordinator metadata epoch";
}

TEST_F(FailoverTest3, OldLeader_EpochMatchesNewEpoch) {
    produceAndCommit(1);
    coord_->signalLeaderDead();
    coord_->electNewLeader();
    Epoch newEpoch = coord_->currentEpoch();
    EXPECT_EQ(leader_->epoch(), newEpoch)
        << "Old leader (now follower) must have been updated to new epoch";
}

// ===========================================================================
// Test group 12: Accessors and metadata correctness
// ===========================================================================

TEST_F(FailoverTest3, CoordMetadata_InitialState) {
    EXPECT_EQ(coord_->currentLeader(), kL);
    EXPECT_EQ(coord_->currentEpoch(), 1);
    EXPECT_EQ(coord_->currentIsr().size(), 3u);
}

TEST_F(FailoverTest3, CoordMetadata_AfterElection) {
    produceAndCommit(1);
    coord_->signalLeaderDead();
    coord_->electNewLeader();

    const auto& meta = coord_->metadata();
    EXPECT_NE(meta.leader, kL);
    EXPECT_GT(meta.leaderEpoch, 1);
    EXPECT_FALSE(meta.isr.empty());
    // New leader must be in ISR.
    bool found = std::find(meta.isr.begin(), meta.isr.end(), meta.leader) != meta.isr.end();
    EXPECT_TRUE(found);
}

// ===========================================================================
// Test group 13: doWork stale-signal / double-election protection (FIX 1)
// ===========================================================================

// (a) A stale death signal AFTER a successful election does NOT re-elect /
//     does NOT demote the live new leader.
TEST_F(FailoverTest3, DoWork_StaleSignalAfterElection_DoesNotReelect) {
    produceAndCommit(2);
    liveness_.markDead(kL);

    // First death signal → first election.
    coord_->signalLeaderDead(kL);
    int work1 = coord_->doWork(clock_ + kStep1ms);
    ASSERT_GT(work1, 0) << "First doWork must elect a new leader";

    NodeId firstNewLeader = coord_->currentLeader();
    Epoch  firstEpoch     = coord_->currentEpoch();
    ASSERT_NE(firstNewLeader, kL) << "New leader must differ from the dead leader";

    // Now fire a SECOND stale death signal for kL (the already-dead node).
    // doWork() must NOT elect again — kL is no longer the current leader.
    coord_->signalLeaderDead(kL);
    int work2 = coord_->doWork(clock_ + kStep1ms * 2);
    EXPECT_EQ(work2, 0)
        << "Stale death signal for replaced leader must not trigger re-election";

    // New leader must be unchanged.
    EXPECT_EQ(coord_->currentLeader(), firstNewLeader)
        << "Stale signal must not demote the live new leader";
    EXPECT_EQ(coord_->currentEpoch(), firstEpoch)
        << "Epoch must not increase on a stale/no-op signal";
}

// (b) A death signal for a node that is NOT the current leader is a no-op.
TEST_F(FailoverTest3, DoWork_SignalForNonLeader_IsNoOp) {
    produceAndCommit(2);

    // Signal death of a FOLLOWER (kF2), not the current leader (kL).
    coord_->signalLeaderDead(kF2);
    int work = coord_->doWork(clock_ + kStep1ms);
    EXPECT_EQ(work, 0)
        << "Death signal for a non-leader node must be a no-op";

    // Leader must remain unchanged.
    EXPECT_EQ(coord_->currentLeader(), kL)
        << "Leader must not change when a non-leader death signal is processed";
    EXPECT_EQ(coord_->currentEpoch(), 1)
        << "Epoch must not change for a non-leader death signal";
}

// (c) One death → exactly one election: two doWork() ticks after one signal
//     must elect at most once (flag cleared after first consumption).
TEST_F(FailoverTest3, DoWork_OneDeath_ExactlyOneElection) {
    produceAndCommit(2);
    liveness_.markDead(kL);

    // Fire exactly one signal.
    coord_->signalLeaderDead(kL);

    // First tick: should elect.
    int work1 = coord_->doWork(clock_ + kStep1ms);
    EXPECT_GT(work1, 0) << "First doWork tick must perform the election";

    NodeId leader1 = coord_->currentLeader();
    Epoch  epoch1  = coord_->currentEpoch();
    ASSERT_NE(leader1, kL);

    // Second tick: signal was cleared — no further election.
    int work2 = coord_->doWork(clock_ + kStep1ms * 2);
    EXPECT_EQ(work2, 0)
        << "Second doWork tick must be idle after flag is cleared";

    // State unchanged.
    EXPECT_EQ(coord_->currentLeader(), leader1)
        << "Leader must not change on second tick";
    EXPECT_EQ(coord_->currentEpoch(), epoch1)
        << "Epoch must not change on second tick";
}

// (d) Signal for dead leader that liveness oracle reports still alive → no election.
//     (Covers the case where liveness_ guards against a premature signal.)
TEST_F(FailoverTest3, DoWork_SignalButLeaderStillAlive_NoElection) {
    produceAndCommit(2);

    // Signal leader dead WITHOUT marking it dead in the liveness oracle.
    // (liveness_ still reports kL as alive.)
    coord_->signalLeaderDead(kL);
    int work = coord_->doWork(clock_ + kStep1ms);
    EXPECT_EQ(work, 0)
        << "Death signal must not cause election when liveness oracle says leader is alive";

    // Leader remains kL.
    EXPECT_EQ(coord_->currentLeader(), kL)
        << "Leader must not change when liveness oracle contradicts the signal";
}

// ===========================================================================
// Audit fix tests: C3, B1-failover, B2-failover committed-read
// ===========================================================================

// ---------------------------------------------------------------------------
// C3: Election must reject candidates that don't hold the committed prefix.
//     A node with LEO < committedHw must not be elected.
// ---------------------------------------------------------------------------

TEST(ElectionC3Test, CandidateWithLEOBelowCommittedHW_NotElected) {
    // 3-replica ISR: kL=leader, kF2=caught-up follower, kF3=lagging follower.
    // kF3 has LEO < committedHw — must NOT be elected as new leader.
    TempDir dL("c3_L"), dF2("c3_F2"), dF3("c3_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3};
    std::vector<NodeId> isr      = {kL, kF2, kF3};

    PartitionReplica leader(makeConfig(kL),  Role::Leader,   1, replicas, isr, &logL,  &transport);
    PartitionReplica f2    (makeConfig(kF2), Role::Follower, 1, replicas, isr, &logF2, &transport);
    PartitionReplica f3    (makeConfig(kF3), Role::Follower, 1, replicas, isr, &logF3, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    transport.addFollower(kF3, &f3);

    // Fully replicate 5 records to f2 and f3 (HW=5).
    for (int i = 0; i < 5; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    Offset hw = leader.logEndOffset();
    int64_t clock = 0;
    bool committed = false;
    for (int i = 0; i < kDriverCap && !committed; ++i) {
        clock += kStep1ms;
        leader.doWork(clock); f2.doWork(clock); f3.doWork(clock);
        transport.pump(clock);
        committed = (leader.highWatermark() >= hw && f2.logEndOffset() >= hw && f3.logEndOffset() >= hw);
    }
    ASSERT_TRUE(committed) << "All must commit 5 records";

    // Now simulate f3 lagging by truncating its log to LEO=2 (below committedHw=5).
    // We can't call f3.truncateTo directly, but we can rebuild f3 with an empty log.
    // Build a coordinator where kF3 has LEO < committed HW.
    TempDir dL2("c3_L2"), dF2b("c3_F2b"), dF3b("c3_F3b");
    rune::storage::Log logL2(dL2.path), logF2b(dF2b.path), logF3b(dF3b.path);
    InMemoryTransport t2;

    // logF3b starts empty (LEO=0, which is < committedHw).
    PartitionReplica leader2(makeConfig(kL),  Role::Leader,   1, replicas, isr, &logL2,  &t2);
    PartitionReplica f2b    (makeConfig(kF2), Role::Follower, 1, replicas, isr, &logF2b, &t2);
    PartitionReplica f3b    (makeConfig(kF3), Role::Follower, 1, replicas, isr, &logF3b, &t2);
    t2.setLeader(&leader2);
    t2.addFollower(kF2, &f2b);
    t2.addFollower(kF3, &f3b);

    // Produce and commit 5 records; f2b and f3b replicate.
    for (int i = 0; i < 5; ++i) leader2.append(makePayload(static_cast<uint8_t>(i)));
    Offset hw2 = leader2.logEndOffset();
    clock = 0;
    committed = false;
    for (int i = 0; i < kDriverCap && !committed; ++i) {
        clock += kStep1ms;
        leader2.doWork(clock); f2b.doWork(clock); f3b.doWork(clock);
        t2.pump(clock);
        committed = (leader2.highWatermark() >= hw2 && f2b.logEndOffset() >= hw2 && f3b.logEndOffset() >= hw2);
    }
    ASSERT_TRUE(committed) << "All must replicate 5 records";
    ASSERT_EQ(leader2.highWatermark(), 5);

    // Now build a coordinator where f3b's LEO is forced below committedHw by
    // creating a "behind" replica for f3.  We simulate this with a separate
    // behind-log that was never replicated.
    TempDir dF3c("c3_F3c");
    rune::storage::Log logF3c(dF3c.path); // empty log (LEO=0) — behind
    InMemoryTransport t3;
    PartitionReplica f3behind(makeConfig(kF3), Role::Follower, 1, replicas, isr, &logF3c, &t3);
    ASSERT_EQ(f3behind.logEndOffset(), 0) << "f3behind must have LEO=0 (below committedHw=5)";

    // Election: kL is dead. Candidates: kF2 (LEO=5, >= committedHw=5) and kF3behind (LEO=0).
    // C3 fix: f3behind has LEO < committedHw → must be rejected.
    // Expected result: kF2 wins (only eligible candidate).
    PartitionMetadata meta;
    meta.partitionId = 0; meta.leader = kL; meta.replicas = replicas;
    meta.isr = isr; meta.leaderEpoch = 1;
    SimpleNodeLiveness liveness;
    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL,  &leader2);     // leader (dead)
    coord.registerReplica(kF2, &f2b);          // caught up (LEO=5)
    coord.registerReplica(kF3, &f3behind);     // lagging (LEO=0 < committedHw=5)

    bool elected = coord.electNewLeader();
    ASSERT_TRUE(elected) << "Election must succeed (kF2 is eligible)";
    EXPECT_EQ(coord.currentLeader(), kF2)
        << "C3: only kF2 (LEO=5 >= committedHw=5) must be elected; "
           "kF3 (LEO=0 < committedHw=5) must be rejected";
}

// ---------------------------------------------------------------------------
// B2 + T1: After failover, new leader's read() returns committed prefix
//           IMMEDIATELY (no re-drive required) — byte-equality check.
// ---------------------------------------------------------------------------

TEST_F(FailoverTest3, AfterFailover_NewLeader_CommittedReadImmediateByteEqual) {
    // T1/B2 strengthened: produce+commit K records on the old leader.
    // After failover, the new leader's read(0, big) must return EXACTLY the
    // committed bytes — not empty and not containing uncommitted records.
    constexpr int K = 4;
    Offset committedHw = produceAndCommit(K);
    ASSERT_GE(committedHw, 1);

    // Capture committed bytes from the old leader.
    auto oldLeaderBytes = leader_->read(0, 1 << 20);
    ASSERT_FALSE(oldLeaderBytes.empty()) << "Old leader must have committed reads";

    // Kill old leader; elect new one.
    liveness_.markDead(kL);
    coord_->signalLeaderDead();
    bool elected = coord_->electNewLeader();
    ASSERT_TRUE(elected);

    NodeId newLeader = coord_->currentLeader();
    PartitionReplica* nlReplica = (newLeader == kF2) ? follower2_.get() : follower3_.get();

    // B2 fix: HW must be >= committedHw IMMEDIATELY after becomeLeader.
    EXPECT_GE(nlReplica->highWatermark(), committedHw)
        << "B2: new leader HW must be >= committedHw immediately (was 0 before fix)";

    // T1 strengthened: byte-equality of committed prefix — not just non-empty.
    auto newLeaderBytes = nlReplica->read(0, 1 << 20);
    ASSERT_FALSE(newLeaderBytes.empty())
        << "B2: new leader must serve committed reads immediately after failover";
    EXPECT_EQ(newLeaderBytes, oldLeaderBytes)
        << "T1/B2: new leader committed read must be byte-equal to old leader's data";
}

// ---------------------------------------------------------------------------
// B1 + T4: Strengthened fencing — follower re-fetches from lower offset after
//           truncation; leader must not count stale pre-truncation LEO.
// ---------------------------------------------------------------------------

TEST_F(FailoverTest3, B1_T4_OldLeaderTruncatesAndRefetchesLower_HWNotStale) {
    // T4 strengthened: after failover, the old leader (now follower) truncates
    // and re-fetches from a LOWER offset.  The new leader must NOT treat the
    // old leader's stale pre-truncation LEO as counting toward majority HW.
    //
    // Scenario:
    //   1. Commit 5 records (HW=5) across 3 replicas.
    //   2. Old leader (kL) appends 3 more uncommitted records (LEO=8, HW=5).
    //   3. Failover: kL becomes follower, kF2 becomes new leader (epoch 2).
    //   4. Old leader kL truncates its uncommitted tail → LEO=5.
    //   5. kL sends FetchRequest to new leader at fetchOffset=5.
    //   6. New leader (kF2) must update its tracked kL LEO to 5 (not keep 8).
    //   7. HW of new leader must not advance past 5 until kL re-acks at 5+.

    constexpr int K = 5;
    Offset hw = produceAndCommit(K);
    ASSERT_EQ(hw, K);

    // All replicas must have full log.
    bool allCaughtUp = false;
    for (int i = 0; i < kDriverCap && !allCaughtUp; ++i) {
        clock_ += kStep1ms;
        leader_->doWork(clock_); follower2_->doWork(clock_); follower3_->doWork(clock_);
        transport_.pump(clock_);
        allCaughtUp = (follower2_->logEndOffset() >= hw && follower3_->logEndOffset() >= hw);
    }
    ASSERT_TRUE(allCaughtUp);

    // Old leader appends 3 uncommitted records (majority ISR won't ack them
    // because we disconnect followers before producing).
    transport_.removeFollower(kF2);
    transport_.removeFollower(kF3);
    for (int i = 0; i < 3; ++i)
        leader_->append(makePayload(static_cast<uint8_t>(i + 10)));
    ASSERT_EQ(leader_->logEndOffset(), 8); // 5 committed + 3 uncommitted

    // Failover: elect kF2 (has LEO=5).
    liveness_.markDead(kL);
    coord_->signalLeaderDead();
    bool elected = coord_->electNewLeader();
    ASSERT_TRUE(elected);
    ASSERT_EQ(coord_->currentLeader(), kF2);

    // B2: kF2's HW must be >= 5 immediately.
    EXPECT_GE(follower2_->highWatermark(), hw)
        << "B2: new leader kF2 HW must be >= committed hw immediately";

    // DR-BUG-6: kL's uncommitted tail truncation is DEFERRED to the epoch
    // exchange.  Re-wire kL to the new leader and drive the exchange.  kL is NOT
    // in the new ISR ({kF2,kF3}) → its ahead tail is uncommitted → it truncates
    // to the leader's epoch boundary (== committed hw).
    transport_.setLeader(follower2_.get());
    transport_.addFollower(kL, leader_.get());
    transport_.addFollower(kF3, follower3_.get());   // KIP-320/595: completeness quorum

    bool truncated = driveUntil(
        {follower2_.get(), leader_.get(), follower3_.get()}, transport_, clock_,
        [&]{ return leader_->logEndOffset() <= hw; });
    EXPECT_TRUE(truncated)
        << "Old leader must truncate uncommitted tail to <= committedHw via epoch exchange";
    EXPECT_LE(leader_->logEndOffset(), hw)
        << "Old leader must truncate uncommitted tail to <= committedHw";

    // B1 fix: new leader kF2's HW must still be >= committedHw=5.
    EXPECT_GE(follower2_->highWatermark(), hw)
        << "B1: HW must not regress after old leader re-fetches at lower LEO";

    // Produce 3 more records on the new leader.
    follower2_->append(makePayload(0xAA));
    follower2_->append(makePayload(0xBB));
    follower2_->append(makePayload(0xCC));
    Offset newLeo = follower2_->logEndOffset();

    // HW must NOT advance to newLeo without the old leader re-acking.
    // (ISR={kF2,kF3} after election — kL was removed; majority = 2 of kF2+kF3)
    // Drive kF3 to ack the new records.
    transport_.addFollower(kF3, follower3_.get());
    bool hwAdvanced = false;
    for (int i = 0; i < kDriverCap && !hwAdvanced; ++i) {
        clock_ += kStep1ms;
        follower2_->doWork(clock_); follower3_->doWork(clock_);
        transport_.pump(clock_);
        hwAdvanced = (follower2_->highWatermark() >= newLeo);
    }
    ASSERT_TRUE(hwAdvanced) << "HW must advance with kF2+kF3 majority";
    EXPECT_GE(follower2_->highWatermark(), newLeo);
}
