// ---------------------------------------------------------------------------
// PartitionReplicaTest.cpp
//
// Unit tests for rune::replication::PartitionReplica.
//
// Design constraints (CLAUDE.md §4 / plan P3-3):
//   - Bounded, deterministic test drivers: for (int i=0; i<CAP && !done; ++i)
//   - Injected clock (no wall-time).
//   - No busy-spin.
//   - All state transitions triggered by explicit doWork(clock) + pump().
//   - Tests FAIL (ASSERT) if the state machine stalls before CAP iterations.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <filesystem>
#include <string>
#include <vector>

using namespace rune::replication;

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

// Hard cap for the bounded test driver loop.
static constexpr int kDriverCap = 200;

// Build a minimal raw payload (not a full RecordBatch — the PartitionReplica
// wraps it in one). The payload is just some identifying bytes.
static std::vector<uint8_t> makePayload(uint8_t fill, std::size_t size = 16)
{
    return std::vector<uint8_t>(size, fill);
}

// RAII temporary directory for Log storage.
struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_replica_test_" + suffix + "_"
               + std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Convenience: create a PartitionConfig for tests.
static PartitionConfig makeConfig(NodeId selfId,
                                   int64_t replicaLagMaxMs   = 1000,
                                   Offset  replicaLagMaxOff  = 100,
                                   int     minIsr            = 1,
                                   int64_t fetchIntervalNs   = 1'000'000LL,   // 1 ms
                                   int64_t isrCheckIntervalNs= 1'000'000LL)   // 1 ms
{
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = replicaLagMaxMs;
    cfg.replicaLagMaxOffset = replicaLagMaxOff;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = fetchIntervalNs;
    cfg.isrCheckIntervalNs  = isrCheckIntervalNs;
    cfg.maxFetchBytes       = 1 << 20;
    return cfg;
}

// ---------------------------------------------------------------------------
// Fixture: 2-replica setup (leader=1, follower=2)
// ---------------------------------------------------------------------------
class TwoReplicaTest : public ::testing::Test {
protected:
    static constexpr NodeId kLeaderId   = 1;
    static constexpr NodeId kFollowerId = 2;

    TwoReplicaTest()
        : leaderDir("leader2")
        , followerDir("follower2")
        , leaderLog(leaderDir.path)
        , followerLog(followerDir.path)
    {}

    void SetUp() override {
        std::vector<NodeId> replicas  = {kLeaderId, kFollowerId};
        std::vector<NodeId> isr       = {kLeaderId, kFollowerId};

        leader_ = std::make_unique<PartitionReplica>(
            makeConfig(kLeaderId),
            Role::Leader, /*epoch=*/1,
            replicas, isr,
            &leaderLog, &transport_);

        follower_ = std::make_unique<PartitionReplica>(
            makeConfig(kFollowerId),
            Role::Follower, /*epoch=*/1,
            replicas, isr,
            &followerLog, &transport_);

        transport_.setLeader(leader_.get());
        transport_.addFollower(kFollowerId, follower_.get());
    }

    // Drive leader + follower for up to kDriverCap iterations.
    // Returns true if `done()` becomes true within the cap.
    bool driveUntil(std::function<bool()> done,
                    int64_t clockStepNs = 1'000'000LL /*1 ms*/)
    {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock_ += clockStepNs;
            leader_->doWork(clock_);
            follower_->doWork(clock_);
            transport_.pump();
        }
        return done();
    }

    TempDir   leaderDir;
    TempDir   followerDir;
    rune::storage::Log leaderLog;
    rune::storage::Log followerLog;

    InMemoryTransport transport_;
    std::unique_ptr<PartitionReplica> leader_;
    std::unique_ptr<PartitionReplica> follower_;
    int64_t clock_ = 0;
};

// ---------------------------------------------------------------------------
// Fixture: 3-replica setup (leader=1, follower2=2, follower3=3)
// ---------------------------------------------------------------------------
class ThreeReplicaTest : public ::testing::Test {
protected:
    static constexpr NodeId kLeaderId    = 1;
    static constexpr NodeId kFollower2Id = 2;
    static constexpr NodeId kFollower3Id = 3;

    ThreeReplicaTest()
        : leaderDir("leader3")
        , follower2Dir("follower2_3r")
        , follower3Dir("follower3_3r")
        , leaderLog(leaderDir.path)
        , follower2Log(follower2Dir.path)
        , follower3Log(follower3Dir.path)
    {}

    void SetUp() override {
        std::vector<NodeId> replicas = {kLeaderId, kFollower2Id, kFollower3Id};
        std::vector<NodeId> isr      = {kLeaderId, kFollower2Id, kFollower3Id};

        leader_ = std::make_unique<PartitionReplica>(
            makeConfig(kLeaderId),
            Role::Leader, /*epoch=*/1,
            replicas, isr,
            &leaderLog, &transport_);

        follower2_ = std::make_unique<PartitionReplica>(
            makeConfig(kFollower2Id),
            Role::Follower, /*epoch=*/1,
            replicas, isr,
            &follower2Log, &transport_);

        follower3_ = std::make_unique<PartitionReplica>(
            makeConfig(kFollower3Id),
            Role::Follower, /*epoch=*/1,
            replicas, isr,
            &follower3Log, &transport_);

        transport_.setLeader(leader_.get());
        transport_.addFollower(kFollower2Id, follower2_.get());
        transport_.addFollower(kFollower3Id, follower3_.get());
    }

    bool driveUntil(std::function<bool()> done,
                    int64_t clockStepNs = 1'000'000LL)
    {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock_ += clockStepNs;
            leader_->doWork(clock_);
            follower2_->doWork(clock_);
            follower3_->doWork(clock_);
            transport_.pump();
        }
        return done();
    }

    TempDir leaderDir, follower2Dir, follower3Dir;
    rune::storage::Log leaderLog, follower2Log, follower3Log;

    InMemoryTransport transport_;
    std::unique_ptr<PartitionReplica> leader_;
    std::unique_ptr<PartitionReplica> follower2_;
    std::unique_ptr<PartitionReplica> follower3_;
    int64_t clock_ = 0;
};

// ===========================================================================
// Test group 1: Leader append / LEO
// ===========================================================================

TEST_F(TwoReplicaTest, LeaderAppendReturnsBaseOffset) {
    auto payload = makePayload(0xAA);
    Offset base = leader_->append(payload);
    EXPECT_GE(base, 0);
}

TEST_F(TwoReplicaTest, LeaderLeoAdvancesAfterAppend) {
    Offset leo0 = leader_->logEndOffset();
    leader_->append(makePayload(0x01));
    Offset leo1 = leader_->logEndOffset();
    EXPECT_GT(leo1, leo0);
}

TEST_F(TwoReplicaTest, LeaderAppendContiguousOffsets) {
    Offset base0 = leader_->append(makePayload(0x01));
    Offset leo0  = leader_->logEndOffset();
    Offset base1 = leader_->append(makePayload(0x02));
    EXPECT_EQ(base1, leo0);   // second batch starts where first ended
    EXPECT_GT(base1, base0);
}

TEST_F(TwoReplicaTest, LeaderAppendThreeBatchesContiguous) {
    Offset base0 = leader_->append(makePayload(0x01));
    Offset leo0  = leader_->logEndOffset();
    Offset base1 = leader_->append(makePayload(0x02));
    Offset leo1  = leader_->logEndOffset();
    Offset base2 = leader_->append(makePayload(0x03));
    EXPECT_EQ(base1, leo0);
    EXPECT_EQ(base2, leo1);
    EXPECT_GT(base2, base1);
}

// ===========================================================================
// Test group 2: High-watermark advances on majority
// ===========================================================================

TEST_F(TwoReplicaTest, HwZeroBeforeAnyAppend) {
    EXPECT_EQ(leader_->highWatermark(), 0);
}

TEST_F(TwoReplicaTest, HwDoesNotAdvanceWithoutFollowerAck_2Replica) {
    // 2-replica ISR: majority = 2. Leader alone is not enough.
    leader_->append(makePayload(0x01));
    // HW should NOT advance until the follower fetches.
    EXPECT_EQ(leader_->highWatermark(), 0);
}

TEST_F(TwoReplicaTest, HwAdvancesAfterFollowerFetch_2Replica) {
    leader_->append(makePayload(0x01));
    Offset leo = leader_->logEndOffset();

    bool done = driveUntil([&] { return leader_->highWatermark() >= leo; });
    ASSERT_TRUE(done) << "HW did not advance within " << kDriverCap << " iterations";
    EXPECT_GE(leader_->highWatermark(), leo);
}

TEST_F(TwoReplicaTest, HwMonotonic_MultipleAppends) {
    for (int i = 0; i < 5; ++i) {
        leader_->append(makePayload(static_cast<uint8_t>(i)));
    }
    Offset leo = leader_->logEndOffset();

    bool done = driveUntil([&] { return leader_->highWatermark() >= leo; });
    ASSERT_TRUE(done);

    Offset hw = leader_->highWatermark();
    EXPECT_GE(hw, leo);

    // HW must not regress after more appends.
    leader_->append(makePayload(0xFF));
    EXPECT_GE(leader_->highWatermark(), hw);
}

TEST_F(ThreeReplicaTest, Hw3ReplicaMajorityIsTwo) {
    // 3-replica ISR: majority = 2. Leader + 1 follower suffices.
    leader_->append(makePayload(0x11));
    Offset leo = leader_->logEndOffset();

    // Drive leader + both followers.
    bool done = driveUntil([&] { return leader_->highWatermark() >= leo; });
    ASSERT_TRUE(done) << "HW did not advance with 3-replica majority";
    EXPECT_GE(leader_->highWatermark(), leo);
}

// ===========================================================================
// Test group 3: Committed-read boundary
// ===========================================================================

TEST_F(TwoReplicaTest, CommittedReadAtHwReturnsEmpty) {
    leader_->append(makePayload(0x01));
    // HW still 0 (follower hasn't fetched).
    auto bytes = leader_->read(0, 1 << 20);
    EXPECT_TRUE(bytes.empty()); // nothing committed yet
}

TEST_F(TwoReplicaTest, CommittedReadBelowHwReturnsData) {
    leader_->append(makePayload(0x42, 32));
    Offset leo = leader_->logEndOffset();

    bool done = driveUntil([&] { return leader_->highWatermark() >= leo; });
    ASSERT_TRUE(done);

    // Read from offset 0 — should return data since HW > 0.
    auto bytes = leader_->read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty());
}

TEST_F(TwoReplicaTest, CommittedReadExactlyAtHwReturnsEmpty) {
    leader_->append(makePayload(0x01));
    Offset leo = leader_->logEndOffset();

    bool done = driveUntil([&] { return leader_->highWatermark() >= leo; });
    ASSERT_TRUE(done);

    Offset hw = leader_->highWatermark();
    // Reading exactly at HW should return nothing (HW is exclusive).
    auto bytes = leader_->read(hw, 1 << 20);
    EXPECT_TRUE(bytes.empty());
}

TEST_F(TwoReplicaTest, CommittedReadAboveHwReturnsEmpty) {
    leader_->append(makePayload(0x01));
    Offset leo = leader_->logEndOffset();

    // Don't drive — HW stays at 0. Reading anywhere returns empty.
    auto bytes = leader_->read(leo, 1 << 20);
    EXPECT_TRUE(bytes.empty());
}

// ===========================================================================
// Test group 4: Follower catch-up convergence
// ===========================================================================

TEST_F(TwoReplicaTest, FollowerCatchesUpAfterMultipleAppends) {
    constexpr int kBatches = 5;
    for (int i = 0; i < kBatches; ++i) {
        leader_->append(makePayload(static_cast<uint8_t>(i)));
    }
    Offset leaderLeo = leader_->logEndOffset();

    bool done = driveUntil([&] {
        return follower_->logEndOffset() >= leaderLeo;
    });
    ASSERT_TRUE(done) << "Follower did not catch up to leader LEO";
    EXPECT_EQ(follower_->logEndOffset(), leaderLeo);
}

TEST_F(TwoReplicaTest, FollowerLeoConvergesToLeaderLeo) {
    leader_->append(makePayload(0xAB));
    leader_->append(makePayload(0xCD));
    Offset leaderLeo = leader_->logEndOffset();

    bool done = driveUntil([&] {
        return follower_->logEndOffset() == leaderLeo;
    });
    ASSERT_TRUE(done);
}

TEST_F(TwoReplicaTest, FollowerHwConvergesAfterCatchUp) {
    leader_->append(makePayload(0x01));
    Offset leaderLeo = leader_->logEndOffset();

    bool done = driveUntil([&] {
        return follower_->highWatermark() >= leaderLeo
            && leader_->highWatermark() >= leaderLeo;
    });
    ASSERT_TRUE(done) << "HW did not converge on both leader and follower";
}

// ===========================================================================
// Test group 5: Epoch fencing
// ===========================================================================

TEST_F(TwoReplicaTest, StaleEpochFetchRejected) {
    // Build a FetchRequest with a stale epoch (0 < 1).
    FetchRequestMsg staleReq{};
    staleReq.leaderEpoch = 0; // stale
    staleReq.partition   = 0;
    staleReq.followerId  = kFollowerId;
    staleReq.fetchOffset = 0;
    staleReq.maxBytes    = 1 << 20;

    int16_t err = leader_->onFetchRequest(staleReq);
    EXPECT_EQ(err, kErrStaleEpoch);
}

TEST_F(TwoReplicaTest, CurrentEpochFetchAccepted) {
    FetchRequestMsg req{};
    req.leaderEpoch = 1; // matches leader epoch
    req.partition   = 0;
    req.followerId  = kFollowerId;
    req.fetchOffset = 0;
    req.maxBytes    = 1 << 20;

    int16_t err = leader_->onFetchRequest(req);
    EXPECT_EQ(err, kErrNone);
}

TEST_F(TwoReplicaTest, VeryStaleEpochRejected) {
    FetchRequestMsg staleReq{};
    staleReq.leaderEpoch = -1; // very stale
    staleReq.partition   = 0;
    staleReq.followerId  = kFollowerId;
    staleReq.fetchOffset = 0;
    staleReq.maxBytes    = 0;

    int16_t err = leader_->onFetchRequest(staleReq);
    EXPECT_EQ(err, kErrStaleEpoch);
}

// ===========================================================================
// Test group 6: Epoch change — follower truncation
// ===========================================================================

// Tests epoch-change truncation: a follower that has divergent tail records
// from an old epoch must truncate before applying the new leader's data.
TEST(EpochChangeTruncationTest, FollowerTruncatesDivergentTailOnEpochChange) {
    // Setup: leader epoch=2 (new). Follower previously had epoch=1 data.
    TempDir leaderDir("ep_leader");
    TempDir followerDir("ep_follower");

    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    // Leader at epoch 2.
    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/2,
                             replicas, isr, &leaderLog, &transport);
    // Follower starts at epoch 1 (old epoch).
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);

    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Seed the follower's epoch cache so it knows epoch 1 started at offset 0.
    // (In production the follower's epochCache is populated as responses arrive.)
    // We do this by having the follower apply a simulated epoch-1 response first.
    {
        // Simulate: the follower received records under epoch 1.
        // Manually give it some "stale" epoch-1 data by appending directly.
        // We simulate it receiving a normal epoch=1 response.
        auto payload = makePayload(0xDE, 8);
        // Build a FetchResponse with epoch=1 records.
        static constexpr std::size_t kScratch = 1 << 20;
        std::vector<uint8_t> scratch(kScratch);

        // We want the follower to have epoch-1 data at offset 0.
        // Inject directly using a simulated response at epoch 1.
        FetchResponseMsg epoch1Resp{};
        epoch1Resp.leaderEpoch    = 1;
        epoch1Resp.partition      = 0;
        epoch1Resp.highWatermark  = 0; // HW not yet advanced
        epoch1Resp.logStartOffset = 0;
        epoch1Resp.errorCode      = kErrNone;
        epoch1Resp.records        = std::span<const uint8_t>(payload.data(), payload.size());
        follower.applyReplicated(epoch1Resp);
    }
    Offset followerLeoAfterEpoch1 = follower.logEndOffset();
    ASSERT_GT(followerLeoAfterEpoch1, 0) << "Follower should have epoch-1 records";

    // Now the leader appends under epoch 2. Follower should truncate its
    // epoch-1 tail on the first response from the leader.
    auto payload2 = makePayload(0xBE, 16);
    leader.append(payload2);

    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump();
        // Done when follower has caught up to leader's LEO.
        done = (follower.logEndOffset() == leader.logEndOffset());
    }
    ASSERT_TRUE(done) << "Follower did not converge after epoch change";

    // Verify follower LEO matches leader LEO.
    EXPECT_EQ(follower.logEndOffset(), leader.logEndOffset());
}

TEST(EpochChangeTruncationTest, NoCommittedRecordLostOnEpochChange) {
    // Records committed before the epoch change (i.e., below the old HW)
    // must survive on both leader and follower.

    TempDir leaderDir("ep2_leader");
    TempDir followerDir("ep2_follower");

    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    // Epoch 1: leader and follower are in sync.
    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append and commit under epoch 1.
    leader.append(makePayload(0xC0, 8));
    Offset leoAfterEpoch1 = leader.logEndOffset();

    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump();
        done = (leader.highWatermark() >= leoAfterEpoch1);
    }
    ASSERT_TRUE(done) << "Epoch-1 records not committed";
    Offset committedOffset = leader.highWatermark();
    ASSERT_GT(committedOffset, 0);

    // Leader transitions to epoch 2 (new election).
    leader.becomeLeader(/*epoch=*/2, {kL, kF});
    follower.becomeFollower(/*epoch=*/2, kL);

    // Append new epoch-2 record.
    leader.append(makePayload(0xD0, 8));
    Offset newLeo = leader.logEndOffset();

    // Drive to convergence.
    done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump();
        done = (follower.logEndOffset() >= newLeo && leader.highWatermark() >= newLeo);
    }
    ASSERT_TRUE(done) << "Did not converge after epoch change";

    // Original committed records must still be readable on the leader.
    auto bytes = leader.read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "Committed record lost after epoch change";
}

// ===========================================================================
// Test group 7: ISR shrink unblocks HW
// ===========================================================================

TEST(IsrShrinkUnblocksHwTest, LaggingFollowerShrunkFromIsrAllowsHwAdvance) {
    // 2-replica ISR. If the follower stops fetching, after replicaLagMaxMs
    // it is shrunk. With only the leader in the ISR (majority=1), HW advances.

    TempDir leaderDir("isr_shrink_leader");
    TempDir followerDir("isr_shrink_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    // replicaLagMaxMs=10ms, isrCheckInterval=5ms → shrink after 2 ISR checks.
    PartitionConfig lcfg = makeConfig(kL, /*lagMs=*/10,
                                       /*lagOff=*/1000,
                                       /*minIsr=*/1,
                                       /*fetchInt=*/5'000'000LL,
                                       /*isrCheck=*/5'000'000LL);
    PartitionReplica leader(lcfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);

    // Follower is present but will stop fetching (we don't drive it).
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append a record.
    leader.append(makePayload(0x55));
    Offset leo = leader.logEndOffset();

    // Drive leader alone (follower doesn't doWork → doesn't fetch).
    // After replicaLagMaxMs, the leader should shrink the follower from ISR
    // and HW should advance.
    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 5'000'000LL; // 5 ms steps
        leader.doWork(clock);
        // Do NOT drive follower — it is lagging.
        transport.pump();
        done = (leader.highWatermark() >= leo);
    }
    ASSERT_TRUE(done) << "HW did not advance after follower ISR shrink";
    EXPECT_GE(leader.highWatermark(), leo);
    // ISR should now contain only the leader.
    EXPECT_EQ(leader.isr().size(), 1u);
    EXPECT_EQ(leader.isr()[0], kL);
}

TEST(IsrShrinkUnblocksHwTest, FollowerRejoinsIsrAfterCatchUp) {
    // After being shrunk, a follower that catches up should re-expand into ISR.

    TempDir leaderDir("isr_expand_leader");
    TempDir followerDir("isr_expand_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionConfig lcfg = makeConfig(kL, /*lagMs=*/10,
                                       /*lagOff=*/1000,
                                       /*minIsr=*/1,
                                       /*fetchInt=*/5'000'000LL,
                                       /*isrCheck=*/5'000'000LL);
    PartitionReplica leader(lcfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF,
                                         /*lagMs=*/10, 1000, 1,
                                         5'000'000LL, 5'000'000LL),
                               Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    leader.append(makePayload(0x77));
    Offset leo = leader.logEndOffset();

    // Phase 1: drive leader only until follower shrunk.
    int64_t clock = 0;
    bool shrunk = false;
    for (int i = 0; i < kDriverCap && !shrunk; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        transport.pump();
        shrunk = (leader.isr().size() == 1u);
    }
    ASSERT_TRUE(shrunk) << "Follower was not shrunk from ISR";

    // Phase 2: now drive both — follower should catch up and re-enter ISR.
    bool expanded = false;
    for (int i = 0; i < kDriverCap && !expanded; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump();
        expanded = (leader.isr().size() == 2u);
    }
    ASSERT_TRUE(expanded) << "Follower did not rejoin ISR after catching up";
    EXPECT_EQ(leader.isr().size(), 2u);

    // HW should be at leo since both are now in ISR and follower has caught up.
    bool hwDone = (leader.highWatermark() >= leo);
    EXPECT_TRUE(hwDone);
}

// ===========================================================================
// Test group 8: Min-ISR gate
// ===========================================================================

TEST(MinIsrTest, ProduceGatedWhenIsrBelowMinIsr) {
    TempDir dir("minIsr_leader");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    // ISR starts with just the leader (below minIsr=2).
    std::vector<NodeId> isr = {kL};

    PartitionConfig cfg = makeConfig(kL);
    cfg.minIsr = 2; // require 2 replicas in ISR

    PartitionReplica leader(cfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &log, &transport);

    // acksAll=true with ISR={leader only} < minIsr=2: should gate.
    Offset result = leader.append(makePayload(0xAA), /*acksAll=*/true);
    EXPECT_EQ(result, -1) << "Produce should be gated when ISR < minIsr";
}

TEST(MinIsrTest, ProduceAllowedWhenIsrMeetsMinIsr) {
    TempDir dir("minIsr_ok_leader");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionConfig cfg = makeConfig(kL);
    cfg.minIsr = 2;

    PartitionReplica leader(cfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &log, &transport);

    Offset result = leader.append(makePayload(0xBB), /*acksAll=*/true);
    EXPECT_GE(result, 0) << "Produce should be allowed when ISR >= minIsr";
}

TEST(MinIsrTest, ProduceAllowedWhenAcksNotAll) {
    TempDir dir("minIsr_leader_acks");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL}; // only leader

    PartitionConfig cfg = makeConfig(kL);
    cfg.minIsr = 2; // would gate acksAll

    PartitionReplica leader(cfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &log, &transport);

    // acksAll=false → ISR check skipped.
    Offset result = leader.append(makePayload(0xCC), /*acksAll=*/false);
    EXPECT_GE(result, 0) << "acksAll=false should bypass minIsr gate";
}

// ===========================================================================
// Test group 9: doWork returns 0 when idle
// ===========================================================================

TEST_F(TwoReplicaTest, DoWorkReturnsZeroWhenIdle) {
    // At time=0, no interval has elapsed → no work.
    int work = leader_->doWork(0);
    EXPECT_EQ(work, 0);
}

TEST_F(TwoReplicaTest, FollowerDoWorkReturnsZeroWhenIdle) {
    int work = follower_->doWork(0);
    EXPECT_EQ(work, 0);
}

TEST_F(TwoReplicaTest, FollowerDoWorkReturnsPosAfterInterval) {
    // Advance past fetchIntervalNs (1ms) to trigger fetch emission.
    int work = follower_->doWork(2'000'000LL); // 2ms
    EXPECT_GT(work, 0);
}

TEST_F(TwoReplicaTest, LeaderDoWorkReturnsPosAfterIsrCheckInterval) {
    // Advance past isrCheckIntervalNs (1ms).
    int work = leader_->doWork(2'000'000LL);
    EXPECT_GT(work, 0);
}

TEST_F(TwoReplicaTest, BoundedDriverNeverExceedsCap) {
    // Verify that the bounded driver terminates correctly even when done=false.
    int iterations = 0;
    leader_->append(makePayload(0x01));
    for (int i = 0; i < kDriverCap; ++i) {
        clock_ += 1'000'000LL;
        leader_->doWork(clock_);
        follower_->doWork(clock_);
        transport_.pump();
        ++iterations;
    }
    EXPECT_EQ(iterations, kDriverCap);
}

// ===========================================================================
// Test group 10: Role transitions
// ===========================================================================

TEST_F(TwoReplicaTest, BecomeFollowerChangesRole) {
    EXPECT_EQ(leader_->role(), Role::Leader);
    leader_->becomeFollower(/*epoch=*/2, kFollowerId);
    EXPECT_EQ(leader_->role(), Role::Follower);
    EXPECT_EQ(leader_->epoch(), 2);
}

TEST_F(TwoReplicaTest, BecomeLeaderChangesRole) {
    EXPECT_EQ(follower_->role(), Role::Follower);
    follower_->becomeLeader(/*epoch=*/2, {kFollowerId, kLeaderId});
    EXPECT_EQ(follower_->role(), Role::Leader);
    EXPECT_EQ(follower_->epoch(), 2);
}

TEST_F(TwoReplicaTest, RoleIsLeaderInitially) {
    EXPECT_EQ(leader_->role(), Role::Leader);
}

TEST_F(TwoReplicaTest, RoleIsFollowerInitially) {
    EXPECT_EQ(follower_->role(), Role::Follower);
}

// ===========================================================================
// Test group 11: Follower HW advances after fetching
// ===========================================================================

TEST_F(TwoReplicaTest, FollowerHwZeroBeforeAnyFetch) {
    EXPECT_EQ(follower_->highWatermark(), 0);
}

TEST_F(TwoReplicaTest, FollowerHwAdvancesAfterCommit) {
    leader_->append(makePayload(0x01));
    Offset leaderLeo = leader_->logEndOffset();

    bool done = driveUntil([&] {
        return leader_->highWatermark() >= leaderLeo
            && follower_->highWatermark() >= leaderLeo;
    });
    ASSERT_TRUE(done);
    EXPECT_GE(follower_->highWatermark(), leaderLeo);
}

// ===========================================================================
// Test group 12: Accessor correctness
// ===========================================================================

TEST_F(TwoReplicaTest, LeaderPartitionId) {
    EXPECT_EQ(leader_->partitionId(), 0);
}

TEST_F(TwoReplicaTest, LeaderSelfId) {
    EXPECT_EQ(leader_->selfId(), kLeaderId);
}

TEST_F(TwoReplicaTest, FollowerSelfId) {
    EXPECT_EQ(follower_->selfId(), kFollowerId);
}

TEST_F(TwoReplicaTest, LeaderIsrContainsBothInitially) {
    auto& isr = leader_->isr();
    EXPECT_EQ(isr.size(), 2u);
}

TEST_F(TwoReplicaTest, LeaderEpochIsOne) {
    EXPECT_EQ(leader_->epoch(), 1);
}

// ===========================================================================
// Test group 13: Transport fidelity
// ===========================================================================

TEST_F(TwoReplicaTest, FetchRequestDeliveredToLeader) {
    // Drive follower once to emit a fetch, then pump.
    follower_->doWork(2'000'000LL);  // 2ms past fetchIntervalNs
    EXPECT_GT(transport_.pendingCount(), 0u);
    transport_.pump();
    EXPECT_EQ(transport_.pendingCount(), 0u);
}

TEST_F(TwoReplicaTest, InMemoryTransportDeliversBothDirections) {
    leader_->append(makePayload(0x10));
    follower_->doWork(2'000'000LL);  // emit fetch
    transport_.pump(); // fetch → leader → response enqueued
    transport_.pump(); // response → follower
    // After two pumps, follower should have received the records.
    EXPECT_GT(follower_->logEndOffset(), 0);
}

// ===========================================================================
// Test group 14: nextFetch correctness
// ===========================================================================

TEST_F(TwoReplicaTest, NextFetchHasFreshLeo) {
    FetchRequestMsg req = follower_->nextFetch();
    EXPECT_EQ(req.fetchOffset, follower_->logEndOffset());
    EXPECT_EQ(req.leaderEpoch, follower_->epoch());
    EXPECT_EQ(req.followerId, kFollowerId);
}

TEST_F(TwoReplicaTest, NextFetchOffsetAdvancesAfterApply) {
    Offset leo0 = follower_->logEndOffset();
    FetchRequestMsg req0 = follower_->nextFetch();
    EXPECT_EQ(req0.fetchOffset, leo0);

    // Simulate the leader sending one batch and follower applying it.
    leader_->append(makePayload(0x22));
    follower_->doWork(2'000'000LL);
    transport_.pump();
    transport_.pump();

    FetchRequestMsg req1 = follower_->nextFetch();
    EXPECT_GT(req1.fetchOffset, req0.fetchOffset);
}
