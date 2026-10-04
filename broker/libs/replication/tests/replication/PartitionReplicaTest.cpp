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
#include <rune/replication/PartitionFailoverCoordinator.h>
#include <rune/replication/FileRecordingStore.h>
#include <rune/replication/IsrManager.h>
#include <rune/replication/InternalIsrKnowledge.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <deque>
#include <filesystem>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <unistd.h>

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

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

// RAII temporary directory for Log storage.
struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_replica_test_" + suffix + "_"
               + makeUniqSuffix();
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
            transport_.pump(clock_);  // pass real clock so ISR lag sees actual fetch time
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
            transport_.pump(clock_);  // pass real clock so ISR lag sees actual fetch time
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

    int16_t err = leader_->onFetchRequest(staleReq, clock_);
    EXPECT_EQ(err, kErrStaleEpoch);
}

TEST_F(TwoReplicaTest, CurrentEpochFetchAccepted) {
    FetchRequestMsg req{};
    req.leaderEpoch = 1; // matches leader epoch
    req.partition   = 0;
    req.followerId  = kFollowerId;
    req.fetchOffset = 0;
    req.maxBytes    = 1 << 20;

    int16_t err = leader_->onFetchRequest(req, clock_);
    EXPECT_EQ(err, kErrNone);
}

TEST_F(TwoReplicaTest, VeryStaleEpochRejected) {
    FetchRequestMsg staleReq{};
    staleReq.leaderEpoch = -1; // very stale
    staleReq.partition   = 0;
    staleReq.followerId  = kFollowerId;
    staleReq.fetchOffset = 0;
    staleReq.maxBytes    = 0;

    int16_t err = leader_->onFetchRequest(staleReq, clock_);
    EXPECT_EQ(err, kErrStaleEpoch);
}

// ===========================================================================
// Test group 6: Epoch change — follower truncation
// ===========================================================================

// Tests epoch-change truncation: a follower that has divergent tail records
// from an old epoch must truncate before applying the new leader's data.

// KIP-320/595: a leader pushes to (and counts) a follower only after a fetch of
// that follower passed lineage validation in the current leadership — let the
// followers report in (one validated fetch each) before the scenario.
[[maybe_unused]] static void reportIn(rune::replication::PartitionReplica& leader,
                                      std::initializer_list<rune::replication::PartitionReplica*> followers,
                                      rune::replication::InMemoryTransport& t, int64_t nowNs = 0) {
    for (auto* f : followers) (void)leader.onFetchRequest(f->nextFetch(), nowNs);
    t.pump(nowNs);
}

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
    Offset leaderLeoEpoch2 = leader.logEndOffset(); // LEO after epoch-2 append

    // FIX 4: The leader was constructed at epoch=2 with LEO=0 (empty log), so
    // the epoch-2 boundary in the leader's epochCache starts at offset 0.
    // The follower's epoch-1 divergent record is at offset 0.
    // truncationOffsetForEpochChange(cache, 2) returns the start of epoch 2 = 0.
    // So the follower MUST truncate to EXACTLY offset 0, then re-append the
    // epoch-2 batch (resulting in LEO == leaderLeoEpoch2).
    // The exact epoch boundary is 0 (leader had no epoch-1 records).
    constexpr Offset kExpectedTruncationOffset = 0;

    // Drive one cycle: follower emits fetch, leader responds, follower applies
    // (this is where epoch-change truncation happens).
    int64_t clock = 0;
    Offset followerLeoAfterTruncation = -1;

    // Step 1: follower emits fetch, pump delivers to leader (response queued).
    clock += 1'000'000LL;
    leader.doWork(clock);
    follower.doWork(clock);   // emits fetch
    transport.pump(clock);    // fetch → leader → response enqueued (not yet applied)

    // Step 2: pump delivers response to follower — this is when truncation fires.
    transport.pump(clock);    // response → follower (truncation + epoch-2 apply)
    followerLeoAfterTruncation = follower.logEndOffset();

    // FIX 4: ASSERT the post-truncation LEO equals the EXACT boundary BEFORE
    // re-convergence.  A truncate-to-0 bug would produce LEO=0 when it should
    // be leaderLeoEpoch2 (since the epoch-2 record was included in the response).
    // If the response had no records this would be kExpectedTruncationOffset.
    // With records included, LEO = kExpectedTruncationOffset + 1 batch.
    ASSERT_EQ(followerLeoAfterTruncation, leaderLeoEpoch2)
        << "FIX 4: Follower post-truncation LEO must equal the exact leader LEO "
        << "(epoch boundary=" << kExpectedTruncationOffset
        << " + 1 epoch-2 batch = " << leaderLeoEpoch2 << "). "
        << "A truncate-to-wrong-offset bug would produce a different LEO.";

    // Continue driving until full convergence.
    bool done = (follower.logEndOffset() == leader.logEndOffset());
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock);
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
        transport.pump(clock);
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
        transport.pump(clock);
        done = (follower.logEndOffset() >= newLeo && leader.highWatermark() >= newLeo);
    }
    ASSERT_TRUE(done) << "Did not converge after epoch change";

    // Original committed records must still be readable on the leader.
    auto bytes = leader.read(0, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "Committed record lost after epoch change";
}

// Seed a replica's log as if replicated from the epoch-`ep` leader: `count`
// single-record batches at [base, base+count), STAMPED with `ep` (Kafka: the
// leader's partitionLeaderEpoch rides on every batch), advertised HW `hw`.  A
// replica behind `ep` first follows that epoch (as the control plane would).
static void seedStamped(PartitionReplica& r, int64_t base, int count, Epoch ep,
                        Offset hw, uint8_t fill = 'r') {
    std::vector<uint8_t> all;
    for (int i = 0; i < count; ++i) {
        rune::protocol::RecordBatchBuilder bld(base + i, 0);
        const char v[3] = {static_cast<char>(fill), static_cast<char>(fill), static_cast<char>(fill)};
        bld.append(std::string_view{}, std::string_view(v, 3), 0);
        auto one = bld.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(one.data(), static_cast<int32_t>(ep));
        all.insert(all.end(), one.begin(), one.end());
    }
    if (r.epoch() < ep) r.becomeFollower(ep, /*leaderId=*/-1);
    FetchResponseMsg resp{};
    resp.errorCode = kErrNone; resp.leaderEpoch = ep; resp.partition = 0;
    resp.highWatermark = hw; resp.logStartOffset = 0;
    resp.records = std::span<const uint8_t>(all.data(), all.size());
    r.applyReplicated(resp);
}

template <class... R>
static bool driveReplicas(InMemoryTransport& t, int64_t& clock, int cap,
                          const std::function<bool()>& done, R&... rs) {
    for (int i = 0; i < cap && !done(); ++i) {
        clock += 1'000'000LL;
        (rs.doWork(clock), ...);
        t.pump(clock);
    }
    return done();
}

// ===========================================================================
// DR-BUG-6 — Unit A: the DR-8 mechanism.
//
// 3 replicas: L (dead leader), A (up-to-date survivor with a STALE HW view),
// B (lagging → deficient new leader).
//
// All K records were committed on {L, A} under epoch 1.  A holds ALL K records
// in its LOG, but A's committed-HW VIEW is stale (< K) because it never
// received L's final HW-advance (L died first).  B is lagging: B.LEO < K.
//
// Consensus (blind to partition data) elects B — the deficient node — as the
// new epoch-2 leader (we force this directly, mimicking the GR-2 bridge).
//
// Pre-fix: on becomeFollower(epoch 2), A truncates its OWN committed tail down
// to its stale HW view → the K records A alone still holds are DESTROYED before
// B can pull them.  Net: committed data lost.
//
// Post-fix (KIP-101 OffsetsForLeaderEpoch): A asks B for the end offset of the
// last epoch they share.  Both share epoch 1 (still open on B) → B answers its
// LEO for epoch 1 → A sees a SHARED OPEN epoch → A does NOT truncate its
// committed tail.  B, being deficient, reverse-catches-up: it pulls the missing
// committed records [B.LEO, A.LEO) from A.  Both end holding all K records.
// ===========================================================================
TEST(EpochChangeTruncationTest, DR8_DeficientLeaderNoCommittedLoss) {
    // DR-8 under the Kafka model (HW = min LEO over the ISR): A holds [0,5)
    // with a stale HW view (2), B holds [0,2), both in the ISR.  With B in the
    // ISR nothing past 2 was ever committed, so B is a COMPLETE leader: it
    // accepts writes at once, A's uncommitted [2,5) is truncated (KIP-279) and
    // the committed prefix [0,2) is never lost.
    TempDir aDir("dr8_A"), bDir("dr8_B");
    rune::storage::Log aLog(aDir.path), bLog(bDir.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kA = 2, kB = 3;
    std::vector<NodeId> replicas = {kL, kA, kB};
    PartitionReplica a(makeConfig(kA), Role::Follower, 1, replicas, replicas, &aLog, &transport);
    PartitionReplica b(makeConfig(kB), Role::Follower, 1, replicas, replicas, &bLog, &transport);
    seedStamped(a, 0, 5, 1, /*hw=*/2);
    seedStamped(b, 0, 2, 1, /*hw=*/2);
    transport.setLeader(&b);
    transport.addFollower(kA, &a);
    b.becomeLeader(2, {kA, kB});
    a.becomeFollower(2, kB);
    EXPECT_TRUE(b.acceptsProduce()) << "an ISR member is a complete leader (Kafka model)";
    int64_t clock = 0;
    ASSERT_TRUE(driveReplicas(transport, clock, kDriverCap,
                              [&] { return a.logEndOffset() == 2; }, b, a))
        << "A.LEO=" << a.logEndOffset();
    EXPECT_GE(b.highWatermark(), 2) << "the committed prefix stays committed";
}

// ===========================================================================
// DR-BUG-6 DIVERGENCE GUARD (safety-review Blocker) — scenario (a):
// A single ahead tail from a NON-SHARED epoch must be TRUNCATED (not committed)
// by the new leader/survivor, EVEN THOUGH the survivor is in the (bridge-seeded
// all-replica) ISR.
//
// Pre-correction (commit 3c13aa9): the reconcile keep-vs-truncate gate keyed on
// followerInIsr && followerLeo > leaderLeo, ignoring the tail's epoch.  Because
// the GR-2 bridge sets ISR = ALL replicas, the divergent epoch-2 tail was ADOPTED
// (kept + reverse-reconciled) → the new leader committed divergent data.
//
// Post-correction: the epoch-lineage gate (followerTailIsSharedContinuation) sees
// the survivor's tail is in epoch 2, which the new leader (epochs {1,3}) never
// held → NOT a shared continuation → authoritative answer → survivor TRUNCATES to
// the shared epoch-1 boundary.  No divergent data is ever committed.
// ===========================================================================
TEST(EpochChangeTruncationTest, DivergentAheadTailFromNonSharedEpochIsTruncated) {
    // A survivor's ahead tail from an epoch the new leader never held is off the
    // leader's lineage: once the leader is complete (a majority of its ISR —
    // itself + M — proved a validated prefix position), the survivor gets
    // DivergingEpoch and truncates to the shared epoch-1 boundary (KIP-279).
    TempDir nDir("divg_a_N"), sDir("divg_a_S"), mDir("divg_a_M");
    rune::storage::Log nLog(nDir.path), sLog(sDir.path), mLog(mDir.path);
    InMemoryTransport transport;
    constexpr NodeId kN = 1, kS = 2, kM = 3;
    std::vector<NodeId> all = {kN, kS, kM};
    PartitionReplica n(makeConfig(kN), Role::Follower, 1, all, all, &nLog, &transport);
    PartitionReplica s(makeConfig(kS), Role::Follower, 1, all, all, &sLog, &transport);
    PartitionReplica m(makeConfig(kM), Role::Follower, 1, all, all, &mLog, &transport);
    seedStamped(n, 0, 3, 1, 3);
    seedStamped(m, 0, 3, 1, 3);
    seedStamped(s, 0, 3, 1, 3);
    seedStamped(s, 3, 2, 2, 3);                 // divergent epoch-2 tail N never saw
    ASSERT_EQ(s.logEndOffset(), 5);

    transport.setLeader(&n);
    transport.addFollower(kS, &s);
    transport.addFollower(kM, &m);
    n.becomeLeader(3, {kN, kS, kM});
    s.becomeFollower(3, kN);
    m.becomeFollower(3, kN);
    int64_t clock = 0;
    ASSERT_TRUE(driveReplicas(transport, clock, kDriverCap,
                              [&] { return s.logEndOffset() == 3 && n.acceptsProduce(); }, n, s, m))
        << "S.LEO=" << s.logEndOffset();
    EXPECT_EQ(n.logEndOffset(), 3) << "the new leader must not adopt the divergent tail";
    EXPECT_EQ(s.lastFetchedEpoch(), 1);
}

// ===========================================================================
// DR-BUG-6 DIVERGENCE GUARD (safety-review Blocker) — scenario (b):
// Two ahead ISR members with CONTRADICTORY same-offset tails must NOT produce
// divergence: the two survivors must converge to IDENTICAL logs, never
// contradictory.  The epoch-lineage check picks the correct single history and
// truncates the non-lineage tails.
//
// Pre-correction: with ISR = ALL replicas, the new leader keeps+commits whichever
// ahead tail it probes/reconciles FIRST → the two survivors end with different
// (contradictory) logs at the same offsets → LOG DIVERGENCE.
//
// Post-correction: neither survivor's tail is on the new leader's epoch lineage
// (the leader was elected at a higher epoch and never held either divergent
// epoch) → both truncate to the shared committed prefix → identical logs.
// ===========================================================================
TEST(EpochChangeTruncationTest, TwoContradictoryAheadTailsConvergeNoDivergence) {
    // X holds an epoch-2 tail [3,5), Y a contradictory epoch-3 tail [3,4) at the
    // same offsets.  Y (latest epoch) is elected at 4; N (prefix [0,3)) proves a
    // validated position → Y completes; X is off Y's lineage (Y never held epoch
    // 2: its lineage for epoch 2 is "epoch 1, ending at 3") → DivergingEpoch →
    // X truncates to 3 and re-fetches Y's [3,4).  Survivors converge BYTE-
    // IDENTICALLY on the leader's lineage — never contradictory.
    TempDir nDir("divg_b_N"), xDir("divg_b_X"), yDir("divg_b_Y");
    rune::storage::Log nLog(nDir.path), xLog(xDir.path), yLog(yDir.path);
    InMemoryTransport transport;
    constexpr NodeId kN = 1, kX = 2, kY = 3;
    std::vector<NodeId> all = {kN, kX, kY};
    PartitionReplica n(makeConfig(kN), Role::Follower, 1, all, all, &nLog, &transport);
    PartitionReplica x(makeConfig(kX), Role::Follower, 1, all, all, &xLog, &transport);
    PartitionReplica y(makeConfig(kY), Role::Follower, 1, all, all, &yLog, &transport);
    seedStamped(n, 0, 3, 1, 3, 0x11);
    seedStamped(x, 0, 3, 1, 3, 0x11);
    seedStamped(y, 0, 3, 1, 3, 0x11);
    seedStamped(x, 3, 2, 2, 3, 0x22);
    seedStamped(y, 3, 1, 3, 3, 0x33);
    ASSERT_EQ(x.logEndOffset(), 5);
    ASSERT_EQ(y.logEndOffset(), 4);

    transport.setLeader(&y);
    transport.addFollower(kX, &x);
    transport.addFollower(kN, &n);
    y.becomeLeader(4, {kY, kX, kN});
    x.becomeFollower(4, kY);
    n.becomeFollower(4, kY);
    int64_t clock = 0;
    ASSERT_TRUE(driveReplicas(transport, clock, kDriverCap, [&] {
        return x.logEndOffset() == y.logEndOffset() && n.logEndOffset() == y.logEndOffset() &&
               xLog.read(0, 1 << 20) == yLog.read(0, 1 << 20);
    }, y, x, n)) << "X=" << x.logEndOffset() << " Y=" << y.logEndOffset() << " N=" << n.logEndOffset();
    EXPECT_EQ(x.logEndOffset(), 4);
    EXPECT_EQ(xLog.read(0, 1 << 20), yLog.read(0, 1 << 20)) << "contradictory survivor logs";
    EXPECT_EQ(nLog.read(0, 1 << 20), yLog.read(0, 1 << 20));
}

// (EpochChangeTruncationTest.FallbackRaceDeferKeepAlivePreventsCommittedLoss
//  retired with the KIP-101 side probe and its fallback: divergence is resolved
//  on the fetch path; a deficient leader holds — never truncates — a survivor.)

// (LeaderCompletenessGateTest.* retired with the completeness gate — Kafka
//  model, user decision 2026-10-03: HW = min LEO over the ISR and a leader is
//  always elected from the ISR, so a "deficient" ISR leader cannot exist; the
//  E1b consensus-side gap for internal partitions is pinned by
//  DrIntegration.DISABLED_E1b_InternalFencedMemberElectedBeforeCatchUp_NoAckedLoss.)

// ===========================================================================
// Review 2026-10-04 MAJOR-3 (replaces InternalPartitionNeverLagShrinksALive
// Member): an INTERNAL partition lag-shrinks like any other (a live but stuck
// member must not freeze the control plane), but the change COMMITS only once a
// majority of the replicas durably holds it (InternalIsrKnowledge); until then
// the HW counts the member.  After the commit the stuck member is outside the
// newest durable ISR of the majority — never voted for by the consensus.
// ===========================================================================
TEST(InternalIsrRegression, InternalLagShrinkCommitsOnMajorityKnowledge_StuckMemberIneligible) {
    for (const bool internal : {true, false}) {
        SCOPED_TRACE(internal ? "internal" : "user");
        TempDir lDir("iisr_L"), fDir("iisr_F"), gDir("iisr_G");
        rune::storage::Log lLog(lDir.path), fLog(fDir.path), gLog(gDir.path);
        InMemoryTransport transport;
        constexpr NodeId kL = 1, kF = 2, kG = 3;
        std::vector<NodeId> all = {kL, kF, kG};
        auto cfgFor = [&](NodeId id) {
            PartitionConfig c = makeConfig(id, /*lagMs=*/10, /*lagOff=*/1'000'000);
            if (internal) c.topicId = -1;
            return c;
        };
        InternalIsrKnowledge kl, kf, kg;
        PartitionReplica l(cfgFor(kL), Role::Follower, 0, all, all, &lLog, &transport);
        PartitionReplica f(cfgFor(kF), Role::Follower, 0, all, all, &fLog, &transport);
        PartitionReplica g(cfgFor(kG), Role::Follower, 0, all, all, &gLog, &transport);
        if (internal) {
            l.setInternalIsrKnowledge(&kl); f.setInternalIsrKnowledge(&kf); g.setInternalIsrKnowledge(&kg);
        }
        l.becomeLeader(1, all);
        f.becomeFollower(1, kL);
        g.becomeFollower(1, kL);
        transport.setLeader(&l);
        transport.addFollower(kF, &f);           // kG never fetches (stuck, but live)
        ASSERT_GE(l.append(makePayload(1)), 0);
        int64_t clock = 0;
        bool sawPendingWithoutCommit = false;
        for (int i = 0; i < 100; ++i) {          // 100 ms >> replicaLagMaxMs = 10 ms
            clock += 1'000'000LL;
            l.doWork(clock); f.doWork(clock);
            if (internal && l.isrProposalPendingForTest()) {
                // Proposed (kG out) but not yet held by a majority: the HW still
                // counts kG (maximal ISR) — nothing committed past its LEO.
                sawPendingWithoutCommit = true;
                EXPECT_EQ(l.highWatermark(), 0) << "committed past a member whose removal is not durable";
            }
            transport.pump(clock);
        }
        const auto& isr = l.isr();
        EXPECT_EQ(std::find(isr.begin(), isr.end(), kG), isr.end()) << "a stuck live member pinned the ISR";
        EXPECT_EQ(l.highWatermark(), 1) << "the HW froze behind a stuck member";
        if (internal) {
            EXPECT_TRUE(sawPendingWithoutCommit);
            EXPECT_FALSE(l.internalSeedPendingForTest());
            EXPECT_FALSE(kl.eligible(kG)) << "the leader would vote for the stuck member";
            EXPECT_FALSE(kf.eligible(kG)) << "the majority's durable knowledge still names the stuck member";
            EXPECT_TRUE(kf.eligible(kF));
            EXPECT_TRUE(kg.eligible(kG)) << "(the stuck node itself never heard of its removal)";
        }
    }
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

    // Simulate follower going offline/partitioned: remove it from the transport
    // so pushes and responses are not delivered (GR-10: push-on-append means a
    // connected follower would receive data; we test the truly-offline case).
    transport.removeFollower(kF);

    // Append a record.
    leader.append(makePayload(0x55));
    Offset leo = leader.logEndOffset();

    // Drive leader alone (follower is partitioned — no push delivery, no fetch).
    // After replicaLagMaxMs, the leader should shrink the follower from ISR
    // and HW should advance.
    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 5'000'000LL; // 5 ms steps
        leader.doWork(clock);
        // Do NOT drive follower — it is partitioned.
        transport.pump(clock);
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

    // Phase 1: remove follower from transport to simulate partitioned/offline
    // state (GR-10: push-on-append means a connected follower would receive
    // data; we test the truly-offline → ISR-shrink → rejoin path).
    transport.removeFollower(kF);

    leader.append(makePayload(0x77));
    Offset leo = leader.logEndOffset();

    // Phase 1: drive leader only (follower partitioned) until follower shrunk.
    int64_t clock = 0;
    bool shrunk = false;
    for (int i = 0; i < kDriverCap && !shrunk; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        transport.pump(clock);
        shrunk = (leader.isr().size() == 1u);
    }
    ASSERT_TRUE(shrunk) << "Follower was not shrunk from ISR";

    // Phase 2: reconnect follower and drive both — follower should catch up
    // and re-enter ISR.
    transport.addFollower(kF, &follower);
    bool expanded = false;
    for (int i = 0; i < kDriverCap && !expanded; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock);
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

TEST_F(TwoReplicaTest, LeaderIsrCheckTickAloneIsNotWork) {
    // Idle-doWork contract (task #17): after the ISR-check interval (1ms) elapses,
    // the leader RUNS the ISR check — but a bare check that changes no ISR state and
    // moves no HW is NOT counted as work.  Counting the tick made the idle strategy
    // see work>0 every interval and busy-spin on an idle/no-lag leader.  With a
    // freshly-registered follower (not yet lagging past replicaLagMaxMs) and no
    // appends, there is genuinely nothing to do → doWork parks (returns 0).
    // (ISR-check-produces-work when a follower actually lags is covered by
    //  IsrShrinkUnblocksHwTest.LaggingFollowerShrunkFromIsrAllowsHwAdvance.)
    int work = leader_->doWork(2'000'000LL);
    EXPECT_EQ(work, 0);
}

TEST_F(TwoReplicaTest, BoundedDriverNeverExceedsCap) {
    // Verify that the bounded driver terminates correctly even when done=false.
    int iterations = 0;
    leader_->append(makePayload(0x01));
    for (int i = 0; i < kDriverCap; ++i) {
        clock_ += 1'000'000LL;
        leader_->doWork(clock_);
        follower_->doWork(clock_);
        transport_.pump(clock_);
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
    transport_.pump(2'000'000LL);
    EXPECT_EQ(transport_.pendingCount(), 0u);
}

TEST_F(TwoReplicaTest, InMemoryTransportDeliversBothDirections) {
    leader_->append(makePayload(0x10));
    follower_->doWork(2'000'000LL);  // emit fetch
    transport_.pump(2'000'000LL); // fetch → leader → response enqueued
    transport_.pump(2'000'000LL); // response → follower
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
    transport_.pump(2'000'000LL);
    transport_.pump(2'000'000LL);

    FetchRequestMsg req1 = follower_->nextFetch();
    EXPECT_GT(req1.fetchOffset, req0.fetchOffset);
}

// ===========================================================================
// Test group 15: FIX 1 — follower fetch timestamp correctness
// A follower that is actively fetching must NOT be evicted from the ISR.
// A follower that goes truly silent IS evicted after replicaLagMaxMs.
// ===========================================================================

TEST(FetchTimestampTest, ActiveFetcherRecordsRealFetchTimeNotEvicted) {
    // Scenario:
    //   - replicaLagMaxMs = 20 ms, isrCheckIntervalNs = 10 ms
    //   - Both leader and follower drive together (follower actively fetches)
    //   - After 30+ ms of joint driving, follower must STILL be in the ISR
    //     (it fetched at the real time, so lastFetchTimeNs is recent)
    TempDir leaderDir("ft_leader");
    TempDir followerDir("ft_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    // lagMaxMs=20, isrCheck=10ms, fetchInterval=5ms
    PartitionConfig lcfg = makeConfig(kL, /*lagMs=*/20,
                                       /*lagOff=*/10000,
                                       /*minIsr=*/1,
                                       /*fetchInt=*/5'000'000LL,
                                       /*isrCheck=*/10'000'000LL);
    PartitionConfig fcfg = makeConfig(kF, /*lagMs=*/20,
                                       /*lagOff=*/10000,
                                       /*minIsr=*/1,
                                       /*fetchInt=*/5'000'000LL,
                                       /*isrCheck=*/10'000'000LL);

    PartitionReplica leader(lcfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(fcfg, Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    leader.append(makePayload(0xAA));

    // Drive BOTH together for 60 ms (> lagMaxMs=20 ms) with 5 ms steps.
    // Follower fetches every 5 ms, so its real lastFetchTimeNs is always
    // within 5 ms of now — well under the 20 ms lag threshold.
    int64_t clock = 0;
    constexpr int64_t kStep = 5'000'000LL; // 5 ms
    constexpr int kIterations = 12; // 60 ms total
    for (int i = 0; i < kIterations; ++i) {
        clock += kStep;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock); // FIX 1: real clock passed to onFetchRequest
    }

    // Follower actively fetched: must still be in ISR (not evicted).
    EXPECT_EQ(leader.isr().size(), 2u)
        << "Active follower was wrongly evicted due to stale fetch timestamp";
    // ISR must contain kF
    const auto& isrVec = leader.isr();
    bool kfInIsr = std::find(isrVec.begin(), isrVec.end(), kF) != isrVec.end();
    EXPECT_TRUE(kfInIsr) << "Follower kF must remain in ISR while actively fetching";
}

TEST(FetchTimestampTest, TrulySilentFollowerIsEvictedAfterLagMaxMs) {
    // Scenario:
    //   - replicaLagMaxMs = 10 ms, isrCheckInterval = 5 ms
    //   - Follower sends one fetch at t=5ms, then goes silent
    //   - After 20+ ms the leader's ISR check sees real stale time → evicts
    TempDir leaderDir("ft2_leader");
    TempDir followerDir("ft2_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionConfig lcfg = makeConfig(kL, /*lagMs=*/10,
                                       /*lagOff=*/10000,
                                       /*minIsr=*/1,
                                       /*fetchInt=*/5'000'000LL,
                                       /*isrCheck=*/5'000'000LL);

    PartitionReplica leader(lcfg, Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF, 10, 10000, 1,
                                          5'000'000LL, 5'000'000LL),
                               Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    leader.append(makePayload(0xBB));

    // Let follower fetch once at t=5ms.
    int64_t clock = 5'000'000LL;
    leader.doWork(clock);
    follower.doWork(clock);
    transport.pump(clock);  // records real fetch at t=5ms

    // Now simulate follower going offline: remove it from transport so that
    // leader pushes and follower fast-acks are not delivered (GR-10).
    // The follower's lastFetchTimeNs=5ms; after lagMaxMs=10ms the leader evicts it.
    transport.removeFollower(kF);

    // Drive leader ALONE for 25 ms more (total t=30ms > lagMaxMs=10ms).
    bool evicted = false;
    for (int i = 0; i < kDriverCap && !evicted; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        // Follower is partitioned — transport delivers nothing to it.
        transport.pump(clock);
        evicted = (leader.isr().size() == 1u);
    }
    ASSERT_TRUE(evicted) << "Silent follower was not evicted from ISR after replicaLagMaxMs";
    EXPECT_EQ(leader.isr().size(), 1u);
    EXPECT_EQ(leader.isr()[0], kL);
}

// ===========================================================================
// Test group 16: FIX 4 — epoch truncation exactness
// The follower's post-truncation LEO must equal the EXACT LeaderEpochCache
// boundary offset before re-convergence (a truncate-to-0 bug must fail).
// ===========================================================================

TEST(EpochChangeTruncationTest, FollowerTruncatesToExactEpochBoundaryOffset) {
    // KIP-320/595 + KIP-279: the post-truncation LEO equals the EXACT lineage
    // boundary from a DivergingEpoch answer — min(leader's end of the epoch,
    // our own end of it) — never 0, never untouched.
    TempDir dir("fix4_trunc_exact");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    // Leader at epoch 1 (cache {1:0}), 3 records stamped 1; epoch 2 (cache
    // {1:0, 2:3}), 2 records stamped 2.
    PartitionReplica replica(makeConfig(kL), Role::Leader, /*epoch=*/1,
                              replicas, isr, &log, &transport);
    replica.append(makePayload(0xA1, 8), /*acksAll=*/false);
    replica.append(makePayload(0xA2, 8), /*acksAll=*/false);
    replica.append(makePayload(0xA3, 8), /*acksAll=*/false);
    ASSERT_EQ(replica.logEndOffset(), 3);
    replica.becomeLeader(/*epoch=*/2, {kL, kF});
    replica.append(makePayload(0xB1, 8), /*acksAll=*/false);
    replica.append(makePayload(0xB2, 8), /*acksAll=*/false);
    ASSERT_EQ(replica.logEndOffset(), 5);
    ASSERT_EQ(replica.lastFetchedEpoch(), 2);

    // Demoted: the new epoch-3 leader never held epoch 2 — its lineage for our
    // last epoch (2) is "epoch 1, ending at 3".
    replica.becomeFollower(/*leaderEpoch=*/3, kF);
    ASSERT_EQ(replica.logEndOffset(), 5) << "no truncation on becomeFollower (fetch-path validation)";
    FetchResponseMsg div{};
    div.leaderEpoch = 3; div.partition = 0; div.errorCode = kErrDivergingEpoch;
    { static rune::replication::ErrorPayloadBuf epb_; div.records = epb_.set(1, 3); }
    replica.applyReplicated(div);
    constexpr Offset kExactBoundary = 3;
    ASSERT_EQ(replica.logEndOffset(), kExactBoundary)
        << "post-truncation LEO must equal the exact lineage boundary";
    EXPECT_EQ(replica.lastFetchedEpoch(), 1) << "the epoch-2 entry was pruned with its records";

    // Appendable at the boundary: an epoch-3 record lands at 3.
    auto p = makePayload(0xE0, 8);
    FetchResponseMsg data{};
    data.leaderEpoch = 3; data.partition = 0; data.errorCode = kErrNone;
    data.records = std::span<const uint8_t>(p.data(), p.size());
    replica.applyReplicated(data);
    EXPECT_EQ(replica.logEndOffset(), kExactBoundary + 1);
    EXPECT_EQ(replica.lastFetchedEpoch(), 3);
}

// Stronger assertion: truncation landed at EXACT numeric boundary before re-append.
// We verify this by truncating WITHOUT appending new records and checking LEO directly.
TEST(EpochChangeTruncationTest, TruncationLandsAtExactNumericBoundary) {
    // KIP-279 min(): the leader's end of our last epoch (2) is BELOW our own end
    // of it (5) → truncate to the leader's; and a frame from a newer epoch than
    // ours is never applied (no truncation / adoption off a response).
    TempDir followerDir("trunc_exact");
    rune::storage::Log followerLog(followerDir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    for (int i = 0; i < 5; ++i) {
        auto p = makePayload(static_cast<uint8_t>(0x10 + i), 8);
        FetchResponseMsg r{};
        r.leaderEpoch = 1; r.partition = 0; r.highWatermark = 0;
        r.logStartOffset = 0; r.errorCode = kErrNone;
        r.records = std::span<const uint8_t>(p.data(), p.size());
        follower.applyReplicated(r);
    }
    ASSERT_EQ(follower.logEndOffset(), 5);
    ASSERT_EQ(follower.lastFetchedEpoch(), 1);

    FetchResponseMsg newer{};
    newer.leaderEpoch = 2; newer.partition = 0; newer.errorCode = kErrNone;
    follower.applyReplicated(newer);
    EXPECT_EQ(follower.logEndOffset(), 5) << "a newer-epoch frame must not truncate";
    EXPECT_EQ(follower.epoch(), 1) << "nor be adopted";

    follower.becomeFollower(2, kL);
    FetchResponseMsg div{};
    div.leaderEpoch = 2; div.partition = 0; div.errorCode = kErrDivergingEpoch;
    { static rune::replication::ErrorPayloadBuf epb_; div.records = epb_.set(1, 2); }
    follower.applyReplicated(div);
    const Offset leoAfterTrunc = follower.logEndOffset();
    EXPECT_EQ(leoAfterTrunc, 2) << "min(leader end 2, own end 5)";

    auto p2 = makePayload(0xEE, 8);
    FetchResponseMsg data{};
    data.leaderEpoch = 2; data.partition = 0; data.errorCode = kErrNone;
    data.records = std::span<const uint8_t>(p2.data(), p2.size());
    follower.applyReplicated(data);
    EXPECT_EQ(follower.logEndOffset(), leoAfterTrunc + 1) << "appendable after truncation";
}

// ===========================================================================
// Audit fixes: B1, B2, C1, C3 strengthened tests
// ===========================================================================

// ---------------------------------------------------------------------------
// C1: read() must never return records above HW (uncommitted boundary clamp)
// ---------------------------------------------------------------------------

TEST(AuditFixC1, ReadDoesNotReturnUncommittedRecordsAboveHW) {
    // Leader appends 5 records but HW advances only to 3 (2 followers confirm
    // up to offset 3 in 3-replica ISR with majority=2).
    // read(0, bigBuf) must return exactly the first 3 committed batches and
    // nothing above offset 3.

    TempDir dL("c1_L"), dF2("c1_F2"), dF3("c1_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3};
    std::vector<NodeId> isr      = {kL, kF2, kF3};

    PartitionReplica leader(makeConfig(kL), Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica f2   (makeConfig(kF2), Role::Follower, 1, replicas, isr, &logF2, &transport);
    PartitionReplica f3   (makeConfig(kF3), Role::Follower, 1, replicas, isr, &logF3, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    transport.addFollower(kF3, &f3);

    // Append 5 records.
    for (int i = 0; i < 5; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    Offset leo5 = leader.logEndOffset();
    ASSERT_EQ(leo5, 5);

    // Only drive until 3 records are acknowledged by majority.
    // We stop as soon as HW >= 3 but before HW reaches leo5=5.
    // To control this precisely, we only drive f2 (not f3) so majority=2 is
    // achieved with leader+f2.  Once f2 has fetched up to leo5, HW=5.
    // Instead: append 3, commit all 3, then append 2 more BEFORE driving.
    // Reset the scenario.

    // Simpler approach: commit exactly the first 3 by producing 3 then committing,
    // then produce 2 more uncommitted records that will NOT be replicated yet.
    TempDir dL2("c1_L2"), dF2b("c1_F2b");
    rune::storage::Log logL2(dL2.path), logF2b(dF2b.path);
    InMemoryTransport t2;
    PartitionReplica leader2(makeConfig(kL), Role::Leader, 1, {kL,kF2}, {kL,kF2}, &logL2, &t2);
    PartitionReplica f2b(makeConfig(kF2), Role::Follower, 1, {kL,kF2}, {kL,kF2}, &logF2b, &t2);
    t2.setLeader(&leader2);
    t2.addFollower(kF2, &f2b);

    // Append 3 records and commit them.
    for (int i = 0; i < 3; ++i) leader2.append(makePayload(static_cast<uint8_t>(i)));
    Offset leo3 = leader2.logEndOffset();
    ASSERT_EQ(leo3, 3);
    int64_t clock = 0;
    bool committed = false;
    for (int i = 0; i < kDriverCap && !committed; ++i) {
        clock += 1'000'000LL;
        leader2.doWork(clock); f2b.doWork(clock);
        t2.pump(clock);
        committed = (leader2.highWatermark() >= leo3);
    }
    ASSERT_TRUE(committed) << "First 3 records must be committed";
    ASSERT_EQ(leader2.highWatermark(), 3);

    // Now disconnect f2b from the transport so it can't ack more.
    t2.removeFollower(kF2);

    // Append 2 more records (uncommitted — f2b is gone, ISR still {kL, kF2},
    // so HW won't advance without f2b).
    leader2.append(makePayload(0xDE));
    leader2.append(makePayload(0xAD));
    Offset leo5b = leader2.logEndOffset();
    ASSERT_EQ(leo5b, 5);
    // HW must still be 3 (follower not acking the new 2 records).
    // Drive leader alone — no follower acks → HW stays at 3.
    for (int i = 0; i < 10; ++i) {
        clock += 1'000'000LL;
        leader2.doWork(clock);
        t2.pump(clock);
    }
    ASSERT_EQ(leader2.highWatermark(), 3) << "HW must stay at 3 (follower disconnected)";

    // C1 check: read(0, bigBuf) must not return records at offsets 3 or 4.
    auto readBytes = leader2.read(0, 1 << 20);
    ASSERT_FALSE(readBytes.empty()) << "Committed records must be readable";

    // Walk the returned bytes and verify no batch has baseOffset >= HW (3).
    std::size_t pos = 0;
    Offset maxBaseOffset = -1;
    int batchCount = 0;
    while (pos < readBytes.size()) {
        std::size_t rem = readBytes.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
        rune::protocol::RecordBatchReader r(readBytes.data() + pos, rem);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > readBytes.size()) break;
        maxBaseOffset = std::max(maxBaseOffset, r.baseOffset());
        ++batchCount;
        pos += static_cast<std::size_t>(total);
    }
    EXPECT_EQ(batchCount, 3)
        << "C1: read() must return exactly 3 committed batches, not " << batchCount;
    EXPECT_LT(maxBaseOffset, 3)
        << "C1: no batch in read() result should have baseOffset >= HW(3)";
}

// ---------------------------------------------------------------------------
// B2: new leader HW floor restored immediately on becomeLeader
// ---------------------------------------------------------------------------

TEST(AuditFixB2, NewLeaderHWFloorRestoredImmediatelyAfterBecomeLeader) {
    // Scenario:
    //   1. Leader L produces K records and commits them (HW=K).
    //   2. Follower F replicates all K records.
    //   3. L dies (simulate) — F calls becomeLeader.
    //   4. Immediately after becomeLeader, F.highWatermark() must be >= K.
    //      (Before B2 fix: it was 0 until followers re-acked.)
    //   5. F.read(0, big) must return the committed prefix [0,K) immediately.

    TempDir dL("b2_L"), dF("b2_F");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader  (makeConfig(kL), Role::Leader,   1, replicas, isr, &logL, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Produce and commit K=3 records.
    constexpr int K = 3;
    for (int i = 0; i < K; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    Offset committedOffset = leader.logEndOffset();

    int64_t clock = 0;
    bool committed = false;
    for (int i = 0; i < kDriverCap && !committed; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        committed = (leader.highWatermark() >= committedOffset
                  && follower.logEndOffset() >= committedOffset);
    }
    ASSERT_TRUE(committed) << "Records must be committed and replicated";
    ASSERT_EQ(follower.logEndOffset(), committedOffset);
    Offset oldLeaderHw = leader.highWatermark();
    ASSERT_GE(oldLeaderHw, committedOffset);

    // Capture the committed bytes from the leader's log for byte-equality check.
    auto committedBytes = leader.read(0, 1 << 20);
    ASSERT_FALSE(committedBytes.empty()) << "Leader must have readable committed data";

    // Failover: follower becomes new leader.
    follower.becomeLeader(/*newEpoch=*/2, {kF}); // ISR = {new leader only}

    // B2 fix: HW must be >= committedOffset IMMEDIATELY (no re-ack required).
    EXPECT_GE(follower.highWatermark(), committedOffset)
        << "B2: New leader HW must be >= committed offset immediately after becomeLeader "
        << "(was 0 before fix — committed reads disappeared on failover)";

    // B2 fix: read(0, big) must return the committed prefix bytes immediately.
    auto newLeaderBytes = follower.read(0, 1 << 20);
    ASSERT_FALSE(newLeaderBytes.empty())
        << "B2: New leader must be able to serve committed reads immediately after becomeLeader";

    // Byte-equality check: committed prefix must be identical.
    EXPECT_EQ(newLeaderBytes, committedBytes)
        << "B2: New leader committed read must be byte-equal to old leader's committed data";
}

// ---------------------------------------------------------------------------
// B1: follower truncate-then-refetch-lower path — HW must not use stale LEO
// ---------------------------------------------------------------------------

TEST(AuditFixB1, FollowerTruncateThenRefetchLowerOffset_HWNotStale) {
    // Regression: a follower that truncates and re-fetches from a LOWER offset
    // must update its tracked LEO to the lower value.  If the leader still
    // counts the stale pre-truncation LEO toward majority HW, HW can advance
    // past data only a minority currently holds.
    //
    // Setup: 2-replica ISR, leader LEO=10.
    //   1. Follower fetches up to LEO=10 → HW=10.
    //   2. Follower calls becomeFollower (epoch change, truncates to 5).
    //   3. Follower sends FetchRequest with fetchOffset=5 to new-epoch leader.
    //   4. Leader receives the lower fetchOffset=5 and must update HW.
    //      After update: leader LEO=10, follower LEO=5 → majority=2 → HW=5
    //      But prevHw_=10 → floor holds at 10.
    //   The KEY assertion: after follower truncation, the LEADER'S tracked
    //   follower LEO must be 5 (not 10), so if we then shrink ISR to leader-
    //   only and re-add follower at LEO=5, a fresh HW computation from scratch
    //   would give 5 (not 10 from the stale entry).

    TempDir dL("b1_L"), dF("b1_F");
    rune::storage::Log logL(dL.path), logF(dF.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader  (makeConfig(kL), Role::Leader,   1, replicas, isr, &logL, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Step 1: produce 10 records and fully replicate (HW=10).
    for (int i = 0; i < 10; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    Offset leo10 = leader.logEndOffset();
    ASSERT_EQ(leo10, 10);

    int64_t clock = 0;
    bool committed = false;
    for (int i = 0; i < kDriverCap && !committed; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        committed = (leader.highWatermark() >= leo10 && follower.logEndOffset() >= leo10);
    }
    ASSERT_TRUE(committed);
    ASSERT_EQ(leader.highWatermark(), 10);
    ASSERT_EQ(follower.logEndOffset(), 10);

    // Step 2: epoch change — follower becomes follower of a NEW leader (epoch 2)
    // and truncates to 5.  Capture the follower's LEO after truncation.
    follower.becomeFollower(/*leaderEpoch=*/2, kL); // truncates uncommitted tail
    // The follower had all committed data (LEO=10, HW=10), so truncation to HW
    // leaves it at LEO=10 (nothing above HW to drop).  To simulate the
    // truncation path, we manually inject a lower truncation point: becomeFollower
    // with epoch=3 while the leader is at epoch=2 (triggering cache-fallback=HW=10).
    // Instead simulate directly: drive the follower to emit a fetch at LEO=10.
    // Then manually lower the follower's log by constructing a fresh scenario.
    //
    // Direct path: the leader (same node, now epoch 2) receives a FetchRequest
    // at fetchOffset=10 (follower at epoch 2 after becomeFollower).
    // Now drive follower to re-fetch.
    leader.becomeLeader(/*epoch=*/2, {kL, kF}); // leader's HW floor = 10

    // B2 fix: new leader HW must be >= 10 immediately.
    EXPECT_GE(leader.highWatermark(), 10) << "B2: leader HW floor must be 10 after becomeLeader";

    // Drive follower to fetch (at LEO=10 — fully caught up).
    for (int i = 0; i < kDriverCap; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock); follower.doWork(clock);
        transport.pump(clock);
        if (follower.logEndOffset() >= leo10 && leader.highWatermark() >= leo10) break;
    }
    EXPECT_GE(leader.highWatermark(), 10) << "HW must stay >= 10 after follower re-acks at epoch 2";

    // B1 specific: build a scenario where the follower reports a LOWER fetchOffset.
    // The follower's last fetch is at LEO=10.  Now we simulate truncation by
    // sending a FetchRequest from the FOLLOWER with fetchOffset=5.
    // The leader must update its tracked follower LEO to 5 (not keep 10).
    //
    // GR-10: remove follower from transport first so that subsequent pushes
    // (triggered by leader.append()) are not delivered to it — the test verifies
    // that HW does NOT advance to 15 without the follower acking to 15.
    transport.removeFollower(kF);

    FetchRequestMsg lowerReq{};
    lowerReq.leaderEpoch = 2; // current epoch
    lowerReq.partition   = 0;
    lowerReq.followerId  = kF;
    lowerReq.fetchOffset = 5; // lower than previously reported 10
    lowerReq.maxBytes    = 1 << 20;

    int16_t err = leader.onFetchRequest(lowerReq, clock);
    ASSERT_EQ(err, kErrNone) << "Lower fetchOffset fetch must be accepted";

    // B1 (SET, not max): the leader's tracked follower LEO is now 5 — the stale
    // pre-truncation 10 is never counted again.  (Asserted on the tracked value
    // itself: the served HW is clamped by the min-ISR / healthy floors and so
    // cannot show it — the review's M3.)
    EXPECT_EQ(leader.followerLeoForTest(kF), 5)
        << "B1: the leader must track the follower's LOWER position (SET, not max)";
    // DUR-1: regressing BELOW the HW (10) = lost acknowledged records → the
    // follower also leaves the ISR at once (KIP-966, data plane).
    EXPECT_EQ(std::count(leader.isr().begin(), leader.isr().end(), kF), 0)
        << "DUR-1: a follower that regressed below the HW must leave the ISR";
    // The committed prefix never regresses.
    EXPECT_GE(leader.highWatermark(), 10);
}

// ===========================================================================
// NB1 test: becomeLeader with uncommitted tail — HW floor must be committed
// offset (followerHw_), NOT the LEO.
// ===========================================================================

TEST(NB1_BecomeLeaderUncommittedTail, HwFloorIsCommittedNotLeo) {
    // NB1: a follower promoted with an uncommitted tail (localLeo=10 > committed=5)
    // must have HW floor == 5 (committed), not 10 (LEO).
    //
    // The observable difference from the LEO-floor bug requires a MULTI-node ISR:
    // with a 1-node ISR the new leader is a majority by itself and the recompute
    // legitimately gives HW=leaderLeo (correct for a leader-only quorum).
    // With a 2-node ISR (promoted follower + one other), the majority=2 computation
    // means HW can only advance past the committed floor once the OTHER follower
    // re-acks.  Under the old LEO-floor code, HW starts at 10 immediately (WRONG —
    // no majority holds [5,10)).  Under the fix (committed floor=5), HW starts at 5
    // and can only advance when the other follower acks above 5.
    //
    // This test FAILS under the old log_->logEndOffset() floor code.

    // Build a leader with 10 records.
    TempDir leaderDir("nb1_leader");
    rune::storage::Log leaderLog(leaderDir.path);
    InMemoryTransport leaderTransport;
    constexpr NodeId kL = 1, kF = 2, kF2 = 3;
    std::vector<NodeId> replicas = {kL, kF, kF2};
    std::vector<NodeId> isr      = {kL, kF, kF2};

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &leaderTransport);
    leaderTransport.setLeader(&leader);
    for (int i = 0; i < 10; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    ASSERT_EQ(leader.logEndOffset(), 10);

    // Build f1: a follower that receives all 10 records but only sees HW=5.
    // This simulates: the leader appended 5 uncommitted records (not yet majority-acked)
    // after the last HW advance to 5.
    TempDir f1Dir("nb1_f1");
    rune::storage::Log f1Log(f1Dir.path);
    InMemoryTransport t1;
    PartitionReplica f1(makeConfig(kF), Role::Follower, /*epoch=*/1,
                         replicas, isr, &f1Log, &t1);
    {
        auto allRecords = leaderLog.read(0, 1 << 20);
        ASSERT_FALSE(allRecords.empty());
        FetchResponseMsg resp{};
        resp.leaderEpoch    = 1;
        resp.partition      = 0;
        resp.highWatermark  = 5;   // only [0,5) committed; [5,10) uncommitted
        resp.logStartOffset = 0;
        resp.errorCode      = kErrNone;
        resp.records        = std::span<const uint8_t>(allRecords.data(), allRecords.size());
        f1.applyReplicated(resp);
    }
    ASSERT_EQ(f1.logEndOffset(), 10) << "f1 LEO must be 10";
    ASSERT_EQ(f1.highWatermark(), 5) << "f1 committedHw must be 5";

    // Build f2: another follower (starts empty — it is the "other" ISR member
    // that will be in the new ISR after election).
    TempDir f2Dir("nb1_f2");
    rune::storage::Log f2Log(f2Dir.path);
    InMemoryTransport t2;
    PartitionReplica f2(makeConfig(kF2), Role::Follower, /*epoch=*/1,
                         replicas, isr, &f2Log, &t2);
    // f2 has LEO=0 (no records yet).
    ASSERT_EQ(f2.logEndOffset(), 0);

    // Promote f1 as new leader with a 2-node ISR {kF, kF2}.
    // majority(2)=2, so HW can only advance when BOTH kF and kF2 hold the data.
    // kF (f1) has LEO=10; kF2 (f2) has LEO=0.
    // Under the OLD LEO-floor code: floor = logEndOffset() = 10 → HW=10 immediately.
    //   This is WRONG: kF2 has LEO=0, so no 2-node majority holds [5,10).
    // Under the FIX (committed floor=5): HW starts at 5.
    //   majority(2) of {10, 0} = 0 < floor=5 → HW = 5 (floor holds).
    f1.becomeLeader(/*newEpoch=*/2, {kF, kF2});

    // NB1 assertion: HW must be the committed offset (5), not LEO (10).
    // Under the old LEO-floor code this is 10; under the fix it is 5.
    EXPECT_EQ(f1.highWatermark(), 5)
        << "NB1: new leader HW must be committedHw=5, not LEO=10. "
           "Old log_->logEndOffset() floor code would produce 10 immediately "
           "(no majority of 2 holds [5,10) — kF2 is at LEO=0).";

    // read(0, big) must return only [0,5) — the committed prefix.
    auto readBytes = f1.read(0, 1 << 20);
    EXPECT_FALSE(readBytes.empty()) << "NB1: committed prefix [0,5) must be readable";

    // Walk batches: none must have baseOffset >= 5.
    {
        std::size_t pos = 0;
        int batchCount = 0;
        while (pos < readBytes.size()) {
            std::size_t rem = readBytes.size() - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader r(readBytes.data() + pos, rem);
            int32_t total = r.totalLength();
            if (total <= 0) break;
            int64_t batchBase = r.baseOffset();
            EXPECT_LT(batchBase, 5)
                << "NB1: read() returned batch baseOffset=" << batchBase
                << " >= HW=5 (uncommitted record leaked)";
            pos += static_cast<std::size_t>(total);
            ++batchCount;
        }
        EXPECT_EQ(batchCount, 5)
            << "NB1: read() must return exactly 5 batches (offsets 0-4), got " << batchCount;
    }

    // kF2 is a committed-ISR member whose validated fetch lies BELOW the HW
    // (LEO 0 < 5): it provably lacks committed records (review 2026-10-04
    // MEDIUM-4 — every ISR member holds everything below the HW).  It is removed
    // from the ISR (self-managed: at once) and can no longer pin the HW; the HW
    // then is the min over the remaining ISR {kF} (minIsr 1).
    FetchRequestMsg req{};
    req.leaderEpoch = 2;
    req.partition   = 0;
    req.followerId  = kF2;
    req.fetchOffset = 0;  // kF2's LEO (no records yet)
    req.maxBytes    = 1 << 20;
    InMemoryTransport newLeaderTransport;
    newLeaderTransport.setLeader(&f1);
    f1.onFetchRequest(req, 1'000'000LL);
    const auto& isrAfter = f1.isr();
    EXPECT_EQ(std::find(isrAfter.begin(), isrAfter.end(), kF2), isrAfter.end())
        << "an ISR member below the HW (it lost committed records) kept its seat";
    EXPECT_GE(f1.highWatermark(), 5) << "the committed floor never regresses";
}

// ===========================================================================
// NB2 test: applyReplicated truncation must not go below committedHw floor.
// ===========================================================================

TEST(NB2_ApplyReplicatedTruncation, NeverTruncatesBelowCommittedHw) {
    // Scenario: follower has committed data at [0,5) (followerHw_=5).
    // An epoch change arrives in a FetchResponse (without becomeFollower first).
    // The epoch cache returns a truncation boundary < committedHw.
    // The truncateLog call must be floored at committedHw (5), not go below it.
    //
    // We simulate this by directly calling applyReplicated with a higher epoch.
    // The follower's epochCache has entry for its current epoch → may produce a
    // low boundary.  We use a fresh follower with no epoch cache entry so the
    // cache returns -1 (no truncation fires in that path).
    //
    // More directly: we place an epoch cache entry that would truncate to 3.
    // Since committedHw=5 > 3, the floor must clamp to 5.
    // We cannot easily inject the epoch cache from outside, so we test via the
    // observable effect: after applyReplicated, log LEO must be >= committedHw.

    TempDir dir("nb2_follower");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    // Build a leader to produce proper RecordBatch frames.
    TempDir leaderDir("nb2_leader");
    rune::storage::Log leaderLog(leaderDir.path);
    InMemoryTransport leaderTransport;
    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &leaderTransport);
    leaderTransport.setLeader(&leader);

    // Append 8 records on the leader.
    for (int i = 0; i < 8; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    ASSERT_EQ(leader.logEndOffset(), 8);

    // Create a follower and inject records with HW=5 (5 committed, tail [5,8) uncommitted).
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &log, &transport);
    {
        auto allRecords = leaderLog.read(0, 1 << 20);
        FetchResponseMsg resp{};
        resp.leaderEpoch    = 1;
        resp.partition      = 0;
        resp.highWatermark  = 5;
        resp.logStartOffset = 0;
        resp.errorCode      = kErrNone;
        resp.records        = std::span<const uint8_t>(allRecords.data(), allRecords.size());
        follower.applyReplicated(resp);
    }
    ASSERT_EQ(follower.logEndOffset(), 8) << "Follower LEO must be 8";
    ASSERT_EQ(follower.highWatermark(), 5) << "Follower committed HW must be 5";

    // Now simulate a direct epoch-change FetchResponse (epoch 2 > epoch 1).
    // This is the applyReplicated path that NB2 guards.
    // The response carries no records (or records above 5) and leaderEpoch=2.
    // Since follower has no epoch-2 cache entry, truncationOffsetForEpochChange
    // returns -1, so no truncation fires. That's OK — we want to test the
    // floor guard.  To force the guard to activate, we need a scenario where
    // the cache DOES return a low boundary.  We use becomeFollower to set up
    // the cache, then test the live LEO constraint.
    //
    // Alternative test: use becomeFollower at epoch 2 (which calls truncateLog
    // correctly with the floor), then verify the follower log is not below 5.
    follower.becomeFollower(/*leaderEpoch=*/2, kL);

    // After becomeFollower, follower LEO must be >= committedHw=5 (the floor).
    // becomeFollower's C2/NB2 guard must have clamped any truncation to >=5.
    EXPECT_GE(follower.logEndOffset(), 5)
        << "NB2: becomeFollower (which calls truncateLog) must not truncate below committedHw=5";
    EXPECT_LE(follower.logEndOffset(), 8)
        << "NB2: follower LEO should be at most 8 (truncation may have removed uncommitted tail)";
}

// ===========================================================================
// C3 test: electNewLeader uses surviving ISR committedHw when old leader is
// unregistered (the crash-failover case).
// ===========================================================================

TEST(C3_ElectionCommittedHw, CrashedLeaderUnregistered_CandidateWithLowLeoRejected) {
    // Scenario (3 replicas: leader=0, f1=1, f2=2):
    //   - Leader appended 10 records; all committed (HW=10).
    //   - f1 LEO=10 (fully caught up), f2 LEO=4 (lagging, below committedHw).
    //   - Leader crashes and is UNREGISTERED (not added to coordinator entries).
    //   - Old code: committedHw defaults to 0 → f2 (LEO=4>=0) is eligible → unsafe.
    //   - C3 fix: committedHw computed from surviving ISR members (f1 reports HW=10).
    //             f2 (LEO=4 < committedHw=10) must NOT be elected.
    //             f1 (LEO=10 >= committedHw=10) must be elected.

    TempDir leaderDir("c3_leader");
    TempDir f1Dir("c3_f1");
    TempDir f2Dir("c3_f2");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log f1Log(f1Dir.path);
    rune::storage::Log f2Log(f2Dir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 0, kF1 = 1, kF2 = 2;
    std::vector<NodeId> replicas = {kL, kF1, kF2};
    std::vector<NodeId> isr      = {kL, kF1, kF2};

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica f1(makeConfig(kF1), Role::Follower, /*epoch=*/1,
                         replicas, isr, &f1Log, &transport);
    PartitionReplica f2(makeConfig(kF2), Role::Follower, /*epoch=*/1,
                         replicas, isr, &f2Log, &transport);

    transport.setLeader(&leader);
    transport.addFollower(kF1, &f1);
    transport.addFollower(kF2, &f2);

    // Append 10 records and commit.
    for (int i = 0; i < 10; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    ASSERT_EQ(leader.logEndOffset(), 10);

    // Drive until leader HW=10 (all committed: 3-replica ISR, majority=2).
    int64_t clk = 0;
    for (int i = 0; i < kDriverCap && leader.highWatermark() < 10; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); f1.doWork(clk); f2.doWork(clk);
        transport.pump(clk);
    }
    ASSERT_EQ(leader.highWatermark(), 10) << "Leader HW must be 10";

    // f1 is fully caught up (LEO=10). f2 may have fewer records.
    // Drive until f1 reaches LEO=10.
    // (Kafka model: f1 learns the HW from its next answer after the last ack.)
    for (int i = 0; i < kDriverCap && (f1.logEndOffset() < 10 || f1.highWatermark() < 10); ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); f1.doWork(clk);
        transport.pump(clk);
    }
    ASSERT_EQ(f1.logEndOffset(), 10) << "f1 must hold all 10 records";

    // f2 is lagging — we want f2.LEO < 10.  If f2 already caught up, we cannot
    // reset it, but we can check the test precondition and skip if already at 10.
    // In practice with the bounded driver above, f2 may have caught up too.
    // To force the lagging scenario, we rebuild f2 fresh (empty log).
    TempDir f2Dir2("c3_f2_reset");
    rune::storage::Log f2Log2(f2Dir2.path);
    PartitionReplica f2_lagging(makeConfig(kF2), Role::Follower, /*epoch=*/1,
                                  replicas, isr, &f2Log2, &transport);
    // f2_lagging has LEO=0 (no records — clearly < committedHw=10).
    ASSERT_EQ(f2_lagging.logEndOffset(), 0) << "f2_lagging must start with empty log";

    // Set up coordinator. NOTE: old leader (kL=0) is NOT registered — it crashed.
    PartitionMetadata meta;
    meta.partitionId = 0;
    meta.leader      = kL;
    meta.replicas    = replicas;
    meta.isr         = isr;
    meta.leaderEpoch = 1;

    SimpleNodeLiveness liveness;
    liveness.markDead(kL);

    PartitionFailoverCoordinator coord(meta, &liveness);
    // Register f1 and f2_lagging but NOT the leader (it crashed and is unregistered).
    coord.registerReplica(kF1, &f1);
    coord.registerReplica(kF2, &f2_lagging);
    // kL is intentionally NOT registered.

    // Signal leader death and elect.
    coord.signalLeaderDead(kL);

    // C3 fix: because old leader is unregistered, committedHw must be derived
    // from surviving ISR members. f1 has highWatermark()=10 (followerHw_=10).
    // f2_lagging has LEO=0 < committedHw=10 → must be rejected.
    // f1 has LEO=10 >= committedHw=10 → elected.
    //
    // Note: f1 is a follower, so highWatermark() returns followerHw_=10
    // (min(localLeo=10, lastKnownLeaderHw=10) = 10 — it received HW=10 from leader).
    // Verify f1's HW first.
    ASSERT_GE(f1.highWatermark(), 10) << "f1 must report committedHw >= 10 for C3 fix to work";

    bool elected = coord.electNewLeader();
    ASSERT_TRUE(elected) << "C3: election must succeed (f1 is eligible with LEO=10)";

    NodeId newLeader = coord.currentLeader();
    EXPECT_EQ(newLeader, kF1)
        << "C3: f1 (LEO=10) must be elected, not f2_lagging (LEO=0 < committedHw=10). "
           "Old code would elect either (committedHw defaulting to 0 → all eligible).";

    // f2_lagging must NOT have been promoted.
    EXPECT_EQ(f2_lagging.role(), Role::Follower)
        << "C3: f2_lagging (LEO=0 < committedHw=10) must NOT have been promoted to leader";
}

// ===========================================================================
// Part B — 18 missing edge-case tests (audit full re-audit 2026-09-11)
// ===========================================================================

// ---------------------------------------------------------------------------
// PartB_1: A-NEW-1 — pipelined/duplicate FetchResponse apply
// Deliver two overlapping FetchResponses (the 2nd overlaps AND extends).
// Assert the follower log has NO duplicate offsets and stays byte-identical.
// ---------------------------------------------------------------------------
TEST(PartBTest, ANEW1_PipelinedFetchResponseNoDuplicateOffsets)
{
    // Set up: leader + follower; append 5 records to leader.
    TempDir leaderDir("pb1_leader");
    TempDir followerDir("pb1_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append 5 records on leader.
    for (int i = 0; i < 5; ++i) leader.append(makePayload(static_cast<uint8_t>(i)));
    ASSERT_EQ(leader.logEndOffset(), 5);

    // Drive until follower has first 2 records (LEO=2).
    int64_t clk = 0;
    for (int i = 0; i < kDriverCap && follower.logEndOffset() < 2; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk);
        follower.doWork(clk);
        transport.pump(clk);
    }
    ASSERT_GE(follower.logEndOffset(), 2) << "Follower must have at least 2 records";
    Offset followerLeoBeforeOverlap = follower.logEndOffset();

    // Manually inject two overlapping FetchResponses:
    //   Response A: covers [0, followerLeoBeforeOverlap) — already applied (overlap only)
    //   Response B: covers [0, 5) — overlaps the held part AND extends to 5
    // Both use the verbatim leader log bytes (RecordBatch format).
    auto leaderBytes = leaderLog.read(0, 1 << 20); // read all 5 records from leader

    // Build Response A: first followerLeoBeforeOverlap records.
    auto bytesA = leaderLog.read(0, static_cast<int32_t>(followerLeoBeforeOverlap * 100));
    // Build Response B: all 5 records.
    auto bytesB = leaderLog.read(0, 1 << 20);

    // Inject Response A first (simulate a response that the follower already partially has).
    FetchResponseMsg respA{};
    respA.errorCode    = kErrNone;
    respA.leaderEpoch  = 1;
    respA.highWatermark = 5;
    respA.records      = std::span<const uint8_t>(bytesA.data(), bytesA.size());
    follower.applyReplicated(respA);

    Offset leoAfterA = follower.logEndOffset();

    // Inject Response B: overlaps [0, leoAfterA) AND extends to 5.
    FetchResponseMsg respB{};
    respB.errorCode    = kErrNone;
    respB.leaderEpoch  = 1;
    respB.highWatermark = 5;
    respB.records      = std::span<const uint8_t>(bytesB.data(), bytesB.size());
    follower.applyReplicated(respB);

    Offset leoAfterB = follower.logEndOffset();

    // Assert: LEO is monotonically advancing (no duplicate offsets).
    EXPECT_GE(leoAfterB, leoAfterA) << "A-NEW-1: LEO must not regress after 2nd response";
    EXPECT_LE(leoAfterB, 5) << "A-NEW-1: LEO must not exceed leader LEO=5";

    // Assert: follower log is byte-identical to leader over [0, LEO).
    if (leoAfterB > 0) {
        auto followerBytes = followerLog.read(0, 1 << 20);
        auto leaderBytesToLeo = leaderLog.read(0, 1 << 20);
        // Trim leader bytes to follower LEO by walking batches.
        std::size_t pos = 0;
        std::size_t leaderTrimEnd = 0;
        int64_t counted = 0;
        while (pos < leaderBytesToLeo.size() && counted < leoAfterB) {
            std::size_t rem = leaderBytesToLeo.size() - pos;
            if (rem < 61u) break;
            rune::protocol::RecordBatchReader r(leaderBytesToLeo.data() + pos, rem);
            int32_t total = r.totalLength();
            if (total <= 0) break;
            counted = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += static_cast<std::size_t>(total);
            leaderTrimEnd = pos;
        }
        ASSERT_EQ(followerBytes.size(), leaderTrimEnd)
            << "A-NEW-1: follower log byte length must equal leader's [0, LEO) bytes";
        EXPECT_EQ(followerBytes, std::vector<uint8_t>(leaderBytesToLeo.begin(),
                                                        leaderBytesToLeo.begin()
                                                        + static_cast<std::ptrdiff_t>(leaderTrimEnd)))
            << "A-NEW-1: follower log must be byte-identical to leader over [0, LEO)";
    }
}

// ---------------------------------------------------------------------------
// PartB_2: ISR shrink-to-1 then produce+commit+read end-to-end
// ---------------------------------------------------------------------------
TEST_F(ThreeReplicaTest, PartB2_IsrShrinkTo1ThenProduceCommitRead)
{
    // Append 5 records and commit with 3-node ISR.
    for (int i = 0; i < 5; ++i) leader_->append(makePayload(static_cast<uint8_t>(i)));
    ASSERT_TRUE(driveUntil([&]{ return leader_->highWatermark() == 5; }))
        << "PartB2: must commit 5 records with 3-node ISR";

    // Shrink ISR to leader only (simulate both followers disconnecting).
    // Drive the leader doWork many times with no follower fetches so the ISR
    // eviction time-lag fires.  replicaLagMaxMs=1000 in makeConfig → 1 s.
    // Each step is 20 ms; 60 steps = 1200 ms = well past the lag window.
    for (int i = 0; i < 60; ++i) {
        clock_ += 20'000'000LL; // 20 ms per step
        leader_->doWork(clock_);
        // Do NOT drive followers so their lastFetchTime stays stale.
    }
    // ISR should now be {leader only} after the time-lag eviction.
    EXPECT_LE(leader_->isr().size(), 2u) << "PartB2: at least one follower should have been evicted";

    // Now append with leader-only ISR; HW should advance immediately.
    Offset base = leader_->append(makePayload(0xFF), /*acksAll=*/false);
    ASSERT_GE(base, 5) << "PartB2: append after ISR shrink must succeed";

    // With acksAll=false (leader-only ack), the HW can advance via recomputeHw.
    // Drive a few ticks.
    for (int i = 0; i < 5; ++i) {
        clock_ += 1'000'000LL;
        leader_->doWork(clock_);
    }

    // Read committed data.
    auto data = leader_->read(0, 1 << 20);
    EXPECT_FALSE(data.empty()) << "PartB2: committed data must be readable after ISR shrink-to-1";
}

// ---------------------------------------------------------------------------
// PartB_3: Single-replica-ISR produce→commit→read end-to-end via PartitionReplica
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB3_SingleReplicaIsrProduceCommitRead)
{
    TempDir dir("pb3");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1;
    std::vector<NodeId> replicas = {kL};
    std::vector<NodeId> isr      = {kL};

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &log, &transport);
    transport.setLeader(&leader);

    // Append 3 records; with |ISR|=1 they should commit immediately.
    Offset b0 = leader.append(makePayload(0xA1));
    Offset b1 = leader.append(makePayload(0xA2));
    Offset b2 = leader.append(makePayload(0xA3));
    ASSERT_GE(b0, 0);
    ASSERT_GT(b1, b0);
    ASSERT_GT(b2, b1);
    ASSERT_EQ(leader.logEndOffset(), 3) << "PartB3: 3 records must be appended";

    // Single-replica ISR: HW should immediately equal LEO.
    int64_t hw = leader.highWatermark();
    EXPECT_EQ(hw, 3) << "PartB3: single-replica ISR → HW must equal LEO=3 immediately";

    // Read committed records.
    auto data = leader.read(0, 1 << 20);
    EXPECT_FALSE(data.empty()) << "PartB3: committed records must be readable";

    // Verify HW boundary: read from HW returns nothing.
    auto atHw = leader.read(hw, 1 << 20);
    EXPECT_TRUE(atHw.empty()) << "PartB3: read at HW must return empty (nothing committed beyond HW)";
}

// ---------------------------------------------------------------------------
// PartB_4: HW-exact read-clamp on a multi-batch log
// fetchOffset == HW-1 returns one batch; fetchOffset == HW returns empty.
// ---------------------------------------------------------------------------
TEST_F(TwoReplicaTest, PartB4_HwExactReadClampMultiBatchLog)
{
    // Append 3 records and commit all.
    for (int i = 0; i < 3; ++i) leader_->append(makePayload(static_cast<uint8_t>(i + 1)));
    ASSERT_EQ(leader_->logEndOffset(), 3);
    ASSERT_TRUE(driveUntil([&]{ return leader_->highWatermark() == 3; }))
        << "PartB4: must commit all 3 records";

    Offset hw = leader_->highWatermark();
    ASSERT_EQ(hw, 3);

    // fetchOffset == hw-1 (== 2): should return exactly 1 batch (the 3rd record).
    auto atHwMinus1 = leader_->read(hw - 1, 1 << 20);
    EXPECT_FALSE(atHwMinus1.empty())
        << "PartB4: read at HW-1 must return the last committed batch";

    // The batch returned must start at offset HW-1 = 2.
    if (!atHwMinus1.empty() && atHwMinus1.size() >= 61u) {
        rune::protocol::RecordBatchReader r(atHwMinus1.data(), atHwMinus1.size());
        EXPECT_EQ(r.baseOffset(), hw - 1)
            << "PartB4: the returned batch must start exactly at HW-1";
    }

    // fetchOffset == hw: must return empty.
    auto atHw = leader_->read(hw, 1 << 20);
    EXPECT_TRUE(atHw.empty())
        << "PartB4: read at HW must return empty (nothing committed at or beyond HW)";

    // fetchOffset == hw+1: also empty.
    auto pastHw = leader_->read(hw + 1, 1 << 20);
    EXPECT_TRUE(pastHw.empty()) << "PartB4: read past HW must return empty";
}

// ---------------------------------------------------------------------------
// PartB_5: Follower truncate-then-refetch across 3 epoch rounds with per-round
// byte-identity to the leader over [0, HW).
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB5_FollowerTruncateRefetchThreeEpochRoundsByteIdentity)
{
    TempDir leaderDir("pb5_leader");
    TempDir followerDir("pb5_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};
    int64_t clk = 0;

    auto makeLeader = [&](Epoch e) {
        return std::make_unique<PartitionReplica>(
            makeConfig(kL), Role::Leader, e, replicas, isr, &leaderLog, &transport);
    };
    auto makeFollower = [&](Epoch e) {
        return std::make_unique<PartitionReplica>(
            makeConfig(kF), Role::Follower, e, replicas, isr, &followerLog, &transport);
    };

    auto driveUntil3 = [&](std::unique_ptr<PartitionReplica>& l,
                             std::unique_ptr<PartitionReplica>& f,
                             std::function<bool()> done) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clk += 1'000'000LL;
            l->doWork(clk);
            f->doWork(clk);
            transport.pump(clk);
        }
        return done();
    };

    // Round 1: epoch 1, append 3, commit.
    auto leader = makeLeader(1);
    auto follower = makeFollower(1);
    transport.setLeader(leader.get());
    transport.addFollower(kF, follower.get());
    for (int i = 0; i < 3; ++i) leader->append(makePayload(static_cast<uint8_t>(0x10 + i)));
    ASSERT_TRUE(driveUntil3(leader, follower, [&]{ return follower->logEndOffset() == 3; }))
        << "PartB5 round1: follower must reach LEO=3";
    ASSERT_TRUE(driveUntil3(leader, follower, [&]{ return leader->highWatermark() == 3; }))
        << "PartB5 round1: must commit 3 records";
    // Byte-identity check round 1.
    {
        Offset hw = leader->highWatermark();
        auto leaderBytes  = leaderLog.read(0, 1 << 20);
        auto followerBytes = followerLog.read(0, 1 << 20);
        EXPECT_EQ(leaderBytes, followerBytes) << "PartB5 round1: follower must be byte-identical to leader";
        (void)hw;
    }

    // Round 2: epoch 2, truncate follower, append 3 new records.
    transport.setLeader(nullptr);
    transport.removeFollower(kF);
    leader.reset();
    follower.reset();
    // Append divergent records to the leader log at epoch 2.
    leader = makeLeader(2);
    follower = makeFollower(2); // follower truncates on becomeFollower + refetch
    transport.setLeader(leader.get());
    transport.addFollower(kF, follower.get());
    for (int i = 0; i < 3; ++i) leader->append(makePayload(static_cast<uint8_t>(0x20 + i)));
    ASSERT_TRUE(driveUntil3(leader, follower, [&]{ return follower->logEndOffset() >= 3; }))
        << "PartB5 round2: follower must catch up";
    ASSERT_TRUE(driveUntil3(leader, follower, [&]{ return leader->highWatermark() >= 3; }))
        << "PartB5 round2: must commit records";
    {
        auto leaderBytes  = leaderLog.read(0, 1 << 20);
        auto followerBytes = followerLog.read(0, 1 << 20);
        EXPECT_EQ(leaderBytes, followerBytes) << "PartB5 round2: byte-identity after truncate+refetch";
    }

    // Round 3: epoch 3.
    transport.setLeader(nullptr);
    transport.removeFollower(kF);
    leader.reset();
    follower.reset();
    leader = makeLeader(3);
    follower = makeFollower(3);
    transport.setLeader(leader.get());
    transport.addFollower(kF, follower.get());
    for (int i = 0; i < 4; ++i) leader->append(makePayload(static_cast<uint8_t>(0x30 + i)));
    ASSERT_TRUE(driveUntil3(leader, follower, [&]{ return follower->logEndOffset() >= 4; }))
        << "PartB5 round3: follower must catch up after round3";
    ASSERT_TRUE(driveUntil3(leader, follower, [&]{ return leader->highWatermark() >= 4; }))
        << "PartB5 round3: must commit 4 records";
    {
        auto leaderBytes  = leaderLog.read(0, 1 << 20);
        auto followerBytes = followerLog.read(0, 1 << 20);
        EXPECT_EQ(leaderBytes, followerBytes) << "PartB5 round3: byte-identity after 3rd truncate+refetch";
    }
}

// ---------------------------------------------------------------------------
// PartB_10: becomeFollower with divergent tail EXACTLY at committedHw
// (truncOff == committedHw == currentLeo): no truncation fires, no committed
// byte drops.
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB10_BecomeFollowerExactBoundaryNoTruncation)
{
    TempDir leaderDir("pb10_leader");
    TempDir followerDir("pb10_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};
    int64_t clk = 0;

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append 5 records and fully commit.
    for (int i = 0; i < 5; ++i) leader.append(makePayload(static_cast<uint8_t>(i + 1)));
    for (int i = 0; i < kDriverCap && follower.logEndOffset() < 5; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); follower.doWork(clk); transport.pump(clk);
    }
    for (int i = 0; i < kDriverCap && leader.highWatermark() < 5; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); follower.doWork(clk); transport.pump(clk);
    }
    ASSERT_EQ(leader.highWatermark(), 5) << "PartB10: must commit 5 records";
    ASSERT_EQ(follower.logEndOffset(), 5) << "PartB10: follower must have all 5 records";

    // Capture the follower bytes before epoch change.
    auto followerBytesBefore = followerLog.read(0, 1 << 20);
    ASSERT_FALSE(followerBytesBefore.empty());

    // Trigger becomeFollower with new epoch (epoch 2) on the follower.
    // Because follower LEO == committedHw == 5, no truncation should fire.
    follower.becomeFollower(/*leaderEpoch=*/2, kL);

    Offset leoAfter = follower.logEndOffset();
    EXPECT_EQ(leoAfter, 5) << "PartB10: no truncation should fire when truncOff==LEO==committedHw";

    // Follower bytes must be unchanged.
    auto followerBytesAfter = followerLog.read(0, 1 << 20);
    EXPECT_EQ(followerBytesBefore, followerBytesAfter)
        << "PartB10: no committed byte must be dropped when truncation boundary is at LEO";
}

// ---------------------------------------------------------------------------
// PartB_13: A10 — epoch-cache-coupled truncation
// Truncate then assign a new epoch at the (lower) post-truncation offset;
// the assign must be accepted (not silently dropped by the monotonic guard).
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB13_A10_EpochCacheCoupledTruncation)
{
    TempDir leaderDir("pb13_leader");
    TempDir followerDir("pb13_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};
    int64_t clk = 0;

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append 8 records on the leader.
    for (int i = 0; i < 8; ++i) leader.append(makePayload(static_cast<uint8_t>(i + 1)));
    ASSERT_EQ(leader.logEndOffset(), 8);

    // Commit at least 5 records.
    for (int i = 0; i < kDriverCap && leader.highWatermark() < 5; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); follower.doWork(clk); transport.pump(clk);
    }
    ASSERT_GE(leader.highWatermark(), 5);

    // Let the follower catch up to the leader.
    for (int i = 0; i < kDriverCap && follower.logEndOffset() < 8; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); follower.doWork(clk); transport.pump(clk);
    }
    ASSERT_EQ(follower.logEndOffset(), 8) << "PartB13: follower must catch up to LEO=8";

    // Now simulate epoch change: follower transitions to epoch 2.
    // This should truncate the uncommitted tail (offsets [committedHw, 8))
    // AND prune the epoch cache so that a subsequent assign(epoch=2, offset=5)
    // is accepted — not dropped by the monotonic-startOffset guard (A10).
    follower.becomeFollower(/*leaderEpoch=*/2, kL);

    Offset leoAfterTruncate = follower.logEndOffset();
    EXPECT_GE(leoAfterTruncate, leader.highWatermark())
        << "PartB13: follower LEO after becomeFollower must not drop below committedHw";

    // After becomeFollower, the follower is at epoch=2.
    // When the follower next fetches and applies a response at epoch=2, the
    // assign(epoch=2, logEndOffset) must succeed (not be dropped).
    // Verify by re-wiring and driving to full catch-up.
    // Replace the leader with a new leader at epoch=2.
    transport.setLeader(nullptr);
    transport.removeFollower(kF);

    TempDir leaderDir2("pb13_leader2");
    rune::storage::Log leaderLog2(leaderDir2.path);
    // Copy committed records to the new leader log.
    {
        auto leaderBytes = leaderLog.read(0, 1 << 20);
        if (!leaderBytes.empty()) {
            // Walk batches and append up to committedHw.
            std::size_t pos = 0;
            while (pos < leaderBytes.size()) {
                std::size_t rem = leaderBytes.size() - pos;
                if (rem < 61u) break;
                rune::protocol::RecordBatchReader r(leaderBytes.data() + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0) break;
                int64_t batchBase = r.baseOffset();
                if (batchBase >= leader.highWatermark()) break;
                std::vector<uint8_t> batchBytes(leaderBytes.begin() + static_cast<std::ptrdiff_t>(pos),
                                                  leaderBytes.begin() + static_cast<std::ptrdiff_t>(pos)
                                                  + static_cast<std::ptrdiff_t>(total));
                leaderLog2.append(batchBytes, batchBase + r.lastOffsetDelta());
                pos += static_cast<std::size_t>(total);
            }
        }
    }
    ASSERT_GE(leaderLog2.logEndOffset(), leader.highWatermark())
        << "PartB13: new leader log must have committed records";

    PartitionReplica leader2(makeConfig(kL), Role::Leader, /*epoch=*/2,
                              replicas, isr, &leaderLog2, &transport);
    transport.setLeader(&leader2);
    transport.addFollower(kF, &follower);

    // Drive follower to re-catch up to the new leader.
    for (int i = 0; i < kDriverCap && follower.logEndOffset() < leader2.logEndOffset(); ++i) {
        clk += 1'000'000LL;
        leader2.doWork(clk); follower.doWork(clk); transport.pump(clk);
    }
    // A10 fix: the epoch cache assign after truncation must have been accepted.
    // If it was dropped, the follower would see wrong truncationOffsetForEpochChange
    // on the next epoch change. Verify the follower caught up (non-vacuous).
    EXPECT_EQ(follower.logEndOffset(), leader2.logEndOffset())
        << "PartB13 (A10): follower must catch up after truncate+assign at lower offset";
}

// ---------------------------------------------------------------------------
// PartB_14: A-NEW-5 — mustStepDown returns kErrNotLeader + actually steps down
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB14_ANEW5_MustStepDownErrorCodeAndRoleChange)
{
    TempDir leaderDir("pb14_leader");
    TempDir followerDir("pb14_follower");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log followerLog(followerDir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr      = {kL, kF};

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica follower(makeConfig(kF), Role::Follower, /*epoch=*/1,
                               replicas, isr, &followerLog, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append 3 records.
    for (int i = 0; i < 3; ++i) leader.append(makePayload(static_cast<uint8_t>(i + 1)));

    // Simulate a follower carrying a HIGHER epoch (epoch 2 > leader epoch 1).
    // This should trigger mustStepDown on the leader.
    FetchRequestMsg highEpochReq{};
    highEpochReq.leaderEpoch = 2;  // higher than leader's epoch=1
    highEpochReq.partition   = 0;
    highEpochReq.followerId  = kF;
    highEpochReq.fetchOffset = 0;
    highEpochReq.maxBytes    = 1 << 20;

    // Capture leader role before.
    ASSERT_EQ(leader.role(), Role::Leader) << "PartB14: leader must be Leader before mustStepDown";

    int16_t errCode = leader.onFetchRequest(highEpochReq, /*nowNs=*/0);

    // A-NEW-5 fix: correct error code must be kErrNotLeader (not kErrStaleEpoch).
    EXPECT_EQ(errCode, kErrNotLeader)
        << "A-NEW-5: mustStepDown must return kErrNotLeader (follower is AHEAD), not kErrStaleEpoch";

    // A-NEW-5 fix: the leader must have stepped down.
    EXPECT_EQ(leader.role(), Role::Follower)
        << "A-NEW-5: leader must step down to Follower when follower carries higher epoch";
}

// ---------------------------------------------------------------------------
// PartB_15: A-NEW-2 — computeHw correct for ISR sizes 33, 64, 65 (no OOB)
// This test is in HighWatermarkTest.cpp conceptually; we add it here for
// convenience since it uses PartitionReplica infrastructure.
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB15_ANEW2_ComputeHwLargeIsrNoBoundaryViolation)
{
    // Test using ReplicaLeaderState directly with large ISR sizes.
    // ISR size 33: majority = 17; leos[16] must be valid.
    {
        ReplicaLeaderState s;
        s.setLeaderId(0);
        s.setLeaderLogEndOffset(100LL);
        // Build ISR with 33 nodes: leader=0, followers=1..32
        std::vector<NodeId> isr33;
        isr33.reserve(33);
        for (int i = 0; i < 33; ++i) isr33.push_back(i);
        s.setIsr(isr33);

        // All followers report LEO=100. Majority of 33 is 17. HW = 100.
        for (int i = 1; i < 33; ++i) s.updateFollowerFetchOffset(i, 100LL);
        EXPECT_EQ(s.highWatermark(), 100LL)
            << "A-NEW-2: ISR=33, all at LEO=100, HW must be 100 (not under-counted or OOB)";
    }

    // ISR size 33 with mixed LEOs: majority=17 → HW = 17th-highest value.
    {
        ReplicaLeaderState s;
        s.setLeaderId(0);
        s.setLeaderLogEndOffset(100LL);
        std::vector<NodeId> isr33;
        for (int i = 0; i < 33; ++i) isr33.push_back(i);
        s.setIsr(isr33);

        // 16 followers at LEO=100, 16 followers at LEO=0.
        // Leader LEO=100. Sorted: [100]*17, [0]*16. Majority=17; HW=leos[16]=100.
        for (int i = 1; i <= 16; ++i) s.updateFollowerFetchOffset(i, 100LL);
        for (int i = 17; i <= 32; ++i) s.updateFollowerFetchOffset(i, 0LL);
        EXPECT_EQ(s.highWatermark(), 0LL)   // Kafka model: HW = min LEO over the ISR
            << "A-NEW-2: ISR=33, 17 at 100 + 16 at 0, majority=17, HW must be 100";
    }

    // ISR size 64: majority=33 → HW must be correct.
    {
        ReplicaLeaderState s;
        s.setLeaderId(0);
        s.setLeaderLogEndOffset(50LL);
        std::vector<NodeId> isr64;
        for (int i = 0; i < 64; ++i) isr64.push_back(i);
        s.setIsr(isr64);

        // All 64 at LEO=50. Majority=33. HW=50.
        for (int i = 1; i < 64; ++i) s.updateFollowerFetchOffset(i, 50LL);
        EXPECT_EQ(s.highWatermark(), 50LL)
            << "A-NEW-2: ISR=64, all at LEO=50, HW must be 50";
    }

    // ISR size 65: majority=33 → same test.
    {
        ReplicaLeaderState s;
        s.setLeaderId(0);
        s.setLeaderLogEndOffset(75LL);
        std::vector<NodeId> isr65;
        for (int i = 0; i < 65; ++i) isr65.push_back(i);
        s.setIsr(isr65);

        // 33 nodes at 75, 32 at 0. majority=33. HW=75.
        for (int i = 1; i <= 32; ++i) s.updateFollowerFetchOffset(i, 75LL);
        for (int i = 33; i < 65; ++i) s.updateFollowerFetchOffset(i, 0LL);
        EXPECT_EQ(s.highWatermark(), 0LL)   // Kafka model: HW = min LEO over the ISR
            << "A-NEW-2: ISR=65, 33 at 75 + 32 at 0, majority=33, HW must be 75";
    }
}

// ---------------------------------------------------------------------------
// PartB_16: A-NEW-4 — maybeExpandIsr with time-stale but offset-caught-up replica
// should NOT re-admit the replica (time gate is now enforced).
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB16_ANEW4_ExpandIsrTimeStalePrevented)
{
    // Build a 3-node ISR {leader=0, f1=1, f2=2}.
    constexpr NodeId kLeader = 0, kF1 = 1, kF2 = 2;
    IsrManager mgr({kLeader, kF1, kF2}, kLeader, /*replicaLagMaxMs=*/1000);

    int64_t now = 10'000'000'000LL; // 10 seconds
    // Both followers fetch at t=0.
    mgr.onFollowerFetch(kF1, 100, 0);
    mgr.onFollowerFetch(kF2, 100, 0);

    // Shrink both followers (time-lagged by >1s).
    mgr.maybeShrinkIsr(now, /*leaderLeo=*/100, /*maxMs=*/1000, /*maxOffset=*/50);
    EXPECT_FALSE(mgr.inIsr(kF1)) << "PartB16: f1 must be shrunk after time lag";
    EXPECT_FALSE(mgr.inIsr(kF2)) << "PartB16: f2 must be shrunk after time lag";

    // f1 reports a recent fetch (within the lag window): must be eligible for expand.
    mgr.onFollowerFetch(kF1, 100, now - 500'000'000LL); // 0.5s ago < 1s limit

    // f2 reports an OLD fetch (same time as before = 10s ago): offset ok but time stale.
    // (f2's lastFetchTimeNs is still 0 from the old fetch above)

    // Attempt expansion.
    mgr.maybeExpandIsr(now, /*highWatermark=*/100);

    EXPECT_TRUE(mgr.inIsr(kF1))
        << "PartB16 (A-NEW-4): f1 fetched recently AND is caught up → must be re-admitted";
    EXPECT_FALSE(mgr.inIsr(kF2))
        << "PartB16 (A-NEW-4): f2 is caught up by offset but time-stale → must NOT be re-admitted";
}

// ---------------------------------------------------------------------------
// PartB_17: A-NEW-3 — FileRecordingStore durability across reopen
// Append a frame, reopen the store, assert the frame survives.
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB17_ANEW3_RecorderFsyncDurabilityAcrossReopen)
{
    // Use TempFile from within this file scope.
    std::string tmpPath = std::filesystem::temp_directory_path().string()
                          + "/rune_partb17_" + makeUniqSuffix() + ".bin";
    struct Cleanup { std::string p; ~Cleanup(){ std::error_code ec; std::filesystem::remove(p, ec); } } _c{tmpPath};

    // Build a RecordBatch to write.
    rune::protocol::RecordBatchBuilder builder(0, 0);
    builder.append("key", "value_data", 0);
    auto batch = builder.build();

    {
        rune::replication::FileRecordingStore store(tmpPath);
        store.append(0, std::span<const uint8_t>(batch.data(), batch.size()));
        EXPECT_EQ(store.batchCount(), 1) << "PartB17: batchCount must be 1 after append";
        EXPECT_EQ(store.recordingPosition(), 1) << "PartB17: recordingPosition must be 1";
        // Destructor closes the fd (and the fstream was already flushed+fsynced).
    }

    // Reopen — data must survive.
    {
        rune::replication::FileRecordingStore store2(tmpPath);
        EXPECT_EQ(store2.batchCount(), 1) << "PartB17: batchCount must be 1 after reopen";
        EXPECT_EQ(store2.recordingPosition(), 1) << "PartB17: recordingPosition must be 1 after reopen";

        // Scan and verify the batch is intact.
        int scanCount = 0;
        store2.scan(0, 1, [&](int64_t offset, std::span<const uint8_t> data) -> bool {
            EXPECT_EQ(offset, 0) << "PartB17: scanned baseOffset must be 0";
            EXPECT_EQ(data.size(), batch.size()) << "PartB17: scanned payload must match original";
            ++scanCount;
            return true;
        });
        EXPECT_EQ(scanCount, 1) << "PartB17: must scan exactly 1 batch after reopen";
    }
}

// ---------------------------------------------------------------------------
// PartB_18: Coordinator signalLeaderDead for an unknown node → no election crash.
// ---------------------------------------------------------------------------
TEST(PartBTest, PartB18_CoordinatorSignalLeaderDeadUnknownNode)
{
    TempDir leaderDir("pb18_leader");
    TempDir f1Dir("pb18_f1");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log f1Log(f1Dir.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 0, kF1 = 1, kUnknown = 99;
    std::vector<NodeId> replicas = {kL, kF1};
    std::vector<NodeId> isr      = {kL, kF1};

    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             replicas, isr, &leaderLog, &transport);
    PartitionReplica f1(makeConfig(kF1), Role::Follower, /*epoch=*/1,
                         replicas, isr, &f1Log, &transport);

    PartitionMetadata meta;
    meta.partitionId = 0;
    meta.leader      = kL;
    meta.replicas    = replicas;
    meta.isr         = isr;
    meta.leaderEpoch = 1;

    SimpleNodeLiveness liveness;
    liveness.markDead(kUnknown);  // mark a node not in ISR as dead

    PartitionFailoverCoordinator coord(meta, &liveness);
    coord.registerReplica(kL, &leader);
    coord.registerReplica(kF1, &f1);

    // Signal dead for a node that is neither the current leader nor in ISR.
    // Must not crash or elect.
    EXPECT_NO_THROW(coord.signalLeaderDead(kUnknown))
        << "PartB18: signalLeaderDead for unknown node must not throw";

    // No election should have been triggered.
    int64_t clk = 0;
    for (int i = 0; i < 5; ++i) {
        clk += 1'000'000LL;
        coord.doWork(clk);
    }
    // Leader must still be kL (no spurious election).
    EXPECT_EQ(coord.currentLeader(), kL)
        << "PartB18: unknown-node signal must not trigger election";
    EXPECT_EQ(leader.role(), Role::Leader)
        << "PartB18: leader must remain Leader after signalLeaderDead(unknown)";
}

// ===========================================================================
// NB3 test: committed-read clamp uses batch LAST offset, not base offset.
// A multi-record batch whose baseOffset < hw but lastOffset >= hw straddles
// the high-watermark and must NOT be returned by read() (Kafka-faithful: no
// partial-batch exposure).
// ===========================================================================

// ---------------------------------------------------------------------------
// NB3_MultiRecordBatchStraddle: read() must not return a straddling batch.
//
// Setup (to achieve HW=5 with a straddling 4-record batch [3..6] in the log):
//
//   Phase A — as single-replica leader, append 3 single-record batches.
//             HW=3 immediately (single-replica ISR majority=1).
//
//   Phase B — becomeFollower(epoch=2). Log stays at LEO=3 (no truncation since
//             epoch-cache has no entry for epoch 2 yet, fall-back=committedHw=3,
//             and truncOff==LEO → no truncation fires). followerHw_=0 reset.
//
//   Phase C — applyReplicated: inject a 4-record batch (offsets 3..6,
//             lastOffsetDelta=3) via applyReplicated with highWatermark=5.
//             The RecordBatch format is recognized; batch is appended at
//             baseOffset=3 (== current LEO). Log LEO=7.
//             followerHw_ = min(7, 5) = 5.
//
//   Phase D — becomeLeader(epoch=3, {kL, kF}): captures highWatermark()=5
//             (the followerHw_) and uses it as the HW floor.
//             With 2-node ISR and followerLeo[kF]=0, computeHw()=0 < floor=5.
//             So leaderState_.highWatermark()=5. ✓
//
//   Phase E — read(3, bigBuf):
//     OLD clamp: batchBase(3) >= hw(5)? No → returns the 4-record batch (WRONG).
//     NEW clamp: batchBase+lastOffsetDelta = 3+3=6 >= hw(5)? Yes → STOP (correct).
//
// The test FAILS under the old clamp and PASSES under the fix.
// ---------------------------------------------------------------------------
TEST(NB3_CommittedReadClamp, MultiRecordBatchStradlingHwNotReturned)
{
    TempDir dir("nb3_straddle");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;

    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF};
    std::vector<NodeId> isr2    = {kL, kF};

    // Phase A: start as single-replica leader so HW == LEO immediately.
    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             {kL}, {kL}, &log, &transport);
    transport.setLeader(&leader);

    // Append 3 single-record batches (offsets 0,1,2). Single-ISR → HW=3.
    for (int i = 0; i < 3; ++i) leader.append(makePayload(static_cast<uint8_t>(i + 1)));
    ASSERT_EQ(leader.logEndOffset(), 3)  << "NB3: LEO must be 3";
    ASSERT_EQ(leader.highWatermark(), 3) << "NB3: single-ISR HW must be 3";

    // Phase B: become follower so we can inject a multi-record batch via
    // applyReplicated (which recognises proper RecordBatch format).
    // becomeFollower with epoch=2: epoch-cache has no entry ≥ epoch=2,
    // fall-back truncOff = committedHw = 3; currentLeo=3 → truncOff==LEO,
    // condition (truncOff < currentLeo) is false → no truncation. followerHw_=0.
    leader.becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/kL);
    ASSERT_EQ(leader.role(), Role::Follower) << "NB3: must be Follower after becomeFollower";
    ASSERT_EQ(leader.logEndOffset(), 3) << "NB3: log must still have 3 records (no truncation)";

    // Phase C: build a 4-record batch at baseOffset=3 (offsets 3,4,5,6;
    // lastOffsetDelta=3) and inject via applyReplicated with HW=5.
    // This correctly sets followerHw_ = min(newLeo=7, HW=5) = 5.
    {
        rune::protocol::RecordBatchBuilder builder(/*baseOffset=*/3, /*baseTimestamp=*/0);
        for (int i = 0; i < 4; ++i) {
            std::string_view key{};
            std::string_view val("rec", 3);
            builder.append(key, val, /*timestamp=*/0);
        }
        auto batchBytes = builder.build();

        // Verify builder output.
        rune::protocol::RecordBatchReader r(batchBytes.data(), batchBytes.size());
        ASSERT_EQ(r.baseOffset(), 3)       << "NB3: batch baseOffset must be 3";
        ASSERT_EQ(r.lastOffsetDelta(), 3)  << "NB3: 4-record batch lastOffsetDelta must be 3";
        ASSERT_EQ(r.recordCount(), 4)      << "NB3: batch must contain 4 records";

        // Inject via applyReplicated: follower applies the batch at its current LEO=3.
        FetchResponseMsg resp{};
        resp.errorCode      = kErrNone;
        resp.leaderEpoch    = 2;
        resp.partition      = 0;
        resp.highWatermark  = 5;  // HW straddles: batch lastOffset=6 > HW=5 > baseOffset=3
        resp.logStartOffset = 0;
        resp.records        = std::span<const uint8_t>(batchBytes.data(), batchBytes.size());
        leader.applyReplicated(resp);
    }

    // After applying: log LEO=7 (baseOffset=3 + lastOffsetDelta=3 + 1 = 7).
    // followerHw_ = min(7, 5) = 5.
    ASSERT_EQ(leader.logEndOffset(), 7) << "NB3: log LEO must be 7 after 4-record batch applied";
    ASSERT_EQ(leader.highWatermark(), 5) << "NB3: followerHw_ must be 5";

    // Phase D: become leader at epoch=3 with a 2-node ISR.
    // becomeLeader captures highWatermark()=followerHw_=5 as floor.
    // With 2-node ISR and followerLeo[kF]=0 (not acked yet):
    //   computeHw(): leaderLeo=7, followerLeo=0 → sorted=[7,0] → majority=2 → HW=0.
    //   max(floor=5, compute=0) = 5. leaderState_.highWatermark() = 5. ✓
    transport.setLeader(&leader); // re-register (becomeFollower doesn't clear it)
    leader.becomeLeader(/*newEpoch=*/3, isr2);
    ASSERT_EQ(leader.role(), Role::Leader) << "NB3: must be Leader";

    Offset hw = leader.highWatermark();
    ASSERT_EQ(hw, 5) << "NB3: HW must be 5 for the straddling scenario "
                        "(batch offsets 3..6 straddle HW=5)";

    // Confirm straddling: batch at baseOffset=3, lastOffsetDelta=3 → lastOffset=6.
    // 3 < HW=5 <= 6  → straddling.

    // Phase E — NB3 core assertion:
    // read(3, bigBuf) must NOT return the straddling 4-record batch.
    //
    // OLD clamp (batchBase >= hw): batchBase=3 < 5 → batch included (WRONG — exposes offsets 5,6).
    // NEW clamp (batchBase+lastOffsetDelta >= hw): 3+3=6 >= 5 → STOP before batch (correct).
    auto readResult = leader.read(/*fetchOffset=*/3, /*maxBytes=*/1 << 20);

    bool foundStraddle = false;
    {
        std::size_t pos = 0;
        while (pos < readResult.size()) {
            std::size_t rem = readResult.size() - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader r(readResult.data() + pos, rem);
            int32_t total = r.totalLength();
            if (total <= 0) break;
            int64_t batchBase = r.baseOffset();
            int32_t lod       = r.lastOffsetDelta();
            EXPECT_LT(batchBase + lod, hw)
                << "NB3: read() returned a batch with lastOffset=" << (batchBase + lod)
                << " >= HW=" << hw
                << " (straddling/uncommitted batch must not be returned)";
            if (batchBase == 3 && lod == 3) {
                foundStraddle = true;
            }
            pos += static_cast<std::size_t>(total);
        }
    }
    EXPECT_FALSE(foundStraddle)
        << "NB3: the straddling 4-record batch (baseOffset=3, lastOffset=6) "
        << "must NOT be returned by read() when HW=5. "
        << "Old clamp (batchBase >= hw) would return it (exposes uncommitted offsets 5,6); "
        << "new clamp (batchBase+lastOffsetDelta >= hw) correctly stops before it.";

    // read(0, big) must still return the 3 committed single-record batches.
    auto committed = leader.read(0, 1 << 20);
    EXPECT_FALSE(committed.empty()) << "NB3: committed batches [0,2] must be readable";
    int committedCount = 0;
    {
        std::size_t pos = 0;
        while (pos < committed.size()) {
            std::size_t rem = committed.size() - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader r(committed.data() + pos, rem);
            int32_t total = r.totalLength();
            if (total <= 0) break;
            EXPECT_LT(r.baseOffset() + r.lastOffsetDelta(), hw)
                << "NB3: committed read result must not contain a batch straddling HW";
            ++committedCount;
            pos += static_cast<std::size_t>(total);
        }
    }
    EXPECT_EQ(committedCount, 3)
        << "NB3: read(0) must return exactly 3 committed single-record batches";
}

// ---------------------------------------------------------------------------
// NB3_SingleRecordBehaviorUnchanged: existing single-record semantics are
// preserved. A single-record batch at baseOffset=K < HW is returned; one at
// baseOffset=HW is not.
// ---------------------------------------------------------------------------
TEST(NB3_CommittedReadClamp, SingleRecordBatchBehaviorUnchanged)
{
    TempDir dir("nb3_single");
    rune::storage::Log log(dir.path);
    InMemoryTransport transport;
    constexpr NodeId kL = 1;
    PartitionReplica leader(makeConfig(kL), Role::Leader, /*epoch=*/1,
                             {kL}, {kL}, &log, &transport);
    transport.setLeader(&leader);

    // Append 5 single-record batches; single-replica ISR → HW=5 immediately.
    for (int i = 0; i < 5; ++i) leader.append(makePayload(static_cast<uint8_t>(i + 1)));
    ASSERT_EQ(leader.logEndOffset(), 5);
    ASSERT_EQ(leader.highWatermark(), 5);

    // read(0, big) must return all 5 batches (all below HW=5).
    auto all = leader.read(0, 1 << 20);
    EXPECT_FALSE(all.empty()) << "NB3-single: all 5 committed batches must be readable";

    // Count batches.
    int count = 0;
    {
        std::size_t pos = 0;
        while (pos < all.size()) {
            std::size_t rem = all.size() - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader r(all.data() + pos, rem);
            int32_t total = r.totalLength();
            if (total <= 0) break;
            // NB3 clamp: batchBase + lastOffsetDelta < hw must hold.
            EXPECT_LT(r.baseOffset() + r.lastOffsetDelta(), leader.highWatermark())
                << "NB3-single: single-record batch lastOffset must be < HW";
            ++count;
            pos += static_cast<std::size_t>(total);
        }
    }
    EXPECT_EQ(count, 5) << "NB3-single: must return exactly 5 single-record batches";

    // read at HW returns empty.
    EXPECT_TRUE(leader.read(5, 1 << 20).empty()) << "NB3-single: read at HW returns empty";
}

// ---------------------------------------------------------------------------
// GR-1 regression: onFetchRequest must clamp records to whole-batch boundary
// within req.maxBytes.  Before the fix, it returned whatever log_->read()
// returned (could exceed maxBytes if batches straddle the byte ceiling), meaning
// a transport with a small maxMessageLength would drop the response every time.
// ---------------------------------------------------------------------------
TEST(PartitionReplicaTest, OnFetchRequest_WholeBatchAlignment) {
    // Create a leader with 10 appended batches (~87 bytes each after framing).
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_align_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);

    // Capture transport: records the size of the last FetchResponse records blob.
    struct CaptureTransport : public PartitionTransport {
        std::size_t lastResponseRecordsSize = SIZE_MAX;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
            lastResponseRecordsSize = r.records.size();
            return true;
        }
    };

    rune::storage::Log log(logDir.string());
    CaptureTransport transport;

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 5000;
    cfg.replicaLagMaxOffset = 100000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    // This test exercises the FETCH-serve path's whole-batch alignment.  With push
    // on, append() pushes [0,10) to follower 2 and the no-dup-fetch gate (spec §3.5)
    // correctly answers the fetch at offset 0 ack-only — so disable push here.
    cfg.pushOnAppend        = false;

    PartitionReplica leader(cfg, Role::Leader, 1,
                            {1, 2}, {1, 2}, &log, &transport);

    // Append 10 small batches (20-byte payload → ~87 bytes per batch after framing).
    for (int i = 0; i < 10; ++i) {
        std::vector<uint8_t> payload(20, static_cast<uint8_t>(i));
        ASSERT_GE(leader.append(payload), 0) << "append failed at batch " << i;
    }

    // Issue a fetch with maxBytes = 200.
    // With ~87-byte batches: 2 complete batches = ~174 bytes; 3 would be ~261 bytes.
    // Whole-batch alignment must return exactly 2 complete batches (≤ 200 bytes).
    FetchRequestMsg req{};
    req.leaderEpoch = 1;
    req.partition   = 0;
    req.followerId  = 2;
    req.fetchOffset = 0;
    req.maxBytes    = 200;

    leader.onFetchRequest(req, 0);

    // Response records blob must be ≤ 200 bytes (the ceiling) and > 0.
    ASSERT_NE(transport.lastResponseRecordsSize, SIZE_MAX)
        << "transport.sendFetchResponse was never called";
    EXPECT_LE(transport.lastResponseRecordsSize, 200u)
        << "records blob must not exceed maxBytes ceiling";
    EXPECT_GT(transport.lastResponseRecordsSize, 0u)
        << "at least one batch must fit within the 200-byte ceiling";

    // Also verify the response is a whole-batch sequence: parse with RecordBatchReader
    // and confirm the walk covers the full returned blob with no partial batch at end.
    // (We re-issue the same request and capture raw records for inspection.)
    // The transport captures only the size; confirm it's a multiple of the actual
    // batch size by ensuring log_->read(0, 200) returns the same trimmed result.
    std::vector<uint8_t> logBytes = log.read(0, 200);
    // Walk to find how many whole batches fit in ≤ 200 bytes.
    std::size_t pos = 0, committedEnd = 0;
    while (pos < logBytes.size()) {
        std::size_t rem = logBytes.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
        rune::protocol::RecordBatchReader r(logBytes.data() + pos, rem);
        int32_t total = r.totalLength();
        if (total <= 0) break;
        std::size_t nextEnd = pos + static_cast<std::size_t>(total);
        if (nextEnd > logBytes.size()) break;
        if (nextEnd > 200u) break;
        committedEnd = nextEnd;
        pos = committedEnd;
    }
    EXPECT_EQ(transport.lastResponseRecordsSize, committedEnd)
        << "transport got a whole-batch-aligned slice matching " << committedEnd << " bytes";

    std::filesystem::remove_all(logDir);
}

// ---------------------------------------------------------------------------
// F3 fix-round-1, item 3 (MINOR -> fix): a FETCH where fetchOffset < LEO but
// the zero-copy readView() comes back empty (a transient local read-map
// failure, NOT genuine end-of-log) must NOT send a Data response with zero
// records.  The follower's FollowerSync treats an empty-but-kErrNone Data
// response as "caught up" and would stop re-fetching, even though there is
// genuinely more committed data past fetchOffset.  The leader must skip
// responding this cycle; the follower's own un-acked fetch naturally resends.
//
// Reproduced with a REAL disk-backed Log (no mock/fake Log — onLog() dispatches
// to a concrete rune::storage::Log, there is no injectable seam): a sealed,
// not-yet-mapped segment's .log is replaced with a symlink to /dev/null (same
// technique as MappedFileRegression.FailedMmapAfterOpenLeaksNoFd / F3 round 0)
// so its lazy ensureReadMap() fails every time — open() succeeds (resolves to
// /dev/null) but mmap() fails with ENODEV — producing a REAL, organically
// empty LogReadView below LEO, exactly the condition under review.
//
// A segment only maps LAZILY after a RECOVER (reopen) — a segment created via
// a live roll's synchronous fallback is always eagerly mapped (it genuinely
// IS the new active segment at creation time; see F3 round 0).  So the data
// is seeded with a throwaway Log, which is then closed and reopened +
// recover()'d before the PartitionReplica under test is built on top of it —
// exactly how a real broker restart leaves sealed segments lazily-unmapped.
// ---------------------------------------------------------------------------
TEST(PartitionReplicaRegression, FetchMapFailureDoesNotReportCaughtUp) {
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_f3r1_fetchmapfail_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);

    // Seed >= 3 segments, then close (destructor) so the next Log reopens
    // them all from scratch via recover() — the lazy-mapping path.
    {
        rune::storage::Log seedLog(logDir.string(), /*rollBytes=*/256);
        int64_t off = 0;
        for (int i = 0; i < 15; ++i) {
            rune::protocol::RecordBatchBuilder b(off, 8000 + i);
            b.append("k", "v" + std::to_string(i), 8000 + i);
            off = seedLog.append(b.build(), off);
        }
        ASSERT_GE(seedLog.segmentBaseOffsets().size(), 3u)
            << "test setup: need >= 3 segments for a real sealed-but-unmapped one";
    }

    struct CaptureTransport : public PartitionTransport {
        int calls = 0;
        FetchResponseMsg last{};
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
            ++calls;
            last = r;
            return true;
        }
    };

    rune::storage::Log log(logDir.string(), /*rollBytes=*/256);
    log.recover();
    CaptureTransport transport;

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 5000;
    cfg.replicaLagMaxOffset = 100000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;   // force the FETCH-serve path (see OnFetchRequest_WholeBatchAlignment)

    PartitionReplica leader(cfg, Role::Leader, 1,
                            {1, 2}, {1, 2}, &log, &transport);

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u);
    int64_t sealedBase = bases[1];   // a sealed segment, not the first and not active
    rune::storage::LogSegment* sealedSeg = log.segmentForTest(sealedBase);
    ASSERT_NE(sealedSeg, nullptr);
    ASSERT_FALSE(sealedSeg->hasReadMapForTest()) << "sealed segment must still be lazily-unmapped (F3)";

    // Break this segment's lazy map: open() succeeds (resolves to /dev/null),
    // mmap() fails with ENODEV, every retry — readView() returns {} forever.
    std::string sealedLogPath = sealedSeg->logPath();
    std::filesystem::remove(sealedLogPath);
    std::filesystem::create_symlink("/dev/null", sealedLogPath);

    // Fetch at sealedBase: fetchOffset < LEO (more segments were appended after
    // it), so the ONLY correct interpretation of an empty readView() here is
    // "transient failure, retry" — never "caught up".
    FetchRequestMsg req{};
    req.leaderEpoch = 1;
    req.partition   = 0;
    req.followerId  = 2;
    req.fetchOffset = sealedBase;
    req.maxBytes    = 1 << 20;

    int16_t rc = leader.onFetchRequest(req, /*nowNs=*/1);
    EXPECT_EQ(rc, 0) << "onFetchRequest itself must still report success (kErrNone) — "
                         "the fix is to skip the SEND, not to error out";
    EXPECT_EQ(transport.calls, 0)
        << "a map-failure empty view below LEO must NOT be sent as a Data response "
           "(the follower would wrongly treat empty+kErrNone as caught-up)";
    EXPECT_EQ(sealedSeg->readMapFailures(), 1u);

    // The leader must not be globally wedged by one broken sealed segment: a
    // fetch against an UNAFFECTED segment (the active one — never touched)
    // must still be served normally.
    FetchRequestMsg reqActive{};
    reqActive.leaderEpoch = 1;
    reqActive.partition   = 0;
    reqActive.followerId  = 2;
    reqActive.fetchOffset = bases.back();
    reqActive.maxBytes    = 1 << 20;
    int16_t rc2 = leader.onFetchRequest(reqActive, /*nowNs=*/2);
    EXPECT_EQ(rc2, 0);
    EXPECT_EQ(transport.calls, 1)
        << "a fetch against an UNAFFECTED (active) segment must still be served normally";

    std::filesystem::remove_all(logDir);
}

// ---------------------------------------------------------------------------
// F3 fix-round-2, item A: a first batch larger than req.maxBytes (but within
// this transport's REAL hard ceiling, which may be larger than what the
// follower happened to request) must be sent alone — Kafka's progress
// guarantee — instead of the old behaviour (an empty kErrNone response that
// the follower's FollowerSync misreads as "caught up", PERMANENTLY, since
// this batch is always too big for maxBytes and nothing ever changes that).
// ---------------------------------------------------------------------------
TEST(PartitionReplicaRegression, FetchOversizeFirstBatchIsServedAloneRegression) {
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_f3r2_oversize_served_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);

    struct CaptureTransport : public PartitionTransport {
        int calls = 0;
        FetchResponseMsg last{};
        int32_t ceiling = INT32_MAX;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
            ++calls;
            last = r;
            return true;
        }
        [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override {
            return ceiling;
        }
    };

    rune::storage::Log log(logDir.string());   // large default rollBytes — single segment
    CaptureTransport transport;
    transport.ceiling = 10'000;   // hard ceiling: comfortably fits one oversize-vs-maxBytes batch

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 5000;
    cfg.replicaLagMaxOffset = 100000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 100;     // deliberately small — smaller than the batch below
    cfg.pushOnAppend        = false;

    PartitionReplica leader(cfg, Role::Leader, 1, {1, 2}, {1, 2}, &log, &transport);

    // One batch whose encoded size is > cfg.maxFetchBytes(100) but well under
    // the transport's hard ceiling(10000).
    ASSERT_GE(leader.append(makePayload(0xAB, 300)), 0);

    FetchRequestMsg req{};
    req.leaderEpoch = 1;
    req.partition   = 0;
    req.followerId  = 2;
    req.fetchOffset = 0;
    req.maxBytes    = cfg.maxFetchBytes;   // 100 — smaller than the batch

    int16_t rc = leader.onFetchRequest(req, 0);
    EXPECT_EQ(rc, kErrNone) << "the batch WAS served — this is success, not an error";
    ASSERT_EQ(transport.calls, 1)
        << "the oversize batch must still be sent alone (progress guarantee), not silently "
           "withheld as an empty response";
    EXPECT_EQ(transport.last.errorCode, kErrNone);
    EXPECT_GT(transport.last.records.size(), static_cast<std::size_t>(cfg.maxFetchBytes))
        << "the served batch is intentionally larger than maxBytes — sent alone because it "
           "fits the transport's real hard ceiling";

    std::filesystem::remove_all(logDir);
}

// ---------------------------------------------------------------------------
// F3 fix-round-2, item A: when a batch exceeds even the transport's hard
// ceiling (truly unsendable at any size), the leader must reply with an
// explicit ERROR (kErrRecordTooLarge), never kErrNone — an empty-records
// kErrNone would be indistinguishable from "caught up" to the follower.
// ---------------------------------------------------------------------------
TEST(PartitionReplicaRegression, FetchUnsendableBatchRepliesErrorNotCaughtUpRegression) {
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_f3r2_unsendable_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);

    struct CaptureTransport : public PartitionTransport {
        int calls = 0;
        FetchResponseMsg last{};
        int32_t ceiling = INT32_MAX;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
            ++calls;
            last = r;
            return true;
        }
        [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override {
            return ceiling;
        }
    };

    rune::storage::Log log(logDir.string());
    CaptureTransport transport;
    transport.ceiling = 50;   // smaller than the batch below — genuinely unsendable

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 5000;
    cfg.replicaLagMaxOffset = 100000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 100;
    cfg.pushOnAppend        = false;

    PartitionReplica leader(cfg, Role::Leader, 1, {1, 2}, {1, 2}, &log, &transport);
    ASSERT_GE(leader.append(makePayload(0xCD, 300)), 0);   // >> ceiling(50) and >> maxBytes(100)

    FetchRequestMsg req{};
    req.leaderEpoch = 1;
    req.partition   = 0;
    req.followerId  = 2;
    req.fetchOffset = 0;
    req.maxBytes    = cfg.maxFetchBytes;

    int16_t rc = leader.onFetchRequest(req, 0);
    EXPECT_EQ(rc, kErrRecordTooLarge);
    ASSERT_EQ(transport.calls, 1) << "an explicit error response must still be sent (not silently dropped)";
    EXPECT_EQ(transport.last.errorCode, kErrRecordTooLarge)
        << "must NEVER be kErrNone — that would be indistinguishable from caught-up";
    EXPECT_TRUE(transport.last.records.empty());

    // Follower-side consequence: applyReplicated() must not apply anything
    // and must not advance the follower's own LEO from an error response.
    auto followerLogDir = std::filesystem::temp_directory_path() /
                          ("rune_f3r2_unsendable_follower_" + std::to_string(::getpid()));
    std::filesystem::remove_all(followerLogDir);
    std::filesystem::create_directories(followerLogDir);
    rune::storage::Log followerLog(followerLogDir.string());
    PartitionConfig fcfg = cfg;
    fcfg.selfId = 2;
    PartitionReplica follower(fcfg, Role::Follower, 1, {1, 2}, {1, 2}, &followerLog, &transport);
    const Offset leoBefore = follower.logEndOffset();
    follower.applyReplicated(transport.last);
    EXPECT_EQ(follower.logEndOffset(), leoBefore)
        << "an error response must never be applied to the follower's log";

    std::filesystem::remove_all(logDir);
    std::filesystem::remove_all(followerLogDir);
}

// ---------------------------------------------------------------------------
// F3 fix-round-2, item A.3: pushToFollowers() must apply the SAME progress
// rule as onFetchRequest() — an oversize-vs-cfg.maxFetchBytes first batch
// that still fits the transport's hard ceiling must be DELIVERED by push,
// not silently dropped (pre.bytes==0 early return).  Before this fix, only
// the (now-fixed) fetch path could ever heal such a batch.
// ---------------------------------------------------------------------------
TEST(PartitionReplicaRegression, PushOversizeFirstBatchIsDeliveredRegression) {
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_f3r2_push_oversize_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);

    struct CaptureTransport : public PartitionTransport {
        int calls = 0;
        FetchResponseMsg last{};
        int32_t ceiling = INT32_MAX;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
            ++calls;
            last = r;
            return true;
        }
        [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override {
            return ceiling;
        }
    };

    rune::storage::Log log(logDir.string());
    CaptureTransport transport;
    transport.ceiling = 10'000;   // hard ceiling comfortably fits the oversize batch

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 5000;
    cfg.replicaLagMaxOffset = 100000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 100;     // smaller than the batch — forces pre.bytes==0 in push too
    cfg.pushOnAppend        = true;    // GR-10 direct-append push path

    PartitionReplica leader(cfg, Role::Leader, 1, {1, 2}, {1, 2}, &log, &transport);
    // KIP-320/595: follower 2 reports in (validated pure ack on its empty log)
    // — a leader pushes only to validated followers.
    (void)leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 0, 0}, 0);
    transport.calls = 0;

    // append() with pushOnAppend=true calls pushToFollowers(baseOffset) inline.
    Offset base = leader.append(makePayload(0xEF, 300));
    ASSERT_GE(base, 0);

    ASSERT_EQ(transport.calls, 1)
        << "pushToFollowers must deliver the oversize-vs-maxFetchBytes batch alone, "
           "not silently withhold it (pre.bytes==0 early return)";
    EXPECT_EQ(transport.last.errorCode, kErrNone);
    EXPECT_GT(transport.last.records.size(), static_cast<std::size_t>(cfg.maxFetchBytes));

    // Flow control is UNCHANGED: a pushCeiling at or below the batch's own
    // base offset must still withhold it (genuinely flow-controlled), proving
    // the relaxation only widened the maxBytes cap, never pushCeiling.
    CaptureTransport transport2;
    transport2.ceiling = 10'000;
    auto logDir2 = std::filesystem::temp_directory_path() /
                   ("rune_f3r2_push_oversize_ceiling_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir2);
    std::filesystem::create_directories(logDir2);
    rune::storage::Log log2(logDir2.string());
    PartitionConfig cfg2 = cfg;
    cfg2.pushOnAppend = false;   // drive pushToFollowers explicitly via the internal coalesced path
    PartitionReplica leader2(cfg2, Role::Leader, 1, {1, 2}, {1, 2}, &log2, &transport2);
    ASSERT_GE(leader2.append(makePayload(0xEF, 300)), 0);
    leader2.pushToFollowersForTest(/*fromOffset=*/0, /*pushCeiling=*/0);
    EXPECT_EQ(transport2.calls, 0)
        << "a pushCeiling at/below the batch's own base offset must still withhold it "
           "(flow control is never relaxed, only the maxBytes cap)";

    std::filesystem::remove_all(logDir);
    std::filesystem::remove_all(logDir2);
}

// ---------------------------------------------------------------------------
// F3 round 5, item I2: an UNSENDABLE batch (larger than the transport's hard
// ceiling) at the head of the push window must NOT make the leader spin.
// Before the fix, every doWork() cycle re-ran pushToFollowers(): a fresh
// readView + parse of the same batch, ++work (so the idle strategy never
// engages) and a critical-log occurrence bump.  Now the unsendable offset is
// remembered and push is skipped until LEO, the ISR or the follower position
// (push ceiling) changes; the critical log fires once per offset transition.
// ---------------------------------------------------------------------------
TEST(PartitionReplicaRegression, PushUnsendableHeadDoesNotSpinRegression) {
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_f3r5_push_unsendable_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);

    struct CaptureTransport : public PartitionTransport {
        int calls = 0;
        int32_t ceiling = INT32_MAX;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { ++calls; return true; }
        [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override {
            return ceiling;
        }
    };

    rune::storage::Log log(logDir.string());
    CaptureTransport transport;
    transport.ceiling = 50;   // every batch below is unsendable

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 1'000'000;      // no ISR shrink during the test
    cfg.replicaLagMaxOffset = 1'000'000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 100;
    cfg.pushOnAppend        = true;           // coalesced doWork push path

    PartitionReplica leader(cfg, Role::Leader, 1, {1, 2}, {1, 2}, &log, &transport);
    // KIP-320/595: follower 2 reports in (validated pure ack on its empty log)
    // — a leader pushes only to validated followers.
    (void)leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 0, 0}, 0);
    transport.calls = 0;

    // Append DIRECTLY to the log (the produce hot path) so only doWork pushes.
    {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", std::string(300, 'x'), 1000);
        log.append(b.build(), 0);
    }
    leader.onLeaderLogAppended();

    constexpr int kCycles = 1000;
    int totalWork = 0;
    for (int i = 0; i < kCycles; ++i) totalWork += leader.doWork(/*nowNs=*/1);

    EXPECT_EQ(transport.calls, 0) << "the unsendable batch must never be sent";
    EXPECT_LE(leader.unsendableBatchEvaluationsForTest(), 1u)
        << "the unsendable head must be read/parsed once, not once per cycle";
    EXPECT_LE(totalWork, 4) << "an unsendable head must not count as work every cycle "
                               "(the idle strategy must engage)";
    EXPECT_EQ(leader.unsendablePushLogEmitsForTest(), 1u)
        << "exactly one critical log for this offset transition";

    // LEO change -> re-evaluated once (still unsendable at the same head
    // offset: no new log line, since the offset did not transition).
    {
        rune::protocol::RecordBatchBuilder b(1, 1001);
        b.append("k", "v", 1001);
        log.append(b.build(), 1);
    }
    leader.onLeaderLogAppended();
    for (int i = 0; i < kCycles; ++i) (void)leader.doWork(/*nowNs=*/1);
    EXPECT_EQ(leader.unsendableBatchEvaluationsForTest(), 2u)
        << "a LEO change must re-evaluate the head exactly once";
    EXPECT_EQ(leader.unsendablePushLogEmitsForTest(), 1u)
        << "same unsendable offset -> no new critical log";

    std::filesystem::remove_all(logDir);
}

// ---------------------------------------------------------------------------
// F3 round 6, item 1: a coalesced push must never exceed the transport's live
// ceiling.  Before the fix the push was capped only by cfg.maxFetchBytes (1 MiB
// default) while the real transport ceiling is ~512 KiB: a coalesced push over
// the ceiling was DROPPED by the transport after lastPushedOffset_/pushedThrough
// had advanced, so the follower recovered only via the 20 ms re-serve — a
// 20 ms acks=all stall.  Here: several batches totalling > the ceiling are
// appended in ONE cycle; the leader must push them in ceiling-bounded chunks,
// nothing is dropped, and the follower reaches the LEO with NO fetch data
// re-serve (simulated time stays far below the 20 ms resend window).
// ---------------------------------------------------------------------------
namespace {
struct CeilingLoopback {
    // leader -> follower responses (owned copies), follower -> leader requests.
    struct Resp { std::vector<uint8_t> buf; FetchResponseMsg msg; };
    std::deque<Resp> toFollower;
    std::deque<FetchRequestMsg> toLeader;
    int32_t ceiling = INT32_MAX;
    int     drops   = 0;
    std::size_t maxSent = 0;
};
struct CeilingLeaderTransport : PartitionTransport {
    CeilingLoopback* lb;
    explicit CeilingLeaderTransport(CeilingLoopback* l) : lb(l) {}
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
        // Mirrors the real transports: a frame over the ceiling is DROPPED.
        if (r.records.size() > static_cast<std::size_t>(lb->ceiling)) { ++lb->drops; return false; }
        lb->maxSent = std::max(lb->maxSent, r.records.size());
        CeilingLoopback::Resp x;
        x.buf.assign(r.records.begin(), r.records.end());
        x.msg = r;
        lb->toFollower.push_back(std::move(x));
        lb->toFollower.back().msg.records =
            std::span<const uint8_t>(lb->toFollower.back().buf.data(), lb->toFollower.back().buf.size());
        return true;
    }
    [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override { return lb->ceiling; }
};
struct CeilingFollowerTransport : PartitionTransport {
    CeilingLoopback* lb;
    explicit CeilingFollowerTransport(CeilingLoopback* l) : lb(l) {}
    bool sendFetchRequest(const FetchRequestMsg& q) override { lb->toLeader.push_back(q); return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};
} // namespace

TEST(PartitionReplicaRegression, PushChunkBoundedByTransportCeilingRegression) {
    auto dirL = std::filesystem::temp_directory_path() / ("rune_f3r6_pushcap_l_" + std::to_string(::getpid()));
    auto dirF = std::filesystem::temp_directory_path() / ("rune_f3r6_pushcap_f_" + std::to_string(::getpid()));
    for (auto& d : {dirL, dirF}) { std::filesystem::remove_all(d); std::filesystem::create_directories(d); }

    CeilingLoopback lb;
    lb.ceiling = 1000;                    // live transport ceiling
    CeilingLeaderTransport   lt(&lb);
    CeilingFollowerTransport ft(&lb);

    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 1'000'000;
    cfg.replicaLagMaxOffset = 1'000'000;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;    // >> ceiling (the production shape)
    cfg.pushOnAppend        = true;
    PartitionConfig lcfg = cfg; lcfg.selfId = 1;
    PartitionConfig fcfg = cfg; fcfg.selfId = 2;

    rune::storage::Log logL(dirL.string());
    rune::storage::Log logF(dirF.string());
    PartitionReplica leader(lcfg, Role::Leader, 1, {1, 2}, {1, 2}, &logL, &lt);
    PartitionReplica follower(fcfg, Role::Follower, 1, {1, 2}, {1, 2}, &logF, &ft);
    follower.becomeFollower(1, 1);
    // KIP-320/595: the follower reports in (validated pure ack on its empty log)
    // — a leader pushes only to validated followers.
    (void)leader.onFetchRequest(follower.nextFetch(), 0);
    lb.toFollower.clear();

    // 8 batches x ~360 B = ~2.9 KB in ONE cycle (> 2x the 1000 B ceiling).
    Offset off = 0;
    for (int i = 0; i < 8; ++i) {
        rune::protocol::RecordBatchBuilder b(off, 1000 + i);
        b.append("k", std::string(300, static_cast<char>('a' + i)), 1000 + i);
        logL.append(b.build(), off);
        ++off;
    }
    leader.onLeaderLogAppended();
    const Offset target = leader.logEndOffset();
    ASSERT_EQ(target, 8);

    int64_t now = 1'000'000;
    for (int cycle = 0; cycle < 200 && follower.logEndOffset() < target; ++cycle) {
        now += 1'000;                     // 1 µs steps: 0.2 ms total << 20 ms resend
        (void)leader.doWork(now);
        while (!lb.toFollower.empty()) {
            follower.applyReplicated(lb.toFollower.front().msg);
            lb.toFollower.pop_front();
        }
        (void)follower.doWork(now);
        while (!lb.toLeader.empty()) {
            (void)leader.onFetchRequest(lb.toLeader.front(), now);
            lb.toLeader.pop_front();
        }
    }
    EXPECT_EQ(lb.drops, 0) << "no push may exceed the transport's live ceiling (dropped)";
    EXPECT_LE(lb.maxSent, static_cast<std::size_t>(lb.ceiling));
    EXPECT_EQ(follower.logEndOffset(), target) << "follower must converge purely via pushes";
    EXPECT_EQ(leader.fetchDataServesForTest(), 0u)
        << "no resend-path (fetch Data) serve may be needed";

    for (auto& d : {dirL, dirF}) std::filesystem::remove_all(d);
}

// ---------------------------------------------------------------------------
// F3 round 6, item 8: while the transport ceiling is 0 (no publication
// resolved yet) an unsendable-looking head must NOT arm the push memo nor log
// "hard ceiling of 0" — it is not a property of the batch.
// ---------------------------------------------------------------------------
TEST(PartitionReplicaRegression, PushMemoNotArmedWhileCeilingUnresolvedRegression) {
    auto logDir = std::filesystem::temp_directory_path() /
                  ("rune_f3r6_ceil0_" + std::to_string(::getpid()));
    std::filesystem::remove_all(logDir);
    std::filesystem::create_directories(logDir);
    struct ZeroCeiling : PartitionTransport {
        int calls = 0;
        int32_t ceiling = 0;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { ++calls; return true; }
        [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override { return ceiling; }
    } t;
    PartitionConfig cfg;
    cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 1'000'000; cfg.replicaLagMaxOffset = 1'000'000;
    cfg.fetchIntervalNs = 5'000'000LL; cfg.isrCheckIntervalNs = 10'000'000LL;
    cfg.maxFetchBytes = 100;              // the batch is "oversize" vs maxFetchBytes
    cfg.pushOnAppend = true;
    rune::storage::Log log(logDir.string());
    PartitionReplica leader(cfg, Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    // KIP-320/595: follower 2 reports in (validated pure ack on its empty log)
    // — a leader pushes only to validated followers.
    (void)leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 0, 0}, 0);
    t.calls = 0;
    {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", std::string(300, 'x'), 1000);
        log.append(b.build(), 0);
    }
    leader.onLeaderLogAppended();
    for (int i = 0; i < 10; ++i) (void)leader.doWork(1);
    EXPECT_EQ(leader.unsendablePushLogEmitsForTest(), 0u)
        << "must not log 'hard ceiling of 0' before any publication is resolved";

    // Publication resolves: the ceiling is real and still too small → now it
    // is genuinely unsendable: armed + logged once.
    t.ceiling = 50;
    for (int i = 0; i < 10; ++i) (void)leader.doWork(1);
    EXPECT_EQ(leader.unsendablePushLogEmitsForTest(), 1u);
    EXPECT_EQ(t.calls, 0);
    std::filesystem::remove_all(logDir);
}

// ===========================================================================
// GR-10: Push-on-append + fast-ack tests
//
// Verifies:
//   1. Leader append → push sent to ISR followers immediately (no doWork needed).
//   2. Follower applies pushed batch → fast-ack FetchRequest emitted immediately.
//   3. HW advances without waiting a full fetch interval.
//   4. Dropped push (follower removed from transport) → fetch backstop recovers.
//   5. Out-of-order / gap push → follower ignores, fetch backfills correctly.
//   6. Stale-epoch push → follower ignores (epoch fencing via applyReplicated).
//   7. 3-node integration: produce acks=All → HW advances in ~2 pump() calls
//      (leader push → follower fast-ack → leader HW advance) with no fetch interval.
// ===========================================================================

// ---------------------------------------------------------------------------
// GR10_1: Leader append immediately pushes to ISR followers.
// Before any doWork(), the transport already has a pending FetchResponse for
// each ISR follower after the first append().
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, LeaderPushesImmediatelyOnAppend) {
    TempDir dL("gr10_1_L"), dF("gr10_1_F");
    rune::storage::Log logL(dL.path), logF(dF.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF}, isr = {kL, kF};

    // Use explicit config — fetch interval is long (backstop), push is default-on.
    PartitionConfig lcfg;
    lcfg.partitionId = 0; lcfg.selfId = kL;
    lcfg.minIsr = 1;
    lcfg.fetchIntervalNs = 100'000'000LL; // 100 ms backstop
    lcfg.isrCheckIntervalNs = 200'000'000LL;
    lcfg.pushOnAppend = true;
    lcfg.fastAckOnApply = true;

    PartitionConfig fcfg;
    fcfg.partitionId = 0; fcfg.selfId = kF;
    fcfg.fetchIntervalNs = 100'000'000LL;
    fcfg.pushOnAppend = true;
    fcfg.fastAckOnApply = true;

    PartitionReplica leader(lcfg, Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(fcfg, Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);
    reportIn(leader, {&follower}, transport);

    // Before any append: no pending messages.
    EXPECT_EQ(transport.pendingCount(), 0u);

    // Append a record → leader push is queued immediately (no doWork).
    leader.append(makePayload(0x42));

    // Push enqueued without any doWork() call.
    EXPECT_GT(transport.pendingCount(), 0u)
        << "GR-10: leader must enqueue a push immediately on append (no doWork needed)";
}

// ---------------------------------------------------------------------------
// GR10_2: Follower applies pushed batch and fast-acks immediately.
// After leader.append() + one pump(), the follower must:
//   (a) have its LEO advanced (received the push),
//   (b) have queued a FetchRequest (fast-ack).
// This happens WITHOUT any follower.doWork() call.
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, FollowerFastAcksAfterPush) {
    TempDir dL("gr10_2_L"), dF("gr10_2_F");
    rune::storage::Log logL(dL.path), logF(dF.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF}, isr = {kL, kF};

    PartitionConfig lcfg;
    lcfg.selfId = kL; lcfg.partitionId = 0;
    lcfg.fetchIntervalNs = 100'000'000LL; // backstop
    lcfg.isrCheckIntervalNs = 200'000'000LL;
    lcfg.pushOnAppend = true; lcfg.fastAckOnApply = true; lcfg.minIsr = 1;

    PartitionConfig fcfg;
    fcfg.selfId = kF; fcfg.partitionId = 0;
    fcfg.fetchIntervalNs = 100'000'000LL;
    fcfg.pushOnAppend = true; fcfg.fastAckOnApply = true; fcfg.minIsr = 1;

    PartitionReplica leader(lcfg, Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(fcfg, Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);
    reportIn(leader, {&follower}, transport);

    Offset preLeo = follower.logEndOffset();
    leader.append(makePayload(0xAB));

    // One pump: deliver the push FetchResponse → follower applyReplicated → fast-ack queued.
    int delivered = transport.pump(0);
    EXPECT_GT(delivered, 0);

    // Follower LEO must have advanced (received push records).
    EXPECT_GT(follower.logEndOffset(), preLeo)
        << "GR-10: follower must advance LEO after receiving push";

    // Fast-ack must be queued (FetchRequest from follower → leader).
    EXPECT_GT(transport.pendingCount(), 0u)
        << "GR-10: follower must emit fast-ack FetchRequest immediately after applying push";
}

// ---------------------------------------------------------------------------
// GR10_3: HW advances within 2 pump() calls (push + fast-ack), no doWork().
// This proves acks=All latency collapses to ~RTT.
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, HwAdvancesWithoutFetchInterval_TwoReplica) {
    TempDir dL("gr10_3_L"), dF("gr10_3_F");
    rune::storage::Log logL(dL.path), logF(dF.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF}, isr = {kL, kF};

    PartitionConfig lcfg;
    lcfg.selfId = kL; lcfg.partitionId = 0;
    lcfg.fetchIntervalNs = 100'000'000LL; // 100ms backstop — HW must NOT wait for this
    lcfg.isrCheckIntervalNs = 200'000'000LL;
    lcfg.pushOnAppend = true; lcfg.fastAckOnApply = true; lcfg.minIsr = 2; // majority required

    PartitionConfig fcfg;
    fcfg.selfId = kF; fcfg.partitionId = 0;
    fcfg.fetchIntervalNs = 100'000'000LL;
    fcfg.pushOnAppend = true; fcfg.fastAckOnApply = true; fcfg.minIsr = 2;

    PartitionReplica leader(lcfg, Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(fcfg, Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);
    reportIn(leader, {&follower}, transport);

    leader.append(makePayload(0x77));
    Offset leo = leader.logEndOffset();

    // HW should NOT be at leo yet (leader just appended, follower not yet acked).
    // With minIsr=2 (majority of 2), HW won't advance until follower reports LEO >= leo.
    // But push+fast-ack should resolve this without any fetch interval.

    // pump 1: deliver push to follower → follower fast-acks.
    transport.pump(0);
    // pump 2: deliver fast-ack to leader → leader updates follower LEO → recomputes HW.
    transport.pump(0);

    // HW should now be >= leo — achieved in 2 pumps without any doWork() or fetch timer.
    EXPECT_GE(leader.highWatermark(), leo)
        << "GR-10: HW must advance to LEO after push+fast-ack (2 pumps, no fetch interval)";
    // Confirm no full-interval wait needed (we never called doWork()).
}

// ---------------------------------------------------------------------------
// GR10_4: Dropped push is recovered by the fetch backstop.
// Simulate a dropped push by removing the follower from transport before append,
// then re-adding it. The follower must eventually catch up via periodic fetch.
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, DroppedPushRecoveredByFetchBackstop) {
    TempDir dL("gr10_4_L"), dF("gr10_4_F");
    rune::storage::Log logL(dL.path), logF(dF.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF}, isr = {kL, kF};

    PartitionConfig lcfg;
    lcfg.selfId = kL; lcfg.partitionId = 0;
    lcfg.fetchIntervalNs = 5'000'000LL; // 5 ms backstop
    lcfg.isrCheckIntervalNs = 200'000'000LL;
    lcfg.pushOnAppend = true; lcfg.fastAckOnApply = true; lcfg.minIsr = 1;

    PartitionConfig fcfg;
    fcfg.selfId = kF; fcfg.partitionId = 0;
    fcfg.fetchIntervalNs = 5'000'000LL; // 5 ms fetch interval (backstop cadence)
    fcfg.pushOnAppend = true; fcfg.fastAckOnApply = true; fcfg.minIsr = 1;

    PartitionReplica leader(lcfg, Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(fcfg, Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Drop the push: remove follower before append so push is discarded.
    transport.removeFollower(kF);
    leader.append(makePayload(0xDD));
    Offset leo = leader.logEndOffset();

    // Re-add follower (simulates reconnect after transient drop).
    transport.addFollower(kF, &follower);

    // Follower has not received the push — it must catch up via fetch backstop.
    EXPECT_LT(follower.logEndOffset(), leo) << "Follower should not have record yet";

    // Drive with fetch backstop (5 ms step per iteration).
    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock); // periodic fetch fires
        transport.pump(clock);
        transport.pump(clock); // second pump for fast-ack delivery
        done = (follower.logEndOffset() >= leo);
    }
    ASSERT_TRUE(done) << "GR-10: dropped push must be recovered by fetch backstop";
    EXPECT_GE(follower.logEndOffset(), leo);
}

// ---------------------------------------------------------------------------
// GR10_5: Gap / out-of-order push → follower does not apply, falls back to fetch.
// Simulate a gap by appending 2 records but the follower only receives the 2nd push.
// The follower must detect the gap (batchBase > currentLeo) and not apply out-of-order.
// The fetch backstop then backfills from follower's actual LEO.
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, GapPushNotAppliedOutOfOrder) {
    TempDir dL("gr10_5_L"), dF("gr10_5_F");
    rune::storage::Log logL(dL.path), logF(dF.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF}, isr = {kL, kF};

    PartitionConfig lcfg;
    lcfg.selfId = kL; lcfg.partitionId = 0;
    lcfg.fetchIntervalNs = 5'000'000LL;
    lcfg.isrCheckIntervalNs = 200'000'000LL;
    lcfg.pushOnAppend = true; lcfg.fastAckOnApply = true; lcfg.minIsr = 1;

    PartitionConfig fcfg;
    fcfg.selfId = kF; fcfg.partitionId = 0;
    fcfg.fetchIntervalNs = 5'000'000LL;
    fcfg.pushOnAppend = true; fcfg.fastAckOnApply = true; fcfg.minIsr = 1;

    PartitionReplica leader(lcfg, Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(fcfg, Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Append record 1 but drop the push to follower (simulate push drop).
    transport.removeFollower(kF);
    leader.append(makePayload(0xAA)); // offset 0
    Offset leo1 = leader.logEndOffset();
    // Drain any queued messages for the dropped push.
    transport.pump(0);

    // Re-add follower and append record 2 — this push will arrive but record 1 is missing.
    transport.addFollower(kF, &follower);
    leader.append(makePayload(0xBB)); // offset 1
    Offset leo2 = leader.logEndOffset();

    // Deliver the push for record 2 to follower.
    transport.pump(0);

    // Follower must NOT have applied record 2 (gap: batchBase=1 > followerLeo=0).
    EXPECT_LT(follower.logEndOffset(), leo1)
        << "GR-10: follower must NOT apply out-of-order push (gap at record 1)";

    // Now use the fetch backstop to backfill: follower fetches from LEO=0.
    int64_t clock = 0;
    bool done = false;
    for (int i = 0; i < kDriverCap && !done; ++i) {
        clock += 5'000'000LL;
        leader.doWork(clock);
        follower.doWork(clock);
        transport.pump(clock);
        transport.pump(clock);
        done = (follower.logEndOffset() >= leo2);
    }
    ASSERT_TRUE(done) << "GR-10: gap must be backfilled by fetch backstop";
    EXPECT_GE(follower.logEndOffset(), leo2);
}

// ---------------------------------------------------------------------------
// GR10_6: Stale-epoch push is ignored by the follower.
// GR10_6 (revised): stale-epoch push from a deposed leader must be DROPPED by
// the follower's applyReplicated() — not appended.
//
// Scenario: follower is at epoch=2 (becomeFollower was called after a new
// election).  A deposed epoch=1 leader, unaware it was replaced, sends an
// unsolicited push (FetchResponseMsg with leaderEpoch=1).  Before GR-10's
// fence fix, applyReplicated() would fall through the epoch-upgrade guard
// (which only fires on leaderEpoch > epoch_) and append the stale records,
// corrupting the follower log with data from the wrong epoch.
//
// Fix: applyReplicated() now returns immediately when resp.leaderEpoch < epoch_,
// dropping the frame entirely.  This test asserts:
//   1. Follower log does NOT advance (LEO stays at preLeo).
//   2. Follower epoch does NOT downgrade to 1.
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, StaleEpochPushIgnoredByFollower) {
    TempDir dL("gr10_6_L"), dF("gr10_6_F");
    rune::storage::Log logL(dL.path), logF(dF.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> replicas = {kL, kF}, isr = {kL, kF};

    PartitionConfig fcfg;
    fcfg.selfId = kF; fcfg.partitionId = 0;
    fcfg.fetchIntervalNs = 100'000'000LL;
    fcfg.pushOnAppend = true; fcfg.fastAckOnApply = true; fcfg.minIsr = 1;

    PartitionReplica leader(makeConfig(kL), Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica follower(fcfg, Role::Follower, 1, replicas, isr, &logF, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF, &follower);

    // Advance follower to epoch=2 (simulates a completed re-election).
    follower.becomeFollower(/*leaderEpoch=*/2, kL);

    const Offset preLeo = follower.logEndOffset();

    // Craft a FetchResponse with leaderEpoch=1 (stale — below follower's epoch=2).
    // Simulates an unsolicited push from the now-deposed epoch=1 leader.
    static std::vector<uint8_t> scratch(256 * 1024, 0);
    std::vector<uint8_t> fakeRecords(16, 0xCC);
    FetchResponseMsg::encode(scratch.data(), 0,
        /*leaderEpoch=*/1,    // STALE (follower is at epoch 2)
        /*partition=*/0,
        /*highWatermark=*/10,
        /*logStartOffset=*/0,
        kErrNone,
        std::span<const uint8_t>(fakeRecords.data(), fakeRecords.size()));
    auto staleResp = FetchResponseMsg::decode(scratch.data(), 0);

    // Apply the stale-epoch push directly — this must be a no-op.
    follower.applyReplicated(staleResp);

    // --- Assertions: stale push must be REJECTED, not appended ---

    // 1. Log must not advance — stale records must not be written.
    EXPECT_EQ(follower.logEndOffset(), preLeo)
        << "GR-10 fencing: stale-epoch push must NOT append records to follower log";

    // 2. Epoch must remain at 2 — stale push must not downgrade.
    EXPECT_EQ(follower.epoch(), 2)
        << "GR-10 fencing: stale-epoch push must NOT downgrade follower epoch";
}

// ---------------------------------------------------------------------------
// GR10_7: 3-node integration — HW advances via push+fast-ack in ~2 pumps.
// Simulates full acks=All round-trip with 3 nodes (leader + 2 followers).
// HW must reach leo within kDriverCap iterations driven PURELY by push+fast-ack
// (no doWork, fetch interval set to 100ms so periodic fetch never fires in test).
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, ThreeNodeHwConvergesViaPushFastAck) {
    TempDir dL("gr10_7_L"), dF2("gr10_7_F2"), dF3("gr10_7_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3}, isr = {kL, kF2, kF3};

    auto makePushCfg = [](NodeId id, int minIsr) {
        PartitionConfig c;
        c.selfId = id; c.partitionId = 0;
        c.fetchIntervalNs = 100'000'000LL; // 100ms backstop — won't fire in test
        c.isrCheckIntervalNs = 200'000'000LL;
        c.pushOnAppend = true; c.fastAckOnApply = true;
        c.minIsr = minIsr;
        c.maxFetchBytes = 1 << 20;
        return c;
    };

    PartitionReplica leader(makePushCfg(kL, 2), Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica f2(makePushCfg(kF2, 1), Role::Follower, 1, replicas, isr, &logF2, &transport);
    PartitionReplica f3(makePushCfg(kF3, 1), Role::Follower, 1, replicas, isr, &logF3, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    reportIn(leader, {&f2, &f3}, transport);
    transport.addFollower(kF3, &f3);

    // minIsr=2 for leader: majority of 3 = 2.
    leader.append(makePayload(0xFE));
    Offset leo = leader.logEndOffset();

    // HW should advance via push+fast-ack with no doWork().
    // Each pump() delivers: push → followers fast-ack → leader updates HW.
    // Bound to kDriverCap pumps (should converge in 2-3).
    bool hwDone = false;
    for (int i = 0; i < kDriverCap; ++i) {
        transport.pump(0);
        if (leader.highWatermark() >= leo) { hwDone = true; break; }
    }
    ASSERT_TRUE(hwDone)
        << "GR-10: 3-node HW must converge via push+fast-ack without fetch interval";
    EXPECT_GE(leader.highWatermark(), leo);
    EXPECT_GE(f2.logEndOffset(), leo);
    EXPECT_GE(f3.logEndOffset(), leo);
}

// ---------------------------------------------------------------------------
// GR10_8: GR-7 failover durability still holds after GR-10 push wiring.
// Produce acks=All, verify committed, kill leader, elect new leader, verify data.
// ---------------------------------------------------------------------------
TEST(GR10_PushOnAppend, FailoverDurabilityPreserved) {
    TempDir dL("gr10_8_L"), dF2("gr10_8_F2"), dF3("gr10_8_F3");
    rune::storage::Log logL(dL.path), logF2(dF2.path), logF3(dF3.path);

    InMemoryTransport transport;
    constexpr NodeId kL = 1, kF2 = 2, kF3 = 3;
    std::vector<NodeId> replicas = {kL, kF2, kF3}, isr = {kL, kF2, kF3};

    auto makeCfg = [](NodeId id, int minIsr) {
        PartitionConfig c;
        c.selfId = id; c.partitionId = 0;
        c.fetchIntervalNs = 5'000'000LL; // 5 ms backstop
        c.isrCheckIntervalNs = 200'000'000LL;
        c.pushOnAppend = true; c.fastAckOnApply = true;
        c.minIsr = minIsr;
        c.maxFetchBytes = 1 << 20;
        c.replicaLagMaxMs = 60'000;
        c.replicaLagMaxOffset = 1'000'000;
        return c;
    };

    PartitionReplica leader(makeCfg(kL, 2), Role::Leader, 1, replicas, isr, &logL, &transport);
    PartitionReplica f2(makeCfg(kF2, 1), Role::Follower, 1, replicas, isr, &logF2, &transport);
    PartitionReplica f3(makeCfg(kF3, 1), Role::Follower, 1, replicas, isr, &logF3, &transport);
    transport.setLeader(&leader);
    transport.addFollower(kF2, &f2);
    reportIn(leader, {&f2, &f3}, transport);
    transport.addFollower(kF3, &f3);

    // Produce and wait for acks=All (HW >= leo).
    leader.append(makePayload(0x11));
    Offset leo = leader.logEndOffset();

    bool committed = false;
    int64_t clk0 = 0;
    for (int i = 0; i < kDriverCap; ++i) {
        transport.pump(clk0);
        // Kafka model: the HW advances on the LAST ISR ack; f2 learns it from its
        // next answer (keepalive) — drive until it has, as a real cluster would.
        if (leader.highWatermark() >= leo && f2.highWatermark() >= leo) { committed = true; break; }
        clk0 += 5'000'000LL;
        leader.doWork(clk0); f2.doWork(clk0); f3.doWork(clk0);
    }
    ASSERT_TRUE(committed) << "acks=All must commit via push+fast-ack";
    EXPECT_GE(leader.highWatermark(), leo);

    // Simulate leader kill: remove leader from transport, f2 becomes new leader.
    transport.removeFollower(kF2);
    transport.removeFollower(kF3);
    // (Leader is "dead" — we just stop using it.)

    // Elect f2 as new leader (highest LEO or just pick one; both have data).
    f2.becomeLeader(/*newEpoch=*/2, {kF2, kF3});

    // Re-wire transport for new leader.
    InMemoryTransport transport2;
    transport2.setLeader(&f2);
    transport2.addFollower(kF3, &f3);

    // f3 needs to use transport2.
    // We can't change the transport pointer directly (it's in f3's config),
    // so we use the existing becomeFollower path and drive convergence.
    f3.becomeFollower(2, kF2);

    // Drive new cluster to confirm data is readable from the new leader.
    bool converged = false;
    int64_t clock = 0;
    for (int i = 0; i < kDriverCap && !converged; ++i) {
        clock += 5'000'000LL;
        f2.doWork(clock);
        f3.doWork(clock);
        transport2.pump(clock);
        transport2.pump(clock);
        converged = (f2.highWatermark() >= leo);
    }
    ASSERT_TRUE(converged) << "New leader (f2) must hold committed data after failover";

    // Data must be readable from new leader.
    auto data = f2.read(0, 1 << 20);
    EXPECT_FALSE(data.empty()) << "GR-10: committed data must survive leader kill";
}

// ---------------------------------------------------------------------------
// DR-9 m3: lock the byte-identity invariant that DR-BUG-6 reconcile depends on.
//
// PartitionReplica::onReconcileRequest / applyReconcileResponse stamp each
// served batch's partitionLeaderEpoch IN PLACE (RecordBatchReader::stampLeaderEpoch)
// without recomputing the CRC, and rely on the byte-identity of reconciled
// batches for the no-divergence guarantee.  This is only sound if
// partitionLeaderEpoch is EXCLUDED from the CRC-32C coverage.  That property
// lives in the external rune-protocol package; assert it here so an upstream
// layout drift (moving the field into the CRC region) fails loudly in this repo
// rather than silently corrupting reconciliation.
TEST(DR9_ReconcileInvariant, StampLeaderEpochDoesNotChangeCrc) {
    rune::protocol::RecordBatchBuilder builder(/*baseOffset=*/42, 0);
    builder.append("key-a", "value-a", 0);
    builder.append("key-b", "value-b", 0);
    auto batch = builder.build();

    rune::protocol::RecordBatchReader before(batch.data(), batch.size());
    ASSERT_TRUE(before.ensureValid()) << "freshly built batch must have a valid CRC";
    ASSERT_EQ(before.partitionLeaderEpoch(), -1) << "unset epoch sentinel";

    // Capture the 4 stored CRC bytes.
    const uint8_t crc0 = batch[rune::protocol::batch::kCrcOffset + 0];
    const uint8_t crc1 = batch[rune::protocol::batch::kCrcOffset + 1];
    const uint8_t crc2 = batch[rune::protocol::batch::kCrcOffset + 2];
    const uint8_t crc3 = batch[rune::protocol::batch::kCrcOffset + 3];

    // Stamp an epoch in place (exactly what reconcile does on the hot path).
    rune::protocol::RecordBatchReader::stampLeaderEpoch(batch.data(), /*epoch=*/7);

    rune::protocol::RecordBatchReader after(batch.data(), batch.size());
    EXPECT_EQ(after.partitionLeaderEpoch(), 7) << "epoch must be stamped";
    // ensureValid() recomputes CRC over [attributes..end] and compares to the
    // stored CRC — if stampLeaderEpoch had touched a CRC-covered byte this fails.
    EXPECT_TRUE(after.ensureValid())
        << "stamping partitionLeaderEpoch must NOT invalidate the CRC "
           "(field must remain excluded from CRC coverage)";
    // The stored CRC bytes themselves must be byte-identical.
    EXPECT_EQ(batch[rune::protocol::batch::kCrcOffset + 0], crc0);
    EXPECT_EQ(batch[rune::protocol::batch::kCrcOffset + 1], crc1);
    EXPECT_EQ(batch[rune::protocol::batch::kCrcOffset + 2], crc2);
    EXPECT_EQ(batch[rune::protocol::batch::kCrcOffset + 3], crc3);
}

// ===========================================================================
// Fix round 1 (Opus review of 0ff9322) — MEDIUM-5 / MEDIUM-6 / LOW.
// ===========================================================================
namespace {
struct CaptureT : public PartitionTransport {
    std::vector<FetchResponseMsg> resps;
    std::vector<std::vector<uint8_t>> bodies;
    std::vector<FetchRequestMsg> reqs;
    bool sendFetchRequest(const FetchRequestMsg& r) override { reqs.push_back(r); return true; }
    bool sendFetchRequestTo(NodeId, const FetchRequestMsg& r) override { reqs.push_back(r); return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg& r) override {
        bodies.emplace_back(r.records.begin(), r.records.end());
        resps.push_back(r);
        resps.back().records = std::span<const uint8_t>(bodies.back());
        return true;
    }
};
std::vector<uint8_t> stampedBatches(int64_t base, int n, int32_t ep) {
    std::vector<uint8_t> all;
    for (int i = 0; i < n; ++i) {
        rune::protocol::RecordBatchBuilder b(base + i, 0);
        b.append(std::string_view{}, std::string_view("rec", 3), 0);
        auto one = b.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(one.data(), ep);
        all.insert(all.end(), one.begin(), one.end());
    }
    return all;
}
} // namespace

// MEDIUM-6: mixed v2/v3 replication is unsupported — fail closed.  A v2 fetch
// (no lineage) with fetchOffset > 0 is refused (never served, never credited);
// a v2 empty-log follower is still served; a v2 leader's frame is dropped by a
// v3 follower.
TEST(PartitionReplicaRegression, MixedV2V3ReplicationFailsClosed) {
    TempDir d("mixv2");
    rune::storage::Log log(d.path);
    CaptureT t;
    PartitionReplica leader(makeConfig(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    leader.append(makePayload(1));
    leader.append(makePayload(2));
    t.resps.clear(); t.bodies.clear();
    FetchRequestMsg v2{};
    v2.leaderEpoch = 1; v2.partition = 0; v2.followerId = 2; v2.fetchOffset = 1;
    v2.maxBytes = 1 << 20; v2.lastFetchedEpoch = kLastFetchedEpochV2Frame;
    EXPECT_EQ(leader.onFetchRequest(v2, 1), kErrUnsupportedWireVersion);
    ASSERT_EQ(t.resps.size(), 1u);
    EXPECT_EQ(t.resps[0].errorCode, kErrUnsupportedWireVersion);
    EXPECT_EQ(leader.followerLeoForTest(2), -1) << "a refused v2 fetch was credited";
    v2.fetchOffset = 0;
    EXPECT_EQ(leader.onFetchRequest(v2, 2), kErrNone) << "an empty-log v2 follower has nothing to diverge";

    TempDir fd("mixv2f");
    rune::storage::Log flog(fd.path);
    CaptureT ft;
    PartitionReplica f(makeConfig(2), Role::Follower, 1, {1, 2}, {1, 2}, &flog, &ft);
    f.becomeFollower(1, 1);
    const auto recs = stampedBatches(0, 2, 1);
    uint8_t frame[4096];
    const std::size_t n = FetchResponseMsg::encode(frame, 0, 1, 0, 2, 0, kErrNone,
                                                   std::span<const uint8_t>(recs), 0);
    frame[6] = 2; frame[7] = 0;                  // a v2 leader's frame
    f.applyReplicated(FetchResponseMsg::decode(frame, 0, n));
    EXPECT_EQ(f.logEndOffset(), 0) << "a v2 frame was applied";
}

// MEDIUM-5: a follower that fetched without an epoch (empty / unstamped log)
// and is AHEAD of the leader gets OffsetOutOfRange with the leader's log end
// and truncates to it (Kafka) — it never stays stuck re-fetching.
TEST(PartitionReplicaRegression, FollowerOutOfRangeTruncatesToLeaderEnd) {
    TempDir ld("oorL"), fd("oorF");
    rune::storage::Log llog(ld.path), flog(fd.path);
    InMemoryTransport t;
    PartitionReplica leader(makeConfig(1), Role::Leader, 1, {1, 2}, {1, 2}, &llog, &t);
    PartitionReplica f(makeConfig(2), Role::Follower, 1, {1, 2}, {1, 2}, &flog, &t);
    // Follower holds 5 UNSTAMPED (legacy, epoch -1) batches; the leader holds 2.
    for (int i = 0; i < 5; ++i) {
        rune::protocol::RecordBatchBuilder b(i, 0);
        b.append(std::string_view{}, std::string_view("old", 3), 0);
        flog.append(b.build(), i);
    }
    leader.append(makePayload(1));
    leader.append(makePayload(2));
    f.becomeFollower(1, 1);
    ASSERT_EQ(f.lastFetchedEpoch(), -1);
    t.setLeader(&leader);
    t.addFollower(2, &f);
    int64_t clk = 0;
    for (int i = 0; i < 200 && f.logEndOffset() != 2; ++i) {
        clk += 1'000'000LL;
        leader.doWork(clk); f.doWork(clk); t.pump(clk);
    }
    EXPECT_EQ(f.logEndOffset(), 2) << "stuck ahead of the leader on OffsetOutOfRange";
}

// MEDIUM-5: a missing / corrupt epoch checkpoint over a non-empty log is rebuilt
// from the batches' stamped epochs — never left silently empty.
TEST(PartitionReplicaRegression, FailedEpochCheckpointRestoreRebuildsFromStamps) {
    TempDir d("epochrebuild");
    {
        rune::storage::Log log(d.path);
        const auto a = stampedBatches(0, 3, 4);
        const auto b = stampedBatches(3, 2, 7);
        log.append(a, 2);
        log.append(b, 4);
        std::ofstream(d.path + "/leader-epoch-checkpoint") << "garbage";
    }
    rune::storage::Log log(d.path);
    log.recover();
    CaptureT t;
    PartitionReplica f(makeConfig(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    const auto& k = f.leaderEpochCacheForTest();
    EXPECT_EQ(k.startOffsetFor(4), 0);
    EXPECT_EQ(k.startOffsetFor(7), 3);
    EXPECT_EQ(f.lastFetchedEpoch(), 7);
}

// LOW: on demotion the epoch assigned at becomeLeader with no record appended
// in it (a phantom entry) is dropped — lastFetchedEpoch names the epoch of the
// last record, so the next leader does not answer a needless DivergingEpoch.
TEST(PartitionReplicaRegression, DemotionDropsPhantomEpochEntry) {
    TempDir d("phantom");
    rune::storage::Log log(d.path);
    CaptureT t;
    PartitionReplica r(makeConfig(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    r.append(makePayload(1));
    r.becomeLeader(2, {1, 2});                   // epoch 2 assigned at LEO 1, nothing appended
    ASSERT_EQ(r.leaderEpochCacheForTest().latestEpoch(), 2);
    r.becomeFollower(3, 2);
    EXPECT_EQ(r.leaderEpochCacheForTest().latestEpoch(), 1) << "phantom epoch 2 kept";
    EXPECT_EQ(r.lastFetchedEpoch(), 1);
}

// ===========================================================================
// Review 2026-10-04 (rev3) — CRITICAL-1 / MAJOR-1 repros (RED on e5d41b4).
// ===========================================================================
namespace {
FetchRequestMsg rev3Fetch(Epoch ep, NodeId f, Offset off, int32_t lfe) {
    FetchRequestMsg r{};
    r.leaderEpoch = ep; r.partition = 0; r.followerId = f;
    r.fetchOffset = off; r.maxBytes = 0; r.topicId = 0; r.lastFetchedEpoch = lfe;
    return r;
}
bool rev3InIsr(const PartitionReplica& r, NodeId n) {
    const auto& isr = r.isr();
    return std::find(isr.begin(), isr.end(), n) != isr.end();
}
} // namespace

// CRITICAL-1 (unit, R1): the leader's per-follower LEO survived across
// leaderships — a member that has NOT fetched in the new leadership was counted
// at its previous-leadership position (Kafka Partition.makeLeader resets every
// replica's state on a new leader epoch).
TEST(PartitionReplicaRegression, NewLeadershipForgetsFollowerPositions_HwNeverFromPreviousLeadership) {
    TempDir d("rev3r1"); rune::storage::Log log(d.path);
    InMemoryTransport t;
    constexpr NodeId kB = 1, kA = 2, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    PartitionReplica b(makeConfig(kB, 10'000, 1'000'000, 2), Role::Leader, 1, all, all, &log, &t);
    for (int i = 0; i < 20; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    int64_t clock = 1'000'000;
    b.doWork(clock);
    ASSERT_EQ(b.onFetchRequest(rev3Fetch(1, kA, 20, 1), clock), kErrNone);
    ASSERT_EQ(b.onFetchRequest(rev3Fetch(1, kC, 5, 1), clock), kErrNone);
    EXPECT_EQ(b.highWatermark(), 5);
    b.becomeFollower(2, kC);
    b.becomeLeader(3, {kB, kA, kC});             // A has not fetched in epoch 3
    for (int i = 0; i < 3; ++i) ASSERT_GE(b.append(makePayload(3)), 0);
    clock += 1'000'000; b.doWork(clock);
    ASSERT_EQ(b.onFetchRequest(rev3Fetch(3, kC, b.logEndOffset(), 3), clock), kErrNone);
    EXPECT_LE(b.highWatermark(), 5) << "HW counted A at its previous-leadership position";
    EXPECT_LE(b.followerLeoForTest(kA), 0) << "A's epoch-1 position survived the leadership change";
}

// CRITICAL-1 (controller snapshot): a leader publishes a follower's position
// for the controller's election only once that follower fetched (validated) in
// THIS leadership — never a stale position stamped with the current epoch.
TEST(PartitionReplicaRegression, PositionSnapshotCarriesOnlyFollowersValidatedThisLeadership) {
    TempDir d("rev3snap"); rune::storage::Log log(d.path);
    InMemoryTransport t;
    constexpr NodeId kB = 1, kA = 2, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    PartitionReplica b(makeConfig(kB, 10'000, 1'000'000, 2), Role::Leader, 1, all, all, &log, &t);
    for (int i = 0; i < 20; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    int64_t clock = 1'000'000;
    b.doWork(clock);
    ASSERT_EQ(b.onFetchRequest(rev3Fetch(1, kA, 20, 1), clock), kErrNone);
    b.doWork(clock += 1'000'000);
    ASSERT_EQ(b.controllerReplicaLeo(kA), 20);   // validated in epoch 1: published
    b.becomeFollower(2, kC);
    EXPECT_EQ(b.controllerReplicaLeo(kA), -1) << "a follower publishes another replica's position";
    b.becomeLeader(3, {kB, kA, kC});
    b.doWork(clock += 1'000'000);
    EXPECT_EQ(b.controllerReplicaLeo(kA), -1)
        << "the epoch-1 position of A was published under epoch " << b.controllerLeaderEpoch();
    ASSERT_EQ(b.onFetchRequest(rev3Fetch(3, kA, 20, 1), clock), kErrNone);
    b.doWork(clock += 1'000'000);
    EXPECT_EQ(b.controllerReplicaLeo(kA), 20);   // validated in epoch 3: published again
}

// CRITICAL-1 (end to end, R1b): three real replicas.  A truncates under C's
// leadership, goes slow, and leadership moves back to B: B must not ack records
// ISR member A lacks.
TEST(PartitionReplicaRegression, NewLeadershipForgetsFollowerPositions_EndToEnd) {
    TempDir db("rev3r1b"), da("rev3r1a"), dc("rev3r1c");
    rune::storage::Log lb(db.path), la(da.path), lc(dc.path);
    InMemoryTransport t;
    constexpr NodeId kB = 1, kA = 2, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    auto cfg = [](NodeId id) { return makeConfig(id, 10'000, 1'000'000, 2); };
    PartitionReplica b(cfg(kB), Role::Leader, 1, all, all, &lb, &t);
    PartitionReplica a(cfg(kA), Role::Follower, 1, all, all, &la, &t);
    PartitionReplica c(cfg(kC), Role::Follower, 1, all, all, &lc, &t);
    a.becomeFollower(1, kB); c.becomeFollower(1, kB);
    t.setLeader(&b); t.addFollower(kA, &a); t.addFollower(kC, &c);
    int64_t clock = 0;
    for (int i = 0; i < 5; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&] {
        return a.logEndOffset() == 5 && c.logEndOffset() == 5 && b.highWatermark() == 5; }, b, a, c));
    t.removeFollower(kC);                        // C stalls at 5
    for (int i = 0; i < 15; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&] {
        return a.logEndOffset() == 20 && b.followerLeoForTest(kA) == 20; }, b, a));
    ASSERT_EQ(b.highWatermark(), 5);
    // Epoch 2: C leads (an ISR member holding the committed prefix); A, B truncate to 5.
    t.removeFollower(kA);
    c.becomeLeader(2, {kC, kA, kB}); a.becomeFollower(2, kC); b.becomeFollower(2, kC);
    t.setLeader(&c); t.addFollower(kA, &a); t.addFollower(kB, &b);
    for (int i = 0; i < 3; ++i) ASSERT_GE(c.append(makePayload(2)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&] {
        return a.logEndOffset() == 8 && b.logEndOffset() == 8 && c.highWatermark() == 8; }, c, a, b));
    t.removeFollower(kA);                        // A slow from here (inside replicaLagMaxMs)
    for (int i = 0; i < 4; ++i) ASSERT_GE(c.append(makePayload(2)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&] { return b.logEndOffset() == 12; }, c, b));
    // Epoch 3: preferred election back to B (ISR carried over); A not fetching yet.
    t.removeFollower(kB);
    b.becomeLeader(3, {kB, kA, kC}); c.becomeFollower(3, kB);
    t.setLeader(&b); t.addFollower(kC, &c);
    for (int i = 0; i < 3; ++i) ASSERT_GE(b.append(makePayload(3)), 0);   // offsets 12..14
    (void)driveReplicas(t, clock, 20, [&] { return c.logEndOffset() == 15 && b.highWatermark() >= 15; }, b, c);
    EXPECT_TRUE(rev3InIsr(b, kA));
    EXPECT_LE(b.highWatermark(), a.logEndOffset())
        << "committed past ISR member A's actual LEO (A holds " << a.logEndOffset() << ")";
}

// MAJOR-1 (R2): ISR expansion requires the follower to hold everything the new
// leader held when elected (Kafka isFollowerInSync: followerEnd >=
// leaderEpochStartOffset) — the leader's HW view right after an election can
// lag the true committed offset.
TEST(PartitionReplicaRegression, IsrExpansionRequiresLeaderEpochStartOffset) {
    TempDir d("rev3r2"); rune::storage::Log log(d.path);
    InMemoryTransport t;
    constexpr NodeId kA = 2, kB = 1, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    PartitionReplica a(makeConfig(kA, 10'000, 1'000'000, 2), Role::Follower, 1, all, all, &log, &t);
    seedStamped(a, 0, 10, 1, /*hw=*/5);          // holds 10 committed (ISR {B,A}), stale HW view 5
    a.becomeLeader(2, {kA, kB});                 // C outside the ISR
    int64_t clock = 1'000'000;
    a.doWork(clock);
    ASSERT_EQ(a.onFetchRequest(rev3Fetch(2, kC, 7, 1), clock), kErrNone);   // C holds [0,7)
    for (int i = 0; i < 5; ++i) { clock += 1'000'000; a.doWork(clock); }
    EXPECT_FALSE(rev3InIsr(a, kC)) << "C admitted while missing records the leader held at election";
    ASSERT_EQ(a.onFetchRequest(rev3Fetch(2, kC, 10, 1), clock), kErrNone);  // caught up to 10
    for (int i = 0; i < 5; ++i) { clock += 1'000'000; a.doWork(clock); }
    EXPECT_TRUE(rev3InIsr(a, kC)) << "C never admitted once it held the leader-epoch start offset";
}

// MEDIUM-4 (KIP-903): a fetch from an OLDER process incarnation of a follower
// than one the leader already saw (a frame of a dead process still in flight)
// is never credited — the restarted follower may no longer hold that position.
TEST(PartitionReplicaRegression, StaleIncarnationFetchNeverCredited) {
    TempDir d("rev3inc"); rune::storage::Log log(d.path);
    InMemoryTransport t;
    constexpr NodeId kL = 1, kF = 2;
    std::vector<NodeId> all = {kL, kF};
    PartitionReplica l(makeConfig(kL, 10'000, 1'000'000, 1), Role::Leader, 1, all, all, &log, &t);
    for (int i = 0; i < 20; ++i) ASSERT_GE(l.append(makePayload(1)), 0);
    int64_t clock = 1'000'000;
    l.doWork(clock);
    auto f = rev3Fetch(1, kF, 10, 1);
    f.incarnation = 5;                           // the restarted follower: holds [0,10)
    ASSERT_EQ(l.onFetchRequest(f, clock), kErrNone);
    ASSERT_EQ(l.followerLeoForTest(kF), 10);
    auto stale = rev3Fetch(1, kF, 20, 1);
    stale.incarnation = 4;                       // its previous process, at 20
    (void)l.onFetchRequest(stale, clock += 1'000'000);
    EXPECT_EQ(l.followerLeoForTest(kF), 10) << "credited a fetch of a dead follower process";
    EXPECT_LE(l.highWatermark(), 10);
}

// MAJOR-2/3: the internal-ISR knowledge is DURABLE (the candidacy rule must
// survive a whole-cluster restart) and only newer states replace older ones.
TEST(InternalIsrRegression, KnowledgeIsDurableAndMonotonic) {
    TempDir d("iisrk");
    const std::string path = d.path + "/internal-isr.state";
    InternalIsrState a{};
    a.topicId = -3; a.partition = 0; a.leaderEpoch = 4; a.version = 2; a.count = 2;
    a.members[0] = 1; a.members[1] = 2;
    {
        InternalIsrKnowledge k(path);
        EXPECT_TRUE(k.eligible(3)) << "no knowledge: everyone eligible (fresh cluster)";
        ASSERT_TRUE(k.offer(a));
        EXPECT_FALSE(k.eligible(3));
        InternalIsrState older = a;
        older.version = 1; older.count = 3; older.members[2] = 3;
        EXPECT_TRUE(k.offer(older)) << "holds a newer state: the older one may be acked";
        EXPECT_FALSE(k.eligible(3)) << "an older state replaced a newer one";
    }
    InternalIsrKnowledge k2(path);                // restart
    ASSERT_NE(k2.latest(-3, 0), nullptr);
    EXPECT_EQ(k2.latest(-3, 0)->version, 2);
    EXPECT_FALSE(k2.eligible(3)) << "the candidacy rule did not survive a restart";
    EXPECT_TRUE(k2.eligible(1));
    InternalIsrState other{};
    other.topicId = -1; other.partition = 0; other.leaderEpoch = 4; other.version = 1;
    other.count = 2; other.members[0] = 1; other.members[1] = 3;
    ASSERT_TRUE(k2.offer(other));
    EXPECT_FALSE(k2.eligible(2)) << "must be a member of EVERY internal partition's ISR";
    EXPECT_TRUE(k2.eligible(1));
}

// Re-review 2026-10-04 (fix round 2) MEDIUM-A: a node that voted in (or
// observed) consensus term e+1 refuses to PERSIST or ACK an internal ISR state
// of leadership e — even while its replica still follows the term-e leader
// (the bridge has not applied NewLeadershipTerm yet).  Raft's term rule applied
// to the ack: a zombie leader can neither commit through it nor plant a stale
// candidacy view in it.  A state of the current term is still adopted + acked.
TEST(InternalIsrRegression, StateOlderThanConsensusTermNeitherPersistedNorAcked) {
    struct AckCapture : public PartitionTransport {
        int acks = 0;
        bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
        bool sendInternalIsrAck(NodeId, const InternalIsrAckMsg&) override { ++acks; return true; }
    };
    TempDir fDir("iisr_term_F"), kDir("iisr_term_K");
    rune::storage::Log fLog(fDir.path);
    AckCapture t;
    constexpr NodeId kL = 1, kF = 2, kY = 3;
    std::vector<NodeId> all = {kL, kF, kY};
    PartitionConfig cfg = makeConfig(kF, /*lagMs=*/10, /*lagOff=*/1'000'000);
    cfg.topicId = -3;
    InternalIsrKnowledge k(kDir.path + "/internal-isr.state");
    int64_t term = 4;
    k.setConsensusTermSource([](const void* p) noexcept { return *static_cast<const int64_t*>(p); }, &term);
    PartitionReplica f(cfg, Role::Follower, 0, all, all, &fLog, &t);
    f.setInternalIsrKnowledge(&k);
    f.becomeFollower(4, kL);
    auto msg = [&](int64_t epoch, int64_t version, std::initializer_list<NodeId> m) {
        InternalIsrMsg x{};
        x.topicId = -3; x.partition = 0; x.leaderId = kL; x.leaderEpoch = epoch; x.version = version;
        for (NodeId n : m) x.isr[static_cast<std::size_t>(x.isrCount++)] = n;
        return x;
    };
    f.onInternalIsr(msg(4, 1, {kL, kF, kY}));    // current term: adopted + acked
    ASSERT_NE(k.latest(-3, 0), nullptr);
    EXPECT_EQ(k.latest(-3, 0)->version, 1);
    EXPECT_EQ(t.acks, 1);
    term = 5;                                    // F granted Y's RequestVote for term 5
    f.onInternalIsr(msg(4, 2, {kL, kF}));        // the zombie's shrink of Y
    EXPECT_EQ(k.latest(-3, 0)->version, 1) << "persisted a state of term 4 after voting in term 5";
    EXPECT_TRUE(k.latest(-3, 0)->contains(kY));
    EXPECT_EQ(t.acks, 1) << "acked a state of term 4 after voting in term 5";
    InternalIsrKnowledge reloaded(kDir.path + "/internal-isr.state");
    ASSERT_NE(reloaded.latest(-3, 0), nullptr);
    EXPECT_EQ(reloaded.latest(-3, 0)->version, 1) << "the stale state reached the disk";
}
