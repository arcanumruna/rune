// ---------------------------------------------------------------------------
// CompactionGapCatchUpTest.cpp — #789 regression: a follower whose LEO falls
// inside a range the leader's cleaner compacted away must catch up.  The
// leader's read from that LEO returns the first surviving batch (base > LEO);
// the follower refused every such batch as a gap and never advanced.
//
// v5 FetchResponse.fromOffset: the leader states where it read from, so a
// follower whose LEO >= fromOffset knows the missing offsets are absent from
// the leader's log (Kafka: a follower appends across compaction gaps).  A
// batch above the LEO with no read origin at or below it (a push after
// dropped frames, a v4 sender) is still refused.
//
// Deterministic: injected clock, InMemoryTransport, bounded driver loop.
// ---------------------------------------------------------------------------
#include <gtest/gtest.h>
#include <rune/storage/testing/DemandChannelForTest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::replication;

namespace {

constexpr NodeId kLeaderId   = 1;
constexpr NodeId kFollowerId = 2;
constexpr int    kDriverCap  = 200;

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& tag) {
        static std::atomic<uint64_t> seq{0};
        path = (std::filesystem::temp_directory_path() /
                ("rune_789_" + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(seq.fetch_add(1)) + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())))
                   .string();
        std::filesystem::create_directories(path);
    }
    ~TempDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

PartitionConfig makeCfg(NodeId self) {
    PartitionConfig c;
    c.partitionId         = 0;
    c.selfId              = self;
    c.replicaLagMaxMs     = 1000;
    c.replicaLagMaxOffset = 100;
    c.minIsr              = 1;
    c.fetchIntervalNs     = 1'000'000LL;
    c.isrCheckIntervalNs  = 1'000'000LL;
    c.maxFetchBytes       = 1 << 20;
    c.pushOnAppend        = false;
    return c;
}

// One epoch-0 batch (written before this leadership) holding offsets [base, base + count).
std::vector<uint8_t> batch(int64_t base, int count) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    for (int i = 0; i < count; ++i) b.append("k", "v" + std::to_string(base + i), 0);
    std::vector<uint8_t> out;
    b.buildInto(out);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(out.data(), 0);
    return out;
}

void put(rune::storage::Log& log, const std::vector<uint8_t>& b, int64_t last) {
    ASSERT_GE(log.append(b.data(), b.size(), last), 0);
}

std::vector<uint8_t> concat(std::initializer_list<const std::vector<uint8_t>*> parts) {
    std::vector<uint8_t> out;
    for (const auto* p : parts) out.insert(out.end(), p->begin(), p->end());
    return out;
}

FetchResponseMsg dataFrame(const std::vector<uint8_t>& records, Offset fromOffset) {
    FetchResponseMsg r{};
    r.leaderEpoch = 1;
    r.partition   = 0;
    r.highWatermark = 0;
    r.records     = std::span<const uint8_t>(records.data(), records.size());
    r.fromOffset  = fromOffset;
    return r;
}

class CompactionGapCatchUpRegression : public ::testing::Test {
protected:
    TempDir            leaderDir{"leader"}, followerDir{"follower"};
    rune::storage::Log leaderLog{leaderDir.path}, followerLog{followerDir.path};
    InMemoryTransport  transport;
    std::unique_ptr<PartitionReplica> leader, follower;
    const std::vector<uint8_t> b0 = batch(0, 3);   // [0, 2]  held by both
    const std::vector<uint8_t> b3 = batch(3, 2);   // [3, 4]  compacted away on the leader
    const std::vector<uint8_t> b5 = batch(5, 2);   // [5, 6]
    const std::vector<uint8_t> b9 = batch(9, 1);   // [9]     (7, 8 compacted away too)

    std::unique_ptr<PartitionReplica> followerAtLeo3() {
        put(followerLog, b0, 2);
        const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
        return std::make_unique<PartitionReplica>(makeCfg(kFollowerId), Role::Follower, 1,
                                                  replicas, replicas, &followerLog, &transport);
    }
};

} // namespace

// End to end over the fetch loop: the leader holds [0,2] [5,6] [9]; the
// follower restarted at LEO 3 (inside the compacted range).  It must reach the
// leader's LEO with the leader's batches, and the leader's HW must commit.
TEST_F(CompactionGapCatchUpRegression, FollowerBehindACompactedRangeCatchesUpRegression) {
    put(leaderLog, b0, 2);
    put(leaderLog, b5, 6);
    put(leaderLog, b9, 9);
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    leader = std::make_unique<PartitionReplica>(makeCfg(kLeaderId), Role::Leader, 1, replicas,
                                                replicas, &leaderLog, &transport);
    transport.setLeader(leader.get());
    follower = followerAtLeo3();
    transport.addFollower(kFollowerId, follower.get());
    ASSERT_EQ(leader->logEndOffset(), 10);
    ASSERT_EQ(follower->logEndOffset(), 3);

    int64_t clock = 0;
    for (int i = 0; i < kDriverCap &&
                    !(follower->logEndOffset() == 10 && leader->highWatermark() == 10); ++i) {
        clock += 1'000'000LL;
        leader->doWork(clock);
        follower->doWork(clock);
        transport.pump(clock);
    }
    EXPECT_EQ(follower->logEndOffset(), 10) << "follower stuck at the compaction gap";
    EXPECT_EQ(leader->highWatermark(), 10) << "the gapped follower never acknowledged";

    const auto v = followerLog.readView(5, 1 << 20);
    const std::vector<uint8_t> want = concat({&b5, &b9});
    ASSERT_GE(v.len, want.size());
    EXPECT_EQ(std::vector<uint8_t>(v.data, v.data + want.size()), want)
        << "the follower must hold the leader's surviving batches verbatim";
}

// The read origin decides: a gap is crossed only when the leader read from at
// or below our LEO.  Every refused frame leaves the log untouched.
TEST_F(CompactionGapCatchUpRegression, GapAboveTheReadOriginIsStillRefusedRegression) {
    follower = followerAtLeo3();
    follower->becomeFollower(1, kLeaderId);
    const std::vector<uint8_t> tail = concat({&b5, &b9});

    follower->applyReplicated(dataFrame(tail, /*v4 sender*/ -1));
    EXPECT_EQ(follower->logEndOffset(), 3) << "no read origin: a gap may be a lost frame";

    follower->applyReplicated(dataFrame(tail, /*push after dropped frames*/ 5));
    EXPECT_EQ(follower->logEndOffset(), 3) << "read above our LEO: [3, 5) may exist on the leader";

    follower->applyReplicated(dataFrame(tail, 3));
    EXPECT_EQ(follower->logEndOffset(), 10) << "read from our LEO: [3, 5) and [7, 9) are compacted";
}

// A held batch that straddles our LEO covers offsets we do not hold: the
// response says nothing about them being absent, so the gap after it stays refused.
TEST_F(CompactionGapCatchUpRegression, BatchStraddlingTheLeoBlocksTheGapRegression) {
    follower = followerAtLeo3();
    follower->becomeFollower(1, kLeaderId);
    const std::vector<uint8_t> wide = batch(1, 4);   // [1, 4]: our LEO 3 sits inside it
    const std::vector<uint8_t> frame = concat({&wide, &b9});
    follower->applyReplicated(dataFrame(frame, 1));
    EXPECT_EQ(follower->logEndOffset(), 3);
}

// The cleaner removed epoch 1's first batch: the first surviving one is not the
// epoch's start, so it is never compared (R16) - a leader that assigned the
// start live would read a different history and the follower would refuse.
TEST_F(CompactionGapCatchUpRegression, EpochStartAcrossAGapIsNeverComparedRegression) {
    follower = followerAtLeo3();
    follower->becomeFollower(1, kLeaderId);
    std::vector<uint8_t> e1 = batch(5, 2);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(e1.data(), 1);
    follower->applyReplicated(dataFrame(e1, 3));
    ASSERT_EQ(follower->logEndOffset(), 7);
    EXPECT_EQ(follower->heldEpochStartForTest(1), -1) << "5 is where epoch 1 survives, not where it starts";
}

// Contiguous frames apply exactly as before, with or without a read origin.
TEST_F(CompactionGapCatchUpRegression, ContiguousApplyUnchangedRegression) {
    follower = followerAtLeo3();
    follower->becomeFollower(1, kLeaderId);
    const std::vector<uint8_t> next = concat({&b3, &b5});
    follower->applyReplicated(dataFrame(next, -1));
    EXPECT_EQ(follower->logEndOffset(), 7) << "a contiguous v4 frame applies as before";
}

// A transient (memory) follower never crosses a gap: its ring is contiguous.
TEST(CompactionGapCatchUp, TransientFollowerRefusesAGapRegression) {
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    InMemoryTransport t;
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    PartitionReplica r(makeCfg(kFollowerId), Role::Follower, 1, replicas, replicas, &log, &t);
    r.becomeFollower(1, kLeaderId);
    const std::vector<uint8_t> b0 = batch(0, 3), b5 = batch(5, 2);
    r.applyReplicated(dataFrame(b0, 0));
    ASSERT_EQ(r.logEndOffset(), 3);
    r.applyReplicated(dataFrame(b5, 3));
    EXPECT_EQ(r.logEndOffset(), 3);
}
