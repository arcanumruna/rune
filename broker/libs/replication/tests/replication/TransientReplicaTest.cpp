// TransientReplicaTest.cpp — Phase 2b E2: replication of a TRANSIENT
// (InMemoryLog) partition.  The leader's ring never evicts at/above its high
// watermark (refuses instead), and a follower whose fetch position is below the
// leader's ring start (a restarted node rejoining empty) restarts its ring there
// and catches up — before the fix it fetched an evicted offset forever.
#include <gtest/gtest.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <cstdint>
#include <span>
#include <vector>

using namespace rune::replication;

namespace {
PartitionConfig tcfg(NodeId self) {
    PartitionConfig c;
    c.topicId = 7; c.partitionId = 0; c.selfId = self; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000'000'000LL;
    c.replicaLagMaxMs = 600'000; c.replicaLagMaxOffset = 1'000'000'000;
    c.pushOnAppend = false; c.maxFetchBytes = 1 << 20;
    return c;
}
constexpr int kCap = 20'000;   // hard cap on every drive loop
} // namespace

TEST(TransientReplicaRegression, LeaderRingNeverEvictsAboveTheHighWatermarkRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    Offset last = 0;
    int appended = 0;
    for (int i = 0; i < 64; ++i) {   // follower 2 (in the ISR) never fetched: HW stays 0
        last = r1.append(std::span<const uint8_t>(payload), /*acksAll=*/false);
        if (last < 0) break;
        ++appended;
    }
    ASSERT_EQ(last, PartitionReplica::kAppendBackPressure) << "a full ring must refuse, not evict unreplicated data";
    EXPECT_GT(appended, 0);
    EXPECT_EQ(mem1.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(mem1.fullRefusals(), 1u);

    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r1.highWatermark() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    ASSERT_EQ(r1.highWatermark(), r1.logEndOffset()) << "follower never caught up";
    EXPECT_GE(r1.append(std::span<const uint8_t>(payload), false), 0) << "committed records make room";
    EXPECT_GT(mem1.logStartOffset(), 0);
}

TEST(TransientReplicaRegression, FollowerBehindTheLeaderRingStartResetsAndCatchesUpRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    const std::vector<NodeId> leaderOnly = {1};
    // Follower 2 is OUT of the ISR (a restarted node rejoining empty): the HW
    // follows the leader alone, so the leader's ring evicts committed records.
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, leaderOnly, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all,        &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 40; ++i) ASSERT_GE(r1.append(std::span<const uint8_t>(payload), false), 0);
    const Offset leaderStart = mem1.logStartOffset();
    ASSERT_GT(leaderStart, 0) << "precondition: the leader's ring evicted its oldest records";
    const uint64_t genBefore = r2.logGeneration();

    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r2.logEndOffset() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    EXPECT_EQ(r2.logEndOffset(), r1.logEndOffset())
        << "stalled: a follower below the leader's ring start is never served from its LEO";
    EXPECT_EQ(r2.transientLogResetsForTest(), 1u);
    EXPECT_EQ(mem2.logStartOffset(), leaderStart);
    // The reset changed the ring's existing content: one generation bump, cut at
    // the OLD ring start (everything held was dropped) — RC state / waiters /
    // views over this source re-validate (Task 9 consumes this).
    EXPECT_EQ(r2.logGeneration(), genBefore + 1);
    EXPECT_EQ(r2.lastTruncationOffset(), 0);
}

TEST(TransientReplicaRegression, LeaderAppendStampsItsEpochIntoTheRing) {
    rune::storage::InMemoryLog mem(1 << 20, 1000);
    InMemoryTransport transport;
    PartitionReplica r1(tcfg(1), Role::Leader, 4, {1}, {1}, &mem, &transport);
    EXPECT_EQ(mem.leaderEpochStamp(), 4) << "a transient leader stamps restamp-appends with its epoch";
    r1.becomeFollower(5, 2);
    EXPECT_EQ(mem.leaderEpochStamp(), -1) << "a follower keeps the producer's bytes";
}
