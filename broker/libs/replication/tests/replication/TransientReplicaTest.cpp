// TransientReplicaTest.cpp — Phase 2b E2: replication of a TRANSIENT
// (InMemoryLog) partition.  The leader's ring never evicts at/above its high
// watermark (refuses instead), and a follower whose fetch position is below the
// leader's ring start (a restarted node rejoining empty) restarts its ring there
// and catches up — before the fix it fetched an evicted offset forever.
#include <gtest/gtest.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <span>
#include <string>
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
struct CountingTransport final : InMemoryTransport {
    int fetches = 0;
    bool sendFetchRequest(const FetchRequestMsg& r) override { ++fetches; return InMemoryTransport::sendFetchRequest(r); }
};
std::vector<uint8_t> stampedBatch(int64_t base, std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", std::string(valueBytes, 'v'), 1000);
    auto x = b.build();
    rune::protocol::RecordBatchReader::stampLeaderEpoch(x.data(), 1);
    return x;
}
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
    // ISR v3 Step 11 (v2-review C2): a start equal to the reset ring start is a log start, not a first record - never sent.
    EXPECT_EQ(r2.nextFetch().lastEpochStartOffset, -1) << "an epoch start equal to the reset ring start must not be sent";
    for (int i = 0; i < 10; ++i) ASSERT_GE(r1.append(std::span<const uint8_t>(payload), false), 0);
    for (int i = 0; i < kCap && r2.logEndOffset() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    EXPECT_EQ(r2.logEndOffset(), r1.logEndOffset());
    EXPECT_EQ(r1.epochHistoryMismatchesForTest(), 0u);
}

// ISR v3 Step 11 (A33): the same scenario under its guard name.  A transient follower reset to the leader's ring start
// reports an epoch start the leader no longer holds (`s > logStart` fails on the follower; the leader's own start is not
// the epoch's first record either): no comparison, never a mismatch, and the follower catches up and keeps following.
// RED: send `epochCache_.startOffsetFor(...)` unconditionally (the follower stalls at the ring start, mismatches > 0).
TEST(TruncationGuardRegression, TransientFollowerResetToTheRingStartIsNeverAMismatchRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    const std::vector<NodeId> leaderOnly = {1};
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, leaderOnly, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all,        &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 40; ++i) ASSERT_GE(r1.append(std::span<const uint8_t>(payload), false), 0);
    ASSERT_GT(mem1.logStartOffset(), 0) << "precondition: the leader's ring evicted its oldest records";
    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r2.logEndOffset() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    ASSERT_EQ(r2.logEndOffset(), r1.logEndOffset());
    ASSERT_EQ(r2.transientLogResetsForTest(), 1u);
    EXPECT_EQ(r2.nextFetch().lastEpochStartOffset, -1);
    for (int i = 0; i < 10; ++i) ASSERT_GE(r1.append(std::span<const uint8_t>(payload), false), 0);
    for (int i = 0; i < kCap && r2.logEndOffset() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    EXPECT_EQ(r2.logEndOffset(), r1.logEndOffset());
    EXPECT_EQ(r1.epochHistoryMismatchesForTest(), 0u);
    EXPECT_FALSE(r2.truncationRefusedForTest());
}

TEST(TransientReplicaRegression, LeaderAppendStampsItsEpochIntoTheRing) {
    rune::storage::InMemoryLog mem(1 << 20, 1000);
    InMemoryTransport transport;
    PartitionReplica r1(tcfg(1), Role::Leader, 4, {1}, {1}, &mem, &transport);
    EXPECT_EQ(mem.leaderEpochStamp(), 4) << "a transient leader stamps restamp-appends with its epoch";
    r1.becomeFollower(5, 2);
    EXPECT_EQ(mem.leaderEpochStamp(), -1) << "a follower keeps the producer's bytes";
}

// Fix round 1 (review item 1): a follower whose per-node ring is smaller than a
// single leader batch refused the apply (kRefusedTooLarge) silently and
// re-fetched the same offset forever.  It must refuse VISIBLY (counted), wedge
// at the offset (stop fetching), and leave the ISR so the HW moves on.
TEST(TransientReplicaRegression, FollowerRingSmallerThanABatchWedgesAndLeavesTheIsrRegression) {
    rune::storage::InMemoryLog mem1(1 << 20, 1000), mem2(4 * 1024, 1000);
    CountingTransport transport;
    const std::vector<NodeId> all = {1, 2};
    PartitionConfig c1 = tcfg(1), c2 = tcfg(2);
    c1.replicaLagMaxMs = c2.replicaLagMaxMs = 20;                      // ms of the injected clock
    c1.isrCheckIntervalNs = c2.isrCheckIntervalNs = 1'000'000LL;
    PartitionReplica r1(c1, Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(c2, Role::Follower, 1, all, all, &mem2, &transport);
    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    const std::vector<uint8_t> big(8 * 1024, 0xB1);   // one ~8 KiB batch > the follower's 4 KiB ring
    ASSERT_EQ(r1.append(std::span<const uint8_t>(big), /*acksAll=*/true), 0);
    int64_t clock = 0;
    int fetchesAtWedge = -1;
    for (int i = 0; i < 200; ++i) {                    // 200 ms >> the 20 ms lag bound
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
        if (fetchesAtWedge < 0 && r2.wedgedAtOffset() == 0) fetchesAtWedge = transport.fetches;
    }
    EXPECT_EQ(r2.transientApplyTooLargeRefusals(), 1u) << "the refusal must be visible (counted)";
    EXPECT_EQ(r2.transientApplyFullRefusals(), 0u);
    EXPECT_EQ(r2.wedgedAtOffset(), 0) << "the follower must wedge explicitly at the unstorable offset";
    ASSERT_GE(fetchesAtWedge, 0) << "never wedged: silent infinite re-fetch";
    EXPECT_LE(transport.fetches - fetchesAtWedge, 1) << "a wedged follower stops re-fetching";
    EXPECT_EQ(r1.isr(), std::vector<NodeId>{1}) << "the wedged follower must leave the ISR";
    EXPECT_EQ(r1.highWatermark(), 1) << "acks=all progresses with the remaining ISR (minIsr 1)";
}

// Fix round 1 (review item 2): the follower's eviction floor was the HW of the
// PREVIOUS response, so a ring full of records this very frame commits refused
// the apply (kRefusedFull) — a wasted frame + round trip per refill.  The floor
// is max(followerHw_, min(LEO, resp.highWatermark)) before the append.
TEST(TransientReplicaRegression, FollowerApplyFloorUsesTheFramesHighWatermarkRegression) {
    rune::storage::InMemoryLog mem(8 * 1024, 1000);
    InMemoryTransport transport;
    PartitionReplica r2(tcfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &mem, &transport);
    auto frameOf = [](int64_t first, int n) {          // n ~1.1 KiB batches from `first`
        std::vector<uint8_t> f;
        for (int64_t b = first; b < first + n; ++b) {
            const auto x = stampedBatch(b, 1024);
            f.insert(f.end(), x.begin(), x.end());
        }
        return f;
    };
    const auto f0 = frameOf(0, 5), f1 = frameOf(5, 5);   // two frames overflow the 8 KiB ring
    FetchResponseMsg r{};
    r.leaderEpoch = 1; r.partition = 0; r.topicId = 7; r.logStartOffset = 0; r.errorCode = 0;
    r.highWatermark = 0;                                  // nothing committed yet
    r.records = std::span<const uint8_t>(f0);
    r2.applyReplicated(r);
    ASSERT_EQ(r2.logEndOffset(), 5);
    r.highWatermark = 5;                                  // THIS frame commits [0,5)
    r.records = std::span<const uint8_t>(f1);
    r2.applyReplicated(r);
    EXPECT_EQ(r2.transientApplyFullRefusals(), 0u) << "a frame's own HW must free the ring before its append";
    EXPECT_EQ(r2.logEndOffset(), 10) << "the frame was refused and must be re-fetched (a wasted round trip)";
    EXPECT_GT(mem.logStartOffset(), 0) << "committed records were evicted to make room";
}

// Fix round 1: a multi-batch frame larger than this node's ring (a push, or a
// leader frame sized by ITS config) is applied as the longest whole-batch prefix
// that fits — never refused TooLarge (which now wedges) while each batch fits.
TEST(TransientReplicaRegression, FollowerAppliesTheFittingPrefixOfAFrameLargerThanItsRingRegression) {
    rune::storage::InMemoryLog mem(4 * 1024, 1000);
    InMemoryTransport transport;
    PartitionReplica r2(tcfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &mem, &transport);
    std::vector<uint8_t> frame;
    for (int64_t b = 0; b < 3; ++b) {                  // three ~2 KiB batches: ~6 KiB > 4 KiB ring
        const auto x = stampedBatch(b, 2000);
        frame.insert(frame.end(), x.begin(), x.end());
    }
    FetchResponseMsg r{};
    r.leaderEpoch = 1; r.partition = 0; r.topicId = 7; r.highWatermark = 0; r.logStartOffset = 0;
    r.errorCode = 0; r.records = std::span<const uint8_t>(frame);
    r2.applyReplicated(r);
    EXPECT_EQ(r2.logEndOffset(), 1) << "the first batch (the prefix that fits) is applied";
    EXPECT_EQ(r2.transientApplyTooLargeRefusals(), 0u);
    EXPECT_EQ(r2.wedgedAtOffset(), -1) << "a storable batch never wedges the follower";
}

// Fix round 1 (fix-on-sight): applyReplicated appended `records.size() - newStart`
// bytes — everything past the first new batch, including a batch past a GAP the
// walk stopped at — while advancing LEO only to the last contiguous batch.  The
// stored entry then carried bytes of offsets it does not own.
TEST(TransientReplicaRegression, FollowerAppliesOnlyTheContiguousBatchesOfAFrameRegression) {
    rune::storage::InMemoryLog mem(1 << 20, 1000);
    InMemoryTransport transport;
    PartitionReplica r2(tcfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &mem, &transport);
    const auto b0 = stampedBatch(0, 100), gap = stampedBatch(5, 100);   // offsets 1..4 missing
    std::vector<uint8_t> frame(b0);
    frame.insert(frame.end(), gap.begin(), gap.end());
    FetchResponseMsg r{};
    r.leaderEpoch = 1; r.partition = 0; r.topicId = 7; r.highWatermark = 0; r.logStartOffset = 0;
    r.errorCode = 0; r.records = std::span<const uint8_t>(frame);
    r2.applyReplicated(r);
    ASSERT_EQ(r2.logEndOffset(), 1);
    const auto v = mem.readView(0, 1 << 20);
    EXPECT_EQ(v.len, b0.size()) << "the batch past the gap must not be stored under offset 0";
}

// Fix round 2 (re-review): the follower's kRefusedFull branch — a frame that
// needs room while every held record is still above the HW is refused VISIBLY
// (counted), applies nothing, does NOT wedge (retriable), and the same records
// apply once a frame carries a HW that commits the held ones.
TEST(TransientReplicaRegression, FollowerRingFullAboveTheHwRefusesRetriablyAndAppliesOnceTheHwMovesRegression) {
    rune::storage::InMemoryLog mem(8 * 1024, 1000);
    InMemoryTransport transport;
    PartitionReplica r2(tcfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &mem, &transport);
    auto frameOf = [](int64_t first, int n) {
        std::vector<uint8_t> f;
        for (int64_t b = first; b < first + n; ++b) {
            const auto x = stampedBatch(b, 1024);
            f.insert(f.end(), x.begin(), x.end());
        }
        return f;
    };
    const auto f0 = frameOf(0, 5), f1 = frameOf(5, 5);   // two frames overflow the 8 KiB ring
    FetchResponseMsg r{};
    r.leaderEpoch = 1; r.partition = 0; r.topicId = 7; r.logStartOffset = 0; r.errorCode = 0;
    r.highWatermark = 0;
    r.records = std::span<const uint8_t>(f0);
    r2.applyReplicated(r);
    ASSERT_EQ(r2.logEndOffset(), 5);
    r.records = std::span<const uint8_t>(f1);            // HW still 0: nothing evictable
    r2.applyReplicated(r);
    EXPECT_EQ(r2.transientApplyFullRefusals(), 1u) << "the refusal must be visible (counted)";
    EXPECT_EQ(r2.transientApplyTooLargeRefusals(), 0u);
    EXPECT_EQ(r2.logEndOffset(), 5) << "a refused frame applies nothing";
    EXPECT_EQ(mem.logStartOffset(), 0) << "no uncommitted record was evicted";
    EXPECT_EQ(r2.wedgedAtOffset(), -1) << "a full ring is retriable: never wedge";
    r.highWatermark = 5;                                  // the leader's next answer commits [0,5)
    r2.applyReplicated(r);
    EXPECT_EQ(r2.logEndOffset(), 10) << "the re-served frame applies once the HW moves";
    EXPECT_EQ(r2.transientApplyFullRefusals(), 1u);
    EXPECT_GT(mem.logStartOffset(), 0);
    EXPECT_EQ(r2.wedgedAtOffset(), -1);
}
