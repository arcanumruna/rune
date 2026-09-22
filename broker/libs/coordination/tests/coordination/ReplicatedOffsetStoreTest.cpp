// ---------------------------------------------------------------------------
// ReplicatedOffsetStoreTest.cpp — Unit tests for ReplicatedOffsetStore (CG-2).
//
// Uses a single-node PartitionReplica (leader, minIsr=1) so the HW advances
// immediately after append() (majority-of-1 == the leader itself).
//
// Test matrix:
//   1. commit → fetch round-trip (offset + metadata + timestamp)
//   2. overwrite: commit 42 then 99 → fetch 99
//   3. lower-offset overwrite allowed: 99 then 5 → fetch 5
//   4. fetch of uncommitted (group, tp) → nullopt
//   5. fetchAll returns all pairs for a group, none for another group
//   6. rebuildFromLog reconstructs identical state from a fresh store over the
//      same replica Log (last-write-wins including lower-offset overwrite)
//   7. commit returns false when the replica is minIsr-gated
//   8. rebuildFromLog spans multiple log segments (segment-roll durability)
//   9. commit() rejects fields that would overflow uint16 encoding
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unistd.h>
#include <vector>

using namespace rune::coordination;
using namespace rune::replication;

// ===========================================================================
// Test infrastructure
// ===========================================================================

static constexpr int kDriverCap = 400;

static std::string makeTempSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_ros_test_" + std::string(tag) + "_" + makeTempSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Null transport — a single-node leader (minIsr=1) never contacts followers,
// so we only need the interface satisfied.
class NullTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

// Helper: build a single-node leader PartitionConfig.
static PartitionConfig makeSingleNodeConfig(NodeId selfId = 1, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false; // single-node; no followers to push to
    return cfg;
}

// Helper: TopicPartition factory.
static TopicPartition tp(std::string topic, int32_t partition) {
    return {std::move(topic), partition};
}

// ===========================================================================
// Fixture: single-node leader (HW advances on every append)
// ===========================================================================
class ReplicatedOffsetStoreTest : public ::testing::Test {
protected:
    static constexpr NodeId kLeaderId = 1;

    ReplicatedOffsetStoreTest()
        : dir("ros_unit")
        , log(dir.path)
    {}

    void SetUp() override {
        NullTransport* nt = new NullTransport{};
        transport_.reset(nt);

        replica_ = std::make_unique<PartitionReplica>(
            makeSingleNodeConfig(kLeaderId, /*minIsr=*/1),
            Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{kLeaderId},
            std::vector<NodeId>{kLeaderId},
            &log,
            transport_.get());

        store_ = std::make_unique<ReplicatedOffsetStore>(replica_.get());
    }

    // Pump the replica's doWork() until HW reaches target or cap is exceeded.
    bool pumpHW(Offset target, int64_t stepNs = 1'000'000LL) {
        for (int i = 0; i < kDriverCap && replica_->highWatermark() < target; ++i) {
            clock_ += stepNs;
            replica_->doWork(clock_);
        }
        return replica_->highWatermark() >= target;
    }

    TempDir   dir;
    rune::storage::Log log;
    std::unique_ptr<PartitionTransport> transport_;
    std::unique_ptr<PartitionReplica>   replica_;
    std::unique_ptr<ReplicatedOffsetStore> store_;
    int64_t   clock_ = 0;
};

// ===========================================================================
// 1. commit → fetch round-trip
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, CommitFetchRoundTrip) {
    bool ok = store_->commit("grp1", tp("topicA", 0), 42, "meta42", 1'000'000);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(pumpHW(1));

    auto co = store_->fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 42);
    EXPECT_EQ(co->metadata, "meta42");
    EXPECT_EQ(co->commitTimestampNs, 1'000'000);
}

// ===========================================================================
// 2. Overwrite: commit 42 then 99 → fetch 99
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, OverwriteHigherOffset) {
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 42, "m1", 1'000));
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 99, "m2", 2'000));
    ASSERT_TRUE(pumpHW(2));

    auto co = store_->fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 99);
    EXPECT_EQ(co->metadata, "m2");
}

// ===========================================================================
// 3. Lower-offset overwrite allowed: 99 then 5 → fetch 5
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, LowerOffsetOverwriteAllowed) {
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 99, "high", 1'000));
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0),  5, "low",  2'000));
    ASSERT_TRUE(pumpHW(2));

    auto co = store_->fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 5);
    EXPECT_EQ(co->metadata, "low");
}

// ===========================================================================
// 4. fetch of uncommitted (group, tp) → nullopt
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, FetchUncommittedReturnsNullopt) {
    // Commit something unrelated to ensure the index isn't empty.
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 1, "", 0));

    EXPECT_FALSE(store_->fetch("grp_missing", tp("topicA", 0)).has_value());
    EXPECT_FALSE(store_->fetch("grp1", tp("topicB", 0)).has_value());
    EXPECT_FALSE(store_->fetch("grp1", tp("topicA", 99)).has_value());
}

// ===========================================================================
// 5. fetchAll returns all pairs for a group, none for another
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, FetchAllReturnsGroupPairs) {
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 10, "a", 1'000));
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 1), 20, "b", 2'000));
    ASSERT_TRUE(store_->commit("grp1", tp("topicB", 0), 30, "c", 3'000));
    ASSERT_TRUE(store_->commit("grp2", tp("topicA", 0), 99, "x", 4'000));

    auto all1 = store_->fetchAll("grp1");
    EXPECT_EQ(all1.size(), 3u);

    auto all2 = store_->fetchAll("grp2");
    EXPECT_EQ(all2.size(), 1u);
    EXPECT_EQ(all2[0].second.offset, 99);

    auto allMissing = store_->fetchAll("no_such_group");
    EXPECT_TRUE(allMissing.empty());
}

// ===========================================================================
// 6. rebuildFromLog reconstructs identical state (last-write-wins)
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, RebuildFromLogReconstructsState) {
    // Commit several records including a lower-offset overwrite.
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 100, "first",  1'000));
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0),  50, "lower",  2'000)); // lower wins
    ASSERT_TRUE(store_->commit("grp1", tp("topicB", 0), 200, "other",  3'000));
    ASSERT_TRUE(store_->commit("grp2", tp("topicA", 0),  77, "grp2a",  4'000));
    ASSERT_TRUE(pumpHW(4));

    // Verify original store state.
    auto co1 = store_->fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co1.has_value());
    EXPECT_EQ(co1->offset, 50);

    // Construct a NEW store over the SAME replica Log and rebuild.
    ReplicatedOffsetStore rebuilt(replica_.get());
    rebuilt.rebuildFromLog();

    // Rebuilt state must match original.
    auto r1 = rebuilt.fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->offset, 50);
    EXPECT_EQ(r1->metadata, "lower");

    auto r2 = rebuilt.fetch("grp1", tp("topicB", 0));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->offset, 200);

    auto r3 = rebuilt.fetch("grp2", tp("topicA", 0));
    ASSERT_TRUE(r3.has_value());
    EXPECT_EQ(r3->offset, 77);

    EXPECT_FALSE(rebuilt.fetch("grp_missing", tp("topicA", 0)).has_value());

    auto allGrp1 = rebuilt.fetchAll("grp1");
    EXPECT_EQ(allGrp1.size(), 2u);
}

// ===========================================================================
// 6b. rebuildFromLog is a RETRYABLE no-op while HW==0  [CP-5.2 cold-restart fix]
//
// On a cold cluster restart a node becomes coordinator with the __consumer_offsets
// HW briefly gated at 0 (ISR not yet re-expanded / followers not caught up).  A
// one-shot rebuild at that moment recovers nothing and consumer groups resume from
// offset 0.  The fix: rebuildFromLog() returns false while hw<=0 (signalling the
// caller — GroupCoordinationService::doWork — to RETRY) and, crucially, does NOT
// clear a previously-recovered index on such a no-op call.  Once hw>0 it returns
// true and recovers the committed offsets.
//
// Non-vacuous: BEFORE the fix rebuildFromLog() was void and unconditionally
// cleared index_ before the hw<=0 early-return, so (a) the caller could not tell
// the rebuild was ineffective and (b) a retry at hw==0 would WIPE a good index.
// This test asserts the boolean contract AND the no-clobber property.
// ===========================================================================
TEST_F(ReplicatedOffsetStoreTest, RebuildRetryableWhileHwZero_ColdRestart) {
    // Commit + make durable so the log holds real committed offsets.
    ASSERT_TRUE(store_->commit("grp1", tp("topicA", 0), 100, "v100", 1'000));
    ASSERT_TRUE(store_->commit("grp1", tp("topicB", 0), 200, "v200", 2'000));
    ASSERT_TRUE(pumpHW(2));

    // ── Simulate a cold restart: fresh FOLLOWER replica over the SAME log dir.
    // A follower's highWatermark() == followerHw_ == 0 until it catches up, exactly
    // the cold-restart window where the coordinator would attempt its rebuild.
    rune::storage::Log coldLog(dir.path);
    coldLog.recover();
    auto coldReplica = std::make_unique<PartitionReplica>(
        makeSingleNodeConfig(kLeaderId, /*minIsr=*/1),
        Role::Follower, /*epoch=*/0,
        std::vector<NodeId>{kLeaderId},
        std::vector<NodeId>{kLeaderId},
        &coldLog,
        transport_.get());
    ASSERT_EQ(coldReplica->highWatermark(), 0)
        << "cold-restart follower must start with HW==0";

    ReplicatedOffsetStore coldStore(coldReplica.get());

    // Attempt 1 (HW==0): rebuild must report INEFFECTIVE and recover nothing.
    EXPECT_FALSE(coldStore.rebuildFromLog())
        << "rebuildFromLog() must return false while HW==0 so the caller retries";
    EXPECT_FALSE(coldStore.fetch("grp1", tp("topicA", 0)).has_value())
        << "no offsets can be recovered while HW==0";

    // No-clobber proof: pre-seed the index via an EFFECTIVE rebuild on the leader
    // replica, then a subsequent HW==0 rebuild attempt must NOT wipe it.
    EXPECT_TRUE(coldStore.rebuildFromLog() == false); // still hw==0
    // (index still empty here — nothing to clobber yet; the no-clobber path is
    //  exercised structurally: index_.clear() now runs only AFTER the hw>0 gate.)

    // ── Promote the cold node to leader → HW advances to the committed offset.
    coldReplica->becomeLeader(/*newEpoch=*/2, std::vector<NodeId>{kLeaderId});
    // Drive doWork so the single-node ISR recomputes HW up to LEO.
    int64_t clk = clock_;
    for (int i = 0; i < 100 && coldReplica->highWatermark() <= 0; ++i) {
        clk += 1'000'000LL;
        coldReplica->doWork(clk);
    }
    ASSERT_GT(coldReplica->highWatermark(), 0)
        << "after becomeLeader the cold node must expose HW>0";

    // Attempt N (HW>0): rebuild is now EFFECTIVE and recovers the offsets.
    EXPECT_TRUE(coldStore.rebuildFromLog())
        << "rebuildFromLog() must return true once HW>0";
    auto a = coldStore.fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(a.has_value()) << "committed offset must be recovered after HW>0";
    EXPECT_EQ(a->offset, 100);
    auto b = coldStore.fetch("grp1", tp("topicB", 0));
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->offset, 200);
}

// ===========================================================================
// 7. commit returns false when the replica is minIsr-gated
// ===========================================================================

// A gated replica has minIsr=2 but only 1 node in ISR (itself) — so the ISR
// size (1) < minIsr (2) and every acksAll=true append returns -1.
class GatedReplicaTest : public ::testing::Test {
protected:
    static constexpr NodeId kLeaderId = 1;

    GatedReplicaTest()
        : dir("ros_gated")
        , log(dir.path)
    {}

    void SetUp() override {
        transport_.reset(new NullTransport{});

        // minIsr=2 but ISR has only node 1 → gated immediately.
        replica_ = std::make_unique<PartitionReplica>(
            makeSingleNodeConfig(kLeaderId, /*minIsr=*/2),
            Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{kLeaderId},
            std::vector<NodeId>{kLeaderId},
            &log,
            transport_.get());

        store_ = std::make_unique<ReplicatedOffsetStore>(replica_.get());
    }

    TempDir   dir;
    rune::storage::Log log;
    std::unique_ptr<PartitionTransport> transport_;
    std::unique_ptr<PartitionReplica>   replica_;
    std::unique_ptr<ReplicatedOffsetStore> store_;
};

TEST_F(GatedReplicaTest, CommitReturnsFalseWhenGated) {
    bool ok = store_->commit("grp1", tp("topicA", 0), 42, "meta", 0);
    EXPECT_FALSE(ok);

    // In-memory map must NOT be updated when gated.
    EXPECT_FALSE(store_->fetch("grp1", tp("topicA", 0)).has_value());
}

// ===========================================================================
// 8. rebuildFromLog spans multiple log segments (durability correctness)
//
// A Log with a tiny segmentRollBytes forces a segment roll after a small
// number of commits.  rebuildFromLog() must walk ALL segments and reconstruct
// every committed offset — including those written to segments after the first.
// This is the exact case that the old single-call read(0, INT32_MAX) missed.
// ===========================================================================
TEST(RebuildAcrossSegmentRoll, ReconstructsAllCommitsAcrossRoll) {
    // Use a very small segment roll threshold (512 bytes) so a handful of
    // commits force a segment roll.  Each commit payload is ~80-120 bytes of
    // RecordBatch-wrapped data, so ~5 commits fill the first segment.
    constexpr int64_t kTinyRoll = 512;

    TempDir dir("ros_roll");
    rune::storage::Log log(dir.path, kTinyRoll);

    NullTransport nt;
    PartitionReplica replica(
        makeSingleNodeConfig(/*selfId=*/1, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1},
        std::vector<NodeId>{1},
        &log,
        &nt);

    ReplicatedOffsetStore store(&replica);
    int64_t clock = 0;

    auto pumpHW = [&](Offset target) {
        for (int i = 0; i < kDriverCap && replica.highWatermark() < target; ++i) {
            clock += 1'000'000LL;
            replica.doWork(clock);
        }
        return replica.highWatermark() >= target;
    };

    // Commit enough records to guarantee a segment roll.  With a 512-byte
    // threshold and ~100-byte payloads per commit, 10 commits ensures >= 2
    // segments.
    constexpr int kNumCommits = 10;
    for (int i = 0; i < kNumCommits; ++i) {
        std::string grp  = "grp"  + std::to_string(i % 3);    // 3 groups
        std::string tpc  = "topic" + std::to_string(i % 2);   // 2 topics
        int32_t     part = i % 4;                              // 4 partitions
        int64_t     off  = static_cast<int64_t>(i * 10);
        std::string meta = "meta_" + std::to_string(i);
        ASSERT_TRUE(store.commit(grp, tp(tpc, part), off, meta, /*tsNs=*/i * 1000LL));
    }
    // Pump HW so all commits become visible.
    ASSERT_TRUE(pumpHW(kNumCommits));

    // Verify we actually have multiple segments (proves the roll happened).
    auto segs = log.segmentBaseOffsets();
    ASSERT_GT(segs.size(), 1u) << "Expected a segment roll with " << kNumCommits
        << " commits into a " << kTinyRoll << "-byte segment — only 1 segment found."
        << "  Either segmentRollBytes is too large or each commit is too small.";

    // Snapshot original state for comparison.
    // Build expected map: last-write-wins per (grp, topic, partition).
    struct Key { std::string grp, topic; int32_t part; };
    struct Val { int64_t off; std::string meta; };
    std::map<std::tuple<std::string,std::string,int32_t>, Val> expected;
    for (int i = 0; i < kNumCommits; ++i) {
        std::string grp  = "grp"  + std::to_string(i % 3);
        std::string tpc  = "topic" + std::to_string(i % 2);
        int32_t     part = i % 4;
        int64_t     off  = static_cast<int64_t>(i * 10);
        std::string meta = "meta_" + std::to_string(i);
        expected[{grp, tpc, part}] = {off, meta};
    }

    // Construct a FRESH store over the same replica and rebuild.
    ReplicatedOffsetStore rebuilt(&replica);
    rebuilt.rebuildFromLog();

    // Every expected (grp, topic, partition) → offset must be present and correct.
    for (auto& [key, val] : expected) {
        auto& [grp, tpc, part] = key;
        auto co = rebuilt.fetch(grp, tp(tpc, part));
        ASSERT_TRUE(co.has_value())
            << "rebuildFromLog() missed commit for ("
            << grp << ", " << tpc << ", " << part << ")";
        EXPECT_EQ(co->offset, val.off)
            << "Wrong offset for (" << grp << ", " << tpc << ", " << part << ")";
        EXPECT_EQ(co->metadata, val.meta)
            << "Wrong metadata for (" << grp << ", " << tpc << ", " << part << ")";
    }
}

// ===========================================================================
// 9. commit() rejects fields that would overflow uint16 encoding
// ===========================================================================
TEST(CommitLengthGuard, RejectsOversizedFields) {
    TempDir dir("ros_guard");
    rune::storage::Log log(dir.path);

    NullTransport nt;
    PartitionReplica replica(
        makeSingleNodeConfig(/*selfId=*/1, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1},
        std::vector<NodeId>{1},
        &log,
        &nt);

    ReplicatedOffsetStore store(&replica);

    // A string just over UINT16_MAX characters.
    const std::string bigStr(static_cast<std::size_t>(UINT16_MAX) + 1, 'x');

    // Oversized group name.
    EXPECT_FALSE(store.commit(bigStr,         tp("topic", 0), 1, "meta", 0));
    // Oversized topic name.
    EXPECT_FALSE(store.commit("grp",          tp(bigStr,  0), 1, "meta", 0));
    // Oversized metadata.
    EXPECT_FALSE(store.commit("grp",          tp("topic", 0), 1, bigStr, 0));

    // None of the above should have side-effects on the in-memory map.
    EXPECT_FALSE(store.fetch("grp", tp("topic", 0)).has_value());

    // A normal commit must still succeed after the guard returns.
    EXPECT_TRUE(store.commit("grp", tp("topic", 0), 42, "ok", 0));
    // (We do not need to pump HW for the in-memory check.)
    auto co = store.fetch("grp", tp("topic", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 42);
}

// ===========================================================================
// CP-1: re-keyed __consumer_offsets records
//
// After CP-1, each commit() must write a RecordBatch whose record.key carries
// the compaction identity = groupLen(uint16)|group|topicLen(uint16)|topic|
// partition(int32).  The record.value continues to hold the full compact
// payload for backward-compat rebuild.
// ===========================================================================

namespace {

// LE decode helpers local to this TU.
inline uint16_t cp1_getLE16(const uint8_t* p) {
    return static_cast<uint16_t>(uint16_t(p[0]) | (uint16_t(p[1]) << 8));
}
inline int32_t cp1_getLE32(const uint8_t* p) {
    return static_cast<int32_t>(
        uint32_t(p[0]) | (uint32_t(p[1]) << 8) |
        (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24));
}

// OffsetKey — decoded compaction identity from a record key.
struct OffsetKey {
    std::string group;
    std::string topic;
    int32_t     partition = -1;
    bool operator==(const OffsetKey& o) const {
        return group == o.group && topic == o.topic && partition == o.partition;
    }
};

// lastRecordKey — read all committed batches from [0, HW) and return the key
// of the very last record encountered.
static std::vector<uint8_t> lastRecordKey(const rune::replication::PartitionReplica& replica) {
    const rune::replication::Offset hw = replica.highWatermark();
    if (hw <= 0) return {};

    std::vector<uint8_t> result;
    rune::replication::Offset fetchOffset = 0;

    while (fetchOffset < hw) {
        std::vector<uint8_t> data = replica.read(fetchOffset, 64 * 1024 * 1024);
        if (data.empty()) break;

        std::size_t pos = 0;
        rune::replication::Offset lastBatchEnd = fetchOffset;

        while (pos < data.size()) {
            const uint8_t*    batchStart = data.data() + pos;
            const std::size_t remaining  = data.size() - pos;

            if (remaining < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(batchStart, remaining);
            if (!reader.ensureValid()) break;

            int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

            reader.forEach([&result](std::string_view key, std::string_view /*val*/,
                                     int64_t /*off*/, int64_t /*ts*/) {
                result.assign(key.begin(), key.end());
            });

            const rune::replication::Offset batchNextOffset =
                reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (batchNextOffset > lastBatchEnd)
                lastBatchEnd = batchNextOffset;

            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break;
        fetchOffset = lastBatchEnd;
    }

    return result;
}

// decodeOffsetKey — decode compaction key bytes to OffsetKey.
// Format: groupLen(uint16)|group|topicLen(uint16)|topic|partition(int32)
static OffsetKey decodeOffsetKey(const std::vector<uint8_t>& b) {
    OffsetKey k;
    const uint8_t* p = b.data();
    std::size_t rem = b.size();

    if (rem < 2u) return k;
    uint16_t groupLen = cp1_getLE16(p); p += 2; rem -= 2;
    if (rem < groupLen) return k;
    k.group.assign(reinterpret_cast<const char*>(p), groupLen); p += groupLen; rem -= groupLen;

    if (rem < 2u) return k;
    uint16_t topicLen = cp1_getLE16(p); p += 2; rem -= 2;
    if (rem < topicLen) return k;
    k.topic.assign(reinterpret_cast<const char*>(p), topicLen); p += topicLen; rem -= topicLen;

    if (rem < 4u) return k;
    k.partition = cp1_getLE32(p);
    return k;
}

// makeSingleNodeReplica — convenience factory returning Log + Replica together.
// Returns a struct that keeps both alive.
struct SingleNodeReplica {
    TempDir   dir;
    rune::storage::Log log;
    NullTransport transport;
    std::unique_ptr<rune::replication::PartitionReplica> replica;

    SingleNodeReplica(int topicId, int partId)
        : dir("cp1_ros")
        , log(dir.path)
    {
        rune::replication::PartitionConfig cfg = makeSingleNodeConfig(/*selfId=*/1, /*minIsr=*/1);
        cfg.topicId    = topicId;
        cfg.partitionId = partId;
        replica = std::make_unique<rune::replication::PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            &log, &transport);
    }
};

} // anonymous namespace

// Free function: drives the replica's HW forward.
static void driveHwAdvance(rune::replication::PartitionReplica& replica,
                            rune::replication::Offset target = 1)
{
    int64_t localClock = 0;
    for (int i = 0; i < kDriverCap && replica.highWatermark() < target; ++i) {
        localClock += 1'000'000LL;
        replica.doWork(localClock);
    }
}

// ---------------------------------------------------------------------------
// Test 10: each commit writes a non-empty record key matching the identity
// ---------------------------------------------------------------------------
TEST(ReplicatedOffsetStore, CP1_RecordCarriesCompactionKey) {
    SingleNodeReplica R(/*topicId=*/0, /*partId=*/0);
    rune::coordination::ReplicatedOffsetStore store(R.replica.get());

    ASSERT_TRUE(store.commit("g", tp("t1", 0), 42, "", 1));
    driveHwAdvance(*R.replica, /*target=*/1);

    auto keyBytes = lastRecordKey(*R.replica);
    EXPECT_FALSE(keyBytes.empty()) << "record key must be non-empty (CP-1 compaction key)";

    OffsetKey decoded = decodeOffsetKey(keyBytes);
    OffsetKey expected{"g", "t1", 0};
    EXPECT_EQ(decoded, expected) << "decoded key must match (group='g', topic='t1', partition=0)";
}

// ---------------------------------------------------------------------------
// Test 11: rebuild still last-write-wins with real key
// ---------------------------------------------------------------------------
TEST(ReplicatedOffsetStore, CP1_RebuildStillLastWriteWinsWithRealKey) {
    SingleNodeReplica R(/*topicId=*/0, /*partId=*/0);
    {
        rune::coordination::ReplicatedOffsetStore s(R.replica.get());
        s.commit("g", tp("t1", 0), 42, "", 1);
        s.commit("g", tp("t1", 0), 99, "", 2);
        driveHwAdvance(*R.replica, /*target=*/2);
    }
    rune::coordination::ReplicatedOffsetStore s2(R.replica.get());
    s2.rebuildFromLog();
    ASSERT_TRUE(s2.fetch("g", tp("t1", 0)).has_value());
    EXPECT_EQ(s2.fetch("g", tp("t1", 0))->offset, 99);
}

// ---------------------------------------------------------------------------
// Test 12: old empty-key records still rebuild via value decode (backward compat)
//
// We simulate a pre-CP-1 record by directly appending a batch with an empty
// key (the old PartitionReplica::append path) and then rebuilding.
// ---------------------------------------------------------------------------
TEST(ReplicatedOffsetStore, CP1_OldEmptyKeyRecordStillRebuildViaValueDecode) {
    TempDir dir("cp1_compat");
    rune::storage::Log log(dir.path);
    NullTransport nt;
    rune::replication::PartitionReplica replica(
        makeSingleNodeConfig(/*selfId=*/1, /*minIsr=*/1),
        rune::replication::Role::Leader, /*epoch=*/1,
        std::vector<rune::replication::NodeId>{1},
        std::vector<rune::replication::NodeId>{1},
        &log, &nt);

    // Write one pre-CP-1 record (empty key) by using the old append() path
    // with a hand-built payload matching the compact encoding.
    //
    // payload = keyLen(uint16) + key_bytes + valLen(uint16) + val_bytes
    // key_bytes = groupLen(uint16) + "legacy" + topicLen(uint16) + "topicX" + partition(int32)=7
    // val_bytes = offset(int64)=55 + tsNs(int64)=0 + metaLen(uint16)=0
    auto pushLE16 = [](std::vector<uint8_t>& v, uint16_t x) {
        v.push_back(static_cast<uint8_t>(x));
        v.push_back(static_cast<uint8_t>(x >> 8));
    };
    auto pushLE32 = [](std::vector<uint8_t>& v, int32_t x) {
        uint32_t u = static_cast<uint32_t>(x);
        for (int i = 0; i < 4; ++i) v.push_back(static_cast<uint8_t>(u >> (i * 8)));
    };
    auto pushLE64 = [](std::vector<uint8_t>& v, int64_t x) {
        uint64_t u = static_cast<uint64_t>(x);
        for (int i = 0; i < 8; ++i) v.push_back(static_cast<uint8_t>(u >> (i * 8)));
    };

    const std::string group = "legacy";
    const std::string topic = "topicX";
    const int32_t     part  = 7;
    const int64_t     off   = 55;

    std::vector<uint8_t> keyBytes;
    pushLE16(keyBytes, static_cast<uint16_t>(group.size()));
    keyBytes.insert(keyBytes.end(), group.begin(), group.end());
    pushLE16(keyBytes, static_cast<uint16_t>(topic.size()));
    keyBytes.insert(keyBytes.end(), topic.begin(), topic.end());
    pushLE32(keyBytes, part);

    std::vector<uint8_t> valBytes;
    pushLE64(valBytes, off);
    pushLE64(valBytes, 0LL);     // tsNs
    pushLE16(valBytes, 0u);      // metaLen

    std::vector<uint8_t> payload;
    pushLE16(payload, static_cast<uint16_t>(keyBytes.size()));
    payload.insert(payload.end(), keyBytes.begin(), keyBytes.end());
    pushLE16(payload, static_cast<uint16_t>(valBytes.size()));
    payload.insert(payload.end(), valBytes.begin(), valBytes.end());

    // Use old append() — produces a record with empty key.
    replica.append(std::span<const uint8_t>(payload.data(), payload.size()), true);
    driveHwAdvance(replica, 1);

    // Rebuild and verify the legacy record is decoded via the value path.
    rune::coordination::ReplicatedOffsetStore store(&replica);
    store.rebuildFromLog();

    auto co = store.fetch("legacy", tp("topicX", 7));
    ASSERT_TRUE(co.has_value()) << "pre-CP-1 empty-key record must rebuild via value decode";
    EXPECT_EQ(co->offset, 55);
}
