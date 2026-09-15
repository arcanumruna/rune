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
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
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
