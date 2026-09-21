// ---------------------------------------------------------------------------
// ReplicatedTxnStateStoreTest.cpp — Unit tests for ReplicatedTxnStateStore (TX-3).
//
// Uses a single-node PartitionReplica (leader, minIsr=1) so the HW advances
// immediately after append() (majority-of-1 == the leader itself).
//
// Test matrix:
//   1. PutThenGetRoundTrip — put, drive HW, get returns the metadata.
//   2. RebuildLastWriteWinsAcrossSegments — two puts same txnId; rebuild
//      returns the last-written state.
//   3. PutReturnsFalseWhenGated — minIsr=2 but only 1 node.
//   4. GetReturnsNullForUnknown — get unknown txnId → nullptr.
//   5. MultiplePartitionsPreservedOnRebuild — partitions survive rebuild.
//   6. MultiTxnRebuild — multiple distinct txnIds survive rebuild.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

using namespace rune::transaction;
using namespace rune::replication;

// ===========================================================================
// Test infrastructure
// ===========================================================================

static constexpr int kDriverCap = 600;

static std::string makeTempSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct TxnTempDir {
    std::string path;
    explicit TxnTempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_rtss_" + std::string(tag) + "_" + makeTempSuffix();
        std::filesystem::create_directories(path);
    }
    ~TxnTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

class NullTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

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
    cfg.pushOnAppend        = false;
    return cfg;
}

// Helper: build a TransactionMetadata.
static TransactionMetadata makeMeta(const std::string& txnId,
                                     ProducerId pid,
                                     ProducerEpoch epoch,
                                     TxnState state)
{
    TransactionMetadata m;
    m.txnId         = txnId;
    m.producerId    = pid;
    m.producerEpoch = epoch;
    m.state         = state;
    m.txnTimeoutMs  = 30'000;
    m.txnStartNs    = (state == TxnState::Ongoing) ? 1'000'000LL : 0;
    return m;
}

// ===========================================================================
// Fixture: single-node leader (HW advances on every append)
// ===========================================================================
class ReplicatedTxnStateStoreTest : public ::testing::Test {
protected:
    static constexpr NodeId kLeaderId = 1;

    ReplicatedTxnStateStoreTest()
        : dir("unit")
        , log(dir.path)
    {}

    void SetUp() override {
        transport_.reset(new NullTransport{});
        replica_ = std::make_unique<PartitionReplica>(
            makeSingleNodeConfig(kLeaderId, /*minIsr=*/1),
            Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{kLeaderId},
            std::vector<NodeId>{kLeaderId},
            &log, transport_.get());

        store_ = std::make_unique<ReplicatedTxnStateStore>(replica_.get());
    }

    bool driveHwAdvance(Offset target = 1, int64_t stepNs = 1'000'000LL) {
        for (int i = 0; i < kDriverCap && replica_->highWatermark() < target; ++i) {
            clock_ += stepNs;
            replica_->doWork(clock_);
        }
        return replica_->highWatermark() >= target;
    }

    TxnTempDir dir;
    rune::storage::Log log;
    std::unique_ptr<PartitionTransport>      transport_;
    std::unique_ptr<PartitionReplica>        replica_;
    std::unique_ptr<ReplicatedTxnStateStore> store_;
    int64_t clock_ = 0;
};

// ===========================================================================
// 1. PutThenGetRoundTrip — from the brief
// ===========================================================================
TEST_F(ReplicatedTxnStateStoreTest, PutThenGetRoundTrip) {
    TransactionMetadata m = makeMeta("tx1", /*pid=*/7, /*epoch=*/0,
                                     TxnState::Ongoing);
    ASSERT_TRUE(store_->put(m, /*coordEpoch=*/1));
    ASSERT_TRUE(driveHwAdvance(1));
    const auto* got = store_->get("tx1");
    ASSERT_TRUE(got);
    EXPECT_EQ(got->state, TxnState::Ongoing);
    EXPECT_EQ(got->producerId, 7);
    EXPECT_EQ(got->producerEpoch, 0);
}

// ===========================================================================
// 2. RebuildLastWriteWinsAcrossSegments — from the brief
// ===========================================================================
TEST(ReplicatedTxnStateStore, RebuildLastWriteWinsAcrossSegments) {
    // Use a tiny segment roll to force multiple segments.
    TxnTempDir dir("rebuild");
    constexpr int64_t kTinyRoll = 512;
    rune::storage::Log log(dir.path, kTinyRoll);

    NullTransport nt;
    PartitionReplica replica(
        makeSingleNodeConfig(/*selfId=*/1, /*minIsr=*/1),
        Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &nt);

    int64_t clock = 0;
    auto pumpHW = [&](Offset target) {
        for (int i = 0; i < kDriverCap && replica.highWatermark() < target; ++i) {
            clock += 1'000'000LL;
            replica.doWork(clock);
        }
        return replica.highWatermark() >= target;
    };

    {
        ReplicatedTxnStateStore s(&replica);
        auto m1 = makeMeta("tx1", 7, 0, TxnState::Ongoing);
        auto m2 = makeMeta("tx1", 7, 0, TxnState::CompleteCommit);
        ASSERT_TRUE(s.put(m1, 1));
        ASSERT_TRUE(s.put(m2, 1));
        ASSERT_TRUE(pumpHW(2));
    }

    ReplicatedTxnStateStore s2(&replica);
    s2.rebuildFromLog();
    ASSERT_TRUE(s2.get("tx1"));
    EXPECT_EQ(s2.get("tx1")->state, TxnState::CompleteCommit);
}

// ===========================================================================
// 3. PutReturnsFalseWhenGated
// ===========================================================================
TEST(ReplicatedTxnStateStore, PutReturnsFalseWhenGated) {
    TxnTempDir dir("gated");
    rune::storage::Log log(dir.path);

    NullTransport nt;
    PartitionReplica replica(
        makeSingleNodeConfig(/*selfId=*/1, /*minIsr=*/2),
        Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &nt);

    ReplicatedTxnStateStore store(&replica);
    auto m = makeMeta("tx1", 7, 0, TxnState::Ongoing);
    EXPECT_FALSE(store.put(m, 1));
    EXPECT_EQ(store.get("tx1"), nullptr);
}

// ===========================================================================
// 4. GetReturnsNullForUnknown
// ===========================================================================
TEST_F(ReplicatedTxnStateStoreTest, GetReturnsNullForUnknown) {
    EXPECT_EQ(store_->get("nonexistent"), nullptr);
    auto m = makeMeta("tx1", 1, 0, TxnState::Empty);
    ASSERT_TRUE(store_->put(m, 0));
    EXPECT_EQ(store_->get("nonexistent"), nullptr);
}

// ===========================================================================
// 5. MultiplePartitionsPreservedOnRebuild
// ===========================================================================
TEST_F(ReplicatedTxnStateStoreTest, MultiplePartitionsPreservedOnRebuild) {
    TransactionMetadata m = makeMeta("tx_parts", 42, 1, TxnState::Ongoing);
    m.partitions.insert(TopicPartition{"topicA", 0});
    m.partitions.insert(TopicPartition{"topicA", 1});
    m.partitions.insert(TopicPartition{"topicB", 0});

    ASSERT_TRUE(store_->put(m, 2));
    ASSERT_TRUE(driveHwAdvance(1));

    ReplicatedTxnStateStore rebuilt(replica_.get());
    rebuilt.rebuildFromLog();

    const auto* got = rebuilt.get("tx_parts");
    ASSERT_TRUE(got);
    EXPECT_EQ(got->state, TxnState::Ongoing);
    EXPECT_EQ(got->producerId, 42);
    EXPECT_EQ(got->partitions.size(), 3u);
}

// ===========================================================================
// 6. MultiTxnRebuild
// ===========================================================================
TEST_F(ReplicatedTxnStateStoreTest, MultiTxnRebuild) {
    auto m1 = makeMeta("txA", 1, 0, TxnState::Ongoing);
    auto m2 = makeMeta("txB", 2, 0, TxnState::PrepareCommit);
    auto m3 = makeMeta("txC", 3, 1, TxnState::CompleteAbort);

    ASSERT_TRUE(store_->put(m1, 1));
    ASSERT_TRUE(store_->put(m2, 1));
    ASSERT_TRUE(store_->put(m3, 1));
    ASSERT_TRUE(driveHwAdvance(3));

    ReplicatedTxnStateStore rebuilt(replica_.get());
    rebuilt.rebuildFromLog();

    ASSERT_TRUE(rebuilt.get("txA"));
    EXPECT_EQ(rebuilt.get("txA")->state, TxnState::Ongoing);
    ASSERT_TRUE(rebuilt.get("txB"));
    EXPECT_EQ(rebuilt.get("txB")->state, TxnState::PrepareCommit);
    ASSERT_TRUE(rebuilt.get("txC"));
    EXPECT_EQ(rebuilt.get("txC")->state, TxnState::CompleteAbort);
    EXPECT_EQ(rebuilt.get("txD"), nullptr);
}

// ===========================================================================
// CP-1: the __transaction_state record must carry the transactionalId as the
// record key (compaction key).  This guards the appendKeyed() re-keying so a
// future refactor back to append() (empty key) regresses loudly.
// ===========================================================================

// lastRecordKey — read [0, HW) and return the key bytes of the last record.
static std::vector<uint8_t> lastRecordKey(const PartitionReplica& replica) {
    const Offset hw = replica.highWatermark();
    if (hw <= 0) return {};

    std::vector<uint8_t> result;
    Offset fetchOffset = 0;

    while (fetchOffset < hw) {
        std::vector<uint8_t> data = replica.read(fetchOffset, 64 * 1024 * 1024);
        if (data.empty()) break;

        std::size_t pos = 0;
        Offset lastBatchEnd = fetchOffset;

        while (pos < data.size()) {
            const uint8_t*    batchStart = data.data() + pos;
            const std::size_t remaining  = data.size() - pos;
            if (remaining < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(batchStart, remaining);
            if (!reader.ensureValid()) break;

            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

            reader.forEach([&result](std::string_view key, std::string_view /*val*/,
                                     int64_t /*off*/, int64_t /*ts*/) {
                result.assign(key.begin(), key.end());
            });

            const Offset batchNextOffset =
                reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (batchNextOffset > lastBatchEnd) lastBatchEnd = batchNextOffset;

            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break;
        fetchOffset = lastBatchEnd;
    }
    return result;
}

TEST_F(ReplicatedTxnStateStoreTest, CP1_RecordCarriesTxnIdKey) {
    auto m = makeMeta("my-txn-id", /*pid=*/7, /*epoch=*/0, TxnState::Ongoing);
    ASSERT_TRUE(store_->put(m, /*coordEpoch=*/1));
    ASSERT_TRUE(driveHwAdvance(1));

    auto keyBytes = lastRecordKey(*replica_);
    ASSERT_FALSE(keyBytes.empty()) << "txn record key must be non-empty (CP-1 compaction key)";

    std::string keyStr(keyBytes.begin(), keyBytes.end());
    EXPECT_EQ(keyStr, "my-txn-id") << "record key must equal the transactionalId";
}
