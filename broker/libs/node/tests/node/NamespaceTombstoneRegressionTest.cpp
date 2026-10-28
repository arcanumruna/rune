// NamespaceTombstoneRegressionTest.cpp - namespaces Phase 2 Task 6b (D20): the offset store and the transaction-state
// store tombstone a deleted namespace's keys.
//
//   * tombstoneNamespace(nsId, batch, now) writes at most `batch` null-value records (the empty value that
//     LogCleaner::isTombstone recognises) for the live keys of nsId; it returns true only on a call that found nothing
//     to write (n keys: ceil(n / batch) calls return false, then one returns true; n = 0: the first call is true).
//   * rebuildFromLog() treats the null value as an erase; compaction drops the key and its tombstone.
//   * nsId 0 (`default`) is never tombstoned.
//   * Followers (three in-process nodes) rebuild to the same state as the leader.
//
// Interaction matrix (store x namespace x timing):
//   | store          | namespaces                     | timing                         | expected                    | test
//   |----------------|--------------------------------|--------------------------------|-----------------------------|-----
//   | offsets        | a (600), b (10), default (3)   | batches of 256, then restart   | a gone, b and default kept  | OffsetsGoneAfterRebuild
//   | txn state      | a (600), b (10), default (3)   | batches of 256, then restart   | a gone, b and default kept  | TxnStateGoneAfterRebuild
//   | both           | n = 0, 1, 255, 256, 257        | batch 256, 7, 1                | ceil(n/batch) false, 1 true | BatchBounds
//   | both           | default (nsId 0)               | any batch                      | nothing written, true       | DefaultNeverTombstoned
//   | offsets        | a (40), b (5)                  | cleaner past delete retention  | no key of a in the log      | CompactionDropsTombstones
//   | both           | a, b                           | leader tombstones, follower up | follower rebuild = leader   | FollowerConverges
//   | both           | a                              | tombstone, re-commit, restart  | the new commit lives        | RecommitAfterTombstone
#include <rune/coordination/NsScopedId.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

namespace co = rune::coordination;
namespace tx = rune::transaction;
namespace rr = rune::replication;
namespace fs = std::filesystem;

namespace {

constexpr int kCap = 2000;   // hard cap on every drive loop

struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        static std::atomic<uint64_t> seq{0};
        path = fs::temp_directory_path().string() + "/rune_nstomb_" + std::string(tag) + "_" + std::to_string(::getpid()) +
               "_" + std::to_string(seq.fetch_add(1));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

class NullTransport : public rr::PartitionTransport {
public:
    bool sendFetchRequest(const rr::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rr::NodeId, const rr::FetchResponseMsg&) override { return true; }
};

class DummyAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

/// A single-node leader replica over a temp log.
struct Leader {
    TempDir dir;
    rune::storage::Log log;
    NullTransport transport;
    std::unique_ptr<rr::PartitionReplica> replica;
    int64_t clock = 0;
    explicit Leader(std::string_view tag, int64_t rollBytes = 64 * 1024 * 1024, int64_t deleteRetentionMs = 86'400'000)
        : dir(tag), log(dir.path, rollBytes) {
        rr::PartitionConfig cfg;
        cfg.topicId = 301; cfg.partitionId = 0; cfg.selfId = 1; cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.minIsr = 1; cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        cfg.cleanupPolicy = rr::CleanupPolicy::Compact;
        cfg.minCleanableDirtyRatio = 0.0;
        cfg.deleteRetentionMs = deleteRetentionMs;
        replica = std::make_unique<rr::PartitionReplica>(cfg, rr::Role::Leader, 1, std::vector<rr::NodeId>{1},
                                                         std::vector<rr::NodeId>{1}, &log, &transport);
        replica->becomeLeader(1, {1});
    }
    /// Pumps until the high watermark reaches the log end.
    bool pumpToEnd() {
        const rr::Offset target = replica->logEndOffset();
        for (int i = 0; i < kCap && replica->highWatermark() < target; ++i) { clock += 1'000'000LL; replica->doWork(clock); }
        return replica->highWatermark() >= target;
    }
};

co::TopicPartition tp(std::string topic = "t", int32_t p = 0) { return co::TopicPartition{std::move(topic), p}; }

tx::TransactionMetadata metaOf(const std::string& id, tx::ProducerId pid) {
    tx::TransactionMetadata m;
    m.txnId = id; m.producerId = pid; m.producerEpoch = 0; m.state = tx::TxnState::Empty;
    return m;
}

/// Calls tombstoneNamespace until it returns true (hard cap).  Returns the number of calls that returned false, or -1
/// when the cap was hit.
int drain(const std::function<bool()>& call) {
    int falses = 0;
    for (int i = 0; i < kCap; ++i) {
        if (call()) return falses;
        ++falses;
    }
    return -1;
}
int ceilDiv(int n, int d) { return (n + d - 1) / d; }

std::string gid(uint32_t ns, int i) { return co::scopedId(ns, "g" + std::to_string(i)); }
std::string tid(uint32_t ns, int i) { return co::scopedId(ns, "x" + std::to_string(i)); }

void fillOffsets(co::ReplicatedOffsetStore& s, uint32_t ns, int n) {
    for (int i = 0; i < n; ++i) ASSERT_TRUE(s.commit(gid(ns, i), tp(), 100 + i, "", 1'000 + i)) << ns << "/" << i;
}
void fillTxns(tx::ReplicatedTxnStateStore& s, uint32_t ns, int n) {
    for (int i = 0; i < n; ++i) ASSERT_TRUE(s.put(metaOf(tid(ns, i), 1'000 * ns + i), 1)) << ns << "/" << i;
}
int liveOffsets(const co::ReplicatedOffsetStore& s, uint32_t ns, int n) {
    int live = 0;
    for (int i = 0; i < n; ++i) live += s.fetch(gid(ns, i), tp()).has_value() ? 1 : 0;
    return live;
}
int liveTxns(const tx::ReplicatedTxnStateStore& s, uint32_t ns, int n) {
    int live = 0;
    for (int i = 0; i < n; ++i) live += s.get(tid(ns, i)) != nullptr ? 1 : 0;
    return live;
}

/// Every (key, value) record of the log, compaction-aware (walks the surviving segments).
std::vector<std::pair<std::string, std::string>> allRecords(rune::storage::Log& log) {
    std::vector<std::pair<std::string, std::string>> out;
    const auto bases = log.segmentBaseOffsets();
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        const int64_t segEnd = si + 1 < bases.size() ? bases[si + 1] : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
        if (cursor < bases[si]) cursor = bases[si];
        for (int guard = 0; guard < 100000; ++guard) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                out.emplace_back(std::string(k), std::string(v));
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return out;
}

/// The group id inside an offset record key (groupLen u16 | group | ...).
std::string groupOfKey(const std::string& key) {
    if (key.size() < 2) return {};
    const size_t len = static_cast<uint8_t>(key[0]) | (static_cast<size_t>(static_cast<uint8_t>(key[1])) << 8);
    return key.size() >= 2 + len ? key.substr(2, len) : std::string{};
}

}  // namespace

// ── offsets ───────────────────────────────────────────────────────────────────────────────────────────────────
// Mutation: rebuildFromLog / applyRecord ignores a null value -> this test fails (a's 600 keys come back).
TEST(NamespaceTombstoneRegression, OffsetsGoneAfterRebuildRegression) {
    Leader l("off");
    co::ReplicatedOffsetStore store(l.replica.get());
    fillOffsets(store, 1, 600);
    fillOffsets(store, 2, 10);
    fillOffsets(store, 0, 3);
    ASSERT_TRUE(l.pumpToEnd());
    const rr::Offset leoBefore = l.replica->logEndOffset();
    const int falses = drain([&] { return store.tombstoneNamespace(1, 256, 9'000); });
    EXPECT_EQ(falses, 3) << "600 keys in batches of 256: 256 + 256 + 88, then the call that finds none";
    EXPECT_EQ(l.replica->logEndOffset(), leoBefore + 600) << "exactly one tombstone per live key";
    EXPECT_EQ(liveOffsets(store, 1, 600), 0);
    EXPECT_EQ(liveOffsets(store, 2, 10), 10);
    ASSERT_TRUE(l.pumpToEnd());
    co::ReplicatedOffsetStore fresh(l.replica.get());
    ASSERT_TRUE(fresh.rebuildFromLog());
    EXPECT_EQ(liveOffsets(fresh, 1, 600), 0) << "a's offsets came back after the rebuild";
    EXPECT_EQ(liveOffsets(fresh, 2, 10), 10);
    EXPECT_EQ(liveOffsets(fresh, 0, 3), 3);
    for (int i = 0; i < 600; i += 97) EXPECT_TRUE(fresh.fetchAll(gid(1, i)).empty());
    // A call after completion writes nothing.
    EXPECT_TRUE(store.tombstoneNamespace(1, 256, 9'001));
    EXPECT_EQ(l.replica->logEndOffset(), leoBefore + 600);
}

TEST(NamespaceTombstoneRegression, TxnStateGoneAfterRebuildRegression) {
    Leader l("txn");
    tx::ReplicatedTxnStateStore store(l.replica.get());
    fillTxns(store, 1, 600);
    fillTxns(store, 2, 10);
    fillTxns(store, 0, 3);
    ASSERT_TRUE(l.pumpToEnd());
    const rr::Offset leoBefore = l.replica->logEndOffset();
    const int falses = drain([&] { return store.tombstoneNamespace(1, 256, 9'000); });
    EXPECT_EQ(falses, 3);
    EXPECT_EQ(l.replica->logEndOffset(), leoBefore + 600);
    EXPECT_EQ(liveTxns(store, 1, 600), 0);
    ASSERT_TRUE(l.pumpToEnd());
    tx::ReplicatedTxnStateStore fresh(l.replica.get());
    fresh.rebuildFromLog();
    EXPECT_EQ(liveTxns(fresh, 1, 600), 0) << "a's transaction state came back after the rebuild";
    EXPECT_EQ(liveTxns(fresh, 2, 10), 10);
    EXPECT_EQ(liveTxns(fresh, 0, 3), 3);
    EXPECT_EQ(fresh.all().size(), 13u);
    EXPECT_TRUE(store.tombstoneNamespace(1, 256, 9'001));
    EXPECT_EQ(l.replica->logEndOffset(), leoBefore + 600);
}

TEST(NamespaceTombstoneRegression, OffsetStoreLiveKeysOfNamespaceCountsOnlyThatNamespaceRegression) {
    {
        Leader l("livekeys_off");
        co::ReplicatedOffsetStore store(l.replica.get());
        fillOffsets(store, 1, 7);
        fillOffsets(store, 2, 3);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(1), 7u);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(2), 3u);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(9), 0u);
        const rr::Offset leo = l.replica->logEndOffset();
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(1), 7u);
        EXPECT_EQ(l.replica->logEndOffset(), leo) << "counting writes nothing";
        ASSERT_GE(drain([&] { return store.tombstoneNamespace(1, 4, 9'000); }), 0);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(1), 0u);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(2), 3u);
    }
}

TEST(NamespaceTombstoneRegression, TxnStoreLiveKeysOfNamespaceCountsOnlyThatNamespaceRegression) {
    {
        Leader l("livekeys_txn");
        tx::ReplicatedTxnStateStore store(l.replica.get());
        fillTxns(store, 1, 7);
        fillTxns(store, 2, 3);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(1), 7u);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(2), 3u);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(9), 0u);
        const rr::Offset leo = l.replica->logEndOffset();
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(1), 7u);
        EXPECT_EQ(l.replica->logEndOffset(), leo) << "counting writes nothing";
        ASSERT_GE(drain([&] { return store.tombstoneNamespace(1, 4, 9'000); }), 0);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(1), 0u);
        EXPECT_EQ(store.liveKeysOfNamespaceForTest(2), 3u);
    }
}

// ── batch bounds ──────────────────────────────────────────────────────────────────────────────────────────────
TEST(NamespaceTombstoneRegression, BatchBoundsRegression) {
    int cases = 0;
    for (const int batch : {256, 7, 1}) {
        for (const int n : {0, 1, 255, 256, 257}) {
            if (batch == 1 && n > 257) continue;
            SCOPED_TRACE("batch " + std::to_string(batch) + " n " + std::to_string(n));
            {
                Leader l("bb_off");
                co::ReplicatedOffsetStore s(l.replica.get());
                fillOffsets(s, 1, n);
                fillOffsets(s, 2, 3);
                const rr::Offset leo = l.replica->logEndOffset();
                EXPECT_EQ(drain([&] { return s.tombstoneNamespace(1, static_cast<uint32_t>(batch), 0); }), ceilDiv(n, batch));
                EXPECT_EQ(l.replica->logEndOffset(), leo + n);
                EXPECT_EQ(liveOffsets(s, 1, n), 0);
                EXPECT_EQ(liveOffsets(s, 2, 3), 3);
                if (n == 0) EXPECT_TRUE(s.tombstoneNamespace(1, static_cast<uint32_t>(batch), 0)) << "n = 0: the first call is true";
            }
            {
                Leader l("bb_txn");
                tx::ReplicatedTxnStateStore s(l.replica.get());
                fillTxns(s, 1, n);
                fillTxns(s, 2, 3);
                const rr::Offset leo = l.replica->logEndOffset();
                EXPECT_EQ(drain([&] { return s.tombstoneNamespace(1, static_cast<uint32_t>(batch), 0); }), ceilDiv(n, batch));
                EXPECT_EQ(l.replica->logEndOffset(), leo + n);
                EXPECT_EQ(liveTxns(s, 1, n), 0);
                EXPECT_EQ(liveTxns(s, 2, 3), 3);
            }
            ++cases;
        }
    }
    EXPECT_EQ(cases, 15);
}

TEST(NamespaceTombstoneRegression, DefaultNeverTombstonedRegression) {
    Leader l("def");
    co::ReplicatedOffsetStore o(l.replica.get());
    fillOffsets(o, 0, 5);
    const rr::Offset leo = l.replica->logEndOffset();
    EXPECT_TRUE(o.tombstoneNamespace(0, 256, 0)) << "nothing to do for default";
    EXPECT_EQ(l.replica->logEndOffset(), leo);
    EXPECT_EQ(liveOffsets(o, 0, 5), 5);
    Leader l2("def2");
    tx::ReplicatedTxnStateStore t(l2.replica.get());
    fillTxns(t, 0, 5);
    const rr::Offset leo2 = l2.replica->logEndOffset();
    EXPECT_TRUE(t.tombstoneNamespace(0, 256, 0));
    EXPECT_EQ(l2.replica->logEndOffset(), leo2);
    EXPECT_EQ(liveTxns(t, 0, 5), 5);
}

// A key committed again after its namespace was tombstoned (the id is a NEW nsId in practice; the same scoped id is
// the worst case) is live again, also after a restart: last write wins over the tombstone.
TEST(NamespaceTombstoneRegression, RecommitAfterTombstoneRegression) {
    Leader l("recommit");
    co::ReplicatedOffsetStore s(l.replica.get());
    fillOffsets(s, 1, 4);
    ASSERT_EQ(drain([&] { return s.tombstoneNamespace(1, 256, 0); }), 1);
    ASSERT_TRUE(s.commit(gid(1, 2), tp(), 777, "", 5'000));
    ASSERT_TRUE(l.pumpToEnd());
    co::ReplicatedOffsetStore fresh(l.replica.get());
    ASSERT_TRUE(fresh.rebuildFromLog());
    EXPECT_EQ(liveOffsets(fresh, 1, 4), 1);
    ASSERT_TRUE(fresh.fetch(gid(1, 2), tp()).has_value());
    EXPECT_EQ(fresh.fetch(gid(1, 2), tp())->offset, 777);
}

// A group with several partitions: every (topic, partition) key is tombstoned, none is missed.
TEST(NamespaceTombstoneRegression, MultiPartitionGroupFullyTombstonedRegression) {
    Leader l("multi");
    co::ReplicatedOffsetStore s(l.replica.get());
    for (int p = 0; p < 40; ++p) ASSERT_TRUE(s.commit(co::scopedId(1, "g"), tp("t" + std::to_string(p % 3), p), p, "", p));
    ASSERT_TRUE(s.commit(co::scopedId(2, "g"), tp("t0", 0), 1, "", 1));
    EXPECT_EQ(drain([&] { return s.tombstoneNamespace(1, 16, 0); }), 3) << "40 keys in batches of 16";
    EXPECT_TRUE(s.fetchAll(co::scopedId(1, "g")).empty());
    ASSERT_TRUE(l.pumpToEnd());
    co::ReplicatedOffsetStore fresh(l.replica.get());
    ASSERT_TRUE(fresh.rebuildFromLog());
    EXPECT_TRUE(fresh.fetchAll(co::scopedId(1, "g")).empty());
    EXPECT_EQ(fresh.fetchAll(co::scopedId(2, "g")).size(), 1u);
}

// ── compaction ────────────────────────────────────────────────────────────────────────────────────────────────
// Once the cleaner runs past the delete retention (1 ms here, the cleaner's clock far ahead) no record of namespace a's
// groups is left in the log: neither the offsets nor their tombstones.  b's survive.
TEST(NamespaceTombstoneRegression, CompactionDropsTombstonesRegression) {
    Leader l("compact", /*rollBytes=*/256, /*deleteRetentionMs=*/1);
    co::ReplicatedOffsetStore s(l.replica.get());
    fillOffsets(s, 1, 40);
    fillOffsets(s, 2, 5);
    ASSERT_EQ(drain([&] { return s.tombstoneNamespace(1, 256, 0); }), 1);
    // Noise: one key overwritten many times, so the log rolls into sealed segments the cleaner can work on.
    for (int round = 0; round < 80; ++round) ASSERT_TRUE(s.commit(co::scopedId(3, "noise"), tp("n"), round, "", round));
    l.replica->onLeaderLogAppended();
    ASSERT_TRUE(l.pumpToEnd());
    ASSERT_GE(l.log.segmentBaseOffsets().size(), 2u) << "the log must have sealed segments to compact";
    const auto countNs = [&](uint32_t ns) {
        int n = 0;
        for (const auto& [k, v] : allRecords(l.log)) n += co::parseScopedId(groupOfKey(k)).nsId == ns && !groupOfKey(k).empty() ? 1 : 0;
        return n;
    };
    ASSERT_GT(countNs(1), 0) << "guard: before compaction the log holds a's keys and tombstones";

    rune::node::ReplicaRegistry registry;
    rr::PartitionReplica* raw = l.replica.get();
    registry.add(301, 0, std::move(l.replica));
    DummyAgent dummy;
    rune::node::NodeRuntimeAgent agent(dummy, nullptr, nullptr, nullptr, &registry, nullptr);
    for (int i = 0; i < 80; ++i) agent.runBoundedCompaction(1'000'000'000LL);
    l.log.reloadSegments();

    EXPECT_EQ(countNs(1), 0) << "compaction must remove a's keys AND their expired tombstones from the log";
    EXPECT_EQ(countNs(2), 5) << "b's offsets are untouched";
    co::ReplicatedOffsetStore fresh(raw);
    ASSERT_TRUE(fresh.rebuildFromLog());
    EXPECT_EQ(liveOffsets(fresh, 1, 40), 0);
    EXPECT_EQ(liveOffsets(fresh, 2, 5), 5);
    l.replica.release();   // owned by the registry now
}

// ── followers ─────────────────────────────────────────────────────────────────────────────────────────────────
namespace {
/// Three in-process nodes over InMemoryTransport (the OffsetReplicationTest rig): node 1 leads, minIsr 2.
struct Trio {
    TempDir d1{"t1"}, d2{"t2"}, d3{"t3"};
    rune::storage::Log log1{d1.path}, log2{d2.path}, log3{d3.path};
    rr::InMemoryTransport transport;
    std::unique_ptr<rr::PartitionReplica> n1, n2, n3;
    int64_t clock = 0;
    static rr::PartitionConfig cfg(rr::NodeId self) {
        rr::PartitionConfig c;
        c.partitionId = 0; c.selfId = self; c.replicaLagMaxMs = 10'000; c.replicaLagMaxOffset = 10'000; c.minIsr = 2;
        c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000LL; c.maxFetchBytes = 1 << 20;
        c.pushOnAppend = true; c.fastAckOnApply = true;
        return c;
    }
    Trio() {
        const std::vector<rr::NodeId> all{1, 2, 3};
        n1 = std::make_unique<rr::PartitionReplica>(cfg(1), rr::Role::Leader, 1, all, all, &log1, &transport);
        n2 = std::make_unique<rr::PartitionReplica>(cfg(2), rr::Role::Follower, 1, all, all, &log2, &transport);
        n3 = std::make_unique<rr::PartitionReplica>(cfg(3), rr::Role::Follower, 1, all, all, &log3, &transport);
        transport.setLeader(n1.get());
        transport.addFollower(2, n2.get());
        transport.addFollower(3, n3.get());
    }
    /// Drives until every node's high watermark reached the leader's log end.
    bool settle() {
        for (int i = 0; i < kCap; ++i) {
            const rr::Offset leo = n1->logEndOffset();
            if (n1->highWatermark() >= leo && n2->highWatermark() >= leo && n3->highWatermark() >= leo) return true;
            clock += 1'000'000LL;
            n1->doWork(clock); n2->doWork(clock); n3->doWork(clock);
            transport.pump(clock);
        }
        return false;
    }
};
}  // namespace

TEST(NamespaceTombstoneRegression, FollowerConvergesOffsetsRegression) {
    Trio t;
    co::ReplicatedOffsetStore leader(t.n1.get());
    fillOffsets(leader, 1, 300);
    fillOffsets(leader, 2, 7);
    ASSERT_TRUE(t.settle());
    ASSERT_EQ(drain([&] { return leader.tombstoneNamespace(1, 256, 0); }), 2);
    ASSERT_TRUE(t.settle());
    // The leader dies; each follower, promoted in turn, rebuilds the same state from ITS OWN log.
    for (rr::PartitionReplica* f : {t.n2.get(), t.n3.get()}) {
        co::ReplicatedOffsetStore rebuilt(f);
        ASSERT_TRUE(rebuilt.rebuildFromLog()) << "the follower's high watermark reached its log end";
        EXPECT_EQ(liveOffsets(rebuilt, 1, 300), 0) << "a follower still holds a deleted namespace's offsets";
        EXPECT_EQ(liveOffsets(rebuilt, 2, 7), 7);
    }
}

TEST(NamespaceTombstoneRegression, FollowerConvergesTxnStateRegression) {
    Trio t;
    tx::ReplicatedTxnStateStore leader(t.n1.get());
    fillTxns(leader, 1, 300);
    fillTxns(leader, 2, 7);
    ASSERT_TRUE(t.settle());
    ASSERT_EQ(drain([&] { return leader.tombstoneNamespace(1, 256, 0); }), 2);
    ASSERT_TRUE(t.settle());
    for (rr::PartitionReplica* f : {t.n2.get(), t.n3.get()}) {
        tx::ReplicatedTxnStateStore rebuilt(f);
        rebuilt.rebuildFromLog();
        EXPECT_EQ(liveTxns(rebuilt, 1, 300), 0);
        EXPECT_EQ(liveTxns(rebuilt, 2, 7), 7);
    }
}
