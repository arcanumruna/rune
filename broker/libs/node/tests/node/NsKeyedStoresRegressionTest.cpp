// NsKeyedStoresRegressionTest.cpp - namespaces Phase 2 Task 5 (D19): group ids and transactional ids are
// namespace-scoped, and groups and transactions work in every namespace.
//
//   * NsScopedId (rune/coordination/NsScopedId.h): nsId 0 is the identity (the v1 bytes), any other namespace is
//     0x1F + u32 nsId (LE) + the local id.
//   * The offset store and the txn-state store keyed by the scoped id: two namespaces keep their own (group, topic)
//     offsets and their own transactional ids, across a restart (rebuild from the log), with a v1 key (today's
//     bytes) still decoding as `default`, and a record cut at ANY offset never yielding an entry.
//   * TransactionCoordinator::abortAllForNamespace (Task 4 calls it on suspend / delete).
//   * A client id that starts with the reserved mark is refused InvalidRequest (39).
//   * A REAL client against a REAL authenticating broker (default config): a consumer group and a transactional
//     producer in a named namespace, and the same ids in another namespace independent of them.
//
// Interaction matrix (operation x namespace pair x timing):
//   | operation                    | namespaces            | timing                    | expected              | test
//   |------------------------------|-----------------------|---------------------------|-----------------------|-----
//   | scopedId / parseScopedId     | 0, 1, 2^32-2, cut ids | -                         | round trip, cut = ns 0| NsScopedId
//   | offset commit + fetch        | a, b, same group/topic| sequential                | own offset            | SameGroupTwoNamespaces
//   | offset rebuild              | a, b, default         | restart                   | same                  | SurvivesRestart
//   | offset key                   | default (v1)          | restart                   | v1 bytes, ns 0        | V1KeyDecodesAsDefault
//   | offset record cut            | a                     | every offset             | no entry, no crash    | KeyTruncatedAtEveryOffset
//   | InitProducerId x2           | a, b, same txn id     | back-to-back              | b untouched           | SameTransactionalId...
//   | txn state rebuild           | a, b, default         | restart                   | same                  | SurvivesRestart
//   | abortAllForNamespace        | a (2), b, default     | Ongoing                   | only a's abort        | AbortAllForNamespace...
//   | JoinGroup / InitProducerId   | reserved prefix       | sequential                | 39                    | ReservedPrefixRefused
//   | group join/commit/fetch      | a, b, same group id   | restart of the consumer   | each its own offset   | NsGroups
//   | transactions                 | a, b, same txn id     | b init between a's txns   | a not fenced          | NsTxns
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/coordination/NsScopedId.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/TransactionCoordinator.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
namespace co = rune::coordination;
namespace tx = rune::transaction;
namespace rr = rune::replication;
using namespace std::chrono_literals;

namespace {

// ── a single-node leader replica over a temp log (the HW advances on every append) ──────────────────────────────
struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        static std::atomic<uint64_t> seq{0};
        path = std::filesystem::temp_directory_path().string() + "/rune_nskeyed_" + std::string(tag) + "_" +
               std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1));
        std::filesystem::create_directories(path);
    }
    ~TempDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

class NullTransport : public rr::PartitionTransport {
public:
    bool sendFetchRequest(const rr::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rr::NodeId, const rr::FetchResponseMsg&) override { return true; }
};

struct Leader {
    TempDir dir;
    rune::storage::Log log;
    NullTransport transport;
    std::unique_ptr<rr::PartitionReplica> replica;
    int64_t clock = 0;
    explicit Leader(std::string_view tag) : dir(tag), log(dir.path) {
        rr::PartitionConfig cfg;
        cfg.partitionId = 0; cfg.selfId = 1; cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.minIsr = 1; cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        replica = std::make_unique<rr::PartitionReplica>(cfg, rr::Role::Leader, 1, std::vector<rr::NodeId>{1},
                                                         std::vector<rr::NodeId>{1}, &log, &transport);
    }
    bool pumpHW(rr::Offset target) {
        for (int i = 0; i < 600 && replica->highWatermark() < target; ++i) { clock += 1'000'000LL; replica->doWork(clock); }
        return replica->highWatermark() >= target;
    }
    /// Every (key, value) record of the log, in order.
    std::vector<std::pair<std::string, std::string>> records() {
        std::vector<std::pair<std::string, std::string>> out;
        const std::vector<uint8_t> data = replica->read(0, 64 * 1024 * 1024);
        std::size_t pos = 0;
        while (pos < data.size()) {
            rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
            if (!reader.ensureValid() || reader.totalLength() <= 0) break;
            reader.forEachRecord([&](std::string_view k, std::string_view v, int64_t, int64_t) {
                out.emplace_back(std::string(k), std::string(v));
            });
            pos += static_cast<std::size_t>(reader.totalLength());
        }
        return out;
    }
};

co::TopicPartition tp(std::string topic, int32_t p = 0) { return co::TopicPartition{std::move(topic), p}; }

}  // namespace

// ── NsScopedId ────────────────────────────────────────────────────────────────────────────────────────────────
TEST(NsScopedIdRegression, RoundTripAndDefaultIsIdentityRegression) {
    EXPECT_EQ(co::scopedId(0, "g"), "g") << "default keeps the v1 bytes";
    EXPECT_EQ(co::scopedId(0, ""), "");
    const std::string s7 = co::scopedId(7, "g");
    EXPECT_EQ(s7, std::string("\x1F\x07\x00\x00\x00g", 6));
    const co::ScopedIdView v7 = co::parseScopedId(s7);
    EXPECT_EQ(v7.nsId, 7u);
    EXPECT_EQ(v7.local, "g");
    const co::ScopedIdView vMax = co::parseScopedId(co::scopedId(0xFFFFFFFEu, "grp"));
    EXPECT_EQ(vMax.nsId, 0xFFFFFFFEu);
    EXPECT_EQ(vMax.local, "grp");
    const co::ScopedIdView vEmpty = co::parseScopedId(co::scopedId(3, ""));
    EXPECT_EQ(vEmpty.nsId, 3u);
    EXPECT_EQ(vEmpty.local, "");
    // Little-endian: nsId 0x01020304.
    EXPECT_EQ(co::scopedId(0x01020304u, "x"), std::string("\x1F\x04\x03\x02\x01x", 6));
    // A default id parses as itself; every prefix of a scoped id shorter than the mark + u32 is nsId 0 (never produced).
    const co::ScopedIdView vd = co::parseScopedId("plain");
    EXPECT_EQ(vd.nsId, 0u);
    EXPECT_EQ(vd.local, "plain");
    for (std::size_t n = 0; n < co::kScopedIdPrefix; ++n) {
        const std::string cut = s7.substr(0, n);
        const co::ScopedIdView c = co::parseScopedId(cut);
        EXPECT_EQ(c.nsId, 0u) << "prefix " << n;
        EXPECT_EQ(c.local, cut) << "prefix " << n;
    }
    // The reserved mark: only ids that start with it.
    EXPECT_TRUE(co::reservedClientId("\x1F"));
    EXPECT_TRUE(co::reservedClientId(s7));
    EXPECT_FALSE(co::reservedClientId(""));
    EXPECT_FALSE(co::reservedClientId("g\x1F"));
    // Different namespaces never collide, whatever the local id.
    EXPECT_NE(co::scopedId(1, "g"), co::scopedId(2, "g"));
    EXPECT_NE(co::scopedId(1, "g"), co::scopedId(0, "g"));
}

// ── the offset store ──────────────────────────────────────────────────────────────────────────────────────────
struct NsKeyedOffsets : ::testing::Test {
    Leader l{"off"};
    co::ReplicatedOffsetStore store{l.replica.get()};
};

TEST_F(NsKeyedOffsets, SameGroupTwoNamespacesIndependentRegression) {
    const std::string ga = co::scopedId(1, "g"), gb = co::scopedId(2, "g");
    ASSERT_TRUE(store.commit(ga, tp("t"), 5, "", 1'000));
    ASSERT_TRUE(store.commit(gb, tp("t"), 9, "", 2'000));
    ASSERT_TRUE(store.commit("g", tp("t"), 3, "", 3'000));   // default
    ASSERT_TRUE(l.pumpHW(3));
    ASSERT_TRUE(store.fetch(ga, tp("t")).has_value());
    ASSERT_TRUE(store.fetch(gb, tp("t")).has_value());
    ASSERT_TRUE(store.fetch("g", tp("t")).has_value());
    EXPECT_EQ(store.fetch(ga, tp("t"))->offset, 5);
    EXPECT_EQ(store.fetch(gb, tp("t"))->offset, 9);
    EXPECT_EQ(store.fetch("g", tp("t"))->offset, 3);
    EXPECT_EQ(store.fetchAll(ga).size(), 1u);
    EXPECT_EQ(store.fetchAll(gb).size(), 1u);
    EXPECT_FALSE(store.fetch(co::scopedId(3, "g"), tp("t")).has_value()) << "a third namespace has none";
}

TEST_F(NsKeyedOffsets, SurvivesRestartAndRebuildRegression) {
    const std::string ga = co::scopedId(1, "g"), gb = co::scopedId(2, "g");
    ASSERT_TRUE(store.commit(ga, tp("t"), 5, "ma", 1'000));
    ASSERT_TRUE(store.commit(gb, tp("t"), 9, "mb", 2'000));
    ASSERT_TRUE(store.commit(ga, tp("t"), 6, "ma2", 3'000));   // last write wins within a namespace
    ASSERT_TRUE(store.commit("g", tp("t"), 1, "md", 4'000));
    ASSERT_TRUE(l.pumpHW(4));
    co::ReplicatedOffsetStore fresh(l.replica.get());
    ASSERT_TRUE(fresh.rebuildFromLog());
    ASSERT_TRUE(fresh.fetch(ga, tp("t")).has_value());
    EXPECT_EQ(fresh.fetch(ga, tp("t"))->offset, 6);
    EXPECT_EQ(fresh.fetch(ga, tp("t"))->metadata, "ma2");
    ASSERT_TRUE(fresh.fetch(gb, tp("t")).has_value());
    EXPECT_EQ(fresh.fetch(gb, tp("t"))->offset, 9);
    ASSERT_TRUE(fresh.fetch("g", tp("t")).has_value());
    EXPECT_EQ(fresh.fetch("g", tp("t"))->offset, 1);
}

TEST_F(NsKeyedOffsets, V1KeyDecodesAsDefaultRegression) {
    ASSERT_TRUE(store.commit("g", tp("t"), 42, "m", 1'000));
    ASSERT_TRUE(store.commit(co::scopedId(7, "g"), tp("t"), 43, "m", 2'000));
    ASSERT_TRUE(l.pumpHW(2));
    const auto recs = l.records();
    ASSERT_EQ(recs.size(), 2u);
    // The pre-change key layout: groupLen u16 | group | topicLen u16 | topic | partition i32 (all little-endian).
    EXPECT_EQ(recs[0].first, std::string("\x01\x00g\x01\x00t\x00\x00\x00\x00", 10)) << "a default group's key bytes changed";
    // A namespace group's key is the same layout over the scoped id (6 bytes).
    EXPECT_EQ(recs[1].first, std::string("\x06\x00\x1F\x07\x00\x00\x00g\x01\x00t\x00\x00\x00\x00", 15));
    co::ReplicatedOffsetStore fresh(l.replica.get());
    ASSERT_TRUE(fresh.rebuildFromLog());
    ASSERT_TRUE(fresh.fetch("g", tp("t")).has_value()) << "a v1 record rebuilds as nsId 0";
    EXPECT_EQ(fresh.fetch("g", tp("t"))->offset, 42);
    EXPECT_EQ(co::parseScopedId("g").nsId, 0u);
    EXPECT_EQ(fresh.fetch(co::scopedId(7, "g"), tp("t"))->offset, 43);
}

/// A record whose payload is cut at every offset is skipped on rebuild: no crash, no entry for the cut record, and
/// the good records before it still load.  (The store has no way to take a malformed record through commit(), so
/// the cut payloads are appended raw through the replica, exactly as a torn write would leave them.)
TEST_F(NsKeyedOffsets, KeyTruncatedAtEveryOffsetRegression) {
    const std::string gx = co::scopedId(5, "cut");
    // The payload commit() would write for (gx, t/0) = 11: keyLen | key | valLen | val.
    std::vector<uint8_t> payload;
    const auto le16 = [&](uint16_t v) { payload.push_back(static_cast<uint8_t>(v)); payload.push_back(static_cast<uint8_t>(v >> 8)); };
    std::vector<uint8_t> key;
    const auto keyLe16 = [&](uint16_t v) { key.push_back(static_cast<uint8_t>(v)); key.push_back(static_cast<uint8_t>(v >> 8)); };
    keyLe16(static_cast<uint16_t>(gx.size())); key.insert(key.end(), gx.begin(), gx.end());
    keyLe16(1); key.push_back('t'); for (int i = 0; i < 4; ++i) key.push_back(0);
    std::vector<uint8_t> val;
    for (int i = 0; i < 8; ++i) val.push_back(i == 0 ? 11 : 0);   // offset 11
    for (int i = 0; i < 8; ++i) val.push_back(0);                 // tsNs
    val.push_back(0); val.push_back(0);                           // metaLen 0
    le16(static_cast<uint16_t>(key.size())); payload.insert(payload.end(), key.begin(), key.end());
    le16(static_cast<uint16_t>(val.size())); payload.insert(payload.end(), val.begin(), val.end());

    ASSERT_TRUE(store.commit(co::scopedId(1, "good"), tp("t"), 7, "", 1'000));
    for (std::size_t cut = 0; cut < payload.size(); ++cut) {
        ASSERT_GE(l.replica->appendKeyed(std::span<const uint8_t>{}, std::span<const uint8_t>(payload.data(), cut), true), 0)
            << "cut " << cut;
    }
    ASSERT_TRUE(l.pumpHW(1 + static_cast<rr::Offset>(payload.size())));
    co::ReplicatedOffsetStore fresh(l.replica.get());
    ASSERT_TRUE(fresh.rebuildFromLog());
    EXPECT_FALSE(fresh.fetch(gx, tp("t")).has_value()) << "a cut record produced an entry";
    EXPECT_TRUE(fresh.fetchAll(gx).empty());
    ASSERT_TRUE(fresh.fetch(co::scopedId(1, "good"), tp("t")).has_value()) << "the good record before the cuts";
    EXPECT_EQ(fresh.fetch(co::scopedId(1, "good"), tp("t"))->offset, 7);
    // The whole payload does decode (control: the cut test is not vacuous).
    ASSERT_GE(l.replica->appendKeyed(std::span<const uint8_t>{}, std::span<const uint8_t>(payload.data(), payload.size()), true), 0);
    ASSERT_TRUE(l.pumpHW(2 + static_cast<rr::Offset>(payload.size())));
    co::ReplicatedOffsetStore whole(l.replica.get());
    ASSERT_TRUE(whole.rebuildFromLog());
    ASSERT_TRUE(whole.fetch(gx, tp("t")).has_value());
    EXPECT_EQ(whole.fetch(gx, tp("t"))->offset, 11);
}

// ── the txn-state store and the coordinator ───────────────────────────────────────────────────────────────────
namespace {
tx::TransactionMetadata metaOf(const std::string& id, tx::ProducerId pid, tx::ProducerEpoch epoch, tx::TxnState st) {
    tx::TransactionMetadata m;
    m.txnId = id; m.producerId = pid; m.producerEpoch = epoch; m.state = st; m.txnTimeoutMs = 30'000;
    m.txnStartNs = st == tx::TxnState::Ongoing ? 1'000'000 : 0;
    return m;
}
}  // namespace

TEST(NsKeyedTxnStateRegression, SameTransactionalIdTwoNamespacesRegression) {
    tx::TransactionCoordinator coord;
    const std::string ta = co::scopedId(1, "t"), tb = co::scopedId(2, "t");
    const auto b0 = coord.initProducerId(tb, 30'000, 1'000);
    ASSERT_TRUE(b0.ok());
    const auto a0 = coord.initProducerId(ta, 30'000, 2'000);
    ASSERT_TRUE(a0.ok());
    EXPECT_NE(a0.producerId, b0.producerId) << "two namespaces' `t` are two transactional ids";
    const auto a1 = coord.initProducerId(ta, 30'000, 3'000);   // re-init in a: epoch bump / fence
    ASSERT_TRUE(a1.ok());
    EXPECT_EQ(a1.producerId, a0.producerId);
    EXPECT_EQ(a1.producerEpoch, a0.producerEpoch + 1) << "a's re-init bumps a's epoch";
    const auto a2 = coord.initProducerId(ta, 30'000, 4'000);
    ASSERT_TRUE(a2.ok());
    const auto* mb = coord.metadata(tb);
    ASSERT_NE(mb, nullptr);
    EXPECT_EQ(mb->producerId, b0.producerId) << "a's fencing touched b's producer id";
    EXPECT_EQ(mb->producerEpoch, b0.producerEpoch) << "a's fencing bumped b's epoch";
    // b's own producer is still the current one: its AddPartitions is not fenced.
    const auto add = coord.addPartitions(tb, b0.producerId, b0.producerEpoch, {tx::TopicPartition{"x", 0}}, 5'000);
    EXPECT_TRUE(add.ok()) << "b was fenced by a's re-init";
    // And a's first epoch IS fenced.
    const auto stale = coord.addPartitions(ta, a0.producerId, a0.producerEpoch, {tx::TopicPartition{"x", 0}}, 5'000);
    EXPECT_FALSE(stale.ok());
}

struct NsKeyedTxn : ::testing::Test {
    Leader l{"txn"};
    tx::ReplicatedTxnStateStore store{l.replica.get()};
};

TEST_F(NsKeyedTxn, SurvivesRestartAndRebuildRegression) {
    const std::string ta = co::scopedId(1, "t"), tb = co::scopedId(2, "t");
    ASSERT_TRUE(store.put(metaOf(ta, 10, 3, tx::TxnState::Empty), 1));
    ASSERT_TRUE(store.put(metaOf(tb, 20, 0, tx::TxnState::Ongoing), 1));
    ASSERT_TRUE(store.put(metaOf("t", 30, 1, tx::TxnState::CompleteCommit), 1));
    ASSERT_TRUE(store.put(metaOf(ta, 10, 4, tx::TxnState::Empty), 1));   // last write wins within a namespace
    ASSERT_TRUE(l.pumpHW(4));
    tx::ReplicatedTxnStateStore fresh(l.replica.get());
    fresh.rebuildFromLog();
    ASSERT_NE(fresh.get(ta), nullptr);
    EXPECT_EQ(fresh.get(ta)->producerEpoch, 4);
    ASSERT_NE(fresh.get(tb), nullptr);
    EXPECT_EQ(fresh.get(tb)->producerId, 20);
    EXPECT_EQ(fresh.get(tb)->state, tx::TxnState::Ongoing);
    ASSERT_NE(fresh.get("t"), nullptr);
    EXPECT_EQ(fresh.get("t")->producerId, 30);
    // The coordinator loaded from the rebuilt snapshot keeps them apart too.
    tx::TransactionCoordinator coord;
    coord.reload(fresh.all(), 2, 9'000);
    ASSERT_NE(coord.metadata(ta), nullptr);
    ASSERT_NE(coord.metadata(tb), nullptr);
    EXPECT_EQ(coord.metadata(ta)->producerId, 10);
    EXPECT_EQ(coord.metadata(tb)->producerId, 20);
}

TEST_F(NsKeyedTxn, V1KeyDecodesAsDefaultRegression) {
    ASSERT_TRUE(store.put(metaOf("t", 30, 1, tx::TxnState::CompleteCommit), 1));
    ASSERT_TRUE(store.put(metaOf(co::scopedId(7, "t"), 31, 0, tx::TxnState::Empty), 1));
    ASSERT_TRUE(l.pumpHW(2));
    const auto recs = l.records();
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].first, "t") << "a default transactional id's record key changed";
    EXPECT_EQ(recs[1].first, std::string("\x1F\x07\x00\x00\x00t", 6));
    tx::ReplicatedTxnStateStore fresh(l.replica.get());
    fresh.rebuildFromLog();
    ASSERT_NE(fresh.get("t"), nullptr) << "a v1 record rebuilds as nsId 0";
    EXPECT_EQ(fresh.get("t")->producerId, 30);
    EXPECT_EQ(co::parseScopedId(fresh.get("t")->txnId).nsId, 0u);
    ASSERT_NE(fresh.get(co::scopedId(7, "t")), nullptr);
    EXPECT_EQ(fresh.get(co::scopedId(7, "t"))->producerId, 31);
}

TEST(NsKeyedTxnStateRegression, AbortAllForNamespaceOnlyThatNamespaceRegression) {
    tx::TransactionCoordinator coord;
    const std::string a1 = co::scopedId(1, "x1"), a2 = co::scopedId(1, "x2"), b1 = co::scopedId(2, "x1"), d1 = "x1";
    const auto open = [&](const std::string& id) {
        const auto r = coord.initProducerId(id, 30'000, 1'000);
        EXPECT_TRUE(r.ok()) << id;
        const auto add = coord.addPartitions(id, r.producerId, r.producerEpoch, {tx::TopicPartition{"t", 0}}, 2'000);
        EXPECT_TRUE(add.ok()) << id;
        ASSERT_NE(coord.metadata(id), nullptr);
        ASSERT_EQ(coord.metadata(id)->state, tx::TxnState::Ongoing) << id;
    };
    open(a1); open(a2); open(b1); open(d1);
    EXPECT_EQ(coord.abortAllForNamespace(1, 3'000), 2u);
    for (const std::string& id : {a1, a2}) {
        const auto* m = coord.metadata(id);
        ASSERT_NE(m, nullptr);
        EXPECT_EQ(m->state, tx::TxnState::PrepareAbort) << co::parseScopedId(id).local;
        EXPECT_TRUE(m->markersPending) << "the caller must write the ABORT markers";
    }
    for (const std::string& id : {b1, d1}) {
        EXPECT_EQ(coord.metadata(id)->state, tx::TxnState::Ongoing);
        EXPECT_FALSE(coord.metadata(id)->markersPending);
    }
    EXPECT_EQ(coord.abortAllForNamespace(1, 4'000), 0u) << "idempotent: nothing is Ongoing in namespace 1 any more";
    EXPECT_EQ(coord.abortAllForNamespace(9, 4'000), 0u) << "a namespace with no transaction";
    EXPECT_EQ(coord.abortAllForNamespace(0, 5'000), 1u) << "default's own";
    EXPECT_EQ(coord.metadata(b1)->state, tx::TxnState::Ongoing);
}

// ── the reserved prefix, through the gateway ──────────────────────────────────────────────────────────────────
TEST(NsKeyedIdRegression, ReservedPrefixRefusedRegression) {
    AuthRig r{"nskreserved"};
    const uint32_t alpha = r.createNamespace("alpha");
    ASSERT_GT(alpha, 0u);
    const int64_t root = r.login("admin", AuthBroker::kAdminPassword);
    const int32_t t = r.defineTopic(root, "alpha/t");
    ASSERT_GE(t, 0);
    r.broker().createUser("alpha/u", AuthRig::kPw,
                          {prefix(SO::Read, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
                           prefix(SO::Describe, ST::Topic, ""), prefix(SO::Read, ST::Group, ""),
                           prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Describe, ST::TransactionalId, "")});
    const int64_t u = r.login("alpha/u");
    constexpr int kInvalidRequest = static_cast<int>(pc::ErrorCode::InvalidRequest);
    // The scoped form of another namespace's id, as a client could forge it.
    const std::string forged = co::scopedId(5, "g");
    for (const int64_t sid : {root, u}) {
        SCOPED_TRACE(sid == root ? "root" : "alpha/u");
        for (const std::string& id : {std::string(1, co::kScopedIdMark) + "g", forged}) {
            EXPECT_EQ(r.joinGroup(sid, id, {t}), kInvalidRequest) << "JoinGroup";
            EXPECT_EQ(r.initProducerId(sid, id), kInvalidRequest) << "InitProducerId";
            EXPECT_EQ(r.offsetFetch(sid, id, t), kInvalidRequest) << "OffsetFetch";
            EXPECT_EQ(r.heartbeat(sid, id, "m"), kInvalidRequest) << "Heartbeat";
        }
    }
    // Control: an id that merely CONTAINS the mark later is an ordinary id.
    EXPECT_EQ(r.initProducerId(u, std::string("tx") + co::kScopedIdMark), 0);
}

// ── real client x real broker ─────────────────────────────────────────────────────────────────────────────────
namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw = "namespace-user-password-12";

std::unique_ptr<rc::Client> newClient(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = user == "admin" ? kAdminPw : kUserPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    c.topics          = std::move(topics);
    auto client = std::make_unique<rc::Client>(c);
    client->connectAndWait(20s);
    return client;
}

/// Namespaces a and b, a user in each with data + group + txn grants, a topic `t` in each.
struct Fix {
    AuthBroker b;
    std::unique_ptr<rc::Client> admin;
    rc::TopicSpec tA, tB;
    explicit Fix(const std::string& tag) : b(tag) {
        b.bootstrapAdmin(kAdminPw);
        admin = newClient(b, "admin");
        for (const char* ns : {"a", "b"}) {
            auto r = admin->createNamespace(ns);
            if (!r) throw std::runtime_error(std::string("createNamespace ") + ns + ": " + r.error().detail);
        }
        for (const char* u : {"a/ua", "b/ub"})
            b.createUser(u, kUserPw,
                         {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""),
                          prefix(SO::Write, ST::Topic, ""), prefix(SO::Read, ST::Topic, ""),
                          prefix(SO::Read, ST::Group, ""),
                          prefix(SO::Describe, ST::Group, ""), prefix(SO::Write, ST::TransactionalId, ""),
                          prefix(SO::Describe, ST::TransactionalId, "")});
        tA = defineT("a/ua");
        tB = defineT("b/ub");
    }
    rc::TopicSpec defineT(const std::string& user) {
        auto c = newClient(b, user);
        const auto r = c->defineTopicAndWait("t", 1, "", false, {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define t: " + r.detail);
        c->close();
        return rc::TopicSpec{"t", r.topicId, 1};
    }
};

std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

/// n plain acked records "<tag><i>" into topic th.
void produceN(rc::Client& c, rc::TopicHandle th, const std::string& tag, int n) {
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    rc::Producer p = c.producer(pcfg);
    for (int i = 0; i < n; ++i) {
        const std::string v = tag + std::to_string(i);
        p.send(th, rc::Key(static_cast<int64_t>(i)), bytesOf(v));
    }
    ASSERT_EQ(p.closeAndWait(30s), rc::OpStatus::Ok);
}

struct Collected {
    std::mutex mu;
    std::vector<std::pair<int64_t, std::string>> recs;   // (offset, value)
    size_t size() { std::lock_guard<std::mutex> lk(mu); return recs.size(); }
    std::vector<std::pair<int64_t, std::string>> snapshot() { std::lock_guard<std::mutex> lk(mu); return recs; }
};

bool waitFor(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

/// One consumer of `group` on topic `t`: reads until `want` records arrived (waits for them), commits what it consumed.
/// Returns the records.
std::vector<std::pair<int64_t, std::string>> consumeAndCommit(AuthBroker& b, const std::string& user, const rc::TopicSpec& t,
                                                              const std::string& group, size_t want, bool commit) {
    auto c = newClient(b, user, {t});
    rc::GroupConfig gcfg;
    gcfg.autoCommit       = false;
    gcfg.autoOffsetReset  = rc::OffsetReset::Earliest;
    gcfg.sessionTimeoutMs = 10'000;
    rc::GroupConsumer gc = c->subscribeGroup(group, {t.name}, gcfg);
    Collected got;
    gc.onRecords([&](const rc::RecordSet& rs) {
        std::lock_guard<std::mutex> lk(got.mu);
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) { got.recs.emplace_back(off, std::string(v)); });
    });
    waitFor([&] { return got.size() >= want; }, 40s);
    if (want == 0) std::this_thread::sleep_for(500ms);
    auto out = got.snapshot();
    if (commit && !out.empty()) gc.commitSyncAndWait(20s);
    gc.close();
    c->close();
    return out;
}

}  // namespace

/// In `a` a consumer of group `g` consumes 5 records of a/t and commits; in `b` group `g` (the same id) consumes the 2
/// records of b/t and commits.  A fresh consumer of `g` in each namespace resumes at its OWN committed offset.
/// Mutation: make `scopedId` ignore the nsId -> both namespaces share one `g` and a resumes at b's offset (or the
/// reverse): this test and SameGroupTwoNamespacesIndependentRegression fail.
TEST(NsGroupsRegression, JoinSyncCommitFetchInNamedNamespaceRegression) {
    Fix f("nsgroups");
    {
        auto pa = newClient(f.b, "a/ua", {f.tA});
        produceN(*pa, pa->topic("t"), "a", 5);
        pa->close();
        auto pb = newClient(f.b, "b/ub", {f.tB});
        produceN(*pb, pb->topic("t"), "b", 2);
        pb->close();
    }
    const auto firstA = consumeAndCommit(f.b, "a/ua", f.tA, "g", 5, true);
    ASSERT_EQ(firstA.size(), 5u) << "a's consumer got a's 5 records";
    for (const auto& r : firstA) EXPECT_EQ(r.second[0], 'a');
    const auto firstB = consumeAndCommit(f.b, "b/ub", f.tB, "g", 2, true);
    ASSERT_EQ(firstB.size(), 2u) << "b's group `g` is not a's: it starts at b's log start";
    for (const auto& r : firstB) EXPECT_EQ(r.second[0], 'b');

    // One more record each; a fresh consumer of `g` resumes at the committed offset (5 in a, 2 in b).
    {
        auto pa = newClient(f.b, "a/ua", {f.tA});
        produceN(*pa, pa->topic("t"), "A", 1);
        pa->close();
        auto pb = newClient(f.b, "b/ub", {f.tB});
        produceN(*pb, pb->topic("t"), "B", 1);
        pb->close();
    }
    const auto resumeA = consumeAndCommit(f.b, "a/ua", f.tA, "g", 1, false);
    ASSERT_EQ(resumeA.size(), 1u);
    EXPECT_EQ(resumeA[0].first, 5) << "a's group resumed somewhere other than its own committed offset";
    EXPECT_EQ(resumeA[0].second, "A0");
    const auto resumeB = consumeAndCommit(f.b, "b/ub", f.tB, "g", 1, false);
    ASSERT_EQ(resumeB.size(), 1u);
    EXPECT_EQ(resumeB[0].first, 2) << "b's group resumed somewhere other than its own committed offset";
    EXPECT_EQ(resumeB[0].second, "B0");
}

/// In `a` a transactional producer `tx` commits 3 records and aborts 2; a read_committed consumer sees exactly the 3
/// (a committed SENTINEL after the abort proves the reader passed it: the reader ends with the 3 + the sentinel).  In
/// `b` the same transactional id `tx` works independently: b's InitProducerId does not fence a's producer (a commits
/// once more after b initialised).
TEST(NsTxnsRegression, CommitAndAbortInNamedNamespaceRegression) {
    Fix f("nstxns");
    const auto txProducer = [&](rc::Client& c) {
        rc::ProducerConfig tcfg;
        tcfg.acks = rc::Acks::All;
        tcfg.transactionalId = "tx";
        return c.producer(tcfg);
    };
    const auto sendTxn = [&](rc::Producer& p, rc::TopicHandle th, const std::string& tag, int n, bool commit) {
        p.beginTransaction();
        for (int i = 0; i < n; ++i) {
            const std::string v = tag + std::to_string(i);
            p.send(th, rc::Key(static_cast<int64_t>(i)), bytesOf(v));
        }
        if (commit) p.commitTransactionAndWait(30s);
        else p.abortTransactionAndWait(30s);
    };

    auto ca = newClient(f.b, "a/ua", {f.tA});
    rc::Producer pa = txProducer(*ca);
    pa.initTransactionsAndWait(30s);
    const rc::TopicHandle ta = ca->topic("t");
    sendTxn(pa, ta, "ok", 3, true);
    sendTxn(pa, ta, "no", 2, false);

    // b's producer with the SAME transactional id initialises now; it must not fence a's.
    auto cb = newClient(f.b, "b/ub", {f.tB});
    rc::Producer pb = txProducer(*cb);
    pb.initTransactionsAndWait(30s);
    const rc::TopicHandle tb = cb->topic("t");
    sendTxn(pb, tb, "bok", 2, true);
    sendTxn(pa, ta, "sentinel", 1, true);   // a: not fenced by b's init (a ProducerFenced here fails the test)

    const auto readCommitted = [&](rc::Client& c, rc::TopicHandle th, const std::string& sentinel) {
        Collected got;
        rc::SubscribeOptions o;
        o.from      = rc::From::earliest();
        o.isolation = rc::Isolation::ReadCommitted;
        rc::Subscription sub = c.subscribe(th, 0, o, [&](const rc::Record& r) {
            std::lock_guard<std::mutex> lk(got.mu);
            got.recs.emplace_back(0, std::string(reinterpret_cast<const char*>(r.value.data()), r.value.size()));
        });
        const bool seen = waitFor([&] {
            for (const auto& r : got.snapshot()) if (r.second == sentinel) return true;
            return false;
        });
        std::vector<std::string> values;
        for (const auto& r : got.snapshot()) values.push_back(r.second);
        sub.unsubscribe();
        EXPECT_TRUE(seen) << "the committed sentinel never became visible";
        return values;
    };
    const auto seenA = readCommitted(*ca, ta, "sentinel0");
    EXPECT_EQ(seenA, (std::vector<std::string>{"ok0", "ok1", "ok2", "sentinel0"}))
        << "a's read_committed view: the 3 committed records (+ the sentinel), never the 2 aborted";
    // b's commit landed in b's topic only.
    auto cb2 = newClient(f.b, "b/ub", {f.tB});
    const auto seenB = readCommitted(*cb2, cb2->topic("t"), "bok1");
    EXPECT_EQ(seenB, (std::vector<std::string>{"bok0", "bok1"}));
    pa.close();
    pb.close();
    ca->close();
    cb->close();
    cb2->close();
}
