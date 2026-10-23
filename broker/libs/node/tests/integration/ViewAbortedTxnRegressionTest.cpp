// ViewAbortedTxnRegressionTest.cpp — issue #537: a materialized view never folds
// the records of an ABORTED transaction, and never an open (undecided) one.
//
// Layer: a REAL Worker driving a REAL WorkerViewSet over a real leader replica
// (disk Log or transient InMemoryLog), single test thread.  Every data batch and
// marker goes through the worker's own op path (Produce / AppendControlRecord),
// so the partition's read_committed state (RcState) is exactly what production
// keeps.  A "restart" is a NEW Worker (and view set) over the same replica: its
// RcState rebuilds from the log (disk Scanning / ring RingScanning) and a
// recovery-installed view re-folds through driveRebuild.  No sleeps; every wait
// is a bounded number of doWork passes.
//
// Matrix (each for a projection view AND a Count/Sum aggregate view, over a disk
// AND a transient source):
//   commit vs abort; two interleaved producers (one commits, one aborts, both
//   orders); an aborted transaction spanning capped fold passes; restart rebuild
//   over an aborted range; an open transaction on a new leader that later aborts
//   (and one that later commits).
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerReturnCodec.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
namespace tx = rune::transaction;
namespace rc = rune::codec;
namespace rv = rune::view;

namespace {

constexpr int32_t kSrc     = 6;            // the "order" source topic
constexpr int32_t kProj    = 0x40000537;   // projection view id
constexpr int32_t kAgg     = 0x40000538;   // aggregate view id
constexpr int     kMaxPass = 4000;         // hard cap on any pass loop

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() /
               ("rune_v537_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" + std::to_string(t));
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

rc::Descriptor orderSrc() {
    return rc::DescriptorBuilder("order", 1)
        .add("orderId", rc::PrimType::Int64, 1, true)
        .add("userId", rc::PrimType::Int64)
        .add("amount", rc::PrimType::Int64)
        .build();
}
// order_by_id(orderId, userId, amount): a projection keyed by the wire key.
rv::ViewPlan projPlan(uint8_t persistence) {
    using P = rc::PrimType; using R = rv::MergeRule;
    rv::ViewPlan v;
    v.viewName = "order_by_id"; v.key = "orderId"; v.sources = {{kSrc, "orderId"}};
    v.output = {{"orderId", P::Int64, 0, "orderId", R::Replace},
                {"userId", P::Int64, 0, "userId", R::Replace},
                {"amount", P::Int64, 0, "amount", R::Replace}};
    v.persistence = persistence;
    return v;
}
// user_totals(userId, n = count(*), total = sum(amount)) GROUP BY userId.
rv::ViewPlan aggPlan(uint8_t persistence) {
    using P = rc::PrimType; using R = rv::MergeRule;
    rv::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{kSrc, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"total", P::Int64, 0, "amount", R::Sum}};
    v.persistence = persistence;
    return v;
}
std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}
std::string_view asSv(const std::vector<uint8_t>& b) {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}
std::vector<uint8_t> orderValue(int64_t orderId, int64_t userId, int64_t amount) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(1); le64(orderId); le64(userId); le64(amount);
    return b;
}
struct Order { int64_t orderId, userId, amount; };

// One producer's batch of orders.  pid < 0: a plain (non-idempotent) batch.
std::vector<uint8_t> orderBatch(const std::vector<Order>& recs, int64_t pid = -1, int16_t epoch = 0,
                                int32_t seq = -1, bool txn = false) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) {
        b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
        b.setTransactional(txn);
    }
    for (const auto& r : recs) {
        const auto k = keyBytes(r.orderId);
        const auto v = orderValue(r.orderId, r.userId, r.amount);
        b.append(asSv(k), asSv(v), 1000);
    }
    return b.build();
}

enum class Backing { Disk, Transient };
const char* name(Backing b) { return b == Backing::Disk ? "disk" : "transient"; }

// One leader partition (kSrc, 0): a disk Log or a transient ring, a replica over it.
struct Source {
    explicit Source(Backing b) : backing(b) {
        PartitionConfig c{};
        c.topicId = kSrc; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxMs = 1'000'000; c.replicaLagMaxOffset = 1'000'000;
        c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
        c.maxFetchBytes = 1 << 20; c.pushOnAppend = false;
        if (b == Backing::Disk) {
            log = std::make_unique<rune::storage::Log>(dir.path.string(), 64LL * 1024 * 1024);
            log->recover();
            replica = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<NodeId>{1},
                                                         std::vector<NodeId>{1}, log.get(), &t);
        } else {
            mem = std::make_unique<rune::storage::InMemoryLog>(64u << 20, 1'000'000);
            replica = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<NodeId>{1},
                                                         std::vector<NodeId>{1}, mem.get(), &t);
        }
    }
    Backing backing;
    TmpDir dir;
    NullTransport t;
    std::unique_ptr<rune::storage::Log> log;
    std::unique_ptr<rune::storage::InMemoryLog> mem;
    std::unique_ptr<PartitionReplica> replica;
};

// A Worker owning the source, with a view set holding the projection and the
// aggregate view (persisted over a disk source, transient over a transient one).
struct Host {
    Host(Source& s, bool recovery) : src(s), vs(ch, viewDir.path.string()) {
        const auto d = orderSrc();
        const rc::Descriptor* srcs[] = {&d};
        const uint8_t persistence = s.backing == Backing::Disk ? 0 : 1;
        for (const auto& [plan, id] : {std::pair{projPlan(persistence), kProj}, std::pair{aggPlan(persistence), kAgg}}) {
            auto comp = rv::compileView(plan, srcs, id).value();
            if (recovery) {
                // Exactly as BrokerNode::fanoutViewPlan_ at boot: a recovery install.
                rune::node::ViewPlanUpdateMsg m;
                m.viewTopicId = id;
                m.persistence = persistence;
                m.recovery    = true;
                m.planBlob    = rv::serialize(comp.plan);
                m.outDescBlob = rc::serialize(comp.outputDescriptor);
                m.srcDescBlob = rc::serialize(d);
                EXPECT_TRUE(rune::node::encodeViewPlanUpdate(ch.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
            } else {
                vs.installView(comp.plan, comp.rowLayout, d, /*persisted=*/persistence == 0);
            }
        }
        w.setOwnedDirect(Worker::OwnedSet{s.replica.get()});
        w.setViewSet(&vs);
    }

    // One worker pass: drive, collect produce / marker answers, discard the
    // checkpoint deltas (no IO agent here: the deltas only must never back up).
    void pass() {
        now += 1'000'000;
        w.doWork(now);
        w.drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult) produceAnswers.push_back(r.errorCode);
            if (r.type == WorkerReturn::Type::ControlRecordAppended) markerAnswers.push_back(r.errorCode);
        }, [](const rune::node::TxnReturn&) {});
        ch.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, rune::node::OneToOneByteRing::index_t,
                           rune::node::OneToOneByteRing::index_t) {});
    }
    void passes(int n) { for (int i = 0; i < n; ++i) pass(); }

    // A produce through the worker's op path, re-sent while the partition's RC
    // state is still building (NotLeader, retriable).  Returns the final code.
    int16_t produce(const std::vector<uint8_t>& batch, int64_t pid = -1, int16_t epoch = 0, int32_t seq = -1) {
        for (int i = 0; i < kMaxPass; ++i) {
            WorkerOp op;
            op.type = WorkerOp::Type::Produce; op.topicId = kSrc; op.partition = 0;
            op.replica = src.replica.get(); op.logPtr = src.log.get(); op.sessionId = 1;
            op.correlationId = ++corr; op.producerId = pid; op.producerEpoch = epoch; op.baseSequence = seq;
            op.durabilityTier = 1; op.recordBatchBytes = batch;
            EXPECT_TRUE(w.tryEnqueueOp(std::move(op)));
            produceAnswers.clear();
            pass();
            if (produceAnswers.empty()) return -1;
            if (produceAnswers.back() != static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader))
                return produceAnswers.back();
        }
        return -2;
    }
    void marker(int64_t pid, int16_t epoch, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kSrc; op.partition = 0;
        op.replica = src.replica.get(); op.logPtr = src.log.get(); op.correlationId = ++corr;
        op.recordBatchBytes = tx::ControlMarker::build(pid, epoch, commit ? tx::ControlType::Commit
                                                                          : tx::ControlType::Abort, 0);
        markerAnswers.clear();
        ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
        pass();
        ASSERT_EQ(markerAnswers.size(), 1u);
        ASSERT_EQ(markerAnswers.back(), 0) << "the marker landed";
    }
    // Drive until both views are caught up to the source's LEO-bounded LSO
    // (no new fold work), bounded.
    void settle() { passes(64); }

    const uint8_t* row(int32_t view, int64_t key, const rv::RowLayout*& L) const {
        return vs.getRow(view, 0, static_cast<uint64_t>(key), L);
    }
    bool hasOrder(int64_t orderId) const {
        const rv::RowLayout* L = nullptr;
        return row(kProj, orderId, L) != nullptr;
    }
    // {n, total} of the user's aggregate row, or {-1, -1} when absent.
    std::pair<int64_t, int64_t> userTotals(int64_t userId) const {
        const rv::RowLayout* L = nullptr;
        const uint8_t* r = row(kAgg, userId, L);
        if (r == nullptr || L == nullptr) return {-1, -1};
        return {rv::readSlot(r, *L, 1).i, rv::readSlot(r, *L, 2).i};
    }

    Source& src;
    TmpDir viewDir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs;
    Worker w{/*workerId=*/0};
    int64_t now = 0;
    int64_t corr = 0;
    std::vector<int16_t> produceAnswers;
    std::vector<int16_t> markerAnswers;
};

class ViewAbortedTxnRegression : public ::testing::TestWithParam<Backing> {};

INSTANTIATE_TEST_SUITE_P(Backings, ViewAbortedTxnRegression,
                         ::testing::Values(Backing::Disk, Backing::Transient),
                         [](const auto& i) { return std::string(name(i.param)); });

constexpr int64_t kPidA = 1001, kPidB = 2002;

} // namespace

// Commit vs abort: the committed transaction's rows are in both views, the
// aborted one's never — before the fix the abort's rows and counts showed up.
TEST_P(ViewAbortedTxnRegression, CommittedTxnFoldsAbortedTxnNeverFoldsRegression) {
    Source s(GetParam());
    Host h(s, /*recovery=*/false);
    ASSERT_EQ(h.produce(orderBatch({{1, 7, 10}}, kPidA, 0, 0, true), kPidA, 0, 0), 0);
    h.marker(kPidA, 0, /*commit=*/true);
    ASSERT_EQ(h.produce(orderBatch({{2, 7, 100}}, kPidB, 0, 0, true), kPidB, 0, 0), 0);
    h.marker(kPidB, 0, /*commit=*/false);
    ASSERT_EQ(h.produce(orderBatch({{3, 7, 1000}})), 0);   // a plain record after both
    h.settle();

    EXPECT_TRUE(h.hasOrder(1));
    EXPECT_FALSE(h.hasOrder(2)) << "an aborted transaction's row must never reach a projection view";
    EXPECT_TRUE(h.hasOrder(3));
    EXPECT_EQ(h.userTotals(7), (std::pair<int64_t, int64_t>{2, 1010}))
        << "an aggregate counts only the committed and the plain records";
}

// Two producers interleave their batches; one commits, the other aborts (both
// orders of the outcomes and of the markers).
TEST_P(ViewAbortedTxnRegression, InterleavedProducersOnlyTheCommittedFoldRegression) {
    for (const bool aCommits : {true, false}) {
        SCOPED_TRACE(aCommits ? "A commits, B aborts" : "A aborts, B commits");
        Source s(GetParam());
        Host h(s, /*recovery=*/false);
        ASSERT_EQ(h.produce(orderBatch({{1, 7, 1}}, kPidA, 0, 0, true), kPidA, 0, 0), 0);
        ASSERT_EQ(h.produce(orderBatch({{2, 7, 10}}, kPidB, 0, 0, true), kPidB, 0, 0), 0);
        ASSERT_EQ(h.produce(orderBatch({{3, 7, 100}, {4, 8, 5}}, kPidA, 0, 1, true), kPidA, 0, 1), 0);
        ASSERT_EQ(h.produce(orderBatch({{5, 8, 50}}, kPidB, 0, 1, true), kPidB, 0, 1), 0);
        h.settle();
        EXPECT_FALSE(h.hasOrder(1)) << "nothing of an OPEN transaction is folded";
        EXPECT_EQ(h.userTotals(7).first, -1);
        // The second outcome lands first: the LSO stays on the earlier open txn.
        h.marker(kPidB, 0, /*commit=*/aCommits ? false : true);
        h.marker(kPidA, 0, /*commit=*/aCommits);
        h.settle();
        const bool bCommits = !aCommits;
        EXPECT_EQ(h.hasOrder(1), aCommits);
        EXPECT_EQ(h.hasOrder(3), aCommits);
        EXPECT_EQ(h.hasOrder(4), aCommits);
        EXPECT_EQ(h.hasOrder(2), bCommits);
        EXPECT_EQ(h.hasOrder(5), bCommits);
        const auto u7 = h.userTotals(7), u8 = h.userTotals(8);
        if (aCommits) {
            EXPECT_EQ(u7, (std::pair<int64_t, int64_t>{2, 101}));
            EXPECT_EQ(u8, (std::pair<int64_t, int64_t>{1, 5}));
        } else {
            EXPECT_EQ(u7, (std::pair<int64_t, int64_t>{1, 10}));
            EXPECT_EQ(u8, (std::pair<int64_t, int64_t>{1, 50}));
        }
    }
}

// An aborted transaction large enough that the persisted fold caps its passes
// (one delta per ring message) — every capped pass skips it, none applies a
// slice of it.  A transient source folds it in kPushMaxChunkBytes reads.
TEST_P(ViewAbortedTxnRegression, AbortedTxnSpanningCappedPassesNeverFoldsRegression) {
    Source s(GetParam());
    Host h(s, /*recovery=*/false);
    constexpr int kBatches = 60, kPerBatch = 100;   // 6000 records: several capped passes
    int32_t seq = 0;
    for (int b = 0; b < kBatches; ++b) {
        std::vector<Order> recs;
        for (int i = 0; i < kPerBatch; ++i) recs.push_back({100'000 + b * kPerBatch + i, 9, 1});
        ASSERT_EQ(h.produce(orderBatch(recs, kPidA, 0, seq, true), kPidA, 0, seq), 0);
        seq += kPerBatch;
    }
    ASSERT_EQ(h.produce(orderBatch({{1, 7, 10}}, kPidB, 0, 0, true), kPidB, 0, 0), 0);
    h.marker(kPidA, 0, /*commit=*/false);
    h.marker(kPidB, 0, /*commit=*/true);
    h.passes(400);
    for (int64_t id : {int64_t{100'000}, int64_t{100'000 + 2999}, int64_t{100'000 + 5999}})
        EXPECT_FALSE(h.hasOrder(id)) << "aborted order " << id << " folded";
    EXPECT_EQ(h.userTotals(9).first, -1) << "no aggregate row from an aborted transaction";
    EXPECT_TRUE(h.hasOrder(1)) << "the committed transaction after it still folds";
    EXPECT_EQ(h.userTotals(7), (std::pair<int64_t, int64_t>{1, 10}));
}

// Restart: a NEW worker over the same log rebuilds its RC state (aborted index
// included) from the log, and the recovery-installed views re-fold the whole
// committed range through driveRebuild — the aborted range is skipped there too.
TEST_P(ViewAbortedTxnRegression, RestartRebuildSkipsAnAbortedRangeRegression) {
    Source s(GetParam());
    {
        Host before(s, /*recovery=*/false);
        ASSERT_EQ(before.produce(orderBatch({{1, 7, 10}}, kPidA, 0, 0, true), kPidA, 0, 0), 0);
        ASSERT_EQ(before.produce(orderBatch({{2, 7, 100}, {3, 8, 1}}, kPidB, 0, 0, true), kPidB, 0, 0), 0);
        before.marker(kPidB, 0, /*commit=*/false);
        before.marker(kPidA, 0, /*commit=*/true);
        ASSERT_EQ(before.produce(orderBatch({{4, 8, 5}})), 0);
        before.settle();
    }
    Host after(s, /*recovery=*/true);
    after.passes(200);
    EXPECT_EQ(after.vs.readiness(kProj, 0), rune::node::ViewReadiness::Ready);
    EXPECT_EQ(after.vs.readiness(kAgg, 0), rune::node::ViewReadiness::Ready);
    EXPECT_TRUE(after.hasOrder(1));
    EXPECT_FALSE(after.hasOrder(2)) << "the restart rebuild folded an aborted record";
    EXPECT_FALSE(after.hasOrder(3)) << "the restart rebuild folded an aborted record";
    EXPECT_TRUE(after.hasOrder(4));
    EXPECT_EQ(after.userTotals(7), (std::pair<int64_t, int64_t>{1, 10}));
    EXPECT_EQ(after.userTotals(8), (std::pair<int64_t, int64_t>{1, 5}));
}

// Leader failover with an OPEN transaction: the new leader (a new worker over
// the surviving log) rebuilds the transaction as open — nothing of it is
// folded — and its later ABORT marker keeps it out; a later COMMIT folds it.
TEST_P(ViewAbortedTxnRegression, OpenTxnAcrossFailoverFoldsOnlyIfItCommitsRegression) {
    for (const bool commits : {false, true}) {
        SCOPED_TRACE(commits ? "later commits" : "later aborts");
        Source s(GetParam());
        {
            Host oldLeader(s, /*recovery=*/false);
            ASSERT_EQ(oldLeader.produce(orderBatch({{1, 7, 10}, {2, 8, 20}}, kPidA, 0, 0, true), kPidA, 0, 0), 0);
            ASSERT_EQ(oldLeader.produce(orderBatch({{3, 8, 30}})), 0);   // plain, behind the open txn
            oldLeader.settle();
            EXPECT_FALSE(oldLeader.hasOrder(1));
            EXPECT_FALSE(oldLeader.hasOrder(3)) << "the LSO pins below the open transaction";
        }
        Host newLeader(s, /*recovery=*/true);
        newLeader.passes(100);
        EXPECT_FALSE(newLeader.hasOrder(1)) << "an open (undecided) transaction is never folded";
        newLeader.marker(kPidA, 0, commits);
        newLeader.settle();
        EXPECT_EQ(newLeader.hasOrder(1), commits);
        EXPECT_EQ(newLeader.hasOrder(2), commits);
        EXPECT_TRUE(newLeader.hasOrder(3));
        using Totals = std::pair<int64_t, int64_t>;
        const Totals want7 = commits ? Totals{1, 10} : Totals{-1, -1};
        const Totals want8 = commits ? Totals{2, 50} : Totals{1, 30};
        EXPECT_EQ(newLeader.userTotals(7), want7);
        EXPECT_EQ(newLeader.userTotals(8), want8);
    }
}
