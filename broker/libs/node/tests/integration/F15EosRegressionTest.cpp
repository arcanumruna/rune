// F15EosRegressionTest.cpp — F15 step S0: failing *Regression* tests that pin
// the EOS transaction-marker / LSO defects on the worker, gateway and txn
// coordinator (design note docs/superpowers/specs/2026-10-01-f15-eos-marker-lso-design.md).
//
// EXPECTED RED on b04f871: S0 lands the tests only; the fixes (S1-S8) land after
// the design review.  Layer: in-process, single test thread — the worker is driven
// by explicit doWork passes and its returns are fed to the real gateway with
// handleWorkerReturnForTest, so every ordering is deterministic.  No sleeps; the
// only waits are condition waits with a hard iteration cap.
#include <rune/gateway/ClientGateway.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <thread>
#include <unistd.h>

using namespace rune::node;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
using rune::storage::Log;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
namespace tx = rune::transaction;

namespace {
constexpr int kCap = 2'000'000;   // hard cap on any condition-wait loop
constexpr int16_t kNotLeader   = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kConcurrent  = static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions);
constexpr int16_t kInvalidTxnState      = static_cast<int16_t>(pc::ErrorCode::InvalidTxnState);
constexpr int16_t kInvalidProducerEpoch = static_cast<int16_t>(pc::ErrorCode::InvalidProducerEpoch);
constexpr int16_t kNotCoordinator       = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_f15_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const rune::replication::FetchResponseMsg&) override {}
};
std::vector<uint8_t> batchAt(int64_t base, int n = 1) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < n; ++i) b.append("k", "v", 1000 + i);
    return b.build();
}
std::vector<uint8_t> txnBatch(int64_t pid, int16_t epoch, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
    b.append("k", "txn", 1000);
    return b.build();
}
WorkerOp stamp(WorkerOp op) {
    if (op.replica) { op.targetReplicaId = op.replica->registryId(); op.incarnation = op.replica->incarnation(); }
    return op;
}
PartitionConfig cfg(NodeId self, uint8_t tier = 1, int32_t topicId = 1, int32_t part = 0) {
    PartitionConfig c; c.topicId = topicId; c.partitionId = part; c.selfId = self; c.minIsr = 1; c.durabilityTier = tier;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
// One full client Produce wire frame (what WorkerIngress delivers to a shard).
std::vector<uint8_t> produceFrame(int64_t sid, int64_t corr, const std::vector<uint8_t>& batch, uint8_t acks,
                                  int32_t topicId, int32_t part, int64_t pid = -1, int16_t epoch = 0,
                                  int32_t seq = -1) {
    pc::Produce msg;
    msg.sessionId = sid; msg.correlationId = corr; msg.topicId = topicId; msg.partition = part;
    msg.partitionKnown = 1; msg.acks = acks; msg.producerId = pid; msg.producerEpoch = epoch;
    msg.baseSequence = seq; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}
WorkerOp becomeFollowerOp(PartitionReplica* r, int32_t epoch, NodeId leader) {
    WorkerOp op;
    op.type = WorkerOp::Type::BecomeFollower; op.topicId = r->topicId();
    op.partition = static_cast<int32_t>(r->partitionId()); op.replica = r; op.logPtr = r->log();
    op.roleEpoch = epoch; op.roleLeaderId = leader;
    return stamp(std::move(op));
}
WorkerOp produceOp(PartitionReplica* r, int64_t corr, uint8_t tier = 1) {
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = r->topicId(); op.partition = static_cast<int32_t>(r->partitionId());
    op.replica = r; op.logPtr = r->log(); op.sessionId = 1; op.correlationId = corr;
    op.recordCount = 1; op.durabilityTier = tier; op.recordBatchBytes = batchAt(0);
    return stamp(std::move(op));
}
WorkerOp markerOp(PartitionReplica* r, int64_t token, const std::vector<uint8_t>& marker) {
    WorkerOp op;
    op.type = WorkerOp::Type::AppendControlRecord; op.topicId = r->topicId();
    op.partition = static_cast<int32_t>(r->partitionId()); op.replica = r; op.logPtr = r->log();
    op.correlationId = token; op.recordBatchBytes = marker;
    return stamp(std::move(op));
}
std::vector<WorkerReturn> drainAll(Worker& w) {
    std::vector<WorkerReturn> rets;
    w.drainReturn([&](WorkerReturn r) { rets.push_back(std::move(r)); });
    return rets;
}
bool logHoldsControlBatchAt0(Log& log) {
    const auto raw = log.read(0, 1 << 20);
    return !raw.empty() && tx::ControlMarker::isControlBatch(raw.data(), 0, raw.size());
}
struct Captured {
    std::vector<int16_t> produceErrors;
    rune::gateway::ClientGateway::TestEgressFn fn() {
        return [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) produceErrors.push_back(r->errorCode);
            return 1;
        };
    }
};

// One real user partition (topic 0 / partition 0) owned by worker 0 of a real
// WorkerPool, behind a real ClientGateway in per-worker mode.  The test thread
// drives the worker and feeds its returns to the gateway (the control thread's
// job), so the order of worker returns is exactly the order the gateway sees.
struct EosRig {
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    Captured cap;
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(cap.fn());
    PartitionReplica* r = nullptr;
    explicit EosRig(std::vector<NodeId> isr = {1}, uint8_t tier = 1, int minIsr = 1) {
        auto c = cfg(1, tier, 0, 0);
        c.minIsr = minIsr;
        auto rep = std::make_unique<PartitionReplica>(c, Role::Leader, 1, isr, isr, topics.log(0, 0), &t);
        if (tier == 2) rep->setExternalFollowerFlush(true);
        r = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setPerWorkerIngressActive(true);
        w().setWorkerIngress(nullptr, true);
        if (tier == 2) w().setShardMode(true);
        runWorker(1);   // ownership applied
    }
    Worker& w() { return pool.worker(0); }
    void runWorker(int64_t now) {
        w().doWork(now);
        w().drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
    }
    // A transactional produce arriving on the shard's own ingress (shard mode).
    int ingressTxn(int64_t sid, int64_t corr, int64_t pid, int16_t epoch, int32_t seq, uint8_t acks, int64_t now) {
        const auto f = produceFrame(sid, corr, txnBatch(pid, epoch, seq), acks, 0, 0, pid, epoch, seq);
        return w().handleIngressFrameForTest(f.data(), f.size(), now);
    }
    // The coordinator's marker dispatch (gateway → owning worker).
    void abortMarker(int64_t pid, int16_t epoch, int64_t now) {
        const auto m = tx::ControlMarker::build(pid, epoch, tx::ControlType::Abort, 0);
        ASSERT_EQ(gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), pid, /*commit=*/false),
                  rune::gateway::ClientGateway::kControlRecordAsync);
        for (int i = 0; i < 64 && gw.markersInFlightForTest() > 0; ++i) runWorker(now + i);
        ASSERT_EQ(gw.markersInFlightForTest(), 0u) << "the marker reply reached the gateway";
    }
    bool abortCovers(int64_t off) const {
        for (const auto& rg : gw.abortedRangesForTest(0, 0)) if (rg.contains(off)) return true;
        return false;
    }
};

// EosRig + a transaction coordinator (optionally with a replicated txn-state
// store) — the round-5 TxnRig, parameterised.
struct TxnRig {
    EosRig m;
    TmpDir sd;
    std::unique_ptr<Log> stateLog;
    std::unique_ptr<PartitionReplica> stateReplica;
    std::unique_ptr<tx::ReplicatedTxnStateStore> store;
    bool leader = true;
    std::unique_ptr<TransactionCoordinationService> tcs;
    std::vector<std::vector<uint8_t>> controlFrames;
    std::string txnId = "tx-f15";
    int64_t pid = -1; int16_t epoch = 0; int32_t seq = 0; int64_t corr = 100; int64_t sid = 0;
    std::array<uint8_t, 4096> out{};
    explicit TxnRig(int minIsr = 1, bool withStore = false) : m({1}, 1, minIsr) {
        if (withStore) {
            stateLog = std::make_unique<Log>(sd.path.string(), 64LL * 1024 * 1024);
            stateLog->recover();
            stateReplica = std::make_unique<PartitionReplica>(cfg(1, 1, 99, 0), Role::Leader, 1,
                std::vector<NodeId>{1}, std::vector<NodeId>{1}, stateLog.get(), &m.t);
            store = std::make_unique<tx::ReplicatedTxnStateStore>(stateReplica.get());
        }
        tcs = std::make_unique<TransactionCoordinationService>(store.get(), &m.topics,
                                                              [this] { return leader; }, [] { return NodeId{1}; });
        tcs->setGateway(&m.gw);
        m.gw.setTxnService(tcs.get());
        m.gw.setCommittedMetadataReady(true);
        m.gw.setOnControlFrame([this](int64_t, const uint8_t* b, std::size_t n) { controlFrames.emplace_back(b, b + n); });
        sid = m.gw.injectTestSession();
    }
    std::span<const uint8_t> tid() const { return {reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size()}; }
    int16_t initPid() {
        pc::InitProducerId q; q.sessionId = 1; q.correlationId = ++corr; q.transactionTimeoutMs = 60'000;
        q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = tcs->handleInitProducerId(f.data(), f.size(), 1, out.data(), out.size());
        auto r = pc::InitProducerIdResponse::decode(out.data(), n);
        if (r && r->errorCode == 0) { pid = r->producerId; epoch = r->producerEpoch; }
        return r ? r->errorCode : -1;
    }
    int16_t addPartition() {
        pc::AddPartitionsToTxn q; q.sessionId = 1; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.transactionalId = tid();
        const pc::TxnTopicPartition tp{0, 0};
        std::vector<uint8_t> f(pc::AddPartitionsToTxn::encodedSizeFor(tid(), 1));
        q.encode(f.data(), std::span<const pc::TxnTopicPartition>(&tp, 1));
        const auto n = tcs->handleAddPartitions(f.data(), f.size(), 1, out.data(), out.size());
        auto r = pc::AddPartitionsToTxnResponse::decode(out.data(), n);
        return (r && r->results.size() == 1) ? r->results[0].errorCode : int16_t{-1};
    }
    // EndTxn error code if answered synchronously, 999 if the response is deferred.
    int16_t endTxn(bool commit) {
        pc::EndTxn q; q.sessionId = 1; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.commit = commit ? 1 : 0; q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = tcs->handleEndTxn(f.data(), f.size(), 1, out.data(), out.size());
        if (n == 0) return 999;
        auto r = pc::EndTxnResponse::decode(out.data(), n);
        return r ? r->errorCode : -1;
    }
    // A transactional data batch through the REAL gateway produce path.
    void produceTxn() {
        const auto batch = txnBatch(pid, epoch, seq);
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = ++corr; msg.topicId = 0; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = 0; msg.producerId = pid; msg.producerEpoch = epoch;
        msg.baseSequence = seq++; msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        m.gw.applyProduceForTest(frame.data(), frame.size());
    }
    void runUntilNoMarkerAwaited(int64_t now) {
        for (int i = 0; i < 64 && tcs->awaitingMarkerTxnsForTest() > 0; ++i) m.runWorker(now + i);
    }
    std::optional<int16_t> endTxnFrameError(std::size_t i) const {
        if (i >= controlFrames.size()) return std::nullopt;
        auto r = pc::EndTxnResponse::decode(controlFrames[i].data(), controlFrames[i].size());
        if (!r) return std::nullopt;
        return r->errorCode;
    }
};
} // namespace

// ============================================================================
// N2 — markers must land as REAL control batches on every append path.
// ============================================================================

TEST(ControlBatchRegression, ReplicaPathMarkerLandsAsControlBatch) {
    // The worker's executeAppendControlRecordWire_ (the shard/per-worker path).
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    const auto marker = tx::ControlMarker::build(7, 0, tx::ControlType::Commit, 0);
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&leader, 1, marker)));
    w.doWork(1);
    const auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    ASSERT_EQ(rets[0].type, WorkerReturn::Type::ControlRecordAppended);
    ASSERT_EQ(rets[0].errorCode, 0);
    ASSERT_EQ(rets[0].baseOffset, 0);
    const auto raw = log.read(0, 1 << 20);
    ASSERT_FALSE(raw.empty());
    rune::protocol::RecordBatchReader rd(raw.data(), raw.size());
    EXPECT_TRUE(tx::ControlMarker::isControlBatch(raw.data(), 0, raw.size()))
        << "landed as attrs=0x" << std::hex << rd.attributes() << std::dec << " producerId=" << rd.producerId()
        << " (a data batch wrapping the marker bytes)";
    EXPECT_EQ(rd.producerId(), 7);
}

TEST(ControlBatchRegression, GatewayInlineMarkerLandsAsControlBatch) {
    // ClientGateway::appendControlRecord's inline replica branch (no worker pool).
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    reg.add(0, 0, std::make_unique<PartitionReplica>(cfg(1, 1, 0, 0), Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, topics.log(0, 0), &t));
    Captured cap;
    auto gw = rune::gateway::ClientGateway::makeForTest(cap.fn());
    gw.setTopicRegistry(&topics);
    gw.setReplicaRegistry(&reg);
    const auto marker = tx::ControlMarker::build(7, 0, tx::ControlType::Abort, 0);
    ASSERT_EQ(gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 7, false), 0);
    EXPECT_TRUE(logHoldsControlBatchAt0(*topics.log(0, 0))) << "the inline replica path wraps the marker as data";
}

// ============================================================================
// C1 — the LSO "batch appended" fact must not depend on the ack or the session:
// an ABORT marker whose reply overtakes a deferred ProduceResult (or a result
// dropped because the session is gone) leaves the aborted batch uncovered →
// visible to read_committed — and the late result then pins the LSO forever.
// ============================================================================

TEST(TxnFactOrderRegression, AcksAllParkedBatchBeforeAbortIsCovered) {
    EosRig R({1, 2});                                       // follower 2 in ISR: acks=all parks on HW
    const int64_t sid = R.gw.injectTestSession();
    ASSERT_EQ(R.ingressTxn(sid, 1, /*pid=*/7, 0, 0, /*acks=all*/1, 2), 1);
    ASSERT_EQ(R.r->log()->logEndOffset(), 1) << "txn batch appended at 0";
    R.runWorker(3);
    ASSERT_TRUE(R.cap.produceErrors.empty()) << "precondition: the ack is parked on the HW";
    R.abortMarker(7, 0, 10);                                // marker lands at 1; its reply overtakes the parked ack
    ASSERT_EQ(R.r->log()->logEndOffset(), 2);
    EXPECT_TRUE(R.abortCovers(0)) << "the aborted batch at offset 0 is not in any aborted range: "
                                     "read_committed would deliver it";
    // The follower catches up → the parked ack fires → the (late) ProduceResult.
    rune::replication::FetchRequestMsg fr{1, 0, 2, R.r->log()->logEndOffset(), 1 << 20, 0};
    (void)R.r->onFetchRequest(fr, 100);
    for (int i = 0; i < 64 && R.cap.produceErrors.empty(); ++i) R.runWorker(200 + i);
    ASSERT_EQ(R.cap.produceErrors.size(), 1u);
    EXPECT_EQ(R.cap.produceErrors[0], 0);
    EXPECT_EQ(R.gw.openTxnsForTest(0, 0), 0u) << "the late result re-opened a resolved txn: the LSO is pinned forever";
}

TEST(TxnFactOrderRegression, StrictDeferredBatchBeforeAbortIsCovered) {
    EosRig R({1}, /*tier=*/2);
    R.w().setReapPausedForTest(true);                       // the Strict ack waits on its fsync
    const int64_t sid = R.gw.injectTestSession();
    ASSERT_EQ(R.ingressTxn(sid, 1, 7, 0, 0, /*acks=*/0, 2), 1);
    R.runWorker(3);
    ASSERT_TRUE(R.cap.produceErrors.empty()) << "precondition: the Strict ack is parked on its fsync";
    R.abortMarker(7, 0, 10);
    EXPECT_TRUE(R.abortCovers(0)) << "the aborted Strict batch at 0 is not covered by the abort";
    R.w().setReapPausedForTest(false);
    for (int i = 0; i < kCap && R.cap.produceErrors.empty(); ++i) { R.runWorker(100 + i); std::this_thread::yield(); }
    ASSERT_EQ(R.cap.produceErrors.size(), 1u);
    EXPECT_EQ(R.gw.openTxnsForTest(0, 0), 0u) << "the late Strict result re-opened the aborted txn (LSO pinned)";
}

TEST(TxnFactOrderRegression, SessionGoneBatchStillRegistered) {
    EosRig R;
    // The producer's session is gone before its ProduceResult drains.
    ASSERT_EQ(R.ingressTxn(/*sid=*/424242, 1, 7, 0, 0, 0, 2), 1);
    R.abortMarker(7, 0, 10);                                // result (dropped) then marker reply, in order
    ASSERT_EQ(R.r->log()->logEndOffset(), 2);
    EXPECT_TRUE(R.abortCovers(0)) << "the batch was appended (offset 0) but never registered: "
                                     "read_committed would deliver aborted data";
}

TEST(TxnFactOrderRegression, ZombieBatchAfterAbortMarkerIsFencedShardIngress) {
    // C1+: the marker fences the partition producer epoch (Kafka).  A zombie
    // batch with an older epoch arriving AFTER the ABORT marker is refused.
    EosRig R;
    const int64_t sid = R.gw.injectTestSession();
    ASSERT_EQ(R.ingressTxn(sid, 1, 7, /*epoch=*/0, 0, 0, 2), 1);
    R.abortMarker(7, /*markerEpoch=*/1, 10);                // fence-abort: bumped epoch
    ASSERT_EQ(R.r->log()->logEndOffset(), 2);
    ASSERT_EQ(R.ingressTxn(sid, 2, 7, /*zombie epoch=*/0, 1, 0, 100), 1);
    R.runWorker(101);
    ASSERT_EQ(R.cap.produceErrors.size(), 2u);
    EXPECT_EQ(R.cap.produceErrors[1], kInvalidProducerEpoch) << "the zombie must be fenced by the marker's epoch";
    EXPECT_EQ(R.r->log()->logEndOffset(), 2) << "the zombie batch was appended after the abort marker";
    EXPECT_EQ(R.gw.openTxnsForTest(0, 0), 0u) << "the zombie opened a txn that never closes (LSO pinned)";
}

// ============================================================================
// I1 + N1 — the OWNING worker is the role authority.  A produce or marker
// queued behind a BecomeFollower must not touch the follower log; demotion
// fails every parked ack retriably (Kafka delayed-produce semantics).
// ============================================================================

TEST(TxnRoleGateRegression, GatewayPathProduceOnFollowerIsNotLeader) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    // The gateway saw Leader and routed the produce, but a BecomeFollower was
    // queued ahead of it on the same op ring.
    ASSERT_TRUE(w.tryEnqueueOp(becomeFollowerOp(&rep, 2, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 77)));
    w.doWork(1);
    ASSERT_EQ(rep.role(), Role::Follower) << "precondition: demoted before the produce ran";
    const auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, kNotLeader) << "a follower must refuse the produce (retriable)";
    EXPECT_EQ(log.logEndOffset(), 0) << "the produce was appended to the FOLLOWER log";
}

TEST(TxnRoleGateRegression, ShardIngressProduceOnFollowerIsNotLeader) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    ASSERT_TRUE(w.tryEnqueueOp(becomeFollowerOp(&rep, 2, 2)));
    w.doWork(1);
    ASSERT_EQ(rep.role(), Role::Follower);
    const auto f = produceFrame(1, 9, batchAt(0), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(f.data(), f.size(), 2), 1);
    const auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, kNotLeader);
    EXPECT_EQ(log.logEndOffset(), 0) << "the shard appended a client produce to a FOLLOWER log";
}

TEST(TxnRoleGateRegression, MarkerOnFollowerIsRetriableNotLeader) {
    // Run in a child: on b04f871 a marker on a follower hits the debug assert
    // in PartitionReplica::append (abort) — that must not take the whole binary
    // down.  Exit 0 == the Kafka-faithful outcome (NotLeader, log untouched).
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
        Worker w(0, 1 << 16, 1 << 16, 64, 64);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        (void)w.tryEnqueueOp(becomeFollowerOp(&rep, 2, 2));
        (void)w.tryEnqueueOp(markerOp(&rep, 5, tx::ControlMarker::build(7, 0, tx::ControlType::Commit, 0)));
        w.doWork(1);
        const auto rets = drainAll(w);
        const bool ok = rets.size() == 1u && rets[0].type == WorkerReturn::Type::ControlRecordAppended &&
                        rets[0].errorCode == kNotLeader && log.logEndOffset() == 0;
        std::_Exit(ok ? 0 : 1);
    }, ::testing::ExitedWithCode(0), "");
}

TEST(TxnRoleGateRegression, DemotionFailsParkedAcksAllNotLeader) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    const auto f = produceFrame(1, 9, batchAt(0), /*acks=all*/1, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(f.data(), f.size(), 1), 1);
    ASSERT_EQ(w.reservedAckSlotsForTest(), 1) << "precondition: acks=all parked on the HW";
    ASSERT_TRUE(w.tryEnqueueOp(becomeFollowerOp(&rep, 2, 2)));
    std::vector<WorkerReturn> rets;
    for (int i = 0; i < 8; ++i) { w.doWork(2 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    ASSERT_EQ(rep.role(), Role::Follower);
    ASSERT_EQ(rets.size(), 1u) << "the demotion must answer the parked acks=all waiter";
    EXPECT_EQ(rets[0].errorCode, kNotLeader);
    EXPECT_EQ(w.reservedAckSlotsForTest(), 0);
}

TEST(TxnRoleGateRegression, DemotionFailsParkedStrictAckNotLeader) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    rep.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    w.setShardMode(true);
    w.setReapPausedForTest(true);
    const auto f = produceFrame(1, 9, batchAt(0), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(f.data(), f.size(), 1), 1);
    w.doWork(1);                                            // coalesced fsync submitted
    const auto tok = w.durablePendingTokenForTest(0);
    ASSERT_NE(tok, kNoToken) << "precondition: the Strict ack waits on its fsync";
    ASSERT_TRUE(w.tryEnqueueOp(becomeFollowerOp(&rep, 2, 2)));
    w.doWork(2);
    ASSERT_EQ(rep.role(), Role::Follower);
    std::vector<WorkerReturn> rets = drainAll(w);
    w.deliverCompletionForTest(FlushCompletion{tok, 0, log.flushBoundary()});
    w.doWork(3);
    for (auto& r : drainAll(w)) rets.push_back(r);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, kNotLeader) << "a parked Strict ack must fail NotLeader on demotion, not fire OK";
}

// ============================================================================
// I3 — a refused / retriable marker is NOT "landed": the txn must not complete
// (no EndTxn OK, next txn refused) until the marker is really in the log.
// ============================================================================

TEST(MarkerRetryRegression, MinIsrRefusedMarkerNeverCompletesTheTxn) {
    TxnRig T(/*minIsr=*/2);                                  // ISR {1} < minIsr 2: acks=all marker refused
    ASSERT_EQ(T.initPid(), 0);
    ASSERT_EQ(T.addPartition(), 0);
    T.produceTxn();                                          // acks=0: no ISR gate, lands at 0
    for (int i = 0; i < 4; ++i) T.m.runWorker(1 + i);
    ASSERT_EQ(T.m.r->log()->logEndOffset(), 1);
    ASSERT_EQ(T.endTxn(/*commit=*/true), 999) << "EndTxn deferred while the marker is in flight";
    T.runUntilNoMarkerAwaited(10);
    EXPECT_EQ(T.m.r->log()->logEndOffset(), 1) << "no marker in the log";
    EXPECT_EQ(T.tcs->awaitingMarkerTxnsForTest(), 1u)
        << "the txn completed although its only marker was refused (controlMarkersRefused="
        << T.tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_TRUE(T.controlFrames.empty())
        << "an EndTxn response was sent (error=" << T.endTxnFrameError(0).value_or(-1)
        << ") while no COMMIT marker exists";
    EXPECT_EQ(T.addPartition(), kConcurrent) << "the next txn may not start while the marker is unwritten";
    EXPECT_EQ(T.m.gw.openTxnsForTest(0, 0), 1u) << "the LSO stays pinned at the open txn (correct)";
}

// ============================================================================
// I4 (+m3, m4) — coordinator failover, durable completion, EndTxn table.
// ============================================================================

TEST(TxnCoordinatorFailoverRegression, PrepareCommitRedrivenAfterFailover) {
    // Coordinator A persists PrepareCommit and dies before any marker.  The new
    // coordinator B (same replicated txn-state log) must reload it and re-drive.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica stateRep(cfg(1, 1, 99, 0), Role::Leader, 1, {1}, {1}, &log, &t);
    tx::ReplicatedTxnStateStore storeA(&stateRep);
    TransactionCoordinationService a(&storeA, nullptr, [] { return true; }, [] { return NodeId{1}; });
    std::array<uint8_t, 1024> out{};
    const std::string id = "tx-fo";
    const std::span<const uint8_t> tid(reinterpret_cast<const uint8_t*>(id.data()), id.size());
    pc::InitProducerId ip; ip.sessionId = 1; ip.correlationId = 1; ip.transactionTimeoutMs = 60'000; ip.transactionalId = tid;
    std::vector<uint8_t> f(ip.encodedSize()); ip.encode(f.data());
    auto ipr = pc::InitProducerIdResponse::decode(out.data(), a.handleInitProducerId(f.data(), f.size(), 1, out.data(), out.size()));
    ASSERT_TRUE(ipr && ipr->errorCode == 0);
    pc::AddPartitionsToTxn ap; ap.sessionId = 1; ap.correlationId = 2; ap.producerId = ipr->producerId;
    ap.producerEpoch = ipr->producerEpoch; ap.transactionalId = tid;
    const pc::TxnTopicPartition tp{0, 0};
    f.assign(pc::AddPartitionsToTxn::encodedSizeFor(tid, 1), 0);
    ap.encode(f.data(), std::span<const pc::TxnTopicPartition>(&tp, 1));
    (void)a.handleAddPartitions(f.data(), f.size(), 1, out.data(), out.size());
    pc::EndTxn et; et.sessionId = 1; et.correlationId = 3; et.producerId = ipr->producerId;
    et.producerEpoch = ipr->producerEpoch; et.commit = 1; et.transactionalId = tid;
    f.assign(et.encodedSize(), 0); et.encode(f.data());
    (void)a.handleEndTxn(f.data(), f.size(), 1, out.data(), out.size());
    // A dies here.  B becomes coordinator over the same replicated log.
    tx::ReplicatedTxnStateStore storeB(&stateRep);
    TransactionCoordinationService b(&storeB, nullptr, [] { return true; }, [] { return NodeId{1}; });
    int markers = 0;
    bool commitMarker = false;
    b.setAppendFnOverride([&](std::string_view, int32_t, std::span<const uint8_t> bytes) -> int64_t {
        tx::ControlMarkerPayload p;
        if (tx::ControlMarker::extract(bytes.data(), 0, p, bytes.size())) commitMarker = (p.controlType == tx::ControlType::Commit);
        return markers++;
    });
    for (int i = 0; i < 4; ++i) (void)b.doWork(10 + i);
    const auto* meta = storeB.get(id);
    ASSERT_NE(meta, nullptr) << "precondition: B's store reloaded the txn";
    ASSERT_EQ(meta->state, tx::TxnState::PrepareCommit) << "precondition: the decision is durable";
    EXPECT_EQ(markers, 1) << "B never reloaded the coordinator: the COMMIT marker is never written";
    EXPECT_TRUE(commitMarker);
}

TEST(TxnCoordinatorFailoverRegression, CompletionIsPersisted) {
    TxnRig T(1, /*withStore=*/true);
    ASSERT_EQ(T.initPid(), 0);
    ASSERT_EQ(T.addPartition(), 0);
    T.produceTxn();
    ASSERT_EQ(T.endTxn(true), 999);
    T.runUntilNoMarkerAwaited(10);
    ASSERT_EQ(T.tcs->awaitingMarkerTxnsForTest(), 0u) << "precondition: completed";
    const auto* meta = T.store->get(T.txnId);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, tx::TxnState::CompleteCommit)
        << "the completion was never persisted (store state=" << static_cast<int>(meta->state)
        << "): a failover re-drives a finished txn";
}

TEST(TxnCoordinatorFailoverRegression, EndTxnRetryAfterCompleteSameDirectionIsOk) {
    TxnRig T;
    ASSERT_EQ(T.initPid(), 0);
    ASSERT_EQ(T.addPartition(), 0);
    T.produceTxn();
    ASSERT_EQ(T.endTxn(true), 999);
    T.runUntilNoMarkerAwaited(10);
    ASSERT_EQ(T.controlFrames.size(), 1u);
    ASSERT_EQ(T.endTxnFrameError(0).value_or(-1), 0) << "precondition: committed";
    // The client lost that response and retries the same EndTxn.
    EXPECT_EQ(T.endTxn(true), 0) << "Kafka: a same-direction retry after completion is OK";
}

TEST(TxnCoordinatorFailoverRegression, EndTxnOppositeDirectionIsInvalidTxnState) {
    TxnRig T;
    ASSERT_EQ(T.initPid(), 0);
    ASSERT_EQ(T.addPartition(), 0);
    T.produceTxn();
    ASSERT_EQ(T.endTxn(/*commit=*/true), 999);              // PrepareCommit, marker in flight
    EXPECT_EQ(T.endTxn(/*commit=*/false), kInvalidTxnState)
        << "an ABORT while PrepareCommit is a client bug (InvalidTxnState), not retriable";
    EXPECT_EQ(T.endTxn(/*commit=*/true), kConcurrent) << "a same-direction retry IS retriable";
}

TEST(TxnCoordinatorFailoverRegression, LossOfLeadershipAnswersNotCoordinatorAndClearsTasks) {
    TxnRig T;
    ASSERT_EQ(T.initPid(), 0);
    ASSERT_EQ(T.addPartition(), 0);
    T.produceTxn();
    ASSERT_EQ(T.endTxn(true), 999);
    ASSERT_EQ(T.tcs->awaitingMarkerTxnsForTest(), 1u);
    T.leader = false;                                        // coordinator deposed before the marker landed
    (void)T.tcs->doWork(50);
    EXPECT_EQ(T.tcs->awaitingMarkerTxnsForTest(), 0u) << "marker tasks of a deposed coordinator must be dropped";
    ASSERT_EQ(T.controlFrames.size(), 1u) << "the deferred EndTxn must be answered on unload";
    EXPECT_EQ(T.endTxnFrameError(0).value_or(-1), kNotCoordinator);
}
