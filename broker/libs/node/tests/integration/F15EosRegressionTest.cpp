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
#include <rune/gateway/GatewayReplicaView.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerReturnCodec.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/ControlMessages.h>
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

// ============================================================================
// F15 S1 — new regression tests (design note §8 S1 row).
// ============================================================================
namespace {
// The DEFAULT (Legacy) config: a worker pool is set but per-worker ingress is
// OFF.  Produces are routed to the owning worker through the op ring; markers
// must take the SAME route (K3), so the control thread never appends to a
// worker-owned log.
struct LegacyRig {
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    std::vector<std::pair<int16_t, int64_t>> produced;   // (errorCode, baseOffset)
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) produced.emplace_back(r->errorCode, r->baseOffset);
            return 1;
        });
    PartitionReplica* r = nullptr;
    explicit LegacyRig(uint8_t tier = 1) {
        auto rep = std::make_unique<PartitionReplica>(cfg(1, tier, 0, 0), Role::Leader, 1,
                                                      std::vector<NodeId>{1}, std::vector<NodeId>{1},
                                                      topics.log(0, 0), &t);
        r = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        // NOTE: no setPerWorkerIngressActive(true) — the default config.
        runWorker(1);   // ownership applied
    }
    Worker& w() { return pool.worker(0); }
    void runWorker(int64_t now) {
        w().doWork(now);
        w().drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
    }
    void produce(int64_t sid, int64_t corr) {
        const auto f = produceFrame(sid, corr, batchAt(0), /*acks=leader*/0, 0, 0);
        gw.applyProduceForTest(f.data(), f.size());
    }
};
// Offset of the first REAL control batch in the log, or -1.
int64_t controlBatchOffset(Log& log) {
    const auto raw = log.read(0, 1 << 20);
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= raw.size()) {
        rune::protocol::RecordBatchReader rd(raw.data() + pos, raw.size() - pos);
        const int32_t total = rd.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > raw.size()) break;
        if (tx::ControlMarker::isControlBatch(raw.data(), pos, raw.size() - pos)) return rd.baseOffset();
        pos += static_cast<std::size_t>(total);
    }
    return -1;
}
} // namespace

TEST(MarkerRoutingRegression, LegacyModeMarkerAppendedByOwningWorker) {
    // K3: in the default config the marker must be appended by the OWNING worker
    // (the single appender of the partition, L1) — never inline on the control
    // thread.  Observable: the log is untouched until the worker runs.
    LegacyRig R;
    const auto m = tx::ControlMarker::build(7, 0, tx::ControlType::Abort, 0);
    const int64_t rc = R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false);
    EXPECT_EQ(R.r->log()->logEndOffset(), 0)
        << "the control thread appended the marker inline to a worker-owned log (write-write race)";
    EXPECT_EQ(rc, rune::gateway::ClientGateway::kControlRecordAsync) << "the marker must be routed to the worker";
    for (int i = 0; i < 64 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.gw.markersInFlightForTest(), 0u) << "the marker reply reached the gateway";
    EXPECT_EQ(R.r->log()->logEndOffset(), 1) << "the worker appended the marker";
    EXPECT_EQ(controlBatchOffset(*R.r->log()), 0) << "as a real control batch";
}

TEST(MarkerRoutingRegression, LegacyMarkerLandsAfterQueuedProduce) {
    // K3 + L4: a produce routed to the worker and THEN a marker: the marker is
    // queued behind the produce on the same op ring, so it lands after it.
    LegacyRig R;
    const int64_t sid = R.gw.injectTestSession();
    R.produce(sid, 1);
    ASSERT_EQ(R.r->log()->logEndOffset(), 0) << "precondition: the produce is queued on the worker's op ring";
    const auto m = tx::ControlMarker::build(7, 0, tx::ControlType::Abort, 0);
    const int64_t rc = R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false);
    for (int i = 0; i < 64 && (R.gw.markersInFlightForTest() > 0 || R.produced.empty()); ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.produced.size(), 1u);
    ASSERT_EQ(R.produced[0].first, 0) << "the produce succeeded";
    // The inline (pre-fix) path returns the marker's offset synchronously; the
    // routed path reports it through the worker — read it from the log.
    const int64_t markerOff = rc >= 0 ? rc : controlBatchOffset(*R.r->log());
    ASSERT_GE(markerOff, 0) << "the marker is in the log";
    EXPECT_GT(markerOff, R.produced[0].second)
        << "the marker (offset " << markerOff << ") overtook the produce queued before it (offset "
        << R.produced[0].second << ")";
}

TEST(ControlBatchRegression, MarkerAppendRequestsFlush) {
    // N2 (Minor): appendControlBatch keeps append()'s flush request.  On a
    // Strict-tier worker the marker's log joins dirtyThisCycle_, so the pass's
    // coalesced (durable-route) flush covers the marker.  Both modes.
    {   // shard mode (per-worker ingress, external follower flush)
        EosRig R({1}, /*tier=*/2);
        const uint64_t before = R.w().ioSubmitCountForTest();
        R.abortMarker(7, 0, 10);
        ASSERT_EQ(R.r->log()->logEndOffset(), 1);
        EXPECT_GT(R.w().ioSubmitCountForTest(), before)
            << "shard mode: no coalesced flush was submitted for the Strict-tier marker";
    }
    {   // default (Legacy) mode
        LegacyRig R(/*tier=*/2);
        const uint64_t before = R.w().ioSubmitCountForTest();
        const auto m = tx::ControlMarker::build(7, 0, tx::ControlType::Abort, 0);
        (void)R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false);
        for (int i = 0; i < 64 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
        ASSERT_EQ(R.r->log()->logEndOffset(), 1);
        EXPECT_GT(R.w().ioSubmitCountForTest(), before)
            << "Legacy mode: no coalesced flush was submitted for the Strict-tier marker";
    }
}

// ============================================================================
// §7 protocol: TxnReturn (template id 6) on ret_; WorkerReturn layout pinned.
// ============================================================================

TEST(ProtocolRegression, TxnReturnRoundTrip) {
    std::array<char, 128> buf{};
    for (const auto kind : {TxnReturn::Kind::ControlRecordCommitted, TxnReturn::Kind::TxnHanging}) {
        TxnReturn in;
        in.kind = kind; in.topicId = 3; in.partition = 7; in.incarnation = 0xABCDu;
        in.leaderEpoch = 42; in.token = 0x0102030405060708LL; in.errorCode = kNotLeader;
        const std::size_t n = encodeTxnReturn(buf.data(), buf.size(), in);
        ASSERT_GT(n, 0u) << "TxnReturn could not be encoded";
        // SBE message header: blockLength u16 @0, templateId u16 @2, schemaId u16 @4 (LE).
        const auto u16 = [&](std::size_t o) {
            return static_cast<uint16_t>(static_cast<uint8_t>(buf[o]) | (static_cast<uint8_t>(buf[o + 1]) << 8));
        };
        EXPECT_EQ(u16(2), kTxnReturnTemplateId) << "TxnReturn must carry template id 6";
        EXPECT_EQ(u16(4), 7) << "internal ring schema id";
        TxnReturn out;
        ASSERT_TRUE(decodeTxnReturn(buf.data(), n, out));
        EXPECT_EQ(out.kind, in.kind);
        EXPECT_EQ(out.topicId, in.topicId);
        EXPECT_EQ(out.partition, in.partition);
        EXPECT_EQ(out.incarnation, in.incarnation);
        EXPECT_EQ(out.leaderEpoch, in.leaderEpoch);
        EXPECT_EQ(out.token, in.token);
        EXPECT_EQ(out.errorCode, in.errorCode);
        EXPECT_FALSE(decodeTxnReturn(buf.data(), n - 1, out)) << "a truncated frame must be refused";
    }
    EXPECT_EQ(encodeTxnReturn(buf.data(), 8, TxnReturn{}), 0u) << "a too-small buffer must be refused";
    // A WorkerReturn frame is not a TxnReturn (discriminated by template id).
    std::array<char, 512> wr{};
    WorkerReturn ret;
    encodeWorkerReturnSbe(wr.data(), workerReturnEncodedLength(ret), ret);
    TxnReturn out;
    EXPECT_FALSE(decodeTxnReturn(wr.data(), workerReturnEncodedLength(ret), out));
}

TEST(ProtocolRegression, WorkerReturnLayoutUnchanged) {
    // The ProduceResult carrier must not grow: the new transactional returns use
    // their own template (TxnReturn), never new WorkerReturn fields.
    namespace isbe = rune::protocol::internal::sbe;
    EXPECT_EQ(isbe::WorkerReturn::sbeTemplateId(), 2);
    EXPECT_EQ(isbe::WorkerReturn::sbeBlockLength(), 123);
    WorkerReturn produceResult;   // empty chunk
    EXPECT_EQ(workerReturnEncodedLength(produceResult), 8u + 123u + 4u);
}

// ============================================================================
// F15 S2 — I1 role gate, N1/I1 per-pass waiter validation, I-A acks=all parked
// on the owning worker in EVERY mode (design note §3, §8 S2 row; step addendum
// R-4a/R-4b/R-4c + the DR-BUG-3 drain-time min-ISR check on the worker).
// ============================================================================
namespace {
// A full client Produce frame for the owning worker's ingress (shard ingress).
int ingressAcksAll(Worker& w, int64_t corr, int64_t now, int32_t topicId = 1) {
    const auto f = produceFrame(1, corr, batchAt(0), /*acks=all*/1, topicId, 0);
    return w.handleIngressFrameForTest(f.data(), f.size(), now);
}
int ingressAcks0(Worker& w, int64_t corr, int64_t now, int32_t topicId = 1) {
    const auto f = produceFrame(1, corr, batchAt(0), /*acks=*/0, topicId, 0);
    return w.handleIngressFrameForTest(f.data(), f.size(), now);
}
std::vector<uint8_t> otherBatchAt(int64_t base, int n = 1) {   // different bytes at the same offsets
    rune::protocol::RecordBatchBuilder b(base, 2000);
    for (int i = 0; i < n; ++i) b.append("other", "DIFFERENT", 2000 + i);
    return b.build();
}
// The node is told (by follower 2's fetch under a higher epoch) that it is a
// stale leader: self-demotion in onFetchRequest — no op, no bridge hook.
void selfDemote(PartitionReplica& r, int32_t newEpoch, int64_t now) {
    rune::replication::FetchRequestMsg up{newEpoch, 0, /*follower*/2, 0, 1 << 20, r.topicId()};
    (void)r.onFetchRequest(up, now);
}
// KIP-101 answer from the new leader: the shared epoch ended at `endOffset`.
void truncateTo(PartitionReplica& r, int64_t endOffset) {
    rune::replication::OffsetsForLeaderEpochRespMsg ole{};
    ole.leaderEpoch = r.epoch(); ole.partition = 0; ole.responseEpoch = 1;
    ole.endOffset = endOffset; ole.errorCode = 0; ole.topicId = r.topicId();
    r.applyOffsetsForLeaderEpochResponse(ole);
}
// Re-fetch from the new leader: `records` appended at the follower's LEO.
void refetch(PartitionReplica& r, const std::vector<uint8_t>& records, int64_t leaderHw) {
    rune::replication::FetchResponseMsg fr{};
    fr.leaderEpoch = r.epoch(); fr.partition = 0; fr.highWatermark = leaderHw; fr.logStartOffset = 0;
    fr.errorCode = 0; fr.topicId = r.topicId(); fr.records = std::span<const uint8_t>(records);
    r.applyReplicated(fr);
}
std::vector<uint8_t> concat(std::initializer_list<std::vector<uint8_t>> parts) {
    std::vector<uint8_t> out;
    for (const auto& p : parts) out.insert(out.end(), p.begin(), p.end());
    return out;
}

// The DEFAULT (Legacy) config with a committed metadata view: acks=all produces
// go gateway → owning worker (executeProduceWire_).  Captures both answer kinds:
// the gateway's inline ErrorResponse and the ProduceResponse.
struct LegacyAcksRig {
    struct Answer { bool inlineError; int16_t code; int64_t base; };
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    std::vector<Answer> answers;
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) answers.push_back({false, r->errorCode, r->baseOffset});
            else if (auto e = pc::ErrorResponse::decode(b, n)) answers.push_back({true, e->code, -1});
            return 1;
        });
    PartitionReplica* r = nullptr;
    int64_t sid = 0;
    int64_t now = 1;
    explicit LegacyAcksRig(std::vector<NodeId> isr = {1, 2}, uint8_t tier = 1, int minIsr = 1,
                           int64_t replicaLagMaxMs = 1'000'000) {
        auto c = cfg(1, tier, 0, 0);
        c.minIsr = minIsr;
        c.replicaLagMaxMs = replicaLagMaxMs;
        auto rep = std::make_unique<PartitionReplica>(c, Role::Leader, 1, isr, isr, topics.log(0, 0), &t);
        r = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setCommittedMetadataReady(true);   // steady state: acks=all is deferred (not refused)
        sid = gw.injectTestSession();
        runWorker();   // ownership applied
    }
    Worker& w() { return pool.worker(0); }
    // One worker pass + the control thread's return drain + its (legacy)
    // gateway-side acks=all drain, exactly as the duty cycle runs them.
    void runWorker() {
        ++now;
        w().doWork(now);
        w().drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
        gw.drainPendingAcksForTest(now);
    }
    void produce(int64_t corr, uint8_t acks, const std::vector<uint8_t>& batch) {
        const auto f = produceFrame(sid, corr, batch, acks, 0, 0);
        gw.applyProduceForTest(f.data(), f.size());
    }
    void produceAll(int64_t corr) { produce(corr, /*acks=all*/1, batchAt(0)); }
    void followerFetch(int32_t epoch = 1) {
        rune::replication::FetchRequestMsg fr{epoch, 0, 2, r->log()->logEndOffset(), 1 << 20, 0};
        (void)r->onFetchRequest(fr, now);
    }
    std::size_t okCount() const {
        std::size_t n = 0;
        for (const auto& a : answers) n += (!a.inlineError && a.code == 0) ? 1u : 0u;
        return n;
    }
};
} // namespace

// The structural guard (design note §8 S2): the gateway's replica handle has no
// role() — the owning worker is the role authority.
static_assert(!rune::gateway::ExposesRole<rune::gateway::GatewayReplicaView>,
              "the gateway must not be able to read the worker-owned role");
static_assert(rune::gateway::ExposesRole<PartitionReplica>, "the guard's detector works");

TEST(TxnRoleGateRegression, SelfDemotionFailsParkedAcks) {
    // onFetchRequest with a higher epoch demotes the replica with no op and no
    // bridge hook: the parked acks=all waiter fails NotLeader within ONE pass.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    ASSERT_EQ(ingressAcksAll(w, 9, 1), 1);
    ASSERT_EQ(w.reservedAckSlotsForTest(), 1) << "precondition: acks=all parked on the HW";
    selfDemote(rep, 2, 2);
    ASSERT_EQ(rep.role(), Role::Follower) << "precondition: self-demoted";
    w.doWork(3);
    const auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u) << "the self-demoted waiter must be answered in the next pass";
    EXPECT_EQ(rets[0].errorCode, kNotLeader);
    EXPECT_EQ(w.reservedAckSlotsForTest(), 0);
}

TEST(TxnRoleGateRegression, EpochBumpWhileLeaderKeepsWaiter) {
    // becomeLeader at a higher epoch with the SAME log: the waiter keeps waiting
    // and fires OK once the new term's HW passes it (Kafka DelayedProduce).
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    ASSERT_EQ(ingressAcksAll(w, 9, 1), 1);
    const uint64_t gen = rep.logGeneration();
    WorkerOp bl;
    bl.type = WorkerOp::Type::BecomeLeader; bl.topicId = 1; bl.partition = 0; bl.replica = &rep;
    bl.logPtr = &log; bl.roleEpoch = 2; bl.isr = {1, 2};
    ASSERT_TRUE(w.tryEnqueueOp(stamp(std::move(bl))));
    std::vector<WorkerReturn> rets;
    for (int i = 0; i < 4; ++i) { w.doWork(2 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    ASSERT_EQ(rep.role(), Role::Leader);
    ASSERT_EQ(rep.epoch(), 2);
    ASSERT_EQ(rep.logGeneration(), gen) << "precondition: an epoch bump does not change the log";
    EXPECT_TRUE(rets.empty()) << "an epoch bump while still leader must not fail the waiter";
    rune::replication::FetchRequestMsg fr{2, 0, 2, log.logEndOffset(), 1 << 20, 1};
    (void)rep.onFetchRequest(fr, 50);
    for (int i = 0; i < 8 && rets.empty(); ++i) { w.doWork(60 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    ASSERT_EQ(rets.size(), 1u) << "the waiter fires once the new term's HW passes it";
    EXPECT_EQ(rets[0].errorCode, 0);
}

TEST(TxnRoleGateRegression, TruncationBelowWaiterFailsIt) {
    // Between two worker passes: self-demotion, KIP-101 truncation BELOW the
    // waiter, a re-fetch of DIFFERENT data at that offset, re-promotion.  The
    // role is Leader again at the next pass and the HW covers the offset — but
    // the content there changed: the waiter must fail NotLeader, never fire OK.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    ASSERT_EQ(ingressAcksAll(w, 9, 1), 1);                  // waiter at offset 0
    w.doWork(2);
    ASSERT_TRUE(drainAll(w).empty());
    selfDemote(rep, 2, 3);
    truncateTo(rep, 0);
    ASSERT_EQ(log.logEndOffset(), 0) << "precondition: truncated below the waiter";
    refetch(rep, otherBatchAt(0), /*leaderHw=*/1);
    ASSERT_EQ(log.logEndOffset(), 1) << "precondition: different data now sits at offset 0";
    rep.becomeLeader(3, {1});
    ASSERT_GT(rep.highWatermark(), 0) << "precondition: the HW covers the waiter's offset";
    std::vector<WorkerReturn> rets;
    for (int i = 0; i < 4; ++i) { w.doWork(10 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, kNotLeader) << "an acks=all OK over truncated + replaced data";
}

TEST(TxnRoleGateRegression, TwoTruncationsBetweenPassesFailWaiter) {
    // Two truncations between passes: the FIRST cuts below the waiter, the
    // second (the last one) above it.  Tracking only the last cut would keep
    // the waiter and fire OK over changed data (addendum: min-cut since park).
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    ASSERT_EQ(ingressAcks0(w, 8, 1), 1);                    // offset 0
    ASSERT_EQ(ingressAcksAll(w, 9, 1), 1);                  // waiter at offset 1
    w.doWork(2);
    ASSERT_EQ(drainAll(w).size(), 1u);                       // the acks=0 answer
    selfDemote(rep, 2, 3);
    truncateTo(rep, 0);                                      // cut 0 <= waiter
    refetch(rep, concat({otherBatchAt(0), otherBatchAt(1), otherBatchAt(2), otherBatchAt(3)}), /*hw=*/0);
    ASSERT_EQ(log.logEndOffset(), 4);
    rep.becomeFollower(3, 2);                                // uncommitted tail → probe armed
    truncateTo(rep, 3);                                      // cut 3 > waiter (the LAST cut)
    ASSERT_EQ(log.logEndOffset(), 3);
    refetch(rep, otherBatchAt(3), /*leaderHw=*/4);
    rep.becomeLeader(4, {1});
    ASSERT_GT(rep.highWatermark(), 1) << "precondition: the HW covers the waiter's offset";
    std::vector<WorkerReturn> rets;
    for (int i = 0; i < 4; ++i) { w.doWork(10 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, kNotLeader) << "fired OK although an earlier cut replaced the waiter's data";
}

TEST(TxnRoleGateRegression, TruncationAboveWaiterKeepsWaiter) {
    // One truncation strictly ABOVE the waiter between passes: the content at
    // and below the waiter is unchanged, so it keeps waiting and fires OK once
    // the HW passes it (the precise min-cut rule, not "fail on any change").
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    ASSERT_EQ(ingressAcksAll(w, 9, 1), 1);                  // waiter at offset 0
    ASSERT_EQ(ingressAcks0(w, 10, 1), 1);                   // 1
    ASSERT_EQ(ingressAcks0(w, 11, 1), 1);                   // 2
    w.doWork(2);
    ASSERT_EQ(drainAll(w).size(), 2u);
    const uint64_t gen = rep.logGeneration();
    selfDemote(rep, 2, 3);
    truncateTo(rep, 2);                                      // cut 2 > waiter
    ASSERT_EQ(log.logEndOffset(), 2);
    rep.becomeLeader(3, {1, 2});
    ASSERT_EQ(rep.logGeneration(), gen + 1) << "precondition: exactly one change to the log";
    std::vector<WorkerReturn> rets;
    for (int i = 0; i < 4; ++i) { w.doWork(10 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    EXPECT_TRUE(rets.empty()) << "a cut above the waiter must not fail it";
    rune::replication::FetchRequestMsg fr{3, 0, 2, log.logEndOffset(), 1 << 20, 1};
    (void)rep.onFetchRequest(fr, 50);
    for (int i = 0; i < 8 && rets.empty(); ++i) { w.doWork(60 + i); for (auto& r : drainAll(w)) rets.push_back(r); }
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, 0);
}

TEST(TxnRoleGateRegression, GatewayUsesIntendedRoleHint) {
    using IR = ReplicaRegistry::IntendedRole;
    {   // hint Leader, replica role_ Follower → the gateway routes; W refuses.
        LegacyAcksRig R({1});
        const auto h = R.reg.lookupHandle(0, 0);
        R.reg.setIntendedRole(0, 0, h.incarnation, IR::Leader);
        R.r->becomeFollower(2, 2);
        R.produce(1, /*acks=*/0, batchAt(0));
        EXPECT_TRUE(R.answers.empty())
            << "the gateway answered inline from the worker-owned role() (a cross-thread read)";
        for (int i = 0; i < 4 && R.answers.empty(); ++i) R.runWorker();
        ASSERT_EQ(R.answers.size(), 1u);
        EXPECT_FALSE(R.answers[0].inlineError) << "the owning worker decides (ProduceResult)";
        EXPECT_EQ(R.answers[0].code, kNotLeader);
        EXPECT_EQ(R.r->log()->logEndOffset(), 0);
    }
    {   // hint Follower, replica Leader → fast inline NotLeader; nothing routed.
        LegacyAcksRig R({1});
        const auto h = R.reg.lookupHandle(0, 0);
        R.reg.setIntendedRole(0, 0, h.incarnation, IR::Follower);
        R.produce(1, /*acks=*/0, batchAt(0));
        ASSERT_EQ(R.answers.size(), 1u) << "a Follower hint must answer NotLeader inline";
        EXPECT_TRUE(R.answers[0].inlineError);
        EXPECT_EQ(R.answers[0].code, kNotLeader);
        for (int i = 0; i < 4; ++i) R.runWorker();
        EXPECT_EQ(R.r->log()->logEndOffset(), 0) << "the produce was routed although the hint says Follower";
        EXPECT_EQ(R.answers.size(), 1u);
    }
}

TEST(TxnRoleGateRegression, LegacyAcksAllDemotionFailsNotLeader) {
    // I-A, DEFAULT config: an acks=all produce via the gateway; self-demotion;
    // truncation + re-fetch past lastOffset with DIFFERENT data; re-promotion.
    // The gateway's HW-snapshot check would ack it OK over replaced data.
    LegacyAcksRig R({1, 2});
    R.produceAll(1);
    R.runWorker();
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    ASSERT_TRUE(R.answers.empty()) << "precondition: the acks=all ack is parked";
    selfDemote(*R.r, 2, R.now);
    ASSERT_EQ(R.r->role(), Role::Follower);
    truncateTo(*R.r, 0);
    ASSERT_EQ(R.r->log()->logEndOffset(), 0);
    refetch(*R.r, otherBatchAt(0), /*leaderHw=*/1);
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    R.r->becomeLeader(3, {1});
    for (int i = 0; i < 8 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    EXPECT_EQ(R.answers[0].code, kNotLeader) << "acks=all acked over truncated + replaced data";
}

TEST(LegacyAcksAllRegression, ParkedOnOwningWorkerFiresWhenHwPasses) {
    // R-4a: in the default config the acks=all waiter lives on the OWNING
    // worker (pendingAcksAll_), not in the gateway's pendingAcks_, and the
    // worker's per-pass flush fires it in this mode too.
    LegacyAcksRig R({1, 2});
    R.produceAll(1);
    R.runWorker();
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    EXPECT_EQ(R.gw.pendingAcksForTest(), 0u) << "the gateway parked a worker-owned acks=all ack";
    EXPECT_EQ(R.w().reservedAckSlotsForTest(), 1) << "the waiter must be parked on the owning worker";
    EXPECT_TRUE(R.answers.empty());
    R.followerFetch();
    for (int i = 0; i < 8 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    EXPECT_EQ(R.answers[0].code, 0);
    EXPECT_EQ(R.answers[0].base, 0);
    EXPECT_EQ(R.w().reservedAckSlotsForTest(), 0);
}

TEST(LegacyAcksAllRegression, StrictAcksAllAwaitsFsyncThenHw) {
    // R-4b: Legacy Strict acks=all — fsync first, THEN the HW (thenAwaitHw).
    LegacyAcksRig R({1, 2}, /*tier=*/2);
    R.w().setReapPausedForTest(true);
    R.produceAll(1);
    R.runWorker();
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    ASSERT_EQ(R.w().durablePendingCountForTest(), 1u) << "precondition: parked on its fsync";
    R.followerFetch();                                        // HW passes BEFORE the fsync
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_TRUE(R.answers.empty()) << "a Strict ack must not precede its fsync";
    R.w().setReapPausedForTest(false);
    for (int i = 0; i < kCap && R.w().durablePendingCountForTest() > 0; ++i) { R.runWorker(); std::this_thread::yield(); }
    for (int i = 0; i < 8 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    EXPECT_EQ(R.answers[0].code, 0);
    EXPECT_EQ(R.gw.pendingAcksForTest(), 0u);
    // And the other order: fsync first, the HW later — the waiter moves from
    // the fsync to the HW gate on the worker.
    R.w().setReapPausedForTest(false);
    R.answers.clear();
    R.produceAll(2);
    R.runWorker();
    for (int i = 0; i < kCap && R.w().durablePendingCountForTest() > 0; ++i) { R.runWorker(); std::this_thread::yield(); }
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_TRUE(R.answers.empty()) << "durable but not committed: the acks=all ack must wait for the HW";
    EXPECT_EQ(R.w().reservedAckSlotsForTest(), 1) << "after the fsync the waiter parks on the HW (worker)";
    R.followerFetch();
    for (int i = 0; i < 8 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    EXPECT_EQ(R.answers[0].code, 0);
    EXPECT_EQ(R.answers[0].base, 1);
}

TEST(LegacyAcksAllRegression, BurstBeyondReservationAnsweredRetriableNoHalt) {
    // R-4c: the worker's G-A6 reservation bounds gateway-routed acks=all
    // waiters (the gateway's old cap went with pendingAcks_): the excess is
    // answered retriable (NotLeader) BEFORE any append, nothing is dropped, no
    // halt, and one HW advance then fires every parked waiter exactly once.
    LegacyAcksRig R({1, 2});
    constexpr int kBurst = 600;                               // > the 64 KiB return ring's ~248 slots
    int sent = 0;
    while (sent < kBurst) {
        for (int k = 0; k < 40 && sent < kBurst; ++k) R.produceAll(++sent);
        R.runWorker();
    }
    for (int i = 0; i < 4; ++i) R.runWorker();
    const int64_t parked = R.w().reservedAckSlotsForTest();
    std::size_t retriable = 0, other = 0;
    for (const auto& a : R.answers) (a.code == kNotLeader ? retriable : other) += 1;
    ASSERT_GT(parked, 0) << "acks=all must park on the owning worker";
    EXPECT_LT(parked, kBurst) << "the reservation must bound the parked waiters";
    EXPECT_EQ(other, 0u) << "only retriable answers before the HW advances";
    EXPECT_EQ(static_cast<int64_t>(retriable) + parked, kBurst) << "every produce answered or parked";
    EXPECT_EQ(R.r->log()->logEndOffset(), parked) << "a refused produce must not be appended";
    R.followerFetch();
    for (int i = 0; i < 64 && R.okCount() < static_cast<std::size_t>(parked); ++i) R.runWorker();
    EXPECT_EQ(R.okCount(), static_cast<std::size_t>(parked)) << "every parked waiter fires exactly once";
    EXPECT_EQ(R.answers.size(), static_cast<std::size_t>(kBurst));
    EXPECT_EQ(R.w().reservedAckSlotsForTest(), 0);
}

TEST(LegacyAcksAllRegression, HeldWhileIsrBelowMinIsrAtFire) {
    // DR-BUG-3 moved with I-A: the drain-time min-ISR check runs on the worker's
    // ack-fire path.  Admitted at ISR {1,2} >= minIsr 2 (the runtime policy);
    // the ISR then shrinks to {1} while the HW passes the batch (the replica's
    // own minIsr is 1, so its HW is not clamped) → held, never OK; acked once
    // the ISR is healthy again.
    LegacyAcksRig R({1, 2}, /*tier=*/1, /*replica minIsr=*/1, /*replicaLagMaxMs=*/1);
    R.gw.setMinIsr(2);
    R.gw.syncMinIsrForTest();
    R.produceAll(1);
    R.runWorker();
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    ASSERT_TRUE(R.answers.empty());
    // Follower 2 never fetches: the worker's per-replica ISR check evicts it
    // (the clock advances in 2 s steps, well inside the 30 s ack timeout).
    for (int i = 0; i < 6 && R.r->isr().size() > 1; ++i) { R.now += 2'000'000'000LL; R.runWorker(); }
    ASSERT_EQ(R.r->isr().size(), 1u) << "precondition: ISR shrank below the runtime minIsr";
    ASSERT_GT(R.r->highWatermark(), 0) << "precondition: the HW passed the batch";
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_TRUE(R.answers.empty()) << "acks=all acked OK while the ISR is below minIsr (DR-BUG-3)";
    for (int i = 0; i < 6 && R.r->isr().size() < 2; ++i) {   // follower 2 catches up → ISR {1,2}
        R.now += 2'000'000'000LL;                              // next ISR-check tick ...
        R.followerFetch();                                     // ... with a fresh fetch (lag < 1 ms)
        R.runWorker();
    }
    ASSERT_EQ(R.r->isr().size(), 2u) << "precondition: the ISR recovered";
    for (int i = 0; i < 8 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    EXPECT_EQ(R.answers[0].code, 0);
}

// ============================================================================
// F15 S2 fix round 1 (review C1, controller ruling Option 1): in the default
// config the gateway's PSM / LSO / dirty-log mirrors still exist until S4/S5,
// so the OWNING worker must hand them the append facts AT APPEND (a facts-only
// return), never only with the final ack — which arrives a replication RTT
// later, or as an error that carries no facts.
// ============================================================================
namespace {
std::vector<uint8_t> idemBatch(int64_t pid, int16_t epoch, int32_t seq, bool txn = false) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setTransactional(txn); b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
    b.append("k", "idem", 1000);
    return b.build();
}
void produceIdem(LegacyAcksRig& R, int64_t corr, int64_t pid, int32_t seq, bool txn = false) {
    const auto f = produceFrame(R.sid, corr, idemBatch(pid, 0, seq, txn), /*acks=all*/1, 0, 0, pid, 0, seq);
    R.gw.applyProduceForTest(f.data(), f.size());
}
} // namespace

TEST(LegacyAcksAllRegression, PipelinedIdempotentAcksAllNotOutOfOrder) {
    // (a) An idempotent producer pipelines seq 0 and seq 1 (acks=all, default
    // client maxInFlight 32): seq 1 reaches the gateway's PSM validate while
    // seq 0's ack is still parked on the HW.  It must be accepted.
    LegacyAcksRig R({1, 2});
    produceIdem(R, 1, /*pid=*/7, /*seq=*/0);
    R.runWorker();
    produceIdem(R, 2, 7, 1);
    R.runWorker();
    ASSERT_TRUE(R.answers.empty()) << "seq 1 answered " << (R.answers.empty() ? 0 : R.answers[0].code)
                                   << " (20 = OutOfOrderSequence) while seq 0 was parked";
    EXPECT_EQ(R.r->log()->logEndOffset(), 2) << "both batches appended";
    R.followerFetch();
    for (int i = 0; i < 8 && R.answers.size() < 2; ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 2u);
    EXPECT_EQ(R.answers[0].code, 0);
    EXPECT_EQ(R.answers[1].code, 0);
}

TEST(LegacyAcksAllRegression, RetryAfterAckTimeoutIsDeduped) {
    // (b) The batch IS appended, its acks=all ack fails (timeout: the follower
    // never fetches), the client retries the same sequence → a duplicate: one
    // copy in the log, answered with the original offset.
    LegacyAcksRig R({1, 2});
    produceIdem(R, 1, 7, 0);
    R.runWorker();
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    R.now += 31'000'000'000LL;                                // past the 30 s acks=all deadline
    for (int i = 0; i < 4 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    ASSERT_NE(R.answers[0].code, 0) << "precondition: the ack failed";
    produceIdem(R, 2, 7, 0);                                  // the client's retry
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_EQ(R.r->log()->logEndOffset(), 1) << "the retry of an appended batch was appended again";
    ASSERT_EQ(R.answers.size(), 2u);
    EXPECT_EQ(R.answers[1].code, 0) << "a duplicate is answered OK";
    EXPECT_EQ(R.answers[1].base, 0) << "with the ORIGINAL offset";
}

TEST(LegacyAcksAllRegression, TxnBatchRegisteredAtAppendAndCoveredAfterFailedAck) {
    // (c) A transactional acks=all batch: its open txn is registered AT APPEND
    // (the read_committed bound is held at its first offset while the ack is
    // parked), and an ABORT after its ack FAILED still covers it.
    LegacyAcksRig R({1, 2});
    produceIdem(R, 1, 7, 0, /*txn=*/true);
    R.runWorker();
    ASSERT_EQ(R.r->log()->logEndOffset(), 1);
    ASSERT_TRUE(R.answers.empty()) << "precondition: the ack is parked";
    EXPECT_EQ(R.gw.openTxnsForTest(0, 0), 1u) << "the open txn is not registered while its ack is parked "
                                                 "(read_committed bound not held)";
    R.now += 31'000'000'000LL;
    for (int i = 0; i < 4 && R.answers.empty(); ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    ASSERT_NE(R.answers[0].code, 0) << "precondition: the ack failed";
    const auto m = tx::ControlMarker::build(7, 0, tx::ControlType::Abort, 0);
    ASSERT_EQ(R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false),
              rune::gateway::ClientGateway::kControlRecordAsync);
    for (int i = 0; i < 64 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker();
    ASSERT_EQ(R.gw.markersInFlightForTest(), 0u);
    ASSERT_EQ(R.r->log()->logEndOffset(), 2) << "the ABORT marker landed at 1";
    bool covered = false;
    for (const auto& rg : R.gw.abortedRangesForTest(0, 0)) covered = covered || rg.contains(0);
    EXPECT_TRUE(covered) << "the appended txn batch whose ack failed is not covered by the abort";
    EXPECT_EQ(R.gw.openTxnsForTest(0, 0), 0u);
}

TEST(LegacyAcksAllRegression, InlinePathStaleAckFailsNotLeaderWithoutDereference) {
    // Review m5: the gateway's own pendingAcks_ still serves the INLINE path (no
    // worker pool): an acks=all entry whose partition is removed + freed (and
    // re-created) resolves through the registry → NotLeader, no dereference.
    for (bool recreate : {false, true}) {
        TmpDir d;
        rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
        NullTransport t;
        ReplicaRegistry reg;
        auto mk = [&] {
            return std::make_unique<PartitionReplica>(cfg(1, 1, 0, 0), Role::Leader, 1,
                std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, topics.log(0, 0), &t);
        };
        reg.add(0, 0, mk());
        std::vector<int16_t> codes;
        auto gw = rune::gateway::ClientGateway::makeForTest([&](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) codes.push_back(r->errorCode);
            return 1;
        });
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setCommittedMetadataReady(true);
        const int64_t sid = gw.injectTestSession();
        const auto f = produceFrame(sid, 1, batchAt(0), /*acks=all*/1, 0, 0);
        gw.applyProduceForTest(f.data(), f.size());
        ASSERT_EQ(gw.pendingAcksForTest(), 1u) << "precondition: inline acks=all parked in the gateway";
        reg.remove(0, 0);
        ASSERT_EQ(reg.collectRetired(reg.generation()), 1u) << "freed";
        if (recreate) reg.add(0, 0, mk());
        gw.drainPendingAcksForTest(1);
        EXPECT_EQ(gw.pendingAcksForTest(), 0u);
        ASSERT_EQ(codes.size(), 1u) << "recreate=" << recreate;
        EXPECT_EQ(codes[0], kNotLeader) << "recreate=" << recreate;
    }
}

TEST(TxnRoleGateRegression, NoOpEpochTruncationKeepsLogGeneration) {
    // Review m10: an epoch-change "truncation" at or above the LEO changes
    // nothing — it must not bump logGeneration (a needless bump can turn a
    // later single cut into "two changes" and fail a waiter / force an RC rebuild).
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    refetch(rep, concat({batchAt(0), batchAt(1)}), /*leaderHw=*/2);
    ASSERT_EQ(log.logEndOffset(), 2);
    const uint64_t gen = rep.logGeneration();
    rune::replication::FetchResponseMsg fr{};
    fr.leaderEpoch = 2; fr.partition = 0; fr.highWatermark = 2; fr.errorCode = 0; fr.topicId = rep.topicId();
    rep.applyReplicated(fr);                                  // epoch upgrade: shared epoch 1 ends at LEO
    ASSERT_EQ(rep.epoch(), 2);
    EXPECT_EQ(log.logEndOffset(), 2);
    EXPECT_EQ(rep.logGeneration(), gen) << "a no-op truncation bumped logGeneration";
    truncateTo(rep, 5);                                       // OLE answer above the LEO: no-op
    EXPECT_EQ(rep.logGeneration(), gen);
}
