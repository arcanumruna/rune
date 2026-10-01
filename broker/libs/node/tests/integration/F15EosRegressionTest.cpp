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
#include <rune/node/ReplicationMetadataBridge.h>
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
#include <rune/storage/LogCleaner.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

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
        ASSERT_EQ(gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), pid, /*commit=*/false, /*coordinatorEpoch=*/0),
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
    ASSERT_EQ(gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 7, false, /*coordinatorEpoch=*/0), 0);
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
    const int64_t rc = R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false, /*coordinatorEpoch=*/0);
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
    const int64_t rc = R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false, /*coordinatorEpoch=*/0);
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
        (void)R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false, /*coordinatorEpoch=*/0);
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
    struct Answer { bool inlineError; int16_t code; int64_t base; int64_t corr = 0; };
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    std::vector<Answer> answers;
    std::vector<std::pair<int64_t, int64_t>> records;   // (subscriptionId, baseOffset) pushed
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) answers.push_back({false, r->errorCode, r->baseOffset});
            else if (auto e = pc::ErrorResponse::decode(b, n)) answers.push_back({true, e->code, -1, e->correlationId});
            else if (auto rec = pc::Records::decode(b, n)) records.emplace_back(rec->subscriptionId, rec->baseOffset);
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
// Review m4: no raw-pointer getter that would let routingPtr()->role() bypass it.
template <typename V> concept ExposesRoutingPtr = requires(const V& v) { v.routingPtr(); };
static_assert(!ExposesRoutingPtr<rune::gateway::GatewayReplicaView>,
              "the routing pointer is stamped into the op, never returned");

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
    ASSERT_EQ(R.gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), 7, /*commit=*/false, /*coordinatorEpoch=*/0),
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

// ============================================================================
// F15 S2 fix round 2 (re-review 1).  B1: an ERROR result must never be taken
// for the append-time facts-only return — the client must be answered and the
// gateway mirrors (PSM / LSO) must never record a batch that was not appended.
// Strict ruling: Strict facts are applied at fsync completion, never at
// append — a retry after a failed fsync must not be answered duplicate-OK.
// ============================================================================
namespace {
// Replace the worker's owned set (as a SetOwnership op from the runtime would).
void setWorkerOwns(LegacyAcksRig& R, std::vector<PartitionReplica*> owned) {
    WorkerOp op;
    op.type = WorkerOp::Type::SetOwnership;
    op.ownedSet = std::move(owned);
    ASSERT_TRUE(R.w().tryEnqueueOp(std::move(op)));
    R.runWorker();
}
} // namespace

TEST(LegacyAcksAllRegression, NotOwnedRefusalIsAnsweredAndNeverRecorded) {
    // Ownership moved between the gateway's route and the worker's execution:
    // the worker refuses the acks=all produce NotLeader.  The client is answered
    // and the retry (ownership back) is APPENDED — never a duplicate-OK at 0.
    LegacyAcksRig R({1, 2});
    setWorkerOwns(R, {});
    produceIdem(R, 1, 7, 0);
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_EQ(R.r->log()->logEndOffset(), 0);
    ASSERT_EQ(R.answers.size(), 1u) << "the not-owned refusal was swallowed as a facts-only return";
    EXPECT_EQ(R.answers[0].code, kNotLeader);
    setWorkerOwns(R, {R.r});
    produceIdem(R, 2, 7, 0);                                  // the client's retry
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_EQ(R.r->log()->logEndOffset(), 1) << "the retry was answered from a PSM entry for a never-appended batch";
    R.followerFetch();
    for (int i = 0; i < 8 && R.answers.size() < 2; ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 2u);
    EXPECT_EQ(R.answers[1].code, 0);
    EXPECT_EQ(R.answers[1].base, 0);
}

TEST(LegacyAcksAllRegression, SchemaRejectIsAnsweredAndNeverRecorded) {
    // A schema-validation reject of an acks=all produce: answered
    // SchemaValidationFailed, PSM untouched (a retry is validated again and
    // rejected again — not a duplicate-OK), txn never registered.
    LegacyAcksRig R({1, 2});
    WorkerOp su;
    su.type = WorkerOp::Type::SchemaUpdate; su.topicId = 0;
    su.schemaUpdateBlob = rune::codec::serialize(rune::codec::DescriptorBuilder("s")
        .add("id", rune::codec::PrimType::Int64).build());
    ASSERT_TRUE(R.w().tryEnqueueOp(std::move(su)));
    R.runWorker();
    ASSERT_TRUE(R.w().hasValidatorForTest(0));
    constexpr int16_t kSchemaFailed = static_cast<int16_t>(pc::ErrorCode::SchemaValidationFailed);
    produceIdem(R, 1, 7, 0, /*txn=*/true);                    // value "idem" is not a valid record
    for (int i = 0; i < 4; ++i) R.runWorker();
    EXPECT_EQ(R.r->log()->logEndOffset(), 0);
    ASSERT_EQ(R.answers.size(), 1u) << "the schema reject was swallowed as a facts-only return";
    EXPECT_EQ(R.answers[0].code, kSchemaFailed);
    EXPECT_EQ(R.gw.openTxnsForTest(0, 0), 0u) << "a rejected txn batch was registered as open (LSO pinned)";
    produceIdem(R, 2, 7, 0, /*txn=*/true);                    // retry
    for (int i = 0; i < 4; ++i) R.runWorker();
    ASSERT_EQ(R.answers.size(), 2u);
    EXPECT_EQ(R.answers[1].code, kSchemaFailed) << "the retry was answered from a PSM entry for a rejected batch";
    EXPECT_EQ(R.r->log()->logEndOffset(), 0);
}

TEST(LegacyAcksAllRegression, StrictRetryAfterFsyncFailureIsNotDuplicateOk) {
    // Strict acks=all whose fsync FAILS: the batch is not durable.  A retry must
    // never be answered duplicate-OK (Strict facts are applied at fsync
    // completion, not at append).
    LegacyAcksRig R({1, 2}, /*tier=*/2);
    R.w().forceFlushErrorForTest(EIO);
    produceIdem(R, 1, 7, 0);
    for (int i = 0; i < kCap && R.answers.empty(); ++i) { R.runWorker(); std::this_thread::yield(); }
    ASSERT_EQ(R.answers.size(), 1u);
    ASSERT_NE(R.answers[0].code, 0) << "precondition: the fsync failed";
    produceIdem(R, 2, 7, 0);                                  // the client's retry
    for (int i = 0; i < kCap && R.answers.size() < 2; ++i) { R.runWorker(); std::this_thread::yield(); }
    ASSERT_EQ(R.answers.size(), 2u);
    EXPECT_NE(R.answers[1].code, 0) << "a Strict retry was answered duplicate-OK for data that is not durable";
}

TEST(LegacyAcksAllRegression, StrictAcksAllDedupsAfterFsync) {
    // Strict acks=all success path: once the fsync completed, the facts are
    // applied — a retry is a duplicate (one copy, original offset) and the next
    // sequence is accepted (not OutOfOrderSequence) while the ack waits on the HW.
    LegacyAcksRig R({1, 2}, /*tier=*/2);
    produceIdem(R, 1, 7, 0);
    R.runWorker();
    for (int i = 0; i < kCap && R.w().durablePendingCountForTest() > 0; ++i) { R.runWorker(); std::this_thread::yield(); }
    R.runWorker();
    ASSERT_EQ(R.w().reservedAckSlotsForTest(), 1) << "precondition: fsynced, parked on the HW";
    ASSERT_TRUE(R.answers.empty());
    produceIdem(R, 2, 7, 0);                                  // retry of seq 0
    R.runWorker();
    ASSERT_EQ(R.answers.size(), 1u);
    EXPECT_EQ(R.answers[0].code, 0);
    EXPECT_EQ(R.answers[0].base, 0);
    EXPECT_EQ(R.r->log()->logEndOffset(), 1) << "one copy";
    produceIdem(R, 3, 7, 1);                                  // seq 1
    R.runWorker();
    EXPECT_EQ(R.answers.size(), 1u) << "seq 1 answered (OutOfOrderSequence?) instead of being parked";
    EXPECT_EQ(R.r->log()->logEndOffset(), 2);
}

// ============================================================================
// F15 S7 — coordinator load state machine (K4), coordinatorEpoch, persisted
// pid block ceiling (I7).  Design note §5.1, §5.6, §8 S7 row.
// ============================================================================

namespace {
// A store-backed TransactionCoordinationService over a configurable
// __transaction_state replica.  No gateway: markers go through the
// appendFnOverride seam and are recorded (direction + coordinatorEpoch).
struct CoordRig {
    TmpDir d;
    std::unique_ptr<Log> log;
    NullTransport t;
    std::unique_ptr<PartitionReplica> rep;
    std::unique_ptr<tx::ReplicatedTxnStateStore> store;
    bool consensusLeader = true;
    std::unique_ptr<TransactionCoordinationService> tcs;
    struct Marker { bool commit; int32_t coordinatorEpoch; };
    std::vector<Marker> markers;
    std::array<uint8_t, 1024> out{};
    int64_t corr = 0;
    struct Init { int16_t err = -1; int64_t pid = -1; int16_t epoch = -1; };

    CoordRig(Role role, rune::replication::Epoch epoch, std::vector<NodeId> replicas, std::vector<NodeId> isr) {
        log = std::make_unique<Log>(d.path.string(), 64LL * 1024 * 1024);
        log->recover();
        rep = std::make_unique<PartitionReplica>(cfg(1, 1, 99, 0), role, epoch, std::move(replicas),
                                                 std::move(isr), log.get(), &t);
        newCoordinator();
    }
    // A fresh coordinator (store + service) over the SAME replicated log: what a
    // failover to this node's next coordinator incarnation looks like.
    void newCoordinator() {
        tcs.reset();
        store = std::make_unique<tx::ReplicatedTxnStateStore>(rep.get());
        tcs = std::make_unique<TransactionCoordinationService>(store.get(), nullptr,
                                                              [this] { return consensusLeader; },
                                                              [] { return NodeId{1}; });
        tcs->setAppendFnOverride([this](std::string_view, int32_t, std::span<const uint8_t> b) -> int64_t {
            tx::ControlMarkerPayload p;
            if (tx::ControlMarker::extract(b.data(), 0, p, b.size()))
                markers.push_back({p.controlType == tx::ControlType::Commit, p.coordinatorEpoch});
            return static_cast<int64_t>(markers.size()) - 1;
        });
    }
    Init initPid(const std::string& txnId, int32_t timeoutMs = 60'000, int64_t now = 1) {
        pc::InitProducerId q; q.sessionId = 1; q.correlationId = ++corr; q.transactionTimeoutMs = timeoutMs;
        q.transactionalId = {reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size()};
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = tcs->handleInitProducerId(f.data(), f.size(), now, out.data(), out.size());
        auto r = pc::InitProducerIdResponse::decode(out.data(), n);
        if (!r) return {};
        return {r->errorCode, r->producerId, r->producerEpoch};
    }
    int16_t addPartition(const std::string& txnId, const Init& ip, int64_t now = 1) {
        const std::span<const uint8_t> tid(reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size());
        pc::AddPartitionsToTxn q; q.sessionId = 1; q.correlationId = ++corr; q.producerId = ip.pid;
        q.producerEpoch = ip.epoch; q.transactionalId = tid;
        const pc::TxnTopicPartition tp{0, 0};
        std::vector<uint8_t> f(pc::AddPartitionsToTxn::encodedSizeFor(tid, 1));
        q.encode(f.data(), std::span<const pc::TxnTopicPartition>(&tp, 1));
        const auto n = tcs->handleAddPartitions(f.data(), f.size(), now, out.data(), out.size());
        auto r = pc::AddPartitionsToTxnResponse::decode(out.data(), n);
        return (r && r->results.size() == 1) ? r->results[0].errorCode : int16_t{-1};
    }
    // Follower 2 reports its log end (a FetchRequest at `offset`).
    void fetchFrom2(int64_t offset, int64_t now) {
        rune::replication::FetchRequestMsg q{};
        q.leaderEpoch = rep->epoch(); q.partition = 0; q.followerId = 2; q.fetchOffset = offset;
        q.maxBytes = 1 << 20; q.topicId = 99;
        (void)rep->onFetchRequest(q, now);
    }
    // One txn-state record written straight to the replica (a previous coordinator's put).
    void putRecord(const std::string& txnId, tx::TxnState s, int64_t pid) {
        tx::ReplicatedTxnStateStore other(rep.get());
        tx::TransactionMetadata m;
        m.txnId = txnId; m.producerId = pid; m.producerEpoch = 0; m.state = s; m.txnStartNs = 1;
        m.partitions.insert(tx::TopicPartition{"t", 0});
        ASSERT_TRUE(other.put(m, 1));
    }
};
} // namespace

TEST(TxnCoordinatorLoadRegression, NotCoordinatorUntilLeaderComplete) {
    // K4: a new txn-state leader whose leadership is not yet complete serves an
    // HW held at the old floor — the coordinator must not load (nor answer, nor
    // write) until leaderComplete_ holds.
    CoordRig R(Role::Follower, 1, {1, 2}, {1, 2});
    R.rep->becomeLeader(2, {1, 2});
    ASSERT_FALSE(R.rep->isLeaderComplete()) << "precondition: follower 2 has not reported in";
    EXPECT_EQ(R.initPid("t").err, kNotCoordinator) << "loaded before leaderComplete_";
    EXPECT_EQ(R.log->logEndOffset(), 0) << "an unloaded coordinator wrote to the txn-state log";
    R.fetchFrom2(0, 2);
    ASSERT_TRUE(R.rep->isLeaderComplete());
    EXPECT_NE(R.initPid("t").err, kNotCoordinator) << "loaded once leaderComplete_ and HW == LEO hold";
    R.fetchFrom2(R.log->logEndOffset(), 3);                 // the pid-block ceiling commits
    EXPECT_EQ(R.initPid("t").err, 0);
}

TEST(TxnCoordinatorLoadRegression, NotCoordinatorUntilHwEqualsLeo) {
    // K4/Q2: a previous coordinator's record above the HW may still be truncated;
    // the coordinator loads only once HW == LEO (it is then committed).
    CoordRig R(Role::Leader, 1, {1, 2}, {1, 2});
    R.putRecord("t-prev", tx::TxnState::Ongoing, 5);
    ASSERT_EQ(R.log->logEndOffset(), 1);
    ASSERT_EQ(R.rep->highWatermark(), 0) << "precondition: follower 2 has not replicated it";
    EXPECT_EQ(R.initPid("t").err, kNotCoordinator) << "loaded while HW < LEO";
    EXPECT_EQ(R.log->logEndOffset(), 1) << "an unloaded coordinator wrote to the txn-state log";
    R.fetchFrom2(1, 2);
    ASSERT_EQ(R.rep->highWatermark(), R.rep->logEndOffset());
    EXPECT_NE(R.initPid("t").err, kNotCoordinator);
    const auto* prev = R.store->get("t-prev");
    ASSERT_NE(prev, nullptr) << "the load folds the committed log";
    EXPECT_EQ(prev->state, tx::TxnState::Ongoing);
}

TEST(TxnCoordinatorLoadRegression, NoTimeoutsWhileUnloaded) {
    CoordRig R(Role::Leader, 1, {1}, {1});
    const auto ip = R.initPid("t", /*timeoutMs=*/100, /*now=*/1);
    ASSERT_EQ(ip.err, 0);
    ASSERT_EQ(R.addPartition("t", ip, 1), 0);
    // The txn-state leadership moved away; the consensus view has not caught up.
    R.rep->becomeFollower(2, 2);
    const int64_t leo = R.log->logEndOffset();
    (void)R.tcs->doWork(10'000'000'000LL);
    EXPECT_TRUE(R.markers.empty()) << "an unloaded coordinator decided a timeout ABORT";
    EXPECT_EQ(R.log->logEndOffset(), leo);
    // Leadership returns (epoch 3): the Ongoing timeout clock restarts at the load.
    R.rep->becomeLeader(3, {1});
    (void)R.tcs->doWork(10'000'000'000LL);
    EXPECT_TRUE(R.markers.empty()) << "the reloaded Ongoing txn must get a fresh timeout window";
    (void)R.tcs->doWork(10'000'000'000LL + 200'000'000LL);
    ASSERT_EQ(R.markers.size(), 1u) << "the timeout runs once loaded";
    EXPECT_FALSE(R.markers[0].commit);
    EXPECT_EQ(R.markers[0].coordinatorEpoch, 3);
}

TEST(TxnCoordinatorLoadRegression, CommittedPrepareAboveStaleFloorIsNeverMissed) {
    // Replaces revision 1's RebuildLoadsCommittedPrepareAboveLocalHw.  Node 1
    // replicated the old leader's Ongoing + PrepareCommit records but never
    // learned that the PrepareCommit committed (its HW view is 0).  On election
    // its served HW is held at that stale floor until leadership completes; a
    // coordinator that loaded to it would see Ongoing and later time it out to
    // ABORT — breaking the committed decision.
    TmpDir od;
    Log oldLog(od.path.string(), 64LL * 1024 * 1024); oldLog.recover();
    NullTransport ot;
    PartitionReplica oldLeader(cfg(2, 1, 99, 0), Role::Leader, 1, {2}, {2}, &oldLog, &ot);
    {
        tx::ReplicatedTxnStateStore s(&oldLeader);
        tx::TransactionMetadata m;
        m.txnId = "t-fo"; m.producerId = 5; m.producerEpoch = 0; m.txnTimeoutMs = 100; m.txnStartNs = 1;
        m.partitions.insert(tx::TopicPartition{"t", 0});
        m.state = tx::TxnState::Ongoing;       ASSERT_TRUE(s.put(m, 1));
        m.state = tx::TxnState::PrepareCommit; ASSERT_TRUE(s.put(m, 1));
    }
    const auto bytes = oldLog.read(0, 1 << 20);
    CoordRig R(Role::Follower, 1, {1, 2}, {1, 2});
    rune::replication::FetchResponseMsg resp{};
    resp.leaderEpoch = 1; resp.partition = 0; resp.highWatermark = 0; resp.logStartOffset = 0;
    resp.errorCode = 0; resp.topicId = 99; resp.records = std::span<const uint8_t>(bytes);
    R.rep->applyReplicated(resp);
    ASSERT_EQ(R.log->logEndOffset(), 2) << "precondition: both records replicated";
    R.rep->becomeLeader(2, {1, 2});
    ASSERT_FALSE(R.rep->isLeaderComplete());
    ASSERT_EQ(R.rep->highWatermark(), 0) << "precondition: served HW held at the stale floor";
    for (int i = 0; i < 4; ++i) (void)R.tcs->doWork(10'000'000'000LL + i);
    EXPECT_TRUE(R.markers.empty()) << "decided before leadership completed";
    EXPECT_EQ(R.initPid("x").err, kNotCoordinator);
    // The old leader (node 2) rejoins as a follower at the same log end.
    R.fetchFrom2(2, 3);
    ASSERT_TRUE(R.rep->isLeaderComplete());
    for (int i = 0; i < 4; ++i) (void)R.tcs->doWork(20'000'000'000LL + i);
    const auto* meta = R.store->get("t-fo");
    ASSERT_NE(meta, nullptr) << "the load must see the committed records";
    EXPECT_NE(meta->state, tx::TxnState::PrepareAbort) << "an ABORT was decided over a committed PrepareCommit";
    ASSERT_EQ(R.markers.size(), 1u) << "the committed PrepareCommit must be re-driven exactly once";
    EXPECT_TRUE(R.markers[0].commit) << "the re-driven marker must be the COMMIT";
}

TEST(TxnCoordinatorLoadRegression, ConsensusLeaderButTxnReplicaFollowerIsNotCoordinator) {
    // No put may reach a follower replica (on base: appendKeyed asserts in Debug,
    // appends on the follower under NDEBUG) — so this runs in a child process.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({
        CoordRig R(Role::Follower, 1, {1, 2}, {1, 2});
        R.consensusLeader = true;
        const auto r = R.initPid("t");
        (void)R.tcs->doWork(1);
        std::_Exit(r.err == kNotCoordinator && R.log->logEndOffset() == 0 ? 0 : 1);
    }, ::testing::ExitedWithCode(0), "");
}

TEST(TxnCoordinatorLoadRegression, CoordinatorEpochIsTxnReplicaLeaderEpoch) {
    CoordRig R(Role::Leader, 7, {1}, {1});
    const auto ip = R.initPid("t", /*timeoutMs=*/100, 1);
    ASSERT_EQ(ip.err, 0);
    ASSERT_EQ(R.addPartition("t", ip, 1), 0);
    const auto* meta = R.store->get("t");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->coordinatorEpoch, 7) << "persisted records carry the txn replica's leader epoch";
    (void)R.tcs->doWork(1'000'000'000LL);                    // timeout ABORT
    ASSERT_EQ(R.markers.size(), 1u);
    EXPECT_EQ(R.markers[0].coordinatorEpoch, 7) << "markers carry the txn replica's leader epoch";
}

TEST(TxnCoordinatorLoadRegression, CoordinatorRoutedToTxnStateLeader) {
    // Routing invariant (§5.1 Minor): FindCoordinator (coordinatorFor) routes to
    // the consensus leader; the coordinator loads only where the
    // __transaction_state replica is Leader.  Today the bridge makes the
    // consensus leader the txn-state leader, so the routed node is the loaded one
    // and its coordinatorEpoch is that leadership's epoch.  When multi-node
    // partition leadership lands (F15-FU2) coordinatorFor must switch to the
    // __transaction_state partition leader; this test then still pins it.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    ReplicaRegistry reg;
    reg.add(-2, 0, std::make_unique<PartitionReplica>(cfg(1, 1, -2, 0), Role::Follower, 0,
                                                      std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &t));
    PartitionReplica* txnRep = reg.lookup(-2, 0);
    ReplicationMetadataBridge bridge(1, {1}, reg);
    int32_t consensusLeader = -1;
    tx::ReplicatedTxnStateStore store(txnRep);
    TransactionCoordinationService tcs(&store, nullptr, [&] { return consensusLeader == 1; },
                                       [&] { return NodeId{consensusLeader}; });
    std::array<uint8_t, 1024> out{};
    auto init = [&](const std::string& id) {
        pc::InitProducerId q; q.sessionId = 1; q.correlationId = 1; q.transactionTimeoutMs = 60'000;
        q.transactionalId = {reinterpret_cast<const uint8_t*>(id.data()), id.size()};
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        auto r = pc::InitProducerIdResponse::decode(out.data(),
                                                    tcs.handleInitProducerId(f.data(), f.size(), 1, out.data(), out.size()));
        return r ? r->errorCode : int16_t{-1};
    };
    const std::string id = "t-route";
    for (const auto& [leader, term] : std::vector<std::pair<int32_t, int64_t>>{{1, 4}, {2, 5}, {1, 6}}) {
        consensusLeader = leader;
        (void)bridge.doWork(leader, term);
        const bool routedHere = tcs.coordinatorFor(id) == 1;
        const bool txnLeaderHere = txnRep->role() == Role::Leader;
        ASSERT_EQ(routedHere, txnLeaderHere) << "term " << term << ": FindCoordinator and the txn-state leader diverged";
        const int64_t leo = log.logEndOffset();
        const auto err = init(id);
        if (routedHere) {
            EXPECT_EQ(err, 0) << "term " << term << ": the routed node must serve";
            const auto* meta = store.get(id);
            ASSERT_NE(meta, nullptr);
            EXPECT_EQ(meta->coordinatorEpoch, term) << "coordinatorEpoch == the txn-state leader epoch (term)";
        } else {
            EXPECT_EQ(err, kNotCoordinator) << "term " << term;
            EXPECT_EQ(log.logEndOffset(), leo);
        }
    }
}

TEST(TxnCoordinatorLoadRegression, EpochChangeUnloadsAndReloads) {
    // m3: an epoch change while still leader = unload (deferred EndTxn answered
    // NotCoordinator, marker tasks dropped) then load under the new epoch, which
    // re-drives the committed PrepareCommit and persists its completion.
    TxnRig T(1, /*withStore=*/true);
    ASSERT_EQ(T.initPid(), 0);
    ASSERT_EQ(T.addPartition(), 0);
    T.produceTxn();
    ASSERT_EQ(T.endTxn(/*commit=*/true), 999);
    T.stateReplica->becomeLeader(2, {1});
    (void)T.tcs->doWork(50);
    ASSERT_EQ(T.controlFrames.size(), 1u) << "the deferred EndTxn must be answered on unload";
    EXPECT_EQ(T.endTxnFrameError(0).value_or(-1), kNotCoordinator);
    T.runUntilNoMarkerAwaited(60);
    EXPECT_EQ(T.tcs->awaitingMarkerTxnsForTest(), 0u);
    const auto* meta = T.store->get(T.txnId);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, tx::TxnState::CompleteCommit) << "the reloaded PrepareCommit completes";
    EXPECT_EQ(meta->coordinatorEpoch, 2) << "under the new coordinatorEpoch";
    EXPECT_EQ(T.controlFrames.size(), 1u) << "no second answer to the old EndTxn";
}

// ============================================================================
// I7 — the pid block ceiling is persisted in __transaction_state.
// ============================================================================

TEST(TxnPidBlockRegression, IdempotentPidsNotReissuedAfterFailover) {
    CoordRig R(Role::Leader, 1, {1}, {1});
    int64_t maxIssued = -1;
    for (int i = 0; i < 3; ++i) {
        const auto r = R.initPid("");                          // idempotent: nothing else is persisted
        ASSERT_EQ(r.err, 0);
        maxIssued = std::max(maxIssued, r.pid);
    }
    R.newCoordinator();                                        // failover to a fresh coordinator
    const auto idem = R.initPid("");
    ASSERT_EQ(idem.err, 0);
    EXPECT_GT(idem.pid, maxIssued) << "an idempotent pid issued by the previous coordinator was reissued";
    const auto txn = R.initPid("t");
    ASSERT_EQ(txn.err, 0);
    EXPECT_GT(txn.pid, maxIssued);
}

TEST(TxnPidBlockRegression, AllocationWaitsForCommittedCeiling) {
    CoordRig R(Role::Leader, 1, {1, 2}, {1, 2});               // HW moves only when follower 2 fetches
    const auto r = R.initPid("t");
    EXPECT_EQ(r.err, kConcurrent) << "a pid was handed out before its block ceiling committed";
    EXPECT_EQ(R.log->logEndOffset(), 1) << "only the ceiling record is written";
    EXPECT_EQ(R.initPid("").err, kConcurrent) << "idempotent InitProducerId waits too";
    R.fetchFrom2(R.log->logEndOffset(), 2);
    const auto ok = R.initPid("t");
    EXPECT_EQ(ok.err, 0) << "allocates once the ceiling is committed";
    EXPECT_GE(ok.pid, 0);
}

// ============================================================================
// F15 S7 fix round 1 — I1: the pid block ceiling record is COMPACTED to the
// latest (reserved non-empty key); concern 3: the fence flag survives a reload.
// ============================================================================
namespace {
// Every record value in [0, LEO) of `log`, across all segments.
std::vector<std::string> allValues(Log& log) {
    std::vector<std::string> out;
    int64_t off = 0;
    for (int guard = 0; guard < 100'000 && off < log.logEndOffset(); ++guard) {
        const auto raw = log.read(off, 1 << 20);
        if (raw.empty()) {                                   // a compaction gap: next segment base
            int64_t nextBase = -1;
            for (const auto b : log.segmentBaseOffsets()) if (b > off) { nextBase = b; break; }
            if (nextBase < 0) break;
            off = nextBase;
            continue;
        }
        std::size_t pos = 0;
        int64_t next = off;
        while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= raw.size()) {
            rune::protocol::RecordBatchReader rd(raw.data() + pos, raw.size() - pos);
            const int32_t total = rd.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > raw.size()) break;
            rd.forEach([&](std::string_view, std::string_view v, int64_t, int64_t) { out.emplace_back(v); });
            next = rd.baseOffset() + rd.lastOffsetDelta() + 1;
            pos += static_cast<std::size_t>(total);
        }
        if (next <= off) break;
        off = next;
    }
    return out;
}
int countCeilingRecords(Log& log) {
    int n = 0;
    for (const auto& v : allValues(log)) {
        uint32_t magic = 0;
        if (v.size() >= 4) std::memcpy(&magic, v.data(), 4);
        if (magic == tx::ReplicatedTxnStateStore::kPidBlockMagic) ++n;
    }
    return n;
}
} // namespace

TEST(TxnPidBlockRegression, CeilingRecordIsCompactedToLatest) {
    // §5.6 "Compaction keeps the latest": every ceiling record shares one
    // compaction key, so a clean leaves exactly the newest (= the max: ceilings
    // are monotone) and the load scan stays bounded.
    TmpDir d;
    Log log(d.path.string(), /*segmentBytes=*/1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1, 1, 99, 0), Role::Leader, 1, {1}, {1}, &log, &t);
    tx::ReplicatedTxnStateStore store(&rep);
    for (int64_t c = 1000; c <= 5000; c += 1000) ASSERT_GE(store.putPidBlockCeiling(c), 0);
    tx::TransactionMetadata pad;
    pad.txnId = "pad"; pad.producerId = 1; pad.producerEpoch = 0;
    while (log.segmentBaseOffsets().size() < 3) ASSERT_TRUE(store.put(pad, 1));
    ASSERT_EQ(countCeilingRecords(log), 5) << "precondition: five ceilings before the clean";
    rune::storage::LogCleaner cleaner;
    cleaner.clean(log, 0, log.segmentBaseOffsets().back(), /*nowNs=*/0, /*deleteRetentionMs=*/1'000'000);
    log.reloadSegments();
    EXPECT_EQ(countCeilingRecords(log), 1) << "the cleaner must keep only the latest ceiling record";
    tx::ReplicatedTxnStateStore reloaded(&rep);
    reloaded.rebuildFromLog();
    EXPECT_EQ(reloaded.persistedPidCeiling(), 5000) << "the surviving record is the max ceiling";
    ASSERT_NE(reloaded.get("pad"), nullptr) << "transactional records still fold";
}

TEST(TxnPidBlockRegression, ReservedKeyIsNotATransactionalId) {
    // The ceiling record's compaction key may never be a transactional id: a txn
    // record under it would be compacted against (or instead of) the ceiling.
    CoordRig R(Role::Leader, 1, {1}, {1});
    ASSERT_EQ(R.initPid("ok").err, 0) << "precondition: loaded, ceiling committed";
    const int64_t leo = R.log->logEndOffset();
    const std::string reserved = std::string("\xFF") + "__rune_pid_block";
    const auto r = R.initPid(reserved);
    EXPECT_NE(r.err, 0) << "the reserved ceiling key was accepted as a transactional id";
    EXPECT_NE(r.err, kConcurrent) << "non-retriable";
    EXPECT_EQ(R.log->logEndOffset(), leo) << "nothing persisted under the reserved key";
    EXPECT_EQ(R.store->get(reserved), nullptr);
}

TEST(TxnCoordinatorLoadRegression, FenceCallerAnswerSurvivesReload) {
    // Concern 3: the fence flag is persisted (header byte 27), so after a reload
    // the fence caller's EndTxn gets the same answer as before the reload: 27
    // (retriable), never 19.
    auto endTxnAfterFence = [](bool reload) {
        CoordRig R(Role::Leader, 1, {1}, {1});
        const auto ip = R.initPid("t-fence");
        EXPECT_EQ(ip.err, 0);
        EXPECT_EQ(R.addPartition("t-fence", ip), 0);
        const auto fence = R.initPid("t-fence");             // fences the Ongoing txn → PrepareAbort
        EXPECT_EQ(fence.err, 0);
        if (reload) R.newCoordinator();
        pc::EndTxn q; q.sessionId = 1; q.correlationId = ++R.corr; q.producerId = fence.pid;
        q.producerEpoch = fence.epoch; q.commit = 1;
        const std::string id = "t-fence";
        q.transactionalId = {reinterpret_cast<const uint8_t*>(id.data()), id.size()};
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = R.tcs->handleEndTxn(f.data(), f.size(), 2, R.out.data(), R.out.size());
        auto r = pc::EndTxnResponse::decode(R.out.data(), n);
        return r ? r->errorCode : int16_t{-1};
    };
    const int16_t noReload = endTxnAfterFence(false);
    ASSERT_EQ(noReload, kConcurrent) << "precondition: the resident answer";
    EXPECT_EQ(endTxnAfterFence(true), noReload) << "the fence flag was lost on reload";
}

TEST(TxnStateStoreRegression, RebuildCrossesCompactionGaps) {
    // Found on sight (fix round 1): after compaction a sealed segment's
    // superseded TAIL records are gone, so a read inside that segment past its
    // last survivor returns nothing.  rebuildFromLog stopped there and silently
    // dropped every later segment's txn state (a coordinator failover would
    // lose committed transactions).  It must skip to the next segment base, as
    // ReplicatedOffsetStore (CP-4) and ProducerStateManager (CP-3) do.
    TmpDir d;
    Log log(d.path.string(), /*segmentBytes=*/1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1, 1, 99, 0), Role::Leader, 1, {1}, {1}, &log, &t);
    tx::ReplicatedTxnStateStore store(&rep);
    tx::TransactionMetadata m;
    m.producerEpoch = 0;
    m.txnId = "early"; m.producerId = 1; ASSERT_TRUE(store.put(m, 1));
    m.txnId = "churn"; m.producerId = 2;
    for (int i = 0; i < 4; ++i) ASSERT_TRUE(store.put(m, 1));   // superseded tail of segment 0
    while (log.segmentBaseOffsets().size() < 2) ASSERT_TRUE(store.put(m, 1));
    m.txnId = "late"; m.producerId = 3; m.state = tx::TxnState::PrepareCommit;
    while (log.segmentBaseOffsets().size() < 3) ASSERT_TRUE(store.put(m, 1));
    rune::storage::LogCleaner cleaner;
    cleaner.clean(log, 0, log.segmentBaseOffsets().back(), 0, 1'000'000);
    log.reloadSegments();
    tx::ReplicatedTxnStateStore reloaded(&rep);
    reloaded.rebuildFromLog();
    EXPECT_NE(reloaded.get("early"), nullptr);
    ASSERT_NE(reloaded.get("late"), nullptr) << "the rebuild stopped at a compaction gap";
    EXPECT_EQ(reloaded.get("late")->state, tx::TxnState::PrepareCommit);
}

// ============================================================================
// F15 S3 — m5 + I8 (design note §6, §1.1 L2): ONE return FIFO in every mode.
// Nothing the worker emits is ever dropped (Legacy included); the FIFO is
// bounded (op drain pauses at kOverflowHighWater; parked waiters statically
// capped by the G-A6 reservation); a claim / encode exception halts (exit 70);
// drainReturn dispatches on the SBE template id (TxnReturn = 6, review m7).
// ============================================================================
namespace {
constexpr int kExitInvariant = 70;   // BrokerNode::kExitInvariantViolation
WorkerOp acksAllOp(PartitionReplica* r, int64_t corr) {
    WorkerOp op = produceOp(r, corr);
    op.deferAck = true;   // acks=all
    return op;
}
WorkerOp pushReadOp(PartitionReplica* r, int64_t subId, int64_t nextOffset, int64_t hwBound) {
    WorkerOp op;
    op.type = WorkerOp::Type::PushRead; op.topicId = r->topicId();
    op.partition = static_cast<int32_t>(r->partitionId()); op.replica = r; op.logPtr = r->log();
    op.sessionId = 1; op.subscriptionId = subId; op.nextOffset = nextOffset; op.hwBound = hwBound;
    return stamp(std::move(op));
}
std::vector<uint8_t> bigBatch(std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", std::string(valueBytes, 'x'), 1000);
    return b.build();
}
void followerCaughtUp(PartitionReplica& r, int64_t now) {
    rune::replication::FetchRequestMsg fr{r.epoch(), 0, 2, r.log()->logEndOffset(), 1 << 20, r.topicId()};
    (void)r.onFetchRequest(fr, now);
}
// Answers only (the S2 temporary facts-only return carries deferAck == true).
void collectAnswers(Worker& w, std::vector<WorkerReturn>& out) {
    w.drainReturn([&](WorkerReturn r) { if (!r.deferAck) out.push_back(std::move(r)); });
}
// Drain + run until `want` answers arrived (bounded passes; no sleeps).
void drainUntil(Worker& w, std::vector<WorkerReturn>& out, std::size_t want, int64_t& now) {
    for (int i = 0; i < 4096 && out.size() < want; ++i) {
        collectAnswers(w, out);
        w.doWork(++now);
    }
    collectAnswers(w, out);
}
bool correlationsAscending(const std::vector<WorkerReturn>& v) {
    for (std::size_t i = 1; i < v.size(); ++i)
        if (v[i].correlationId <= v[i - 1].correlationId) return false;
    return true;
}
} // namespace

TEST(ReturnOverflowRegression, LegacyFullRingNeverDropsProduceResult) {
    // Default (Legacy) config: no per-worker ingress.  The 64 KiB return ring
    // holds ~400 ProduceResults; the control thread does not drain while the
    // worker answers 1000 produces.  Base: "ret_ ring full — WorkerReturn
    // dropped" (Worker.h ~925) — every answer past the ring is lost.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 23, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    int64_t now = 1;
    // Phase 1 — immediate answers (acks=leader) through a full ring.
    constexpr int kN = 1000;
    for (int i = 0; i < kN; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, i + 1)));
    w.doWork(++now);
    ASSERT_EQ(log.logEndOffset(), kN);
    std::vector<WorkerReturn> answers;
    drainUntil(w, answers, kN, now);
    ASSERT_EQ(answers.size(), static_cast<std::size_t>(kN)) << "Legacy dropped ProduceResults on a full return ring";
    EXPECT_TRUE(correlationsAscending(answers)) << "answers reordered";
    for (const auto& a : answers) EXPECT_EQ(a.errorCode, 0);
    // Phase 2 — the acks=all FIRE path (addendum S3): 100 waiters parked, the
    // ring then filled by 600 acks=leader answers, then the HW passes: every
    // fired acks=all answer must reach the gateway — never dropped.
    answers.clear();
    constexpr int kParked = 100;
    for (int i = 0; i < kParked; ++i) ASSERT_TRUE(w.tryEnqueueOp(acksAllOp(&rep, 10'000 + i)));
    w.doWork(++now);
    collectAnswers(w, answers);   // facts-only returns drained; nothing answered yet
    ASSERT_TRUE(answers.empty());
    ASSERT_EQ(w.reservedAckSlotsForTest(), kParked) << "precondition: acks=all parked on the worker";
    constexpr int kFill = 600;
    for (int i = 0; i < kFill; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 20'000 + i)));
    w.doWork(++now);              // ring full of acks=leader answers
    followerCaughtUp(rep, now);   // HW passes every parked waiter
    w.doWork(++now);              // fire into a full ring
    drainUntil(w, answers, kParked + kFill, now);
    std::size_t firedOk = 0, fillOk = 0;
    for (const auto& a : answers) {
        if (a.errorCode != 0) continue;
        if (a.correlationId >= 10'000 && a.correlationId < 10'000 + kParked) ++firedOk;
        if (a.correlationId >= 20'000) ++fillOk;
    }
    EXPECT_EQ(fillOk, static_cast<std::size_t>(kFill)) << "acks=leader answers dropped";
    EXPECT_EQ(firedOk, static_cast<std::size_t>(kParked)) << "fired acks=all answers dropped on a full ring";
    EXPECT_EQ(w.reservedAckSlotsForTest(), 0);
}

TEST(ReturnOverflowRegression, BoundedAndNeverDrops) {
    // Both modes.  6 × 1000 produces while the control thread never drains:
    // the return FIFO must stay bounded (op drain pauses at the high-water
    // mark; the rest waits in the op ring) and, once drained, every answer
    // arrives exactly once and in order.  Base: Legacy drops; per-worker mode
    // grows retOverflow_ without bound (and erases in O(n)).
    for (const bool perWorker : {false, true}) {
        SCOPED_TRACE(perWorker ? "per-worker ingress" : "Legacy");
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
        Worker w(0, 1 << 23, 1 << 16, 64, 64);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        w.setWorkerIngress(nullptr, perWorker);
        int64_t now = 1;
        std::size_t maxFifo = 0;
        constexpr int kRounds = 6, kPer = 1000;
        for (int r = 0; r < kRounds; ++r) {
            for (int i = 0; i < kPer; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, r * kPer + i + 1)));
            for (int p = 0; p < 4; ++p) {
                w.doWork(++now);
                maxFifo = std::max(maxFifo, w.retFifoSizeForTest());
            }
        }
        EXPECT_LE(maxFifo, Worker::kOverflowHighWater + 1024u) << "return FIFO unbounded (high water + one op drain)";
        EXPECT_LE(maxFifo, Worker::kRetFifoCapacity);
        std::vector<WorkerReturn> answers;
        drainUntil(w, answers, kRounds * kPer, now);
        ASSERT_EQ(answers.size(), static_cast<std::size_t>(kRounds * kPer)) << "answers dropped";
        EXPECT_TRUE(correlationsAscending(answers)) << "answers reordered";
        EXPECT_EQ(log.logEndOffset(), kRounds * kPer);
        EXPECT_EQ(w.retFifoSizeForTest(), 0u);
    }
}

TEST(ReturnOverflowRegression, OpDrainPausesAtHighWater) {
    // Every mode: once the FIFO holds >= kOverflowHighWater returns, the worker
    // stops draining its op ring (control-side producers already queue on a
    // full op ring), and resumes once the control thread drains.
    for (const bool perWorker : {false, true}) {
        SCOPED_TRACE(perWorker ? "per-worker ingress" : "Legacy");
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
        Worker w(0, 1 << 23, 1 << 16, 64, 64);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        w.setWorkerIngress(nullptr, perWorker);
        int64_t now = 1;
        constexpr int kN = 2000;
        for (int i = 0; i < kN; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, i + 1)));
        for (int p = 0; p < 8 && w.retFifoSizeForTest() < Worker::kOverflowHighWater; ++p) w.doWork(++now);
        ASSERT_GE(w.retFifoSizeForTest(), Worker::kOverflowHighWater)
            << "precondition: the return FIFO holds the answers the full ring refused";
        const int64_t leo = log.logEndOffset();
        ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, kN + 1)));
        w.doWork(++now);
        EXPECT_EQ(log.logEndOffset(), leo) << "the op drain must pause at the high-water mark";
        EXPECT_GT(w.retFifoHighWaterPausesForTest(), 0u);
        w.doWork(++now);                                     // a second paused pass, same episode
        EXPECT_GE(w.retFifoHighWaterPausesForTest(), 2u);
        EXPECT_EQ(w.retFifoPauseEpisodesForTest(), 1u) << "pause episodes are counted (and logged) per episode, not per pass";
        std::vector<WorkerReturn> answers;
        drainUntil(w, answers, kN + 1, now);
        ASSERT_EQ(answers.size(), static_cast<std::size_t>(kN + 1));
        EXPECT_TRUE(correlationsAscending(answers));
        EXPECT_EQ(log.logEndOffset(), kN + 1) << "the paused ops run once the FIFO drains";
    }
}

TEST(ReturnOverflowRegression, ClaimExceptionHalts) {
    // A return the ring can never hold makes tryClaim throw.  Base logged
    // "tryClaim failed" and DROPPED it.  Now: an invariant-violation halt (exit
    // 70), never a drop.  (Fix round 2: a PushRead of an oversize STORED batch
    // is no longer this case — it answers the subscription an error, see
    // PushReadOfBatchLargerThanReturnRingAnswersErrorNoHalt — so the fault is
    // injected as a return carrying a chunk larger than the ring's 8 KiB max
    // message, e.g. a refused marker echo.)
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({
        Worker w(0, 1 << 20, 1 << 16, 64, 64);
        WorkerReturn r;
        r.type = WorkerReturn::Type::ControlRecordRefused;
        r.chunk.assign(16 * 1024, 0xAB);
        w.enqueueReturnForTest(r);
        std::_Exit(0);
    }, ::testing::ExitedWithCode(kExitInvariant), "");
}

TEST(ReturnOverflowRegression, FlushClaimExceptionHalts) {
    // The same oversize return queued BEHIND a full ring: the FIFO flush hits
    // the claim exception.  Base: flushRetOverflow_'s catch erased it and
    // continued (a silent drop).  Now: halt (exit 70).
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
        Worker w(0, 1 << 22, 1 << 16, 64, 64);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        for (int i = 0; i < 800; ++i) (void)w.tryEnqueueOp(produceOp(&rep, 1 + i));   // fill the ring
        w.doWork(1);
        if (w.retFifoSizeForTest() == 0) std::_Exit(2);                              // precondition
        WorkerReturn r;
        r.type = WorkerReturn::Type::ControlRecordRefused;
        r.chunk.assign(16 * 1024, 0xAB);
        w.enqueueReturnForTest(r);                                                   // queued behind
        int64_t now = 1;
        for (int i = 0; i < 64; ++i) { w.doWork(++now); (void)drainAll(w); }
        std::_Exit(0);
    }, ::testing::ExitedWithCode(kExitInvariant), "");
}

TEST(ReturnOverflowRegression, TxnReturnDispatchedByTemplateIdInFifoOrder) {
    // Review m7: drainReturn dispatches on the SBE template id; a TxnReturn
    // (id 6) reaches its own callback, in emission order with the
    // ProduceResults — directly through the ring and through the FIFO.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 23, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    std::vector<int64_t> seq;   // ProduceResult → correlationId; TxnReturn → -token
    auto drain = [&] {
        w.drainReturn([&](WorkerReturn r) { seq.push_back(r.correlationId); },
                      [&](const TxnReturn& r) { seq.push_back(-r.token); });
    };
    TxnReturn txn;
    txn.kind = TxnReturn::Kind::ControlRecordCommitted; txn.topicId = 1; txn.partition = 0;
    txn.incarnation = 3; txn.leaderEpoch = 1; txn.token = 77;
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 1)));
    w.doWork(1);
    w.enqueueTxnReturnForTest(txn);
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 2)));
    w.doWork(2);
    drain();
    ASSERT_EQ(seq, (std::vector<int64_t>{1, -77, 2}));
    // Through the FIFO: the ring is full when the TxnReturn is emitted.
    seq.clear();
    constexpr int kFill = 800;
    for (int i = 0; i < kFill; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 100 + i)));
    w.doWork(3);
    txn.token = 78;
    w.enqueueTxnReturnForTest(txn);
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 5000)));
    int64_t now = 3;
    for (int i = 0; i < 64 && seq.size() < kFill + 2u; ++i) { drain(); w.doWork(++now); }
    drain();
    ASSERT_EQ(seq.size(), kFill + 2u);
    EXPECT_EQ(seq[kFill], -78) << "the TxnReturn must follow every earlier ProduceResult";
    EXPECT_EQ(seq[kFill + 1], 5000) << "and precede every later one";
}

TEST(ReturnOverflowRegression, ParkedAcksAllStaticallyBounded) {
    // Addendum S3 (S2 concern 5): pendingAcksAll_ is bounded by a STATIC cap
    // tied to the G-A6 reservation, not by ret-ring capacity (~127k on the
    // default 32 MiB ring).  A gateway-routed burst beyond the cap: the excess
    // is answered retriable (NotLeader) before any append, nothing dropped, no
    // halt; one HW advance then fires every parked waiter exactly once.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 23);   // default 32 MiB return ring
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    const int total = static_cast<int>(Worker::kMaxOwedReturns) + 500;
    for (int i = 0; i < total; ++i) ASSERT_TRUE(w.tryEnqueueOp(acksAllOp(&rep, i + 1)));
    std::vector<WorkerReturn> answers;
    int64_t now = 1;
    for (int p = 0; p < 16; ++p) { w.doWork(++now); collectAnswers(w, answers); }
    const int64_t parked = w.reservedAckSlotsForTest();
    EXPECT_LE(parked, static_cast<int64_t>(Worker::kMaxOwedReturns)) << "parked acks=all waiters exceed the static cap";
    EXPECT_EQ(parked + static_cast<int64_t>(answers.size()), total) << "every produce answered or parked";
    for (const auto& a : answers) EXPECT_EQ(a.errorCode, kNotLeader);
    EXPECT_EQ(log.logEndOffset(), parked) << "a refused produce must not be appended";
    followerCaughtUp(rep, now);
    answers.clear();
    drainUntil(w, answers, static_cast<std::size_t>(parked), now);
    std::size_t ok = 0;
    for (const auto& a : answers) ok += a.errorCode == 0 ? 1u : 0u;
    EXPECT_EQ(ok, static_cast<std::size_t>(parked)) << "every parked waiter fires exactly once";
    EXPECT_EQ(w.reservedAckSlotsForTest(), 0);
}

TEST(ReturnOverflowRegression, ShardIngressPollBudgetHonoursStaticBound) {
    // The shard / per-worker ingress poll budget K also respects the static
    // owed-return cap: with kMaxOwedReturns waiters parked, K == 0.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 20);   // default 32 MiB return ring
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    for (std::size_t i = 0; i < Worker::kMaxOwedReturns; ++i)
        ASSERT_EQ(ingressAcksAll(w, static_cast<int64_t>(i) + 1, 1), 1);
    ASSERT_EQ(w.reservedAckSlotsForTest(), static_cast<int64_t>(Worker::kMaxOwedReturns));
    EXPECT_EQ(w.ingressPollBudgetForTest(), 0) << "the poll budget ignores the static owed-return cap";
}

TEST(ReturnOverflowRegression, FactsOnlyReturnPrecedesItsAnswerThroughFullRing) {
    // The S2 temporary facts-only return (acks=all: at append; Strict: at fsync
    // completion) and the final answer share the ONE FIFO: through a full ring
    // neither is dropped and, per produce, the facts arrive before the answer.
    for (const uint8_t tier : {uint8_t{1}, uint8_t{2}}) {
        SCOPED_TRACE(tier == 2 ? "Strict acks=all" : "acks=all");
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1, tier), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
        Worker w(0, 1 << 23, 1 << 16, 64, 64);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        int64_t now = 1;
        constexpr int kFill = 600, kParked = 60;
        // The parked produces are admitted first (their G-A6 reservation needs
        // ring room); then acks=leader answers fill the ring, so the Strict
        // facts (emitted at fsync completion) and every final answer are
        // emitted into a FULL ring and must queue in the FIFO.
        w.setReapPausedForTest(true);
        for (int i = 0; i < kParked; ++i) {
            WorkerOp op = acksAllOp(&rep, 10'000 + i);
            op.durabilityTier = tier;
            ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
        }
        w.doWork(++now);
        ASSERT_EQ(w.reservedAckSlotsForTest(), tier == 2 ? 2 * kParked : kParked) << "precondition: all parked";
        for (int i = 0; i < kFill; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 1 + i)));
        std::vector<std::pair<int64_t, bool>> seq;   // (correlationId, facts-only?)
        auto drain = [&] { w.drainReturn([&](WorkerReturn r) { seq.emplace_back(r.correlationId, r.deferAck); }); };
        w.doWork(++now);                 // ring full of acks=leader answers
        w.setReapPausedForTest(false);
        EXPECT_GT(w.retFifoSizeForTest(), 0u) << "the ring is full: returns must queue in the FIFO";
        // Strict: run until every fsync completed (facts emitted at completion).
        for (int i = 0; i < kCap && w.durablePendingCountForTest() > 0; ++i) { w.doWork(++now); std::this_thread::yield(); }
        followerCaughtUp(rep, now);
        for (int i = 0; i < 4096 && seq.size() < static_cast<std::size_t>(kFill + 2 * kParked); ++i) { drain(); w.doWork(++now); }
        drain();
        ASSERT_EQ(seq.size(), static_cast<std::size_t>(kFill + 2 * kParked)) << "a facts-only return or an answer was dropped";
        for (int i = 0; i < kParked; ++i) {
            const int64_t corr = 10'000 + i;
            std::ptrdiff_t facts = -1, answer = -1;
            for (std::size_t k = 0; k < seq.size(); ++k) {
                if (seq[k].first != corr) continue;
                (seq[k].second ? facts : answer) = static_cast<std::ptrdiff_t>(k);
            }
            ASSERT_GE(facts, 0) << "facts dropped for " << corr;
            ASSERT_GE(answer, 0) << "answer dropped for " << corr;
            EXPECT_LT(facts, answer) << "the answer overtook its facts for " << corr;
        }
    }
}

// ============================================================================
// F15 S3 fix round 1 (review I-1, I-2, m1; owed-cap ruling).
// ============================================================================
TEST(ReturnOverflowRegression, ShardIngressOversizeBatchRejectedMessageTooLarge) {
    // Shard / per-worker ingress had no batch-size guard: a batch bounded only
    // by Aeron's max message (term / 8, up to 16 MiB) was appended, and its
    // later PushRead could exceed the return ring (halt, crash loop).  Now it is
    // answered MessageTooLarge (non-retriable, as the gateway does) and never
    // appended; a batch at the limit still appends.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 20, 64u << 20);   // 8 MiB max return message: the 8 MiB produce cap binds
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    const auto over = produceFrame(1, 9, bigBatch(Worker::kMaxProduceBatchBytes + 1024), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(over.data(), over.size(), 1), 1);
    auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(log.logEndOffset(), 0) << "an oversize batch must never be stored";
    const auto atLimit = produceFrame(1, 10, bigBatch(Worker::kMaxProduceBatchBytes - 4096), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(atLimit.data(), atLimit.size(), 2), 1);
    rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, 0);
    EXPECT_EQ(log.logEndOffset(), 1);
}

TEST(ReturnOverflowRegression, PushReadChunkCutAtWholeBatches) {
    // Review I-2: Log::read returns more than fits the return ring's max
    // message (8 KiB here) — the chunk is cut at whole batches: no halt, an
    // exact cut, the next read resumes at the next batch, every byte delivered
    // once and in order.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 20, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    for (const std::size_t kb : {3u, 6u, 3u}) {
        WorkerOp p = produceOp(&rep, static_cast<int64_t>(kb));
        p.recordBatchBytes = bigBatch(kb * 1024);
        ASSERT_TRUE(w.tryEnqueueOp(std::move(p)));
    }
    int64_t now = 1;
    w.doWork(++now);
    (void)drainAll(w);
    ASSERT_EQ(log.logEndOffset(), 3);
    const auto whole = log.read(0, 1 << 20);
    std::vector<uint8_t> delivered;
    std::vector<std::size_t> sizes;
    int64_t next = 0;
    for (int i = 0; i < 6 && next < 3; ++i) {
        ASSERT_TRUE(w.tryEnqueueOp(pushReadOp(&rep, 7, next, 3)));
        w.doWork(++now);
        std::vector<WorkerReturn> rets = drainAll(w);
        ASSERT_EQ(rets.size(), 1u);
        const auto& c = rets[0].chunk;
        ASSERT_FALSE(c.empty()) << "no progress at offset " << next;
        // Whole batches only: walk them and advance the cursor past the last.
        std::size_t pos = 0;
        while (pos < c.size()) {
            rune::protocol::RecordBatchReader r(c.data() + pos, c.size() - pos);
            ASSERT_GT(r.totalLength(), 0);
            ASSERT_LE(pos + static_cast<std::size_t>(r.totalLength()), c.size()) << "a cut inside a batch";
            next = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += static_cast<std::size_t>(r.totalLength());
        }
        sizes.push_back(c.size());
        delivered.insert(delivered.end(), c.begin(), c.end());
    }
    EXPECT_EQ(next, 3);
    ASSERT_EQ(sizes.size(), 3u) << "each read must be cut to exactly one batch here (8 KiB max message)";
    EXPECT_EQ(delivered, whole) << "every byte delivered once, in order";
}

TEST(ReturnOverflowRegression, BoundHoldsUnderWorstCaseMix) {
    // Review m1: owed waiters at the cap + an op burst into a full ring + the
    // HW firing every waiter, with the control thread not draining: the FIFO
    // stays within its static bound (no halt), and once drained every answer
    // arrives exactly once (acks=leader answers in order).
    TmpDir d;
    Log log(d.path.string(), 256LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 24, 1 << 20, 64, 64);   // 1 MiB return ring
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setAckSlotBytes(64);                    // reserve by the real ProduceResult size
    w.setIngressDrainLimit(Worker::kMaxIngressDrainLimit);
    int64_t now = 1;
    const int owed = static_cast<int>(w.maxOwedReturns()) - 1;
    for (int i = 0; i < owed; ++i) ASSERT_TRUE(w.tryEnqueueOp(acksAllOp(&rep, 100'000 + i)));
    std::vector<WorkerReturn> answers;
    for (int p = 0; p < 8; ++p) { w.doWork(++now); collectAnswers(w, answers); }
    ASSERT_TRUE(answers.empty()) << "precondition: every acks=all parked";
    ASSERT_EQ(w.reservedAckSlotsForTest(), owed);
    constexpr int kBurst = 12'000;            // > the 1 MiB ring's ~6.9k ProduceResults
    for (int i = 0; i < kBurst; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 1 + i)));
    std::size_t maxFifo = 0;
    for (int p = 0; p < 4; ++p) { w.doWork(++now); maxFifo = std::max(maxFifo, w.retFifoSizeForTest()); }
    followerCaughtUp(rep, now);               // HW passes every parked waiter
    for (int p = 0; p < 4; ++p) { w.doWork(++now); maxFifo = std::max(maxFifo, w.retFifoSizeForTest()); }
    EXPECT_GE(maxFifo, static_cast<std::size_t>(owed)) << "the mix must actually stress the FIFO";
    EXPECT_LE(maxFifo, Worker::kRetFifoBound);
    drainUntil(w, answers, static_cast<std::size_t>(kBurst + owed), now);
    ASSERT_EQ(answers.size(), static_cast<std::size_t>(kBurst + owed));
    std::vector<WorkerReturn> leader;
    std::size_t firedOk = 0;
    for (auto& a : answers) {
        if (a.correlationId >= 100'000) firedOk += a.errorCode == 0 ? 1u : 0u;
        else leader.push_back(a);
    }
    EXPECT_EQ(firedOk, static_cast<std::size_t>(owed));
    EXPECT_TRUE(correlationsAscending(leader));
}

TEST(ReturnOverflowRegression, ConfiguredOwedCapBoundsParkedWaiters) {
    // Owed-cap ruling: RUNE_WORKER_MAX_OWED_RETURNS (here 256) bounds the
    // parked waiters on both paths, and every cap refusal is counted.
    {
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
        Worker w(0, 1 << 22);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        w.setMaxOwedReturns(256);
        ASSERT_EQ(w.maxOwedReturns(), 256u);
        constexpr int kTotal = 400;
        for (int i = 0; i < kTotal; ++i) ASSERT_TRUE(w.tryEnqueueOp(acksAllOp(&rep, i + 1)));
        std::vector<WorkerReturn> answers;
        int64_t now = 1;
        for (int p = 0; p < 4; ++p) { w.doWork(++now); collectAnswers(w, answers); }
        const int64_t parked = w.reservedAckSlotsForTest();
        EXPECT_LE(parked, 256);
        EXPECT_EQ(parked + static_cast<int64_t>(answers.size()), kTotal);
        for (const auto& a : answers) EXPECT_EQ(a.errorCode, kNotLeader);
        EXPECT_EQ(w.owedCapThrottlesForTest(), answers.size()) << "every cap refusal is counted";
        EXPECT_EQ(w.owedCapEpisodesForTest(), 1u) << "one throttle episode (one enter log), not one per refusal";
    }
    {
        TmpDir d;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
        Worker w(0, 1 << 20);
        w.setOwnedDirect(Worker::OwnedSet{&rep});
        w.setWorkerIngress(nullptr, true);
        w.setMaxOwedReturns(256);
        for (int i = 0; i < 256; ++i) ASSERT_EQ(ingressAcksAll(w, i + 1, 1), 1);
        EXPECT_EQ(w.ingressPollBudgetForTest(), 0) << "the shard poll budget ignores the configured cap";
    }
}

// ============================================================================
// F15 S3 fix round 2 (re-review 1: I-3, I-4).
// ============================================================================
TEST(ReturnOverflowRegression, ShardIngressBatchLargerThanReturnRingRejectedMessageTooLarge) {
    // I-3: shard intake is bounded by THIS worker's return ring (a batch it
    // stores must fit one PushReadResult), not by the Aeron term size — so the
    // shipped shard configs (term >= ring) start, and no batch the ring cannot
    // carry is ever stored.  1 MiB return ring: max message 128 KiB.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 22, 1 << 20, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    const auto over = produceFrame(1, 9, bigBatch(200 * 1024), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(over.data(), over.size(), 1), 1);
    auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(log.logEndOffset(), 0) << "a batch the return ring cannot carry must never be stored";
    const auto fits = produceFrame(1, 10, bigBatch(100 * 1024), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(fits.data(), fits.size(), 2), 1);
    rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, 0);
    EXPECT_EQ(log.logEndOffset(), 1);
}

namespace {
[[noreturn]] void oversizeStoredBatchChild() {
        TmpDir d, d2;
        Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
        Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
        NullTransport t;
        PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
        PartitionReplica rep2(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &log2, &t);
        const auto big = bigBatch(16 * 1024);
        (void)log.append(big, 0);                          // stored under a bigger config
        const auto small = batchAt(0);
        (void)log2.append(small, 0);
        Worker w(0, 1 << 20, 1 << 16, 64, 64);             // now: 8 KiB return max message
        w.setOwnedDirect(Worker::OwnedSet{&rep, &rep2});
        (void)w.tryEnqueueOp(pushReadOp(&rep, 7, 0, 1));
        (void)w.tryEnqueueOp(pushReadOp(&rep2, 8, 0, 1));
        w.doWork(1);
        const auto rets = drainAll(w);
        const bool ok = rets.size() == 2u &&
            rets[0].subscriptionId == 7 && rets[0].chunk.empty() &&
            rets[0].errorCode == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge) &&
            rets[1].subscriptionId == 8 && rets[1].errorCode == 0 && !rets[1].chunk.empty() &&
            w.pushReadOversizeForTest() == 1u;
        std::_Exit(ok ? 0 : 1);
}
} // namespace

TEST(ReturnOverflowRegression, PushReadOfBatchLargerThanReturnRingAnswersErrorNoHalt) {
    // I-4: a batch ALREADY on disk that is larger than the current return
    // ring's max message (stored under a bigger earlier config, or received
    // from a peer and then led).  Base: the PushRead claim throws → exit 70 on
    // every restart (crash loop).  Now: that read answers MessageTooLarge with
    // an empty chunk, the oversize read is counted, and the worker keeps
    // serving other partitions.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(oversizeStoredBatchChild(), ::testing::ExitedWithCode(0), "");
}

namespace {
[[noreturn]] void gatewayOversizeChild() {
        auto subscribe = [](LegacyAcksRig& R, int64_t corr, int64_t from) {
            pc::Subscribe sub;
            sub.sessionId = R.sid; sub.correlationId = corr; sub.topicId = 0; sub.partition = 0;
            sub.partitionKnown = 1; sub.fromOffset = from; sub.isolation = 0;
            std::vector<uint8_t> frame(sub.encodedSize());
            sub.encode(frame.data());
            R.gw.applySubscribeForTest(frame.data(), frame.size());
        };
        LegacyAcksRig R({1});                                // pool rings 64 KiB: 8 KiB max message
        const auto big = bigBatch(16 * 1024);
        (void)R.r->log()->append(big, 0);                    // on disk from an earlier config
        const auto small = batchAt(1);
        (void)R.r->log()->append(small, 1);
        R.r->onLeaderLogAppended();
        R.runWorker();
        subscribe(R, 77, 0);                                 // will hit the oversize batch
        for (int i = 0; i < 8; ++i) { R.gw.pushSubscriptionsForTest(); R.runWorker(); }
        std::size_t tooLarge = 0;
        bool corrZero = true;
        for (const auto& a : R.answers)
            if (a.inlineError && a.code == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge)) {
                ++tooLarge;
                corrZero = corrZero && a.corr == 0;          // m-f: never a subscription id as a correlation id
            }
        if (tooLarge != 1u || !corrZero || R.w().pushReadOversizeForTest() != 1u) std::_Exit(1);
        // m-i: a healthy subscription (past the oversize batch) keeps flowing while the
        // failed one stays parked, and re-subscribing past the batch recovers.
        R.records.clear();
        subscribe(R, 78, 1);
        for (int i = 0; i < 8; ++i) { R.gw.pushSubscriptionsForTest(); R.runWorker(); }
        if (R.records.size() != 1u || R.records[0].second != 1) std::_Exit(2);
        if (R.w().pushReadOversizeForTest() != 1u) std::_Exit(3);   // the failed one never re-reads
        R.records.clear();
        subscribe(R, 79, 1);                                 // the client re-subscribes past the batch
        for (int i = 0; i < 8; ++i) { R.gw.pushSubscriptionsForTest(); R.runWorker(); }
        if (R.records.size() != 1u || R.records[0].second != 1) std::_Exit(4);
        std::_Exit(0);
}
} // namespace

TEST(ReturnOverflowRegression, GatewayFailsSubscriptionOnOversizeBatchWithoutSpinning) {
    // I-4, gateway side (default config): the subscription whose next batch
    // the return ring cannot carry gets ONE non-retriable ErrorResponse
    // (MessageTooLarge; correlationId 0 — never a subscription id, which the
    // client would match against a pending connect / ping, re-review 2 m-f)
    // and is parked — no further PushRead for it (no spin on the batch).
    // Re-review 2 m-i: a healthy subscription keeps flowing meanwhile, and a
    // re-subscribe past the batch recovers.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(gatewayOversizeChild(), ::testing::ExitedWithCode(0), "");
}

// ============================================================================
// F15 S3 fix round 3 (re-review 2: B-1, B-2, m-h).
// ============================================================================
namespace {
// A record batch of EXACTLY n bytes (bigBatch's value length searched).
std::vector<uint8_t> batchOfSize(std::size_t n) {
    for (std::size_t v = n > 256 ? n - 256 : 0; v <= n; ++v) {
        auto b = bigBatch(v);
        if (b.size() == n) return b;
        if (b.size() > n) break;
    }
    return {};
}
[[noreturn]] void gatewayOpRingOversizeChild() {
    LegacyAcksRig R({1});                                // pool rings 64 KiB: 8 KiB max op / return message
    const std::size_t limit = R.gw.maxProduceBatchBytesForTest();
    // A batch the op ring cannot carry, but under the 8 MiB gateway ceiling.
    // Base: tryEnqueueOp throws on the control thread → std::terminate.
    const auto over = batchOfSize(std::min<std::size_t>(limit, 8 * 1024) + 1);
    if (over.empty()) std::_Exit(10);
    R.produce(1, /*acks=all*/1, over);
    R.runWorker();
    if (R.answers.size() != 1u || R.answers[0].inlineError ||
        R.answers[0].code != static_cast<int16_t>(pc::ErrorCode::MessageTooLarge)) std::_Exit(1);
    if (R.r->log()->logEndOffset() != 0) std::_Exit(2);
    // One limit on every path: the gateway's equals the owning worker's.
    if (limit != R.w().maxIntakeBatchBytes()) std::_Exit(3);
    const auto at = batchOfSize(limit);                  // exactly at the limit: appended
    if (at.empty()) std::_Exit(11);
    R.answers.clear();
    R.produce(2, /*acks=all*/1, at);
    for (int i = 0; i < 8 && R.answers.empty(); ++i) R.runWorker();
    if (R.answers.size() != 1u || R.answers[0].code != 0 || R.r->log()->logEndOffset() != 1) std::_Exit(4);
    std::_Exit(0);
}
} // namespace

TEST(ReturnOverflowRegression, GatewayBatchOverOpRingAnswersMessageTooLargeNoCrash) {
    // B-1: the gateway's only guard was 8 MiB; a batch above the worker op
    // ring's max message made tryEnqueueOp throw uncaught on the control
    // thread (broker terminates).  Now the gateway applies the SAME limit as
    // the worker (Worker::maxAcceptableBatchBytes) → MessageTooLarge before
    // any append; a batch exactly at the limit appends.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(gatewayOpRingOversizeChild(), ::testing::ExitedWithCode(0), "");
}

TEST(ReturnOverflowRegression, OneBatchLimitOnEveryPath) {
    // B-1 / m-g / m-h: ONE function bounds the client-visible batch limit on
    // every path: min(8 MiB, op ring, return ring, replication frame (outbound
    // ring / 8 and term / 8, minus framing)).
    constexpr std::size_t MiB = 1u << 20;
    const std::size_t dflt = Worker::maxAcceptableBatchBytes(32 * MiB, 32 * MiB, 4 * MiB, 4 * MiB);
    EXPECT_LE(dflt, 512u * 1024u) << "default: bounded by the replication frame (4 MiB term / 8)";
    EXPECT_GT(dflt, 512u * 1024u - 512u);
    // CLAUDE.md env: 64 MiB term, 16 MiB replication outbound ring, 32 MiB rings.
    const std::size_t claude = Worker::maxAcceptableBatchBytes(32 * MiB, 32 * MiB, 16 * MiB, 64 * MiB);
    EXPECT_LE(claude, 2 * MiB) << "CLAUDE.md env: bounded by the 16 MiB outbound ring / 8";
    EXPECT_GT(claude, 2 * MiB - 512u);
    // Huge replication budget: the op ring (32 MiB / 8 − op framing) binds.
    const std::size_t rings = Worker::maxAcceptableBatchBytes(32 * MiB, 32 * MiB, 256 * MiB, 256 * MiB);
    EXPECT_LT(rings, 4 * MiB);
    EXPECT_GT(rings, 4 * MiB - 1024u);
    EXPECT_EQ(Worker::maxAcceptableBatchBytes(256 * MiB, 256 * MiB, 256 * MiB, 256 * MiB),
              Worker::kMaxProduceBatchBytes) << "the 8 MiB ceiling";
    // Behavioural (shard path): a worker configured with the default limit
    // refuses limit + 1 and appends exactly the limit.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 32 * MiB, 32 * MiB);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setWorkerIngress(nullptr, true);
    w.setMaxBatchBytes(dflt);
    ASSERT_EQ(w.maxIntakeBatchBytes(), dflt);
    const auto over = produceFrame(1, 9, batchOfSize(dflt + 1), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(over.data(), over.size(), 1), 1);
    auto rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(log.logEndOffset(), 0);
    const auto at = produceFrame(1, 10, batchOfSize(dflt), /*acks=*/0, 1, 0);
    ASSERT_EQ(w.handleIngressFrameForTest(at.data(), at.size(), 2), 1);
    rets = drainAll(w);
    ASSERT_EQ(rets.size(), 1u);
    EXPECT_EQ(rets[0].errorCode, 0);
    EXPECT_EQ(log.logEndOffset(), 1);
}

TEST(ReturnOverflowRegression, OscillatingBacklogStaysOnePauseEpisode) {
    // B-2: under sustained return back-pressure the FIFO oscillates just
    // below / at the high-water mark; with hysteresis (the episode ends only at
    // FIFO <= high-water / 2) that is ONE episode (one enter + one leave log),
    // not one per control-drain cadence.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 23, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setReturnDrainLimit(100);                      // the control thread drains 100 per cycle
    constexpr int kN = 4000;
    for (int i = 0; i < kN; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, i + 1)));
    int64_t now = 1;
    for (int p = 0; p < 8 && w.retFifoSizeForTest() < Worker::kOverflowHighWater; ++p) w.doWork(++now);
    ASSERT_GE(w.retFifoSizeForTest(), Worker::kOverflowHighWater) << "precondition: at high water";
    // Each control cycle frees ~100 ring slots: the next pass flushes ~100 FIFO
    // entries into the ring (FIFO dips below high water), drains ~100 more ops
    // (FIFO back at high water), and the following pass pauses again.
    std::size_t received = 0, minFifo = SIZE_MAX;
    const int64_t leo0 = log.logEndOffset();
    for (int c = 0; c < 20; ++c) {
        received += w.drainReturn([](WorkerReturn) {});
        w.doWork(++now);
        w.doWork(++now);
        minFifo = std::min(minFifo, w.retFifoSizeForTest());
    }
    ASSERT_GT(log.logEndOffset(), leo0 + 1000) << "precondition: the op drain kept resuming (oscillation)";
    ASSERT_GT(minFifo, Worker::kOverflowHighWater / 2) << "precondition: the backlog stays above low water";
    EXPECT_EQ(w.retFifoPauseEpisodesForTest(), 1u) << "the pause episode flapped";
    EXPECT_TRUE(w.retFifoPausedForTest());
    std::vector<WorkerReturn> answers;
    drainUntil(w, answers, kN - received, now);
    w.doWork(++now);
    EXPECT_FALSE(w.retFifoPausedForTest()) << "the episode ends once the FIFO drains below low water";
    EXPECT_EQ(w.retFifoPauseEpisodesForTest(), 1u);
}

TEST(ReturnOverflowRegression, OscillatingOwedAtCapStaysOneThrottleEpisode) {
    // B-2: owed returns oscillating near the cap (a partial HW advance frees a
    // few, new produces refill to the cap) is ONE throttle episode; it ends
    // only once owed <= cap / 2.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    Worker w(0, 1 << 22);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.setMaxOwedReturns(256);
    int64_t now = 1, corr = 0;
    std::vector<WorkerReturn> answers;
    for (int round = 0; round < 6; ++round) {
        for (int i = 0; i < (round == 0 ? 300 : 100); ++i) ASSERT_TRUE(w.tryEnqueueOp(acksAllOp(&rep, ++corr)));
        w.doWork(++now);
        collectAnswers(w, answers);
        ASSERT_TRUE(w.owedCapThrottlingForTest()) << "precondition: at the cap, round " << round;
        // A partial commit: the follower catches up 40 offsets → 40 waiters fire.
        const int64_t hw = rep.highWatermark();
        rune::replication::FetchRequestMsg fr{rep.epoch(), 0, 2, hw + 40, 1 << 20, rep.topicId()};
        (void)rep.onFetchRequest(fr, now);
        w.doWork(++now);
        collectAnswers(w, answers);
        ASSERT_GT(w.reservedAckSlotsForTest(), 128) << "precondition: owed stays above cap / 2";
    }
    EXPECT_EQ(w.owedCapEpisodesForTest(), 1u) << "the throttle episode flapped";
    followerCaughtUp(rep, now);
    w.doWork(++now);
    w.doWork(++now);
    EXPECT_FALSE(w.owedCapThrottlingForTest()) << "the episode ends once owed <= cap / 2";
    EXPECT_EQ(w.owedCapEpisodesForTest(), 1u);
}
