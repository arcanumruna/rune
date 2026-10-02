// F15WorkerPsmBench.cpp — F15 S5 design note §9 benchmarks (review I-1).
//
// BM_GatewayPathProduce/<pid>: one IDEMPOTENT (pid >= 0) produce through the
//   REAL default-config gateway path, end to end on one thread:
//   ClientGateway::applyProduceForTest (decode, validate, route → WorkerOp)
//   → Worker::doWork (executeProduceWire_: producer-state validate → append →
//   update) → Worker::drainReturn → ClientGateway::handleWorkerReturnForTest
//   (ProduceResponse encode).  Before S5 the producer-state check ran on the
//   gateway (mirror) and the facts were fed back; after S5 it runs on the
//   worker.  The system cost per produce is what is compared.  Arg 0 = pid -1
//   (non-idempotent control arm), Arg 1 = pid 7 (idempotent).
//
// BM_FlushOwedAcksAll/<N>: steady state of N acks=all waiters parked on the
//   owning worker (gateway-routed ops).  Each iteration appends kFire new
//   batches, moves the follower so exactly the kFire OLDEST waiters pass the
//   HW (partial fire, front of the queue — the HW is monotone), runs one
//   worker pass and drains the answers.  N in {64, 512, 4096}.
//
// Everything allocated before the timed loop (batches / frames pre-built);
// no sleeps.  Run base vs head interleaved (ABAB) on k3s-node-01.

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <benchmark/benchmark.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const rune::replication::FetchResponseMsg&) override {}
};

std::string tempDir(const char* tag) {
    static int seq = 0;
    const fs::path p = fs::temp_directory_path() /
        (std::string("rune_f15s5_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(++seq));
    fs::create_directories(p);
    return p.string();
}

PartitionConfig partCfg() {
    PartitionConfig c;
    c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000'000LL;
    c.replicaLagMaxMs = 100'000'000; c.pushOnAppend = false;
    return c;
}

std::vector<uint8_t> batch(int64_t pid, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    b.append("k", std::string(100, 'v'), 1000);
    return b.build();
}

// The default (Legacy) config: real TopicRegistry / ReplicaRegistry / WorkerPool
// behind a real ClientGateway; the bench thread plays the worker and control
// threads in turn (deterministic, no cross-thread wait).
struct GatewayRig {
    std::string dir = tempDir("gw");
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, dir};
    NullTransport t;
    rune::node::ReplicaRegistry reg;
    rune::node::WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 22, 1 << 23, 64, 64};
    int64_t answers = 0;
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t*, std::size_t) -> int64_t { ++answers; return 1; });
    PartitionReplica* r = nullptr;
    int64_t sid = 0;
    int64_t now = 1;
    explicit GatewayRig(std::vector<NodeId> isr) {
        auto rep = std::make_unique<PartitionReplica>(partCfg(), Role::Leader, 1, isr, isr, topics.log(0, 0), &t);
        r = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setCommittedMetadataReady(true);
        sid = gw.injectTestSession();
        pass();   // ownership applied
    }
    ~GatewayRig() { std::error_code ec; fs::remove_all(dir, ec); }
    Worker& w() { return pool.worker(0); }
    void pass() {
        w().doWork(++now);
        w().drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
    }
};

std::vector<uint8_t> produceFrame(int64_t sid, int64_t corr, const std::vector<uint8_t>& b, uint8_t acks,
                                  int64_t pid, int32_t seq) {
    pc::Produce m;
    m.sessionId = sid; m.correlationId = corr; m.topicId = 0; m.partition = 0; m.partitionKnown = 1;
    m.acks = acks; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = pid >= 0 ? seq : -1;
    m.recordBatch = std::span<const uint8_t>(b);
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}

// ---------------------------------------------------------------------------
void BM_GatewayPathProduce(benchmark::State& state) {
    const bool idem = state.range(0) != 0;
    GatewayRig R({1});
    constexpr int kPool = 1 << 15;
    std::vector<std::vector<uint8_t>> frames;
    int64_t pid = idem ? 7 : -1;
    int32_t seq = 0;
    int next = 0;
    auto refill = [&] {
        frames.clear();
        frames.reserve(kPool);
        for (int i = 0; i < kPool; ++i, ++seq)
            frames.push_back(produceFrame(R.sid, seq + 1, batch(pid, seq), /*acks=leader*/0, pid, seq));
        next = 0;
    };
    refill();
    for (auto _ : state) {
        if (next == kPool) [[unlikely]] { state.PauseTiming(); refill(); state.ResumeTiming(); }
        const auto& f = frames[static_cast<std::size_t>(next++)];
        R.gw.applyProduceForTest(f.data(), f.size());
        R.pass();
    }
    if (R.answers < static_cast<int64_t>(state.iterations()))
        state.SkipWithError("a produce was not answered");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayPathProduce)->Arg(0)->Arg(1)->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
void BM_FlushOwedAcksAll(benchmark::State& state) {
    const int64_t parked = state.range(0);
    constexpr int kFire = 16;
    GatewayRig R({1, 2});
    R.w().setMaxOwedReturns(8192);
    const auto b = batch(-1, 0);
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = 0; op.partition = 0;
    op.replica = R.r; op.logPtr = R.r->log(); op.sessionId = R.sid;
    op.acks = 1; op.deferAck = true; op.producerId = -1; op.baseSequence = -1;
    op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
    op.targetReplicaId = R.r->registryId(); op.incarnation = R.r->incarnation();
    op.recordBatchView = std::span<const uint8_t>(b);
    int64_t corr = 0;
    auto enqueue = [&](int n) {
        for (int i = 0; i < n; ++i) {
            op.correlationId = ++corr;
            if (!R.w().tryEnqueueOp(op)) { state.SkipWithError("op ring full"); return; }
        }
    };
    auto follower = [&](int64_t fetchOffset) {
        rune::replication::FetchRequestMsg fr{R.r->epoch(), 0, 2, fetchOffset, 1, R.r->topicId()};
        (void)R.r->onFetchRequest(fr, R.now);
    };
    // Prefill: `parked` waiters, none fired (follower at 0).
    for (int64_t done = 0; done < parked; done += 256) {
        enqueue(static_cast<int>(std::min<int64_t>(256, parked - done)));
        R.pass();
    }
    follower(0);
    if (R.w().reservedAckSlotsForTest() != parked) state.SkipWithError("prefill did not park every waiter");
    const int64_t answered0 = R.answers;
    for (auto _ : state) {
        enqueue(kFire);
        R.pass();                                             // append kFire: parked + kFire waiters
        follower(R.r->log()->logEndOffset() - parked);        // HW passes exactly the kFire oldest
        R.pass();                                             // fire them (front of the queue)
    }
    if (R.answers - answered0 != static_cast<int64_t>(state.iterations()) * kFire)
        state.SkipWithError("fired count mismatch");
    state.SetItemsProcessed(state.iterations() * kFire);
}
BENCHMARK(BM_FlushOwedAcksAll)->Arg(64)->Arg(512)->Arg(4096)->Unit(benchmark::kNanosecond)->MinTime(0.5);

} // namespace
