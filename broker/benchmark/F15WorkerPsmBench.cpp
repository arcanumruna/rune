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
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
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

// Hot-path logging off for every arm (house rule: benchmarks run with
// RUNE_FS_LOG=0 and log level >= warn; the bench binary has no env parsing,
// so the level is set here).
void quietLogs() { spdlog::set_level(spdlog::level::warn); }

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
    int32_t partitions;
    rune::gateway::TopicRegistry topics;
    NullTransport t;
    rune::node::ReplicaRegistry reg;
    rune::node::WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 22, 1 << 25, 64, 64};   // 32 MiB return ring (production default)
    int64_t answers = 0;
    int64_t errors  = 0;     ///< answers with a non-zero code (the bench must measure the OK path)
    int16_t lastErr = 0;
    int16_t firstErr = 0;
    int64_t firstErrAt = 0;
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t* b, std::size_t n) -> int64_t {
            ++answers;
            if (auto r = pc::ProduceResponse::decode(b, n); r && r->errorCode != 0) {
                if (errors == 0) { firstErr = r->errorCode; firstErrAt = answers; }
                ++errors; lastErr = r->errorCode;
            }
            else if (!r) { ++errors; lastErr = -1; }
            return 1;
        });
    PartitionReplica* r = nullptr;    ///< partition 0
    PartitionReplica* r1 = nullptr;   ///< partition 1 (stalled-front arm), if any
    int64_t sid = 0;
    int64_t now = 1;
    explicit GatewayRig(std::vector<NodeId> isr, int32_t parts = 1)
        : partitions(parts), topics({rune::node::TopicSpec{"t", parts}}, dir) {
        for (int32_t p = 0; p < parts; ++p) {
            PartitionConfig c = partCfg();
            c.partitionId = p;
            auto rep = std::make_unique<PartitionReplica>(c, Role::Leader, 1, isr, isr, topics.log(0, p), &t);
            (p == 0 ? r : r1) = rep.get();
            reg.add(0, p, std::move(rep));
        }
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
    quietLogs();
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
        const int64_t before = R.answers;
        R.gw.applyProduceForTest(f.data(), f.size());
        // Until THIS produce is answered (a pass may not drain the op — e.g.
        // a background flush pass): the next sequence is never validated
        // before the previous one landed (base's mirror window, review minor 6).
        for (int k = 0; k < 1024 && R.answers == before; ++k) R.pass();
    }
    if (R.answers < static_cast<int64_t>(state.iterations()))
        state.SkipWithError("a produce was not answered");
    else if (R.errors != 0)
        state.SkipWithError(("produces answered with an error: first code " + std::to_string(R.firstErr) +
                             " at answer " + std::to_string(R.firstErrAt) + ", last code " +
                             std::to_string(R.lastErr) + ", errors " + std::to_string(R.errors)).c_str());
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayPathProduce)->Arg(0)->Arg(1)->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
void BM_FlushOwedAcksAll(benchmark::State& state) {
    quietLogs();
    const int64_t parked = state.range(0);
    const bool stalled = state.range(1) != 0;
    constexpr int kFire = 16;
    // N-1: the PRODUCTION owed cap (Worker::kMaxOwedReturns = 4096); the
    // largest arm parks cap - kFire so a pass's kFire new waiters still fit.
    GatewayRig R({1, 2}, stalled ? 2 : 1);
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
    // Stalled-front arm (re-review I-2): ONE acks=all waiter on partition 1,
    // parked FIRST, whose follower never fetches — it never fires and pins the
    // front of the worker's waiter queue while partition 0 keeps firing.
    if (stalled) {
        WorkerOp s1 = op;
        s1.partition = 1; s1.replica = R.r1; s1.logPtr = R.r1->log();
        s1.targetReplicaId = R.r1->registryId(); s1.incarnation = R.r1->incarnation();
        s1.correlationId = ++corr;
        if (!R.w().tryEnqueueOp(s1)) state.SkipWithError("op ring full");
        R.pass();
    }
    // Prefill: `parked` waiters, none fired (follower at 0).
    for (int64_t done = 0; done < parked; done += 256) {
        enqueue(static_cast<int>(std::min<int64_t>(256, parked - done)));
        R.pass();
    }
    follower(0);
    if (R.w().reservedAckSlotsForTest() != parked + (stalled ? 1 : 0))
        state.SkipWithError("prefill did not park every waiter");
    const int64_t answered0 = R.answers;
    for (auto _ : state) {
        enqueue(kFire);
        R.pass();                                             // append kFire: parked + kFire waiters
        follower(R.r->log()->logEndOffset() - parked);        // HW passes exactly the kFire oldest
        R.pass();                                             // fire them (front of the queue)
    }
    if (R.answers - answered0 != static_cast<int64_t>(state.iterations()) * kFire)
        state.SkipWithError("fired count mismatch");
    else if (R.errors != 0)
        state.SkipWithError(("waiters answered with an error: first code " + std::to_string(R.firstErr) +
                             " at answer " + std::to_string(R.firstErrAt) + ", errors " +
                             std::to_string(R.errors)).c_str());
    state.SetItemsProcessed(state.iterations() * kFire);
}
BENCHMARK(BM_FlushOwedAcksAll)
    ->Args({64, 0})->Args({512, 0})->Args({4096 - 64, 0})
    ->Args({64, 1})->Args({512, 1})->Args({4096 - 64, 1})
    ->Unit(benchmark::kNanosecond)->MinTime(0.5);

// ---------------------------------------------------------------------------
// BM_StrictPipelined/<perPass> (re-review 2 M-7): Strict-tier (fsync-acked)
// gateway-routed produces PIPELINED on one log — every worker pass enqueues
// `perPass` new batches and reaps whatever fsync completed.  Measures the
// per-log Strict throughput (items/s = acked batches) and the enqueue→ack
// latency (p50/p99 µs).  Before F15 S5 fix round 2 several coalesced fsyncs
// could be in flight on the log; since M-6 one is (the next one covers every
// batch appended meanwhile).  Real disk log + the worker's real io backend
// (io_uring on Linux).  Window bounded (kMaxInFlight) so the owed cap is
// never hit.
void BM_StrictPipelined(benchmark::State& state) {
    quietLogs();
    const int perPass = static_cast<int>(state.range(0));
    constexpr int kMaxInFlight = 1024;
    const std::string dir = tempDir("strict");
    rune::storage::Log log(dir, 64LL * 1024 * 1024);
    log.recover();
    NullTransport t;
    PartitionConfig c = partCfg();
    c.durabilityTier = 2;
    PartitionReplica rep(c, Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 22, 1 << 25, 256, 256);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    const auto b = batch(-1, 0);
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = 0; op.partition = 0;
    op.replica = &rep; op.logPtr = &log; op.sessionId = 1; op.acks = 0;
    op.producerId = -1; op.baseSequence = -1; op.lastOffsetDelta = 0; op.recordCount = 1;
    op.durabilityTier = 2;
    op.recordBatchView = std::span<const uint8_t>(b);
    using Clock = std::chrono::steady_clock;
    std::vector<Clock::time_point> sent;
    sent.reserve(1 << 22);
    std::vector<int64_t> latNs;
    latNs.reserve(1 << 22);
    int64_t corr = 0, acked = 0, errors = 0, now = 1;
    auto drain = [&] {
        w.drainReturn([&](WorkerReturn r) {
            if (r.type != WorkerReturn::Type::ProduceResult) return;
            if (r.errorCode != 0) { ++errors; return; }
            const auto i = static_cast<std::size_t>(r.correlationId - 1);
            if (i < sent.size() && latNs.size() < latNs.capacity())
                latNs.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - sent[i]).count());
            ++acked;
        });
    };
    for (auto _ : state) {
        for (int k = 0; k < perPass && corr - acked < kMaxInFlight && sent.size() < sent.capacity(); ++k) {
            op.correlationId = ++corr;
            sent.push_back(Clock::now());
            if (!w.tryEnqueueOp(op)) { state.SkipWithError("op ring full"); break; }
        }
        w.doWork(++now);
        drain();
    }
    for (int i = 0; i < 50'000'000 && acked + errors < corr; ++i) { w.doWork(++now); drain(); }   // settle (bounded)
    if (errors != 0) state.SkipWithError("Strict produces answered with an error");
    std::sort(latNs.begin(), latNs.end());
    if (!latNs.empty()) {
        state.counters["p50us"] = static_cast<double>(latNs[latNs.size() / 2]) / 1000.0;
        state.counters["p99us"] = static_cast<double>(latNs[latNs.size() * 99 / 100]) / 1000.0;
    }
    state.counters["acked_per_s"] = benchmark::Counter(static_cast<double>(acked), benchmark::Counter::kIsRate);
    state.SetItemsProcessed(acked);
    std::error_code ec; fs::remove_all(dir, ec);
}
BENCHMARK(BM_StrictPipelined)->Arg(1)->Arg(16)->Unit(benchmark::kMicrosecond)->MinTime(2.0)->UseRealTime();

} // namespace
