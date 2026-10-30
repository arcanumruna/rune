// NamespaceHotPathBench.cpp - namespaces Phase 2 Task 15 (spec 5.4; CLAUDE.md rules 6, 12): the cost the namespace gate and the
// rate counters add to the three request paths a tenant pays for - a leader PRODUCE (enqueue -> ack), a PUSH READ (fetch) and a
// view POINT READ (GetViewRow) - on the REAL Worker over a disk Log, with the namespace on and every limit set.
//
// The comparison is ABAB between two BUILDS of this one file:
//   A  the Phase 1 merge (4f7e53bb), which has neither the slot table nor the gate nor the counters.  The file compiles there
//      because everything namespace-specific sits under RUNE_NSB (= the headers of Task 8 exist), so A measures the bare paths.
//   B  this tip: the log is tagged, the slot has all 29 limits set, and the worker pays the gate (one array index, one nsId
//      compare, one deadline compare) and the counter adds on each request.
// Nothing in the broker is switched off for A: the "no gate / no counter" build IS the earlier commit.
//
// Output: per case p50 / p99 / p99.9 in microseconds (counters p50Us, p99Us, p999Us) over the timed iterations.  Gate: the
// point read's p99 stays under the 50 us CLAUDE.md budget.  Run per BENCHMARKING.md (prepared box, Release, ABAB, n>=5 for latency).
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>

#if __has_include(<rune/node/NsRingMessages.h>)
#define RUNE_NSB 1
#include <rune/node/NsRingMessages.h>
#include <rune/protocol/NamespaceLimits.h>
#else
#define RUNE_NSB 0
#endif

#include "BenchGate.h"
#include <benchmark/benchmark.h>

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;
namespace rv = rune::view;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::storage::Log;

constexpr int32_t kView = 0x40000007;
[[maybe_unused]] constexpr uint32_t kNs = 7;
[[maybe_unused]] constexpr uint16_t kSlot = 1;

std::string makeTempDir() {
    static std::size_t ctr = 0;
    const std::string p = (fs::temp_directory_path() / ("rune_nshp_" + std::to_string(++ctr) + "_" + std::to_string(::getpid()))).string();
    fs::create_directories(p);
    return p;
}

class NullTransport : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

std::vector<uint8_t> makeBatch(std::size_t payload, int records = 1) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    const std::string val(payload, 'x');
    for (int i = 0; i < records; ++i) b.append(std::string_view{}, std::string_view{val}, 0);
    return b.build();
}

std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i));
    return b;
}
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

struct Fixture {
    std::string dir = makeTempDir();
    std::unique_ptr<Log> log;
    NullTransport transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker> worker;
    int64_t now = 1;
    int64_t corr = 0;

    Fixture() {
        log = std::make_unique<Log>(dir, 64LL * 1024 * 1024);
        log->recover();
        PartitionConfig cfg{};
        cfg.topicId = 1; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                                                     std::vector<rune::replication::NodeId>{1}, log.get(), &transport);
        worker = std::make_unique<Worker>(0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
#if RUNE_NSB
        // B: the namespace is ON and every limit is set (far away: the gate has real values to compare, nothing throttles).
        log->setNsTag(kNs, kSlot);
        rune::protocol::NamespaceLimits l;
        l.produceBytesPerSec = 1ull << 40; l.produceRecordsPerSec = 1ull << 40; l.fetchBytesPerSec = 1ull << 40;
        l.requestOpsPerSec = 1ull << 40; l.queryCyclesPerSec = 1ull << 40; l.maxDiskBytes = 1ull << 40;
        l.maxViewMemoryBytes = 1ull << 40; l.maxTopics = 1000; l.maxPartitionsTotal = 1000;
        worker->applyNsSlotAssignDirect(kSlot, kNs, l);
#endif
    }
    ~Fixture() { std::error_code ec; fs::remove_all(dir, ec); }

    WorkerOp produceOp(const std::vector<uint8_t>& batch) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.topicId = 1; op.partition = 0;
        op.replica = replica.get(); op.logPtr = log.get();
        op.sessionId = 1; op.correlationId = ++corr;
        op.lastOffsetDelta = 0; op.recordCount = 1;
        op.durabilityTier = 1;
        op.recordBatchBytes = batch;
        return op;
    }
    /// Drive until the single answer of the last op arrives; false = none within the bound.
    bool settle() {
        bool got = false;
        for (int i = 0; i < 1'000'000 && !got; ++i) {
            worker->doWork(now++);
            worker->drainReturn([&](WorkerReturn&&) { got = true; });
        }
        return got;
    }
};

void reportPercentiles(benchmark::State& state, std::vector<int64_t>& ns) {
    if (ns.empty()) return;
    std::sort(ns.begin(), ns.end());
    state.counters["p50Us"]  = benchmark::Counter(static_cast<double>(ns[ns.size() / 2]) / 1000.0);
    state.counters["p99Us"]  = benchmark::Counter(static_cast<double>(ns[ns.size() * 99 / 100]) / 1000.0);
    state.counters["p999Us"] = benchmark::Counter(static_cast<double>(ns[ns.size() * 999 / 1000]) / 1000.0);
}

// ---- 1. leader produce: enqueue -> ack (acks=leader / majority tier with minIsr 1: no fsync on the ack path) ----
void BM_NsProduce(benchmark::State& state) {
    Fixture fix;
    const auto batch = makeBatch(static_cast<std::size_t>(state.range(0)));
    std::vector<int64_t> samples;
    samples.reserve(1 << 17);
    for (auto _ : state) {
        WorkerOp op = fix.produceOp(batch);
        const auto t0 = std::chrono::steady_clock::now();
        while (!fix.worker->tryEnqueueOp(op)) fix.worker->doWork(fix.now++);
        if (!fix.settle()) { state.SkipWithError("produce unanswered"); break; }
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count());
    }
    state.SetItemsProcessed(state.iterations());
    reportPercentiles(state, samples);
    rune::bench::gatePass(state);
}
BENCHMARK(BM_NsProduce)->Arg(256)->Arg(1024)->Unit(benchmark::kMicrosecond);

// ---- 2. push read (fetch): a read of already-stored records, enqueue -> the worker's answer ----
void BM_NsFetch(benchmark::State& state) {
    Fixture fix;
    const auto batch = makeBatch(256, 4);
    for (int i = 0; i < 256; ++i) {   // stored records to read
        WorkerOp op = fix.produceOp(batch);
        op.lastOffsetDelta = 3; op.recordCount = 4;
        while (!fix.worker->tryEnqueueOp(op)) fix.worker->doWork(fix.now++);
        if (!fix.settle()) { state.SkipWithError("fill unanswered"); return; }
    }
    std::vector<int64_t> samples;
    samples.reserve(1 << 17);
    int64_t sub = 1000;
    for (auto _ : state) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead;
        op.topicId = 1; op.partition = 0; op.replica = fix.replica.get();
        op.subscriptionId = ++sub; op.sessionId = 1; op.nextOffset = 0;
        op.hwBound = INT64_MAX; op.isolation = 0; op.logPtr = fix.log.get();
        const auto t0 = std::chrono::steady_clock::now();
        while (!fix.worker->tryEnqueueOp(op)) fix.worker->doWork(fix.now++);
        if (!fix.settle()) { state.SkipWithError("push read unanswered"); break; }
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count());
    }
    state.SetItemsProcessed(state.iterations());
    reportPercentiles(state, samples);
    rune::bench::gatePass(state);
}
BENCHMARK(BM_NsFetch)->Unit(benchmark::kMicrosecond);

// ---- 3. view point read: GetViewRow served from the worker's view set (budget: p99 < 50 us, CLAUDE.md) ----
rune::codec::Descriptor charSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}
struct SimLog {
    std::vector<uint8_t> bytes;
    int64_t end = 0;
    void append(int n) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        for (int i = 0; i < n; ++i) {
            const auto kb = le(static_cast<uint64_t>(i + 1), 8);
            std::vector<uint8_t> vb;
            for (const auto& x : {le(12, 2), le(1, 2), le(static_cast<uint64_t>(i + 1), 8), le(static_cast<uint32_t>(10 * (i + 1)), 4)})
                vb.insert(vb.end(), x.begin(), x.end());
            b.append(sv(kb), sv(vb), 0);
        }
        bytes = b.build();
        end = n;
    }
    rune::storage::LogReadView read(int64_t from, int64_t) const {
        return from >= 0 && from < end ? rune::storage::LogReadView{bytes.data(), bytes.size(), 0, end} : rune::storage::LogReadView{};
    }
};

void BM_NsViewPointRead(benchmark::State& state) {
    Fixture fix;
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs{channel, (fs::temp_directory_path() / ("rune-nshp-bench-view-" + std::to_string(::getpid()))).string()};
    const rune::codec::Descriptor src = charSource();
    rv::ViewPlan v;
    v.viewName = "char_hp";
    v.key = "characterId";
    v.sources = {{1, "characterId"}};
    v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rv::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rv::MergeRule::Replace}};
    v.persistence = 1;
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(v, srcs, kView);
    if (!comp.has_value()) { state.SkipWithError("view compile"); return; }
#if RUNE_NSB
    vs.configureNsSlots(8);
    {
        rune::protocol::NamespaceLimits l;
        l.maxViewMemoryBytes = 1ull << 40;
        vs.onNsSlotAssign(kSlot, kNs, l);
    }
#endif
    vs.installView(comp.value().plan, comp.value().rowLayout, src, false);
    SimLog log;
    log.append(1024);
    auto rd = [&](int64_t f, int64_t t) { return log.read(f, t); };
    for (int i = 0; i < 2; ++i) {
#if RUNE_NSB
        (void)vs.driveFolds(1, 0, rd, log.end, rv::kNoSkip, 0, nullptr, fix.replica->leadershipGeneration(), true,
                            rune::node::NsTag{kSlot, kNs});
#else
        (void)vs.driveFolds(1, 0, rd, log.end, rv::kNoSkip, 0, nullptr, fix.replica->leadershipGeneration(), true);
#endif
    }
    fix.worker->setViewSet(&vs);
    std::vector<int64_t> samples;
    samples.reserve(1 << 17);
    int64_t key = 0;
    for (auto _ : state) {
        WorkerOp op;
        op.type = WorkerOp::Type::GetViewRow;
        op.topicId = kView; op.partition = 0; op.sessionId = 1; op.correlationId = ++fix.corr;
        op.nextOffset = 1 + (key++ & 1023); op.hwBound = 0;
        const auto t0 = std::chrono::steady_clock::now();
        while (!fix.worker->tryEnqueueOp(op)) fix.worker->doWork(fix.now++);
        if (!fix.settle()) { state.SkipWithError("view read unanswered"); break; }
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count());
    }
    state.SetItemsProcessed(state.iterations());
    reportPercentiles(state, samples);
    if (!samples.empty()) {
        const double p99Us = static_cast<double>(samples[samples.size() * 99 / 100]) / 1000.0;
        rune::bench::gateCheck(state, p99Us < 50.0, "point read p99 over the 50 us budget (CLAUDE.md)");
    }
}
BENCHMARK(BM_NsViewPointRead)->Unit(benchmark::kMicrosecond);

}  // namespace
