// WorkerProduceLatencyBench.cpp — produce→ack latency of the REAL production
// thread-per-core path (NOT a synthetic PartitionReplica micro-bench).
//
// This drives exactly what runs in production for a keyed produce:
//   Worker::tryEnqueueOp  → encode WorkerOp into the OneToOne ingress byte ring
//   Worker::doWork        → drainIngress_ → executeProduceWire_:
//                             - log->appendNoSync()  (page cache, NO per-op fsync)
//                             - Strict tier: io_.submit() fsync to the IoBackend
//                               (UringFlushBackend = real io_uring on Linux) and
//                               DEFER the ack until reapDurableCompletions_ reaps it
//                             - Majority/Leader tier: ack immediately (append path)
//   Worker::drainReturn   → the ack (WorkerReturn) the gateway would send the client
//
// We time enqueue → ack (the client-observed produce latency) and report p50/p99.
//
// Tiers measured:
//   Majority (durabilityTier=1): ack on append/HW — NO fsync on the ack path.
//     Proves the real produce→ack path is storage-independent (~µs even on a slow
//     disk), because per-op fdatasync was a legacy sync-append() artifact.
//   Strict   (durabilityTier=2): ack on io_uring fsync completion — storage-bound
//     (async + coalesced, worker never blocks), so this reflects the disk's fsync.

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/IoBackend.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::storage::Log;

static std::size_t s_dirCtr = 0;
static std::string makeTempDir() {
    std::string p = (fs::temp_directory_path() /
        ("rune_wplat_" + std::to_string(++s_dirCtr) + "_" +
         std::to_string(::getpid()))).string();
    fs::create_directories(p);
    return p;
}

// A single-record RecordBatch with a `payloadSize`-byte value.
static std::vector<uint8_t> makeBatch(std::size_t payloadSize) {
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/0);
    std::string val(payloadSize, 'x');
    b.append(std::string_view{}, std::string_view{val}, /*timestamp=*/0);
    return b.build();
}

// Non-owning: PartitionReplica needs a transport; produce path never uses it here
// (minIsr=1, leader-only), but the ctor requires one.
class NullTransport : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override { return true; }
};

// Builds the REAL Worker + disk Log + leader PartitionReplica + real IoBackend.
struct RealWorkerFixture {
    std::string dir;
    std::unique_ptr<Log> log;
    NullTransport transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker> worker;

    RealWorkerFixture() {
        dir = makeTempDir();
        log = std::make_unique<Log>(dir, /*rollBytes=*/64LL * 1024 * 1024);
        log->recover();
        PartitionConfig cfg{};
        cfg.topicId = 1; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(
            cfg, rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, log.get(), &transport);
        worker = std::make_unique<Worker>(0);  // ingress/return rings default to k*ByteCapacity (32 MiB)
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
    }
    ~RealWorkerFixture() { std::error_code ec; fs::remove_all(dir, ec); }
};

// Report p50/p99 (µs) from per-iteration produce→ack samples.
static void reportPercentiles(benchmark::State& state, std::vector<int64_t>& ns) {
    if (ns.empty()) return;
    std::sort(ns.begin(), ns.end());
    state.counters["p50Us"] =
        benchmark::Counter(static_cast<double>(ns[ns.size()/2]) / 1000.0);
    state.counters["p99Us"] =
        benchmark::Counter(static_cast<double>(ns[static_cast<std::size_t>(ns.size()*99/100)]) / 1000.0);
}

// One produce → ack round trip on the REAL worker path, timed.
static void runTier(benchmark::State& state, uint8_t tier, std::size_t payload) {
    RealWorkerFixture fix;
    std::vector<int64_t> samples;
    samples.reserve(1 << 16);
    int64_t nowNs = 1;
    int64_t corr = 0;

    for (auto _ : state) {
        auto batch = makeBatch(payload);
        int32_t lastOffsetDelta = 0; // one record → delta 0

        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.topicId = 1; op.partition = 0;
        op.replica = fix.replica.get(); op.logPtr = fix.log.get();
        op.sessionId = 1; op.correlationId = ++corr;
        op.lastOffsetDelta = lastOffsetDelta; op.recordCount = 1;
        op.durabilityTier = tier;
        op.recordBatchBytes = std::move(batch);

        auto t0 = std::chrono::steady_clock::now();
        while (!fix.worker->tryEnqueueOp(op)) {   // back-pressure: retry
            fix.worker->doWork(nowNs++);
        }
        // Drive doWork until the ack lands (Strict: fsync completion reaped;
        // Majority: appended → returned same cycle).
        bool acked = false;
        for (int i = 0; i < 5'000'000 && !acked; ++i) {
            fix.worker->doWork(nowNs++);
            fix.worker->drainReturn([&](WorkerReturn&&){ acked = true; });
        }
        auto t1 = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(acked);
        samples.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    state.SetItemsProcessed(state.iterations());
    reportPercentiles(state, samples);
}

// Majority tier (durabilityTier=1): ack on append/HW — NO fsync on the ack path.
static void BM_WorkerProduce_Majority_1KiB(benchmark::State& s) { runTier(s, 1, 1024); }
static void BM_WorkerProduce_Majority_256B(benchmark::State& s) { runTier(s, 1, 256);  }
// Strict tier (durabilityTier=2): ack on io_uring fsync completion — storage-bound.
static void BM_WorkerProduce_Strict_1KiB(benchmark::State& s)   { runTier(s, 2, 1024); }
static void BM_WorkerProduce_Strict_256B(benchmark::State& s)   { runTier(s, 2, 256);  }

// Task 10 fix 3: the per-op (id, incarnation) ownership guard in
// Worker::executeFromWire_.  N owned partitions; rotating owned id (the
// always-taken path).  Expected: one indexed load + compare + branch.
static void BM_WorkerOwnershipCheck(benchmark::State& state) {
    const auto n = static_cast<std::size_t>(state.range(0));
    NullTransport t;
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> logs;
    std::vector<std::unique_ptr<PartitionReplica>> reps;
    Worker::OwnedSet owned;
    for (std::size_t i = 0; i < n; ++i) {
        logs.push_back(std::make_unique<rune::storage::InMemoryLog>(1 << 12, 16));
        PartitionConfig cfg{};
        cfg.topicId = 1; cfg.partitionId = static_cast<int32_t>(i); cfg.selfId = 1; cfg.minIsr = 1;
        reps.push_back(std::make_unique<PartitionReplica>(
            cfg, rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, logs.back().get(), &t));
        reps.back()->setRegistryHandle(static_cast<uint32_t>(i + 1), static_cast<uint32_t>(i + 1));
        owned.push_back(reps.back().get());
    }
    Worker w(0, 1 << 16, 1 << 16, 16, 16);
    w.setOwnedDirect(owned);
    std::size_t i = 0;
    for (auto _ : state) {
        // Exactly the dispatch guard: (id, incarnation) carried in the op.
        benchmark::DoNotOptimize(w.ownsIncarnationForTest(static_cast<uint32_t>(i + 1),
                                                         static_cast<uint32_t>(i + 1)));
        i = (i + 1 == n) ? 0 : i + 1;
    }
    state.SetItemsProcessed(state.iterations());
}
// Baseline: the same loop doing only the indexed pointer load + compare.
static void BM_WorkerOwnershipCheck_Baseline(benchmark::State& state) {
    const auto n = static_cast<std::size_t>(state.range(0));
    std::vector<const void*> ptrs(n);
    for (std::size_t k = 0; k < n; ++k) ptrs[k] = &ptrs[k];
    std::size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(ptrs[i] != nullptr);
        i = (i + 1 == n) ? 0 : i + 1;
    }
    state.SetItemsProcessed(state.iterations());
}

// F15 §3.1 (I1): the produce role gate the owning worker runs on BOTH produce
// paths (executeProduceWire_, handleWorkerIngressFrame_) — one load + compare on
// the replica's hot line.  Budget: <= +0.2 ns over the baseline below.
static void BM_WorkerRoleGate(benchmark::State& state) {
    const auto n = static_cast<std::size_t>(state.range(0));
    NullTransport t;
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> logs;
    std::vector<std::unique_ptr<PartitionReplica>> reps;
    std::vector<const PartitionReplica*> ptrs;
    for (std::size_t i = 0; i < n; ++i) {
        logs.push_back(std::make_unique<rune::storage::InMemoryLog>(1 << 12, 16));
        PartitionConfig cfg{};
        cfg.topicId = 1; cfg.partitionId = static_cast<int32_t>(i); cfg.selfId = 1; cfg.minIsr = 1;
        reps.push_back(std::make_unique<PartitionReplica>(
            cfg, rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, logs.back().get(), &t));
        ptrs.push_back(reps.back().get());
    }
    std::size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(Worker::produceRoleGatePassesForTest(ptrs[i]));
        i = (i + 1 == n) ? 0 : i + 1;
    }
    state.SetItemsProcessed(state.iterations());
}
// Baseline: the same rotation over replica pointers, null check only.
static void BM_WorkerRoleGate_Baseline(benchmark::State& state) {
    const auto n = static_cast<std::size_t>(state.range(0));
    std::vector<const void*> ptrs(n);
    for (std::size_t k = 0; k < n; ++k) ptrs[k] = &ptrs[k];
    std::size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(ptrs[i] != nullptr);
        i = (i + 1 == n) ? 0 : i + 1;
    }
    state.SetItemsProcessed(state.iterations());
}

} // namespace

BENCHMARK(BM_WorkerRoleGate)->Arg(1)->Arg(64)->Arg(1024)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_WorkerRoleGate_Baseline)->Arg(1)->Arg(64)->Arg(1024)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_WorkerOwnershipCheck)->Arg(1)->Arg(64)->Arg(1024)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_WorkerOwnershipCheck_Baseline)->Arg(1)->Arg(64)->Arg(1024)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_WorkerProduce_Majority_1KiB)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduce_Majority_256B)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduce_Strict_1KiB)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduce_Strict_256B)->Unit(benchmark::kNanosecond)->MinTime(0.2);
