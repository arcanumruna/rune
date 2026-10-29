// ProduceValidateBench.cpp — Task 7: per-worker produce hot path with schema
// validation OFF vs ON.
//
// Benchmarks the Worker produce path (tryEnqueueOp → doWork → drainReturn)
// with and without a schema Validator installed for the topic, using the
// "characters" descriptor from the rune-codec tests.
//
// The delta between OFF and ON measures the cost of the validation hook added
// in executeProduceWire_ (Worker.h) immediately before appendNoSyncRestamp.
//
// Methodology:
//   - Sequential A/B (n=15 each): Google Benchmark runs all 15 repetitions of
//     BM_ProduceValidate_Off THEN all 15 of BM_ProduceValidate_On (AABB, not
//     ABAB).  True per-iteration interleaving is not achievable with separately
//     registered BM_* functions in Google Benchmark.  The sequential ordering
//     is sufficient to measure the per-produce cost delta between the two paths.
//   - SetItemsProcessed() → Google Benchmark reports throughput + ns/item.
//   - DoNotOptimize on the acked boolean prevents DCE of the timed section.
//
// The cited code path: Worker::executeProduceWire_ (Worker.h) — the
// `auto it = validators_.find(topicId)` branch added by Task 7.
//
// Checked writes Part 02 adds BM_CheckedWriteHook/*: the typed-batch check alone (the <= 1 us p50 budget gate) and the
// whole produce path for an untyped topic, a typed topic below the feature level and a typed topic at it.
//
// Uses a disk-backed Log + PartitionReplica (same pattern as
// WorkerProduceLatencyBench.cpp / AsyncDurabilityTest.cpp).
// No Aeron media driver needed.
// ---------------------------------------------------------------------------

#include <benchmark/benchmark.h>

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WriteState.h>
#include "BenchGate.h"
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Validator.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::storage::Log;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::protocol::RecordBatchBuilder;

static constexpr int32_t kTopicId    = 1;
static constexpr int32_t kPartition  = 0;

// ---------------------------------------------------------------------------
// NullTransport
// ---------------------------------------------------------------------------
class NullTransport : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override { return true; }
};

// ---------------------------------------------------------------------------
// Build the "characters" descriptor (matching ValidatorTest / T7 brief).
//   id    : Int64
//   type  : Enum   {0,1,2}
//   name  : String
// blockLength = 12 bytes.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeCharactersBlob() {
    auto desc = DescriptorBuilder("characters")
        .add("id",   PrimType::Int64)
        .add("type", PrimType::Enum, /*sinceVersion=*/1, /*isKey=*/false, {0, 1, 2})
        .add("name", PrimType::String)
        .build();
    return rune::codec::serialize(desc);
}

// ---------------------------------------------------------------------------
// Build a valid record value (blockLength=12, version=1):
//   uint16 blockLength = 12
//   uint16 version     = 1
//   int64  id          = 1
//   int32  type        = 1   (valid enum)
//   uint32 nameLen     = 5
//   bytes  name        = "Alice"
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeValidValue() {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) {
        b.push_back(static_cast<uint8_t>(v));
        b.push_back(static_cast<uint8_t>(v >> 8));
    };
    auto le32 = [&](uint32_t v) {
        b.push_back(static_cast<uint8_t>(v));
        b.push_back(static_cast<uint8_t>(v >> 8));
        b.push_back(static_cast<uint8_t>(v >> 16));
        b.push_back(static_cast<uint8_t>(v >> 24));
    };
    auto le64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
    };
    le16(12);   // blockLength = 12
    le16(1);    // version = 1
    le64(1);    // id = 1
    le32(1);    // type = 1 (valid enum)
    le32(5);    // name length = 5
    b.insert(b.end(), {'A','l','i','c','e'}); // name = "Alice"
    return b;
}

// ---------------------------------------------------------------------------
// Build a single-record RecordBatch whose value is `recValue`.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch(const std::vector<uint8_t>& recValue) {
    RecordBatchBuilder b(0, /*baseTimestamp=*/1000);
    std::string_view val(reinterpret_cast<const char*>(recValue.data()), recValue.size());
    b.append(/*key=*/{}, val, 1000);
    return b.build();
}

// ---------------------------------------------------------------------------
// TempDir RAII — manages a temp directory for the disk-backed Log.
// ---------------------------------------------------------------------------
static std::atomic<uint32_t> g_dirSeq{0};
struct TempDir {
    fs::path path;
    explicit TempDir(const char* tag = "pvbench") {
        auto seq = g_dirSeq.fetch_add(1, std::memory_order_relaxed);
        path = fs::temp_directory_path() /
               (std::string("rune_") + tag + "_" +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// ---------------------------------------------------------------------------
// BenchFixture — owns one disk-backed Log + PartitionReplica + Worker.
//
// Disk-backed Log is required because executeProduceWire_ calls
// log->appendNoSyncRestamp which is only on Log (not InMemoryLog).
//
// Pre-built batch and valid record value are stored to avoid per-iteration
// allocation inside the benchmark loop.
// ---------------------------------------------------------------------------
struct BenchFixture {
    TempDir                           tempDir;
    std::unique_ptr<Log>              log;
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker>           worker;

    std::vector<uint8_t> batchBytes;   // pre-built batch (reused every iteration)

    explicit BenchFixture()
    {
        log = std::make_unique<Log>(tempDir.path.string(),
                                    /*rollBytes=*/64LL * 1024 * 1024);
        log->recover();

        PartitionConfig cfg{};
        cfg.topicId             = kTopicId;
        cfg.partitionId         = static_cast<rune::replication::PartitionId>(kPartition);
        cfg.selfId              = 1;
        cfg.minIsr              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;
        replica = std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            log.get(), &transport);
        worker = std::make_unique<Worker>(/*workerId=*/0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        batchBytes = makeBatch(makeValidValue());
    }

    ~BenchFixture() = default;
};

// Global fixture (reset between benchmark runs via setup).
static std::unique_ptr<BenchFixture> g_fix;

static void SetupOff(const benchmark::State&) {
    g_fix = std::make_unique<BenchFixture>();
    // validate OFF: no SchemaUpdate pushed — validators_ is empty.
}

static void SetupOn(const benchmark::State&) {
    g_fix = std::make_unique<BenchFixture>();
    // validate ON: push a SchemaUpdate so the worker installs a Validator.
    WorkerOp schemaOp;
    schemaOp.type             = WorkerOp::Type::SchemaUpdate;
    schemaOp.topicId          = kTopicId;
    // Task 12: validators_ is now keyed by (topicId, version) — must match
    // makeValidValue()'s stamped version=1, or every produce in the timed
    // loop would silently hit the "unknown schema version" reject path.
    schemaOp.clientVersion    = 1;
    schemaOp.schemaUpdateBlob = makeCharactersBlob();
    g_fix->worker->tryEnqueueOp(std::move(schemaOp));
    // Checked writes: the topic's write spec makes the partition validate (a keyless, legacy-validating topic).
    rune::node::TopicWriteSpecWire spec;
    spec.topicId = kTopicId;
    spec.legacyValidate = true;
    WorkerOp specOp;
    specOp.type = WorkerOp::Type::WriteSpecUpdate;
    specOp.topicId = kTopicId;
    specOp.schemaUpdateBlob.resize(rune::node::kTopicWriteSpecWireLen);
    rune::node::encodeTopicWriteSpec(spec, specOp.schemaUpdateBlob);
    g_fix->worker->tryEnqueueOp(std::move(specOp));
    // Drive doWork to apply the SchemaUpdate before the timed loop.
    g_fix->worker->doWork(1);
}

// The same, at the checked feature level: every typed topic validates fully.
static void SetupChecked(const benchmark::State& s) {
    SetupOn(s);
    WorkerOp lvl;
    lvl.type = WorkerOp::Type::ClusterFeatureLevel;
    lvl.nextOffset = rune::replication::MetadataFeature::kCheckedWrites;
    g_fix->worker->tryEnqueueOp(std::move(lvl));
    g_fix->worker->doWork(1);
}

static void Teardown(const benchmark::State&) {
    g_fix.reset();
}

// ---------------------------------------------------------------------------
// The produce loop every full-path benchmark below runs: tryEnqueueOp → doWork → drainReturn of one single-record batch.
// ---------------------------------------------------------------------------
static void produceLoop(benchmark::State& state) {
    int64_t nowNs = 2;
    int64_t corr  = 0;

    for (auto _ : state) {
        WorkerOp op;
        op.type             = WorkerOp::Type::Produce;
        op.topicId          = kTopicId;
        op.partition        = kPartition;
        op.replica          = g_fix->replica.get();
        op.logPtr           = nullptr; // disk-backed replica: getLogFromWire_ uses replica->log()
        op.sessionId        = 1;
        op.correlationId    = ++corr;
        op.lastOffsetDelta  = 0;   // single record → delta 0
        op.recordCount      = 1;
        op.durabilityTier   = 0;   // Leader tier
        // recordBatchView: zero-copy span into the preallocated batchBytes.
        op.recordBatchView  = std::span<const uint8_t>(g_fix->batchBytes.data(),
                                                        g_fix->batchBytes.size());

        while (!g_fix->worker->tryEnqueueOp(op)) {
            g_fix->worker->doWork(nowNs++);
        }

        bool acked = false;
        for (int i = 0; i < 10'000 && !acked; ++i) {
            g_fix->worker->doWork(nowNs++);
            g_fix->worker->drainReturn([&](WorkerReturn&&) { acked = true; });
        }
        benchmark::DoNotOptimize(acked);
    }

    state.SetItemsProcessed(state.iterations());
}

// BM_ProduceValidate_Off — no write state (an untyped topic).
static void BM_ProduceValidate_Off(benchmark::State& state) { produceLoop(state); }

// BM_ProduceValidate_On — a validated topic: checkTypedBatch walks the batch (Worker::produceCore_).
static void BM_ProduceValidate_On(benchmark::State& state) { produceLoop(state); }

// ---------------------------------------------------------------------------
// BM_CheckedWriteHook — checked writes Part 02.
//   CheckOnly1Record / CheckOnly64Records: checkTypedBatch alone on a valid keyed batch at the checked level.  The
//     one-record p50 is a hard budget gate (spec §14: new typed-topic validation <= 1 us per one-record batch).
//   UntypedPath / TypedLegacyPath / TypedCheckedPath: the whole produce path; their deltas are the cost of the write
//     state's branch and of full validation.
// ---------------------------------------------------------------------------
static rune::node::TopicWriteSpec makeCheckSpec() {
    rune::node::TopicWriteSpec spec;
    const auto desc = DescriptorBuilder("characters")
        .add("id",   PrimType::Int64)
        .add("type", PrimType::Enum, 1, false, {0, 1, 2})
        .add("name", PrimType::String)
        .build();
    spec.installVersion(1, desc);
    rune::node::TopicWriteSpecWire facts;
    facts.topicId = kTopicId;
    facts.keyFieldId = desc.find("id")->id;
    facts.keyType = PrimType::Int64;
    facts.keyWidth = 8;
    facts.legacyValidate = true;
    spec.setFacts(facts);
    return spec;
}

static std::vector<uint8_t> makeKeyedBatch(int records) {
    const auto value = makeValidValue();   // id = 1
    const char key[8] = {1, 0, 0, 0, 0, 0, 0, 0};   // the key field's bytes
    RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < records; ++i)
        b.append(std::string_view(key, 8), std::string_view(reinterpret_cast<const char*>(value.data()), value.size()), 1000);
    return b.build();
}

static void BM_CheckedWriteHook_CheckOnly1Record(benchmark::State& state) {
    const auto spec  = makeCheckSpec();
    const auto batch = makeKeyedBatch(1);
    constexpr int kCalls = 1000;
    std::vector<double> ns(kCalls);
    for (auto _ : state) {
        for (int i = 0; i < kCalls; ++i) {
            const auto t0 = std::chrono::steady_clock::now();
            const auto verdict = rune::node::checkTypedBatch(spec, batch, rune::replication::MetadataFeature::kCheckedWrites);
            const auto t1 = std::chrono::steady_clock::now();
            benchmark::DoNotOptimize(verdict);
            ns[static_cast<std::size_t>(i)] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
    }
    std::sort(ns.begin(), ns.end());
    const double p50 = ns[kCalls / 2], p99 = ns[kCalls * 99 / 100];
    state.counters["p50_ns"] = p50;
    state.counters["p99_ns"] = p99;
    rune::bench::gateCheck(state, p50 <= 1000.0, "p50_ns > 1000 (spec 14: new validation <= 1 us p50 per one-record batch)");
    state.SetItemsProcessed(state.iterations() * kCalls);
}

static void BM_CheckedWriteHook_CheckOnly64Records(benchmark::State& state) {
    const auto spec  = makeCheckSpec();
    const auto batch = makeKeyedBatch(64);
    for (auto _ : state) {
        const auto verdict = rune::node::checkTypedBatch(spec, batch, rune::replication::MetadataFeature::kCheckedWrites);
        benchmark::DoNotOptimize(verdict);
    }
    state.counters["ns_per_record"] = benchmark::Counter(static_cast<double>(state.iterations()) * 64.0,
                                                         benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.SetItemsProcessed(state.iterations() * 64);
}

static void BM_CheckedWriteHook_UntypedPath(benchmark::State& state) { produceLoop(state); }
static void BM_CheckedWriteHook_TypedLegacyPath(benchmark::State& state) { produceLoop(state); }
static void BM_CheckedWriteHook_TypedCheckedPath(benchmark::State& state) { produceLoop(state); }

// ---------------------------------------------------------------------------
// Sequential A/B registration (n=15 repetitions each).
//
// Google Benchmark runs all 15 repetitions of BM_ProduceValidate_Off, then
// all 15 of BM_ProduceValidate_On (sequential AABB — not interleaved ABAB).
// True ABAB interleaving is not achievable with separately registered BM_*
// functions.  Report the mean_time (ns/op) delta:
//   delta_ns = mean(On) − mean(Off)
// is the cost of the Task 7 validation hook per produce.
// ---------------------------------------------------------------------------
BENCHMARK(BM_ProduceValidate_Off)
    ->Setup(SetupOff)
    ->Teardown(Teardown)
    ->Repetitions(15)
    ->ReportAggregatesOnly(false)
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_ProduceValidate_On)
    ->Setup(SetupOn)
    ->Teardown(Teardown)
    ->Repetitions(15)
    ->ReportAggregatesOnly(false)
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_CheckedWriteHook_CheckOnly1Record)
    ->Name("BM_CheckedWriteHook/CheckOnly1Record")
    ->Repetitions(15)
    ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_CheckOnly64Records)
    ->Name("BM_CheckedWriteHook/CheckOnly64Records")
    ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_UntypedPath)
    ->Name("BM_CheckedWriteHook/UntypedPath")
    ->Setup(SetupOff)->Teardown(Teardown)->Repetitions(15)->ReportAggregatesOnly(false)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_TypedLegacyPath)
    ->Name("BM_CheckedWriteHook/TypedLegacyPath")
    ->Setup(SetupOn)->Teardown(Teardown)->Repetitions(15)->ReportAggregatesOnly(false)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_TypedCheckedPath)
    ->Name("BM_CheckedWriteHook/TypedCheckedPath")
    ->Setup(SetupChecked)->Teardown(Teardown)->Repetitions(15)->ReportAggregatesOnly(false)->Unit(benchmark::kNanosecond);

} // namespace
