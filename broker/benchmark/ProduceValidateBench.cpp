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
// Checked writes Part 02 adds BM_CheckedWriteHook/*: the write hook's check alone (the <= 1 us p50 budget gate) and the
// whole produce path for an untyped topic, a typed topic below the feature level and a typed topic at it.
// Part 06 adds BM_CheckedWriteHook/autoinc_*: check + stage + onAppended of a key-0 create batch on an autoincrement topic.
//
// Uses a disk-backed Log + PartitionReplica (same pattern as
// WorkerProduceLatencyBench.cpp / AsyncDurabilityTest.cpp).
// No Aeron media driver needed.
// ---------------------------------------------------------------------------

#include <benchmark/benchmark.h>

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/CheckedWriteHook.h>
#include <rune/node/VerdictSource.h>
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
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

// Defined once in benchmark/BenchmarkMain.cpp (see ViewFoldBench.cpp).
extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

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
    int64_t  nowNs   = 2;
    int64_t  corr    = 0;
    uint64_t refused = 0;   // a refused produce never appends: the loop would time the reject path instead

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
            g_fix->worker->drainReturn([&](WorkerReturn&& r) { acked = true; refused += r.errorCode != 0 ? 1u : 0u; });
        }
        benchmark::DoNotOptimize(acked);
    }

    state.SetItemsProcessed(state.iterations());
    rune::bench::gateCheck(state, refused == 0, "a produce was refused: the setup must make the timed batch valid");
}

// BM_ProduceValidate_Off — no write state (an untyped topic).
static void BM_ProduceValidate_Off(benchmark::State& state) { produceLoop(state); }

// BM_ProduceValidate_On — a validated topic: checkTypedBatch walks the batch (Worker::produceCore_).
static void BM_ProduceValidate_On(benchmark::State& state) { produceLoop(state); }

// ---------------------------------------------------------------------------
// BM_CheckedWriteHook — checked writes Part 02.
//   hook_check/1 / hook_check/64: CheckedWriteHook::check alone on a valid keyed batch at the checked level.  The
//     one-record p50 is a hard budget gate (spec §14: new typed-topic validation <= 1 us per one-record batch).
//   UntypedPath / TypedLegacyPath / typed_produce/1: the whole produce path; their deltas are the cost of the write
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

static rune::node::WriteFacts checkedFacts() {
    rune::node::WriteFacts facts;
    facts.levelCheckedWrites = true;
    facts.acks               = 1;
    return facts;
}

static void BM_CheckedWriteHook_HookCheck1(benchmark::State& state) {
    const auto spec  = makeCheckSpec();
    const auto batch = makeKeyedBatch(1);
    rune::node::PartitionWriteState ws;
    ws.spec = &spec;
    const auto facts = checkedFacts();
    constexpr int kCalls = 1000;
    std::vector<double> ns(kCalls);
    for (auto _ : state) {
        for (int i = 0; i < kCalls; ++i) {
            rune::node::CheckedWriteHook hook(ws, facts, {});
            const auto t0 = std::chrono::steady_clock::now();
            const auto verdict = hook.check(batch.data(), batch.size(), 1);
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

static void BM_CheckedWriteHook_HookCheck64(benchmark::State& state) {
    const auto spec  = makeCheckSpec();
    const auto batch = makeKeyedBatch(64);
    rune::node::PartitionWriteState ws;
    ws.spec = &spec;
    const auto facts = checkedFacts();
    for (auto _ : state) {
        rune::node::CheckedWriteHook hook(ws, facts, {});
        const auto verdict = hook.check(batch.data(), batch.size(), 64);
        benchmark::DoNotOptimize(verdict);
    }
    state.counters["ns_per_batch"] = benchmark::Counter(static_cast<double>(state.iterations()), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
    state.SetItemsProcessed(state.iterations() * 64);
}

// ---------------------------------------------------------------------------
// BM_CheckedWriteHook/autoinc_{1,16,4k} — checked writes Part 06: check() + stage() + onAppended() of one key-0 create batch
// of 1, 16 and 64 (<= 4096 B) records on a spec-02 autoincrement topic, over a log whose append returns at once.  The
// reservation block is 64 x the batch's records, so one batch in 64 appends a reservation record.
//   hook_p50_ns / hook_p99_ns: the three calls per batch, the scratch copy and id patch included.
//   steady_alloc: heap allocations over the measured batches (GATE: == 0).
//   autoinc_4k p50 is a hard budget gate (spec 14: 1 us, the copy included).
// ---------------------------------------------------------------------------
struct NullLog {
    int64_t next = 0;
    int64_t appendNoSyncRestamp(const uint8_t*, std::size_t, int32_t lastOffsetDelta, int64_t& base) {
        base = next;
        next += lastOffsetDelta + 1;
        return base;
    }
};

static void autoIncHookBench(benchmark::State& state, int records) {
    const auto desc = DescriptorBuilder("a", 1).add("id", PrimType::UInt64).add("v", PrimType::Int64).add("tag", PrimType::UInt16).build();
    rune::node::TopicWriteSpec spec;
    spec.installVersion(1, desc);
    rune::node::TopicWriteSpecWire wire;
    wire.topicId = kTopicId;
    wire.keyFieldId = 0;
    wire.keyType = PrimType::UInt64;
    wire.keyWidth = 8;
    wire.keyIndex = true;
    wire.autoIncrement = true;
    wire.autoIncrementMax = rune::node::effectiveAutoIncMax(PrimType::UInt64, rune::protocol::kAutoIncrementMaxDefault);
    wire.autoIncrementReserve = 64u * static_cast<uint32_t>(records);
    wire.partitionCount = 1;
    spec.setFacts(wire);

    rune::node::PartitionWriteState ws;
    ws.spec = &spec;
    rune::node::ConstraintSet set;
    set.id = 1;
    set.count = 1;
    set.members[0] = rune::node::ConstraintSetMember{0, rune::view::VerdictViewKind::KeyIndex, rune::node::kKeyIndexPlanFingerprint};
    ws.constraintSets.onChange(7, set);
    ws.assignedBases.reserve(64);
    ws.autoInc.configure(0, 1, spec.autoIncrementMax, wire.autoIncrementReserve);
    ws.autoInc.beginScan();
    (void)ws.autoInc.finishScan();

    rune::node::WriteFacts facts;
    facts.acks = 3;
    facts.idempotent = true;
    facts.levelCheckedWrites = true;
    facts.importGranted = true;
    facts.producerId = 42;

    // {id u64, v int64, tag u16}: [blockLength u16][version u16][id][v][tag], one key-0 create per record
    const auto le = [](uint64_t v, std::size_t n) { std::string r(n, '\0'); for (std::size_t i = 0; i < n; ++i) r[i] = static_cast<char>(v >> (8 * i)); return r; };
    const std::string key = le(0, 8), value = le(18, 2) + le(1, 2) + le(0, 8) + le(7, 8) + le(3, 2);
    RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < records; ++i) b.append(key, value, 1000, 1);
    const auto batch = b.build();
    std::vector<uint8_t> scratch(64 * 1024);
    NullLog log;

    const auto once = [&] {
        rune::node::CheckedWriteHook hook(ws, facts, scratch);
        const auto verdict = hook.check(batch.data(), batch.size(), records);
        const auto staged = hook.stage(log, batch.data(), batch.size());
        int64_t base = 0;
        (void)log.appendNoSyncRestamp(staged.batch, staged.len, records - 1, base);
        hook.onAppended(base, base + records - 1);
        benchmark::DoNotOptimize(verdict);
        benchmark::DoNotOptimize(staged);
    };

    constexpr int kWarmup = 4096, kCalls = 65536;
    std::vector<double> ns(kCalls);
    int64_t steadyAlloc = 0;
    for (auto _ : state) {
        for (int i = 0; i < kWarmup; ++i) once();
        g_allocCount.store(0, std::memory_order_relaxed);
        g_allocEnabled.store(true, std::memory_order_relaxed);
        for (int i = 0; i < kCalls; ++i) {
            const auto t0 = std::chrono::steady_clock::now();
            once();
            const auto t1 = std::chrono::steady_clock::now();
            ns[static_cast<std::size_t>(i)] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
        g_allocEnabled.store(false, std::memory_order_relaxed);
        steadyAlloc = g_allocCount.load(std::memory_order_relaxed);
    }
    std::sort(ns.begin(), ns.end());
    const double p50 = ns[kCalls / 2], p99 = ns[kCalls * 99 / 100];
    state.counters["hook_p50_ns"] = p50;
    state.counters["hook_p99_ns"] = p99;
    state.counters["steady_alloc"] = static_cast<double>(steadyAlloc);
    state.SetItemsProcessed(state.iterations() * kCalls);
    const bool gated = records == 64;
    const bool pass = steadyAlloc == 0 && (!gated || p50 < 1000.0);
    rune::bench::gateCheck(state, pass, "an autoincrement batch allocates in steady state or the hook p50 reaches 1000 ns (spec 14)");
    if (gated)
        std::fprintf(stderr, "[CheckedWriteHook autoinc gate] p50=%.0f ns (budget 1000) => %s\n", p50, pass ? "PASS" : "FAIL");
}

static void BM_CheckedWriteHook_AutoInc1(benchmark::State& state) { autoIncHookBench(state, 1); }
static void BM_CheckedWriteHook_AutoInc16(benchmark::State& state) { autoIncHookBench(state, 16); }
static void BM_CheckedWriteHook_AutoInc4k(benchmark::State& state) { autoIncHookBench(state, 64); }

static void BM_CheckedWriteHook_UntypedPath(benchmark::State& state) { produceLoop(state); }
static void BM_CheckedWriteHook_TypedLegacyPath(benchmark::State& state) { produceLoop(state); }
static void BM_CheckedWriteHook_TypedProduce1(benchmark::State& state) { produceLoop(state); }

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

BENCHMARK(BM_CheckedWriteHook_HookCheck1)
    ->Name("BM_CheckedWriteHook/hook_check/1")
    ->Repetitions(15)
    ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_HookCheck64)
    ->Name("BM_CheckedWriteHook/hook_check/64")
    ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_UntypedPath)
    ->Name("BM_CheckedWriteHook/UntypedPath")
    ->Setup(SetupOff)->Teardown(Teardown)->Repetitions(15)->ReportAggregatesOnly(false)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_TypedLegacyPath)
    ->Name("BM_CheckedWriteHook/TypedLegacyPath")
    ->Setup(SetupOn)->Teardown(Teardown)->Repetitions(15)->ReportAggregatesOnly(false)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_TypedProduce1)
    ->Name("BM_CheckedWriteHook/typed_produce/1")
    ->Setup(SetupChecked)->Teardown(Teardown)->Repetitions(15)->ReportAggregatesOnly(false)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_CheckedWriteHook_AutoInc1)->Name("BM_CheckedWriteHook/autoinc_1")->Iterations(1)->UseRealTime()->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_CheckedWriteHook_AutoInc16)->Name("BM_CheckedWriteHook/autoinc_16")->Iterations(1)->UseRealTime()->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_CheckedWriteHook_AutoInc4k)->Name("BM_CheckedWriteHook/autoinc_4k")->Iterations(1)->UseRealTime()->Unit(benchmark::kMicrosecond);

} // namespace
