// benchmark/ViewFoldSourceBench.cpp — Phase 2b E2 carry-over (a): a VIEW folding
// a TRANSIENT source vs a DISK source on the REAL worker pass.
//
//   BM_ViewFoldSource_{Disk,Transient}_Latency — produce ONE keyed record (op
//     ring, acks=leader) → worker passes until the view row carries its value.
//     One sample = "a produced record is visible in the view" on one worker.
//     Counters p50Us / p99Us / p999Us.
//   BM_ViewFoldSource_{Disk,Transient}_Catchup — 20 000 records already in the
//     source, the view installed afterwards (a Fresh instance folds from the
//     ring/log start) → worker passes until it folded them all.  Counter
//     records_per_s (fold throughput through the real read path).
//
// Gate (E2): Transient p50Us and p99Us <= Disk's; Transient records_per_s >=
// Disk's (the ring read is a pointer view, the disk read a pread).
#include <benchmark/benchmark.h>
#include <rune/codec/Descriptor.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;

constexpr int32_t  kSrc            = 1;
constexpr int32_t  kView           = 0x40000077;
constexpr uint64_t kKeys           = 1024;
constexpr uint64_t kBatches        = 4096;     // pre-built; key = i % kKeys, hp = i
constexpr uint64_t kCatchupRecords = 20'000;

class NullXport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {
        return true;
    }
};

std::string tempDir(const char* tag) {
    static int ctr = 0;
    const fs::path p = fs::temp_directory_path() /
                       ("rune_vfsrc_" + std::string(tag) + "_" + std::to_string(++ctr) + "_" + std::to_string(::getpid()));
    fs::create_directories(p);
    return p.string();
}

rune::codec::Descriptor charSrc() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

rune::view::ViewPlan hpPlan() {
    rune::view::ViewPlan v;
    v.viewName = "char_hp_src_bench"; v.key = "characterId"; v.sources = {{kSrc, "characterId"}};
    v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;   // a transient view: no checkpoint IO in the measured pass (legal over both sources)
    return v;
}

// One keyed single-record batch: key = 8-byte LE id, value = [blockLength 12][version 1][id i64][hp i32].
std::vector<uint8_t> keyedBatch(uint64_t id, int32_t hp) {
    std::string key(8, '\0');
    for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<char>(id >> (8 * i));
    std::string value;
    auto le = [&](uint64_t x, int n) { for (int i = 0; i < n; ++i) value.push_back(static_cast<char>(x >> (8 * i))); };
    le(12, 2); le(1, 2); le(id, 8); le(static_cast<uint32_t>(hp), 4);
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(key, value, 0);
    return b.build();
}

struct FoldSourceFixture {
    std::string                                 dir;
    std::unique_ptr<rune::storage::Log>         disk;   // disk source
    std::unique_ptr<rune::storage::InMemoryLog> mem;    // transient source
    NullXport                                   transport;
    std::unique_ptr<PartitionReplica>           replica;
    rune::node::ViewSnapshotChannel             ch;
    std::unique_ptr<rune::node::WorkerViewSet>  vs;
    std::unique_ptr<Worker>                     worker;
    int64_t                                     nowNs = 1;
    int64_t                                     corr  = 0;

    explicit FoldSourceFixture(bool transient) : dir(tempDir(transient ? "t" : "d")) {
        PartitionConfig cfg{};
        cfg.topicId = kSrc; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        const std::vector<rune::replication::NodeId> one{1};
        if (transient) {
            mem     = std::make_unique<rune::storage::InMemoryLog>(64u << 20, 1'000'000);
            replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Leader, 1, one, one,
                                                         mem.get(), &transport);
        } else {
            fs::create_directories(dir + "/log");
            disk = std::make_unique<rune::storage::Log>(dir + "/log", 64LL * 1024 * 1024);
            disk->recover();
            replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Leader, 1, one, one,
                                                         disk.get(), &transport);
        }
        vs     = std::make_unique<rune::node::WorkerViewSet>(ch, dir + "/views");
        worker = std::make_unique<Worker>(0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
    }
    ~FoldSourceFixture() {
        worker.reset();
        vs.reset();
        replica.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    void installView() {
        const auto src = charSrc();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(hpPlan(), srcs, kView).value();
        vs->installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        worker->setViewSet(vs.get());
    }
    /// The gateway-routed produce op for `batch` (acks=leader); zero-copy view.
    WorkerOp produceOp(const std::vector<uint8_t>& batch) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = kSrc; op.partition = 0;
        op.replica = replica.get(); op.logPtr = disk.get();   // transient: nullptr (the ring is the replica's)
        op.sessionId = 1; op.correlationId = ++corr;
        op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
        op.recordBatchView = std::span<const uint8_t>(batch);
        return op;
    }
    void pass() {
        worker->doWork(nowNs++);
        worker->drainReturn([](WorkerReturn&&) {});   // the acks (keep the return ring drained)
    }
    [[nodiscard]] bool rowIs(uint64_t key, int32_t hp) const {
        const rune::view::RowLayout* L = nullptr;
        const uint8_t* row = vs->getRow(kView, 0, key, L);
        return row != nullptr && rune::view::readSlot(row, *L, 1).i == hp;
    }
};

void reportUs(benchmark::State& state, std::vector<int64_t>& ns) {
    if (ns.empty()) return;
    std::sort(ns.begin(), ns.end());
    state.counters["p50Us"] = static_cast<double>(ns[ns.size() / 2]) / 1000.0;
    state.counters["p99Us"] = static_cast<double>(ns[std::min(ns.size() - 1, ns.size() * 99 / 100)]) / 1000.0;
    state.counters["p999Us"] = static_cast<double>(ns[std::min(ns.size() - 1, ns.size() * 999 / 1000)]) / 1000.0;
}

void runLatency(benchmark::State& state, bool transient) {
    FoldSourceFixture f(transient);
    f.installView();
    std::vector<std::vector<uint8_t>> batches;
    batches.reserve(kBatches);
    for (uint64_t i = 0; i < kBatches; ++i) batches.push_back(keyedBatch(i % kKeys, static_cast<int32_t>(i)));
    // Warm-up (not timed): every key folded once — table grown, descriptor cache warm.
    for (uint64_t i = 0; i < kKeys; ++i) {
        while (!f.worker->tryEnqueueOp(f.produceOp(batches[i]))) f.pass();
        for (int k = 0; k < 1'000'000 && !f.rowIs(i, static_cast<int32_t>(i)); ++k) f.pass();
    }
    std::vector<int64_t> samples;
    samples.reserve(1 << 16);
    uint64_t i = kKeys;
    for (auto _ : state) {
        const uint64_t idx = i % kBatches;
        const WorkerOp op  = f.produceOp(batches[idx]);
        const auto t0 = std::chrono::steady_clock::now();
        while (!f.worker->tryEnqueueOp(op)) f.pass();
        bool visible = false;
        for (int k = 0; k < 5'000'000 && !visible; ++k) {
            f.pass();
            visible = f.rowIs(idx % kKeys, static_cast<int32_t>(idx));
        }
        const auto t1 = std::chrono::steady_clock::now();
        if (!visible) { state.SkipWithError("the produced record never reached the view"); break; }
        if (samples.size() < samples.capacity())
            samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        ++i;
    }
    state.SetItemsProcessed(state.iterations());
    reportUs(state, samples);
}

void runCatchup(benchmark::State& state, bool transient) {
    for (auto _ : state) {
        state.PauseTiming();
        auto f = std::make_unique<FoldSourceFixture>(transient);
        std::vector<uint8_t> batch;
        for (uint64_t r = 0; r < kCatchupRecords; ++r) {
            batch = keyedBatch(r % kKeys, static_cast<int32_t>(r));
            while (!f->worker->tryEnqueueOp(f->produceOp(batch))) f->pass();
            f->pass();   // the op ring holds a view of `batch`: execute it before the next rebuild
        }
        for (int k = 0; k < 64; ++k) f->pass();   // HW / RC state settle
        f->installView();                         // Fresh: folds from the source start
        const uint64_t lastKey = (kCatchupRecords - 1) % kKeys;
        const auto     lastHp  = static_cast<int32_t>(kCatchupRecords - 1);
        state.ResumeTiming();
        const auto t0 = std::chrono::steady_clock::now();
        bool done = false;
        for (int k = 0; k < 50'000'000 && !done; ++k) {
            f->pass();
            done = f->rowIs(lastKey, lastHp);
        }
        const auto t1 = std::chrono::steady_clock::now();
        state.PauseTiming();
        if (!done) { state.SkipWithError("the view never folded the whole source"); break; }
        const double secs = std::chrono::duration<double>(t1 - t0).count();
        state.counters["records_per_s"] = static_cast<double>(kCatchupRecords) / secs;
        f.reset();
        state.ResumeTiming();
    }
}

} // namespace

static void BM_ViewFoldSource_Disk_Latency(benchmark::State& s)      { runLatency(s, /*transient=*/false); }
static void BM_ViewFoldSource_Transient_Latency(benchmark::State& s) { runLatency(s, /*transient=*/true); }
static void BM_ViewFoldSource_Disk_Catchup(benchmark::State& s)      { runCatchup(s, false); }
static void BM_ViewFoldSource_Transient_Catchup(benchmark::State& s) { runCatchup(s, true); }

BENCHMARK(BM_ViewFoldSource_Disk_Latency)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_ViewFoldSource_Transient_Latency)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_ViewFoldSource_Disk_Catchup)->Unit(benchmark::kMillisecond)->Iterations(3);
BENCHMARK(BM_ViewFoldSource_Transient_Catchup)->Unit(benchmark::kMillisecond)->Iterations(3);
