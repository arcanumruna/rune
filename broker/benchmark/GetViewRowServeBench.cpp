// benchmark/GetViewRowServeBench.cpp — issue #82 (VLS Part A): the GetViewRow SERVE path on the REAL worker pass.
//
//   BM_GetViewRowServe — kBatch GetViewRow ops (op ring) for keys of a Ready view over a leader source partition,
//     one worker pass answers them (Worker::executeGetViewRowWire_: plan + fingerprint, the source-role check, the
//     read lease, the leadership generation, the fold barrier, the table probe, the zero-copy row gather), the
//     answers are drained off the return ring.  Counters: ns_per_read (wall time of the timed loop / reads) and
//     allocs_per_op (operator new calls per read inside the timed loop; the gate is 0).
//
// The same source builds on a baseline without the read lease: armLease_() arms it only where the worker has it (a
// multi-node broker's worker: the lease is required and a valid one is held), so a baseline-vs-tip ABAB compares the
// production read path before and after.  Gate (VLS bench table): <= +30 ns per read vs the baseline; 0 allocations.
#include "BenchGate.h"

#include <benchmark/benchmark.h>
#include <rune/codec/Descriptor.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

extern std::atomic<int64_t> g_allocCount;   // BenchmarkMain.cpp
extern std::atomic<bool>    g_allocEnabled;

namespace {

namespace fs = std::filesystem;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;

constexpr int32_t  kSrc   = 1;
constexpr int32_t  kView  = 0x40000082;
constexpr uint64_t kKeys  = 1024;
constexpr int      kBatch = 8;   // reads per pass (within the default ingress drain limit)

class NullXport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {
        return true;
    }
};

rune::codec::Descriptor charSrc() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

rune::view::ViewPlan hpPlan() {
    rune::view::ViewPlan v;
    v.viewName = "char_hp_serve_bench"; v.key = "characterId"; v.sources = {{kSrc, "characterId"}};
    v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;   // transient: no checkpoint IO in the measured pass
    return v;
}

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

/// Arms the read lease where the worker has one (the tip): required, and a lease that never expires during the run
/// (the steady state of a multi-node broker whose controller renews it every 20 ms).  A no-op on the baseline.
template <class W, class Op>
void armLease_(W& w) {
    if constexpr (requires(W& x) { x.requireViewReadLease(); }) {
        w.requireViewReadLease();
        Op op;
        op.type       = Op::Type::ReadLease;
        op.nextOffset = INT64_MAX - 1;   // the lease end (lease clock): valid for the whole run
        while (!w.tryEnqueueOp(std::move(op))) w.doWork(1);
    }
}

struct ServeFixture {
    std::unique_ptr<rune::storage::InMemoryLog> mem;
    NullXport                                   transport;
    std::unique_ptr<PartitionReplica>           replica;
    rune::node::ViewSnapshotChannel             ch;
    std::unique_ptr<rune::node::WorkerViewSet>  vs;
    std::unique_ptr<Worker>                     worker;
    std::string                                 dir;
    int64_t                                     nowNs = 1;
    int64_t                                     corr  = 0;

    ServeFixture() {
        dir = (fs::temp_directory_path() / ("rune_gvrserve_" + std::to_string(::getpid()))).string();
        fs::create_directories(dir);
        PartitionConfig cfg{};
        cfg.topicId = kSrc; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        const std::vector<rune::replication::NodeId> one{1};
        mem     = std::make_unique<rune::storage::InMemoryLog>(64u << 20, 1'000'000);
        replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Follower, 0, one, one, mem.get(),
                                                     &transport);
        replica->becomeLeader(1, {1});   // a real leadership generation, as production
        vs      = std::make_unique<rune::node::WorkerViewSet>(ch, dir + "/views");
        worker  = std::make_unique<Worker>(0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        const auto src = charSrc();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(hpPlan(), srcs, kView).value();
        vs->installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        worker->setViewSet(vs.get());
        armLease_<Worker, WorkerOp>(*worker);
    }
    ~ServeFixture() {
        worker.reset();
        vs.reset();
        replica.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    void pass() { worker->doWork(nowNs++); }
    /// Produce every key once and pass until the view serves the last one.
    bool warm() {
        std::vector<std::vector<uint8_t>> batches;
        for (uint64_t k = 0; k < kKeys; ++k) batches.push_back(keyedBatch(k, static_cast<int32_t>(k)));
        for (uint64_t k = 0; k < kKeys; ++k) {
            WorkerOp op;
            op.type = WorkerOp::Type::Produce; op.topicId = kSrc; op.partition = 0;
            op.replica = replica.get(); op.sessionId = 1; op.correlationId = ++corr;
            op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
            op.recordBatchView = std::span<const uint8_t>(batches[k]);
            while (!worker->tryEnqueueOp(std::move(op))) { pass(); worker->drainReturn([](WorkerReturn&&) {}); }
            pass();   // the op holds a view of batches[k]: execute it before the vector goes
            worker->drainReturn([](WorkerReturn&&) {});
        }
        for (int i = 0; i < 100'000; ++i) {
            pass();
            worker->drainReturn([](WorkerReturn&&) {});
            if (readOne(kKeys - 1) == 0) return true;
        }
        return false;
    }
    WorkerOp readOp(uint64_t key) {
        WorkerOp op;
        op.type = WorkerOp::Type::GetViewRow; op.topicId = kView; op.partition = 0;
        op.sessionId = 1; op.correlationId = ++corr;
        op.nextOffset = static_cast<int64_t>(key); op.hwBound = 0;
        return op;
    }
    /// One read, answered within a few passes: its status (-1 = none).
    int readOne(uint64_t key) {
        if (!worker->tryEnqueueOp(readOp(key))) return -1;
        int status = -1;
        for (int i = 0; i < 8 && status < 0; ++i) {
            pass();
            worker->drainReturn([&](WorkerReturn&& r) {
                if (r.type == WorkerReturn::Type::ViewRowResult) status = r.errorCode;
            });
        }
        return status;
    }
};

} // namespace

static void BM_GetViewRowServe(benchmark::State& state) {
    ServeFixture f;
    if (!f.warm()) { state.SkipWithError("the view never served the warm-up rows"); return; }
    uint64_t key = 0;
    int64_t  reads = 0, bad = 0;
    g_allocCount.store(0);
    g_allocEnabled.store(true, std::memory_order_release);
    const auto t0 = std::chrono::steady_clock::now();
    for (auto _ : state) {
        for (int i = 0; i < kBatch; ++i) {
            while (!f.worker->tryEnqueueOp(f.readOp(key))) f.pass();
            key = (key + 1) % kKeys;
        }
        int got = 0;
        for (int p = 0; p < 64 && got < kBatch; ++p) {
            f.pass();
            f.worker->drainReturn([&](WorkerReturn&& r) {
                ++got;
                if (r.errorCode != 0) ++bad;
            });
        }
        reads += got;
    }
    const auto t1 = std::chrono::steady_clock::now();
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t allocs = g_allocCount.load(std::memory_order_acquire);
    const double ns = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    state.SetItemsProcessed(reads);
    state.counters["ns_per_read"]   = reads > 0 ? ns / static_cast<double>(reads) : 0.0;
    state.counters["allocs_per_op"] = reads > 0 ? static_cast<double>(allocs) / static_cast<double>(reads) : 0.0;
    state.counters["not_ok"]        = static_cast<double>(bad);
    rune::bench::gateCheck(state, bad == 0 && allocs == 0, "every read answers OK with zero allocations");
}
BENCHMARK(BM_GetViewRowServe)->Unit(benchmark::kNanosecond)->MinTime(0.5);
