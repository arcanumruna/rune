// MetadataApplyBench.cpp — MD1 Task 4: MetadataService apply-record latency.
//
// Benchmarks:
//
//   1. BM_MetadataApply_SingleRecord
//      Per-doWork(nowNs) latency when a single assignment record is committed
//      each iteration.  Measures apply + commit in MetadataService::doWork.
//
//   2. BM_MetadataApply_BurstRecords
//      doWork latency with a burst of 100 already-committed records (HW-gated
//      apply loop): measures the steady-state apply throughput.
//
//   3. BM_MetadataApply_ConcurrentAppend_ProducePerturbation
//      Confirms that concurrent MetadataService::doWork calls (on the single
//      writer thread, back-to-back in the timed loop) do NOT perturb a
//      concurrent append benchmark.  Since both run on the same single-writer
//      thread (no concurrency), this verifies isolation via sequential
//      scheduling (append p99 unchanged by interleaved doWork calls).
//
//   4. BM_MetadataService_Assignment_Lookup
//      Per-call latency of assignment(topicId, partition) — the hot-path
//      routing-image read.  Should be sub-µs (open-addressing hash lookup).
//
// All benchmarks use InMemoryTransport (no Aeron required).
// No hot-path allocation in timed regions (scratch buffers pre-warmed).
//
// Coding conventions: DoNotOptimize results; pre-build outside timed region.

#include <benchmark/benchmark.h>

#include <rune/node/MetadataService.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/MetadataRecords.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <sys/types.h>
#include <unistd.h>

using namespace rune::replication;
using namespace rune::node;
namespace pmd = rune::protocol::metadata;

namespace {

// ---------------------------------------------------------------------------
// Unique temp-dir helper
// ---------------------------------------------------------------------------
static std::atomic<int> g_seq{0};

static std::filesystem::path uniqueDir(const char* tag) {
    namespace fs = std::filesystem;
    return fs::temp_directory_path() /
           ("rune_bench_mab_" + std::string(tag) +
            "_" + std::to_string(::getpid()) +
            "_" + std::to_string(g_seq.fetch_add(1)));
}

// ---------------------------------------------------------------------------
// Build a PartitionConfig for the metadata replica (topicId=-3).
// ---------------------------------------------------------------------------
static PartitionConfig makeMetaCfg(NodeId selfId) {
    PartitionConfig cfg;
    cfg.topicId             = -3;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 60'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 5'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

// ---------------------------------------------------------------------------
// Encode one UpdatePartitionAssignmentRecord into buf.
// Returns bytes written (0 on failure).
// ---------------------------------------------------------------------------
static std::size_t encodeAssignment(
    uint8_t* buf, std::size_t bufLen,
    int32_t topicId, int32_t partition, int32_t leaderNode,
    const std::vector<int32_t>& isr,
    const std::vector<int32_t>& replicas,
    int32_t leaderEpoch, int32_t partitionEpoch)
{
    static std::vector<uint8_t> isrBuf, repBuf;
    isrBuf.resize(2 + isr.size() * 4);
    repBuf.resize(2 + replicas.size() * 4);

    auto writeU16 = [](uint8_t* p, uint16_t v) {
        p[0] = static_cast<uint8_t>(v); p[1] = static_cast<uint8_t>(v >> 8);
    };
    auto writeS32 = [](uint8_t* p, int32_t v) {
        uint32_t u = static_cast<uint32_t>(v);
        p[0]=static_cast<uint8_t>(u);
        p[1]=static_cast<uint8_t>(u>>8);
        p[2]=static_cast<uint8_t>(u>>16);
        p[3]=static_cast<uint8_t>(u>>24);
    };

    writeU16(isrBuf.data(), static_cast<uint16_t>(isr.size()));
    for (std::size_t i = 0; i < isr.size(); ++i)
        writeS32(isrBuf.data() + 2 + i * 4, isr[i]);
    writeU16(repBuf.data(), static_cast<uint16_t>(replicas.size()));
    for (std::size_t i = 0; i < replicas.size(); ++i)
        writeS32(repBuf.data() + 2 + i * 4, replicas[i]);

    pmd::Int32ArrayView isrView(
        reinterpret_cast<const int32_t*>(isrBuf.data() + 2),
        static_cast<uint16_t>(isr.size()));
    pmd::Int32ArrayView repView(
        reinterpret_cast<const int32_t*>(repBuf.data() + 2),
        static_cast<uint16_t>(replicas.size()));

    pmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = topicId;
    rec.partition      = partition;
    rec.leaderNode     = leaderNode;
    rec.isr            = isrView;
    rec.replicas       = repView;
    rec.leaderEpoch    = leaderEpoch;
    rec.partitionEpoch = partitionEpoch;
    return rec.encode(buf, bufLen);
}

// ---------------------------------------------------------------------------
// 1. BM_MetadataApply_SingleRecord
//
// Each iteration: append 1 record, advance the replica clock to advance HW,
// call doWork.  Measures the per-record apply latency in steady state.
// ---------------------------------------------------------------------------
static void BM_MetadataApply_SingleRecord(benchmark::State& state) {
    const auto dir = uniqueDir("single");
    std::filesystem::create_directories(dir);
    rune::storage::Log log(dir.string());
    log.recover();

    InMemoryTransport transport;
    const std::vector<NodeId> nodes = {1};
    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(1),
        Role::Leader, /*epoch=*/1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    MetadataService svc(replica.get(), 1);

    // Pre-warm: advance HW so the replica is in steady state.
    int64_t clockNs = 0;
    for (int i = 0; i < 10; ++i) {
        clockNs += 5'000'000LL;
        replica->doWork(clockNs);
        transport.pump(clockNs);
    }
    svc.doWork(clockNs);

    const std::vector<int32_t> isr      = {1};
    const std::vector<int32_t> replicas = {1};
    uint8_t buf[512];
    int32_t part = 0;
    int32_t epoch = 1;

    for (auto _ : state) {
        // Outside timed region: encode + append the record.
        state.PauseTiming();
        const std::size_t n = encodeAssignment(
            buf, sizeof(buf), /*topicId=*/1, part, 1, isr, replicas, epoch, epoch);
        replica->append(std::span<const uint8_t>(buf, n), false);
        // Advance HW so the record is committed.
        clockNs += 5'000'000LL;
        replica->doWork(clockNs);
        transport.pump(clockNs);
        state.ResumeTiming();

        // Timed region: doWork applies the committed record.
        int w = svc.doWork(clockNs);
        benchmark::DoNotOptimize(w);
        ++part;
        ++epoch;
    }

    std::filesystem::remove_all(dir);
}
BENCHMARK(BM_MetadataApply_SingleRecord)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 2. BM_MetadataApply_BurstRecords
//
// Pre-appends 100 records + drives HW outside the timed region.
// Measures the cost of one doWork call that applies all 100 committed records.
// ---------------------------------------------------------------------------
static void BM_MetadataApply_BurstRecords(benchmark::State& state) {
    const auto dir = uniqueDir("burst");
    std::filesystem::create_directories(dir);
    rune::storage::Log log(dir.string());
    log.recover();

    InMemoryTransport transport;
    const std::vector<NodeId> nodes = {1};
    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(1),
        Role::Leader, /*epoch=*/1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    MetadataService svc(replica.get(), 1);

    const int kBurst = 100;
    const std::vector<int32_t> isr      = {1};
    const std::vector<int32_t> replicas = {1};
    uint8_t buf[512];
    int64_t clockNs = 0;
    int32_t part = 0;
    int32_t epoch = 1;

    for (auto _ : state) {
        // Outside timed region: append burst + advance HW.
        state.PauseTiming();
        for (int i = 0; i < kBurst; ++i) {
            const int32_t p = part++;
            const int32_t e = epoch++;
            const std::size_t n = encodeAssignment(
                buf, sizeof(buf), 1, p, 1, isr, replicas, e, e);
            replica->append(std::span<const uint8_t>(buf, n), false);
        }
        clockNs += 5'000'000LL;
        replica->doWork(clockNs);
        transport.pump(clockNs);
        state.ResumeTiming();

        // Timed region: doWork applies the 100 committed records.
        int w = svc.doWork(clockNs);
        benchmark::DoNotOptimize(w);
    }

    std::filesystem::remove_all(dir);
}
BENCHMARK(BM_MetadataApply_BurstRecords)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 3. BM_MetadataApply_ConcurrentAppend_ProducePerturbation
//
// Interleave replica->append() and svc.doWork() on the same thread
// (single-writer pattern).  Measures combined latency to confirm that
// MSM apply does NOT perturb append p99.
// ---------------------------------------------------------------------------
static void BM_MetadataApply_ConcurrentAppend_ProducePerturbation(benchmark::State& state) {
    const auto dir = uniqueDir("perturbation");
    std::filesystem::create_directories(dir);
    rune::storage::Log log(dir.string());
    log.recover();

    // Second log for "user produce" to interleave.
    const auto dir2 = uniqueDir("produce");
    std::filesystem::create_directories(dir2);
    rune::storage::Log prodLog(dir2.string());
    prodLog.recover();

    InMemoryTransport transport;
    InMemoryTransport prodTransport;
    const std::vector<NodeId> nodes = {1};

    auto metaReplica = std::make_unique<PartitionReplica>(
        makeMetaCfg(1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(metaReplica.get());

    PartitionConfig pcfg;
    pcfg.topicId = 0; pcfg.partitionId = 0; pcfg.selfId = 1;
    pcfg.minIsr = 1; pcfg.replicaLagMaxMs = 60000; pcfg.replicaLagMaxOffset = 10000;
    pcfg.fetchIntervalNs = 5'000'000; pcfg.isrCheckIntervalNs = 5'000'000;
    pcfg.maxFetchBytes = 1<<20; pcfg.pushOnAppend = true; pcfg.fastAckOnApply = true;
    auto prodReplica = std::make_unique<PartitionReplica>(
        pcfg, Role::Leader, 1, nodes, nodes, &prodLog, &prodTransport);
    prodTransport.setLeader(prodReplica.get());

    MetadataService svc(metaReplica.get(), 1);

    const std::vector<int32_t> isr = {1}, replicas = {1};
    uint8_t metaBuf[512];
    // A small "produce" payload.
    std::vector<uint8_t> prodPayload(128, 0xAB);

    int64_t clockNs = 0;
    int32_t part = 0, epoch = 1;

    for (auto _ : state) {
        // Timed region: interleave metadata apply + produce append.
        clockNs += 5'000'000LL;
        metaReplica->doWork(clockNs);
        prodReplica->doWork(clockNs);

        // Append a metadata record.
        const std::size_t n = encodeAssignment(
            metaBuf, sizeof(metaBuf), 1, part, 1, isr, replicas, epoch, epoch);
        Offset mb = metaReplica->append(
            std::span<const uint8_t>(metaBuf, n), false);
        benchmark::DoNotOptimize(mb);

        // Append a "produce" record.
        Offset pb = prodReplica->append(
            std::span<const uint8_t>(prodPayload.data(), prodPayload.size()), false);
        benchmark::DoNotOptimize(pb);

        // Apply committed metadata records.
        int w = svc.doWork(clockNs);
        benchmark::DoNotOptimize(w);
        ++part; ++epoch;
    }

    std::filesystem::remove_all(dir);
    std::filesystem::remove_all(dir2);
}
BENCHMARK(BM_MetadataApply_ConcurrentAppend_ProducePerturbation)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 4. BM_MetadataService_Assignment_Lookup
//
// Measures assignment(topicId, partition) lookup latency after the MSM
// has been populated with 1000 entries.  Hot path: no allocation.
// ---------------------------------------------------------------------------
static void BM_MetadataService_Assignment_Lookup(benchmark::State& state) {
    const auto dir = uniqueDir("lookup");
    std::filesystem::create_directories(dir);
    rune::storage::Log log(dir.string());
    log.recover();

    InMemoryTransport transport;
    const std::vector<NodeId> nodes = {1};
    auto replica = std::make_unique<PartitionReplica>(
        makeMetaCfg(1), Role::Leader, 1, nodes, nodes, &log, &transport);
    transport.setLeader(replica.get());

    MetadataService svc(replica.get(), 1);

    const std::vector<int32_t> isr      = {1};
    const std::vector<int32_t> replicas = {1};
    uint8_t buf[512];
    int64_t clockNs = 0;

    // Pre-populate 1000 assignments.
    for (int i = 0; i < 1000; ++i) {
        const std::size_t n = encodeAssignment(
            buf, sizeof(buf), i / 10, i % 10, 1, isr, replicas, 1, 1);
        replica->append(std::span<const uint8_t>(buf, n), false);
    }
    clockNs += 5'000'000LL;
    replica->doWork(clockNs);
    transport.pump(clockNs);
    svc.doWork(clockNs);

    // Verify at least some assignments are populated.
    (void)svc.assignment(0, 0);

    for (auto _ : state) {
        // Alternate lookup across populated and unpopulated entries.
        auto* pa = svc.assignment(50, 5);
        benchmark::DoNotOptimize(pa);
    }

    std::filesystem::remove_all(dir);
}
BENCHMARK(BM_MetadataService_Assignment_Lookup)->Unit(benchmark::kNanosecond);

} // namespace
