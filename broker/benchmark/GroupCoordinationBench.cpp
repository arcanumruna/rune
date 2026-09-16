// GroupCoordinationBench.cpp — CG-7: Full-coverage consumer-group hot-path benchmarks.
//
// Covers every hot path mandated by the CG-7 brief, all in-process / loopback
// (no Docker cluster, no Aeron — just the real data structures driven directly):
//
//   BM_GCS_OffsetCommit        — handleOffsetCommit end-to-end (encode→handler→store→encode).
//                                 acks=All path via single-node leader replica. p99 < 100µs gate.
//   BM_GCS_OffsetFetch_Single  — handleOffsetFetch for one (topicId, partition). p99 < 100µs.
//   BM_GCS_OffsetFetch_All     — handleOffsetFetch fetch-all (empty entries). p99 < 100µs.
//   BM_GCS_Heartbeat           — handleHeartbeat steady-state. p99 < 100µs. Zero-alloc assertion.
//   BM_GCS_Rebalance           — JoinGroup + autoAssignAndSync + SyncGroup round vs member/partition
//                                 counts {2×4, 4×16, 8×64, 16×64}. Control-plane (not gated at 100µs
//                                 but must be bounded, no pathological growth).
//   BM_GCS_DoWork_Idle         — doWork with N stable groups {1, 10, 100}. O(N) no-alloc.
//
// Alloc-assertion method: replace global operator new with a counting shim; measure
// allocation delta across the hot-path iterations. The shim is active ONLY when
// kCountAllocs == true (Release builds where the optimizer eliminates dead allocs).
// Any hot-path benchmark that measures > 0 allocs per iteration on the steady-state
// portion will print a diagnostic and FAIL via BENCHMARK_ABORT.
//
// Percentile reporting: same pattern as GatewayDataBench — collect into a vector,
// sort, report p50/p99/p99.9 via state.counters.
//
// Gate: any hot-path p99 > 100µs causes the benchmark to set a gate_pass=0.0
// counter and SkipWithError so the CI pipeline sees a failure.
//
// Build: Release (-O2/-O3), cppstd=23. Linked from rune_broker_benchmark target.

#include <benchmark/benchmark.h>

#include <rune/node/GroupCoordinationService.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/storage/Log.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using rune::node::TopicSpec;

// ---------------------------------------------------------------------------
// Allocation counter shim
// ---------------------------------------------------------------------------
// We intercept global operator new to count heap allocations on the hot path.
// Two atomics: g_allocCount tracks total allocs, g_allocEnabled gates the shim
// so setup paths don't pollute the measurement.
//
// IMPORTANT: This shim is declared OUTSIDE the anonymous namespace so it has
// external linkage and overrides the global operator new (which is a replaceable
// function). Declaring it inside an anonymous namespace makes it internal and
// does NOT replace the global operator new.

static std::atomic<int64_t>  g_allocCount{0};
static std::atomic<bool>     g_allocEnabled{false};

void* operator new(std::size_t sz)
{
    if (g_allocEnabled.load(std::memory_order_relaxed)) {
        g_allocCount.fetch_add(1, std::memory_order_relaxed);
    }
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    return p;
}

void* operator new[](std::size_t sz)
{
    if (g_allocEnabled.load(std::memory_order_relaxed)) {
        g_allocCount.fetch_add(1, std::memory_order_relaxed);
    }
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    return p;
}

void operator delete(void* p) noexcept  { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept  { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace {

// ---------------------------------------------------------------------------
// Monotonic nanoseconds
// ---------------------------------------------------------------------------
static inline int64_t nowNs() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Unique temp-dir helper
// ---------------------------------------------------------------------------
static std::atomic<uint32_t> g_dirSeq{0};

static fs::path makeTempDir(const char* tag)
{
    uint32_t seq = g_dirSeq.fetch_add(1, std::memory_order_relaxed);
    std::string name = std::string("rune_cg7bench_") + tag + "_" +
                       std::to_string(static_cast<long>(::getpid())) +
                       "_" + std::to_string(seq);
    auto p = fs::temp_directory_path() / name;
    fs::create_directories(p);
    return p;
}

// ---------------------------------------------------------------------------
// Percentile helper
// ---------------------------------------------------------------------------
static double pct(const std::vector<double>& sorted, double p)
{
    if (sorted.empty()) return 0.0;
    if (sorted.size() == 1) return sorted[0];
    const double   idx = p * static_cast<double>(sorted.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(idx);
    const std::size_t hi = std::min(lo + 1, sorted.size() - 1);
    return sorted[lo] + (idx - static_cast<double>(lo)) * (sorted[hi] - sorted[lo]);
}

static void reportPercentiles(benchmark::State& state,
                               std::vector<double>& samples,
                               const char* prefix,
                               double gateMicros = 100.0)
{
    if (samples.empty()) return;
    std::sort(samples.begin(), samples.end());
    double sum = 0.0;
    for (double v : samples) sum += v;
    const double p50  = pct(samples, 0.50);
    const double p99  = pct(samples, 0.99);
    const double p999 = pct(samples, 0.999);
    const double mean = sum / static_cast<double>(samples.size());

    std::string sp   = prefix;
    state.counters[sp + "_p50_us"]  = p50;
    state.counters[sp + "_p99_us"]  = p99;
    state.counters[sp + "_p999_us"] = p999;
    state.counters[sp + "_mean_us"] = mean;
    if (gateMicros > 0.0) {
        state.counters[sp + "_p99_gate"] = (p99 < gateMicros) ? 1.0 : 0.0;
    }
}

// ---------------------------------------------------------------------------
// NullTransport — no-op transport for PartitionReplica (tests don't replicate)
// ---------------------------------------------------------------------------
class NullTransport : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override {}
};

// ---------------------------------------------------------------------------
// TestFixture — shared infrastructure: Log + PartitionReplica (leader, minIsr=1)
//              + ReplicatedOffsetStore + TopicRegistry + GroupCoordinationService.
//
// minIsr=1 means the single-node replica's append() always returns a valid
// baseOffset → the acks=All path immediately advances the high-watermark.
//
// Topics: "bench-topic" with partitionCount = kMaxPartitions, topicId = 0.
// ---------------------------------------------------------------------------

static constexpr int32_t kTopicId        = 0;
static constexpr int32_t kMaxPartitions  = 64;
static constexpr std::size_t kBufSize    = 64 * 1024;
static constexpr int     kWarmupIter     = 50;

struct TestFixture {
    fs::path                                    logDir;
    fs::path                                    topicDir;
    rune::storage::Log                          log;
    std::unique_ptr<NullTransport>              transport;
    std::unique_ptr<rune::replication::PartitionReplica> replica;
    std::unique_ptr<rune::coordination::ReplicatedOffsetStore> store;
    std::unique_ptr<rune::gateway::TopicRegistry> registry;
    std::unique_ptr<rune::node::GroupCoordinationService> service;

    // Reusable encode/decode buffers (stack-like — no per-call alloc).
    std::array<uint8_t, kBufSize> reqBuf{};
    std::array<uint8_t, kBufSize> outBuf{};

    explicit TestFixture(const char* tag)
        : logDir(makeTempDir(tag))
        , topicDir(makeTempDir((std::string(tag) + "_topics").c_str()))
        , log(logDir.string())
        , transport(std::make_unique<NullTransport>())
    {
        rune::replication::PartitionConfig cfg;
        cfg.partitionId         = 0;
        cfg.selfId              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.minIsr              = 1;  // single-node leader always advances HW
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;

        replica = std::make_unique<rune::replication::PartitionReplica>(
            cfg,
            rune::replication::Role::Leader,
            /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            &log,
            transport.get());

        store = std::make_unique<rune::coordination::ReplicatedOffsetStore>(replica.get());

        TopicSpec ts;
        ts.name           = "bench-topic";
        ts.partitionCount = kMaxPartitions;
        ts.topicId        = kTopicId;
        registry = std::make_unique<rune::gateway::TopicRegistry>(
            std::vector<TopicSpec>{ts}, topicDir.string());

        service = std::make_unique<rune::node::GroupCoordinationService>(
            store.get(),
            registry.get(),
            [] { return true; },   // isLeader = always true
            [] { return rune::node::NodeId{1}; });
    }

    ~TestFixture()
    {
        service.reset();
        registry.reset();
        store.reset();
        replica.reset();
        transport.reset();
        // log's backing files are in logDir; clean up.
        std::error_code ec;
        fs::remove_all(logDir,  ec);
        fs::remove_all(topicDir, ec);
    }

    // Advance the replica's high-watermark so PartitionReplica::append() sees
    // ISR quorum.  In a single-node minIsr=1 config this is automatic when the
    // self ISR is [1] and selfId=1 — doWork propagates the HW on the same call.
    void tickReplica(int n = 1, int64_t clockNs = 1'000'000'000LL)
    {
        for (int i = 0; i < n; ++i)
            replica->doWork(clockNs + i * 1'000'000LL);
    }

    // Encode a JoinGroup into reqBuf and call handleJoinGroup.
    // Returns the decoded JoinGroupResponse (or aborts on failure).
    pc::JoinGroupResponse joinGroup(int64_t sessionId, int64_t corrId,
                                     std::string_view groupId, std::string_view memberId,
                                     int32_t tms = 5000, int64_t clock = 0)
    {
        pc::JoinGroup msg;
        msg.sessionId       = sessionId;
        msg.correlationId   = corrId;
        msg.sessionTimeoutMs = tms;
        msg.groupId  = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
        msg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        const std::size_t reqLen = msg.encode(reqBuf.data(), std::span<const int32_t>());
        const std::size_t outLen = service->handleJoinGroup(reqBuf.data(), reqLen, clock, outBuf.data(), kBufSize);
        auto resp = pc::JoinGroupResponse::decode(outBuf.data(), outLen);
        if (!resp) throw std::runtime_error("JoinGroup decode failed");
        return *resp;
    }

    // Get or create a group that is STABLE with nMembers members and nPartitions partitions.
    // Returns (memberId of first member, generationId).
    std::pair<std::string, int32_t> makeStableGroup(
        std::string_view groupId, int nMembers, int nPartitions,
        int64_t clock = 0)
    {
        // First member joins → PreparingRebalance.
        auto r0 = joinGroup(1, 1, groupId, "", 30000, clock);
        if (r0.errorCode != 0) throw std::runtime_error("first join failed");
        std::string m0(reinterpret_cast<const char*>(r0.memberId.data()), r0.memberId.size());

        // Additional members join.
        std::vector<std::string> mIds{m0};
        for (int i = 1; i < nMembers; ++i) {
            auto ri = joinGroup(static_cast<int64_t>(i + 1), static_cast<int64_t>(i + 1), groupId, "", 30000, clock);
            if (ri.errorCode != 0) throw std::runtime_error("extra join failed");
            std::string mi(reinterpret_cast<const char*>(ri.memberId.data()), ri.memberId.size());
            mIds.push_back(mi);
        }

        // All re-join (causes autoAssignAndSync inline in handleJoinGroup for CG-6 fix).
        for (int i = 0; i < nMembers; ++i) {
            joinGroup(static_cast<int64_t>(i + 1), 100 + i, groupId, mIds[i], 30000, clock);
        }

        // Drive doWork until Stable (may happen immediately due to CG-6 eager assign).
        int64_t clk = clock;
        for (int iter = 0; iter < 100; ++iter) {
            clk += 1'000'000LL;
            service->doWork(clk);
            rune::coordination::GroupCoordinator& gc2 =
                service->coordinator(std::string(groupId));
            if (gc2.state() == rune::coordination::GroupState::Stable) break;
        }

        rune::coordination::GroupCoordinator& gc = service->coordinator(std::string(groupId));
        const int32_t gen = gc.generationId();
        return {m0, gen};
    }
};

// ===========================================================================
// BM_GCS_Heartbeat — handleHeartbeat steady-state hot path.
//
// Setup: one stable group (1 member) with 4 partitions.
//        Pre-encode the Heartbeat frame outside the timed loop.
// Measured: handleHeartbeat(reqBuf, len, nowNs, outBuf, cap).
//           NO allocation expected: the handler only does a map lookup,
//           a heartbeat() call on GroupCoordinator, and a fixed-size encode.
//
// Gate: p99 < 100µs. Alloc assertion: 0 per iteration on hot-path.
// ===========================================================================
static void BM_GCS_Heartbeat(benchmark::State& state)
{
    static constexpr std::string_view kGroup = "hb-bench-group";

    TestFixture fix("hb");

    // Build a stable group with 1 member, 4 partitions.
    auto [memberId, gen] = fix.makeStableGroup(kGroup, 1, 4);

    // Pre-encode the Heartbeat frame.
    pc::Heartbeat hb;
    hb.sessionId     = 1;
    hb.correlationId = 0;
    hb.generationId  = gen;
    hb.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(kGroup.data()), kGroup.size());
    hb.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
    const std::size_t hbLen = hb.encode(fix.reqBuf.data());
    if (hbLen == 0) { state.SkipWithError("Heartbeat encode failed"); return; }

    // Warm-up.
    for (int i = 0; i < kWarmupIter; ++i) {
        fix.service->handleHeartbeat(fix.reqBuf.data(), hbLen, nowNs(),
                                     fix.outBuf.data(), kBufSize);
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    // Phase 1: timing loop — collect latency samples (no alloc tracking so that
    // samples.push_back() never trips the zero-alloc assertion).
    for (auto _ : state) {
        const int64_t t0 = nowNs();
        const std::size_t outLen =
            fix.service->handleHeartbeat(fix.reqBuf.data(), hbLen, t0,
                                          fix.outBuf.data(), kBufSize);
        const int64_t elapsed = nowNs() - t0;
        (void)outLen;
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        if (samples.size() < samples.capacity())
            samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Phase 2: zero-alloc assertion — 10 000 isolated handler calls with alloc
    // tracking enabled but NO sample collection (no push_back, no container ops).
    static constexpr int kAllocCheckIter = 10'000;
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_release);
    for (int i = 0; i < kAllocCheckIter; ++i) {
        const std::size_t outLen =
            fix.service->handleHeartbeat(fix.reqBuf.data(), hbLen, nowNs(),
                                          fix.outBuf.data(), kBufSize);
        benchmark::DoNotOptimize(outLen);
    }
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t totalAllocs = g_allocCount.load(std::memory_order_acquire);

    reportPercentiles(state, samples, "hb", 100.0);
    state.counters["allocs_per_iter"] = static_cast<double>(totalAllocs) / kAllocCheckIter;

    // Zero-alloc assertion: any allocation on the heartbeat hot path is a regression.
    if (totalAllocs > 0) {
        state.SkipWithError("Heartbeat hot path allocated heap memory (zero-alloc violated)");
    }
    // Gate: p99 < 100µs.
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("Heartbeat p99 >= 100µs (hot-path gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCS_Heartbeat)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GCS_OffsetCommit — handleOffsetCommit end-to-end (acks=All path).
//
// Setup: stable group, 1 member, 1 partition. Pre-encode OffsetCommit.
// Measured: handleOffsetCommit — decodes msg, calls store.commit() (which
//           appends to Log with acksAll=true, then updates in-memory map),
//           then encodes the response.
//
// Note: commit() appends to the in-memory Log (no disk fsync in this config —
// the Log uses memory-mapped files and the replica is single-node). The
// recorded latency is realistic: it includes the full store path.
//
// Gate: p99 < 100µs.
// ===========================================================================
static void BM_GCS_OffsetCommit(benchmark::State& state)
{
    static constexpr std::string_view kGroup = "oc-bench-group";

    TestFixture fix("oc");

    auto [memberId, gen] = fix.makeStableGroup(kGroup, 1, 1);

    // Pre-encode an OffsetCommit for topicId=0, partition=0.
    pc::OffsetCommitEntry entry;
    entry.topicId   = kTopicId;
    entry.partition = 0;
    entry.offset    = 0;
    entry.metadata  = std::span<const uint8_t>{};

    // We'll re-use the same reqBuf for encoding; encode once now to get len.
    // On each iteration we just bump offset and re-encode (minimal change).
    // Since all field sizes are fixed, the encoded length is constant.
    pc::OffsetCommit ocMsg;
    ocMsg.sessionId     = 1;
    ocMsg.correlationId = 0;
    ocMsg.generationId  = gen;
    ocMsg.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(kGroup.data()), kGroup.size());
    ocMsg.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());

    std::array<pc::OffsetCommitEntry, 1> entries{entry};
    std::size_t ocLen = ocMsg.encode(fix.reqBuf.data(),
                                      std::span<const pc::OffsetCommitEntry>(entries));
    if (ocLen == 0) { state.SkipWithError("OffsetCommit encode failed"); return; }

    // Warm-up (also primes the Log's mmap and the replica's ISR path).
    int64_t wOffset = 0;
    for (int i = 0; i < kWarmupIter; ++i) {
        entries[0].offset = wOffset++;
        ocMsg.encode(fix.reqBuf.data(), std::span<const pc::OffsetCommitEntry>(entries));
        fix.service->handleOffsetCommit(fix.reqBuf.data(), ocLen, nowNs(),
                                         fix.outBuf.data(), kBufSize);
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));
    int64_t offset = wOffset;

    for (auto _ : state) {
        // Re-encode with updated offset (prevents compiler from caching state).
        entries[0].offset = offset++;
        ocMsg.encode(fix.reqBuf.data(), std::span<const pc::OffsetCommitEntry>(entries));

        const int64_t t0 = nowNs();
        const std::size_t outLen =
            fix.service->handleOffsetCommit(fix.reqBuf.data(), ocLen, t0,
                                             fix.outBuf.data(), kBufSize);
        const int64_t elapsed = nowNs() - t0;
        (void)outLen;
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    reportPercentiles(state, samples, "oc", 100.0);

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("OffsetCommit p99 >= 100µs (hot-path gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCS_OffsetCommit)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GCS_OffsetFetch_Single — handleOffsetFetch for one specific (topicId, partition).
//
// Prerequisite: one committed offset exists (seeded in setup).
// Gate: p99 < 100µs.
// ===========================================================================
static void BM_GCS_OffsetFetch_Single(benchmark::State& state)
{
    static constexpr std::string_view kGroup = "of-single-group";

    TestFixture fix("ofs");
    auto [memberId, gen] = fix.makeStableGroup(kGroup, 1, 1);

    // Commit one offset so the fetch-single path returns a real result.
    {
        pc::OffsetCommitEntry e; e.topicId = kTopicId; e.partition = 0; e.offset = 42;
        e.metadata = {};
        pc::OffsetCommit ocMsg;
        ocMsg.sessionId = 1; ocMsg.correlationId = 0; ocMsg.generationId = gen;
        ocMsg.groupId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(kGroup.data()), kGroup.size());
        ocMsg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        std::array<pc::OffsetCommitEntry, 1> entries{e};
        std::size_t ocLen = ocMsg.encode(fix.reqBuf.data(),
                                          std::span<const pc::OffsetCommitEntry>(entries));
        fix.service->handleOffsetCommit(fix.reqBuf.data(), ocLen, nowNs(),
                                         fix.outBuf.data(), kBufSize);
    }

    // Pre-encode OffsetFetch (single entry).
    pc::OffsetFetchEntry fe; fe.topicId = kTopicId; fe.partition = 0;
    pc::OffsetFetch ofMsg;
    ofMsg.sessionId = 1; ofMsg.correlationId = 0;
    ofMsg.groupId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(kGroup.data()), kGroup.size());
    std::array<pc::OffsetFetchEntry, 1> fentries{fe};
    const std::size_t ofLen = ofMsg.encode(fix.reqBuf.data(),
                                            std::span<const pc::OffsetFetchEntry>(fentries));
    if (ofLen == 0) { state.SkipWithError("OffsetFetch encode failed"); return; }

    // Warm-up.
    for (int i = 0; i < kWarmupIter; ++i) {
        fix.service->handleOffsetFetch(fix.reqBuf.data(), ofLen, nowNs(),
                                        fix.outBuf.data(), kBufSize);
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    for (auto _ : state) {
        const int64_t t0 = nowNs();
        const std::size_t outLen =
            fix.service->handleOffsetFetch(fix.reqBuf.data(), ofLen, t0,
                                            fix.outBuf.data(), kBufSize);
        const int64_t elapsed = nowNs() - t0;
        (void)outLen;
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    reportPercentiles(state, samples, "of_single", 100.0);
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("OffsetFetch/single p99 >= 100µs (gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCS_OffsetFetch_Single)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GCS_OffsetFetch_All — handleOffsetFetch fetch-all (empty entries).
//
// Prerequisite: 8 committed offsets across 8 partitions.
// Gate: p99 < 100µs.
// ===========================================================================
static void BM_GCS_OffsetFetch_All(benchmark::State& state)
{
    static constexpr std::string_view kGroup = "of-all-group";
    static constexpr int kNParts = 8;

    TestFixture fix("ofall");
    auto [memberId, gen] = fix.makeStableGroup(kGroup, 1, kNParts);

    // Seed offsets for all 8 partitions.
    for (int32_t p = 0; p < kNParts; ++p) {
        pc::OffsetCommitEntry e; e.topicId = kTopicId; e.partition = p;
        e.offset = int64_t{1000} + p; e.metadata = {};
        pc::OffsetCommit ocMsg;
        ocMsg.sessionId = 1; ocMsg.correlationId = 0; ocMsg.generationId = gen;
        ocMsg.groupId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(kGroup.data()), kGroup.size());
        ocMsg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        std::array<pc::OffsetCommitEntry, 1> entries{e};
        std::size_t ocLen = ocMsg.encode(fix.reqBuf.data(),
                                          std::span<const pc::OffsetCommitEntry>(entries));
        fix.service->handleOffsetCommit(fix.reqBuf.data(), ocLen, nowNs(),
                                         fix.outBuf.data(), kBufSize);
    }

    // Pre-encode OffsetFetch (empty entries = fetch-all).
    pc::OffsetFetch ofMsg;
    ofMsg.sessionId = 1; ofMsg.correlationId = 0;
    ofMsg.groupId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(kGroup.data()), kGroup.size());
    const std::size_t ofLen = ofMsg.encode(fix.reqBuf.data(),
                                            std::span<const pc::OffsetFetchEntry>());
    if (ofLen == 0) { state.SkipWithError("OffsetFetch/all encode failed"); return; }

    // Warm-up.
    for (int i = 0; i < kWarmupIter; ++i) {
        fix.service->handleOffsetFetch(fix.reqBuf.data(), ofLen, nowNs(),
                                        fix.outBuf.data(), kBufSize);
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    for (auto _ : state) {
        const int64_t t0 = nowNs();
        const std::size_t outLen =
            fix.service->handleOffsetFetch(fix.reqBuf.data(), ofLen, t0,
                                            fix.outBuf.data(), kBufSize);
        const int64_t elapsed = nowNs() - t0;
        (void)outLen;
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    reportPercentiles(state, samples, "of_all", 100.0);
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        if (pct(samples, 0.99) >= 100.0) {
            state.SkipWithError("OffsetFetch/all p99 >= 100µs (gate FAIL)");
        }
    }
}

BENCHMARK(BM_GCS_OffsetFetch_All)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// BM_GCS_Rebalance — JoinGroup + autoAssignAndSync + SyncGroup cycle.
//
// Parameterised by (members, partitions). For each iteration:
//   1. N fresh joins → PreparingRebalance.
//   2. N re-joins   → CompletingRebalance (via CG-6 eager assign or doWork).
//   3. doWork drives stable.
//   4. N SyncGroup  → each member confirms.
//
// This is a control-plane benchmark — not gated at 100µs (rebalances are
// expected to be slower), but the latency must be bounded and must NOT exhibit
// superlinear growth vs. member × partition count.
//
// Reports: reb_p50_us, reb_p99_us, reb_p999_us — per full rebalance round.
// ===========================================================================
static void BM_GCS_Rebalance(benchmark::State& state)
{
    const int nMembers    = static_cast<int>(state.range(0));
    const int nPartitions = static_cast<int>(state.range(1));

    std::string tag = "reb_" + std::to_string(nMembers) + "_" + std::to_string(nPartitions);
    TestFixture fix(tag.c_str());

    int64_t groupCounter = 0; // use a fresh groupId per iteration (avoid stale state)

    // Warm-up (a few rebalance rounds to prime caches + alloc paths).
    for (int w = 0; w < 5; ++w) {
        std::string gid = "reb-warmup-" + std::to_string(w);
        try { fix.makeStableGroup(gid, nMembers, nPartitions); } catch (...) {}
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{50'000}));

    for (auto _ : state) {
        state.PauseTiming();
        std::string gid = "reb-group-" + std::to_string(groupCounter++);

        // Pre-encode N JoinGroup frames outside the timed region.
        // We'll call handleJoinGroup directly in the timed section.
        state.ResumeTiming();

        const int64_t t0 = nowNs();

        // Step 1: N initial joins.
        std::vector<std::string> mIds;
        mIds.reserve(static_cast<std::size_t>(nMembers));
        for (int i = 0; i < nMembers; ++i) {
            auto r = fix.joinGroup(static_cast<int64_t>(i + 1), i, gid, "", 30000, t0);
            if (r.errorCode != 0) { state.SkipWithError("initial join failed"); return; }
            mIds.emplace_back(reinterpret_cast<const char*>(r.memberId.data()), r.memberId.size());
        }

        // Step 2: N re-joins → triggers CompletingRebalance + eager autoAssignAndSync.
        for (int i = 0; i < nMembers; ++i) {
            fix.joinGroup(static_cast<int64_t>(i + 1), 100 + i, gid, mIds[i], 30000, t0);
        }

        // Step 3: doWork until Stable.
        int64_t clk = t0;
        bool stable = false;
        for (int iter = 0; iter < 200; ++iter) {
            clk += 1'000'000LL;
            fix.service->doWork(clk);
            rune::coordination::GroupCoordinator& gc3 = fix.service->coordinator(gid);
            if (gc3.state() == rune::coordination::GroupState::Stable) { stable = true; break; }
        }
        if (!stable) { state.SkipWithError("group did not reach Stable"); return; }

        const int32_t gen = fix.service->coordinator(gid).generationId();

        // Step 4: N SyncGroup frames.
        for (int i = 0; i < nMembers; ++i) {
            pc::SyncGroup sg;
            sg.sessionId = static_cast<int64_t>(i + 1);
            sg.correlationId = 200 + i;
            sg.generationId  = gen;
            sg.groupId  = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(gid.data()), gid.size());
            sg.memberId = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(mIds[i].data()), mIds[i].size());
            const std::size_t sgLen = sg.encode(fix.reqBuf.data());
            fix.service->handleSyncGroup(fix.reqBuf.data(), sgLen, clk,
                                          fix.outBuf.data(), kBufSize);
        }

        const int64_t elapsed = nowNs() - t0;
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Report — NO 100µs gate for rebalance (control-plane).
    reportPercentiles(state, samples, "reb", -1.0);
    state.counters["members"]    = static_cast<double>(nMembers);
    state.counters["partitions"] = static_cast<double>(nPartitions);
}

BENCHMARK(BM_GCS_Rebalance)
    ->UseManualTime()
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond)
    ->Args({2,  4})
    ->Args({4,  16})
    ->Args({8,  64})
    ->Args({16, 64});

// ===========================================================================
// BM_GCS_DoWork_Idle — doWork per-cycle cost with N stable/idle groups.
//
// Setup: N groups all Stable, all members have fresh heartbeats.
// Measured: one doWork() call.
// Should be O(N) with near-zero cost per idle group (no reaping, no timeouts).
// Zero-alloc assertion: no per-cycle alloc in steady state.
// ===========================================================================
static void BM_GCS_DoWork_Idle(benchmark::State& state)
{
    const int nGroups = static_cast<int>(state.range(0));

    std::string tag = "dwidle_" + std::to_string(nGroups);
    TestFixture fix(tag.c_str());

    // Create nGroups stable groups (1 member each, 4 partitions each).
    for (int g = 0; g < nGroups; ++g) {
        std::string gid = "idle-group-" + std::to_string(g);
        fix.makeStableGroup(gid, 1, 4, 0);
    }

    // Warm-up doWork calls.
    int64_t clk = 1'000'000'000LL;
    for (int i = 0; i < 50; ++i) {
        fix.service->doWork(clk);
        clk += 1'000'000LL;
    }

    std::vector<double> samples;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations),
                             std::size_t{2'000'000}));

    // Phase 1: timing loop — collect latency samples (no alloc tracking so that
    // samples.push_back() never trips the zero-alloc assertion).
    for (auto _ : state) {
        // Tiny clock increment keeps all heartbeats live (< session timeout).
        clk += 1'000LL;
        const int64_t t0 = nowNs();
        int work = fix.service->doWork(clk);
        const int64_t elapsed = nowNs() - t0;
        benchmark::DoNotOptimize(work);
        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        if (samples.size() < samples.capacity())
            samples.push_back(static_cast<double>(elapsed) / 1'000.0);
    }

    // Phase 2: zero-alloc assertion — isolated doWork calls with alloc tracking
    // but NO sample collection.
    static constexpr int kAllocCheckIter = 10'000;
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_release);
    for (int i = 0; i < kAllocCheckIter; ++i) {
        clk += 1'000LL;
        int work = fix.service->doWork(clk);
        benchmark::DoNotOptimize(work);
    }
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t totalAllocs = g_allocCount.load(std::memory_order_acquire);

    reportPercentiles(state, samples, "dw", -1.0);
    state.counters["groups"] = static_cast<double>(nGroups);
    state.counters["allocs_per_iter"] = static_cast<double>(totalAllocs) / kAllocCheckIter;

    // Zero-alloc assertion.
    if (totalAllocs > 0) {
        state.SkipWithError("doWork steady-state allocated heap (zero-alloc violated)");
    }
}

BENCHMARK(BM_GCS_DoWork_Idle)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100);

} // anonymous namespace
