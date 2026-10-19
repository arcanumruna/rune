// ReplicationBenchmark.cpp — P3-7: Replication latency benchmarks.
//
// Benchmarks (latency-focused, no hot-path alloc in timed region):
//
//   BM_ProduceMajorityAck_*   — THE p99<100µs target: leader append(batch)
//                               + drive 2 followers to fetch + HW advances.
//                               Measured end-to-end from append to HW ≥ base.
//                               Reported with benchmark_repetitions → percentile
//                               counters p50Us / p99Us (manual histogram via
//                               PauseTiming / ResumeTimig approach matching
//                               the consensus bench pattern).
//
//   BM_HwRecompute_Isr*       — ReplicaLeaderState::updateFollowerFetchOffset
//                               (which triggers recomputeHw) with ISR=3/5/7.
//
//   BM_FetchRequest_Encode    — FetchRequestMsg::encode() ns.
//   BM_FetchRequest_Decode    — FetchRequestMsg::decode() ns.
//   BM_FetchResponse_Encode   — FetchResponseMsg::encode() with 1 KiB payload ns.
//   BM_FetchResponse_Decode   — FetchResponseMsg::decode() ns.
//
//   BM_LeaderAppend_*         — leader append throughput varying batch sizes.
//   BM_FollowerApplyReplicated — follower applyReplicated throughput.
//
//   BM_IsrMaybeShrink_*       — IsrManager::maybeShrinkIsr per-call ns (ISR 3/5/7).
//   BM_IsrMaybeExpand_*       — IsrManager::maybeExpandIsr per-call ns.
//
//   BM_RecorderAppend         — PartitionRecorder::onCommitted() per-batch ns.
//   BM_ReplayIntoLog          — PartitionReplay::replayIntoLog() per-batch ns.
//
// DCE prevention: volatile encode buffers + DoNotOptimize on result values;
// varying offsets (++offset) prevent constant-folding. SetItemsProcessed
// on every benchmark.

#include <benchmark/benchmark.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/HighWatermark.h>
#include <rune/replication/IsrManager.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/replication/PartitionRecorder.h>
#include <rune/replication/PartitionReplay.h>
#include <rune/replication/FileRecordingStore.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;
using namespace rune::replication;

// ---------------------------------------------------------------------------
// Volatile encode buffer (prevents DCE of store sequences)
// ---------------------------------------------------------------------------
static volatile uint8_t g_encodeBuf[4 * 1024]; // 4 KiB — covers FetchResponse + payload

static uint8_t* encodeBuf() {
    return const_cast<uint8_t*>(reinterpret_cast<volatile uint8_t*>(g_encodeBuf));
}

// ---------------------------------------------------------------------------
// Temp path helpers
// ---------------------------------------------------------------------------
static int s_benchPathCounter = 0;

static std::string makeTempPath(const char* tag, const char* ext) {
    return (fs::temp_directory_path() /
            (std::string("rune_rbench_") + tag + "_" +
             std::to_string(++s_benchPathCounter) + ext)).string();
}

static std::string makeTempDir(const char* tag) {
    std::string p = (fs::temp_directory_path() /
            (std::string("rune_rbench_") + tag + "_" +
             std::to_string(++s_benchPathCounter))).string();
    fs::create_directories(p);
    return p;
}

// ---------------------------------------------------------------------------
// PartitionConfig builder for benchmarks
// ---------------------------------------------------------------------------
static PartitionConfig makeConfig(NodeId selfId,
                                   int64_t fetchIntervalNs   = 0LL, // 0 = always emit
                                   int64_t isrCheckIntervalNs = 0LL)
{
    PartitionConfig cfg;
    cfg.partitionId          = 0;
    cfg.selfId               = selfId;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.minIsr               = 2; // majority in 3-ISR
    cfg.fetchIntervalNs      = fetchIntervalNs;
    cfg.isrCheckIntervalNs   = isrCheckIntervalNs;
    cfg.maxFetchBytes        = 1 << 20; // 1 MiB
    return cfg;
}

// ---------------------------------------------------------------------------
// Pump helper: drive one full produce→majority-ack round without any wall-
// clock sleeping.  Uses the injected clock incremented by 1ns between calls.
//
//   1. Leader's doWork() (noop in this context since we call directly)
//   2. Each follower's doWork() — emits FetchRequest via transport
//   3. transport.pump(clock) — delivers FetchRequests to leader
//                              → leader calls onFetchRequest → recomputes HW
//   4. transport.pump(clock) again — delivers FetchResponses to followers
//
// Returns true when leader HW >= targetHw.
// Bounded to maxSteps to avoid an infinite loop on implementation bugs.
// ---------------------------------------------------------------------------
static bool driveToHw(PartitionReplica& leader,
                       std::vector<PartitionReplica*>& followers,
                       InMemoryTransport& transport,
                       Offset targetHw,
                       int64_t& clockNs,
                       int maxSteps = 40)
{
    for (int step = 0; step < maxSteps; ++step) {
        clockNs += 1;
        for (auto* f : followers) f->doWork(clockNs);
        transport.pump(clockNs);
        transport.pump(clockNs);
        if (leader.highWatermark() >= targetHw) return true;
    }
    return false;
}

// ===========================================================================
// 1. Produce → majority-ack latency (THE headline benchmark)
//
// Cluster: 1 leader + 2 followers (3-ISR, majority=2 including leader)
//   → a single follower fetch is sufficient to advance HW.
//
// Pre-built: Log objects in temp dirs, replica objects constructed, ISR set.
// Timed region: one leader.append() + drive to HW covering that record.
//
// We collect per-iteration wall-clock samples and report p50/p99 at end
// via state.counters (mirroring the consensus bench pattern).
// ===========================================================================
struct ProduceMajorityAckFixture {
    static constexpr NodeId kLeaderId  = 0;
    static constexpr NodeId kFollId1   = 1;
    static constexpr NodeId kFollId2   = 2;

    std::string leaderDir, foll1Dir, foll2Dir;
    rune::storage::Log leaderLog, foll1Log, foll2Log;
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> leader, foll1, foll2;
    std::vector<PartitionReplica*> followers;
    int64_t clockNs = 1'000'000'000LL;
    std::vector<uint8_t> payload;

    explicit ProduceMajorityAckFixture(std::size_t payloadSize = 1024)
        : leaderDir(makeTempDir("pma_lead"))
        , foll1Dir(makeTempDir("pma_f1"))
        , foll2Dir(makeTempDir("pma_f2"))
        , leaderLog(leaderDir)
        , foll1Log(foll1Dir)
        , foll2Log(foll2Dir)
        , payload(payloadSize, 0xAB)
    {
        std::vector<NodeId> replicas = {kLeaderId, kFollId1, kFollId2};
        std::vector<NodeId> isr      = {kLeaderId, kFollId1, kFollId2};

        // fetchIntervalNs = 0 so doWork() always emits a FetchRequest.
        leader = std::make_unique<PartitionReplica>(
            makeConfig(kLeaderId, 0, 0),
            Role::Leader, /*epoch=*/1,
            replicas, isr, &leaderLog, &transport);

        foll1 = std::make_unique<PartitionReplica>(
            makeConfig(kFollId1, 0, 0),
            Role::Follower, /*epoch=*/1,
            replicas, isr, &foll1Log, &transport);

        foll2 = std::make_unique<PartitionReplica>(
            makeConfig(kFollId2, 0, 0),
            Role::Follower, /*epoch=*/1,
            replicas, isr, &foll2Log, &transport);

        transport.setLeader(leader.get());
        transport.addFollower(kFollId1, foll1.get());
        transport.addFollower(kFollId2, foll2.get());
        followers = {foll1.get(), foll2.get()};
    }

    ~ProduceMajorityAckFixture() {
        std::error_code ec;
        fs::remove_all(leaderDir, ec);
        fs::remove_all(foll1Dir, ec);
        fs::remove_all(foll2Dir, ec);
    }
};

static void BM_ProduceMajorityAck_1KiB(benchmark::State& state) {
    ProduceMajorityAckFixture fix(1024);

    // Per-iteration latency samples (ns) for p50/p99 reporting.
    std::vector<int64_t> samples;
    samples.reserve(8192);

    for (auto _ : state) {
        // --- Timed: append + drive to HW ---
        auto t0 = std::chrono::steady_clock::now();

        Offset base = fix.leader->append(fix.payload, /*acksAll=*/true);
        benchmark::DoNotOptimize(base);

        bool ok = driveToHw(*fix.leader, fix.followers, fix.transport,
                             fix.leader->logEndOffset(), fix.clockNs);
        benchmark::ClobberMemory();
        benchmark::DoNotOptimize(ok);

        auto t1 = std::chrono::steady_clock::now();
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }

    state.SetItemsProcessed(state.iterations());

    // Report p50/p99 percentiles (µs).
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        int64_t p50 = samples[samples.size() / 2];
        int64_t p99 = samples[static_cast<std::size_t>(samples.size() * 99 / 100)];
        state.counters["p50Us"] = benchmark::Counter(static_cast<double>(p50) / 1000.0);
        state.counters["p99Us"] = benchmark::Counter(static_cast<double>(p99) / 1000.0);
    }
}
BENCHMARK(BM_ProduceMajorityAck_1KiB)
    ->Unit(benchmark::kNanosecond)
    ->MinTime(0.05);

static void BM_ProduceMajorityAck_256B(benchmark::State& state) {
    ProduceMajorityAckFixture fix(256);
    std::vector<int64_t> samples;
    samples.reserve(8192);

    for (auto _ : state) {
        auto t0 = std::chrono::steady_clock::now();

        Offset base = fix.leader->append(fix.payload, /*acksAll=*/true);
        benchmark::DoNotOptimize(base);

        bool ok = driveToHw(*fix.leader, fix.followers, fix.transport,
                             fix.leader->logEndOffset(), fix.clockNs);
        benchmark::ClobberMemory();
        benchmark::DoNotOptimize(ok);

        auto t1 = std::chrono::steady_clock::now();
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }

    state.SetItemsProcessed(state.iterations());

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        int64_t p50 = samples[samples.size() / 2];
        int64_t p99 = samples[static_cast<std::size_t>(samples.size() * 99 / 100)];
        state.counters["p50Us"] = benchmark::Counter(static_cast<double>(p50) / 1000.0);
        state.counters["p99Us"] = benchmark::Counter(static_cast<double>(p99) / 1000.0);
    }
}
BENCHMARK(BM_ProduceMajorityAck_256B)
    ->Unit(benchmark::kNanosecond)
    ->MinTime(0.05);

// ===========================================================================
// 2. HW recompute — ReplicaLeaderState::updateFollowerFetchOffset
//    (triggers internal recomputeHw) with ISR sizes 3/5/7.
// ===========================================================================

static void BM_HwRecompute_Isr(benchmark::State& state) {
    const int isrSize = static_cast<int>(state.range(0));

    ReplicaLeaderState leaderState;
    leaderState.setLeaderId(0);

    std::vector<NodeId> isr;
    for (int i = 0; i < isrSize; ++i) isr.push_back(static_cast<NodeId>(i));
    leaderState.setIsr(isr);
    leaderState.setLeaderLogEndOffset(10'000);

    // Pre-seed follower LEOs.
    for (int i = 1; i < isrSize; ++i) {
        leaderState.updateFollowerFetchOffset(static_cast<NodeId>(i), 9'000 + i * 100);
    }

    Offset fetchOff = 9'500;

    for (auto _ : state) {
        benchmark::DoNotOptimize(fetchOff);
        leaderState.updateFollowerFetchOffset(1, fetchOff);
        Offset hw = leaderState.highWatermark();
        benchmark::DoNotOptimize(hw);
        ++fetchOff;
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["isrSize"] = benchmark::Counter(static_cast<double>(isrSize));
}
BENCHMARK(BM_HwRecompute_Isr)
    ->Arg(3)->Arg(5)->Arg(7)
    ->Unit(benchmark::kNanosecond);

// ===========================================================================
// 3. FetchRequest / FetchResponse SBE encode + decode
// ===========================================================================

static void BM_FetchRequest_Encode(benchmark::State& state) {
    Offset fetchOff = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(fetchOff);
        std::size_t n = FetchRequestMsg::encode(
            encodeBuf(), 0,
            /*leaderEpoch=*/1,
            /*partition=*/0,
            /*followerId=*/1,
            fetchOff,
            /*maxBytes=*/65536);
        benchmark::DoNotOptimize(n);
        ++fetchOff;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FetchRequest_Encode)->Unit(benchmark::kNanosecond);

static void BM_FetchRequest_Decode(benchmark::State& state) {
    std::array<uint8_t, FetchRequestMsg::TOTAL_SIZE> buf{};
    FetchRequestMsg::encode(buf.data(), 0, 1, 0, 1, 4096, 65536);

    for (auto _ : state) {
        auto m = FetchRequestMsg::decode(buf.data(), 0, buf.size());
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FetchRequest_Decode)->Unit(benchmark::kNanosecond);

static void BM_FetchResponse_Encode_1KiB(benchmark::State& state) {
    // 1 KiB payload pre-built outside the timed region.
    static const std::vector<uint8_t> kPayload(1024, 0xBB);
    std::span<const uint8_t> payload(kPayload);

    Offset hw = 4096;
    for (auto _ : state) {
        benchmark::DoNotOptimize(hw);
        std::size_t n = FetchResponseMsg::encode(
            encodeBuf(), 0,
            /*leaderEpoch=*/1,
            /*partition=*/0,
            hw,
            /*logStartOffset=*/0,
            /*errorCode=*/0,
            payload);
        benchmark::DoNotOptimize(n);
        hw += 1024;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FetchResponse_Encode_1KiB)->Unit(benchmark::kNanosecond);

static void BM_FetchResponse_Decode_1KiB(benchmark::State& state) {
    static const std::vector<uint8_t> kPayload(1024, 0xBB);
    // Size the buffer from the codec, NOT a hardcoded overhead: the SBE
    // FetchResponse block grew past the old `8 + 30 + 4` assumption (e.g. the
    // topicId field), so encoding into that fixed array overflowed it and
    // aborted via the stack canary in Release. encodedSize() is authoritative.
    std::vector<uint8_t> buf(FetchResponseMsg::encodedSize(kPayload.size()));
    FetchResponseMsg::encode(buf.data(), 0, 1, 0, 4096, 0, 0,
                             std::span<const uint8_t>(kPayload));

    for (auto _ : state) {
        auto m = FetchResponseMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FetchResponse_Decode_1KiB)->Unit(benchmark::kNanosecond);

// ===========================================================================
// 4. Leader append throughput — varying batch sizes
// ===========================================================================

static void BM_LeaderAppend(benchmark::State& state) {
    const std::size_t batchSize = static_cast<std::size_t>(state.range(0));

    std::string dir = makeTempDir("lead_app");
    rune::storage::Log log{dir};

    // Single-leader with ISR=self only (no followers needed for throughput).
    std::vector<NodeId> replicas = {0};
    std::vector<NodeId> isr      = {0};
    InMemoryTransport transport;
    PartitionReplica leader{
        makeConfig(0, 0, 0),
        Role::Leader, /*epoch=*/1,
        replicas, isr, &log, &transport};

    std::vector<uint8_t> payload(batchSize, 0xCC);
    int64_t totalItems = 0;

    for (auto _ : state) {
        Offset base = leader.append(payload, /*acksAll=*/false);
        benchmark::DoNotOptimize(base);
        ++totalItems;
    }

    state.SetItemsProcessed(totalItems);
    state.SetBytesProcessed(static_cast<int64_t>(totalItems * batchSize));
    state.counters["batchBytes"] = benchmark::Counter(static_cast<double>(batchSize));

    std::error_code ec;
    fs::remove_all(dir, ec);
}
BENCHMARK(BM_LeaderAppend)
    ->Arg(256)->Arg(1024)->Arg(4096)->Arg(65536)
    ->Unit(benchmark::kNanosecond);

// ===========================================================================
// 5. Follower applyReplicated throughput
// ===========================================================================

static void BM_FollowerApplyReplicated(benchmark::State& state) {
    const std::size_t batchSize = static_cast<std::size_t>(state.range(0));

    std::string follDir = makeTempDir("foll_apply");
    rune::storage::Log follLog{follDir};

    std::vector<NodeId> replicas = {0, 1};
    std::vector<NodeId> isr      = {0, 1};
    InMemoryTransport transport;
    PartitionReplica follower{
        makeConfig(1, 0, 0),
        Role::Follower, /*epoch=*/1,
        replicas, isr, &follLog, &transport};

    // Pre-build a FetchResponse with `batchSize` bytes of records.
    std::vector<uint8_t> payload(batchSize, 0xDD);
    // Codec-authoritative size (the old `8 + 30 + 4` overhead is stale and
    // overflowed this heap buffer once the SBE FetchResponse block grew).
    std::vector<uint8_t> responseBuf(FetchResponseMsg::encodedSize(batchSize));

    Offset hw = 0;
    for (auto _ : state) {
        // Build a FetchResponse in the pre-allocated buffer.
        FetchResponseMsg::encode(
            responseBuf.data(), 0,
            /*leaderEpoch=*/1, /*partition=*/0,
            hw + static_cast<Offset>(batchSize), /*logStartOffset=*/0, 0,
            std::span<const uint8_t>(payload));

        auto resp = FetchResponseMsg::decode(responseBuf.data(), 0);
        resp.highWatermark = hw + static_cast<Offset>(batchSize);

        follower.applyReplicated(resp);
        benchmark::ClobberMemory();

        hw += static_cast<Offset>(batchSize);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * batchSize));
    state.counters["batchBytes"] = benchmark::Counter(static_cast<double>(batchSize));

    std::error_code ec;
    fs::remove_all(follDir, ec);
}
BENCHMARK(BM_FollowerApplyReplicated)
    ->Arg(256)->Arg(1024)->Arg(4096)
    ->Unit(benchmark::kNanosecond);

// ===========================================================================
// 6. IsrManager — maybeShrinkIsr / maybeExpandIsr per-call ns
// ===========================================================================

static void BM_IsrMaybeShrink(benchmark::State& state) {
    const int isrSize = static_cast<int>(state.range(0));

    std::vector<NodeId> replicas;
    for (int i = 0; i < isrSize; ++i) replicas.push_back(i);

    IsrManager isr{replicas, /*leaderId=*/0};

    // Seed fresh follower fetch times so they appear in-sync (no shrink).
    int64_t nowNs = 1'000'000'000LL;
    for (int i = 1; i < isrSize; ++i) {
        isr.onFollowerFetch(static_cast<NodeId>(i), 9'000, nowNs);
    }

    Offset leaderLeo = 9'100;

    for (auto _ : state) {
        benchmark::DoNotOptimize(leaderLeo);
        auto newIsr = isr.maybeShrinkIsr(nowNs, leaderLeo, 10'000, 10'000);
        benchmark::DoNotOptimize(newIsr);
        ++leaderLeo;
        nowNs += 1;
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["isrSize"] = benchmark::Counter(static_cast<double>(isrSize));
}
BENCHMARK(BM_IsrMaybeShrink)
    ->Arg(3)->Arg(5)->Arg(7)
    ->Unit(benchmark::kNanosecond);

static void BM_IsrMaybeExpand(benchmark::State& state) {
    const int isrSize = static_cast<int>(state.range(0));

    std::vector<NodeId> replicas;
    for (int i = 0; i < isrSize; ++i) replicas.push_back(i);

    IsrManager isr{replicas, /*leaderId=*/0};

    // Shrink followers out of ISR first (simulate lagging followers).
    {
        int64_t nowNs = 0;
        Offset  leo   = 100'000;
        isr.maybeShrinkIsr(nowNs, leo, /*lagMaxMs=*/0, /*lagMaxOff=*/0);
    }

    // Seed follower fetch offsets to be caught-up.
    int64_t nowNs       = 1'000'000'000LL;
    Offset  hw          = 9'000;
    Offset  fetchOffset = 9'000; // = HW → triggers expand

    for (int i = 1; i < isrSize; ++i) {
        isr.onFollowerFetch(static_cast<NodeId>(i), fetchOffset, nowNs);
    }

    for (auto _ : state) {
        benchmark::DoNotOptimize(hw);
        auto newIsr = isr.maybeExpandIsr(nowNs, hw);
        benchmark::DoNotOptimize(newIsr);
        ++hw;
        nowNs += 1;
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["isrSize"] = benchmark::Counter(static_cast<double>(isrSize));
}
BENCHMARK(BM_IsrMaybeExpand)
    ->Arg(3)->Arg(5)->Arg(7)
    ->Unit(benchmark::kNanosecond);

// ===========================================================================
// 7. PartitionRecorder append + PartitionReplay into a Log
// ===========================================================================

// Build a properly-framed RecordBatch at `baseOffset` with `valueSize` payload.
// Each batch has one record so lastOffset = baseOffset.
static std::vector<uint8_t> makeBenchBatch(int64_t baseOffset, std::size_t valueSize) {
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp=*/0);
    std::string key("k");
    std::string val(valueSize, static_cast<char>(0xEE));
    b.append(key, val, /*timestampDelta=*/0);
    return b.build();
}

static void BM_RecorderAppend(benchmark::State& state) {
    const std::size_t valueSize = static_cast<std::size_t>(state.range(0));

    // Leader log pre-populated with kPreloadBatches properly-framed RecordBatches.
    std::string logDir    = makeTempDir("rec_log");
    std::string storePath = makeTempPath("rec_store", ".bin");
    rune::storage::Log log{logDir};

    static constexpr int kPreloadBatches = 2000;

    // Pre-build one batch to get the encoded size for offset tracking.
    auto proto = makeBenchBatch(0, valueSize);

    // Each batch has baseOffset = i, lastOffset = i (single record per batch).
    for (int i = 0; i < kPreloadBatches; ++i) {
        auto batch = makeBenchBatch(static_cast<int64_t>(i), valueSize);
        log.append(batch, static_cast<int64_t>(i)); // lastOffset == baseOffset (1 record)
    }

    FileRecordingStore store{storePath};
    PartitionRecorder recorder{0, &log, &store};
    recorder.startRecording(0);

    // Commit one batch per timed iteration.
    int64_t nextOff = 0;
    (void)proto;

    for (auto _ : state) {
        if (nextOff >= static_cast<int64_t>(kPreloadBatches)) {
            break; // exhausted pre-loaded log; stop early
        }
        // onCommitted(startOffset, endOffset) — endOffset = nextOff + 1 (exclusive)
        recorder.onCommitted(nextOff, nextOff + 1);
        benchmark::ClobberMemory();
        ++nextOff;
    }

    state.SetItemsProcessed(nextOff);
    state.SetBytesProcessed(nextOff * static_cast<int64_t>(valueSize));
    state.counters["valueBytes"] = benchmark::Counter(static_cast<double>(valueSize));

    recorder.stopRecording();
    std::error_code ec;
    fs::remove_all(logDir, ec);
    fs::remove(storePath, ec);
}
BENCHMARK(BM_RecorderAppend)
    ->Arg(256)->Arg(1024)->Arg(4096)
    ->Unit(benchmark::kNanosecond);

static void BM_ReplayIntoLog(benchmark::State& state) {
    const std::size_t valueSize = static_cast<std::size_t>(state.range(0));

    // Build a FileRecordingStore pre-populated with properly-framed RecordBatches.
    static constexpr int kPreloadBatches = 200;
    std::string storePath = makeTempPath("rpl_store", ".bin");
    FileRecordingStore store{storePath};

    for (int i = 0; i < kPreloadBatches; ++i) {
        auto batch = makeBenchBatch(static_cast<int64_t>(i), valueSize);
        store.append(static_cast<int64_t>(i),
                     std::span<const uint8_t>(batch.data(), batch.size()));
    }

    PartitionReplay replayer{&store};
    int64_t totalBatches = 0;

    for (auto _ : state) {
        // Replay all kPreloadBatches into a fresh log each iteration.
        state.PauseTiming();
        std::string logDir = makeTempDir("rpl_log");
        rune::storage::Log log{logDir};
        state.ResumeTiming();

        int64_t n = replayer.replayIntoLog(log, 0, static_cast<int64_t>(kPreloadBatches));
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
        totalBatches += n;

        state.PauseTiming();
        std::error_code ec;
        fs::remove_all(logDir, ec);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(totalBatches > 0 ? totalBatches : state.iterations());
    state.counters["valueBytes"] = benchmark::Counter(static_cast<double>(valueSize));

    std::error_code ec;
    fs::remove(storePath, ec);
}
BENCHMARK(BM_ReplayIntoLog)
    ->Arg(256)->Arg(1024)->Arg(4096)
    ->Unit(benchmark::kNanosecond);

} // namespace
