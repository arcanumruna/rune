// TransactionBenchmark.cpp — P5-6: Phase-5 transaction benchmarks.
//
// Covers (all latency-focused, no hot-path alloc in the timed region):
//
//   1. BM_Dedup_ValidateUpdate           — ProducerStateManager::validate+update per
//                                          batch (the idempotent-produce hot path; ~ns).
//   2. BM_TxnBeginCommit_p50p99          — full initProducerId→addPartitions→
//                                          endTransaction(commit)→completeTransaction
//                                          cycle; latency per cycle (p50/p99 via
//                                          --benchmark_repetitions=20 in the driver).
//   3. BM_ControlMarker_Build            — ControlMarker::build() per control batch.
//   4. BM_ControlMarker_IsControlExtract — isControlBatch+extract per op.
//   5. BM_ReadCommitted_vs_Uncommitted   — ReadCommittedView::read() under both
//                                          isolation levels on a log with data +
//                                          markers + some aborted txns; measures
//                                          the filtering delta.
//   6. BM_InitProducerId_EpochBump       — initProducerId on an already-known
//                                          transactionalId (epoch bump / zombie-fence
//                                          cost) vs first allocation.
//   7. BM_AddOffsetsToTxn               — addOffsetsToTxn cost per call.
//   8. BM_Sbe_InitProducerIdReq_Encode   — SBE encode per-message ns.
//      BM_Sbe_InitProducerIdReq_Decode   — SBE decode per-message ns.
//   9. BM_Sbe_EndTxnRequest_Encode/Decode — EndTxn SBE encode+decode ns.
//
// Coding conventions (mirrors CoordinationBenchmark.cpp):
//   - Pre-build all inputs outside the timed region (PauseTiming / setup before loop).
//   - DoNotOptimize results; ClobberMemory after writes.
//   - Short min_time enforced via the CLI flag; bodies do no sleeping.
//   - Unique temp dirs: getpid() + atomic counter (no collision across params).

#include <benchmark/benchmark.h>

#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/TransactionMessages.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>
#include <sys/types.h>
#include <unistd.h>

namespace {

// ---------------------------------------------------------------------------
// Unique temp dir helper.
// ---------------------------------------------------------------------------
static std::atomic<int> g_dirSeq{0};

static std::filesystem::path uniqueDir(const char* tag) {
    namespace fs = std::filesystem;
    return fs::temp_directory_path() /
           ("rune_bench_txn_" + std::string(tag) +
            "_" + std::to_string(::getpid()) +
            "_" + std::to_string(g_dirSeq.fetch_add(1)));
}

// ---------------------------------------------------------------------------
// 1. BM_Dedup_ValidateUpdate
//
// ProducerStateManager::validate + update per batch on the produce hot path.
// Single producer, contiguous sequences.  Should be low single-digit ns.
//
// This is the HEADLINE hot-path number for idempotent dedup overhead per batch.
// ---------------------------------------------------------------------------
static void BM_Dedup_ValidateUpdate(benchmark::State& state) {
    static constexpr int32_t kWrapAt     = 1 << 20;
    static constexpr int64_t kProducerId = 42;
    static constexpr int16_t kEpoch      = 0;
    static constexpr int32_t kPartition  = 0;

    rune::storage::ProducerStateManager psm;
    int32_t seq      = 0;
    int64_t logOffset = 0;

    for (auto _ : state) {
        if (seq >= kWrapAt) {
            state.PauseTiming();
            psm = rune::storage::ProducerStateManager{};
            seq = 0;
            logOffset = 0;
            state.ResumeTiming();
        }

        auto result = psm.validate(/*topicId=*/0, kProducerId, kPartition, kEpoch, seq, seq);
        benchmark::DoNotOptimize(result);
        if (result.isAccept()) {
            psm.update(/*topicId=*/0, kProducerId, kPartition, kEpoch, seq, seq, logOffset);
            ++logOffset;
        }
        ++seq;
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Dedup_ValidateUpdate)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 2. BM_TxnBeginCommit
//
// Full transactional cycle (in-memory, no durable log — no fsync):
//   initProducerId(txnId) → addPartitions → endTransaction(commit) → completeTransaction
//
// Measures pure coordinator state-machine cost (memory only).
// Note: The durable path (TransactionLog on disk) adds fsync cost per transition;
// this benchmark isolates the state-machine overhead.
//
// Use --benchmark_repetitions=20 to collect a p50/p99 distribution.
// ---------------------------------------------------------------------------
static void BM_TxnBeginCommit(benchmark::State& state) {
    using namespace rune::transaction;

    static constexpr int64_t kNow = 1'000'000'000LL;
    static constexpr int32_t kTxnTimeoutMs = 60'000;

    // Pre-build the partition list (single partition — minimal allocation).
    std::vector<TopicPartition> partitions{{"bench-topic", 0}};

    // Coordinator with NO durable log (measures pure state-machine cost).
    TransactionCoordinator coord;

    // Pre-register the transactional id (warm path: known txnId → epoch bump
    // benchmark is BM_InitProducerId_EpochBump).
    // We register it once, then let the loop do full cycles.
    auto r0 = coord.initProducerId("bench-txnid", kTxnTimeoutMs, kNow);
    benchmark::DoNotOptimize(r0);

    int64_t nowNs = kNow + 1'000'000LL;

    for (auto _ : state) {
        // Each iteration: epoch-bump initProducerId (known txnId) → addPartitions
        //                 → endTransaction(commit) → completeTransaction.
        auto r1 = coord.initProducerId("bench-txnid", kTxnTimeoutMs, nowNs);
        benchmark::DoNotOptimize(r1);

        if (r1.ok()) {
            auto r2 = coord.addPartitions("bench-txnid",
                                          r1.producerId, r1.producerEpoch,
                                          partitions, nowNs);
            benchmark::DoNotOptimize(r2);

            auto r3 = coord.endTransaction("bench-txnid",
                                           r1.producerId, r1.producerEpoch,
                                           /*commit=*/true, nowNs);
            benchmark::DoNotOptimize(r3);

            auto r4 = coord.completeTransaction("bench-txnid");
            benchmark::DoNotOptimize(r4);
        }

        nowNs += 1'000LL;
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_TxnBeginCommit)
    ->Unit(benchmark::kMicrosecond)
    ->Repetitions(20)
    ->ReportAggregatesOnly(true);

// ---------------------------------------------------------------------------
// 3. BM_ControlMarker_Build
//
// ControlMarker::build() per-call ns.  Builds a 75-byte COMMIT batch in a
// heap-allocated vector each call.  The vector alloc is unavoidable (build()
// returns vector<uint8_t>), but we DoNotOptimize the result so the compiler
// can't elide it.
// ---------------------------------------------------------------------------
static void BM_ControlMarker_Build(benchmark::State& state) {
    using namespace rune::transaction;

    static constexpr int64_t     kProducerId = 1;
    static constexpr int16_t     kEpoch      = 0;
    static constexpr int32_t     kCoordEpoch = 0;
    const bool commit = (state.range(0) == 0); // 0=Commit, 1=Abort

    for (auto _ : state) {
        auto batch = ControlMarker::build(kProducerId, kEpoch,
                                           commit ? ControlType::Commit
                                                  : ControlType::Abort,
                                           kCoordEpoch);
        benchmark::DoNotOptimize(batch.data());
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ControlMarker_Build)
    ->Arg(0) // Commit
    ->Arg(1) // Abort
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 4. BM_ControlMarker_IsControlExtract
//
// isControlBatch + extract on a pre-built batch buffer.
// No allocation in the timed region — operates on a static buffer.
// ---------------------------------------------------------------------------
static void BM_ControlMarker_IsControlExtract(benchmark::State& state) {
    using namespace rune::transaction;

    // Pre-build a COMMIT marker batch outside the timed region.
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);

    for (auto _ : state) {
        bool isCtrl = ControlMarker::isControlBatch(
            batch.data(), 0, batch.size());
        benchmark::DoNotOptimize(isCtrl);

        if (isCtrl) {
            ControlMarkerPayload payload;
            bool ok = ControlMarker::extract(batch.data(), 0, payload, batch.size());
            benchmark::DoNotOptimize(ok);
            benchmark::DoNotOptimize(payload.producerId);
        }
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ControlMarker_IsControlExtract)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 5. BM_ReadCommitted_vs_Uncommitted
//
// ReadCommittedView::read() under read_committed vs read_uncommitted on a
// pre-built log with:
//   - kNumCommitted committed-txn data batches (visible under read_committed)
//   - kNumAborted   aborted-txn data batches   (invisible under read_committed)
//   - interleaved COMMIT / ABORT control markers
//
// The benchmark measures the cost of the isolation-level filter: the delta
// between read_uncommitted (returns all data) and read_committed (LSO + aborted
// filter).
//
// Parameterised: state.range(0) = 0 → ReadUncommitted, 1 → ReadCommitted.
// ---------------------------------------------------------------------------

static void BM_ReadIsolation(benchmark::State& state) {
    namespace fs = std::filesystem;
    using namespace rune::transaction;

    static constexpr int kNumCommitted = 20;
    static constexpr int kNumAborted   = 10;
    static constexpr int64_t kRollBytes = 256 * 1024;

    const IsolationLevel level = (state.range(0) == 0)
                                     ? IsolationLevel::ReadUncommitted
                                     : IsolationLevel::ReadCommitted;

    // Build the log BEFORE the timed loop — no PauseTiming needed inside.
    auto dir = uniqueDir("rcv");
    fs::create_directories(dir);

    // Use a stable log across iterations — build once, read many times.
    rune::storage::Log log(dir.string(), kRollBytes);

    // Committed transactions: pid 1000 + i.
    for (int i = 0; i < kNumCommitted; ++i) {
        int64_t pid = 1000 + i;
        int64_t dataOff = log.logEndOffset();
        {
            rune::protocol::RecordBatchBuilder rb(dataOff, 0);
            rb.setProducerId(pid);
            rb.append({}, "committed-data", 0);
            auto batch = rb.build();
            log.append(batch, dataOff);
        }
        int64_t ctrlOff = log.logEndOffset();
        {
            auto marker = ControlMarker::build(pid, 0, ControlType::Commit, 0);
            // Stamp LE baseOffset (excluded from CRC region).
            uint64_t u = static_cast<uint64_t>(ctrlOff);
            for (int b = 0; b < 8; ++b)
                marker[b] = static_cast<uint8_t>(u >> (b * 8));
            log.append(marker, ctrlOff);
        }
    }

    // Aborted transactions: pid 2000 + i.
    for (int i = 0; i < kNumAborted; ++i) {
        int64_t pid = 2000 + i;
        int64_t dataOff = log.logEndOffset();
        {
            rune::protocol::RecordBatchBuilder rb(dataOff, 0);
            rb.setProducerId(pid);
            rb.append({}, "aborted-data", 0);
            auto batch = rb.build();
            log.append(batch, dataOff);
        }
        int64_t ctrlOff = log.logEndOffset();
        {
            auto marker = ControlMarker::build(pid, 0, ControlType::Abort, 0);
            uint64_t u = static_cast<uint64_t>(ctrlOff);
            for (int b = 0; b < 8; ++b)
                marker[b] = static_cast<uint8_t>(u >> (b * 8));
            log.append(marker, ctrlOff);
        }
    }

    const int64_t hw = log.logEndOffset();

    // Build the LsoTracker + AbortedTxnIndex by scanning the log once.
    LsoTracker tracker;
    AbortedTxnIndex abortedIndex;
    ReadCommittedView view(&tracker, &abortedIndex);
    view.rebuild(&log, hw);

    // Timed loop — pure read, no setup inside.
    for (auto _ : state) {
        auto result = view.read(&log, hw, /*fetchOffset=*/0,
                                /*maxBytes=*/1 << 20, level);
        benchmark::DoNotOptimize(result.size());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["committed_batches"] = benchmark::Counter(
        static_cast<double>(kNumCommitted));
    state.counters["aborted_batches"]   = benchmark::Counter(
        static_cast<double>(kNumAborted));

    // Cleanup after the loop.
    fs::remove_all(dir);
}
BENCHMARK(BM_ReadIsolation)
    ->Arg(0) // ReadUncommitted
    ->Arg(1) // ReadCommitted
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 6. BM_InitProducerId_EpochBump
//
// initProducerId on a known transactionalId (re-init path: epoch bump to fence
// the zombie producer).  Measures the cost of zombie fencing vs first alloc.
//
// state.range(0): 0 = first allocation (unknown txnId), 1 = epoch bump (known).
// ---------------------------------------------------------------------------
static void BM_InitProducerId_EpochBump(benchmark::State& state) {
    using namespace rune::transaction;

    static constexpr int64_t kNow          = 1'000'000'000LL;
    static constexpr int32_t kTxnTimeoutMs = 60'000;
    const bool doEpochBump = (state.range(0) == 1);

    int64_t nowNs = kNow;

    for (auto _ : state) {
        // Each iteration needs a fresh coordinator (unknown txnId for first-alloc;
        // pre-registered + Ongoing-less for epoch-bump).
        state.PauseTiming();
        TransactionCoordinator coord;
        if (doEpochBump) {
            // Pre-register the txnId so the next initProducerId bumps the epoch.
            coord.initProducerId("bench-txnid", kTxnTimeoutMs, nowNs);
        }
        state.ResumeTiming();

        auto r = coord.initProducerId("bench-txnid", kTxnTimeoutMs, nowNs);
        benchmark::DoNotOptimize(r);

        nowNs += 1'000LL;
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InitProducerId_EpochBump)
    ->Arg(0) // first allocation
    ->Arg(1) // epoch bump (re-init, zombie fence)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 7. BM_AddOffsetsToTxn
//
// addOffsetsToTxn cost per call.
// Pre-setup: a coordinator in Ongoing state with 1 partition.
// Timed: addOffsetsToTxn(1 group offset) per iteration.
// ---------------------------------------------------------------------------
static void BM_AddOffsetsToTxn(benchmark::State& state) {
    using namespace rune::transaction;
    using rune::coordination::TopicPartition;

    static constexpr int64_t kNow          = 1'000'000'000LL;
    static constexpr int32_t kTxnTimeoutMs = 60'000;

    const int numOffsets = static_cast<int>(state.range(0));

    // Pre-build offset map outside the loop.
    std::map<rune::coordination::TopicPartition,
             rune::coordination::Offset> offsetMap;
    for (int i = 0; i < numOffsets; ++i) {
        offsetMap[rune::coordination::TopicPartition{"bench-topic",
                                                      static_cast<int32_t>(i)}]
            = static_cast<rune::coordination::Offset>(i * 100);
    }

    int64_t nowNs = kNow;

    for (auto _ : state) {
        // Fresh coordinator + Ongoing setup outside timed region.
        state.PauseTiming();
        TransactionCoordinator coord;
        auto r0 = coord.initProducerId("bench-txnid", kTxnTimeoutMs, nowNs);
        std::vector<rune::transaction::TopicPartition> parts{{"bench-topic", 99}};
        coord.addPartitions("bench-txnid",
                             r0.producerId, r0.producerEpoch,
                             parts, nowNs);
        state.ResumeTiming();

        auto r = coord.addOffsetsToTxn("bench-txnid",
                                        r0.producerId, r0.producerEpoch,
                                        "bench-group",
                                        offsetMap,
                                        nowNs);
        benchmark::DoNotOptimize(r);
        benchmark::ClobberMemory();

        nowNs += 1'000LL;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["offsets"] = benchmark::Counter(static_cast<double>(numOffsets));
}
BENCHMARK(BM_AddOffsetsToTxn)
    ->Arg(1)
    ->Arg(10)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 8. SBE encode + decode — InitProducerIdRequest
// ---------------------------------------------------------------------------
static volatile uint8_t g_txnBuf[512];

static uint8_t* txnBufRW() {
    return const_cast<uint8_t*>(reinterpret_cast<volatile uint8_t*>(g_txnBuf));
}

static void BM_Sbe_InitProducerIdReq_Encode(benchmark::State& state) {
    using namespace rune::transaction;

    int32_t timeout = 60'000;
    for (auto _ : state) {
        benchmark::DoNotOptimize(timeout);
        std::size_t n = InitProducerIdRequestMsg::encode(
            txnBufRW(), 0, timeout, "bench-txnid");
        benchmark::DoNotOptimize(n);
        ++timeout;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_InitProducerIdReq_Encode)->Unit(benchmark::kNanosecond);

static void BM_Sbe_InitProducerIdReq_Decode(benchmark::State& state) {
    using namespace rune::transaction;

    std::array<uint8_t, 64> buf{};
    std::size_t n = InitProducerIdRequestMsg::encode(buf.data(), 0, 60'000, "bench-txnid");

    for (auto _ : state) {
        auto m = InitProducerIdRequestMsg::decode(buf.data(), 0, n);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_InitProducerIdReq_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 9. SBE encode + decode — EndTxnRequest
// ---------------------------------------------------------------------------
static void BM_Sbe_EndTxnRequest_Encode(benchmark::State& state) {
    using namespace rune::transaction;

    int64_t pid = 1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(pid);
        std::size_t n = EndTxnRequestMsg::encode(
            txnBufRW(), 0, pid, /*epoch=*/0, /*commit=*/true, "bench-txnid");
        benchmark::DoNotOptimize(n);
        ++pid;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_EndTxnRequest_Encode)->Unit(benchmark::kNanosecond);

static void BM_Sbe_EndTxnRequest_Decode(benchmark::State& state) {
    using namespace rune::transaction;

    std::array<uint8_t, 64> buf{};
    std::size_t n = EndTxnRequestMsg::encode(buf.data(), 0, 1, 0, true, "bench-txnid");

    for (auto _ : state) {
        auto m = EndTxnRequestMsg::decode(buf.data(), 0, n);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_EndTxnRequest_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 10. BM_Sbe_WriteTxnMarkersReq_Encode/Decode
//     (includes partition array — exercises the variable-length codec path)
// ---------------------------------------------------------------------------
static void BM_Sbe_WriteTxnMarkersReq_Encode(benchmark::State& state) {
    using namespace rune::transaction;

    std::vector<TopicPartition> parts{{"topic-a", 0}, {"topic-b", 1}};
    int64_t pid = 1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(pid);
        std::size_t n = WriteTxnMarkersRequestMsg::encode(
            txnBufRW(), 0, pid, /*epoch=*/0,
            ControlType::Commit, /*coordEpoch=*/0,
            "bench-txnid", parts);
        benchmark::DoNotOptimize(n);
        ++pid;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_WriteTxnMarkersReq_Encode)->Unit(benchmark::kNanosecond);

static void BM_Sbe_WriteTxnMarkersReq_Decode(benchmark::State& state) {
    using namespace rune::transaction;

    std::vector<TopicPartition> parts{{"topic-a", 0}, {"topic-b", 1}};
    std::array<uint8_t, 128> buf{};
    std::size_t n = WriteTxnMarkersRequestMsg::encode(
        buf.data(), 0, 1, 0, ControlType::Commit, 0, "bench-txnid", parts);

    for (auto _ : state) {
        auto m = WriteTxnMarkersRequestMsg::decode(buf.data(), 0, n);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_WriteTxnMarkersReq_Decode)->Unit(benchmark::kNanosecond);

} // namespace
