// InMemoryLog benchmark.
//
// Exercises every public operation of rune::storage::InMemoryLog — the
// single-owner circular ring buffer (zero-disk, zero-alloc steady-state
// append, O(1) evict) used for replication tests and memory-backed partitions.
//
// Ops covered:
//   construct, append / appendNoSync (steady-state, eviction, wrap),
//   append under BackPressure (refused), read (hot / range / out-of-range),
//   truncateTo, logEndOffset / logStartOffset, segmentBaseOffsets,
//   sync (no-op), recover (no-op).
//
// Batch shape matches LogBenchmark / LogSegmentBenchmark: ~48-byte single-record
// MMORPG entity-position event (key="pos", value="x:1234,y:5678,z:9012").

#include <benchmark/benchmark.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

using rune::storage::FullPolicy;
using rune::storage::InMemoryLog;

static std::vector<uint8_t> makePoseBatch(int64_t baseOffset, int64_t ts) {
    rune::protocol::RecordBatchBuilder b(baseOffset, ts);
    b.append("pos", "x:1234,y:5678,z:9012", ts);
    return b.build();
}

// Byte size of one pose batch — used to size rings in terms of "batches".
static std::size_t poseBatchBytes() {
    static const std::size_t sz = makePoseBatch(0, 0).size();
    return sz;
}

// Fill a log with `count` single-record pose batches (offsets 0..count-1).
static void fillLog(InMemoryLog& log, int count, int64_t startOff = 0) {
    for (int i = 0; i < count; ++i) {
        const int64_t off = startOff + i;
        auto batch = makePoseBatch(off, 1700000000000LL + off);
        log.append(batch, off);
    }
}

// ---------------------------------------------------------------------------
// BM_InMemoryLog_Construct
//
// Construction pre-allocates the fixed ring (ring_.resize(maxBytes)) and
// reserves the entry descriptor vector.  Measures that one-time allocation
// cost for a 64 MiB ring holding up to 1M batches.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_Construct(benchmark::State& state) {
    static constexpr std::size_t kMaxBytes   = 64ull * 1024 * 1024;
    static constexpr std::size_t kMaxRecords = 1ull << 20;
    for (auto _ : state) {
        InMemoryLog log(kMaxBytes, kMaxRecords);
        benchmark::DoNotOptimize(&log);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_Construct)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_Append
//
// Ring sized generously (64 MiB / 1M records) so appends almost never evict —
// isolates the pure O(1) contiguous-memcpy append cost.  Batch construction is
// PauseTiming'd so only append() is measured.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_Append(benchmark::State& state) {
    static constexpr std::size_t kMaxBytes   = 64ull * 1024 * 1024;
    static constexpr std::size_t kMaxRecords = 1ull << 20;

    InMemoryLog log(kMaxBytes, kMaxRecords);
    int64_t nextOff = 0;
    std::vector<uint8_t> batch;

    for (auto _ : state) {
        state.PauseTiming();
        batch = makePoseBatch(nextOff, 1700000000000LL + nextOff);
        state.ResumeTiming();

        int64_t result = log.append(batch, nextOff);
        benchmark::DoNotOptimize(result);
        ++nextOff;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(poseBatchBytes()));
}
BENCHMARK(BM_InMemoryLog_Append)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_AppendNoSync
//
// appendNoSync() is a straight alias for the same append path (no disk path to
// skip) — measured separately to confirm parity with append().
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_AppendNoSync(benchmark::State& state) {
    static constexpr std::size_t kMaxBytes   = 64ull * 1024 * 1024;
    static constexpr std::size_t kMaxRecords = 1ull << 20;

    InMemoryLog log(kMaxBytes, kMaxRecords);
    int64_t nextOff = 0;
    std::vector<uint8_t> batch;

    for (auto _ : state) {
        state.PauseTiming();
        batch = makePoseBatch(nextOff, 1700000000000LL + nextOff);
        state.ResumeTiming();

        int64_t result = log.appendNoSync(batch.data(), batch.size(), nextOff);
        benchmark::DoNotOptimize(result);
        ++nextOff;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(poseBatchBytes()));
}
BENCHMARK(BM_InMemoryLog_AppendNoSync)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_AppendSpan/{1,4,16,64}
//
// Phase 2b E2 Task 11: a FOLLOWER applies a fetched span of N whole batches in
// ONE appendNoSync; the ring copies it once and indexes it per batch (two header
// walks: validate + index).  Steady state: the same span bytes every iteration
// (the headers are rebased onto the LEO), a 4 MiB ring that evicts per batch.
// items = batches (the per-batch cost is the time / item).
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_AppendSpan(benchmark::State& state) {
    const int n = static_cast<int>(state.range(0));
    std::vector<uint8_t> span;
    for (int i = 0; i < n; ++i) {
        const auto b = makePoseBatch(i, 1700000000000LL + i);
        span.insert(span.end(), b.begin(), b.end());
    }
    InMemoryLog log(4ull * 1024 * 1024, 1ull << 20);
    for (auto _ : state) {
        const int64_t last = log.logEndOffset() + n - 1;
        int64_t result = log.appendNoSync(span.data(), span.size(), last);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(span.size()));
}
BENCHMARK(BM_InMemoryLog_AppendSpan)->Arg(1)->Arg(4)->Arg(16)->Arg(64)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_AppendRestampFloor/{1,8}
//
// Phase 2b E2 Task 12: the transient LEADER produce path in steady state — a
// full 4 MiB ring with an eviction floor (the HW) right behind the log end, so
// every append first decides the fit side-effect-free (a dry run over the
// batches it would evict), then evicts them and lands.  Arg k = the appended
// batch's size in pose-batch units: k = 1 evicts about one resident pose batch
// per append, k = 8 about eight.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_AppendRestampFloor(benchmark::State& state) {
    const int k = static_cast<int>(state.range(0));
    std::vector<uint8_t> small = makePoseBatch(0, 1700000000000LL);
    InMemoryLog log(4ull * 1024 * 1024, 1ull << 20);
    int64_t base = 0;
    while (log.logStartOffset() == 0)   // fill until it evicts (steady state)
        (void)log.appendNoSyncRestamp(small.data(), small.size(), 0, base);
    std::vector<uint8_t> big(small);   // the ring copies opaque bytes: a k-times-larger "batch"
    big.resize(small.size() * static_cast<std::size_t>(k));
    for (auto _ : state) {
        log.setEvictionFloor(log.logEndOffset());   // everything appended so far is committed
        int64_t r = log.appendNoSyncRestamp(big.data(), big.size(), 0, base);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(big.size()));
}
BENCHMARK(BM_InMemoryLog_AppendRestampFloor)->Arg(1)->Arg(8)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_AppendRefusedFloor/{1,64}
//
// Phase 2b E2 Task 12 (review M5): the REFUSED transient append — a full ring
// whose eviction floor leaves exactly k evictable batches, and an append too
// large for the room those k would free.  The dry run walks the k batches,
// meets the floor and refuses (TransientLogFull) with no side effect, so every
// iteration measures the same refusal.  Arg = k, the batches the dry run walks.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_AppendRefusedFloor(benchmark::State& state) {
    const int k = static_cast<int>(state.range(0));
    std::vector<uint8_t> small = makePoseBatch(0, 1700000000000LL);
    InMemoryLog log(4ull * 1024 * 1024, 1ull << 20);
    int64_t base = 0;
    while (log.logStartOffset() == 0)   // fill until it evicts: full
        (void)log.appendNoSyncRestamp(small.data(), small.size(), 0, base);
    log.setEvictionFloor(log.logStartOffset() + k);   // only k batches are committed
    std::vector<uint8_t> big(small);
    big.resize(small.size() * static_cast<std::size_t>(k + 2));   // more than k batches (+ slack) free
    const int64_t leo = log.logEndOffset();
    for (auto _ : state) {
        int64_t r = log.appendNoSyncRestamp(big.data(), big.size(), 0, base);
        benchmark::DoNotOptimize(r);
    }
    if (log.logEndOffset() != leo) state.SkipWithError("an append landed: the setup is not a refusal");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_AppendRefusedFloor)->Arg(1)->Arg(64)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_AppendSteadyStateFull
//
// The realistic memory-backed hot path: a ring pre-filled to its record cap so
// EVERY measured append must evict the oldest batch (O(1)) and, once the tail
// reaches the buffer end, wrap.  Exercises append + evictOldest + wrap-padding
// together.  Parameterised by ring capacity in batches.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_AppendSteadyStateFull(benchmark::State& state) {
    const int capBatches = static_cast<int>(state.range(0));
    // Size the byte ring to exactly hold capBatches (record cap binds first).
    const std::size_t maxBytes = poseBatchBytes() * static_cast<std::size_t>(capBatches);

    InMemoryLog log(maxBytes, static_cast<std::size_t>(capBatches),
                    FullPolicy::DropOldest);
    fillLog(log, capBatches);              // ring now full → next append evicts

    int64_t nextOff = capBatches;
    std::vector<uint8_t> batch;

    for (auto _ : state) {
        state.PauseTiming();
        batch = makePoseBatch(nextOff, 1700000000000LL + nextOff);
        state.ResumeTiming();

        int64_t result = log.append(batch, nextOff);
        benchmark::DoNotOptimize(result);
        ++nextOff;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(poseBatchBytes()));
    state.counters["cap_batches"] = benchmark::Counter(static_cast<double>(capBatches));
}
BENCHMARK(BM_InMemoryLog_AppendSteadyStateFull)
    ->Arg(1024)
    ->Arg(65536)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_AppendBackPressure
//
// BackPressure mode: ring pre-filled to capacity so every measured append is
// REFUSED (returns -1) with no eviction and no state change.  Measures the
// cap-check fast-reject path.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_AppendBackPressure(benchmark::State& state) {
    static constexpr int kCapBatches = 1024;
    const std::size_t maxBytes = poseBatchBytes() * kCapBatches;

    InMemoryLog log(maxBytes, kCapBatches, FullPolicy::BackPressure);
    fillLog(log, kCapBatches);             // ring full → further appends refused

    const int64_t off = kCapBatches;       // fixed offset; append never succeeds
    auto batch = makePoseBatch(off, 1700000000000LL + off);

    for (auto _ : state) {
        int64_t result = log.append(batch, off);
        benchmark::DoNotOptimize(result);  // expected: -1 every iteration
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_AppendBackPressure)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_ReadHot
//
// Pre-built log; read a single batch back from a fixed mid offset each
// iteration (as a memory-backed pushSubscriptions cycle would).  Exercises the
// entry scan + single contiguous copy.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_ReadHot(benchmark::State& state) {
    static constexpr int kBatches = 4096;
    const std::size_t maxBytes = poseBatchBytes() * (kBatches + 16);

    InMemoryLog log(maxBytes, kBatches + 16);
    fillLog(log, kBatches);

    const int64_t midOffset = kBatches / 2;

    for (auto _ : state) {
        auto data = log.read(midOffset, static_cast<int32_t>(poseBatchBytes()));
        benchmark::DoNotOptimize(data);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(poseBatchBytes()));
}
BENCHMARK(BM_InMemoryLog_ReadHot)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_ReadRange
//
// Read a multi-batch span from the start with a large maxBytes budget — the
// entry scan copies many contiguous batches into the output vector.
// Parameterised by the number of batches the budget spans.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_ReadRange(benchmark::State& state) {
    const int spanBatches = static_cast<int>(state.range(0));
    const int totalBatches = spanBatches + 64;
    const std::size_t maxBytes = poseBatchBytes() * (totalBatches + 16);

    InMemoryLog log(maxBytes, totalBatches + 16);
    fillLog(log, totalBatches);

    const int32_t budget =
        static_cast<int32_t>(poseBatchBytes() * static_cast<std::size_t>(spanBatches));

    for (auto _ : state) {
        auto data = log.read(0, budget);
        benchmark::DoNotOptimize(data);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * spanBatches);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(budget));
}
BENCHMARK(BM_InMemoryLog_ReadRange)
    ->Arg(16)
    ->Arg(256)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_ReadOutOfRange
//
// Read below logStartOffset after eviction has advanced it — the out-of-range
// early-return path (returns an empty vector without scanning entries).
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_ReadOutOfRange(benchmark::State& state) {
    static constexpr int kCapBatches = 1024;
    const std::size_t maxBytes = poseBatchBytes() * kCapBatches;

    // DropOldest ring filled past capacity so logStartOffset_ has advanced.
    InMemoryLog log(maxBytes, kCapBatches, FullPolicy::DropOldest);
    fillLog(log, kCapBatches * 2);         // offsets 0..(2*cap-1); first half evicted

    // Offset 0 is now below logStartOffset_ → out-of-range.
    for (auto _ : state) {
        auto data = log.read(0, 4096);
        benchmark::DoNotOptimize(data);    // expected: empty
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_ReadOutOfRange)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_TruncateTo
//
// Rebuild a full log then truncate half of it away.  Fresh fill is
// PauseTiming'd so only truncateTo() (tail-entry removal + bookkeeping) is
// measured.  Parameterised by number of entries removed.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_TruncateTo(benchmark::State& state) {
    const int removeCount = static_cast<int>(state.range(0));
    const int keepCount   = removeCount;          // keep as many as we remove
    const int totalCount  = keepCount + removeCount;
    const std::size_t maxBytes = poseBatchBytes() * (totalCount + 16);

    InMemoryLog log(maxBytes, totalCount + 16);

    for (auto _ : state) {
        state.PauseTiming();
        InMemoryLog fresh(maxBytes, totalCount + 16);
        fillLog(fresh, totalCount);
        state.ResumeTiming();

        fresh.truncateTo(keepCount);              // drop the top `removeCount` entries
        benchmark::DoNotOptimize(fresh.logEndOffset());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * removeCount);
    state.counters["removed"] = benchmark::Counter(static_cast<double>(removeCount));
}
BENCHMARK(BM_InMemoryLog_TruncateTo)
    ->Arg(64)
    ->Arg(1024)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_Accessors
//
// The trivial noexcept queries used on the hot path: logEndOffset(),
// logStartOffset(), isDiskBacked().
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_Accessors(benchmark::State& state) {
    InMemoryLog log(poseBatchBytes() * 1024, 1024);
    fillLog(log, 512);

    for (auto _ : state) {
        int64_t leo = log.logEndOffset();
        int64_t lso = log.logStartOffset();
        bool    disk = log.isDiskBacked();
        benchmark::DoNotOptimize(leo);
        benchmark::DoNotOptimize(lso);
        benchmark::DoNotOptimize(disk);
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_Accessors)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_SegmentBaseOffsets
//
// Returns a single-element vector {logStartOffset_} — allocates a small vector
// each call (used by PSM::rebuildFromLog).
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_SegmentBaseOffsets(benchmark::State& state) {
    InMemoryLog log(poseBatchBytes() * 1024, 1024);
    fillLog(log, 512);

    for (auto _ : state) {
        auto segs = log.segmentBaseOffsets();
        benchmark::DoNotOptimize(segs);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_SegmentBaseOffsets)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_InMemoryLog_SyncRecover
//
// sync() and recover() are no-ops for the memory-backed log; measured together
// to confirm they compile away to near-zero on the durability seam.
// ---------------------------------------------------------------------------
static void BM_InMemoryLog_SyncRecover(benchmark::State& state) {
    InMemoryLog log(poseBatchBytes() * 1024, 1024);
    fillLog(log, 512);

    for (auto _ : state) {
        log.sync();
        int64_t r = log.recover();
        benchmark::DoNotOptimize(r);
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_InMemoryLog_SyncRecover)->Unit(benchmark::kNanosecond);

} // namespace
