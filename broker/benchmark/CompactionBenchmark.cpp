// CompactionBenchmark.cpp — CP-5.5: comprehensive compaction + retention benchmarks.
//
// Coverage:
//
//  1. BM_SkimpyOffsetMap_Put        — put() at various load factors + key sizes.
//                                     Compares cache-friendly wyhash map vs std::unordered_map baseline.
//  2. BM_SkimpyOffsetMap_Get        — get() hot-path (map warm in L2/L3).
//  3. BM_SkimpyOffsetMap_vs_UnorderedMap_Put — head-to-head put() baseline comparison.
//  4. BM_SkimpyOffsetMap_vs_UnorderedMap_Get — head-to-head get() baseline comparison.
//  5. BM_Compaction_Throughput      — MB/s + records/s to compact a single dirty segment.
//                                     Parameterised by key_cardinality and dirty_ratio.
//  6. BM_Compaction_MultiSegment    — coalesced multi-segment group compaction throughput.
//  7. BM_Append_P99_During_Compaction — headline: sustained append while a background
//                                     compaction goroutine runs concurrently; report
//                                     p50/p99/p999 append latency with compaction OFF vs ON.
//                                     Scales to many partitions. Flags <100µs gate.
//  8. BM_CleanerCheckpoint_Store    — CleanerOffsetCheckpoint store() latency (mmap write + msync).
//  9. BM_CleanerCheckpoint_Load     — checkpoint load (object construction / mmap read).
// 10. BM_Retention_Pass             — deleteOldSegments decisions + enqueue cost per N segments.
// 11. BM_Coalescing_GroupBySize     — groupSegmentsBySize() time as #segments grows.
// 12. BM_FilthiestFirst_Selection   — filthiest-first scan cost as #partitions grows.
//
// Harness conventions (mirrors TransactionBenchmark.cpp):
//   - All inputs pre-built outside the timed region.
//   - DoNotOptimize results; ClobberMemory after writes.
//   - Unique temp dirs: getpid() + atomic counter (no collision across param combos).
//   - Append-p99 benchmark uses hand-rolled histogram (no external dep).

#include <benchmark/benchmark.h>

#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/types.h>
#include <unistd.h>

// Shared heap alloc counter from BenchmarkMain.cpp.
extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::atomic<int> g_dirSeq{0};

static std::filesystem::path uniqueDir(const char* tag) {
    namespace fs = std::filesystem;
    return fs::temp_directory_path() /
           ("rune_bench_compact_" + std::string(tag) +
            "_" + std::to_string(::getpid()) +
            "_" + std::to_string(g_dirSeq.fetch_add(1)));
}

// Build a key string of the given length.
static std::string makeKey(int idx, int keyLen) {
    std::string k(static_cast<std::size_t>(keyLen), '0');
    // Embed idx so keys are distinct.
    auto s = std::to_string(idx);
    if (s.size() < static_cast<std::size_t>(keyLen)) {
        std::copy(s.begin(), s.end(),
                  k.begin() + static_cast<std::ptrdiff_t>(keyLen) - static_cast<std::ptrdiff_t>(s.size()));
    } else {
        std::copy(s.begin(), s.begin() + keyLen, k.begin());
    }
    return k;
}

// Build a record batch for the given key+value.
static std::vector<uint8_t> makeBatch(int64_t baseOffset, std::string_view key,
                                       std::string_view value) {
    rune::protocol::RecordBatchBuilder b(baseOffset, 1'700'000'000'000LL + baseOffset);
    b.append(key, value, 1'700'000'000'000LL + baseOffset);
    return b.build();
}

// Build a dirty log: numKeys distinct keys, each written (dirtyFactor) times
// (so (dirtyFactor-1)/dirtyFactor of records are superseded).
// Returns (logDir, numRecords, numKeys).
static std::filesystem::path buildDirtyLog(const char* tag,
                                           int numKeys, int dirtyFactor,
                                           int64_t rollBytes = 64LL * 1024 * 1024) {
    namespace fs = std::filesystem;
    auto dir = uniqueDir(tag);
    fs::create_directories(dir);
    rune::storage::Log log(dir.string(), rollBytes);

    int64_t off = 0;
    for (int round = 0; round < dirtyFactor; ++round) {
        for (int k = 0; k < numKeys; ++k) {
            auto key   = makeKey(k, 12);
            auto value = "v" + std::to_string(round * numKeys + k);
            auto batch = makeBatch(off, key, value);
            log.append(batch, off);
            ++off;
        }
    }
    return dir;
}

// ---------------------------------------------------------------------------
// 1. BM_SkimpyOffsetMap_Put
//
// put() hot path at various map sizes (load factors) and key lengths.
// state.range(0) = map capacity (maxSlots), range(1) = key length in bytes.
// ---------------------------------------------------------------------------
static void BM_SkimpyOffsetMap_Put(benchmark::State& state) {
    const int maxSlots = static_cast<int>(state.range(0));
    const int keyLen   = static_cast<int>(state.range(1));

    // Pre-build keys outside the timed region.
    const int numKeys = std::max(1, maxSlots / 2); // 50% fill — moderate load factor
    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(numKeys));
    for (int i = 0; i < numKeys; ++i)
        keys.push_back(makeKey(i, keyLen));

    rune::storage::SkimpyOffsetMap map(static_cast<std::size_t>(maxSlots));
    int64_t idx = 0;

    for (auto _ : state) {
        if (idx >= static_cast<int64_t>(numKeys)) {
            state.PauseTiming();
            map.clear();
            idx = 0;
            state.ResumeTiming();
        }
        const auto& k = keys[static_cast<std::size_t>(idx % numKeys)];
        bool ok = map.put(k, idx);
        benchmark::DoNotOptimize(ok);
        ++idx;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["maxSlots"] = benchmark::Counter(static_cast<double>(maxSlots));
    state.counters["keyLen"]   = benchmark::Counter(static_cast<double>(keyLen));
    // Memory per entry: 16 bytes (hash64 + offset64), flat array.
    state.counters["bytes_per_entry"] = 16.0;
}
BENCHMARK(BM_SkimpyOffsetMap_Put)
    ->Args({1024,    8})
    ->Args({1024,   32})
    ->Args({65536,   8})
    ->Args({65536,  32})
    ->Args({1048576, 8})
    ->Args({1048576,32})
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 2. BM_SkimpyOffsetMap_Get
//
// get() on a pre-filled map (warm L2/L3).  50% hit rate (alternating present /
// absent keys) to exercise both the hit and miss probe chains.
// ---------------------------------------------------------------------------
static void BM_SkimpyOffsetMap_Get(benchmark::State& state) {
    const int maxSlots = static_cast<int>(state.range(0));
    const int keyLen   = static_cast<int>(state.range(1));

    const int numKeys = std::max(1, maxSlots / 2);
    std::vector<std::string> presentKeys, absentKeys;
    presentKeys.reserve(static_cast<std::size_t>(numKeys));
    absentKeys.reserve(static_cast<std::size_t>(numKeys));
    for (int i = 0; i < numKeys; ++i) {
        presentKeys.push_back(makeKey(i, keyLen));
        absentKeys.push_back(makeKey(i + numKeys * 2, keyLen)); // guaranteed absent
    }

    rune::storage::SkimpyOffsetMap map(static_cast<std::size_t>(maxSlots));
    for (int i = 0; i < numKeys; ++i)
        map.put(presentKeys[static_cast<std::size_t>(i)], static_cast<int64_t>(i));

    int64_t idx = 0;
    for (auto _ : state) {
        // Alternate present / absent.
        const auto& k = (idx % 2 == 0)
                        ? presentKeys[static_cast<std::size_t>(idx / 2 % numKeys)]
                        : absentKeys [static_cast<std::size_t>(idx / 2 % numKeys)];
        auto result = map.get(k);
        benchmark::DoNotOptimize(result);
        ++idx;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["maxSlots"] = benchmark::Counter(static_cast<double>(maxSlots));
    state.counters["keyLen"]   = benchmark::Counter(static_cast<double>(keyLen));
}
BENCHMARK(BM_SkimpyOffsetMap_Get)
    ->Args({1024,    8})
    ->Args({65536,   8})
    ->Args({1048576, 8})
    ->Args({1048576,32})
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 3 & 4. BM_SkimpyOffsetMap_vs_UnorderedMap_Put / Get
//
// Head-to-head: SkimpyOffsetMap vs std::unordered_map<string,int64_t>.
// Same key set, same map size.  Demonstrates the cache-friendly flat-array win.
// state.range(0) = numKeys, range(1) = 0=SkimpyOffsetMap / 1=unordered_map.
// ---------------------------------------------------------------------------
static void BM_OffsetMapComparison_Put(benchmark::State& state) {
    const int numKeys = static_cast<int>(state.range(0));
    const bool useUnordered = (state.range(1) == 1);

    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(numKeys));
    for (int i = 0; i < numKeys; ++i)
        keys.push_back(makeKey(i, 12));

    // Sized objects (reset inside PauseTiming on wrap).
    rune::storage::SkimpyOffsetMap skimpy(static_cast<std::size_t>(numKeys * 2));
    std::unordered_map<std::string, int64_t> umap;
    umap.reserve(static_cast<std::size_t>(numKeys * 2));

    int64_t idx = 0;

    for (auto _ : state) {
        if (idx >= static_cast<int64_t>(numKeys)) {
            state.PauseTiming();
            if (useUnordered) umap.clear();
            else              skimpy.clear();
            idx = 0;
            state.ResumeTiming();
        }
        const auto& k = keys[static_cast<std::size_t>(idx)];
        if (useUnordered) {
            auto& slot = umap[k];
            if (idx > slot) slot = idx;
            benchmark::DoNotOptimize(slot);
        } else {
            bool ok = skimpy.put(k, idx);
            benchmark::DoNotOptimize(ok);
        }
        ++idx;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["numKeys"]   = benchmark::Counter(static_cast<double>(numKeys));
    state.counters["map_type"]  = benchmark::Counter(static_cast<double>(useUnordered));
}
BENCHMARK(BM_OffsetMapComparison_Put)
    ->Args({4096,    0})   // SkimpyOffsetMap, 4K keys
    ->Args({4096,    1})   // unordered_map, 4K keys
    ->Args({65536,   0})
    ->Args({65536,   1})
    ->Args({524288,  0})
    ->Args({524288,  1})
    ->Unit(benchmark::kNanosecond);

static void BM_OffsetMapComparison_Get(benchmark::State& state) {
    const int numKeys = static_cast<int>(state.range(0));
    const bool useUnordered = (state.range(1) == 1);

    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(numKeys));
    for (int i = 0; i < numKeys; ++i)
        keys.push_back(makeKey(i, 12));

    rune::storage::SkimpyOffsetMap skimpy(static_cast<std::size_t>(numKeys * 2));
    std::unordered_map<std::string, int64_t> umap;
    umap.reserve(static_cast<std::size_t>(numKeys * 2));

    for (int i = 0; i < numKeys; ++i) {
        skimpy.put(keys[static_cast<std::size_t>(i)], static_cast<int64_t>(i));
        umap[keys[static_cast<std::size_t>(i)]] = static_cast<int64_t>(i);
    }

    int64_t idx = 0;
    for (auto _ : state) {
        const auto& k = keys[static_cast<std::size_t>(idx % numKeys)];
        if (useUnordered) {
            auto it = umap.find(k);
            benchmark::DoNotOptimize(it);
        } else {
            auto res = skimpy.get(k);
            benchmark::DoNotOptimize(res);
        }
        ++idx;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["numKeys"]  = benchmark::Counter(static_cast<double>(numKeys));
    state.counters["map_type"] = benchmark::Counter(static_cast<double>(useUnordered));
}
BENCHMARK(BM_OffsetMapComparison_Get)
    ->Args({4096,    0})
    ->Args({4096,    1})
    ->Args({65536,   0})
    ->Args({65536,   1})
    ->Args({524288,  0})
    ->Args({524288,  1})
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 5. BM_Compaction_Throughput
//
// Single-segment compaction: MB/s and records/s.
// Builds a dirty log (numKeys distinct keys, each written dirtyFactor times),
// then times produceCleanedFileBounded() on the single sealed segment.
//
// state.range(0) = numKeys (key cardinality)
// state.range(1) = dirtyFactor (how many redundant writes per key; 1 = 100% dirty)
// ---------------------------------------------------------------------------
static void BM_Compaction_Throughput(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int numKeys    = static_cast<int>(state.range(0));
    const int dirtyFactor = static_cast<int>(state.range(1));

    // Build the dirty log ONCE outside the benchmark loop.
    // Use a 1 MiB roll so we get a realistic segment size with numKeys*dirtyFactor records.
    // Each record is ~80 bytes (batch overhead + key + value), so:
    //   500  keys × 5 = 2500 records × 80 B = ~200 KiB per segment → fits in 1 seg.
    //   5000 keys × 5 = 25000 records × 80 B = ~2 MB → ~2 sealed segs at 1 MiB roll.
    // We then force a sentinel roll to ensure at least one sealed segment.
    static constexpr int64_t kRollBytes = 1LL * 1024 * 1024; // 1 MiB roll
    auto dir = buildDirtyLog("throughput", numKeys, dirtyFactor, kRollBytes);

    // Open the log, force an extra batch to ensure the main data is sealed.
    rune::storage::Log log(dir.string(), kRollBytes);
    log.recover();
    {
        // Append enough records to push the active segment past the current data.
        // One sentinel batch forces a roll if the segment is near full.
        int64_t off = log.logEndOffset();
        // Force a segment roll by appending kRollBytes worth of tiny batches.
        int64_t bytesNeeded = kRollBytes;
        while (bytesNeeded > 0) {
            auto tiny = makeBatch(off, "sentinel", std::string(128, 'x'));
            log.append(tiny, off);
            ++off;
            bytesNeeded -= static_cast<int64_t>(tiny.size());
        }
    }
    auto bases = log.segmentBaseOffsets();
    if (bases.size() < 2) {
        // Couldn't seal — nothing to compact, skip.
        fs::remove_all(dir);
        return;
    }
    const int64_t activeBase = bases.back();
    std::vector<int64_t> sealedBases;
    for (auto b : bases) if (b != activeBase) sealedBases.push_back(b);

    if (sealedBases.empty()) {
        // Nothing to compact — skip.
        fs::remove_all(dir);
        return;
    }

    const int64_t segBase  = sealedBases.front();
    const int64_t nowNs    = 1'700'000'000'000'000'000LL;
    rune::storage::LogCleaner cleaner;

    // Estimate total sealed bytes for throughput reporting.
    int64_t segBytes = 0;
    {
        for (auto sb : sealedBases) {
            std::ostringstream p;
            p << dir.string() << "/" << std::setfill('0') << std::setw(20) << sb << ".log";
            std::error_code ec;
            auto fsz = std::filesystem::file_size(p.str(), ec);
            if (!ec) segBytes += static_cast<int64_t>(fsz);
        }
    }

    for (auto _ : state) {
        state.PauseTiming();
        // Remove any previously produced .log.new for all sealed segments.
        for (auto sb : sealedBases) {
            std::ostringstream p;
            p << dir.string() << "/" << std::setfill('0') << std::setw(20) << sb << ".log.new";
            std::error_code ec;
            std::filesystem::remove(p.str(), ec);
        }
        state.ResumeTiming();

        // Compact the first sealed segment (the most-dirty one).
        auto result = cleaner.produceCleanedFileBounded(
            dir.string(),
            sealedBases,
            segBase,
            activeBase,
            /*globalDirtyStart=*/0,
            nowNs,
            /*deleteRetentionMs=*/7 * 24 * 60 * 60 * 1000LL,
            /*dedupeBufferBytes=*/128LL * 1024 * 1024,
            /*throttler=*/nullptr,
            /*nowMs=*/-1);
        benchmark::DoNotOptimize(result.compacted);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * segBytes);
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations())
                            * static_cast<int64_t>(numKeys) * dirtyFactor);
    state.counters["numKeys"]     = benchmark::Counter(static_cast<double>(numKeys));
    state.counters["dirtyFactor"] = benchmark::Counter(static_cast<double>(dirtyFactor));
    state.counters["seg_bytes"]   = benchmark::Counter(static_cast<double>(segBytes));
    fs::remove_all(dir);
}

BENCHMARK(BM_Compaction_Throughput)
    ->Args({500,  2})    // 500 keys, 2x dirty (50% redundant)  — fast
    ->Args({500,  5})    // 500 keys, 5x dirty (80% redundant)
    ->Args({2000, 2})    // 2K keys, 2x dirty
    ->Args({2000, 5})
    ->Args({5000, 2})    // 5K keys, 2x dirty — largest realistic size
    ->Args({5000, 5})
    ->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------
// 6. BM_Compaction_MultiSegment (coalesced group)
//
// Compact a group of N small segments into one output via produceCleanedGroupBounded.
// state.range(0) = number of segments in the group.
// ---------------------------------------------------------------------------
static void BM_Compaction_MultiSegment(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int numSegs = static_cast<int>(state.range(0));

    // Build a log with numSegs sealed segments of ~50 records each.
    static constexpr int64_t kRollBytes = 4 * 1024LL; // 4 KiB → small segments
    static constexpr int kRecsPerSeg    = 80;
    static constexpr int kNumKeys       = 200;

    auto dir = uniqueDir("multiSeg");
    fs::create_directories(dir);
    {
        rune::storage::Log log(dir.string(), kRollBytes);
        int64_t off = 0;
        for (int s = 0; s < numSegs; ++s) {
            for (int r = 0; r < kRecsPerSeg; ++r) {
                auto key   = makeKey(r % kNumKeys, 12);
                auto value = "val" + std::to_string(off);
                auto batch = makeBatch(off, key, value);
                log.append(batch, off);
                ++off;
            }
        }
        // Force one more small seg so the last above is sealed.
        auto sentinel = makeBatch(off, "sentinel", "sentinel");
        log.append(sentinel, off);
    }

    rune::storage::Log log(dir.string(), kRollBytes);
    log.recover();
    auto bases = log.segmentBaseOffsets();
    if (static_cast<int>(bases.size()) < 2) {
        fs::remove_all(dir);
        return;
    }
    const int64_t activeBase = bases.back();
    std::vector<int64_t> sealedBases;
    std::vector<int64_t> sealedSizes;
    for (std::size_t i = 0; i + 1 < bases.size(); ++i) {
        sealedBases.push_back(bases[i]);
        // Approximate size from file.
        std::ostringstream p;
        p << dir.string() << "/" << std::setfill('0') << std::setw(20) << bases[i] << ".log";
        std::error_code ec;
        auto sz = std::filesystem::file_size(p.str(), ec);
        sealedSizes.push_back(ec ? 4096 : static_cast<int64_t>(sz));
    }

    // Build a group from all sealed segs.
    rune::storage::SegmentGroup group;
    group.bases = sealedBases;
    group.combinedBytes = 0;
    for (auto s : sealedSizes) group.combinedBytes += s;

    const int64_t nowNs = 1'700'000'000'000'000'000LL;
    rune::storage::LogCleaner cleaner;

    for (auto _ : state) {
        state.PauseTiming();
        // Remove .log.new temps from previous iteration.
        for (auto b : sealedBases) {
            std::ostringstream p;
            p << dir.string() << "/" << std::setfill('0') << std::setw(20) << b << ".log.new";
            std::error_code ec;
            std::filesystem::remove(p.str(), ec);
        }
        state.ResumeTiming();

        auto result = cleaner.produceCleanedGroupBounded(
            dir.string(),
            sealedBases,
            group,
            activeBase,
            /*globalDirtyStart=*/0,
            nowNs,
            /*deleteRetentionMs=*/7 * 24 * 60 * 60 * 1000LL,
            /*dedupeBufferBytes=*/16LL * 1024 * 1024,
            /*throttler=*/nullptr,
            /*nowMs=*/-1);
        benchmark::DoNotOptimize(result.compacted);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["numSegs"] = benchmark::Counter(static_cast<double>(numSegs));
    fs::remove_all(dir);
}

BENCHMARK(BM_Compaction_MultiSegment)
    ->Arg(2)
    ->Arg(4)
    ->Arg(8)
    ->Arg(16)
    ->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------
// 7. BM_Append_P99_During_Compaction
//
// THE HEADLINE BENCHMARK: prove append p99 stays <100µs while a concurrent
// compaction goroutine does file I/O.
//
// Protocol:
//   - Create N separate Logs in separate directories (one per "partition").
//   - Launch a background compactor thread that repeatedly calls
//     produceCleanedFileBounded on the FIRST log (most dirty).
//   - Main thread drives sustained appends across ALL logs (round-robin),
//     measuring per-append latency with a hand-rolled histogram.
//   - Run two configurations: compaction=OFF (baseline) and compaction=ON.
//   - Report p50/p99/p999 latency; flag if p99 >= 100 µs.
//
// state.range(0) = numPartitions
// state.range(1) = 0=compaction OFF, 1=compaction ON
//
// Bounded: 5000 appends total per iteration regardless of compaction status.
// ---------------------------------------------------------------------------

// Hand-rolled latency histogram (ns resolution).
struct LatencyHistogram {
    static constexpr int kBuckets = 2000; // 0–2000 µs in 1µs buckets
    std::array<int64_t, kBuckets> counts{};
    int64_t overflow = 0;
    int64_t totalSamples = 0;

    void record(int64_t ns) {
        ++totalSamples;
        int64_t us = ns / 1000;
        if (us >= kBuckets) { ++overflow; return; }
        ++counts[static_cast<std::size_t>(us)];
    }

    // Returns p-th percentile in microseconds (0.0–1.0).
    double percentile(double p) const {
        if (totalSamples == 0) return 0.0;
        int64_t target = static_cast<int64_t>(p * static_cast<double>(totalSamples));
        int64_t cumulative = 0;
        for (int i = 0; i < kBuckets; ++i) {
            cumulative += counts[static_cast<std::size_t>(i)];
            if (cumulative >= target) return static_cast<double>(i);
        }
        return static_cast<double>(kBuckets);
    }
};

static void BM_Append_P99_During_Compaction(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int  numPartitions  = static_cast<int>(state.range(0));
    const bool compactionOn   = (state.range(1) == 1);

    static constexpr int     kAppendsPerIter = 5000;   // total appends to measure per run
    static constexpr int64_t kRollBytes      = 4 * 1024LL;   // 4 KiB — frequent rolling
    static constexpr int     kWarmupBatches  = 200;    // batches per partition before timing

    // Create partition directories and seed with some dirty data.
    std::vector<std::filesystem::path> dirs;
    dirs.reserve(static_cast<std::size_t>(numPartitions));
    for (int i = 0; i < numPartitions; ++i)
        dirs.push_back(uniqueDir(("p" + std::to_string(i)).c_str()));

    // Build one batch to append (pre-built for minimal timed overhead).
    // Each partition gets a fresh log; we pre-fill with warm-up data.
    std::vector<std::unique_ptr<rune::storage::Log>> logs;
    logs.reserve(static_cast<std::size_t>(numPartitions));
    for (int i = 0; i < numPartitions; ++i) {
        fs::create_directories(dirs[static_cast<std::size_t>(i)]);
        auto logp = std::make_unique<rune::storage::Log>(dirs[static_cast<std::size_t>(i)].string(), kRollBytes);
        // Warm up with kWarmupBatches writes so there's dirty data to compact.
        int64_t off = 0;
        for (int w = 0; w < kWarmupBatches; ++w) {
            auto batch = makeBatch(off, makeKey(w % 50, 8), "warm");
            logp->append(batch, off);
            ++off;
        }
        logs.push_back(std::move(logp));
    }

    // Compaction background thread: repeatedly compact log[0].
    std::atomic<bool> stopCompactor{false};
    std::thread compactorThread;

    if (compactionOn) {
        compactorThread = std::thread([&] {
            rune::storage::LogCleaner cleaner;
            rune::storage::Log& log0 = *logs[0];
            const std::string& d0 = log0.dir();

            while (!stopCompactor.load(std::memory_order_relaxed)) {
                // Get sealed bases from disk (avoid segments_ — pure file scan).
                auto allBases = log0.segmentBaseOffsets();
                if (allBases.size() < 2) {
                    std::this_thread::yield();
                    continue;
                }
                int64_t activeBase = allBases.back();
                std::vector<int64_t> sealedBases;
                for (auto b : allBases) if (b != activeBase) sealedBases.push_back(b);
                if (sealedBases.empty()) { std::this_thread::yield(); continue; }

                int64_t segBase = sealedBases.front();
                // Remove stale .log.new.
                {
                    std::ostringstream p;
                    p << d0 << "/" << std::setfill('0') << std::setw(20) << segBase << ".log.new";
                    std::error_code ec;
                    std::filesystem::remove(p.str(), ec);
                }
                auto r = cleaner.produceCleanedFileBounded(
                    d0, sealedBases, segBase, activeBase,
                    0, 1'700'000'000'000'000'000LL,
                    7 * 24 * 3600 * 1000LL,
                    128LL * 1024 * 1024,
                    nullptr, -1);
                benchmark::DoNotOptimize(r.compacted);
                // Let append thread breathe between compaction passes.
                std::this_thread::yield();
            }
        });
    }

    // Pre-build ALL batches for one full iteration outside the timed region.
    // Batch baseOffset must match each log's expected LEO, so we build them
    // in PauseTiming just before each timed burst.
    // We snapshot per-partition LEOs, build batches, then resume timing.
    //
    // Inner hot path: only appendNoSync() is in the timed region — no alloc.
    LatencyHistogram hist;

    // Track current LEO per partition.
    std::vector<int64_t> leos(static_cast<std::size_t>(numPartitions));
    for (int p = 0; p < numPartitions; ++p)
        leos[static_cast<std::size_t>(p)] = logs[static_cast<std::size_t>(p)]->logEndOffset();

    // Pre-allocated batch storage (kAppendsPerIter batches).
    struct BatchEntry { std::vector<uint8_t> data; int64_t off; int partition; };
    std::vector<BatchEntry> prebuilt(static_cast<std::size_t>(kAppendsPerIter));

    for (auto _ : state) {
        // Build the batch for each append in PauseTiming.
        state.PauseTiming();
        hist = LatencyHistogram{};
        // Re-snapshot LEOs (they advanced in the previous iteration).
        for (int p = 0; p < numPartitions; ++p)
            leos[static_cast<std::size_t>(p)] = logs[static_cast<std::size_t>(p)]->logEndOffset();
        for (int a = 0; a < kAppendsPerIter; ++a) {
            const int pi = a % numPartitions;
            int64_t off = leos[static_cast<std::size_t>(pi)];
            prebuilt[static_cast<std::size_t>(a)].data = makeBatch(off, makeKey(a % 64, 8), "v");
            prebuilt[static_cast<std::size_t>(a)].off = off;
            prebuilt[static_cast<std::size_t>(a)].partition = pi;
            leos[static_cast<std::size_t>(pi)] = off + 1;
        }
        state.ResumeTiming();

        for (int a = 0; a < kAppendsPerIter; ++a) {
            const auto& entry = prebuilt[static_cast<std::size_t>(a)];
            auto& log = *logs[static_cast<std::size_t>(entry.partition)];

            auto t0 = std::chrono::steady_clock::now();
            int64_t result = log.appendNoSync(entry.data.data(), entry.data.size(), entry.off);
            auto t1 = std::chrono::steady_clock::now();

            benchmark::DoNotOptimize(result);
            int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
            hist.record(ns);
        }
        benchmark::ClobberMemory();
    }

    if (compactionOn) {
        stopCompactor.store(true, std::memory_order_relaxed);
        compactorThread.join();
    }

    // Report p50/p99/p999 in microseconds.
    double p50  = hist.percentile(0.50);
    double p99  = hist.percentile(0.99);
    double p999 = hist.percentile(0.999);

    state.counters["p50_us"]          = p50;
    state.counters["p99_us"]          = p99;
    state.counters["p999_us"]         = p999;
    state.counters["compaction_on"]   = benchmark::Counter(static_cast<double>(compactionOn));
    state.counters["num_partitions"]  = benchmark::Counter(static_cast<double>(numPartitions));
    // Gate: p99 < 100 µs.  1.0 = PASS, 0.0 = FAIL (flag for CP-10 fix).
    state.counters["p99_gate_pass"]   = (p99 < 100.0) ? 1.0 : 0.0;
    state.counters["total_appends"]   = benchmark::Counter(
        static_cast<double>(state.iterations()) * kAppendsPerIter,
        benchmark::Counter::kIsRate);

    for (auto& d : dirs) fs::remove_all(d);
}

BENCHMARK(BM_Append_P99_During_Compaction)
    // Baseline: compaction OFF.
    ->Args({1,   0})
    ->Args({4,   0})
    ->Args({16,  0})
    ->Args({64,  0})
    ->Args({128, 0})
    // Compaction ON at the same partition counts.
    ->Args({1,   1})
    ->Args({4,   1})
    ->Args({16,  1})
    ->Args({64,  1})
    ->Args({128, 1})
    ->Unit(benchmark::kMillisecond)
    ->Iterations(3);  // 3 iterations each: fast but enough for stable p99

// ---------------------------------------------------------------------------
// 8. BM_CleanerCheckpoint_Store
//
// CleanerOffsetCheckpoint::store() latency — mmap write + msync per call.
// This is the cleaner's persistence cost per compaction pass.
// ---------------------------------------------------------------------------
static void BM_CleanerCheckpoint_Store(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = uniqueDir("ckpt_store");
    fs::create_directories(dir);
    auto path = (dir / "cleaner-offset-checkpoint").string();

    rune::storage::CleanerOffsetCheckpoint ckpt(path, 0);

    int64_t offset = 0;
    for (auto _ : state) {
        ckpt.store(offset);
        benchmark::DoNotOptimize(offset);
        ++offset;
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_CleanerCheckpoint_Store)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 9. BM_CleanerCheckpoint_Load
//
// CleanerOffsetCheckpoint construction (open + mmap read + validation).
// Represents cold-start / broker-restart checkpoint recovery cost.
// ---------------------------------------------------------------------------
static void BM_CleanerCheckpoint_Load(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = uniqueDir("ckpt_load");
    fs::create_directories(dir);
    auto path = (dir / "cleaner-offset-checkpoint").string();

    // Pre-write a valid checkpoint.
    {
        rune::storage::CleanerOffsetCheckpoint ckpt(path, 0);
        ckpt.store(12345678LL);
    }

    for (auto _ : state) {
        rune::storage::CleanerOffsetCheckpoint ckpt(path, 999999999LL);
        benchmark::DoNotOptimize(ckpt.value());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_CleanerCheckpoint_Load)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 10. BM_Retention_Pass
//
// deleteOldSegments() decisions per N segments.  Mirrors RetentionBenchmark.cpp
// but focuses on the decision pass cost (not just the delete itself), using
// retainBytes = half of total to exercise both "keep" and "delete" branches.
// state.range(0) = number of sealed segments.
// ---------------------------------------------------------------------------
static void BM_Retention_Pass(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int segCount = static_cast<int>(state.range(0));

    static constexpr int64_t kRollBytes     = 4 * 1024LL;
    static constexpr int     kBatchesPerSeg = 82;

    auto dir = uniqueDir("retention");

    for (auto _ : state) {
        state.PauseTiming();
        fs::remove_all(dir);
        fs::create_directories(dir);
        {
            rune::storage::Log log(dir.string(), kRollBytes);
            const int totalBatches = segCount * kBatchesPerSeg + kBatchesPerSeg;
            for (int i = 0; i < totalBatches; ++i) {
                auto batch = makeBatch(static_cast<int64_t>(i), "key", "val");
                log.append(batch, static_cast<int64_t>(i));
            }
        }
        rune::storage::Log log(dir.string(), kRollBytes);
        // retainBytes = half total → delete oldest half, keep newest half.
        int64_t totalBytes = log.totalBytes();
        int64_t retainBytes = totalBytes / 2;
        state.ResumeTiming();

        int deleted = log.deleteOldSegments(retainBytes);
        benchmark::DoNotOptimize(deleted);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations())
                            * static_cast<int64_t>(segCount));
    state.counters["segs_per_iter"] = benchmark::Counter(static_cast<double>(segCount));
    fs::remove_all(dir);
}

BENCHMARK(BM_Retention_Pass)
    ->Arg(4)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 11. BM_Coalescing_GroupBySize
//
// LogCleaner::groupSegmentsBySize() computation cost as #segments grows.
// Pure in-memory computation — no I/O.
// state.range(0) = number of sealed segments.
// ---------------------------------------------------------------------------
static void BM_Coalescing_GroupBySize(benchmark::State& state) {
    const int numSegs = static_cast<int>(state.range(0));
    static constexpr int64_t kSegBytes   = 4096LL;     // 4 KiB each
    static constexpr int64_t kRollBytes  = 64LL * 1024 * 1024;

    // Build sorted bases and sizes.
    std::vector<int64_t> bases, sizes;
    bases.reserve(static_cast<std::size_t>(numSegs));
    sizes.reserve(static_cast<std::size_t>(numSegs));
    for (int i = 0; i < numSegs; ++i) {
        bases.push_back(static_cast<int64_t>(i) * 100);
        sizes.push_back(kSegBytes);
    }

    for (auto _ : state) {
        auto groups = rune::storage::LogCleaner::groupSegmentsBySize(
            bases, sizes, kRollBytes);
        benchmark::DoNotOptimize(groups.size());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["numSegs"] = benchmark::Counter(static_cast<double>(numSegs));
}

BENCHMARK(BM_Coalescing_GroupBySize)
    ->Arg(10)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Arg(5000)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 12. BM_FilthiestFirst_Selection
//
// Simulate the LogCleanerAgent filthiest-first scan: iterate over N partitions,
// compute per-partition dirty ratio from a SegInfo snapshot, and pick the max.
// Exercises how the per-cycle O(N) scan scales with #partitions.
//
// Uses a pure-CPU model (no I/O); the SegInfo snapshot vector matches what the
// agent reads via sealedSegmentSnapshot().
// state.range(0) = number of partitions.
// ---------------------------------------------------------------------------
static void BM_FilthiestFirst_Selection(benchmark::State& state) {
    const int numPartitions = static_cast<int>(state.range(0));
    static constexpr int kSegsPerPartition = 8;

    // Build a fake snapshot for each partition.
    struct FakePartition {
        std::vector<rune::storage::SegInfo> segs;
        int64_t firstDirtyOffset = 0;
        int64_t hw               = 0;
    };

    std::vector<FakePartition> parts(static_cast<std::size_t>(numPartitions));
    for (int p = 0; p < numPartitions; ++p) {
        auto& fp = parts[static_cast<std::size_t>(p)];
        fp.firstDirtyOffset = static_cast<int64_t>(p % 3) * 100LL; // vary dirty start
        fp.hw = static_cast<int64_t>(kSegsPerPartition) * 1000LL;
        for (int s = 0; s < kSegsPerPartition; ++s) {
            rune::storage::SegInfo si;
            si.baseOffset      = static_cast<int64_t>(s) * 1000LL;
            si.sizeBytes       = 4096LL;
            si.largestTimestamp = 1'700'000'000'000LL + s * 60'000LL;
            fp.segs.push_back(si);
        }
    }

    for (auto _ : state) {
        // Simulate filthiest-first scan.
        double bestRatio = 0.0;
        int    bestIdx   = -1;

        for (int p = 0; p < numPartitions; ++p) {
            const auto& fp = parts[static_cast<std::size_t>(p)];
            const int64_t firstDirty = fp.firstDirtyOffset;
            const int64_t hw         = fp.hw;

            int64_t cleanBytes = 0, cleanableBytes = 0;
            for (std::size_t i = 0; i < fp.segs.size(); ++i) {
                const auto& si = fp.segs[i];
                if (si.baseOffset >= hw) break;
                int64_t siEnd = (i + 1 < fp.segs.size())
                                ? fp.segs[i + 1].baseOffset
                                : hw;
                if (siEnd <= firstDirty) cleanBytes += si.sizeBytes;
                else                     cleanableBytes += si.sizeBytes;
            }
            const int64_t total = cleanBytes + cleanableBytes;
            if (total <= 0) continue;
            double ratio = static_cast<double>(cleanableBytes) / static_cast<double>(total);
            if (ratio > bestRatio) { bestRatio = ratio; bestIdx = p; }
        }

        benchmark::DoNotOptimize(bestRatio);
        benchmark::DoNotOptimize(bestIdx);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["numPartitions"] = benchmark::Counter(static_cast<double>(numPartitions));
}

BENCHMARK(BM_FilthiestFirst_Selection)
    ->Arg(10)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Arg(5000)
    ->Unit(benchmark::kNanosecond);

} // namespace
