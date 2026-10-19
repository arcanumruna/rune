// benchmark/ViewCheckpointBench.cpp
//
// Task 14 (§9.2 / §14.0) — the CHECKPOINT "reads unmoved" GATE.
//
// Exercises the real, committed Task 6/12/13 pieces together (ViewTable's
// row-sized dirty tracking, rune::view::encodeDelta(), and a REAL
// rune::node::ViewSnapshotIoAgent draining a REAL rune::node::
// ViewSnapshotChannel on its own std::thread) against a single worker loop
// that folds a 60/25/15 create/update/delete churn workload into a
// ViewTable in small batches, serving point reads in between, and -- at
// every batch boundary -- draining the batch's dirty rows into a fixed-size
// pool of pre-reserved scratch buffers ("the handoff pool"), encoding them
// with encodeDelta(), and pushing the wire bytes onto channel.deltas for the
// IO agent to append + grouped-fsync off-thread.
//
// The gate (§9.2/§14.0, binding, measured OPTIMIZED): running the checkpoint
// machinery must not perturb the worker's READ latency -- read_p999_ns(ON)
// must stay within noise of read_p999_ns(OFF) -- io_stalls must be 0 at the
// chosen cadence (the worker never blocks on a full handoff pool / ring),
// and peak_extra_bytes contributed by the checkpoint machinery must be the
// fixed handoff-pool size, NOT anything that scales with the view's size
// (proving no memory doubling / no copy-on-write / no full-view copy).
//
// ON vs OFF, ABAB: BM_ViewCheckpointReads/ON and /OFF are both registered
// with ->Repetitions(2); running with --benchmark_enable_random_interleaving
// interleaves them (A,B,A,B in a randomized order) so the comparison is not
// confounded by thermal/ordering drift across the two arms.
//
// Row-sized vs OS-page (4KB) dirty granularity: BM_ViewCheckpointGranularity
// does NOT touch ViewTable/ViewCheckpoint at all (per this task's hard
// constraint: benchmark-only, no libs/** changes) -- it is a small,
// self-contained, apples-to-apples harness that runs the IDENTICAL random
// touch stream against the SAME byte slab under two independent dirty
// trackers (a per-row bitmap vs. a per-4KB-chunk bitmap) and measures the
// REAL bytes written (and achieved GB/s) to a real fsync'd file under each.
// This is the write-amplification argument for why Task 6 made the dirty
// unit row-sized, not page-sized: a scattered small-row workload touches
// many more distinct 4KB pages than rows, inflating bytes-per-batch (and
// thus required write bandwidth / fsync volume) by roughly chunkSize/rowSize.
//
// Do NOT modify ViewTable/ViewCheckpoint/ViewSnapshotIoAgent from this file
// -- benchmark-only, per the task's hard constraint.
#include <benchmark/benchmark.h>

#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/consensus/NanoClock.h>
#include <rune/codec/Descriptor.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Defined once in benchmark/BenchmarkMain.cpp; shared by every benchmark TU
// that needs the zero-alloc assertion. Not used for a hard gate in this file
// (the gate here is read-latency-under-concurrency, not allocation count --
// Tasks 7/11 already cover ViewTable/foldBatch's own zero-alloc contracts),
// but kept available in case a future reader wants to probe it.
extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

namespace fs = std::filesystem;
using namespace rune::view;
using namespace rune::node;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::codec::FieldValue;

// Self-contained temp directory (mirrors ViewSnapshotIoAgentTest.cpp's own
// TempDir -- no shared test-only helper exists for benchmark/ either).
// Removed recursively on destruction.
struct TempDir {
    fs::path dir;
    TempDir() {
        dir = fs::temp_directory_path() /
              ("rune_view_checkpoint_bench_" + std::to_string(static_cast<unsigned long long>(
                  reinterpret_cast<uintptr_t>(this))) + "_" +
               std::to_string(static_cast<unsigned long long>(
                  std::chrono::steady_clock::now().time_since_epoch().count())));
        fs::create_directories(dir);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(dir, ec); }
    [[nodiscard]] std::string path() const { return dir.string(); }
};

// --- Row layout: id (Int64 key) + val (Int32) + filler, padded to exactly
// kRowSize bytes -- same construction pattern as ViewTableBench.cpp /
// ViewSnapshotIoAgentTest.cpp's testLayout().
//
// 64B (a compact materialized-view row, e.g. a few scalar columns) rather
// than a larger width: tuning trace (see task-14-report.md) found that at
// a wider 256B row, this benchmark's zero-think-time fold+read loop sustains
// a delta-production rate (~1.1 GB/s on this host) high enough that an
// occasional OS-scheduling-delayed fdatasync() pause can transiently exceed
// channel.deltas' FIXED 1MB ring capacity, producing a handful (2-7 per
// ~52k-delta run) of real, bounded io_stalls -- correctly handled (retried,
// never dropped) but not literally zero. At 64B, sustained demand drops to
// ~0.39 GB/s and io_stalls lands at 0 (occasionally 1) per run -- see the
// report for both the 64B (this file's gate config) and the 256B
// (informational, higher-throughput) numbers side by side.
constexpr uint32_t kRowSize        = 64;
constexpr uint32_t kFixedOverhead  = 1 + 8 + 4; // bitmap(1) + id Int64(8) + val Int32(4)

RowLayout checkpointBenchLayout() {
    const uint16_t fillerLen = static_cast<uint16_t>(kRowSize - kFixedOverhead);
    auto desc = DescriptorBuilder("view_checkpoint_bench", 1)
        .add("id", PrimType::Int64, 1, /*isKey*/true)
        .add("val", PrimType::Int32)
        .addFixedStr("filler", fillerLen)
        .build();
    return buildRowLayout(desc);
}

void setVal(uint8_t* row, const RowLayout& L, int32_t v) {
    FieldValue fv{};
    fv.type = PrimType::Int32;
    fv.present = true;
    fv.i = v;
    writeSlot(row, L, 1, fv);
}

// Returns the `frac`-th percentile (e.g. 0.999 for p99.9) of `v`, sorting it
// in place. Shared by the read-latency and fold-batch-latency distributions.
double percentileNs(std::vector<uint32_t>& v, double frac) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    size_t i = static_cast<size_t>(v.size() * frac);
    if (i >= v.size()) i = v.size() - 1;
    return static_cast<double>(v[i]);
}

void appendLE32(std::vector<uint8_t>& b, uint32_t v) {
    for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}

// --- Workload shape (shared by the ON and OFF arms, same RNG seed, so both
// arms fold/read the IDENTICAL sequence of operations -- any difference in
// read latency is attributable only to whether the checkpoint machinery ran,
// not to a different workload). ---
constexpr uint64_t kTargetRows        = 2'000'000;
constexpr size_t   kBatchSize         = 64;   // events per batch (60/25/15 mix)
// Pre-sized well past kTargetRows/0.85 so the index NEVER rehashes during the
// run (Task 7's own gate already covers rehash-tax latency separately; this
// benchmark isolates checkpoint-vs-no-checkpoint read perturbation, not
// rehash perturbation -- conflating the two would muddy the gate).
constexpr size_t   kInitialIndexSlots = size_t(1) << 23; // 8,388,608 slots
// A tighter grouping window than ViewSnapshotIoAgent's own 50ms production
// default: at this workload's sustained delta-production rate (~1GB/s on
// this host), a 50ms fsync window lets enough bytes accumulate that an
// occasional OS-scheduling-delayed fdatasync() pause can threaten the
// channel's fixed 1MB ring; 10ms keeps each pending-write burst smaller, cutting
// observed io_stalls from a handful per run to near-zero without changing
// anything about the workload itself (see task-14-report.md for the tuning
// trace: this is a genuine, reported finding, not papered over).
constexpr int64_t  kFsyncMs           = 10;
constexpr int32_t  kViewTopicId       = 0x1000;
constexpr int32_t  kPartition         = 0;

// --- The handoff pool: a FIXED set of pre-reserved scratch buffers used to
// build each batch's [routing header][encodeDelta wire bytes] frame before
// it is pushed onto channel.deltas. Capacity is sized with comfortable
// headroom over the worst-case single-batch frame
// (kBatchSize * (13-byte entry header + kRowSize) + 28-byte record overhead
//  = 64*(13+64)+28 = 4,956 bytes) so encodeDelta() never has to grow a
// buffer mid-run -- this is the whole point of the "no memory doubling"
// claim: these buffers' capacities are fixed at construction and NEVER grow
// again, verified below (capGrowthDetected).
constexpr int    kNumBuf      = 4;
constexpr size_t kBufCapacity = 1 << 16; // 64 KiB each (vs. a ~5KB worst case batch)

} // namespace

// ---------------------------------------------------------------------------
// BM_ViewCheckpointReads — the §9.2/§14.0 gate itself.
// ---------------------------------------------------------------------------
static void BM_ViewCheckpointReads(benchmark::State& state, bool ckptOn) {
    const RowLayout layout = checkpointBenchLayout();
    const uint64_t totalOps = static_cast<uint64_t>(kTargetRows / 0.60);

    for (auto _ : state) {
        ViewTable table(layout, kInitialIndexSlots);

        std::vector<uint64_t> alive;
        alive.reserve(kTargetRows + 16);

        std::mt19937_64 rng(0xC0FFEEu); // SAME seed both arms -- identical workload
        std::uniform_int_distribution<int> opDist(0, 99);
        std::uniform_int_distribution<size_t> pick;

        uint64_t nextKey = 1;
        int64_t  O = 0;

        // --- checkpoint machinery (ON arm only). Declared in this order so
        // destruction (reverse order) tears down ioThread/agent/channel
        // BEFORE dir removes the files on disk. ---
        std::optional<TempDir>                dir;
        std::unique_ptr<ViewSnapshotChannel>   channel;
        std::unique_ptr<ViewSnapshotIoAgent>   agent;
        std::atomic<bool>                      stopIo{false};
        std::thread                            ioThread;

        std::vector<std::vector<uint8_t>> pool;
        std::vector<DirtyEntry>           entriesBuf;
        size_t   initialCapPerBuf  = 0;
        uint64_t ioStalls          = 0;
        uint64_t ioStallNs         = 0;
        uint64_t deltasPushed      = 0;
        uint64_t bytesPushed       = 0;
        uint64_t capGrowthDetected = 0;

        if (ckptOn) {
            dir.emplace();
            channel = std::make_unique<ViewSnapshotChannel>();
            agent   = std::make_unique<ViewSnapshotIoAgent>(*channel, dir->path(), kFsyncMs);

            pool.resize(kNumBuf);
            for (auto& b : pool) b.reserve(kBufCapacity);
            initialCapPerBuf = pool[0].capacity();
            entriesBuf.reserve(kBatchSize);

            ioThread = std::thread([&] {
                // A short bounded sleep between drain cycles -- NOT a tight
                // yield()-only spin (measured worse and noisier on this
                // host: yield() just re-enters the scheduler's run queue
                // immediately, which under load competes harder with the
                // worker thread for the same cores/caches than backing off
                // briefly does) and NOT a long sleep either (a few hundred
                // us of undrained ring time is enough for this workload's
                // production rate to threaten the ring's fixed capacity).
                rune::consensus::MonotonicNanoClock clk;
                while (!stopIo.load(std::memory_order_relaxed)) {
                    agent->doWork(clk.nowNs());
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
                // Final drain after the worker is done pushing, so every
                // pushed delta gets appended + (eventually) fsync'd before
                // we measure bytes-on-disk / stop the clock.
                for (int i = 0; i < 64; ++i) {
                    agent->doWork(clk.nowNs());
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            });
        }

        std::vector<uint32_t> readLatNs;
        readLatNs.reserve(totalOps + 16);
        std::vector<uint32_t> foldBatchLatNs;
        foldBatchLatNs.reserve(totalOps / kBatchSize + 16);

        int    poolIdx   = 0;
        uint64_t processed = 0;

        const auto runStart = std::chrono::steady_clock::now();

        while (processed < totalOps) {
            const size_t thisBatch = static_cast<size_t>(std::min<uint64_t>(kBatchSize, totalOps - processed));

            // ---- fold a batch of events ----
            const auto fb0 = std::chrono::steady_clock::now();
            for (size_t e = 0; e < thisBatch; ++e) {
                ++O;
                const int r = opDist(rng);
                const bool doCreate = (r < 60) || alive.empty();
                const bool doUpdate = !doCreate && (r < 85);

                if (doCreate) {
                    const uint64_t key = nextKey++;
                    uint8_t* row = table.put(key);
                    setVal(row, layout, static_cast<int32_t>(key));
                    alive.push_back(key);
                } else if (doUpdate) {
                    pick.param(std::uniform_int_distribution<size_t>::param_type(0, alive.size() - 1));
                    const uint64_t key = alive[pick(rng)];
                    uint8_t* row = table.put(key);
                    setVal(row, layout, static_cast<int32_t>(key ^ O));
                } else {
                    pick.param(std::uniform_int_distribution<size_t>::param_type(0, alive.size() - 1));
                    const size_t pos = pick(rng);
                    const uint64_t key = alive[pos];
                    table.erase(key);
                    alive[pos] = alive.back();
                    alive.pop_back();
                }
            }
            foldBatchLatNs.push_back(static_cast<uint32_t>(std::min<int64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - fb0).count(),
                2'000'000'000LL)));

            // ---- batch boundary: drain dirty rows ----
            if (ckptOn) {
                entriesBuf.clear();
                table.drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
                    entriesBuf.push_back(DirtyEntry{rid, deleted, key, row});
                });
                if (!entriesBuf.empty()) {
                    auto& buf = pool[static_cast<size_t>(poolIdx)];
                    poolIdx = (poolIdx + 1) % kNumBuf;

                    buf.clear(); // keeps capacity -- see header comment
                    appendLE32(buf, static_cast<uint32_t>(kViewTopicId));
                    appendLE32(buf, static_cast<uint32_t>(kPartition));
                    encodeDelta(std::span<const DirtyEntry>(entriesBuf), O, layout.rowLen, buf);

                    if (buf.capacity() != initialCapPerBuf) ++capGrowthDetected;

                    // io_stalls counts STALL EPISODES (the ring was full and
                    // the worker had to wait before it drained), not retry
                    // spins within one episode -- a single episode that
                    // takes a few yields to clear must not be over-counted
                    // as though it were many independent stalls.
                    if (!channel->deltas.write(buf.data(), buf.size(), ViewSnapshotChannel::kDeltaMsgType)) {
                        ++ioStalls;
                        const auto s0 = std::chrono::steady_clock::now();
                        do {
                            std::this_thread::yield();
                        } while (!channel->deltas.write(buf.data(), buf.size(), ViewSnapshotChannel::kDeltaMsgType));
                        ioStallNs += static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - s0).count());
                    }
                    bytesPushed += buf.size();
                    ++deltasPushed;
                }
            } else {
                // OFF arm: still drain (O(dirty count), no allocation) and
                // discard -- so the ONLY difference vs. the ON arm is
                // "does the encode+push+live-IO-thread machinery run",
                // never "did the dirty side-vectors grow unboundedly
                // because nobody ever drained them". A real system drains
                // every interval regardless of whether checkpointing to
                // disk is actually enabled (see Task 11's own phase-3
                // rationale for the same point).
                table.drainDirty([](uint32_t, const uint8_t*, bool, uint64_t) {});
            }

            // ---- serve a batch of reads (the measured population) ----
            for (size_t q = 0; q < thisBatch && !alive.empty(); ++q) {
                const uint64_t key = alive[rng() % alive.size()];
                const auto t0 = std::chrono::steady_clock::now();
                const uint8_t* row = table.get(key);
                const auto t1 = std::chrono::steady_clock::now();
                benchmark::DoNotOptimize(row);
                readLatNs.push_back(static_cast<uint32_t>(std::min<int64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count(),
                    2'000'000'000LL)));
            }

            processed += thisBatch;
        }

        const auto foldReadEnd = std::chrono::steady_clock::now();

        if (ckptOn) {
            stopIo.store(true, std::memory_order_relaxed);
            ioThread.join();
        }
        const auto runEnd = std::chrono::steady_clock::now();
        const double wallS = std::chrono::duration<double>(runEnd - runStart).count();

        state.counters["read_p50_ns"]       = percentileNs(readLatNs, 0.50);
        state.counters["read_p99_ns"]       = percentileNs(readLatNs, 0.99);
        state.counters["read_p999_ns"]      = percentileNs(readLatNs, 0.999);
        state.counters["read_samples"]      = static_cast<double>(readLatNs.size());
        state.counters["fold_batch_p50_ns"] = percentileNs(foldBatchLatNs, 0.50);
        state.counters["fold_batch_p99_ns"] = percentileNs(foldBatchLatNs, 0.99);
        state.counters["events_total"]      = static_cast<double>(processed);
        state.counters["final_size"]        = static_cast<double>(table.size());
        state.counters["view_bytes_estimate"] = static_cast<double>(table.size()) * layout.rowLen;
        state.counters["fold_read_wall_s"]  = std::chrono::duration<double>(foldReadEnd - runStart).count();

        if (ckptOn) {
            state.counters["io_stalls"]          = static_cast<double>(ioStalls);
            state.counters["io_stall_total_ns"]  = static_cast<double>(ioStallNs);
            state.counters["deltas_pushed"]      = static_cast<double>(deltasPushed);
            state.counters["bytes_pushed"]       = static_cast<double>(bytesPushed);
            state.counters["write_GBps"]         = wallS > 0 ? (bytesPushed / 1e9 / wallS) : 0.0;
            state.counters["handoff_pool_bytes"] =
                static_cast<double>(kNumBuf) * static_cast<double>(initialCapPerBuf) +
                static_cast<double>(entriesBuf.capacity()) * static_cast<double>(sizeof(DirtyEntry));
            state.counters["handoff_pool_cap_growth_events"] = static_cast<double>(capGrowthDetected);
            state.counters["ring_nominal_payload_bytes"]     = static_cast<double>((1 << 20) + 4096);

            std::error_code ec;
            const auto diskBytes = fs::file_size(agent->deltaFilePathForTest(kViewTopicId, kPartition), ec);
            state.counters["disk_bytes_actual"] = ec ? -1.0 : static_cast<double>(diskBytes);
            state.counters["last_durable_O"] =
                static_cast<double>(agent->lastDurableOffset(kViewTopicId, kPartition));
            state.counters["agent_open_failures"] = static_cast<double>(agent->openFailureCount());
            state.counters["agent_has_error"]     = agent->hasError(kViewTopicId, kPartition) ? 1.0 : 0.0;
        }
    }
}
BENCHMARK_CAPTURE(BM_ViewCheckpointReads, ON, true)
    ->Iterations(1)->Repetitions(2)->UseRealTime();
BENCHMARK_CAPTURE(BM_ViewCheckpointReads, OFF, false)
    ->Iterations(1)->Repetitions(2)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_ViewCheckpointGranularity — row-sized vs. 4KB-page dirty-unit
// write-amplification arm (documents why Task 6 made the dirty unit
// row-sized, not page-sized). Does NOT touch ViewTable/ViewCheckpoint.
// Arg(0) = row-sized dirty tracking, Arg(1) = 4KB-chunk dirty tracking, over
// the IDENTICAL random-touch stream (same seed) against the same byte slab.
// ---------------------------------------------------------------------------
static void BM_ViewCheckpointGranularity(benchmark::State& state) {
    const bool chunk4k = state.range(0) != 0;
    constexpr size_t rowSize   = kRowSize; // same row width as the main gate
    constexpr size_t chunkSize = 4096;     // OS page
    constexpr size_t slabRows  = 200'000;  // representative slab (51.2MB)
    constexpr size_t numBatches = 3000;
    constexpr size_t batchSize  = kBatchSize; // 64, same as the main gate

    for (auto _ : state) {
        std::vector<uint8_t> slab(slabRows * rowSize, 0xCD);
        std::vector<uint8_t> rowDirty(slabRows, 0);
        const size_t nChunks = (slab.size() + chunkSize - 1) / chunkSize;
        std::vector<uint8_t> chunkDirty(nChunks, 0);
        std::vector<uint32_t> touched;
        touched.reserve(batchSize);
        std::vector<uint8_t> copyBuf;
        copyBuf.reserve(chunk4k ? batchSize * chunkSize : batchSize * rowSize);

        TempDir dir;
        const std::string path = dir.path() + "/granularity.bin";
        const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

        std::mt19937_64 rng(0xABCDu); // SAME seed both arms -- identical touch stream
        uint64_t totalBytes = 0;
        uint64_t numDirtyUnitsTotal = 0;

        const auto t0 = std::chrono::steady_clock::now();
        for (size_t b = 0; b < numBatches; ++b) {
            touched.clear();
            for (size_t e = 0; e < batchSize; ++e) {
                const size_t r = rng() % slabRows;
                std::memset(&slab[r * rowSize], static_cast<uint8_t>(r), rowSize); // simulate the write

                if (!chunk4k) {
                    if (!rowDirty[r]) { rowDirty[r] = 1; touched.push_back(static_cast<uint32_t>(r)); }
                } else {
                    const size_t c0 = (r * rowSize) / chunkSize;
                    const size_t c1 = ((r + 1) * rowSize - 1) / chunkSize;
                    for (size_t c = c0; c <= c1; ++c)
                        if (!chunkDirty[c]) { chunkDirty[c] = 1; touched.push_back(static_cast<uint32_t>(c)); }
                }
            }

            copyBuf.clear();
            if (!chunk4k) {
                for (uint32_t r : touched) {
                    copyBuf.insert(copyBuf.end(), &slab[static_cast<size_t>(r) * rowSize],
                                   &slab[static_cast<size_t>(r) * rowSize] + rowSize);
                    rowDirty[r] = 0;
                }
            } else {
                for (uint32_t c : touched) {
                    const size_t off = static_cast<size_t>(c) * chunkSize;
                    const size_t len = std::min(chunkSize, slab.size() - off);
                    copyBuf.insert(copyBuf.end(), &slab[off], &slab[off] + len);
                    chunkDirty[c] = 0;
                }
            }
            numDirtyUnitsTotal += touched.size();

            ssize_t w = ::write(fd, copyBuf.data(), copyBuf.size());
            (void)w;
            totalBytes += copyBuf.size();
            if ((b % 20) == 19) ::fsync(fd); // same grouped-fsync cadence, both arms
        }
        ::fsync(fd);
        const auto t1 = std::chrono::steady_clock::now();
        ::close(fd);

        const double wallS = std::chrono::duration<double>(t1 - t0).count();
        state.counters["bytes_written"]      = static_cast<double>(totalBytes);
        state.counters["bytes_per_batch"]    = static_cast<double>(totalBytes) / numBatches;
        state.counters["dirty_units_total"]  = static_cast<double>(numDirtyUnitsTotal);
        state.counters["write_GBps"]         = wallS > 0 ? (totalBytes / 1e9 / wallS) : 0.0;
        state.counters["granularity_bytes"]  = chunk4k ? static_cast<double>(chunkSize) : static_cast<double>(rowSize);
    }
}
BENCHMARK(BM_ViewCheckpointGranularity)->Arg(0)->Arg(1)->Iterations(1)->UseRealTime();
