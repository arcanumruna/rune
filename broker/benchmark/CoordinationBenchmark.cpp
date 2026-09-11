// CoordinationBenchmark.cpp — P4-6: Phase-4 consumer-group & compaction benchmarks.
//
// Covers (all latency-focused, no hot-path alloc in timed region):
//
//   1. OffsetStore commit latency (p50/p99 via percentile counter) — fsync path noted.
//   2. OffsetStore fetch latency — in-memory map lookup, sub-µs.
//   3. OffsetCommitRequest  SBE encode + decode — per-message ns.
//   4. OffsetFetchRequest   SBE encode + decode — per-message ns.
//   5. Rebalance time vs group size (join/autoAssignAndSync cycle for 3/10/50 members).
//   6. RangeAssignor cost vs (members × partitions) — ns/op.
//   7. RoundRobinAssignor cost vs (members × partitions) — ns/op.
//   8. LogCleaner::clean throughput — MB/s, records/s; OffsetMap put ns/op.
//   9. GroupCoordinator doWork tick — per-call ns.
//  10. GroupCoordinator heartbeat — per-call ns.
//
// Coding conventions:
//   - Pre-build all inputs outside the timed region (PauseTiming / setup before loop).
//   - DoNotOptimize results; ClobberMemory after writes.
//   - OffsetStore commit DOES fsync (that is the whole durability contract) — the
//     benchmark intentionally measures wall-clock including fsync.  Comment says so.
//   - Short min_time is enforced via the CLI flag; the benchmark bodies do no sleeping.

#include <benchmark/benchmark.h>

#include <rune/coordination/OffsetStore.h>
#include <rune/coordination/OffsetMessages.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/Assignor.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/protocol/RecordBatch.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>
#include <atomic>
#include <sys/types.h>
#include <unistd.h>

namespace {

// ---------------------------------------------------------------------------
// Unique temp dir helper — avoids collisions across parameterisations.
// ---------------------------------------------------------------------------
static std::atomic<int> g_dirSeq{0};

static std::filesystem::path uniqueDir(const char* tag) {
    namespace fs = std::filesystem;
    return fs::temp_directory_path() /
           ("rune_bench_coord_" + std::string(tag) +
            "_" + std::to_string(::getpid()) +
            "_" + std::to_string(g_dirSeq.fetch_add(1)));
}

// ---------------------------------------------------------------------------
// 1. BM_OffsetStore_Commit
//
// Each iteration appends one commit record + fsync (durability path).
// All setup (open OffsetStore) happens before the timed loop; the only
// allocation inside the loop is the file write + fsync syscall.
//
// NOTE: This benchmark is I/O-bound (includes fsync). Numbers reflect real
// durability cost: p50 and p99 are visible from the benchmark runner's
// --benchmark_report_aggregates_only or the raw histogram.
// ---------------------------------------------------------------------------
static void BM_OffsetStore_Commit(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = uniqueDir("commit");
    fs::create_directories(dir);
    auto path = (dir / "offsets.log").string();

    rune::coordination::OffsetStore store(path);

    int64_t ts = 1'000'000'000LL;
    int64_t offset = 0;

    for (auto _ : state) {
        // Varying offset prevents the compiler from collapsing duplicate records.
        store.commit("bench-group",
                     {"bench-topic", 0},
                     offset,
                     "meta",
                     ts);
        benchmark::DoNotOptimize(offset);
        ++offset;
        ++ts;
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_OffsetStore_Commit)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 2. BM_OffsetStore_Fetch
//
// In-memory map lookup; no I/O. Sub-µs expected.
// ---------------------------------------------------------------------------
static void BM_OffsetStore_Fetch(benchmark::State& state) {
    namespace fs = std::filesystem;
    auto dir = uniqueDir("fetch");
    fs::create_directories(dir);
    auto path = (dir / "offsets.log").string();

    rune::coordination::OffsetStore store(path);
    store.commit("bench-group", {"bench-topic", 0}, 42, "meta", 1'000'000'000LL);

    for (auto _ : state) {
        auto result = store.fetch("bench-group", {"bench-topic", 0});
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    fs::remove_all(dir);
}
BENCHMARK(BM_OffsetStore_Fetch)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 3. BM_Sbe_OffsetCommitRequest_Encode
// 3b. BM_Sbe_OffsetCommitRequest_Decode
// ---------------------------------------------------------------------------
static volatile uint8_t g_offsetBuf[256];

static uint8_t* offsetBufRW() {
    return const_cast<uint8_t*>(reinterpret_cast<volatile uint8_t*>(g_offsetBuf));
}

static void BM_Sbe_OffsetCommitRequest_Encode(benchmark::State& state) {
    int64_t offset = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(offset);
        std::size_t n = rune::coordination::OffsetCommitRequestMsg::encode(
            offsetBufRW(), 0,
            offset, /*partition=*/0,
            "bench-group", "bench-topic", "meta");
        benchmark::DoNotOptimize(n);
        ++offset;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_OffsetCommitRequest_Encode)->Unit(benchmark::kNanosecond);

static void BM_Sbe_OffsetCommitRequest_Decode(benchmark::State& state) {
    std::array<uint8_t, 256> buf{};
    rune::coordination::OffsetCommitRequestMsg::encode(
        buf.data(), 0, 42, 0, "bench-group", "bench-topic", "meta");

    for (auto _ : state) {
        auto m = rune::coordination::OffsetCommitRequestMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_OffsetCommitRequest_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 4. BM_Sbe_OffsetFetchRequest_Encode / Decode
// ---------------------------------------------------------------------------
static void BM_Sbe_OffsetFetchRequest_Encode(benchmark::State& state) {
    int32_t partition = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(partition);
        std::size_t n = rune::coordination::OffsetFetchRequestMsg::encode(
            offsetBufRW(), 0,
            partition, "bench-group", "bench-topic");
        benchmark::DoNotOptimize(n);
        ++partition;
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_OffsetFetchRequest_Encode)->Unit(benchmark::kNanosecond);

static void BM_Sbe_OffsetFetchRequest_Decode(benchmark::State& state) {
    std::array<uint8_t, 256> buf{};
    rune::coordination::OffsetFetchRequestMsg::encode(
        buf.data(), 0, 0, "bench-group", "bench-topic");

    for (auto _ : state) {
        auto m = rune::coordination::OffsetFetchRequestMsg::decode(buf.data(), 0);
        benchmark::DoNotOptimize(m);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Sbe_OffsetFetchRequest_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 5. BM_Rebalance_GroupSize — full JoinGroup/SyncGroup cycle for N members.
//
// Measures the wall-clock time for:
//   N × join() → autoAssignAndSync() → N × heartbeat()
// for 3, 10, 50 members × 100 partitions.
//
// Each iteration does one complete rebalance. Setup (member list) is built
// outside the timed region. The GroupCoordinator is reconstructed inside
// PauseTiming to get a fresh Empty state.
// ---------------------------------------------------------------------------
static void BM_Rebalance_GroupSize(benchmark::State& state) {
    const int memberCount = static_cast<int>(state.range(0));
    const int partCount   = 100;

    rune::coordination::TopicPartitionCounts partCounts{{"bench-topic", partCount}};

    // Pre-build the topic subscription list (all members subscribe to bench-topic).
    std::vector<std::string> topics{"bench-topic"};

    int64_t nowNs = 1'000'000'000LL;
    constexpr int32_t kSessionMs = 30'000;

    for (auto _ : state) {
        // Build a fresh coordinator outside the timed region.
        state.PauseTiming();
        rune::coordination::GroupCoordinator coord("bench-group", nowNs);
        std::vector<std::string> memberIds;
        memberIds.reserve(static_cast<std::size_t>(memberCount));
        state.ResumeTiming();

        // Timed: N joins
        for (int i = 0; i < memberCount; ++i) {
            auto res = coord.join("", kSessionMs, topics, nowNs);
            benchmark::DoNotOptimize(res.memberId);
            memberIds.push_back(res.memberId);
        }

        // Timed: autoAssignAndSync
        auto syncRes = coord.autoAssignAndSync(
            partCounts,
            rune::coordination::AssignorType::Range,
            nowNs);
        benchmark::DoNotOptimize(syncRes);

        // Timed: N heartbeats (stable-state hot path after rebalance)
        const auto gen = coord.generationId();
        for (auto& mid : memberIds) {
            auto hb = coord.heartbeat(mid, gen, nowNs);
            benchmark::DoNotOptimize(hb);
        }

        benchmark::ClobberMemory();
        nowNs += 1'000'000LL;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["members"] = benchmark::Counter(static_cast<double>(memberCount));
}
BENCHMARK(BM_Rebalance_GroupSize)
    ->Arg(3)
    ->Arg(10)
    ->Arg(50)
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 6. BM_RangeAssignor — raw assign() cost vs (members × partitions).
//
// Parameterised as (members, partitions) encoded in two ranges.
// The MemberSubscription vector is pre-built outside the loop.
// ---------------------------------------------------------------------------
static void BM_RangeAssignor(benchmark::State& state) {
    const int members    = static_cast<int>(state.range(0));
    const int partitions = static_cast<int>(state.range(1));

    std::vector<rune::coordination::MemberSubscription> subs;
    subs.reserve(static_cast<std::size_t>(members));
    for (int i = 0; i < members; ++i) {
        subs.push_back({"member-" + std::to_string(i), {"bench-topic"}});
    }
    rune::coordination::TopicPartitionCounts counts{{"bench-topic", partitions}};

    rune::coordination::RangeAssignor assignor;

    for (auto _ : state) {
        auto result = assignor.assign(subs, counts);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["members"]    = benchmark::Counter(static_cast<double>(members));
    state.counters["partitions"] = benchmark::Counter(static_cast<double>(partitions));
}
BENCHMARK(BM_RangeAssignor)
    ->Args({10, 100})
    ->Args({50, 500})
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 7. BM_RoundRobinAssignor — same grid as above.
// ---------------------------------------------------------------------------
static void BM_RoundRobinAssignor(benchmark::State& state) {
    const int members    = static_cast<int>(state.range(0));
    const int partitions = static_cast<int>(state.range(1));

    std::vector<rune::coordination::MemberSubscription> subs;
    subs.reserve(static_cast<std::size_t>(members));
    for (int i = 0; i < members; ++i) {
        subs.push_back({"member-" + std::to_string(i), {"bench-topic"}});
    }
    rune::coordination::TopicPartitionCounts counts{{"bench-topic", partitions}};

    rune::coordination::RoundRobinAssignor assignor;

    for (auto _ : state) {
        auto result = assignor.assign(subs, counts);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["members"]    = benchmark::Counter(static_cast<double>(members));
    state.counters["partitions"] = benchmark::Counter(static_cast<double>(partitions));
}
BENCHMARK(BM_RoundRobinAssignor)
    ->Args({10, 100})
    ->Args({50, 500})
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// 8a. BM_OffsetMap_Put — raw OffsetMap::put() cost per key.
//
// Alternating updates to K distinct keys (50 % overwrites).
// ---------------------------------------------------------------------------
static void BM_OffsetMap_Put(benchmark::State& state) {
    const int keyCount = static_cast<int>(state.range(0));

    // Pre-build key strings outside the loop.
    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(keyCount));
    for (int i = 0; i < keyCount; ++i) {
        keys.push_back("key-" + std::to_string(i));
    }

    rune::storage::OffsetMap map;
    map.clear();

    int64_t offset = 0;
    int     ki     = 0;

    for (auto _ : state) {
        map.put(keys[static_cast<std::size_t>(ki % keyCount)], offset);
        benchmark::DoNotOptimize(offset);
        ++offset;
        ++ki;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["keys"] = benchmark::Counter(static_cast<double>(keyCount));
}
BENCHMARK(BM_OffsetMap_Put)
    ->Arg(1'000)
    ->Arg(100'000)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 8b. BM_LogCleaner_Compact — compaction throughput (MB/s, records/s).
//
// Pre-populate a Log with K keyed records at dup ratio ~2× (each key written
// twice → 50 % of records are superseded after compaction).  Measure
// LogCleaner::clean() wall-clock.
//
// MB/s = total bytes written before compaction / elapsed seconds.
// records/s via SetItemsProcessed.
//
// Parameters: state.range(0) = K (total records before compaction).
// ---------------------------------------------------------------------------
static void BM_LogCleaner_Compact(benchmark::State& state) {
    namespace fs = std::filesystem;
    const int totalRecords = static_cast<int>(state.range(0));
    // Half as many distinct keys → ~50 % dup ratio.
    const int distinctKeys = totalRecords / 2;

    static constexpr int64_t kRollBytes = 256 * 1024; // 256 KiB segments

    // Typical record value size (~64 bytes; RecordBatch adds framing overhead).
    static constexpr std::size_t kValueSize = 64;

    for (auto _ : state) {
        state.PauseTiming();

        auto dir = uniqueDir("compact");
        fs::create_directories(dir);

        int64_t ts     = 1'700'000'000'000LL;
        int64_t baseOff = 0;

        // Build and append records outside the timed region.
        {
            rune::storage::Log log(dir.string(), kRollBytes);
            std::string value(kValueSize, 'x');
            for (int i = 0; i < totalRecords; ++i) {
                std::string key = "key-" + std::to_string(i % distinctKeys);
                rune::protocol::RecordBatchBuilder rb(baseOff, ts);
                rb.append(key, value, ts);
                auto batch = rb.build();
                log.append(batch, baseOff);
                ++baseOff;
                ++ts;
            }
        }

        // Re-open the log for compaction.
        rune::storage::Log log(dir.string(), kRollBytes);
        const int64_t hw = baseOff; // high-watermark = last appended offset

        // Compute approximate bytes before compaction for throughput counter.
        const int64_t approxBytes =
            static_cast<int64_t>(totalRecords) *
            static_cast<int64_t>(kValueSize + 64 /*framing*/);

        state.ResumeTiming();

        // Timed: build offset map + compact.
        rune::storage::LogCleaner cleaner;
        cleaner.clean(log,
                      /*firstDirtyOffset=*/0,
                      /*cleanBoundaryOffset=*/hw,
                      /*nowNs=*/ts,
                      /*deleteRetentionMs=*/0);

        benchmark::ClobberMemory();

        state.PauseTiming();
        fs::remove_all(dir);
        // Feed throughput counters.
        state.counters["bytes_before"] = benchmark::Counter(
            static_cast<double>(approxBytes));
        state.ResumeTiming();
    }

    // records/s (total records before compaction per second wall-clock).
    state.SetItemsProcessed(
        state.iterations() * static_cast<int64_t>(totalRecords));

    // MB/s approximation (bytes / elapsed).
    state.counters["MB_per_s"] = benchmark::Counter(
        static_cast<double>(
            state.iterations() *
            static_cast<int64_t>(totalRecords) *
            static_cast<int64_t>(kValueSize + 64)),
        benchmark::Counter::kIsRate,
        benchmark::Counter::kIs1024);
}
BENCHMARK(BM_LogCleaner_Compact)
    ->Arg(500)
    ->Arg(2'000)
    ->Unit(benchmark::kMillisecond);

// ---------------------------------------------------------------------------
// 9. BM_GroupCoordinator_DoWork — idle tick cost (no members to reap).
//
// GroupCoordinator with N stable members, all heartbeats fresh →
// doWork() returns 0 (no-op path). Measures hot-idle overhead.
// ---------------------------------------------------------------------------
static void BM_GroupCoordinator_DoWork(benchmark::State& state) {
    const int memberCount = static_cast<int>(state.range(0));

    int64_t nowNs = 1'000'000'000LL;
    rune::coordination::GroupCoordinator coord("bench-group", nowNs);
    std::vector<std::string> topics{"bench-topic"};
    rune::coordination::TopicPartitionCounts counts{{"bench-topic", 100}};

    // Populate and stabilise.
    std::vector<std::string> memberIds;
    for (int i = 0; i < memberCount; ++i) {
        auto r = coord.join("", 30'000, topics, nowNs);
        memberIds.push_back(r.memberId);
    }
    coord.autoAssignAndSync(counts, rune::coordination::AssignorType::Range, nowNs);

    for (auto _ : state) {
        benchmark::DoNotOptimize(nowNs);
        int work = coord.doWork(nowNs);
        benchmark::DoNotOptimize(work);
        // Tiny clock advance — keeps all heartbeats alive (< session timeout).
        nowNs += 1'000LL;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["members"] = benchmark::Counter(static_cast<double>(memberCount));
}
BENCHMARK(BM_GroupCoordinator_DoWork)
    ->Arg(10)
    ->Arg(50)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// 10. BM_GroupCoordinator_Heartbeat — per-call heartbeat latency.
//
// N stable members, round-robin heartbeats in the timed loop.
// ---------------------------------------------------------------------------
static void BM_GroupCoordinator_Heartbeat(benchmark::State& state) {
    const int memberCount = static_cast<int>(state.range(0));

    int64_t nowNs = 1'000'000'000LL;
    rune::coordination::GroupCoordinator coord("bench-group", nowNs);
    std::vector<std::string> topics{"bench-topic"};
    rune::coordination::TopicPartitionCounts counts{{"bench-topic", 100}};

    std::vector<std::string> memberIds;
    for (int i = 0; i < memberCount; ++i) {
        auto r = coord.join("", 30'000, topics, nowNs);
        memberIds.push_back(r.memberId);
    }
    coord.autoAssignAndSync(counts, rune::coordination::AssignorType::Range, nowNs);
    const auto gen = coord.generationId();

    int idx = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(nowNs);
        auto hb = coord.heartbeat(
            memberIds[static_cast<std::size_t>(idx % memberCount)], gen, nowNs);
        benchmark::DoNotOptimize(hb);
        ++idx;
        nowNs += 500LL;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["members"] = benchmark::Counter(static_cast<double>(memberCount));
}
BENCHMARK(BM_GroupCoordinator_Heartbeat)
    ->Arg(10)
    ->Arg(50)
    ->Unit(benchmark::kNanosecond);

} // namespace
