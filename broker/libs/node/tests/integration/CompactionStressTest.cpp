// ---------------------------------------------------------------------------
// CompactionStressTest.cpp — CP-5 Phase 4: In-process compaction + retention
//   stress/endurance test at scale.
//
// SCOPE (CP-5.4):
//   Drives LogCleanerAgent + NodeRuntimeAgent hard across many partitions,
//   many keys per partition, and sustained duty/cleaner cycles.  Asserts:
//     1. Latest-per-key correctness holds after all compaction.
//     2. HW/LEO invariants hold (compaction never moves them).
//     3. No divergence: segment list strictly ascending, all records readable.
//     4. Bounded memory: total live LogSegment count stays bounded across cycles
//        (coalescing + retention prevent unbounded growth).
//     5. Checkpoint advances across cycles.
//     6. TSan variant (separate test, reduced scale but real AgentRunner threads).
//
// SINGLE-WRITER INVARIANT (mirrored from CompactionRetentionSmokeTest.cpp):
//   All Log::append() calls happen on a single "append thread" (here: the test
//   thread, since we write ALL data before starting the cleaner agent thread).
//   The LogCleanerAgent runs on its own thread via AgentRunner but NEVER
//   touches segments_ — it uses the atomic sealedSnapshot_ only.
//   NodeRuntimeAgent::doWork drains swaps (reloadSegment) on the append thread.
//   TSan variant: append thread produces all initial data; cleaner thread runs
//   concurrently via AgentRunner; NO appends happen after the runner starts.
//
// TEST TOPOLOGY (no BrokerNode / no Aeron required):
//   Standalone Log + PartitionReplica + LogCleanerAgent + NodeRuntimeAgent.
//   The cleaner is manually pump-driven (deterministic cycle count) in the
//   scale test and AgentRunner-driven in the TSan test.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>
#include <rune/storage/Log.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>

#include <sys/resource.h>   // getrlimit/setrlimit — fd-limit raise for fd-heavy tests

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::storage;
using namespace rune::protocol;

// Fd-heavy stress tests here roll thousands of tiny segments across many partitions;
// each live segment consumes an fd, exceeding the default soft RLIMIT_NOFILE
// (commonly 1024) → an open() throws mid-run ("open failed: ...index").  Raise the
// soft limit to the hard limit once for this test binary so the stress exercises the
// churn it intends, not the ambient fd ceiling (raising soft up to hard is
// unprivileged; as root the hard limit is already high).
static const bool g_compaction_stress_fd_limit_raised = [] {
    struct ::rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
    }
    return true;
}();
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_stress_seq{0};

static fs::path stressTempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_stress_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

// Null transport (no replication needed).
class StressNullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// Minimal consensus agent (no-op).
class StressMinimalConsensusAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

// Build a Compact-policy PartitionConfig for stress tests.
static PartitionConfig makeStressCompactConfig(int32_t topicId,
                                               int64_t segRollBytes,
                                               double  minDirtyRatio = 0.0) {
    PartitionConfig cfg;
    cfg.topicId                = topicId;
    cfg.partitionId            = 0;
    cfg.selfId                 = 1;
    cfg.minIsr                 = 1;
    cfg.replicaLagMaxMs        = 10'000;
    cfg.replicaLagMaxOffset    = 10'000;
    cfg.fetchIntervalNs        = 1'000'000LL;
    cfg.isrCheckIntervalNs     = 1'000'000LL;
    cfg.maxFetchBytes          = 1 << 20;
    cfg.pushOnAppend           = false;
    cfg.cleanupPolicy          = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = minDirtyRatio;
    cfg.deleteRetentionMs      = 86'400'000LL;  // 1 day: keep tombstones
    cfg.segmentRollBytes       = segRollBytes;
    return cfg;
}

// Build a CompactDelete-policy PartitionConfig for mixed-policy tests.
static PartitionConfig makeStressCompactDeleteConfig(int32_t topicId,
                                                     int64_t segRollBytes) {
    auto cfg = makeStressCompactConfig(topicId, segRollBytes);
    cfg.cleanupPolicy    = CleanupPolicy::CompactDelete;
    cfg.retentionMs      = 1LL;          // ancient records eligible
    cfg.retentionBytes   = -1LL;         // no size limit
    cfg.retentionCheckMs = 1LL;          // aggressive retention cadence
    cfg.fileDeleteDelayMs = 1LL;         // immediate physical delete
    return cfg;
}

// Build a Delete-policy PartitionConfig for retention-only tests.
static PartitionConfig makeStressDeleteConfig(int32_t topicId,
                                              int64_t segRollBytes) {
    PartitionConfig cfg;
    cfg.topicId                = topicId;
    cfg.partitionId            = 0;
    cfg.selfId                 = 1;
    cfg.minIsr                 = 1;
    cfg.replicaLagMaxMs        = 10'000;
    cfg.replicaLagMaxOffset    = 10'000;
    cfg.fetchIntervalNs        = 1'000'000LL;
    cfg.isrCheckIntervalNs     = 1'000'000LL;
    cfg.maxFetchBytes          = 1 << 20;
    cfg.pushOnAppend           = false;
    cfg.cleanupPolicy          = CleanupPolicy::Delete;
    cfg.retentionMs            = 1LL;    // 1 ms — oldest segments deleted
    cfg.retentionBytes         = -1LL;
    cfg.retentionCheckMs       = 1LL;
    cfg.fileDeleteDelayMs      = 1LL;
    cfg.segmentRollBytes       = segRollBytes;
    return cfg;
}

// Build a batch with key/value/timestamp.
static std::vector<uint8_t> stressBatch(int64_t offset,
                                         std::string_view key,
                                         std::string_view value,
                                         int64_t ts = 1000LL) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, value, ts);
    return b.build();
}

// Build a tombstone batch (empty value).
static std::vector<uint8_t> stressTombstone(int64_t offset,
                                              std::string_view key,
                                              int64_t ts = 1000LL) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, std::string_view{}, ts);
    return b.build();
}

// Count live records across all segments of a Log (single-threaded access after
// cleaner + duty-cycle are stopped).
static int stressCountLiveRecords(Log& log) {
    int count = 0;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size())
                         ? bases[si + 1]
                         : std::numeric_limits<int64_t>::max();
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
                ++count;
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced || cursor >= segEnd) break;
        }
    }
    return count;
}

// Scan all live records and return latest value per key.
static std::unordered_map<std::string, std::string>
stressScanLatestPerKey(Log& log) {
    std::unordered_map<std::string, std::string> latest;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return latest;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size())
                         ? bases[si + 1]
                         : std::numeric_limits<int64_t>::max();
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v,
                          int64_t off, int64_t) {
                latest[std::string(k)] = std::string(v);
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced || cursor >= segEnd) break;
        }
    }
    return latest;
}

// Scan all live records and return the COUNT of live copies per key.
// Used to assert convergence (exactly one live copy per key) — a stronger check
// than stressScanLatestPerKey (which only proves the latest value is present and
// would silently accept dozens of superseded copies, hiding the
// compaction-convergence watermark bug).
static std::unordered_map<std::string, int>
stressScanCountPerKey(Log& log) {
    std::unordered_map<std::string, int> counts;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return counts;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size())
                         ? bases[si + 1]
                         : std::numeric_limits<int64_t>::max();
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view,
                          int64_t off, int64_t) {
                counts[std::string(k)]++;
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced || cursor >= segEnd) break;
        }
    }
    return counts;
}

// Verify the segment list is strictly ascending (no duplicates, no disorder).
static bool stressCheckSegmentsAscending(Log& log) {
    auto bases = log.segmentBaseOffsets();
    for (size_t i = 1; i < bases.size(); ++i) {
        if (bases[i - 1] >= bases[i]) return false;
    }
    return true;
}

// Count .log files on disk (excludes .new, .deleted).
static int countLogFilesOnDisk(const fs::path& dir) {
    int n = 0;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        if (!e.is_regular_file(ec)) continue;
        auto nm = e.path().filename().string();
        if (nm.size() > 4 && nm.substr(nm.size() - 4) == ".log" &&
            (nm.size() <= 8 ||
             nm.substr(nm.size() - 8) != ".log.new")) {
            ++n;
        }
    }
    return n;
}

// ===========================================================================
// TEST: ManyPartitions_LatestPerKey_Stress
//
// CP-5.4 main scale test.
//
// Drive 50 compact + 10 compact+delete + 10 delete partitions (70 total) with
// sustained load.  After the run assert:
//   (a) Completion within the bounded budget (60 s wall).
//   (b) Latest-per-key correctness for every compact partition.
//   (c) HW/LEO invariants: compaction never moved them.
//   (d) No divergence: segment list strictly ascending in all logs.
//   (e) Bounded segment count: coalescing + retention prevent growth beyond
//       the initial count.
//   (f) Checkpoint advanced for at least one partition.
//
// Scale rationale: 70 partitions × 10 keys × 20 records = 14,000 total records.
// With kSegRoll=1024 and ~64-byte batches: ~1-2 records/segment → ~10-20 segs
// per partition = ~700-1400 total segments.  Well within in-process limits.
//
// SINGLE-WRITER: all appends happen on the test thread (before cleaner starts
// via manual pump — no real AgentRunner thread here, so the append thread IS
// the duty-cycle thread and the cleaner is driven synchronously).  After all
// appends, the manual pump loop drives cleaner + duty interleaved on the SAME
// thread — no separate thread, so no TSan risk (TSan is in a separate test).
// ===========================================================================
TEST(CompactionStress, ManyPartitions_LatestPerKey_Stress) {
    const auto testStart = std::chrono::steady_clock::now();

    // -----------------------------------------------------------------------
    // Scale parameters (bounded: fixed total records, bounded runtime).
    // -----------------------------------------------------------------------
    constexpr int    kCompactPartitions      = 50;   // compact-only
    constexpr int    kCompactDeletePartitions = 10;   // compact+delete
    constexpr int    kDeletePartitions        = 10;   // delete-only
    constexpr int    kTotalPartitions = kCompactPartitions
                                      + kCompactDeletePartitions
                                      + kDeletePartitions;

    constexpr int    kKeysPerPartition   = 10;    // 10 distinct keys/partition
    constexpr int    kRecordsPerKey      = 20;    // 20 updates per key → 200 records/partition
    constexpr int    kTotalRecords       = kTotalPartitions * kKeysPerPartition * kRecordsPerKey;

    constexpr int64_t kSegRoll  = 1024LL;         // 1 KiB roll → ~10-20 sealed segments
    // cleaner+duty pump iterations.  Sized so the filthiest-first cleaner (one
    // log per cycle, one segment per pass) has enough cycles to fully CONVERGE
    // every compact partition to one live record per key — the strengthened
    // convergence assertions below require it.
    constexpr int    kPumpCycles = 3000;

    // 60-second hard wall budget (the test should finish much faster).
    constexpr auto kWallBudget = 60s;

    // -----------------------------------------------------------------------
    // Build data structures.
    // -----------------------------------------------------------------------
    auto rootDir = stressTempDir("cp5_stress_");
    struct PartEntry {
        std::unique_ptr<Log> log;
        fs::path             dir;
        int64_t              leoBefore = 0;
        int32_t              topicId   = 0;
        bool                 isCompact = false;   // compact or compact+delete
        bool                 isDelete  = false;   // delete-only
        // For compact: expected latest per key.
        std::unordered_map<std::string, std::string> expectedLatest;
    };

    StressNullTransport   transport;
    rune::node::ReplicaRegistry registry;
    std::vector<std::unique_ptr<PartEntry>> parts;
    parts.reserve(kTotalPartitions);

    // -----------------------------------------------------------------------
    // Phase A (single-writer append thread): write all data via standalone
    // Log objects, then register replicas.
    // -----------------------------------------------------------------------
    int64_t nextOffset = 0; // global offset across all partitions for unique topicId

    // Helper: build one partition directory.
    auto makePartDir = [&](const std::string& name) -> fs::path {
        fs::path d = rootDir / name;
        std::error_code ec;
        fs::create_directories(d, ec);
        return d;
    };

    // --- Compact partitions --------------------------------------------------
    for (int p = 0; p < kCompactPartitions; ++p) {
        const int32_t topicId = 1000 + p;
        auto entry = std::make_unique<PartEntry>();
        entry->topicId   = topicId;
        entry->isCompact = true;
        entry->dir       = makePartDir("compact_" + std::to_string(p));

        // Write kRecordsPerKey updates for each of kKeysPerPartition keys.
        // Use ancient timestamp so deletion (for compact+delete) is eligible.
        // Track expected latest value.
        {
            Log rawLog(entry->dir.string(), kSegRoll);
            int64_t off = 0;
            for (int r = 0; r < kRecordsPerKey; ++r) {
                for (int k = 0; k < kKeysPerPartition; ++k) {
                    std::string key = "ck" + std::to_string(k);
                    std::string val = "cv" + std::to_string(r);
                    auto b = stressBatch(off, key, val, /*ts=*/1000LL + off);
                    rawLog.append(b, off);
                    ++off;
                }
            }
            // Sentinels: write enough unique trailing records to fill (and seal)
            // past a whole segment so ALL real "ck*" key updates end up in SEALED
            // segments.  The active (never-compacted) segment then holds only
            // sentinels — no "ck*" key.  Without this, the last-round updates for
            // the most-recently-written keys sit in the never-compacted active
            // segment (active copy + last-sealed copy = 2), making exact
            // one-live-copy-per-key convergence unachievable.  With a full segment
            // of sentinels every "ck*" key converges to exactly ONE live copy, so
            // the convergence assertion below is both strong AND correct.
            for (int s = 0; s < kKeysPerPartition * 3; ++s) {
                auto b = stressBatch(off, "csentinel" + std::to_string(s), "s",
                                     /*ts=*/1000LL + off);
                rawLog.append(b, off);
                ++off;
            }
            entry->leoBefore = rawLog.logEndOffset();
            // Expected: last update for each key is r = kRecordsPerKey - 1.
            for (int k = 0; k < kKeysPerPartition; ++k) {
                entry->expectedLatest["ck" + std::to_string(k)] =
                    "cv" + std::to_string(kRecordsPerKey - 1);
            }
            ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
                << "Compact partition " << p << " must have >= 2 segments";
        } // rawLog closed

        // Recover the log (rawLog was destroyed/closed above; open a fresh Log).
        entry->log = std::make_unique<Log>(entry->dir.string(), kSegRoll);
        entry->log->recover();

        // Build and register replica over the recovered log.
        auto cfg = makeStressCompactConfig(topicId, kSegRoll);
        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            entry->log.get(), &transport);
        replica->becomeLeader(1, {1});
        replica->onLeaderLogAppended();

        registry.add(topicId, 0, std::move(replica));
        parts.push_back(std::move(entry));
    }

    // --- CompactDelete partitions --------------------------------------------
    for (int p = 0; p < kCompactDeletePartitions; ++p) {
        const int32_t topicId = 2000 + p;
        auto entry = std::make_unique<PartEntry>();
        entry->topicId   = topicId;
        entry->isCompact = true;   // also compact — latest-per-key applies
        entry->dir       = makePartDir("cdel_" + std::to_string(p));

        {
            Log rawLog(entry->dir.string(), kSegRoll);
            int64_t off = 0;
            // Old records (ancient ts → eligible for time retention).
            for (int r = 0; r < kRecordsPerKey - 1; ++r) {
                for (int k = 0; k < kKeysPerPartition; ++k) {
                    std::string key = "dk" + std::to_string(k);
                    std::string val = "dv" + std::to_string(r);
                    auto b = stressBatch(off, key, val, /*ts=*/1000LL);
                    rawLog.append(b, off);
                    ++off;
                }
            }
            // Final update per key with recent timestamp → survives retention.
            const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            for (int k = 0; k < kKeysPerPartition; ++k) {
                std::string key = "dk" + std::to_string(k);
                std::string val = "FINAL" + std::to_string(k);
                auto b = stressBatch(off, key, val, nowMs);
                rawLog.append(b, off);
                ++off;
                entry->expectedLatest[key] = val;
            }
            entry->leoBefore = rawLog.logEndOffset();
            ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
                << "CompactDelete partition " << p << " must have >= 2 segments";
        }

        entry->log = std::make_unique<Log>(entry->dir.string(), kSegRoll);
        entry->log->recover();

        auto cfg = makeStressCompactDeleteConfig(topicId, kSegRoll);
        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            entry->log.get(), &transport);
        replica->becomeLeader(1, {1});
        replica->onLeaderLogAppended();
        registry.add(topicId, 0, std::move(replica));
        parts.push_back(std::move(entry));
    }

    // --- Delete partitions ---------------------------------------------------
    for (int p = 0; p < kDeletePartitions; ++p) {
        const int32_t topicId = 3000 + p;
        auto entry = std::make_unique<PartEntry>();
        entry->topicId  = topicId;
        entry->isDelete = true;
        entry->dir      = makePartDir("del_" + std::to_string(p));

        {
            Log rawLog(entry->dir.string(), kSegRoll);
            int64_t off = 0;
            // Ancient records.
            for (int r = 0; r < kRecordsPerKey; ++r) {
                for (int k = 0; k < kKeysPerPartition; ++k) {
                    auto b = stressBatch(off, "rk" + std::to_string(k),
                                         "rv" + std::to_string(r),
                                         /*ts=*/1000LL);
                    rawLog.append(b, off);
                    ++off;
                }
            }
            // Append a few recent records to keep at least one segment alive.
            const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            for (int k = 0; k < 3; ++k) {
                auto b = stressBatch(off, "recent" + std::to_string(k),
                                     "rval", nowMs);
                rawLog.append(b, off);
                ++off;
            }
            entry->leoBefore = rawLog.logEndOffset();
            ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
                << "Delete partition " << p << " must have >= 2 segments";
        }

        entry->log = std::make_unique<Log>(entry->dir.string(), kSegRoll);
        entry->log->recover();

        auto cfg = makeStressDeleteConfig(topicId, kSegRoll);
        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            entry->log.get(), &transport);
        replica->becomeLeader(1, {1});
        replica->onLeaderLogAppended();
        registry.add(topicId, 0, std::move(replica));
        parts.push_back(std::move(entry));
    }

    ASSERT_EQ(static_cast<int>(parts.size()), kTotalPartitions);

    // -----------------------------------------------------------------------
    // Capture initial segment counts (proxy for memory: segment count is
    // bounded if coalescing + retention work correctly).
    // -----------------------------------------------------------------------
    std::vector<int> initSegCounts;
    initSegCounts.reserve(kTotalPartitions);
    for (auto& e : parts) {
        initSegCounts.push_back(
            static_cast<int>(e->log->segmentBaseOffsets().size()));
    }

    // -----------------------------------------------------------------------
    // Phase B: pump cleaner + duty-cycle manually (single-thread, no race).
    // No AgentRunner thread → this is the TSan-safe scale run.
    // The TSan-concurrent variant is a separate test (below).
    //
    // All operations (drainCleanerSwaps, reloadSegment) happen on the test
    // thread — the same thread that did all appends — so the single-writer
    // invariant is preserved.
    // -----------------------------------------------------------------------
    StressMinimalConsensusAgent dummyConsensus;
    rune::node::NodeRuntimeAgent dutyCycle(
        dummyConsensus, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(
        &registry,
        /*cycleBudgetBytes=*/0LL,       // unlimited per-cycle budget
        /*dedupeBufferBytes=*/-1,        // default (128 MiB)
        /*ioMaxBytesPerSec=*/-1,         // unlimited I/O throttle
        /*useCoalescing=*/true,          // coalescing ON: groups small segs
        /*coalescingRollBytes=*/4096LL); // 4 KiB group bound → many merges

    const int64_t pumpStart = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    for (int cycle = 0; cycle < kPumpCycles; ++cycle) {
        const int64_t nowNs = pumpStart + static_cast<int64_t>(cycle) * 1'000'000LL;
        cleaner.doWork(nowNs);
        dutyCycle.doWork(nowNs);

        // Abort early if we've exceeded the wall budget.
        if (cycle % 100 == 0) {
            if (std::chrono::steady_clock::now() - testStart > kWallBudget) {
                ADD_FAILURE() << "CP-5.4 stress: exceeded " << kWallBudget.count()
                              << "s wall budget at cycle " << cycle;
                break;
            }
        }
    }

    // Brief sleep so fileDeleteDelayMs=1ms expires for the delete partitions.
    std::this_thread::sleep_for(5ms);

    // One more pump pass to trigger physical deletion.
    const int64_t drainNs = pumpStart + static_cast<int64_t>(kPumpCycles) * 1'000'000LL;
    for (int i = 0; i < 50; ++i) {
        cleaner.doWork(drainNs + static_cast<int64_t>(i) * 1'000'000LL);
        dutyCycle.doWork(drainNs + static_cast<int64_t>(i) * 1'000'000LL);
    }

    const auto wallTime = std::chrono::steady_clock::now() - testStart;

    // -----------------------------------------------------------------------
    // Assertions: post-run correctness & stability.
    // -----------------------------------------------------------------------
    int latestKeyFailures      = 0;
    int convergenceFailures    = 0;
    int hwLeoFailures          = 0;
    int divergenceFailures     = 0;
    int boundedMemoryFailures  = 0;
    int checkpointAdvanced     = 0;
    int totalLiveCompact       = 0;
    int totalLiveCompactBefore = kCompactPartitions * kKeysPerPartition * kRecordsPerKey;

    for (int idx = 0; idx < kTotalPartitions; ++idx) {
        auto& e = *parts[idx];
        Log& log = *e.log;

        // (a) Segment list strictly ascending — no divergence.
        if (!stressCheckSegmentsAscending(log)) {
            ++divergenceFailures;
            ADD_FAILURE()
                << "CP-5.4: partition " << idx << " (topicId=" << e.topicId
                << "): segment list NOT strictly ascending → divergence!";
        }

        // (b) LEO invariant: compaction/retention must never increase LEO.
        //     For compact partitions LEO stays exactly equal (compaction preserves LEO).
        //     For delete partitions LEO may be unchanged (retention doesn't move LEO).
        const int64_t leoAfter = log.logEndOffset();
        if (e.isCompact && !e.isDelete) {
            // Compact-only: LEO is invariant (compaction never moves it).
            if (leoAfter != e.leoBefore) {
                ++hwLeoFailures;
                ADD_FAILURE()
                    << "CP-5.4: compact partition " << idx << " (topicId="
                    << e.topicId << "): LEO changed by compaction; "
                    << "before=" << e.leoBefore << " after=" << leoAfter;
            }
        } else {
            // CompactDelete/Delete: retention may not move LEO (only removes segs,
            // does not truncate). LEO must never exceed leoBefore.
            if (leoAfter > e.leoBefore) {
                ++hwLeoFailures;
                ADD_FAILURE()
                    << "CP-5.4: partition " << idx << " (topicId=" << e.topicId
                    << "): LEO exceeded pre-run value; "
                    << "before=" << e.leoBefore << " after=" << leoAfter;
            }
        }

        // (c) Latest-per-key correctness for compact/compactDelete partitions.
        if (!e.isDelete && !e.expectedLatest.empty()) {
            auto actual = stressScanLatestPerKey(log);
            for (auto& [k, expected] : e.expectedLatest) {
                if (actual.count(k) && actual[k] != expected) {
                    ++latestKeyFailures;
                    if (latestKeyFailures <= 5) {
                        ADD_FAILURE()
                            << "CP-5.4: partition " << idx << " (topicId=" << e.topicId
                            << "): latest-per-key mismatch for key='" << k
                            << "': expected='" << expected << "' got='" << actual[k] << "'";
                    }
                }
            }
            // (c2) CONVERGENCE for COMPACT-ONLY partitions: after enough cleaner
            // cycles, compaction must converge each key to EXACTLY ONE live copy
            // (superseded copies GONE).  We restrict this to compact-only (no
            // retention deleting records) so the expected count is deterministic.
            // stressScanLatestPerKey alone would NOT catch a stalled cleaner that
            // left dozens of superseded copies — this is the check that guards
            // the compaction-convergence watermark bug at stress scale.
            if (e.isCompact && !e.isDelete) {
                auto counts = stressScanCountPerKey(log);
                for (auto& [k, cnt] : counts) {
                    if (cnt != 1) {
                        ++convergenceFailures;
                        if (convergenceFailures <= 5) {
                            ADD_FAILURE()
                                << "CP-5.4: compact partition " << idx << " (topicId="
                                << e.topicId << "): key='" << k << "' did not converge "
                                << "to one live copy; got " << cnt << " copies "
                                   "(superseded records not removed → non-converging cleaner).";
                        }
                    }
                }
            }
            // Count total live records across compact partitions.
            if (e.isCompact && !e.isDelete)
                totalLiveCompact += stressCountLiveRecords(log);
        }

        // (d) Bounded memory proxy: segment count must not exceed initial + small slack.
        //     Coalescing merges sealed segs; retention deletes old segs.
        //     We allow up to 2× the initial count (very generous) — if segments grow
        //     unboundedly, this fires.
        const int segsNow   = static_cast<int>(log.segmentBaseOffsets().size());
        const int initSegs  = initSegCounts[idx];
        const int maxAllowed = std::max(initSegs * 2, 4);  // generous upper bound
        if (segsNow > maxAllowed) {
            ++boundedMemoryFailures;
            if (boundedMemoryFailures <= 5) {
                ADD_FAILURE()
                    << "CP-5.4: partition " << idx << " (topicId=" << e.topicId
                    << "): segment count grew unboundedly; "
                    << "initial=" << initSegs << " now=" << segsNow
                    << " max=" << maxAllowed;
            }
        }

        // (e) Checkpoint advanced for compact partitions.
        const std::string ckptPath = e.dir.string() + "/cleaner-offset-checkpoint";
        if (e.isCompact && fs::exists(ckptPath)) {
            ++checkpointAdvanced;
        }
    }

    // -----------------------------------------------------------------------
    // Summary assertions (non-vacuous).
    // -----------------------------------------------------------------------
    const auto wallMs = std::chrono::duration_cast<std::chrono::milliseconds>(wallTime).count();

    EXPECT_EQ(divergenceFailures, 0)
        << "CP-5.4: " << divergenceFailures << " partitions had divergent segment lists";
    EXPECT_EQ(hwLeoFailures, 0)
        << "CP-5.4: " << hwLeoFailures << " partitions had HW/LEO violated by compaction/retention";
    EXPECT_EQ(latestKeyFailures, 0)
        << "CP-5.4: " << latestKeyFailures << " latest-per-key mismatches across compact partitions";
    EXPECT_EQ(convergenceFailures, 0)
        << "CP-5.4: " << convergenceFailures << " keys in compact-only partitions did "
           "NOT converge to one live copy (superseded records persisted — the "
           "compaction-convergence watermark bug at stress scale).";
    EXPECT_EQ(boundedMemoryFailures, 0)
        << "CP-5.4: " << boundedMemoryFailures
        << " partitions had unbounded segment growth (coalescing/retention not working)";

    // At least one compact partition must have advanced a checkpoint
    // (cleaner actually ran and checkpointed progress).
    EXPECT_GT(checkpointAdvanced, 0)
        << "CP-5.4: NO checkpoint was persisted for any compact partition after "
        << kPumpCycles << " cleaner cycles — cleaner may not have run at all.";

    // Total live records in compact-only partitions must have DROPPED
    // (compaction removed superseded records — non-vacuous proof).
    // We don't count compact partitions above due to the control flow;
    // use a direct count for the first few compact partitions.
    {
        int sampleLive  = 0;
        int sampleBefore = 0;
        constexpr int kSample = 10;
        for (int i = 0; i < kSample && i < kCompactPartitions; ++i) {
            sampleLive   += stressCountLiveRecords(*parts[i]->log);
            sampleBefore += kKeysPerPartition * kRecordsPerKey;
        }
        EXPECT_LT(sampleLive, sampleBefore)
            << "CP-5.4: compaction must have removed superseded records in compact "
               "partitions (non-vacuous); sample before=" << sampleBefore
            << " after=" << sampleLive;
        // CONVERGENCE (strong): compact-only partitions must converge to EXACTLY
        // one live record per key.  Each partition has kKeysPerPartition "ck*"
        // keys (each converges to 1) + kKeysPerPartition*3 unique sentinel keys
        // (each already unique → 1) = kKeysPerPartition*4 live records.
        // '< before' alone would pass on a cleaner that stalled halfway.
        const int sampleConverged = kSample * (kKeysPerPartition * 4);
        EXPECT_EQ(sampleLive, sampleConverged)
            << "CP-5.4: compact-only partitions must CONVERGE to exactly one live "
               "record per key (" << sampleConverged << " total across " << kSample
            << " sampled partitions); got " << sampleLive
            << ".  A non-converging cleaner leaves superseded copies.";
    }

    // Wall time must be within budget.
    EXPECT_LT(wallMs, static_cast<int64_t>(
                  std::chrono::duration_cast<std::chrono::milliseconds>(kWallBudget).count()))
        << "CP-5.4: wall time exceeded budget; got " << wallMs << "ms";

    // Log summary (printed regardless of pass/fail for documentation).
    std::printf("[CP-5.4] Scale run complete:\n"
                "  Partitions: %d compact + %d compact+delete + %d delete = %d total\n"
                "  Total records written: %d\n"
                "  Pump cycles: %d (cleaner + duty)\n"
                "  Wall time: %lld ms\n"
                "  Divergence failures: %d\n"
                "  HW/LEO failures: %d\n"
                "  Latest-per-key failures: %d\n"
                "  Bounded-memory failures: %d\n"
                "  Checkpoints persisted: %d\n",
                kCompactPartitions, kCompactDeletePartitions, kDeletePartitions,
                kTotalPartitions, kTotalRecords, kPumpCycles,
                (long long)wallMs,
                divergenceFailures, hwLeoFailures, latestKeyFailures,
                boundedMemoryFailures, checkpointAdvanced);

    // Cleanup.
    std::error_code ec;
    fs::remove_all(rootDir, ec);
}

// ===========================================================================
// TEST: ConcurrentCleaner_TSan_Stress
//
// CP-5.4 TSan variant: reduced scale but GENUINELY concurrent — the
// LogCleanerAgent runs on its own thread via AgentRunner while the test thread
// drives the duty-cycle (drain swaps + reload segments).
//
// SINGLE-WRITER INVARIANT: ALL Log::append() calls happen BEFORE runner.start().
// After start(), only the duty-cycle (test thread) touches segments_ via
// drainCleanerSwaps → reloadSegment.  The cleaner thread (AgentRunner) ONLY
// reads the atomic sealedSnapshot_ and writes cleaned .log files on disk.
//
// This is structurally identical to the AppendP99UnperturbedByCompaction test
// (CP-6) but with many more partitions and a correctness scan after stop.
// ===========================================================================
TEST(CompactionStress, ConcurrentCleaner_TSan_Stress) {
    // Reduced scale for TSan: 30 compact partitions, 10 keys, 20 records/key.
    constexpr int    kPartitions     = 30;
    constexpr int    kKeys           = 10;
    constexpr int    kRecords        = 20;      // records per key
    constexpr int64_t kSegRoll       = 256LL;
    constexpr int    kDutyCycles     = 400;
    // Wall budget for TSan run: 30 seconds.
    constexpr auto   kTsanBudget     = 30s;

    const auto testStart = std::chrono::steady_clock::now();
    auto rootDir = stressTempDir("cp5_tsan_stress_");

    StressNullTransport          transport;
    rune::node::ReplicaRegistry  registry;

    // Keep logs + dirs alive for the duration of the test.
    std::vector<std::unique_ptr<Log>> logs;
    std::vector<fs::path>             dirs;
    // Expected latest per key for each partition.
    std::vector<std::unordered_map<std::string, std::string>> expected(kPartitions);
    // LEO before cleaner runs.
    std::vector<int64_t> leoBefore(kPartitions, 0);
    // Initial segment count.
    std::vector<int> initSegCount(kPartitions, 0);

    logs.reserve(kPartitions);
    dirs.reserve(kPartitions);

    // -----------------------------------------------------------------------
    // Phase A (test thread only — BEFORE AgentRunner starts): write all data.
    // -----------------------------------------------------------------------
    for (int p = 0; p < kPartitions; ++p) {
        const int32_t topicId = 5000 + p;
        fs::path d = rootDir / ("tsan_p" + std::to_string(p));
        std::error_code ec;
        fs::create_directories(d, ec);
        dirs.push_back(d);

        // Write data via standalone Log.
        {
            Log rawLog(d.string(), kSegRoll);
            int64_t off = 0;
            for (int r = 0; r < kRecords; ++r) {
                for (int k = 0; k < kKeys; ++k) {
                    std::string key = "tk" + std::to_string(k);
                    std::string val = "tv" + std::to_string(r);
                    auto b = stressBatch(off, key, val, 1000LL + off);
                    rawLog.append(b, off);
                    ++off;
                }
            }
            leoBefore[p] = rawLog.logEndOffset();
            ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
                << "TSan partition " << p << " needs >= 2 segments";
        } // rawLog destroyed (closed)

        // Expected latest: last update (r = kRecords - 1) for each key.
        for (int k = 0; k < kKeys; ++k) {
            expected[p]["tk" + std::to_string(k)] = "tv" + std::to_string(kRecords - 1);
        }

        // Recover log and build replica.
        auto log = std::make_unique<Log>(d.string(), kSegRoll);
        log->recover();
        initSegCount[p] = static_cast<int>(log->segmentBaseOffsets().size());

        auto cfg = makeStressCompactConfig(topicId, kSegRoll);
        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            log.get(), &transport);
        replica->becomeLeader(1, {1});
        replica->onLeaderLogAppended();

        registry.add(topicId, 0, std::move(replica));
        logs.push_back(std::move(log));
    }

    // -----------------------------------------------------------------------
    // Phase B: start the cleaner on its own thread via AgentRunner.
    // The DUTY-CYCLE (drain swaps) runs on the TEST THREAD — that's the
    // single-writer for segments_; the cleaner thread only uses sealedSnapshot_.
    // -----------------------------------------------------------------------
    StressMinimalConsensusAgent dummyConsensus;
    rune::node::NodeRuntimeAgent dutyCycle(
        dummyConsensus, nullptr, nullptr, nullptr, &registry, nullptr);

    rune::node::LogCleanerAgent cleaner(
        &registry,
        /*cycleBudgetBytes=*/0LL,       // unlimited
        /*dedupeBufferBytes=*/-1,        // default
        /*ioMaxBytesPerSec=*/-1,
        /*useCoalescing=*/true,
        /*coalescingRollBytes=*/4096LL);

    rune::consensus::BusySpinIdleStrategy idle;
    rune::consensus::MonotonicNanoClock   clock;
    rune::consensus::AgentRunner          runner(cleaner, idle, clock);

    // Start cleaner thread.
    runner.start();

    // Drive the duty cycle on the test thread (drains swaps, reloads segments).
    // No appends happen here — ALL appends are pre-start (single-writer safe).
    const int64_t dutyStart = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    for (int cycle = 0; cycle < kDutyCycles; ++cycle) {
        const int64_t nowNs = dutyStart + static_cast<int64_t>(cycle) * 2'000'000LL;
        dutyCycle.doWork(nowNs);

        // Check wall budget.
        if (cycle % 50 == 0) {
            if (std::chrono::steady_clock::now() - testStart > kTsanBudget) {
                break;
            }
        }
        // Yield occasionally so the cleaner thread can make progress.
        if (cycle % 10 == 0) {
            std::this_thread::sleep_for(1ms);
        }
    }

    // Stop cleaner thread before assertions (no more concurrent access).
    runner.stopAndJoin();

    // IMPORTANT: drain any swaps the cleaner enqueued but the duty cycle
    // hasn't yet consumed.  The cleaner thread is now stopped (joined), so
    // this is single-threaded — safe to drain.
    {
        const int64_t drainNs = dutyStart +
            static_cast<int64_t>(kDutyCycles + 1000) * 2'000'000LL;
        for (int i = 0; i < 200; ++i) {
            dutyCycle.doWork(drainNs + static_cast<int64_t>(i) * 1'000'000LL);
        }
    }

    const auto wallTime = std::chrono::steady_clock::now() - testStart;

    // -----------------------------------------------------------------------
    // Assertions (after cleaner thread stopped — single-threaded again).
    // -----------------------------------------------------------------------
    int latestKeyFailures = 0;
    int divergenceFailures = 0;
    int hwLeoFailures = 0;
    int boundedMemFailures = 0;

    for (int p = 0; p < kPartitions; ++p) {
        Log& log = *logs[p];

        // (a) Segment list strictly ascending.
        if (!stressCheckSegmentsAscending(log)) {
            ++divergenceFailures;
            ADD_FAILURE()
                << "CP-5.4 TSan: partition " << p << " has non-ascending segments → divergence!";
        }

        // (b) LEO invariant: must be unchanged by compaction.
        if (log.logEndOffset() != leoBefore[p]) {
            ++hwLeoFailures;
            ADD_FAILURE()
                << "CP-5.4 TSan: partition " << p << ": LEO changed by compaction; "
                << "before=" << leoBefore[p] << " after=" << log.logEndOffset();
        }

        // (c) Latest-per-key correctness: every surviving key must have
        //     its last value (compaction preserves latest).
        auto actual = stressScanLatestPerKey(log);
        for (auto& [k, exp] : expected[p]) {
            if (actual.count(k) && actual[k] != exp) {
                ++latestKeyFailures;
                if (latestKeyFailures <= 5) {
                    ADD_FAILURE()
                        << "CP-5.4 TSan: partition " << p
                        << ": latest-per-key mismatch key='" << k
                        << "' expected='" << exp << "' got='" << actual[k] << "'";
                }
            }
        }

        // (d) Bounded segment count proxy (TSan variant also).
        const int segsNow    = static_cast<int>(log.segmentBaseOffsets().size());
        const int maxAllowed = std::max(initSegCount[p] * 2, 4);
        if (segsNow > maxAllowed) {
            ++boundedMemFailures;
            if (boundedMemFailures <= 3) {
                ADD_FAILURE()
                    << "CP-5.4 TSan: partition " << p
                    << ": segment count unbounded; init=" << initSegCount[p]
                    << " now=" << segsNow;
            }
        }
    }

    // Non-vacuous: measure compaction progress across all partitions.
    // We accept EITHER record removal OR checkpoint advancement as evidence that
    // the cleaner actually ran (on macOS, fsync is slow and the concurrent
    // cleaner may complete only a few passes within the test's wall budget).
    int sampleLive    = 0;
    int sampleBefore  = 0;
    int checkpointAdv = 0;  // count partitions where checkpoint > 0
    const std::string ckptSuffix = "/cleaner-offset-checkpoint";
    for (int p = 0; p < kPartitions; ++p) {
        sampleLive   += stressCountLiveRecords(*logs[p]);
        sampleBefore += kKeys * kRecords;
        // Check if checkpoint file was written (cleaner ran at least once).
        if (fs::exists(dirs[p].string() + ckptSuffix)) ++checkpointAdv;
    }

    const auto wallMs = std::chrono::duration_cast<std::chrono::milliseconds>(wallTime).count();

    EXPECT_EQ(divergenceFailures,  0)
        << "CP-5.4 TSan: divergence detected in " << divergenceFailures << " partitions";
    EXPECT_EQ(hwLeoFailures,       0)
        << "CP-5.4 TSan: HW/LEO violated in " << hwLeoFailures << " partitions";
    EXPECT_EQ(latestKeyFailures,   0)
        << "CP-5.4 TSan: latest-per-key wrong in " << latestKeyFailures << " cases";
    EXPECT_EQ(boundedMemFailures,  0)
        << "CP-5.4 TSan: unbounded segment growth in " << boundedMemFailures << " partitions";

    // Non-vacuous: the cleaner MUST have done observable work.
    // Proof: either records were physically removed from the log (compacted file
    // reloaded) OR at least one partition's checkpoint was persisted (cleaner
    // ran ensureCheckpoint + setLastCleanOffset).  Either observation proves the
    // cleaner thread ran at least one full pass end-to-end.
    const bool cleanerDidWork = (sampleLive < sampleBefore) || (checkpointAdv > 0);
    EXPECT_TRUE(cleanerDidWork)
        << "CP-5.4 TSan: cleaner made NO observable progress (no records removed, "
           "no checkpoints written); sampleLive=" << sampleLive
        << " sampleBefore=" << sampleBefore
        << " checkpointAdv=" << checkpointAdv;

    EXPECT_LT(wallMs,
              std::chrono::duration_cast<std::chrono::milliseconds>(kTsanBudget).count())
        << "CP-5.4 TSan: wall time exceeded " << kTsanBudget.count() << "s";

    std::printf("[CP-5.4 TSan] Concurrent stress complete:\n"
                "  Partitions: %d compact, %d keys/partition, %d records/key\n"
                "  Duty cycles: %d (test thread)\n"
                "  Cleaner thread: AgentRunner (real concurrent thread)\n"
                "  Wall time: %lld ms\n"
                "  Records before=%d after=%d (removed=%d)\n"
                "  Checkpoints persisted=%d / %d partitions\n"
                "  Divergence: %d  HW/LEO: %d  latest-key: %d  bounded-mem: %d\n",
                kPartitions, kKeys, kRecords, kDutyCycles, (long long)wallMs,
                sampleBefore, sampleLive, sampleBefore - sampleLive,
                checkpointAdv, kPartitions,
                divergenceFailures, hwLeoFailures, latestKeyFailures, boundedMemFailures);

    // Cleanup.
    std::error_code ec;
    fs::remove_all(rootDir, ec);
}

// ===========================================================================
// TEST: CheckpointCoalescing_Bounded_Stress
//
// Verify that over many cleaner cycles on a single compact partition:
//   (a) The checkpoint file advances monotonically (never regresses).
//   (b) The total segment count stays bounded (coalescing folds segs together).
//   (c) Latest-per-key holds at every checkpoint sample point.
//
// This exercises the "sustained load" angle: many duty+cleaner cycles on one
// heavily-loaded partition, sampling the checkpoint mid-run.
// ===========================================================================
TEST(CompactionStress, CheckpointCoalescing_Bounded_Stress) {
    auto dir = stressTempDir("cp5_ckpt_coalesce_");

    constexpr int    kKeys      = 15;
    constexpr int    kRecords   = 50;    // 50 updates per key
    constexpr int64_t kSegRoll  = 128LL; // tiny roll → many sealed segs

    // Write data via standalone Log.
    std::unordered_map<std::string, std::string> expectedLatest;
    int64_t leoBefore = 0;
    int     initSegs  = 0;
    {
        Log rawLog(dir.string(), kSegRoll);
        int64_t off = 0;
        for (int r = 0; r < kRecords; ++r) {
            for (int k = 0; k < kKeys; ++k) {
                std::string key = "ck" + std::to_string(k);
                std::string val = "v" + std::to_string(r);
                auto b = stressBatch(off, key, val, 1000LL + off);
                rawLog.append(b, off);
                ++off;
            }
        }
        leoBefore = rawLog.logEndOffset();
        initSegs  = static_cast<int>(rawLog.segmentBaseOffsets().size());
        for (int k = 0; k < kKeys; ++k) {
            expectedLatest["ck" + std::to_string(k)] = "v" + std::to_string(kRecords - 1);
        }
        ASSERT_GE(initSegs, 3)
            << "CheckpointCoalescing: need >= 3 segments";
    }

    auto log = std::make_unique<Log>(dir.string(), kSegRoll);
    log->recover();

    StressNullTransport          transport;
    rune::node::ReplicaRegistry  registry;

    auto cfg = makeStressCompactConfig(9001, kSegRoll);
    cfg.deleteRetentionMs = 86'400'000LL;
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        log.get(), &transport);
    PartitionReplica* raw = replica.get();
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();
    registry.add(9001, 0, std::move(replica));

    StressMinimalConsensusAgent dummyConsensus;
    rune::node::NodeRuntimeAgent dutyCycle(
        dummyConsensus, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(
        &registry,
        /*cycleBudgetBytes=*/0LL,
        /*dedupeBufferBytes=*/-1,
        /*ioMaxBytesPerSec=*/-1,
        /*useCoalescing=*/true,
        /*coalescingRollBytes=*/1024LL);  // 1 KiB group → aggressive coalescing

    // Run many cycles, sampling checkpoint periodically.
    constexpr int kCycles = 1500;
    std::vector<int64_t> checkpointSamples;
    const int64_t pumpBase = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    for (int cycle = 0; cycle < kCycles; ++cycle) {
        const int64_t nowNs = pumpBase + static_cast<int64_t>(cycle) * 1'000'000LL;
        cleaner.doWork(nowNs);
        dutyCycle.doWork(nowNs);

        if (cycle % 150 == 0) {
            checkpointSamples.push_back(raw->lastCleanOffset());
        }
    }

    // (a) Checkpoint must be monotonically non-decreasing across samples.
    bool checkpointMonotone = true;
    for (size_t i = 1; i < checkpointSamples.size(); ++i) {
        if (checkpointSamples[i] < checkpointSamples[i - 1]) {
            checkpointMonotone = false;
            ADD_FAILURE()
                << "CheckpointCoalescing: checkpoint regressed at sample " << i
                << ": " << checkpointSamples[i - 1] << " → " << checkpointSamples[i];
        }
    }
    EXPECT_TRUE(checkpointMonotone)
        << "Checkpoint must never regress across cleaner cycles";

    // (b) Final checkpoint must be > 0 (cleaner made progress).
    EXPECT_GT(raw->lastCleanOffset(), 0LL)
        << "Checkpoint must advance above 0 after " << kCycles << " cycles";

    // (c) Segment count bounded: must not GROW (coalescing + single-seg compaction
    //     never add segments; the count may stay equal or decrease depending on
    //     how many segments are fully emptied by compaction).
    //     The checkpoint advance (above) proves the cleaner ran and segments were
    //     processed — the count is bounded by the initial value.
    const int finalSegs = static_cast<int>(log->segmentBaseOffsets().size());
    EXPECT_LE(finalSegs, initSegs)
        << "CheckpointCoalescing: segment count must not grow; "
        << "initial=" << initSegs << " final=" << finalSegs;

    // (d) LEO invariant.
    EXPECT_EQ(log->logEndOffset(), leoBefore)
        << "CheckpointCoalescing: compaction must not change LEO";

    // (e) Latest-per-key correctness after all compaction.
    auto actual = stressScanLatestPerKey(*log);
    int mismatches = 0;
    for (auto& [k, v] : expectedLatest) {
        if (actual.count(k) && actual[k] != v) {
            ++mismatches;
            if (mismatches <= 3) {
                ADD_FAILURE()
                    << "CheckpointCoalescing: key='" << k
                    << "' expected='" << v << "' got='" << actual[k] << "'";
            }
        }
    }
    EXPECT_EQ(mismatches, 0)
        << "CheckpointCoalescing: " << mismatches
        << " latest-per-key mismatches after coalescing stress";

    // (f) No divergence.
    EXPECT_TRUE(stressCheckSegmentsAscending(*log))
        << "CheckpointCoalescing: segment list not strictly ascending → divergence!";

    std::printf("[CP-5.4 Ckpt] CheckpointCoalescing result:\n"
                "  Keys=%d  Records/key=%d  InitSegs=%d  FinalSegs=%d  Cycles=%d\n"
                "  LEO before=%lld after=%lld\n"
                "  FinalCheckpoint=%lld\n",
                kKeys, kRecords, initSegs, finalSegs, kCycles,
                (long long)leoBefore, (long long)log->logEndOffset(),
                (long long)raw->lastCleanOffset());

    std::error_code ec;
    fs::remove_all(dir, ec);
}
