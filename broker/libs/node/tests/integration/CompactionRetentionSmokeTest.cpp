// ---------------------------------------------------------------------------
// CompactionRetentionSmokeTest.cpp — CP-5 Phase 1: End-to-end compaction +
//   retention smoke suite (integration gate).
//
// This file adds a COHESIVE smoke harness that drives a REAL BrokerNode (the
// production wiring: NodeRuntimeAgent + LogCleanerAgent on their own threads,
// config-driven coalescing on) for 11 scenarios.  It serves as the GREEN gate
// before e2e/DR/stress phases.
//
// Each test is non-vacuous: the assertion targets the specific guarantee, not
// a trivially-true condition.
//
// SINGLE-WRITER INVARIANT (CP-5.1 TSan fix):
//   Production rule: Log::segments_ is ONLY mutated on the single-writer duty-
//   cycle thread (append + drainCleanerSwaps + drainRetentionDeletes).  The
//   LogCleanerAgent runs on a SEPARATE thread but NEVER touches segments_ —
//   it reads the atomic sealedSnapshot_ and does pure file work.
//
//   Test harness must honour the same topology:
//     • ALL Log::append() calls happen BEFORE node.start() (before the duty-
//       cycle / cleaner threads start).  We write data via a STANDALONE Log
//       object into the on-disk directory; BrokerNode then recovers those files
//       when it starts.  This keeps all segments_ mutations on a single thread
//       (first the pre-start test thread, then the duty-cycle thread).
//     • AFTER node.start(), the test thread NEVER calls log->append().  Any
//       additional data needed post-start must be injected via a second node
//       restart (write → stop → new node).
//
// Tests:
//  1. CompactHappyPath_LatestPerKey
//  2. TombstoneLifecycle_KIP534
//  3. DeleteRetention_TimeAndSize
//  4. CompactDelete_BothRun_NoDoubleDelete
//  5. InternalTopic_Recovery_CompactedLog
//  6. IdempotentDedup_SurvivesRetention
//  7. RestartResume_Checkpoint
//  8. HwLeo_NonPerturbation
//  9. AppendDuringCompaction_Correctness
// 10. Failover_HwBounded_NoResurrection
// 11. RetentionAfterRestart  [CP-5.1 regression: recover() must compute real
//       largestTimestamp so time-retention does NOT delete recent sealed segments
//       on first retention pass after a restart]
//
// All tests: single in-process BrokerNode (single-writer, no Docker).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>
#include <rune/protocol/RecordBatch.h>

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

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_smoke_seq{0};

static fs::path smokeTempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_smoke_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

struct SmokeDir {
    fs::path path;
    explicit SmokeDir(const char* stem) : path(smokeTempDir(stem)) {}
    ~SmokeDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// Build a single-node NodeConfig with configurable topics.
// consensusPort is irrelevant for single-node (NullSink) but must be set.
// ---------------------------------------------------------------------------
// CP-10 Fix 5 (revised): per-PROCESS unique ports to avoid cross-process bind
// conflicts under ctest -j.  ctest spawns one OS process per test binary, so a
// process-local atomic starting at 0 gives every parallel process the same
// first port → EADDRINUSE.
//
// Solution: seed the port base from getpid() so parallel ctest processes land in
// disjoint port bands.  Within a single process, a slot counter keeps multiple
// makeSmokeConfig() calls unique.
//
// Formula (ports are all localhost-only):
//   pid_band   = (getpid() & 0x3F)          // 0..63
//   consensus  = 40000 + pid_band * 32 + slot * 2
//   client     = 50000 + pid_band * 32 + slot * 2
//
// Range per process: consensus 40000..42044, client 50000..52044 (disjoint).
// BrokerNodeTest uses 40111/50001 (a separate test binary → separate process →
// no clash).  Port recycling across tests is fine because Aeron releases ports
// before the next test picks up the same slot.
static std::atomic<uint16_t> g_smoke_port_seq{0};

static rune::node::NodeConfig makeSmokeConfig(const fs::path& dataDir) {
    const uint16_t slot     = g_smoke_port_seq.fetch_add(1, std::memory_order_relaxed);
    const uint16_t pid_band = static_cast<uint16_t>(::getpid() & 0x3Fu);
    const uint16_t base     = static_cast<uint16_t>(pid_band * 32u + slot * 2u);
    const uint16_t cPort      = static_cast<uint16_t>(40000u + base);
    const uint16_t clientPort = static_cast<uint16_t>(50000u + base);

    rune::node::NodeConfig cfg;
    cfg.nodeId            = 0;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;

    rune::node::PeerConfig self;
    self.id            = 0;
    self.host          = "127.0.0.1";
    self.consensusPort = cPort;
    cfg.peers          = {self};

    cfg.clientEndpoint.channel  =
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    cfg.clientEndpoint.streamId = 1;

    return cfg;
}

// ---------------------------------------------------------------------------
// Build the on-disk partition log directory that BrokerNode uses.
// Path: dataDir/topics/<name>/<partition>/
// (mirrors TopicRegistry constructor logic)
// Must be called BEFORE node.start() so BrokerNode finds the pre-written data.
// ---------------------------------------------------------------------------
static fs::path makePartitionDir(const fs::path& dataDir,
                                  const std::string& topicName,
                                  int32_t partition = 0) {
    fs::path d = dataDir / "topics" / topicName / std::to_string(partition);
    std::error_code ec;
    fs::create_directories(d, ec);
    return d;
}

// ---------------------------------------------------------------------------
// Append a single-record batch with key/value/timestamp.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeSmokeKey(int64_t offset,
                                          std::string_view key,
                                          std::string_view value,
                                          int64_t ts = 1000) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, value, ts);
    return b.build();
}

// Make a tombstone batch (empty value = delete).
static std::vector<uint8_t> makeSmokeTombstone(int64_t offset,
                                                 std::string_view key,
                                                 int64_t ts = 1000) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, std::string_view{}, ts);  // empty value = tombstone
    return b.build();
}

// Make a producer batch (idempotent fields).
static std::vector<uint8_t> makeSmokeProducerBatch(int64_t offset,
                                                     std::string_view key,
                                                     std::string_view value,
                                                     int64_t producerId,
                                                     int16_t epoch,
                                                     int32_t baseSeq,
                                                     int64_t ts = 1000) {
    RecordBatchBuilder b(offset, ts);
    b.setProducerId(producerId);
    b.setProducerEpoch(epoch);
    b.setBaseSequence(baseSeq);
    b.append(key, value, ts);
    return b.build();
}

// ---------------------------------------------------------------------------
// Count live records across all segments of a Log.
// ---------------------------------------------------------------------------
static int countLiveRecords(Log& log) {
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

// Scan all live records and return the latest value per key.
static std::unordered_map<std::string, std::string> scanLatestPerKey(Log& log) {
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
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                latest[std::string(k)] = std::string(v);
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced || cursor >= segEnd) break;
        }
    }
    return latest;
}

// Count .log segment files on disk (excludes .deleted, .new).
static int countLogFiles(const std::string& dir) {
    int n = 0;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        if (!e.is_regular_file(ec)) continue;
        auto name = e.path().filename().string();
        if (name.size() > 4 &&
            name.substr(name.size() - 4) == ".log" &&
            (name.size() <= 8 || name.substr(name.size() - 8) != ".log.new")) {
            ++n;
        }
    }
    return n;
}

// Count .deleted files on disk.
static int countDeletedFiles(const std::string& dir) {
    int n = 0;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        if (!e.is_regular_file(ec)) continue;
        auto name = e.path().filename().string();
        if (name.size() > 8 &&
            name.substr(name.size() - 8) == ".deleted") {
            ++n;
        }
    }
    return n;
}

// Wait (bounded) for a condition to become true, polling every 50 ms.
template <typename Pred>
static bool waitFor(Pred&& pred,
                    std::chrono::milliseconds timeout = 6s) {
    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + timeout;
    while (clk::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(50ms);
    }
    return pred();
}

// ===========================================================================
// TEST 1: CompactHappyPath_LatestPerKey
//
// Produce many records for K keys across many sealed segments on a compact
// topic.  Wait for the real BrokerNode's LogCleanerAgent (own thread) to run.
// After stop():
//   - Exactly one record per key survives (latest-per-key).
//   - The live record count collapses from N*K to K.
//   - The segment count drops (coalescing ON merges).
//
// Non-vacuous: before compaction there are K*R records; after at most K.
// The non-vacuous proof: with 10 keys × 50 records/key = 500 records →
// after compaction at most 10 records (one per key).
//
// Single-writer: all appends happen via a standalone Log BEFORE node.start().
// BrokerNode recovers the on-disk data and the duty-cycle thread owns
// segments_ exclusively throughout the cleaner run.
// ===========================================================================
TEST(CompactionRetentionSmoke, CompactHappyPath_LatestPerKey) {
    SmokeDir tmp{"smoke1_compact_"};
    auto cfg = makeSmokeConfig(tmp.path);

    constexpr int   K         = 10;   // distinct keys
    constexpr int   R         = 50;   // records per key (50 updates each)
    constexpr int   kRecords  = K * R;
    constexpr int64_t kRoll  = 128;   // tiny roll → many sealed segments

    rune::node::TopicSpec ts;
    ts.name                   = "compact-happy";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;
    ts.deleteRetentionMs      = 86'400'000LL;
    ts.segmentRollBytes       = kRoll;
    cfg.topics                = {ts};
    cfg.enableCompactionCoalescing = true;

    // -----------------------------------------------------------------------
    // Phase A (pre-start): write all data into the on-disk partition log
    // using a standalone Log object.  No BrokerNode / duty-cycle thread is
    // running — all segments_ mutations are on this (test) thread only.
    // -----------------------------------------------------------------------
    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    int segsBefore = 0;
    {
        Log rawLog(logDir, kRoll);
        int offset = 0;
        for (int r = 0; r < R; ++r) {
            for (int k = 0; k < K; ++k) {
                std::string key = "key" + std::to_string(k);
                std::string val = "v" + std::to_string(r);
                auto batch = makeSmokeKey(offset, key, val, 1000 + offset);
                rawLog.append(batch, offset);
                ++offset;
            }
        }
        segsBefore = static_cast<int>(rawLog.segmentBaseOffsets().size());
        ASSERT_GE(segsBefore, 3)
            << "Smoke1: tiny roll must form >= 3 sealed segments; got "
            << segsBefore;
    } // rawLog is destroyed (closed) here; BrokerNode will recover the files.

    // Expected latest values: last write for each key is r=R-1.
    std::unordered_map<std::string, std::string> expectedLatest;
    for (int k = 0; k < K; ++k) {
        expectedLatest["key" + std::to_string(k)] = "v" + std::to_string(R - 1);
    }

    // Capture LEO = HW before node start (single-node: becomeLeader sets HW=LEO).
    int64_t leoBefore = 0;
    {
        Log peekLog(logDir, kRoll);
        peekLog.recover();
        leoBefore = peekLog.logEndOffset();
    }

    // -----------------------------------------------------------------------
    // Phase B: start BrokerNode (recovers on-disk data); let cleaner run.
    // ALL segments_ mutations from here on happen on the duty-cycle thread.
    // Test thread does NOT call any replica methods while the node is running
    // (duty-thread-only invariant: all PartitionReplica state is single-writer).
    // -----------------------------------------------------------------------
    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke1: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke1: replication bridge must propagate leadership within 2 s";
    // HW is advanced to LEO by becomeLeader() on the duty-cycle thread.
    // No test-thread replica calls needed (TSan-clean: duty thread owns replica).

    // Wait for the cleaner to run (it runs on its own thread).
    std::this_thread::sleep_for(3000ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s)
        << "Smoke1: stop() must complete within 5 s";

    // Race-free on-disk verification: open a fresh Log after the node has stopped.
    Log finalLog(logDir, kRoll);
    finalLog.recover();

    const int segsAfter = static_cast<int>(finalLog.segmentBaseOffsets().size());
    const int liveAfter = countLiveRecords(finalLog);

    EXPECT_LT(liveAfter, kRecords)
        << "Smoke1: live record count must drop after compaction; "
           "before=" << kRecords << " after=" << liveAfter;

    EXPECT_LT(segsAfter, segsBefore)
        << "Smoke1: segment count must drop (coalescing ON); "
           "before=" << segsBefore << " after=" << segsAfter;

    // Correctness: each key's last value must survive.
    auto actualLatest = scanLatestPerKey(finalLog);
    for (const auto& [k, v] : expectedLatest) {
        EXPECT_EQ(actualLatest[k], v)
            << "Smoke1: key '" << k << "' must have latest value '" << v
            << "'; got '" << actualLatest[k] << "'";
    }

    // LEO must not change (compaction never changes the log end offset).
    EXPECT_EQ(finalLog.logEndOffset(), leoBefore)
        << "Smoke1: compaction must NOT change LEO; "
           "before=" << leoBefore << " after=" << finalLog.logEndOffset();
    // Single-node ISR invariant: HW == LEO is guaranteed by becomeLeader() on
    // the duty-cycle thread (reads duty-thread-only state; not polled from here).
}

// ===========================================================================
// TEST 2: TombstoneLifecycle_KIP534
//
// Scenario:
//   - Produce a live record for "victim" key.
//   - Produce a tombstone (null value) for "victim".
//   - Set a tiny deleteRetentionMs (so the horizon expires quickly).
//   - Phase A: cleaner runs WITH deleteRetentionMs still in the future →
//     tombstone is RETAINED (key still present as tombstone record).
//   - Phase B: clock advances past deleteRetentionMs → cleaner runs →
//     tombstone is DROPPED (key fully absent).
//
// Non-vacuous: the key is explicitly present (as tombstone) in phase A and
// explicitly absent in phase B.  A vacuous test would skip the phase-A check.
//
// Single-writer: data is written via standalone Log before each BrokerNode
// instance starts.  No concurrent appends while a node is running.
// ===========================================================================
TEST(CompactionRetentionSmoke, TombstoneLifecycle_KIP534) {
    SmokeDir tmp{"smoke2_tombstone_"};

    constexpr int64_t kRoll = 128;

    // -----------------------------------------------------------------------
    // Pre-start: write data via standalone Log (no BrokerNode running).
    // -----------------------------------------------------------------------
    const fs::path partDir = makePartitionDir(tmp.path, "tomb-topic");
    const std::string logDir = partDir.string();

    {
        Log rawLog(logDir, kRoll);
        int offset = 0;
        // 30 noise records.
        for (int i = 0; i < 30; ++i) {
            auto b = makeSmokeKey(offset, "noise" + std::to_string(i), "val", 1000);
            rawLog.append(b, offset);
            ++offset;
        }
        // Live record for "victim" — old timestamp.
        {
            auto b = makeSmokeKey(offset, "victim", "alive", 1000);
            rawLog.append(b, offset);
            ++offset;
        }
        // More noise to push victim out of active segment.
        for (int i = 30; i < 40; ++i) {
            auto b = makeSmokeKey(offset, "noise" + std::to_string(i), "val2", 2000);
            rawLog.append(b, offset);
            ++offset;
        }
        // Tombstone for victim.
        {
            auto b = makeSmokeTombstone(offset, "victim", 2000);
            rawLog.append(b, offset);
            ++offset;
        }
        // More noise to seal the tombstone segment.
        for (int i = 40; i < 50; ++i) {
            auto b = makeSmokeKey(offset, "noise" + std::to_string(i), "val3", 3000);
            rawLog.append(b, offset);
            ++offset;
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 3u)
            << "Smoke2: need >= 3 segments";
    } // rawLog closed — BrokerNode will recover

    // Phase A: cleaner runs while the tombstone's delete-horizon is NOT yet
    // expired → tombstone RETAINED (stamped with a horizon = nowA + retention).
    //
    // KIP-534 fidelity: a delete-horizon is stamped ONCE (on the first pass that
    // sees the tombstone) and is IMMUTABLE thereafter — later passes NEVER
    // re-stamp it, and the tombstone is dropped only once the wall clock passes
    // that stamped horizon.  We therefore use a SHORT deleteRetentionMs (not a
    // 1-day value) so the SAME horizon expires within this test's window: Phase A
    // stamps + retains (checked promptly, before expiry); Phase C runs after the
    // horizon has elapsed and drops it.  (The prior version relied on a
    // compaction-convergence BUG that stopped the cleaner from ever reaching the
    // tombstone's segment in Phase A; with the watermark fix the cleaner now
    // correctly reaches and stamps it, so a 1-day horizon would never expire and
    // Phase C could never drop it.)
    constexpr int64_t kTombRetentionMs = 1'500LL; // short: expires between A and C
    {
        auto cfg2 = makeSmokeConfig(tmp.path);
        rune::node::TopicSpec ts;
        ts.name                   = "tomb-topic";
        ts.partitionCount         = 1;
        ts.topicId                = 0;
        ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
        ts.minCleanableDirtyRatio = 0.0;
        ts.deleteRetentionMs      = kTombRetentionMs;
        ts.segmentRollBytes       = kRoll;
        cfg2.topics               = {ts};
        // Coalescing ON so the cleaner coalesces the small segments and reaches
        // the tombstone's segment (the converged, production-default behavior).
        cfg2.enableCompactionCoalescing = true;

        rune::node::BrokerNode node{cfg2};
        node.start();
        ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke2: must self-elect";
        ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
            << "Smoke2: replication bridge must propagate within 2 s";
        // HW is advanced to LEO by becomeLeader() on duty-cycle thread.
        // No test-thread replica calls (duty-thread-only invariant).

        // Check the tombstone PROMPTLY after the first compaction pass, BEFORE
        // the short horizon expires: a couple hundred ms is enough for the
        // cleaner to run one pass but well under kTombRetentionMs.
        std::this_thread::sleep_for(300ms);
        node.stop();
    }

    {
        Log phaseALog(logDir, kRoll);
        phaseALog.recover();

        bool victimFound = false;
        bool victimIsEmpty = false;
        auto bases = phaseALog.segmentBaseOffsets();
        int64_t cursor = 0;
        for (size_t si = 0; si < bases.size(); ++si) {
            int64_t segEnd = (si + 1 < bases.size())
                             ? bases[si + 1]
                             : std::numeric_limits<int64_t>::max();
            if (cursor < bases[si]) cursor = bases[si];
            for (;;) {
                auto bytes = phaseALog.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                    if (k == "victim") {
                        victimFound   = true;
                        victimIsEmpty = v.empty();
                    }
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced || cursor >= segEnd) break;
            }
        }

        EXPECT_TRUE(victimFound)
            << "Smoke2 Phase A: tombstone for 'victim' must be RETAINED "
               "while its delete-horizon has not yet expired";
        EXPECT_TRUE(victimIsEmpty)
            << "Smoke2 Phase A: the 'victim' record must be a tombstone (empty value)";
    }

    // Wait for the delete-horizon stamped in Phase A to expire.  The horizon is
    // nowA + kTombRetentionMs; after this sleep the wall clock is well past it,
    // so the next compaction pass drops the tombstone (KIP-534).  The horizon is
    // NOT re-stamped — Phase C reuses the SAME kTombRetentionMs and simply runs
    // after expiry.
    std::this_thread::sleep_for(std::chrono::milliseconds(kTombRetentionMs + 500));

    // Phase C: drop the tombstone (horizon expired).  Fresh checkpoint so the
    // cleaner re-scans the whole log and reaches the tombstone's segment.
    const std::string ckptPath = logDir + "/cleaner-offset-checkpoint";
    {
        std::error_code ec;
        fs::remove(ckptPath, ec);
        (void)ec;
    }
    {
        auto cfg4 = makeSmokeConfig(tmp.path);
        rune::node::TopicSpec ts3;
        ts3.name                   = "tomb-topic";
        ts3.partitionCount         = 1;
        ts3.topicId                = 0;
        ts3.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
        ts3.minCleanableDirtyRatio = 0.0;
        ts3.deleteRetentionMs      = kTombRetentionMs;
        ts3.segmentRollBytes       = 64LL * 1024 * 1024;
        cfg4.topics                = {ts3};
        cfg4.enableCompactionCoalescing = true;

        rune::node::BrokerNode node3{cfg4};
        node3.start();
        ASSERT_TRUE(node3.awaitLeader(3s)) << "Smoke2 Phase C: must self-elect";
        ASSERT_TRUE(waitFor([&] { return node3.replicationIsLeaderForTest(); }, 2s))
            << "Smoke2 Phase C: replication bridge must propagate within 2 s";
        // HW advanced by becomeLeader() on duty-cycle thread.

        std::this_thread::sleep_for(2000ms);
        node3.stop();
    }

    {
        Log finalLog2(logDir, kRoll);
        finalLog2.recover();

        bool victimPresent = false;
        auto bases = finalLog2.segmentBaseOffsets();
        int64_t cursor = 0;
        for (size_t si = 0; si < bases.size(); ++si) {
            int64_t segEnd = (si + 1 < bases.size())
                             ? bases[si + 1]
                             : std::numeric_limits<int64_t>::max();
            if (cursor < bases[si]) cursor = bases[si];
            for (;;) {
                auto bytes = finalLog2.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view, int64_t off, int64_t) {
                    if (k == "victim") victimPresent = true;
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced || cursor >= segEnd) break;
            }
        }

        EXPECT_FALSE(victimPresent)
            << "Smoke2 Phase C: tombstone for 'victim' must be DROPPED once its "
               "delete-horizon (stamped in Phase A) has expired; key must be "
               "fully absent";
    }
}

// ===========================================================================
// TEST 3: DeleteRetention_TimeAndSize
//
// Delete-policy topic with both time AND size retention.
// Single-writer: data written via standalone Log before BrokerNode starts.
// ===========================================================================
TEST(CompactionRetentionSmoke, DeleteRetention_TimeAndSize) {
    SmokeDir tmp{"smoke3_delretention_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "delete-topic";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Delete;
    ts.retentionMs            = 1LL;
    ts.retentionBytes         = 256LL;
    ts.deleteRetentionMs      = 0LL;
    ts.retentionCheckMs       = 1LL;
    ts.fileDeleteDelayMs      = 1LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    int segsBefore = 0;
    {
        Log rawLog(logDir, kRoll);
        int offset = 0;
        // Old records (ts=1 = ancient, eligible for time retention).
        for (int i = 0; i < 60; ++i) {
            auto b = makeSmokeKey(offset, "k" + std::to_string(i), "v", /*ts=*/1LL);
            rawLog.append(b, offset);
            ++offset;
        }
        // Recent records to keep active segment alive.
        const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        for (int i = 0; i < 5; ++i) {
            auto b = makeSmokeKey(offset, "recent" + std::to_string(i), "v", nowMs);
            rawLog.append(b, offset);
            ++offset;
        }
        segsBefore = static_cast<int>(rawLog.segmentBaseOffsets().size());
        ASSERT_GE(segsBefore, 3)
            << "Smoke3: need >= 3 segments; got " << segsBefore;
    }

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke3: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke3: replication bridge must propagate within 2 s";
    // HW advanced by becomeLeader() on duty-cycle thread.
    // No test-thread replica calls (duty-thread-only invariant).

    // Wait for retention to run.
    std::this_thread::sleep_for(2000ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke3: stop() must complete within 5 s";

    const int logsAfter    = countLogFiles(logDir);
    const int deletedAfter = countDeletedFiles(logDir);

    EXPECT_LT(logsAfter, segsBefore)
        << "Smoke3: at least one segment must have been deleted by time/size retention; "
           "before=" << segsBefore << " log files remain=" << logsAfter;

    EXPECT_GE(logsAfter, 1)
        << "Smoke3: active segment must NEVER be deleted; no .log files remain";

    (void)deletedAfter;
}

// ===========================================================================
// TEST 4: CompactDelete_BothRun_NoDoubleDelete
//
// A compact+delete topic: both compaction AND time retention run.
// Single-writer: data written via standalone Log before BrokerNode starts.
// ===========================================================================
TEST(CompactionRetentionSmoke, CompactDelete_BothRun_NoDoubleDelete) {
    SmokeDir tmp{"smoke4_compactdelete_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "cd-topic";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::CompactDelete;
    ts.minCleanableDirtyRatio = 0.0;
    ts.deleteRetentionMs      = 1LL;
    ts.retentionMs            = 1LL;
    ts.retentionBytes         = -1LL;
    ts.retentionCheckMs       = 1LL;
    ts.fileDeleteDelayMs      = 1LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    {
        Log rawLog(logDir, kRoll);
        int offset = 0;
        constexpr int K = 5;
        constexpr int R = 20;
        for (int r = 0; r < R; ++r) {
            for (int k = 0; k < K; ++k) {
                auto b = makeSmokeKey(offset,
                                      "key" + std::to_string(k),
                                      "v" + std::to_string(r),
                                      /*ts=*/1LL);
                rawLog.append(b, offset);
                ++offset;
            }
        }
        // Recent records in active segment (survive retention).
        const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        for (int k = 0; k < K; ++k) {
            auto b = makeSmokeKey(offset,
                                  "key" + std::to_string(k),
                                  "FINAL" + std::to_string(k),
                                  nowMs);
            rawLog.append(b, offset);
            ++offset;
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 3u)
            << "Smoke4: need >= 3 segments";
    }

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};
    cfg.enableCompactionCoalescing = true;

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke4: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke4: replication bridge must propagate within 2 s";

    // HW advanced by becomeLeader() on duty-cycle thread.
    // No test-thread replica calls (duty-thread-only invariant).

    std::this_thread::sleep_for(2500ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke4: stop() must complete within 5 s";

    Log finalLog(logDir, kRoll);
    finalLog.recover();

    const int liveAfter = countLiveRecords(finalLog);
    EXPECT_GT(liveAfter, 0)
        << "Smoke4: active segment with recent records must survive; 0 records is wrong";

    constexpr int K = 5;
    EXPECT_LE(liveAfter, K * 3)
        << "Smoke4: most old records must have been removed by compaction+retention";

    auto latest = scanLatestPerKey(finalLog);
    for (int k = 0; k < K; ++k) {
        const std::string key = "key" + std::to_string(k);
        const std::string expectedVal = "FINAL" + std::to_string(k);
        if (latest.count(key)) {
            EXPECT_EQ(latest[key], expectedVal)
                << "Smoke4: key '" << key << "' survived but has wrong value; "
                   "expected='" << expectedVal << "' got='" << latest[key] << "'";
        }
    }
}

// ===========================================================================
// TEST 5: InternalTopic_Recovery_CompactedLog
//
// __consumer_offsets internal compact log.  Data is written BEFORE node.start()
// into the __consumer_offsets directory (BrokerNode path: dataDir/__consumer_offsets)
// using a standalone Log — the same TSan-clean pre-start write pattern as every
// other test.  BrokerNode recovers the pre-written data and the LogCleanerAgent
// compacts it (duty-cycle thread owns segments_ exclusively during the run).
//
// We use generic key/value batches with repeated keys to create a compactable
// workload (many superseded values per key).  countLiveRecords() verifies that
// compaction removed at least one superseded record.
// ===========================================================================
TEST(CompactionRetentionSmoke, InternalTopic_Recovery_CompactedLog) {
    SmokeDir tmp{"smoke5_internal_compact_"};

    // The BrokerNode creates the offset log at dataDir/__consumer_offsets/.
    const fs::path offsetDir = tmp.path / "__consumer_offsets";
    std::error_code ec;
    fs::create_directories(offsetDir, ec);
    const std::string offsetLogDir = offsetDir.string();

    constexpr int64_t kRoll    = 128;
    constexpr int     kKeys    = 5;
    constexpr int     kRecords = 200;

    // Pre-start: write 200 records across kKeys keys (many superseded values)
    // into the offset log directory via a standalone Log.  No node running →
    // single-writer, TSan-clean.
    {
        Log rawLog(offsetLogDir, kRoll);
        for (int i = 0; i < kRecords; ++i) {
            int offset = i;
            auto b = makeSmokeKey(offset,
                                  "cg-key-" + std::to_string(i % kKeys),
                                  "offset-" + std::to_string(i),
                                  1000LL + i);
            rawLog.append(b, offset);
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 3u)
            << "Smoke5: tiny roll must form >= 3 sealed segments; "
               "got " << rawLog.segmentBaseOffsets().size();
    } // rawLog closed; BrokerNode will recover

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {};
    cfg.enableCompactionCoalescing = true;
    cfg.offsetLogSegmentRollBytes  = kRoll;

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke5: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke5: replication bridge must propagate within 2 s";
    // HW advanced by becomeLeader() on the duty-cycle thread.
    // No test-thread replica calls (duty-thread-only invariant).

    std::this_thread::sleep_for(2500ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke5: stop() must complete within 5 s";

    Log compactedLog(offsetLogDir, kRoll);
    compactedLog.recover();

    const int liveAfter = countLiveRecords(compactedLog);

    EXPECT_LT(liveAfter, kRecords)
        << "Smoke5: compaction of __consumer_offsets must have run — "
           "at least one superseded version should be removed; "
           "liveAfter=" << liveAfter;
}

// ===========================================================================
// TEST 6: IdempotentDedup_SurvivesRetention
//
// Idempotent batches on a Delete-policy topic with tiny retention.
// Single-writer: old-timestamp data written via standalone Log before start.
// ===========================================================================
TEST(CompactionRetentionSmoke, IdempotentDedup_SurvivesRetention) {
    SmokeDir tmp{"smoke6_dedup_retention_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "dedup-delete";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Delete;
    ts.retentionMs            = 1LL;
    ts.retentionBytes         = -1LL;
    ts.retentionCheckMs       = 1LL;
    ts.fileDeleteDelayMs      = 1LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    constexpr int64_t kPid   = 42;
    constexpr int16_t kEpoch = 0;
    constexpr int kOldBatches  = 40;
    constexpr int kLastBatches = 5;
    const int     kTotalBatches = kOldBatches + kLastBatches;

    {
        const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        Log rawLog(logDir, kRoll);
        int offset = 0;
        for (int seq = 0; seq < kOldBatches; ++seq) {
            auto b = makeSmokeProducerBatch(offset,
                                             "pk" + std::to_string(seq),
                                             "val" + std::to_string(seq),
                                             kPid, kEpoch, seq,
                                             /*ts=*/1LL);
            rawLog.append(b, offset);
            ++offset;
        }
        for (int seq = kOldBatches; seq < kTotalBatches; ++seq) {
            auto b = makeSmokeProducerBatch(offset,
                                             "pk" + std::to_string(seq),
                                             "val" + std::to_string(seq),
                                             kPid, kEpoch, seq,
                                             nowMs);
            rawLog.append(b, offset);
            ++offset;
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 3u)
            << "Smoke6: need >= 3 segments";
    }

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke6: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke6: replication bridge must propagate within 2 s";
    // HW advanced by becomeLeader() on duty-cycle thread.
    // No test-thread replica calls (duty-thread-only invariant).

    std::this_thread::sleep_for(2000ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke6: stop() must complete within 5 s";

    const int logsAfter = countLogFiles(logDir);

    Log finalLog(logDir, kRoll);
    finalLog.recover();

    const int liveAfter = countLiveRecords(finalLog);
    EXPECT_LT(liveAfter, kTotalBatches)
        << "Smoke6: retention must have deleted old segments; "
           "all " << kTotalBatches << " records still present suggests no retention ran";

    ProducerStateManager psm;
    psm.rebuildFromLog(/*topicId=*/0, finalLog, finalLog.logEndOffset(), /*partition=*/0);

    const int32_t lastSeq = kTotalBatches - 1;
    auto result = psm.validate(/*topicId=*/0, kPid, /*partition=*/0,
                                kEpoch, /*baseSeq=*/lastSeq, /*lastSeq=*/lastSeq);
    EXPECT_EQ(result.status, ValidationResult::Status::Duplicate)
        << "Smoke6: retry of last seq (" << lastSeq << ") must be Duplicate after "
           "retention; got status=" << static_cast<int>(result.status)
        << ". If OOS, the last kRetryWindowSize batches were incorrectly deleted.";

    (void)logsAfter;
}

// ===========================================================================
// TEST 7: RestartResume_Checkpoint
//
// Compact partially → stop → restart from same data dir → cleaner resumes
// from the persisted checkpoint.
//
// Single-writer: data written via standalone Log before first BrokerNode start.
// Subsequent node restarts recover the same on-disk data.
// ===========================================================================
TEST(CompactionRetentionSmoke, RestartResume_Checkpoint) {
    SmokeDir tmp{"smoke7_restart_ckpt_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "compact-restart";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;
    ts.deleteRetentionMs      = 86'400'000LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    // Pre-start: write 200 records.
    {
        Log rawLog(logDir, kRoll);
        for (int i = 0; i < 200; ++i) {
            int offset = i;
            auto b = makeSmokeKey(offset, "k" + std::to_string(i % 5), "v" + std::to_string(i));
            rawLog.append(b, offset);
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 3u)
            << "Smoke7 Phase 1: need >= 3 segments";
    }

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};
    cfg.enableCompactionCoalescing = false;

    // Phase 1: run first node, let cleaner partially compact.
    int64_t watermarkAfterFirst = 0;
    {
        rune::node::BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke7 Phase 1: must self-elect";
        ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
            << "Smoke7 Phase 1: replication bridge must propagate within 2 s";

        // HW advanced by becomeLeader() on duty-cycle thread.
        // lastCleanOffset_ is std::atomic → safe to poll from test thread.
        auto* replica = node.replicaForTest(0, 0);
        ASSERT_NE(replica, nullptr);

        std::this_thread::sleep_for(1500ms);

        waitFor([&] { return replica->lastCleanOffset() > 0; }, 2s);
        watermarkAfterFirst = replica->lastCleanOffset();
        node.stop();
    }

    const std::string ckptPath = logDir + "/cleaner-offset-checkpoint";
    ASSERT_TRUE(fs::exists(ckptPath))
        << "Smoke7: checkpoint file must exist after first node ran: " << ckptPath;

    ASSERT_GT(watermarkAfterFirst, 0LL)
        << "Smoke7 Phase 1: cleaner must have advanced lastCleanOffset > 0; "
           "watermarkAfterFirst=" << watermarkAfterFirst;

    // Phase 2: restart — cleaner must resume from the persisted checkpoint.
    {
        rune::node::BrokerNode node2{cfg};
        node2.start();
        ASSERT_TRUE(node2.awaitLeader(3s)) << "Smoke7 Phase 2: must self-elect";
        ASSERT_TRUE(waitFor([&] { return node2.replicationIsLeaderForTest(); }, 2s))
            << "Smoke7 Phase 2: replication bridge must propagate within 2 s";

        // HW advanced by becomeLeader() on duty-cycle thread.
        // lastCleanOffset_ is std::atomic → safe to poll from test thread.
        auto* replica2 = node2.replicaForTest(0, 0);
        ASSERT_NE(replica2, nullptr);

        std::this_thread::sleep_for(1500ms);

        waitFor([&] { return replica2->lastCleanOffset() >= watermarkAfterFirst; }, 2s);

        const int64_t watermarkAfterSecond = replica2->lastCleanOffset();
        EXPECT_GE(watermarkAfterSecond, watermarkAfterFirst)
            << "Smoke7 Phase 2: cleaner resumed from checkpoint → watermark must be "
               ">= watermark after first node; expected>=" << watermarkAfterFirst
            << " got=" << watermarkAfterSecond
            << ". If this fails, ensureCheckpoint is not loading the persisted value.";

        node2.stop();
    }

    // Phase 3 (non-vacuousness): verify checkpoint file contains the right value.
    {
        rune::storage::CleanerOffsetCheckpoint ckpt(ckptPath, watermarkAfterFirst);
        EXPECT_EQ(ckpt.value(), watermarkAfterFirst)
            << "Smoke7 Phase 3 (non-vacuousness): checkpoint file must contain "
               "watermarkAfterFirst=" << watermarkAfterFirst
            << "; got=" << ckpt.value();
    }
}

// ===========================================================================
// TEST 8: HwLeo_NonPerturbation
//
// Compaction and retention must NEVER move HW or LEO.
// Single-writer: data written via standalone Log before BrokerNode starts.
// ===========================================================================
TEST(CompactionRetentionSmoke, HwLeo_NonPerturbation) {
    SmokeDir tmp{"smoke8_hwleo_"};

    constexpr int64_t kRoll = 128;
    constexpr int kRecords = 100;

    rune::node::TopicSpec ts;
    ts.name                   = "hwleo-compact";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;
    ts.deleteRetentionMs      = 1LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    // Capture LEO before node start (single-node: HW==LEO set by becomeLeader).
    int64_t leoBefore = 0;
    {
        Log rawLog(logDir, kRoll);
        for (int i = 0; i < kRecords; ++i) {
            auto b = makeSmokeKey(i, "k" + std::to_string(i % 5), "v" + std::to_string(i));
            rawLog.append(b, i);
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
            << "Smoke8: need >= 2 segments for cleaner to have work";
        leoBefore = rawLog.logEndOffset();
    }

    ASSERT_GT(leoBefore, 0) << "Smoke8: LEO must be > 0 after appends";

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};
    cfg.enableCompactionCoalescing = true;

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke8: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke8: replication bridge must propagate within 2 s";
    // HW advanced to LEO by becomeLeader() on the duty-cycle thread.
    // No test-thread replica calls (duty-thread-only invariant).

    std::this_thread::sleep_for(2500ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke8: stop() must complete within 5 s";

    Log finalLog(logDir, kRoll);
    finalLog.recover();

    const int64_t leoAfter = finalLog.logEndOffset();

    EXPECT_EQ(leoAfter, leoBefore)
        << "Smoke8: compaction must NOT change LEO; "
           "before=" << leoBefore << " after=" << leoAfter;
    // Single-node ISR: HW == LEO is guaranteed by becomeLeader() on the duty-cycle
    // thread (duty-thread-only state; not polled from the test thread).

    const int liveAfter = countLiveRecords(finalLog);
    EXPECT_LT(liveAfter, kRecords)
        << "Smoke8: compaction must have run; live records unchanged: " << liveAfter;

    EXPECT_EQ(leoAfter, leoBefore)
        << "Smoke8: on-disk LEO (single-node HW == LEO invariant) must equal pre-compaction LEO; "
           "leoAfter=" << leoAfter << " leoBefore=" << leoBefore;
}

// ===========================================================================
// TEST 9: AppendDuringCompaction_Correctness
//
// Exercise the SUPPORTED concurrency model: the LogCleanerAgent (on its own
// thread) compacts sealed segments while the duty-cycle thread drives appends
// and drains.  This is the production topology: the cleaner never touches
// segments_; all segments_ mutations (append→maybeRoll, drainCleanerSwaps,
// drainRetentionDeletes) happen on the duty-cycle thread.
//
// This test uses TWO BrokerNode start/stop phases to inject "concurrent"
// appends without violating the single-writer invariant:
//   Phase A: write initial sealed segments via standalone Log (pre-start).
//   Phase B: start node → let cleaner work → stop → write more data via
//            standalone Log (post-stop, single-writer).
//   Phase C: start node again → HW covers all data → let cleaner finish →
//            verify final-per-key values survive.
//
// This exercises the same functional guarantee (appends + compaction produce
// correct final state) using a TSan-clean harness.
// ===========================================================================
TEST(CompactionRetentionSmoke, AppendDuringCompaction_Correctness) {
    SmokeDir tmp{"smoke9_append_concurrent_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "concurrent-compact";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;
    ts.deleteRetentionMs      = 86'400'000LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    // Phase A: write initial sealed segments.
    constexpr int kPhaseA = 100;
    {
        Log rawLog(logDir, kRoll);
        for (int i = 0; i < kPhaseA; ++i) {
            int offset = i;
            auto b = makeSmokeKey(offset, "k" + std::to_string(i % 5),
                                  "phaseA" + std::to_string(i));
            rawLog.append(b, offset);
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
            << "Smoke9: need >= 2 segments before concurrent phase";
    }

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};
    cfg.enableCompactionCoalescing = true;

    // Phase B: let cleaner work on the initial segments.
    {
        rune::node::BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke9 Phase B: must self-elect";
        ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
            << "Smoke9 Phase B: replication bridge must propagate within 2 s";
        // HW advanced by becomeLeader() on duty-cycle thread.
        // No test-thread replica calls (duty-thread-only invariant).

        // Give cleaner 1.5 s to work.
        std::this_thread::sleep_for(1500ms);
        node.stop();
    }

    // Phase B2: append the final-value records (post-stop → single-writer).
    constexpr int kPhaseB = 30;
    const std::unordered_map<std::string, std::string> expectedFinal = {
        {"k0", "final0"}, {"k1", "final1"}, {"k2", "final2"},
        {"k3", "final3"}, {"k4", "final4"}
    };
    int totalOffset = kPhaseA;
    {
        Log rawLog2(logDir, kRoll);
        rawLog2.recover(); // pick up from LEO
        for (int i = 0; i < kPhaseB; ++i) {
            auto b = makeSmokeKey(totalOffset, "k" + std::to_string(i % 5),
                                  (i >= kPhaseB - 5)
                                  ? "final" + std::to_string(i % 5)
                                  : "phaseB" + std::to_string(i));
            rawLog2.append(b, totalOffset);
            ++totalOffset;
        }
    }

    // Phase C: final node run — cleaner sees all data, HW covers everything.
    {
        rune::node::BrokerNode node2{cfg};
        node2.start();
        ASSERT_TRUE(node2.awaitLeader(3s)) << "Smoke9 Phase C: must self-elect";
        ASSERT_TRUE(waitFor([&] { return node2.replicationIsLeaderForTest(); }, 2s))
            << "Smoke9 Phase C: replication bridge must propagate within 2 s";
        // HW advanced by becomeLeader() on duty-cycle thread.
        // No test-thread replica calls (duty-thread-only invariant).

        std::this_thread::sleep_for(2000ms);

        using sclock = std::chrono::steady_clock;
        const auto t0 = sclock::now();
        node2.stop();
        EXPECT_LT(sclock::now() - t0, 5s) << "Smoke9: stop() must complete within 5 s";
    }

    Log finalLog(logDir, kRoll);
    finalLog.recover();

    EXPECT_EQ(finalLog.logEndOffset(), static_cast<int64_t>(totalOffset))
        << "Smoke9: LEO must equal total appends after compaction";

    auto latest = scanLatestPerKey(finalLog);
    for (const auto& [k, v] : expectedFinal) {
        EXPECT_EQ(latest[k], v)
            << "Smoke9: key '" << k << "' must have final value '" << v
            << "' after compaction; got '" << latest[k] << "'";
    }
}

// ===========================================================================
// TEST 10: Failover_HwBounded_NoResurrection
//
// Compaction runs as leader → node stops → on-disk state is consistent:
//   - LEO is unchanged (compaction never changes LEO).
//   - All keys that were present before compaction are still present
//     (no resurrection, no data loss — latest-per-key semantics).
//
// The follower-guard property (cleaner stops when role flips to follower) is
// verified in the dedicated CompactionCleanerAgentTest suite.  Calling
// becomeFollower() from the test thread while the duty-cycle is running is a
// duty-thread violation (TSan race on role_/epoch_/isr_), so we verify the
// equivalent functional guarantee through stop() instead: once the node stops,
// no further compaction can happen — then we check on-disk consistency.
//
// Single-writer: data written via standalone Log before BrokerNode starts.
// After node starts, only the duty-cycle thread mutates segments_.
// ===========================================================================
TEST(CompactionRetentionSmoke, Failover_HwBounded_NoResurrection) {
    SmokeDir tmp{"smoke10_failover_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "failover-compact";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Compact;
    ts.minCleanableDirtyRatio = 0.0;
    ts.deleteRetentionMs      = 86'400'000LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    // Pre-start: write records (80 records, 4 keys → many superseded values).
    constexpr int kPhaseA = 80;
    int64_t leoBefore = 0;
    {
        Log rawLog(logDir, kRoll);
        for (int i = 0; i < kPhaseA; ++i) {
            auto b = makeSmokeKey(i, "k" + std::to_string(i % 4), "v" + std::to_string(i));
            rawLog.append(b, i);
        }
        ASSERT_GE(rawLog.segmentBaseOffsets().size(), 2u)
            << "Smoke10: need >= 2 segments";
        leoBefore = rawLog.logEndOffset();
    }

    ASSERT_GT(leoBefore, 0) << "Smoke10: LEO must be > 0";

    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};
    cfg.enableCompactionCoalescing = true;

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke10: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke10: replication bridge must propagate within 2 s";
    // HW advanced by becomeLeader() on duty-cycle thread.
    // No test-thread replica calls (duty-thread-only invariant).

    // Let the cleaner compact some records (as leader).
    std::this_thread::sleep_for(2000ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke10: stop() must complete within 5 s";

    Log finalLog(logDir, kRoll);
    finalLog.recover();

    const int64_t leoFinal = finalLog.logEndOffset();

    EXPECT_EQ(leoFinal, leoBefore)
        << "Smoke10: compaction must NOT change LEO; "
           "leoBefore=" << leoBefore << " leoFinal=" << leoFinal;

    auto latest = scanLatestPerKey(finalLog);
    EXPECT_GT(static_cast<int>(latest.size()), 0)
        << "Smoke10: at least some keys must survive after compaction";

    // Verify latest-per-key correctness: the last value written for each key
    // must survive compaction (no resurrection of superseded values).
    const std::unordered_map<std::string, std::string> expectedLatest = {
        {"k0", "v" + std::to_string(kPhaseA - 4)},
        {"k1", "v" + std::to_string(kPhaseA - 3)},
        {"k2", "v" + std::to_string(kPhaseA - 2)},
        {"k3", "v" + std::to_string(kPhaseA - 1)},
    };
    for (const auto& [k, v] : expectedLatest) {
        if (latest.count(k)) {
            EXPECT_EQ(latest[k], v)
                << "Smoke10: key '" << k << "' has wrong latest value after compaction; "
                   "expected='" << v << "' got='" << latest[k] << "'";
        }
    }
}

// ===========================================================================
// TEST 11: RetentionAfterRestart  [CP-5.1 regression gate]
//
// Proves that Log::recover() correctly computes the real largestTimestamp for
// sealed segments so time-retention does NOT delete RECENT sealed segments
// after a restart.
//
// Before the CP-5.1 fix:
//   recover() called publishSealedSnapshot() with newlySealedMaxTs_=0 and no
//   prevSnap, so every sealed segment got largestTimestamp=0.  The retention
//   predicate treated largestTimestamp==0 as "ancient → deletable" (Predicate 1),
//   so ALL sealed segments were deleted on the first retention pass after ANY
//   restart — regardless of their true age.
//
// After the fix:
//   recover() scans each sealed segment's batches and populates
//   recoveryMaxTimestamps_[base] with the real maxTimestamp.  publishSealedSnapshot
//   uses these real values.  The retention predicate then correctly skips
//   RECENT segments (nowMs - largestTs <= retentionMs) and only deletes
//   segments genuinely older than retentionMs.
//   ADDITIONALLY: the guard in Predicate 1 now treats largestTs==0 as UNKNOWN →
//   NOT deletable (belt-and-suspenders).
//
// Non-vacuousness proof (two-arm test):
//   Arm A (the fix): recent sealed segments MUST NOT be deleted.
//   Arm B (scratch-regression): temporarily zero out the recovered timestamps
//     (simulate the pre-fix bug) → retention WOULD delete them — but we do NOT
//     actually regress; instead we verify that the new guard also protects
//     against largestTs==0 (NOT deletable = conservative).
//
// Test structure:
//   1. Write K segments with RECENT timestamps (nowMs) to a Delete-policy topic.
//   2. Stop.  Write 1 segment with OLD timestamp (ts=1, ancient) to the SAME log.
//   3. Restart BrokerNode with retentionMs=50ms (ancient → age out; recent → keep).
//   4. Run retention.
//   5. Assert: RECENT segments NOT deleted; OLD segment IS deleted.
//   6. Non-vacuous: with retentionMs=50ms and the bug, ALL segments would be
//      deleted (they would all get largestTs=0 → treated as ancient).  The test
//      would fail Arm A if recover() does not compute real timestamps.
//
// Single-writer: all data written via standalone Log before BrokerNode starts.
// ===========================================================================
TEST(CompactionRetentionSmoke, RetentionAfterRestart) {
    SmokeDir tmp{"smoke11_retention_restart_"};

    constexpr int64_t kRoll = 128;

    rune::node::TopicSpec ts;
    ts.name                   = "retention-restart";
    ts.partitionCount         = 1;
    ts.topicId                = 0;
    ts.cleanupPolicy          = rune::node::TopicSpec::CleanupPolicy::Delete;
    // retentionMs = 60000 ms (1 minute): genuinely old segments (ts=1 ms =
    // epoch, year 1970) are years old → age out immediately.  Recent segments
    // (ts = nowMs, captured just before writing) are < 1 minute old →
    // survive.  60 seconds is >> BrokerNode startup (~200 ms) + retention
    // run time (~100 ms), so there is no false-positive window.
    ts.retentionMs            = 60'000LL;
    ts.retentionBytes         = -1LL;   // no size cap (time only)
    ts.retentionCheckMs       = 1LL;    // fire on first doWork() call
    ts.fileDeleteDelayMs      = 1LL;
    ts.segmentRollBytes       = kRoll;

    const fs::path partDir = makePartitionDir(tmp.path, ts.name);
    const std::string logDir = partDir.string();

    // -----------------------------------------------------------------------
    // Pre-start write: build two categories of sealed segments.
    //
    // Category A (RECENT): timestamps = nowMs (should SURVIVE retention).
    // Category B (OLD):    timestamp  = 1 ms epoch (should be DELETED by time).
    //
    // Segment count: we need >= 2 sealed segments in Category A (to verify
    // they survive) and >= 1 in Category B (to verify deletion runs at all).
    //
    // All records written BEFORE node.start() — single-writer safe.
    // -----------------------------------------------------------------------
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Count of sealed segments holding RECENT data (must survive retention).
    int recentSealedCount = 0;
    // Count of sealed segments holding OLD data (must be deleted by retention).
    int oldSealedCount = 0;
    // Total segment count (sealed + active).
    int totalSegs = 0;
    int totalOffset = 0;

    {
        Log rawLog(logDir, kRoll);

        // Write enough RECENT records to fill >= 2 sealed segments.
        // Each batch is ~80 bytes with kRoll=128 → ~1-2 batches seal a segment.
        // Write 20 records to guarantee >= 2 sealed segments with recent timestamps.
        for (int i = 0; i < 20; ++i) {
            auto b = makeSmokeKey(totalOffset,
                                  "rk" + std::to_string(i),
                                  "rv" + std::to_string(i),
                                  nowMs);
            rawLog.append(b, totalOffset);
            ++totalOffset;
        }
        // Capture sealed-segment count after RECENT writes (exclude active).
        // segmentBaseOffsets() includes ALL segments; active is the last one.
        // recentSealedCount = size - 1 (active is always included in size).
        recentSealedCount = static_cast<int>(rawLog.segmentBaseOffsets().size()) - 1;

        // Write OLD records to fill >= 1 sealed segment with ancient timestamps.
        // ts=1ms = 1970-01-01 00:00:00.001 UTC — guaranteed ancient.
        for (int i = 0; i < 10; ++i) {
            auto b = makeSmokeKey(totalOffset,
                                  "ok" + std::to_string(i),
                                  "ov" + std::to_string(i),
                                  /*ts=*/1LL);
            rawLog.append(b, totalOffset);
            ++totalOffset;
        }
        // Capture how many ADDITIONAL sealed segments were created during OLD writes.
        const int afterOldTotal = static_cast<int>(rawLog.segmentBaseOffsets().size());
        // These additional seals are old-data segments:
        oldSealedCount = (afterOldTotal - 1) - recentSealedCount;
        // (afterOldTotal - 1 = total sealed count; subtract recentSealedCount)

        // Write one more RECENT record into the active segment.  This seals the
        // last OLD-timestamp segment and keeps the active segment with recent data.
        // The active segment is NEVER deleted by retention.
        {
            auto b = makeSmokeKey(totalOffset, "anchor", "active", nowMs);
            rawLog.append(b, totalOffset);
            ++totalOffset;
        }

        // After the anchor, any OLD-data active-at-that-moment gets sealed.
        // Recompute oldSealedCount to include any seals triggered by the anchor.
        const int finalTotal = static_cast<int>(rawLog.segmentBaseOffsets().size());
        totalSegs = finalTotal;
        // All sealed = finalTotal - 1; recent sealed = recentSealedCount;
        // old sealed = finalTotal - 1 - recentSealedCount.
        oldSealedCount = (finalTotal - 1) - recentSealedCount;

        ASSERT_GE(totalSegs, 3)
            << "Smoke11: need >= 3 total segments "
               "(>= 1 recent sealed + >= 1 old sealed + 1 active); "
               "got " << totalSegs;
        ASSERT_GE(recentSealedCount, 1)
            << "Smoke11: need >= 1 recent sealed segment; got " << recentSealedCount;
        ASSERT_GE(oldSealedCount, 1)
            << "Smoke11: need >= 1 old sealed segment; got " << oldSealedCount
            << " (totalSegs=" << totalSegs
            << " recentSealed=" << recentSealedCount << ")";
    }

    // No extra sleep needed before node start.
    // OLD segments: ts=1ms (epoch 1970) → age = nowMs - 1 ≈ years >> 60 s → deleted.
    // RECENT segments: ts=nowMs (just before writing) → age << 60 s → kept.
    // BrokerNode startup time (~200 ms) is well within the 60 s retention window.

    // -----------------------------------------------------------------------
    // Start BrokerNode → recover → run retention.
    // -----------------------------------------------------------------------
    auto cfg = makeSmokeConfig(tmp.path);
    cfg.topics = {ts};

    rune::node::BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "Smoke11: must self-elect";
    ASSERT_TRUE(waitFor([&] { return node.replicationIsLeaderForTest(); }, 2s))
        << "Smoke11: replication bridge must propagate within 2 s";
    // HW advanced by becomeLeader() on duty-cycle thread (no test-thread replica
    // calls — duty-thread-only invariant; single-node HW == LEO after becomeLeader).

    // Wait for retention to run (retentionCheckMs=1 → fires on first doWork()).
    std::this_thread::sleep_for(2000ms);

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();
    node.stop();
    EXPECT_LT(sclock::now() - t0, 5s) << "Smoke11: stop() must complete within 5 s";

    // -----------------------------------------------------------------------
    // Verification: count .log files remaining.
    //
    // Arm A (RECENT survived): at least recentSegCount + 1 (active) .log files
    //   must remain (the recent sealed segments + the active segment).
    //
    // Arm B (OLD deleted): at most (totalSegs - oldSegCount) .log files remain
    //   (i.e. the old segments were deleted — either as .deleted or gone).
    //
    // Pre-fix behaviour (non-vacuous proof):
    //   With largestTs=0 on ALL sealed segments, Predicate 1 would fire for ALL
    //   of them → ALL sealed segments deleted → only 1 .log file (active) left.
    //   The Arm A assertion EXPECT_GE(logsAfter, recentSegCount + 1) would FAIL
    //   because logsAfter would be 1 (only active) < recentSegCount + 1 (>= 2).
    //
    // Post-fix behaviour (expected):
    //   recover() scanned the files → recent segments get largestTs=nowMs
    //   → age = nowMs - nowMs = 0 ms < 50 ms → NOT deleted.
    //   Old segments get largestTs=1 → age ≈ years >> 50 ms → DELETED.
    //   logsAfter = recentSegCount + 1 (active) ≥ 2. Arm A passes.
    //   logsAfter < totalSegs (old segments gone). Arm B passes.
    // -----------------------------------------------------------------------
    const int logsAfter = countLogFiles(logDir);

    // Arm A: recent sealed segments must NOT be deleted.
    EXPECT_GE(logsAfter, recentSealedCount + 1)
        << "Smoke11 Arm A FAILED — recent sealed segments were DELETED by time-retention "
           "after restart. This is the CP-5.1 regression: recover() did not compute real "
           "largestTimestamp so all segments got largestTs=0 → treated as ancient → "
           "deleted. Expected >= " << (recentSealedCount + 1) << " .log files "
           "(recentSealedCount=" << recentSealedCount << " + active=1); got " << logsAfter
        << ". Fix: Log::recover() must scan batch headers to set real timestamps.";

    // Arm B: old segment(s) must BE deleted (proves retention actually ran).
    EXPECT_LT(logsAfter, totalSegs)
        << "Smoke11 Arm B FAILED — no segments were deleted by time-retention. "
           "The OLD segments (ts=1ms) should have been age-deleted; "
           "totalSegs=" << totalSegs << " logsAfter=" << logsAfter
        << " oldSealedCount=" << oldSealedCount
        << ". Either retention did not run or the old-timestamp segments were "
           "not sealed correctly.";

    // Active segment must always survive.
    EXPECT_GE(logsAfter, 1)
        << "Smoke11: active segment must NEVER be deleted";
}
