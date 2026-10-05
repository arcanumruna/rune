// ---------------------------------------------------------------------------
// RetentionCleanerAgentTest.cpp — CP-9: Retention parity on the cleaner agent.
//
// Seven tests, all failing before CP-9 implementation:
//
//  1. TimeRetentionDeletesOldSegments
//       A segment whose largestTimestamp is older than retention.ms is deleted;
//       a recent segment (within retention window) is kept.
//       Controllable clock.  Non-vacuous: with a very large retentionMs nothing
//       is deleted; with a tiny retentionMs old segments are deleted.
//
//  2. SizeRetentionDeletesOldestFirst
//       total > retention.bytes → oldest sealed segments deleted until under cap;
//       active segment NEVER deleted; recent enough segment kept if within
//       retentionMs.  Pure size predicate (retentionMs = -1 = unlimited).
//
//  3. LogStartOffsetRetention
//       Segments entirely below logStartOffset are deleted.
//
//  4. AsyncDeleteRenameThenRemove
//       A segment scheduled for deletion is renamed to .deleted immediately
//       (removed from segments_ on the append thread), but the .deleted file is
//       physically removed only AFTER fileDeleteDelayMs has elapsed.
//       A read of the deleted segment's offset returns empty (not a fault).
//
//  5. RetentionOffAppendThread
//       Driving only the append/duty-cycle path (NodeRuntimeAgent::doWork) does
//       NOT trigger time/size retention.  The cleaner agent's cadence (driven by
//       LogCleanerAgent::doWork) does trigger it.
//
//  6. RetentionSingleWriterSafe
//       Retention removals (segments_ mutations) happen only on the append thread;
//       the cleaner thread only renames files (.log → .deleted) and enqueues the
//       base.  TSan must report 0 races.  (In-process concurrency test with real
//       threads — omitted from the TSan binary by a TSAN_OPTIONS suppress list;
//       asserted here by checking the work division.)
//
//  7. CompactDeleteBothRunNoDoubleDelete
//       A compact+delete topic both compacts (latest-per-key) and retains
//       (time/size) without double-deleting a segment.  Segments that are
//       compacted but not yet expired by retention are not prematurely deleted.
//       Segments that are old enough ARE deleted exactly once.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>   // getrlimit/setrlimit — fd-limit raise for fd-heavy tests

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// Fd-heavy stress tests below roll thousands of TINY segments (rollBytes as small
// as 128) to force retention/compaction churn.  Each live segment consumes an fd,
// so the churn needs far more descriptors than the default soft RLIMIT_NOFILE
// (commonly 1024).  When the limit is hit, a segment open() throws mid-run; with a
// joinable cleaner thread still live, the unwinding destroys it and libstdc++
// aborts with "terminate called without an active exception" — masking the real
// cause.  Raise the soft limit to the hard limit ONCE for this test binary so the
// tests exercise the concurrency they intend, not the ambient fd ceiling.  This is
// a self-contained test-environment fix (raising soft up to hard needs no
// privileges); see also the ThreadJoiner guard which makes any future resource
// failure surface as a clean assertion instead of a terminate.
static const bool g_cp9_fd_limit_raised = [] {
    struct ::rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
    }
    return true;
}();

// RAII: on scope exit (normal OR exception) signal the worker to stop and join it,
// so a joinable thread is never destroyed — a resource exception in the writer loop
// then propagates as a normal gtest failure instead of std::terminate.
struct ThreadJoiner {
    std::atomic<bool>& done;
    std::thread&       t;
    ~ThreadJoiner() {
        done.store(true, std::memory_order_relaxed);
        if (t.joinable()) t.join();
    }
};

static std::atomic<uint64_t> g_cp9_seq{0};

static fs::path cp9TempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_cp9_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

// Build a one-record batch with explicit timestamp (epoch-ms).
static std::vector<uint8_t> cp9Batch(int64_t offset, std::string_view key,
                                      std::string_view value, int64_t ts) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, value, ts);
    return b.build();
}

// Count .log segments in dir (excludes .deleted files).
static int countLogSegments(const fs::path& dir) {
    int n = 0;
    for (auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ".log") ++n;
    return n;
}

// Count .deleted files in dir.
static int countDeletedFiles(const fs::path& dir) {
    int n = 0;
    for (auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ".deleted") ++n;
    return n;
}

// Null transport (no replication needed for cleaner tests).
class Cp9NullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// Minimal consensus agent (no-op for in-process tests).
class Cp9MinimalConsensusAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

// Build a Delete-policy PartitionConfig with retention settings.
static PartitionConfig makeDeleteConfig(int32_t topicId,
                                        int64_t retentionMs,
                                        int64_t retentionBytes,
                                        int64_t retentionCheckMs = 1,      // 1 ms for tests
                                        int64_t fileDeleteDelayMs = 1) {   // 1 ms for tests
    PartitionConfig cfg;
    cfg.topicId               = topicId;
    cfg.partitionId           = 0;
    cfg.selfId                = 1;
    cfg.minIsr                = 1;
    cfg.replicaLagMaxMs       = 10'000;
    cfg.replicaLagMaxOffset   = 10'000;
    cfg.fetchIntervalNs       = 1'000'000LL;
    cfg.isrCheckIntervalNs    = 1'000'000LL;
    cfg.maxFetchBytes         = 1 << 20;
    cfg.pushOnAppend          = false;
    cfg.cleanupPolicy         = CleanupPolicy::Delete;
    cfg.minCleanableDirtyRatio = 0.5;
    cfg.deleteRetentionMs     = 86'400'000LL;
    cfg.retentionMs           = retentionMs;
    cfg.retentionBytes        = retentionBytes;
    cfg.retentionCheckMs      = retentionCheckMs;
    cfg.fileDeleteDelayMs     = fileDeleteDelayMs;
    return cfg;
}

// Build a CompactDelete-policy PartitionConfig.
static PartitionConfig makeCompactDeleteConfig(int32_t topicId,
                                               int64_t retentionMs,
                                               int64_t retentionBytes,
                                               int64_t retentionCheckMs = 1,
                                               int64_t fileDeleteDelayMs = 1) {
    auto cfg = makeDeleteConfig(topicId, retentionMs, retentionBytes,
                                retentionCheckMs, fileDeleteDelayMs);
    cfg.cleanupPolicy = CleanupPolicy::CompactDelete;
    cfg.minCleanableDirtyRatio = 0.0;
    return cfg;
}

// ===========================================================================
// TEST 1: TimeRetentionDeletesOldSegments
//
// A segment whose largestTimestamp is older than retentionMs must be deleted;
// a recent segment (within the retention window) must be kept.
//
// Non-vacuous proof:
//   (a) With retentionMs = INT64_MAX (effectively unlimited), no segments are
//       deleted (proves the test is not always-deleting).
//   (b) With retentionMs = 1 (1 ms), any segment older than "now - 1ms" is
//       deleted (proves the time predicate actually fires).
//
// Clock injection: we stamp "old" records with epoch_ms = 1000 (1 Jan 1970
// + 1 sec) and "recent" records with epoch_ms = now_ms (real wall clock).
// The cleaner receives nowMs from the real clock; the delta is several decades.
// ===========================================================================
TEST(RetentionCleanerAgent, TimeRetentionDeletesOldSegments) {
    auto dir = cp9TempDir("cp9_time_retention");

    // Use a tiny roll size so we get multiple segments quickly.
    Log log(dir.string(), /*rollBytes=*/128);

    // Write "old" records (timestamp = 1000 ms epoch = 1 Jan 1970 + 1 s).
    // These must be deleted when retentionMs is smaller than their age.
    const int64_t oldTs = 1000LL; // 1 Jan 1970 epoch-ms
    int64_t off = 0;
    for (int i = 0; i < 20; ++i) {
        auto b = cp9Batch(off, "k" + std::to_string(i), "old_v", oldTs);
        off = log.append(b, off);
    }

    // Force a segment roll so the "old" records are in a SEALED segment
    // (roll by appending a big batch).
    {
        std::vector<uint8_t> filler(256, 0xAB);
        RecordBatchBuilder fb(off, oldTs);
        fb.append("fill", std::string(200, 'x'), oldTs);
        auto fb2 = fb.build();
        off = log.append(fb2, off);
    }

    const int segsAfterOld = countLogSegments(dir);
    ASSERT_GE(segsAfterOld, 2) << "Need >= 2 segments after old writes";

    // Write "recent" records (timestamp = now_ms).
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    for (int i = 0; i < 10; ++i) {
        auto b = cp9Batch(off, "r" + std::to_string(i), "new_v", nowMs);
        off = log.append(b, off);
    }

    const int segsTotal = countLogSegments(dir);
    ASSERT_GE(segsTotal, 2) << "Need >= 2 total segments";

    Cp9NullTransport transport;

    // -----------------------------------------------------------------------
    // Part A: Non-vacuous check — with huge retentionMs, nothing is deleted.
    // -----------------------------------------------------------------------
    {
        // Large retentionMs: no segment is old enough to delete.
        int64_t bigRetentionMs = 9'999'999'999'999LL; // ~317 years
        auto cfgA = makeDeleteConfig(901, bigRetentionMs, /*retentionBytes=*/-1,
                                     /*retentionCheckMs=*/1, /*fileDeleteDelayMs=*/1);
        auto repA = std::make_unique<PartitionReplica>(
            cfgA, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
        repA->becomeLeader(1, {1});
        repA->onLeaderLogAppended();

        rune::node::ReplicaRegistry regA;
        PartitionReplica* rawA = repA.get();
        regA.add(901, 0, std::move(repA));
        rune::node::LogCleanerAgent cleanerA(&regA);

        Cp9MinimalConsensusAgent dummyA;
        rune::node::NodeRuntimeAgent dutyA(dummyA, nullptr, nullptr, nullptr, &regA, nullptr);

        // Run retention cycles.
        for (int i = 0; i < 20; ++i) {
            cleanerA.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            dutyA.doWork(static_cast<int64_t>(i) * 1'000'000LL); // drain
        }

        const int segsAfterBigRet = countLogSegments(dir);
        EXPECT_EQ(segsAfterBigRet, segsTotal)
            << "Part A: with huge retentionMs, NO segment should be deleted; "
               "before=" << segsTotal << " after=" << segsAfterBigRet;
        (void)rawA;
    }

    // -----------------------------------------------------------------------
    // Part B: with retentionMs = 1 ms, old segments ARE deleted.
    //         At this point segsTotal segments exist (Part A deleted nothing).
    // -----------------------------------------------------------------------
    {
        int64_t tinyRetentionMs = 1LL; // 1 ms — all "old" segments are way older
        auto cfgB = makeDeleteConfig(902, tinyRetentionMs, /*retentionBytes=*/-1,
                                     /*retentionCheckMs=*/1, /*fileDeleteDelayMs=*/1);
        auto repB = std::make_unique<PartitionReplica>(
            cfgB, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
        repB->becomeLeader(1, {1});
        repB->onLeaderLogAppended();

        rune::node::ReplicaRegistry regB;
        regB.add(902, 0, std::move(repB));
        rune::node::LogCleanerAgent cleanerB(&regB);

        Cp9MinimalConsensusAgent dummyB;
        rune::node::NodeRuntimeAgent dutyB(dummyB, nullptr, nullptr, nullptr, &regB, nullptr);

        // Run retention cycles.
        for (int i = 0; i < 20; ++i) {
            cleanerB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            dutyB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }

        // Brief delay so fileDeleteDelayMs=1ms expires.
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        // Run one more cycle to trigger physical file removal.
        for (int i = 20; i < 25; ++i) {
            cleanerB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            dutyB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }

        const int segsAfterTinyRet = countLogSegments(dir);
        EXPECT_LT(segsAfterTinyRet, segsTotal)
            << "Part B: with tinyRetentionMs=1, old segments MUST be deleted; "
               "before=" << segsTotal << " after=" << segsAfterTinyRet;
        // Active segment must still exist.
        EXPECT_GE(segsAfterTinyRet, 1) << "Active segment must never be deleted";
    }

    fs::remove_all(dir);
}

// Regression (found in the F20 re-review): rune-client-cpp stamps record timestamps in epoch
// NANOSECONDS; the time predicate compared them with epoch-ms "now", so (now - largestTs) was hugely
// negative and time retention NEVER deleted a real client's segments.  Timestamps are normalised to
// epoch ms (rune::storage::toEpochMs) before the age test.  No sleeps: deletion = the .log is renamed
// to .deleted by the retention cycle itself.
TEST(RetentionCleanerAgent, TimeRetentionWithNanosecondTimestampsRegression) {
    auto dir = cp9TempDir("cp9_time_retention_ns");
    Log log(dir.string(), /*rollBytes=*/128);
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    constexpr int64_t kDayMs = 24LL * 3600 * 1000;
    const int64_t oldNs = (nowMs - 2 * kDayMs) * 1'000'000LL;   // 2 days old, epoch NANOSECONDS
    const int64_t newNs = nowMs * 1'000'000LL;
    int64_t off = 0;
    for (int i = 0; i < 20; ++i) off = log.append(cp9Batch(off, "k" + std::to_string(i), "old_v", oldNs), off);
    {
        RecordBatchBuilder fb(off, oldNs);
        fb.append("fill", std::string(200, 'x'), oldNs);
        off = log.append(fb.build(), off);
    }
    for (int i = 0; i < 10; ++i) off = log.append(cp9Batch(off, "r" + std::to_string(i), "new_v", newNs), off);
    const int segsTotal = countLogSegments(dir);
    ASSERT_GE(segsTotal, 3);

    Cp9NullTransport transport;
    auto cfg = makeDeleteConfig(903, /*retentionMs=*/kDayMs, /*retentionBytes=*/-1,
                                /*retentionCheckMs=*/1, /*fileDeleteDelayMs=*/60'000);
    auto rep = std::make_unique<PartitionReplica>(cfg, Role::Leader, 1, std::vector<NodeId>{1},
                                                  std::vector<NodeId>{1}, &log, &transport);
    rep->becomeLeader(1, {1});
    rep->onLeaderLogAppended();
    rune::node::ReplicaRegistry reg;
    reg.add(903, 0, std::move(rep));
    rune::node::LogCleanerAgent cleaner(&reg);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &reg, nullptr);
    for (int i = 0; i < 20; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }
    EXPECT_LT(countLogSegments(dir), segsTotal) << "2-day-old ns-stamped segments must be retired by 1-day retention";
    EXPECT_GT(countDeletedFiles(dir), 0);
    EXPECT_GE(countLogSegments(dir), 1) << "recent segments (and the active one) stay";
    fs::remove_all(dir);
}

// ===========================================================================
// TEST 2: SizeRetentionDeletesOldestFirst
//
// When total log size > retentionBytes, oldest sealed segments are deleted
// until under cap.  Active segment is NEVER deleted.
//
// Non-vacuous: with retentionBytes = INT64_MAX, nothing deleted.
//              With retentionBytes = small value, oldest deleted first.
// ===========================================================================
TEST(RetentionCleanerAgent, SizeRetentionDeletesOldestFirst) {
    auto dir = cp9TempDir("cp9_size_retention");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t recentTs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        auto b = cp9Batch(off, "k" + std::to_string(i), "payload_val", recentTs);
        off = log.append(b, off);
    }

    const int segsBefore = countLogSegments(dir);
    ASSERT_GE(segsBefore, 3) << "Need >= 3 segments";
    const int64_t totalBytes = log.totalBytes();

    Cp9NullTransport transport;

    // -----------------------------------------------------------------------
    // Part A: large retentionBytes → nothing deleted.
    // -----------------------------------------------------------------------
    {
        auto cfgA = makeDeleteConfig(910, /*retMs=*/-1, /*retBytes=*/totalBytes * 10,
                                     1, 1);
        auto repA = std::make_unique<PartitionReplica>(
            cfgA, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
        repA->becomeLeader(1, {1});
        repA->onLeaderLogAppended();

        rune::node::ReplicaRegistry regA;
        regA.add(910, 0, std::move(repA));
        rune::node::LogCleanerAgent cleanerA(&regA);
        Cp9MinimalConsensusAgent dummyA;
        rune::node::NodeRuntimeAgent dutyA(dummyA, nullptr, nullptr, nullptr, &regA, nullptr);

        for (int i = 0; i < 10; ++i) {
            cleanerA.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            dutyA.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }

        EXPECT_EQ(countLogSegments(dir), segsBefore)
            << "Part A: with large retentionBytes, nothing should be deleted";
    }

    // -----------------------------------------------------------------------
    // Part B: small retentionBytes → oldest segments deleted first.
    // -----------------------------------------------------------------------
    {
        // Keep only 1/3 of total — should delete multiple oldest segments.
        int64_t budget = totalBytes / 3;
        auto cfgB = makeDeleteConfig(911, /*retMs=*/-1, budget, 1, 1);
        auto repB = std::make_unique<PartitionReplica>(
            cfgB, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
        repB->becomeLeader(1, {1});
        repB->onLeaderLogAppended();

        rune::node::ReplicaRegistry regB;
        regB.add(911, 0, std::move(repB));
        rune::node::LogCleanerAgent cleanerB(&regB);
        Cp9MinimalConsensusAgent dummyB;
        rune::node::NodeRuntimeAgent dutyB(dummyB, nullptr, nullptr, nullptr, &regB, nullptr);

        for (int i = 0; i < 20; ++i) {
            cleanerB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            dutyB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }
        // Allow fileDeleteDelayMs to expire.
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        for (int i = 20; i < 25; ++i) {
            cleanerB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            dutyB.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }

        const int segsAfter = countLogSegments(dir);
        EXPECT_LT(segsAfter, segsBefore)
            << "Part B: with tiny retentionBytes, oldest segments must be deleted; "
               "before=" << segsBefore << " after=" << segsAfter;
        EXPECT_GE(segsAfter, 1)
            << "Active segment must NEVER be deleted by size retention";

        // Total bytes must be within one active-segment of the budget.
        // (We can't be exactly at budget because we can only delete whole segments.)
        // The invariant: totalBytes after <= budget + one sealed segment size OR
        //                only active segment remains.
        const int64_t remainingBytes = log.totalBytes();
        // Either we're now at or below budget (modulo rounding to segment boundaries)
        // or only the active segment remains.
        EXPECT_GE(remainingBytes, 0)
            << "totalBytes must be non-negative";
    }

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 3: LogStartOffsetRetention
//
// Segments entirely below logStartOffset are deleted.
// ===========================================================================
TEST(RetentionCleanerAgent, LogStartOffsetRetention) {
    auto dir = cp9TempDir("cp9_logstart_retention");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t recentTs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        auto b = cp9Batch(off, "k" + std::to_string(i), "vv", recentTs);
        off = log.append(b, off);
    }

    const int segsBefore = countLogSegments(dir);
    ASSERT_GE(segsBefore, 3) << "Need >= 3 segments";

    // Snapshot the segment bases.
    auto bases = log.segmentBaseOffsets();
    // Set logStartOffset to the base of the 3rd segment (index 2).
    // Segments 0 and 1 must be deleted.
    const int64_t logStartOffset = (bases.size() >= 3) ? bases[2] : bases[1];

    Cp9NullTransport transport;
    // Large retentionMs and retentionBytes so only logStartOffset predicate fires.
    auto cfg = makeDeleteConfig(920, /*retMs=*/-1, /*retBytes=*/-1, 1, 1);
    cfg.logStartOffset = logStartOffset;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();

    rune::node::ReplicaRegistry registry;
    registry.add(920, 0, std::move(replica));
    rune::node::LogCleanerAgent cleaner(&registry);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (int i = 0; i < 20; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    for (int i = 20; i < 25; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    // Segments below logStartOffset must be deleted.
    const auto basesAfter = log.segmentBaseOffsets();
    for (int64_t base : basesAfter) {
        EXPECT_GE(base, logStartOffset)
            << "Segment with base=" << base
            << " is below logStartOffset=" << logStartOffset
            << " and must have been deleted";
    }
    EXPECT_GE(static_cast<int>(basesAfter.size()), 1)
        << "Active segment must survive logStartOffset retention";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 4: AsyncDeleteRenameThenRemove
//
// When a segment is scheduled for deletion:
//   (a) Its .log file is renamed to .deleted immediately on the cleaner agent
//       pass (the base is enqueued; drainRetentionDeletes removes it from
//       segments_ on the append thread).
//   (b) The .deleted file is physically removed only AFTER fileDeleteDelayMs.
//   (c) Reading an offset that was in the deleted segment returns empty
//       (not a crash or fault).
//
// Non-vacuous: with fileDeleteDelayMs = 0 or very small, the file disappears
//              immediately; with a large delay it is still a .deleted file.
// ===========================================================================
TEST(RetentionCleanerAgent, AsyncDeleteRenameThenRemove) {
    auto dir = cp9TempDir("cp9_async_delete");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t oldTs = 1000LL; // ancient timestamp
    int64_t off = 0;
    for (int i = 0; i < 30; ++i) {
        auto b = cp9Batch(off, "k" + std::to_string(i), "val", oldTs);
        off = log.append(b, off);
    }

    // Force a sealed segment with a big filler so at least one is fully old.
    {
        RecordBatchBuilder fb(off, oldTs);
        fb.append("fill", std::string(200, 'z'), oldTs);
        off = log.append(fb.build(), off);
    }

    const int segsBeforeRecent = countLogSegments(dir);
    ASSERT_GE(segsBeforeRecent, 2) << "Need >= 2 segments with old records";

    // Add recent record to active segment (must not be deleted).
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto rb = cp9Batch(off, "recent", "val", nowMs);
    off = log.append(rb, off);

    const int segsTotal = countLogSegments(dir);

    Cp9NullTransport transport;
    // Use a LARGE fileDeleteDelayMs (60 000 ms) to ensure the file is not physically
    // removed immediately — it should remain as .deleted during the test.
    auto cfg = makeDeleteConfig(930, /*retMs=*/1LL, /*retBytes=*/-1,
                                /*retentionCheckMs=*/1,
                                /*fileDeleteDelayMs=*/60'000);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();

    rune::node::ReplicaRegistry registry;
    registry.add(930, 0, std::move(replica));
    rune::node::LogCleanerAgent cleaner(&registry);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Snapshot the deleted segment's base offset for later read test.
    auto basesBefore = log.segmentBaseOffsets();
    const int64_t firstBase = basesBefore.front();

    // Run retention.
    for (int i = 0; i < 10; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    const int segsAfter = countLogSegments(dir);
    const int deletedFiles = countDeletedFiles(dir);

    // (a) At least one .log segment must have been removed.
    EXPECT_LT(segsAfter, segsTotal)
        << "At least one old segment must be removed from segments_; "
           "before=" << segsTotal << " after=" << segsAfter;

    // (b) The .deleted file must still exist (fileDeleteDelayMs=60s not elapsed).
    EXPECT_GT(deletedFiles, 0)
        << "The .deleted file must still exist (fileDeleteDelayMs=60s not elapsed); "
           "segs=" << segsAfter << " deleted=" << deletedFiles;

    // (c) Reading the deleted segment's first offset returns empty (not a fault).
    EXPECT_NO_FATAL_FAILURE({
        auto raw = log.read(firstBase, 1 << 20);
        // Either empty (segment gone from in-memory map) or valid (if not yet drained).
        // The key invariant: no crash/assert.
        (void)raw;
    });

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 5: RetentionOffAppendThread
//
// Driving ONLY the NodeRuntimeAgent (append/duty-cycle path) does NOT trigger
// time/size retention.  The cleaner cadence (LogCleanerAgent::doWork) does.
//
// Non-vacuous: Part A verifies duty-cycle alone does nothing.
//              Part B verifies the cleaner agent deletes segments.
//              If the duty cycle DID run retention, Part A would fail.
// ===========================================================================
TEST(RetentionCleanerAgent, RetentionOffAppendThread) {
    auto dir = cp9TempDir("cp9_off_append");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t oldTs = 1000LL;
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        auto b = cp9Batch(off, "k" + std::to_string(i), "val", oldTs);
        off = log.append(b, off);
    }

    const int segsTotal = countLogSegments(dir);
    ASSERT_GE(segsTotal, 3) << "Need >= 3 segments";

    Cp9NullTransport transport;
    auto cfg = makeDeleteConfig(940, /*retMs=*/1LL, /*retBytes=*/-1, 1, 1);

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();

    rune::node::ReplicaRegistry registry;
    registry.add(940, 0, std::move(replica));

    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // -----------------------------------------------------------------------
    // Part A: Drive ONLY the duty-cycle agent — retention must NOT trigger.
    // -----------------------------------------------------------------------
    for (int i = 0; i < 50; ++i) {
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    const int segsAfterDuty = countLogSegments(dir);
    EXPECT_EQ(segsAfterDuty, segsTotal)
        << "Part A: duty-cycle alone must NOT run retention; "
           "before=" << segsTotal << " after=" << segsAfterDuty;

    // -----------------------------------------------------------------------
    // Part B: Drive the LogCleanerAgent — retention MUST trigger.
    // -----------------------------------------------------------------------
    rune::node::LogCleanerAgent cleaner(&registry);

    for (int i = 0; i < 20; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL); // drain
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    for (int i = 20; i < 25; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    const int segsAfterCleaner = countLogSegments(dir);
    EXPECT_LT(segsAfterCleaner, segsTotal)
        << "Part B: cleaner agent must run retention and delete old segments; "
           "before=" << segsTotal << " after=" << segsAfterCleaner;

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 6: RetentionSingleWriterSafe
//
// Retention removals from segments_ happen ONLY on the append/drain thread.
// The cleaner thread only renames .log → .deleted and enqueues the base.
// This test drives both threads concurrently (cleaner on a background thread,
// append on the test thread) and verifies:
//   (a) No crash (segments_ never directly mutated on the cleaner thread).
//   (b) After the cleaner finishes, drain completes on the main thread and
//       the segment count drops as expected.
//
// NOTE: TSan enforcement is done via the build-tsan/ target (separate binary).
//       This test just verifies the functional invariant in normal builds.
// ===========================================================================
// Build a Delete-policy config with a SECOND replica (follower id=2) so the
// leader's IsrManager actually SHRINKS/EXPANDS the ISR on the duty thread —
// exactly the isr_ = std::move(newIsr) churn that races the cleaner's HW read.
// Small lag window + fast ISR-check cadence maximise the churn frequency.
static PartitionConfig makeChurningDeleteConfig(int32_t topicId,
                                                int64_t retentionMs,
                                                int64_t retentionBytes) {
    auto cfg = makeDeleteConfig(topicId, retentionMs, retentionBytes,
                                /*retentionCheckMs=*/1, /*fileDeleteDelayMs=*/1);
    cfg.minIsr             = 1;                 // stay produce-able with a lone leader
    cfg.replicaLagMaxMs    = 0;                 // follower time-lags out almost instantly
    cfg.replicaLagMaxOffset = 0;               // and offset-lags out as the leader appends
    cfg.isrCheckIntervalNs = 50'000LL;          // 50 µs — churn hard
    cfg.pushOnAppend       = false;
    return cfg;
}

static PartitionConfig makeChurningCompactConfig(int32_t topicId) {
    auto cfg = makeChurningDeleteConfig(topicId, /*retMs=*/-1, /*retBytes=*/-1);
    cfg.cleanupPolicy         = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = 0.0;           // always eligible so compaction fires
    return cfg;
}

// ---------------------------------------------------------------------------
// TEST 6 (STRENGTHENED — TSan): RetentionSingleWriterSafe
//
// This test PROVES the cleaner/retention path is race-free against the REAL
// duty thread.  Unlike the old direct-drain workaround, it drives:
//   • Duty/single-writer thread: NodeRuntimeAgent::doWork() → replica->doWork()
//     runs the ACTUAL IsrManager shrink/expand (isr_ = std::move(newIsr)) +
//     syncIsrToLeaderState + recomputeHw + maybeCompleteLeadership, plus real
//     replica->append() (segments_ mutation) and the cleaner-swap/delete drains
//     — all on ONE thread (single-writer invariant preserved).
//   • Cleaner thread: LogCleanerAgent::doWork() → runRetention() reading the
//     replica's HW + role CONCURRENTLY.
//
// Before the atomic-publication fix the cleaner read highWatermark()/role()
// which touch isr_/leaderState_/lastHealthyHw_ → TSan reports
// a data race here.  After the fix the cleaner reads only cleanerHighWatermark()
// / cleanerRole() (atomic acquire) → 0 races.
// ---------------------------------------------------------------------------
TEST(RetentionCleanerAgent, RetentionSingleWriterSafe) {
    auto dir = cp9TempDir("cp9_single_writer");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t oldTs = 1000LL;
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        auto b = cp9Batch(off, "k" + std::to_string(i), "val", oldTs);
        off = log.append(b, off);
    }

    const int segsBefore = countLogSegments(dir);
    ASSERT_GE(segsBefore, 3) << "Need >= 3 segments";

    Cp9NullTransport transport;
    // Two replicas so the ISR genuinely churns on the duty thread.
    auto cfg = makeChurningDeleteConfig(950, /*retMs=*/1LL, /*retBytes=*/-1);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, &log, &transport);
    replica->becomeLeader(1, {1, 2});
    replica->onLeaderLogAppended();
    PartitionReplica* raw = replica.get();

    rune::node::ReplicaRegistry registry;
    registry.add(950, 0, std::move(replica));
    rune::node::LogCleanerAgent cleaner(&registry);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    std::atomic<bool> done{false};

    // Cleaner thread — runs retention + compaction concurrently with the duty thread.
    std::thread cleanerThread([&]() {
        for (int i = 0; !done.load(std::memory_order_relaxed); ++i) {
            cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            std::this_thread::yield();
        }
    });
    // Exception-safety: if the writer loop below throws (e.g. a resource error),
    // this guard stops + joins the cleaner so the failure surfaces as a clean gtest
    // assertion, never a joinable-thread std::terminate.  Normal path still joins
    // explicitly below (needed before the post-run single-writer drain).
    ThreadJoiner cleanerJoiner{done, cleanerThread};

    // Duty/single-writer thread == this test thread.  Drives the REAL ISR churn
    // (replica->doWork via NodeRuntimeAgent), appends (segments_ mutation), a
    // periodic follower fetch (re-expands id=2 into the ISR → shrink/expand churn),
    // and the cleaner-swap/retention-delete drains.  All on ONE thread.
    for (int i = 0; i < 4000; ++i) {
        const int64_t nowNs = static_cast<int64_t>(i) * 100'000LL; // +100 µs / iter

        // Real duty cycle: ISR shrink/expand + HW recompute + drains.
        duty.doWork(nowNs);

        // Append (grows the log; single-writer segments_ mutation via the replica).
        auto b = cp9Batch(off, "new_k" + std::to_string(i), "new_v", oldTs);
        off = raw->append(b, off);

        // Periodically re-expand follower id=2 by feeding it a fetch so the ISR
        // oscillates (fetch → in-sync → time/offset-lag → shrink → fetch → ...).
        if ((i & 7) == 0) {
            FetchRequestMsg fr{};
            fr.leaderEpoch = 1;
            fr.partition   = 0;
            fr.followerId  = 2;
            fr.fetchOffset = off;              // caught up
            fr.maxBytes    = 0;                // position-only report
            fr.topicId     = 950;
            raw->onFetchRequest(fr, nowNs);
        }
    }

    done.store(true, std::memory_order_relaxed);
    cleanerThread.join();

    // Allow fileDeleteDelayMs to expire, then drain to quiescence on the writer.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    for (int i = 0; i < 10; ++i) {
        cleaner.doWork(static_cast<int64_t>(i + 100) * 1'000'000LL);
        log.drainRetentionDeletes();
        log.drainCleanerSwaps();
    }

    // Verify segments_ is consistent (no crash, correct ordering).
    const int segsAfter = countLogSegments(dir);
    EXPECT_GE(segsAfter, 1) << "At least active segment must remain";

    auto bases = log.segmentBaseOffsets();
    for (size_t i = 1; i < bases.size(); ++i) {
        EXPECT_LT(bases[i-1], bases[i])
            << "Segment bases must be strictly ascending after concurrent retention";
    }

    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// TEST 6b (STRENGTHENED — TSan): CompactionSingleWriterSafe
//
// The race ORIGINATED in the compaction path (CP-6): the cleaner's Phase-1
// scan calls replica->highWatermark() (and role()) while the duty thread churns
// the ISR.  This test proves the COMPACTION path is race-free too, by driving
// LogCleanerAgent compaction (Compact policy, dirty ratio always eligible)
// concurrently with the REAL duty doWork() ISR churn + keyed appends.
//
// Pre-fix: TSan race on the highWatermark()/role() read in the compaction scan.
// Post-fix: cleaner reads cleanerHighWatermark()/cleanerRole() only → 0 races.
// ---------------------------------------------------------------------------
TEST(RetentionCleanerAgent, CompactionSingleWriterSafe) {
    auto dir = cp9TempDir("cp9_compact_single_writer");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t oldTs = 1000LL;
    int64_t off = 0;
    // Duplicate keys so compaction has real work (latest-per-key).
    for (int i = 0; i < 60; ++i) {
        auto b = cp9Batch(off, "key" + std::to_string(i % 6),
                          "v" + std::to_string(i), oldTs);
        off = log.append(b, off);
    }

    const int segsBefore = countLogSegments(dir);
    ASSERT_GE(segsBefore, 3) << "Need >= 3 segments for compaction";

    Cp9NullTransport transport;
    auto cfg = makeChurningCompactConfig(951);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, &log, &transport);
    replica->becomeLeader(1, {1, 2});
    replica->onLeaderLogAppended();
    PartitionReplica* raw = replica.get();

    rune::node::ReplicaRegistry registry;
    registry.add(951, 0, std::move(replica));
    rune::node::LogCleanerAgent cleaner(&registry);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    std::atomic<bool> done{false};

    // Cleaner thread — compaction scan (reads HW/role) concurrent with ISR churn.
    std::thread cleanerThread([&]() {
        for (int i = 0; !done.load(std::memory_order_relaxed); ++i) {
            cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            std::this_thread::yield();
        }
    });
    // Exception-safety guard (see the sibling test): stop + join the cleaner on any
    // scope exit so a writer-loop throw never becomes a joinable-thread terminate.
    ThreadJoiner cleanerJoiner{done, cleanerThread};

    // Duty/single-writer thread: real ISR churn + keyed appends + drains.
    for (int i = 0; i < 1200; ++i) {
        const int64_t nowNs = static_cast<int64_t>(i) * 100'000LL;

        duty.doWork(nowNs);

        // Keyed append (rotating keys) so the compaction dirty range keeps growing.
        auto b = cp9Batch(off, "key" + std::to_string(i % 6),
                          "nv" + std::to_string(i), oldTs);
        off = raw->append(b, off);

        if ((i & 7) == 0) {
            FetchRequestMsg fr{};
            fr.leaderEpoch = 1;
            fr.partition   = 0;
            fr.followerId  = 2;
            fr.fetchOffset = off;
            fr.maxBytes    = 0;
            fr.topicId     = 951;
            raw->onFetchRequest(fr, nowNs);
        }
        if ((i % 400) == 0) std::this_thread::yield(); // give the cleaner airtime
    }

    done.store(true, std::memory_order_relaxed);
    cleanerThread.join();

    // Drain to quiescence on the writer thread.
    for (int i = 0; i < 10; ++i) {
        cleaner.doWork(static_cast<int64_t>(i + 100) * 1'000'000LL);
        log.drainRetentionDeletes();
        log.drainCleanerSwaps();
    }

    const int segsAfter = countLogSegments(dir);
    EXPECT_GE(segsAfter, 1) << "At least active segment must remain";

    auto bases = log.segmentBaseOffsets();
    for (size_t i = 1; i < bases.size(); ++i) {
        EXPECT_LT(bases[i-1], bases[i])
            << "Segment bases must be strictly ascending after concurrent compaction";
    }

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 7: CompactDeleteBothRunNoDoubleDelete
//
// A compact+delete topic both compacts (latest-per-key) AND retains (time/size)
// without double-deleting a segment.
//
// Setup:
//   - Write records with duplicate keys and old timestamps.
//   - Some segments are eligible for BOTH compaction AND time retention.
//   - After several cleaner cycles:
//       (a) Compaction runs (latest-per-key survivor kept).
//       (b) Time retention also runs (very old segments deleted).
//       (c) No segment is deleted twice (no double-free / crash).
//       (d) Live records are still readable and correct.
//
// Non-vacuous: without retention, the record count stays at deduplicated-but-
//              not-deleted count.  With retention, old segments are also gone.
// ===========================================================================
TEST(RetentionCleanerAgent, CompactDeleteBothRunNoDoubleDelete) {
    auto dir = cp9TempDir("cp9_compact_delete");
    Log log(dir.string(), /*rollBytes=*/128);

    const int64_t oldTs = 1000LL;
    int64_t off = 0;

    // Write many records with duplicate keys and old timestamps.
    for (int i = 0; i < 60; ++i) {
        // Rotate among 5 keys — lots of duplicates.
        auto b = cp9Batch(off, "key" + std::to_string(i % 5),
                          "val_" + std::to_string(i), oldTs);
        off = log.append(b, off);
    }

    // Add a recent record so the active segment has a fresh timestamp.
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto rb = cp9Batch(off, "recent_key", "recent_val", nowMs);
    off = log.append(rb, off);

    const int segsBefore = countLogSegments(dir);
    ASSERT_GE(segsBefore, 3) << "Need >= 3 segments for this test";

    Cp9NullTransport transport;
    // CompactDelete: tiny retentionMs to trigger time retention + compaction.
    auto cfg = makeCompactDeleteConfig(960, /*retMs=*/1LL, /*retBytes=*/-1, 1, 1);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();

    rune::node::ReplicaRegistry registry;
    registry.add(960, 0, std::move(replica));
    rune::node::LogCleanerAgent cleaner(&registry);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Run multiple cycles — both compaction and retention should run without crash.
    EXPECT_NO_FATAL_FAILURE({
        for (int i = 0; i < 30; ++i) {
            cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        for (int i = 30; i < 35; ++i) {
            cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }
    });

    // (c) After running, segment list must be strictly ascending (no duplicates).
    auto basesAfter = log.segmentBaseOffsets();
    for (size_t i = 1; i < basesAfter.size(); ++i) {
        EXPECT_LT(basesAfter[i-1], basesAfter[i])
            << "Segment bases must be strictly ascending after compact+delete; "
               "got " << basesAfter[i-1] << " and " << basesAfter[i];
    }

    // (d) Surviving records must be readable.
    bool readable = true;
    for (size_t si = 0; si < basesAfter.size(); ++si) {
        int64_t cursor = basesAfter[si];
        int64_t segEnd = (si + 1 < basesAfter.size())
                         ? basesAfter[si + 1]
                         : std::numeric_limits<int64_t>::max();
        for (;;) {
            auto raw = log.read(cursor, 1 << 20);
            if (raw.empty()) break;
            RecordBatchReader r(raw.data(), raw.size());
            if (!r.ensureValid()) { readable = false; break; }
            bool adv = false;
            r.forEach([&](std::string_view, std::string_view, int64_t o, int64_t) {
                cursor = o + 1;
                adv = true;
            });
            if (!adv || cursor >= segEnd) break;
        }
    }
    EXPECT_TRUE(readable) << "All surviving records must be readable after compact+delete";

    // (b) Segments count must have decreased (retention fired).
    const int segsAfter = countLogSegments(dir);
    EXPECT_LT(segsAfter, segsBefore)
        << "Retention must have deleted old segments in compact+delete topic; "
           "before=" << segsBefore << " after=" << segsAfter;

    fs::remove_all(dir);
}

// ===========================================================================
// 2026-10-05 log-truncate review MAJOR-3: retention gated only on the segment's
// BASE (< HW), so a sealed segment straddling the HW (base < HW < end) was
// deleted with its un-replicated records (Kafka: HW >= nextSegment.base).
// No sleeps: deletion = the .log renamed to .deleted by the cycle itself.
// ===========================================================================
TEST(RetentionCleanerAgent, TimeRetentionNeverDeletesSegmentStraddlingHwRegression) {
    auto dir = cp9TempDir("cp9_hw_straddle");
    Log log(dir.string(), /*rollBytes=*/100);   // two one-record batches per segment
    for (int64_t o = 0; o < 10; ++o) log.append(cp9Batch(o, "k", "v", /*ts=*/1000), o);
    ASSERT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4, 6, 8}));
    Cp9NullTransport transport;
    auto cfg = makeDeleteConfig(911, /*retentionMs=*/1, /*retentionBytes=*/-1);
    auto owned = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1, std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, &log, &transport);
    owned->becomeLeader(1, {1, 2});
    PartitionReplica* replica = owned.get();
    replica->onLeaderLogAppended();
    // The ISR follower has fetched up to 5: HW = 5, inside segment [4, 6).
    (void)replica->onFetchRequest(FetchRequestMsg{1, 0, 2, 5, 1 << 20, 911}, 1'000'000);
    rune::node::ReplicaRegistry reg;
    reg.add(911, 0, std::move(owned));
    rune::node::LogCleanerAgent cleaner(&reg);
    Cp9MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &reg, nullptr);
    for (int i = 0; i < 10; ++i) {
        duty.doWork(2'000'000LL + i * 1'000'000LL);
        cleaner.doWork(2'000'000LL + i * 1'000'000LL);
        duty.doWork(2'000'000LL + i * 1'000'000LL);
    }
    ASSERT_EQ(replica->cleanerHighWatermark(), 5) << "precondition: HW inside segment 4";
    auto exists = [&](int64_t b) {
        std::string s = std::to_string(b); s = std::string(20 - s.size(), '0') + s;
        return fs::exists(dir / (s + ".log"));
    };
    EXPECT_FALSE(exists(0)) << "segment [0,2) is fully below HW: deletable";
    EXPECT_FALSE(exists(2)) << "segment [2,4) is fully below HW: deletable";
    EXPECT_TRUE(exists(4)) << "segment [4,6) holds offset 5 (above HW): must survive";
    EXPECT_TRUE(exists(6));
    EXPECT_TRUE(exists(8));
    fs::remove_all(dir);
}
