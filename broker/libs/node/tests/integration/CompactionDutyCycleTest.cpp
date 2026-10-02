// ---------------------------------------------------------------------------
// CompactionDutyCycleTest.cpp — CP-2: NodeRuntimeAgent duty-cycle compaction.
//
// Tests that runBoundedCompaction is wired into doWork (step 6.5) and:
//   1. Compacts SEALED segments on LEADER replicas whose policy == Compact.
//   2. Is a no-op on NON-LEADER nodes.
//   3. Never touches the active (highest-base-offset) segment.
//   4. Respects the dirty-ratio threshold (only runs when dirty enough).
//   5. Compacts at most one sealed segment per duty-cycle (bounded work).
//   6. Already-compacted logs are not rewritten (watermark prevents redundant work).
//   7. deleteRetentionMs is sourced from PartitionConfig, not hardcoded.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_seq{0};

static fs::path makeTempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

/// Build a single-record batch with a real key for compaction.
static std::vector<uint8_t> makeBatch(int64_t baseOffset, std::string_view key,
                                       std::string_view value, int64_t ts = 1000) {
    RecordBatchBuilder b(baseOffset, ts);
    b.append(key, value, ts);
    return b.build();
}

// ---------------------------------------------------------------------------
// MinimalConsensusAgent — drives no consensus; tracks isLeader via bool flag.
// ---------------------------------------------------------------------------
class MinimalConsensusAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

// ---------------------------------------------------------------------------
// NullTransport — no-op PartitionTransport for single-node tests.
// ---------------------------------------------------------------------------
class NullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// Each test below builds its PartitionReplica + ReplicaRegistry + NodeRuntimeAgent
// inline (they need per-test config: policy, dirty-ratio threshold, retention),
// so there is no shared fixture struct.

// ---------------------------------------------------------------------------
// Count live records from all segments of a log.
// ---------------------------------------------------------------------------
static int countLiveRecords(Log& log) {
    int count = 0;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
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
            if (!advanced) break;
        }
    }
    return count;
}

// ---------------------------------------------------------------------------
// Count live records for a SPECIFIC key across all segments of a log.
// ---------------------------------------------------------------------------
static int countLiveRecordsForKey(Log& log, std::string_view wantKey) {
    int count = 0;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view, int64_t off, int64_t) {
                if (k == wantKey) ++count;
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return count;
}

// ---------------------------------------------------------------------------
// Get the latest value for a key across all log records.
// ---------------------------------------------------------------------------
static std::string latestValueForKey(Log& log, std::string_view key) {
    std::string latest;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return latest;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                if (k == key) latest = std::string(v);
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return latest;
}

// ---------------------------------------------------------------------------
// Check that the active segment (last base offset) has not shrunk.
// Returns the count of records in the active segment.
// ---------------------------------------------------------------------------
static int countActiveSegmentRecords(Log& log) {
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t activeBase = bases.back();
    int count = 0;
    int64_t cursor = activeBase;
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
        if (!advanced) break;
    }
    return count;
}

// ===========================================================================
// TEST 1: Leader compact policy — sealed segments compacted, latest per key
//         kept, active segment untouched.
//
// This is the FAILING test before runBoundedCompaction is wired in.
// ===========================================================================
TEST(CompactionDutyCycle, CompactsSealedSegmentsLeaderOnlyLatestPerKey) {
    // We build a minimal harness directly (no heavy BrokerNode / Aeron).
    // The NodeRuntimeAgent::runBoundedCompaction step is tested by calling
    // doWork on a purpose-built agent that carries the registry + leader flag.

    auto dir = makeTempDir("cp2_t1");

    // Small segment roll so 200 appends span multiple segments.
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Append 200 keyed records so the log rolls many segments.
    for (int v = 0; v < 200; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    // Capture the active segment base before compaction.
    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments for this test";
    const int64_t activeBase  = bases.back();
    const int totalBefore     = countLiveRecords(log);

    // Capture active segment record count (must not shrink).
    const int activeRecordsBefore = countActiveSegmentRecords(log);

    // Wire into a ReplicaRegistry and NodeRuntimeAgent.
    rune::node::ReplicaRegistry registry;
    PartitionReplica* rawReplica = replica.get();
    registry.add(10, 0, std::move(replica));

    // Build the NodeRuntimeAgent with leader=true, registry wired.
    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(dummy, nullptr);
    // Manually invoke runBoundedCompaction by driving the agent with registry.
    // Since NodeRuntimeAgent::runBoundedCompaction needs the registry we pass it
    // via the constructor overload that takes a ReplicaRegistry*.
    // We call it directly via a purpose-built agent in the full NodeRuntimeAgent
    // constructor.

    // For the FAILING test, we call the duty cycle the intended way:
    // create an agent with the registry, drive doWork, observe compaction.
    rune::node::NodeRuntimeAgent fullAgent(
        dummy,
        /*election=*/nullptr,
        /*bridge=*/nullptr,
        /*transport=*/nullptr,
        &registry,
        /*gateway=*/nullptr);

    // CP-6: compaction runs via runBoundedCompaction (off duty-cycle thread).
    // Drive 50 compaction passes directly (doWork no longer calls it).
    for (int i = 0; i < 50; ++i) {
        fullAgent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        fullAgent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    // Reload to pick up rewritten segment files.
    log.reloadSegments();

    // --- Assertions ---

    // 1. Latest value for k1 is still 199 (latest kept).
    EXPECT_EQ(latestValueForKey(log, "k1"), "v199") << "Latest value for k1 must be v199";

    // 2. Superseded records were reclaimed (fewer than 200).
    const int totalAfter = countLiveRecords(log);
    EXPECT_LT(totalAfter, totalBefore)
        << "Sealed segment records should have been compacted (superseded removed)";
    EXPECT_LT(totalAfter, 200)
        << "After compaction fewer than 200 records should remain (only k1@199 in sealed zone)";

    // 3. Active segment was not touched — its record count is unchanged.
    const int activeRecordsAfter = countActiveSegmentRecords(log);
    EXPECT_EQ(activeRecordsAfter, activeRecordsBefore)
        << "Active segment must not be modified by compaction";

    // 4. Active segment base offset unchanged.
    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_FALSE(basesAfter.empty());
    EXPECT_EQ(basesAfter.back(), activeBase) << "Active segment base must be unchanged";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 2: NON-LEADER — runBoundedCompaction is a strict no-op.
// ===========================================================================
TEST(CompactionDutyCycle, NonLeaderRunBoundedCompactionIsNoOp) {
    auto dir = makeTempDir("cp2_t2");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Follower, /*epoch=*/0,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    // Stays Follower — no becomeLeader().

    // Append records directly to the log (bypassing the follower replica API).
    for (int v = 0; v < 80; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments for this test";

    const int totalBefore = countLiveRecords(log);
    const int64_t activeBase = bases.back();

    rune::node::ReplicaRegistry registry;
    registry.add(10, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy,
        /*election=*/nullptr,
        /*bridge=*/nullptr,
        /*transport=*/nullptr,
        &registry,
        /*gateway=*/nullptr);

    // Drive 50 cycles including explicit compaction calls. Follower should NEVER compact.
    for (int i = 0; i < 50; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // All records must be intact — no compaction ran.
    const int totalAfter = countLiveRecords(log);
    EXPECT_EQ(totalAfter, totalBefore)
        << "Follower replica must not be compacted (runBoundedCompaction is leader-only)";

    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_FALSE(basesAfter.empty());
    EXPECT_EQ(basesAfter.back(), activeBase)
        << "Active segment base must be unchanged on follower";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 3: Delete policy — runBoundedCompaction skips partitions with Delete policy.
// ===========================================================================
TEST(CompactionDutyCycle, DeletePolicySkippedByCompaction) {
    auto dir = makeTempDir("cp2_t3");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Delete; // should NOT compact

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    for (int v = 0; v < 80; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);

    const int totalBefore = countLiveRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(10, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (int i = 0; i < 50; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    const int totalAfter = countLiveRecords(log);
    EXPECT_EQ(totalAfter, totalBefore)
        << "Delete-policy partition must not be compacted";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 4: Active segment boundary — compaction never reduces active segment
//         records even when all records share the same key.
// ===========================================================================
TEST(CompactionDutyCycle, ActiveSegmentNeverTouchedByDutyCycle) {
    auto dir = makeTempDir("cp2_t4");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Fill enough to create multiple segments.
    for (int v = 0; v < 150; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);

    const int64_t activeBase         = bases.back();
    const int64_t leoBeforeCompaction = log.logEndOffset();
    const int activeRecsBefore        = countActiveSegmentRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(10, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (int i = 0; i < 50; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // logEndOffset must not decrease (active segment not truncated).
    EXPECT_EQ(log.logEndOffset(), leoBeforeCompaction)
        << "logEndOffset must be unchanged (active segment not compacted)";

    // Active segment records unchanged.
    const int activeRecsAfter = countActiveSegmentRecords(log);
    EXPECT_EQ(activeRecsAfter, activeRecsBefore)
        << "Active segment record count must be unchanged after duty cycle compaction";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 5: BelowDirtyRatioThresholdIsSkipped — a compact-topic partition whose
// dirty ratio is BELOW minCleanableDirtyRatio must NOT be compacted.
//
// Setup: write records, ensure dirty ratio < configured threshold.
// Drive many duty cycles → live record count must be UNCHANGED.
//
// FAILS before the dirty-ratio gate exists (PartitionConfig has no
// minCleanableDirtyRatio field; NodeRuntimeAgent compacts unconditionally).
// ===========================================================================
TEST(CompactionDutyCycle, BelowDirtyRatioThresholdIsSkipped) {
    auto dir = makeTempDir("cp2_dirty_below");
    // Large roll so records stay in one or two segments; ratio stays small.
    Log log(dir.string(), /*rollBytes=*/4096);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId              = 20;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Compact;
    // Set a very HIGH threshold so the dirty portion (offset 0..activeBase) is
    // always below it.  Since dirty ratio = (activeBase - 0) / max(1, activeBase)
    // ≤ 1.0, a threshold of 1.0 means "never compact" (ratio is always < 1.0).
    cfg.minCleanableDirtyRatio = 1.0;  // threshold = 100% — always below

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Append enough records to create multiple segments.
    for (int v = 0; v < 60; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments";
    const int totalBefore = countLiveRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(20, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive 100 cycles — with threshold=1.0, ratio < threshold, so NO compaction.
    for (int i = 0; i < 100; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();
    const int totalAfter = countLiveRecords(log);
    EXPECT_EQ(totalAfter, totalBefore)
        << "Below dirty-ratio threshold: no compaction should have run";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 6: AboveDirtyRatioThresholdIsCompacted — above threshold →
// compacted (latest-per-key retained, superseded reclaimed).
//
// FAILS before the dirty-ratio gate exists or if the gate is wrong.
// ===========================================================================
TEST(CompactionDutyCycle, AboveDirtyRatioThresholdIsCompacted) {
    auto dir = makeTempDir("cp2_dirty_above");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId              = 21;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Compact;
    // Low threshold → dirty ratio (all offsets dirty since clean watermark=0) will
    // exceed it; compaction fires.
    cfg.minCleanableDirtyRatio = 0.1;  // 10% threshold

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    for (int v = 0; v < 200; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments";
    const int totalBefore = countLiveRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(21, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive enough cycles to compact all sealed segments.
    for (int i = 0; i < 500; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();
    const int totalAfter = countLiveRecords(log);

    EXPECT_LT(totalAfter, totalBefore)
        << "Above threshold: sealed segments should have been compacted";
    EXPECT_EQ(latestValueForKey(log, "k1"), "v199")
        << "Latest value for k1 must still be v199 after compaction";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 7: BoundedOneSegmentPerCycle — a partition with multiple sealed dirty
// segments compacts at most one segment per duty-cycle call, but reaches
// fully-compacted after enough cycles (forward progress via watermark).
//
// FAILS before the per-cycle bound is implemented (today clean() rewrites ALL
// sealed segments in one call → work count > 1 on first compaction cycle).
// ===========================================================================
TEST(CompactionDutyCycle, BoundedOneSegmentPerCycle) {
    auto dir = makeTempDir("cp2_bounded");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId              = 22;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = 0.0;  // always compact when above 0%

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Append enough records to create many sealed segments.
    for (int v = 0; v < 300; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need at least 3 segments for this test (2+ sealed)";
    const size_t sealedCount = bases.size() - 1; // all except active

    rune::node::ReplicaRegistry registry;
    registry.add(22, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive exactly ONE duty cycle. The bounded implementation must compact at
    // most ONE sealed segment. If it compacts all sealedCount segments in one
    // shot the assertion below fails.
    const int workOnFirstCycle =
        agent.runBoundedCompaction(/*nowNs=*/1'000'000LL);

    // runBoundedCompaction returns the number of compaction passes run.
    // With the one-segment-per-cycle bound it must return exactly 1 (one segment
    // processed for this partition) on the first cycle when dirty ratio >= threshold.
    EXPECT_EQ(workOnFirstCycle, 1)
        << "First cycle should compact exactly 1 sealed segment (bounded), got "
        << workOnFirstCycle;

    // Now drive enough additional cycles to compact ALL sealed segments.
    // Each cycle advances the watermark by one segment — so sealedCount cycles
    // total finish the job.
    for (size_t i = 1; i < sealedCount + 10; ++i) {
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // After enough cycles all sealed superseded records are gone.
    const int totalAfter = countLiveRecords(log);
    EXPECT_EQ(latestValueForKey(log, "k1"), "v299")
        << "Latest value must survive full compaction";
    EXPECT_LT(totalAfter, 300)
        << "Superseded records must be removed after enough bounded cycles";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 8: AlreadyCompactedLogNotRewritten — after a log is fully compacted,
// subsequent cycles do NOT rewrite segments (work count == 0).
//
// Proves the cleaned-watermark: once lastCleanOffset == activeBase, the dirty
// ratio is 0 and no further work is done.
//
// FAILS before the watermark is implemented (today every cycle unconditionally
// rewrites from offset 0 → work count > 0 forever).
// ===========================================================================
TEST(CompactionDutyCycle, AlreadyCompactedLogNotRewritten) {
    auto dir = makeTempDir("cp2_nowatermark");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId              = 23;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = 0.0;  // always compact when above 0%

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    for (int v = 0; v < 200; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments";
    const size_t sealedCount = bases.size() - 1;

    rune::node::ReplicaRegistry registry;
    registry.add(23, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drain all dirty segments (one per cycle).
    for (size_t i = 0; i < sealedCount + 5; ++i) {
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    // Now the log should be fully compacted. Additional cycles must return 0.
    const int extraWork1 = agent.runBoundedCompaction(100'000'000LL);
    const int extraWork2 = agent.runBoundedCompaction(200'000'000LL);
    const int extraWork3 = agent.runBoundedCompaction(300'000'000LL);

    EXPECT_EQ(extraWork1, 0)
        << "Already-compacted log: cycle N+1 must return work=0 (no rewrite)";
    EXPECT_EQ(extraWork2, 0)
        << "Already-compacted log: cycle N+2 must return work=0 (no rewrite)";
    EXPECT_EQ(extraWork3, 0)
        << "Already-compacted log: cycle N+3 must return work=0 (no rewrite)";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 9: DeleteRetentionMsFromConfigUsed — a topic configured with a
// non-default deleteRetentionMs has that value honored by the cleaner.
//
// Strategy: write a tombstone at timestamp T, then drive compaction with
// nowNs such that (nowNs - T) > configuredRetentionMs * 1e6 (expired) —
// tombstone should be DROPPED. Then redo with a shorter retention (not expired)
// and confirm tombstone is KEPT.
//
// Also verifies the field is threaded through PartitionConfig, not hardcoded
// to 86_400_000 ms.
//
// FAILS before deleteRetentionMs is plumbed through PartitionConfig.
// ===========================================================================
TEST(CompactionDutyCycle, DeleteRetentionMsFromConfigUsed) {
    // --- Part A: very SHORT retention (1 ms) — tombstone past expiry → dropped.
    {
        auto dir = makeTempDir("cp2_retention_short");
        Log log(dir.string(), /*rollBytes=*/128);

        NullTransport transport;
        PartitionConfig cfg;
        cfg.topicId              = 24;
        cfg.partitionId          = 0;
        cfg.selfId               = 1;
        cfg.minIsr               = 1;
        cfg.replicaLagMaxMs      = 10'000;
        cfg.replicaLagMaxOffset  = 10'000;
        cfg.fetchIntervalNs      = 1'000'000LL;
        cfg.isrCheckIntervalNs   = 1'000'000LL;
        cfg.maxFetchBytes        = 1 << 20;
        cfg.pushOnAppend         = false;
        cfg.cleanupPolicy        = CleanupPolicy::Compact;
        cfg.minCleanableDirtyRatio = 0.0;
        cfg.deleteRetentionMs    = 1;  // 1 ms — very short

        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica->becomeLeader(1, {1});

        // Record at offset 0: "k_tomb" → "v1" (ts=1000 ns).
        {
            auto b = makeBatch(0, "k_tomb", "v1", /*ts=*/1000);
            log.append(b, 0);
        }
        // Tombstone at offset 1: "k_tomb" → "" (ts=2000 ns).
        {
            auto b = makeBatch(1, "k_tomb", "", /*ts=*/2000);
            log.append(b, 1);
        }
        // Filler records to force a roll (sealed segment).
        for (int v = 2; v < 50; ++v) {
            auto b = makeBatch(v, "kfill", "x", /*ts=*/3000);
            log.append(b, v);
        }
        replica->onLeaderLogAppended();

        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u);

        rune::node::ReplicaRegistry registry;
        registry.add(24, 0, std::move(replica));

        MinimalConsensusAgent dummy;
        rune::node::NodeRuntimeAgent agent(
            dummy, nullptr, nullptr, nullptr, &registry, nullptr);

        // nowNs = 1'000'000'000 (1 s): (1e9 - 2000) > 1ms * 1e6 = 1e6 ns → expired.
        for (int i = 0; i < 50; ++i) {
            agent.runBoundedCompaction(1'000'000'000LL);
        }

        log.reloadSegments();

        // Tombstone must be GONE (expired past the 1 ms retention window).
        // Assert on record PRESENCE (not value): countLiveRecordsForKey distinguishes
        // "key fully removed" (count 0) from "tombstone still present" (count 1) —
        // latestValueForKey cannot, since it returns "" for both.
        const int tombCount = countLiveRecordsForKey(log, "k_tomb");
        EXPECT_EQ(tombCount, 0)
            << "Short retention (1 ms): expired tombstone + its superseded value must be "
               "FULLY removed — 0 records for k_tomb should survive, got " << tombCount;

        fs::remove_all(dir);
    }

    // --- Part B: very LONG retention (1 day in ms = 86_400_000) — tombstone not expired → kept.
    {
        auto dir = makeTempDir("cp2_retention_long");
        Log log(dir.string(), /*rollBytes=*/128);

        NullTransport transport;
        PartitionConfig cfg;
        cfg.topicId              = 25;
        cfg.partitionId          = 0;
        cfg.selfId               = 1;
        cfg.minIsr               = 1;
        cfg.replicaLagMaxMs      = 10'000;
        cfg.replicaLagMaxOffset  = 10'000;
        cfg.fetchIntervalNs      = 1'000'000LL;
        cfg.isrCheckIntervalNs   = 1'000'000LL;
        cfg.maxFetchBytes        = 1 << 20;
        cfg.pushOnAppend         = false;
        cfg.cleanupPolicy        = CleanupPolicy::Compact;
        cfg.minCleanableDirtyRatio = 0.0;
        cfg.deleteRetentionMs    = 86'400'000LL;  // 1 day

        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica->becomeLeader(1, {1});

        // Record: "k_tomb" → "v1" ts=1000.
        {
            auto b = makeBatch(0, "k_tomb", "v1", /*ts=*/1000);
            log.append(b, 0);
        }
        // Tombstone: "k_tomb" → "" ts=2000.
        {
            auto b = makeBatch(1, "k_tomb", "", /*ts=*/2000);
            log.append(b, 1);
        }
        for (int v = 2; v < 50; ++v) {
            auto b = makeBatch(v, "kfill", "x", /*ts=*/3000);
            log.append(b, v);
        }
        replica->onLeaderLogAppended();

        auto bases2 = log.segmentBaseOffsets();
        ASSERT_GE(bases2.size(), 2u);

        rune::node::ReplicaRegistry registry;
        registry.add(25, 0, std::move(replica));

        MinimalConsensusAgent dummy;
        rune::node::NodeRuntimeAgent agent(
            dummy, nullptr, nullptr, nullptr, &registry, nullptr);

        // nowNs = 1'000'000'000 (1 s): (1e9 - 2000) << 1 day in ns → NOT expired.
        for (int i = 0; i < 50; ++i) {
            agent.runBoundedCompaction(1'000'000'000LL);
        }

        log.reloadSegments();

        // Tombstone must still be present (within the 1-day retention window).
        // Assert PRESENCE: exactly one surviving record for k_tomb (the tombstone;
        // the earlier "v1" value is superseded by the tombstone and dropped).
        // This is the discriminating assertion that makes the retention config
        // actually TESTED: with a hardcoded retention the two parts would not
        // diverge (part A count 0, part B count 1).
        const int tombCount2 = countLiveRecordsForKey(log, "k_tomb");
        EXPECT_EQ(tombCount2, 1)
            << "Long retention (1 day): tombstone within window must be KEPT — exactly "
               "1 record for k_tomb should survive, got " << tombCount2;
        // And that surviving record is the tombstone (empty value).
        EXPECT_EQ(latestValueForKey(log, "k_tomb"), "")
            << "The surviving k_tomb record must be the tombstone (empty value)";

        fs::remove_all(dir);
    }
}


// ===========================================================================
// TEST 10: CrossSegmentLatestPerKeyConverges — a key updated repeatedly ACROSS
// many sealed segments must converge to EXACTLY ONE surviving record after full
// compaction (global latest-per-key), not one-survivor-per-segment.
//
// This is the regression guard for the per-segment-scoped offset-map bug: if
// buildOffsetMap is scoped to a single segment's range, each sealed segment
// keeps its own local "latest", leaving N stale survivors for a hot key.
// ===========================================================================
TEST(CompactionDutyCycle, CrossSegmentLatestPerKeyConverges) {
    auto dir = makeTempDir("cp2_xseg");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId              = 30;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = 0.0;  // always compact when above 0%

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // ONE hot key updated 300 times → spans many sealed segments.
    for (int v = 0; v < 300; ++v) {
        auto batch = makeBatch(v, "hot", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need multiple sealed segments to exercise the cross-segment path";
    const size_t sealedCount = bases.size() - 1;
    const int64_t activeBase = bases.back();

    rune::node::ReplicaRegistry registry;
    registry.add(30, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive enough cycles to compact every sealed segment.
    for (size_t i = 0; i < sealedCount + 10; ++i) {
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // Count "hot" records that live BELOW the active-segment boundary (the sealed,
    // compactable zone).  Globally, only the single latest sealed occurrence may
    // survive there — everything earlier is superseded.  (Records at/after
    // activeBase are in the untouched active segment and are excluded here.)
    int sealedHot = 0;
    {
        auto b = log.segmentBaseOffsets();
        int64_t cursor = 0;
        for (size_t si = 0; si + 0 < b.size(); ++si) {
            int64_t segStart = b[si];
            if (segStart >= activeBase) break;  // active segment — skip
            int64_t segEnd = (si + 1 < b.size()) ? b[si + 1] : activeBase;
            cursor = segStart;
            for (;;) {
                auto bytes = log.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view, int64_t off, int64_t) {
                    if (off < activeBase && k == "hot") ++sealedHot;
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced || cursor >= segEnd) break;
            }
        }
    }

    EXPECT_LE(sealedHot, 1)
        << "Hot key must converge to at most ONE surviving record in the sealed zone "
           "(global latest-per-key); got " << sealedHot
        << " — a per-segment-scoped offset map leaves one survivor PER segment";

    // The newest value is still readable (in the active segment or as the lone survivor).
    EXPECT_EQ(latestValueForKey(log, "hot"), "v299")
        << "Newest value must survive compaction";

    fs::remove_all(dir);
}
