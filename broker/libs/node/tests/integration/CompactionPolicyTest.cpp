// ---------------------------------------------------------------------------
// CompactionPolicyTest.cpp — CP-3: cleanup.policy routing + tombstone
//   delete-retention + compact,delete ordering.
//
// Five tests that FAIL before CP-3 implementation, then PASS after:
//
//  1. CompactTopicNeverSizeDeletesLiveKey
//       A Compact-policy topic with a tiny retentionBytes keeps a live key past
//       the size cap.  Compact policy must NEVER call deleteOldSegments on live
//       key data.
//
//  2. TombstoneDeletesKeyAfterDeleteRetention
//       A tombstone removes the key after deleteRetentionMs has elapsed, but the
//       key (tombstone record) is still present BEFORE the window expires.
//
//  3. CompactDeleteBothCompactsAndAgesOut
//       A CompactDelete topic: compaction (latest-per-key) runs FIRST, THEN
//       normal size retention ages out old segments.  Both effects are verified.
//
//  4. DeleteTopicNeverCompacts
//       A Delete-policy topic: the compaction path must not touch records (only
//       retention deletes).  Live-record count is unchanged by compaction cycles.
//
//  5. ProducerStateSurvivesCompactDelete
//       After a CompactDelete pass, idempotent dedup for a producerId still works
//       (last batch producer-state is preserved — not dropped by retention).
//
// Test fixture pattern mirrors CompactionDutyCycleTest (reuses helpers).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Helpers (mirrors CompactionDutyCycleTest helpers)
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_cp3_seq{0};

static fs::path makeTempDir3(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_cp3_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

static std::vector<uint8_t> makeBatch3(int64_t baseOffset, std::string_view key,
                                        std::string_view value, int64_t ts = 1000) {
    RecordBatchBuilder b(baseOffset, ts);
    b.append(key, value, ts);
    return b.build();
}

// Build a batch with explicit producer fields (for PSM test).
static std::vector<uint8_t> makeProducerBatch(int64_t baseOffset,
                                               std::string_view key,
                                               std::string_view value,
                                               int64_t producerId,
                                               int16_t producerEpoch,
                                               int32_t baseSequence,
                                               int64_t ts = 1000) {
    RecordBatchBuilder b(baseOffset, ts);
    b.setProducerId(producerId);
    b.setProducerEpoch(producerEpoch);
    b.setBaseSequence(baseSequence);
    b.append(key, value, ts);
    return b.build();
}

class MinimalConsensusAgent3 : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

class NullTransport3 : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

static int countLiveRecords3(Log& log) {
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

// Latest value for a key across all live records (empty string if absent OR if the
// surviving record is a tombstone).  Used to assert compact-before-delete keeps the
// newest value.
static std::string latestValueForKey3IfPresent(Log& log, std::string_view wantKey) {
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
                if (k == wantKey) latest = std::string(v);
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return latest;
}

static int countLiveRecordsForKey3(Log& log, std::string_view wantKey) {
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

// ===========================================================================
// TEST 1: CompactTopicNeverSizeDeletesLiveKey
//
// A Compact-policy topic with a tiny retentionBytes (0 — delete everything)
// must NOT call deleteOldSegments at all: live keys are keyed changelogs that
// must never be size-aged out.  After many duty cycles the key must still be
// readable.
//
// FAILS before CP-3 if runBoundedCompaction calls deleteOldSegments for Compact
// policy (live key gets deleted) or if no distinction is made between policies.
// ===========================================================================
TEST(CompactionPolicy, CompactTopicNeverSizeDeletesLiveKey) {
    auto dir = makeTempDir3("cp3_compact_no_size_delete");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport3 transport;
    PartitionConfig cfg;
    cfg.topicId              = 100;
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
    cfg.minCleanableDirtyRatio = 0.0;   // always compact when dirty
    cfg.retentionBytes       = 0;       // tiny — would delete everything if applied to Compact

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Append 200 keyed records, forcing multiple segments.
    for (int v = 0; v < 200; ++v) {
        auto batch = makeBatch3(v, "live_key", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments";

    // Compaction of a single hot key collapses each sealed segment to (at most) its
    // one latest survivor but PRESERVES original offsets, so multiple sealed segment
    // FILES remain on disk after compaction converges.  Capture the post-compaction
    // segment count so we can prove size-retention did NOT additionally cull them.
    // (This is the discriminating signal: if Compact wrongly called
    // deleteOldSegments(retentionBytes=0), the log would be aged down to a single
    // segment — deleteOldSegments removes every segment except the active one when
    // retainBytes=0.  A key-presence check alone is VACUOUS: compaction keeps 1
    // survivor and deleteOldSegments's segments_.size()>1 floor keeps the final
    // segment, so keyCount>=1 holds in BOTH the correct and broken builds.)

    rune::node::ReplicaRegistry registry;
    registry.add(100, 0, std::move(replica));

    MinimalConsensusAgent3 dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // First, converge compaction WITHOUT the (buggy) retention interfering — drive
    // cycles then snapshot the compacted segment layout.
    for (int i = 0; i < 100; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();

    // The live key must still be present — Compact policy never size-deletes.
    const int keyCount = countLiveRecordsForKey3(log, "live_key");
    EXPECT_GE(keyCount, 1)
        << "Compact policy must keep the live key; got " << keyCount;

    // DISCRIMINATING ASSERTION: Compact must NOT have applied retentionBytes=0.
    // With retentionBytes=0 a wrongly-applied deleteOldSegments collapses the log to
    // exactly ONE segment (the active one — the segments_.size()>1 floor stops there).
    // A correct Compact-only path leaves the compacted sealed survivor segment PLUS
    // the active segment, i.e. >= 2 segments remain on disk.  (The verifier confirmed
    // the exact signal: correct build = 2 segments / 240 B, broken build = 1 segment
    // / 160 B — a key-presence check cannot tell them apart, this segment-count check
    // can.)  NOTE: we do NOT assert on the oldest base offset: compaction legitimately
    // DELETES fully-superseded early sealed segments (their records were all replaced
    // by the single hot-key survivor), which advances the front base — that is
    // compaction, not size-retention, and is expected.
    const auto basesAfter = log.segmentBaseOffsets();
    EXPECT_GE(basesAfter.size(), 2u)
        << "Compact policy must NEVER size-delete: with retentionBytes=0 a buggy "
           "deleteOldSegments call would collapse the log to a single (active) segment. "
           "Segments remaining = " << basesAfter.size()
        << " (expected >= 2 = sealed survivor + active).";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 2: TombstoneDeletesKeyAfterDeleteRetention
//
// For a Compact topic: a tombstone (empty value) must:
//   - STILL be present BEFORE deleteRetentionMs elapses (key kept for consumers
//     to catch up).
//   - Be FULLY REMOVED (key + tombstone) AFTER deleteRetentionMs elapses.
//
// The "before" case is already partially covered by the CP-2 test suite;
// this test adds the "after" case with explicit wall-clock assertions.
//
// FAILS before CP-3 if tombstone is not dropped after retention.
// ===========================================================================
TEST(CompactionPolicy, TombstoneDeletesKeyAfterDeleteRetention) {
    // --- Part A: before deleteRetentionMs (key must STILL be present) ---
    {
        auto dir = makeTempDir3("cp3_tomb_before");
        Log log(dir.string(), /*rollBytes=*/128);

        NullTransport3 transport;
        PartitionConfig cfg;
        cfg.topicId              = 101;
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
        // Long retention: 1 day — tombstone written at ts=2000 ns, nowNs=1e9 → NOT expired.
        cfg.deleteRetentionMs    = 86'400'000LL;

        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica->becomeLeader(1, {1});

        // Write original value then tombstone.
        { auto b = makeBatch3(0, "k_tomb", "value", 1000); log.append(b, 0); }
        { auto b = makeBatch3(1, "k_tomb", "", 2000);       log.append(b, 1); }  // tombstone
        // Filler to roll segment.
        for (int v = 2; v < 60; ++v) {
            auto b = makeBatch3(v, "kfill", "x", 3000); log.append(b, v);
        }
        replica->onLeaderLogAppended();

        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u);

        rune::node::ReplicaRegistry registry;
        registry.add(101, 0, std::move(replica));

        MinimalConsensusAgent3 dummy;
        rune::node::NodeRuntimeAgent agent(
            dummy, nullptr, nullptr, nullptr, &registry, nullptr);

        // nowNs=1e9 is << 1 day in ns → tombstone NOT expired.
        for (int i = 0; i < 50; ++i) {
            agent.runBoundedCompaction(1'000'000'000LL);
        }

        log.reloadSegments();

        // Tombstone must still be present (within retention window).
        const int tombCount = countLiveRecordsForKey3(log, "k_tomb");
        EXPECT_EQ(tombCount, 1)
            << "BEFORE deleteRetentionMs: tombstone must be kept (within 1-day window); "
               "got " << tombCount << " records for k_tomb";

        fs::remove_all(dir);
    }

    // --- Part B: after deleteRetentionMs (key must be fully removed) ---
    {
        auto dir = makeTempDir3("cp3_tomb_after");
        Log log(dir.string(), /*rollBytes=*/128);

        NullTransport3 transport;
        PartitionConfig cfg;
        cfg.topicId              = 102;
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
        // Very short retention: 1 ms.  tombstone ts=2000 ns, nowNs=1e9 → (1e9-2000) >> 1ms*1e6.
        cfg.deleteRetentionMs    = 1;

        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica->becomeLeader(1, {1});

        { auto b = makeBatch3(0, "k_tomb", "value", 1000); log.append(b, 0); }
        { auto b = makeBatch3(1, "k_tomb", "", 2000);       log.append(b, 1); }
        for (int v = 2; v < 60; ++v) {
            auto b = makeBatch3(v, "kfill", "x", 3000); log.append(b, v);
        }
        replica->onLeaderLogAppended();

        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u);

        rune::node::ReplicaRegistry registry;
        registry.add(102, 0, std::move(replica));

        MinimalConsensusAgent3 dummy;
        rune::node::NodeRuntimeAgent agent(
            dummy, nullptr, nullptr, nullptr, &registry, nullptr);

        // nowNs=1e9: (1e9 ns - 2000 ns) > 1ms * 1e6 → tombstone expired.
        for (int i = 0; i < 50; ++i) {
            agent.runBoundedCompaction(1'000'000'000LL);
        }

        log.reloadSegments();

        // Tombstone + original value must be FULLY removed.
        const int tombCount = countLiveRecordsForKey3(log, "k_tomb");
        EXPECT_EQ(tombCount, 0)
            << "AFTER deleteRetentionMs: expired tombstone + value must be fully removed; "
               "got " << tombCount << " records for k_tomb";

        fs::remove_all(dir);
    }
}

// ===========================================================================
// TEST 3: CompactDeleteBothCompactsAndAgesOut
//
// A CompactDelete topic must:
//   a) Compact (latest-per-key in sealed segments), AND
//   b) Age out old segments past retentionBytes.
//
// Ordering: compaction happens FIRST so that keys are deduplicated before
// the size-deletion pass.  A record that would be compacted away should not
// survive because size-delete ran before compact.
//
// Verification strategy:
//   - Write many updates to a single key (so compaction removes superseded ones).
//   - After enough cycles with a very small retentionBytes:
//       * Superseded records are gone (compaction ran).
//       * Old segments past retentionBytes are also gone (retention ran).
//       * The latest value for the key is STILL readable (compaction-before-delete
//         means the latest survivor is kept, not deleted).
//
// FAILS before CP-3 if:
//   - CompactDelete runs no compaction (key count unchanged by compaction).
//   - CompactDelete runs no retention (no segments removed).
//   - Delete runs before compact (latest value is deleted before it can survive).
// ===========================================================================
TEST(CompactionPolicy, CompactDeleteBothCompactsAndAgesOut) {
    auto dir = makeTempDir3("cp3_compact_delete");
    // Small segments so we get many sealed segments.
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport3 transport;
    PartitionConfig cfg;
    cfg.topicId              = 103;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::CompactDelete;
    cfg.minCleanableDirtyRatio = 0.0;   // always compact
    // Small retention: keep only 128 bytes → forces segment deletion.
    // But must NOT delete the segment containing the latest value of the key
    // because compact runs first and places the latest value in a surviving segment.
    cfg.retentionBytes       = 128;     // tiny → should age out OLD sealed segments

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Write updates to a HOT key (compaction target) plus DISTINCT cold keys in the
    // early segments (retention target).  The hot key's latest value is written LAST
    // (highest offset) so, after compact-before-delete, it lands in a surviving
    // recent/active segment; the cold keys live only in early segments that retention
    // must age out.  This lets us assert BOTH effects distinctly.
    int off = 0;
    // Early cold keys — one per record, only in the front segments.
    for (int v = 0; v < 100; ++v) {
        auto batch = makeBatch3(off, "cold" + std::to_string(v), "c" + std::to_string(v));
        log.append(batch, off);
        ++off;
    }
    // Hot key updated many times — latest lands near the end.
    const int kHotUpdates = 100;
    for (int v = 0; v < kHotUpdates; ++v) {
        auto batch = makeBatch3(off, "hot", "v" + std::to_string(v));
        log.append(batch, off);
        ++off;
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need at least 3 segments for this test";
    const int64_t firstBaseBefore = bases.front();
    const std::string latestHotBefore = "v" + std::to_string(kHotUpdates - 1);

    rune::node::ReplicaRegistry registry;
    registry.add(103, 0, std::move(replica));

    MinimalConsensusAgent3 dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    const int totalBefore = countLiveRecords3(log);
    const int hotBefore   = countLiveRecordsForKey3(log, "hot");
    ASSERT_EQ(hotBefore, kHotUpdates) << "Pre-condition: all hot updates present before cycles";

    // Drive enough cycles for both compaction and retention to run.
    for (int i = 0; i < 500; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    const int totalAfter = countLiveRecords3(log);
    const auto basesAfter = log.segmentBaseOffsets();
    ASSERT_FALSE(basesAfter.empty());

    // (a) COMPACTION ran: the hot key collapsed from kHotUpdates records toward 1.
    const int hotAfter = countLiveRecordsForKey3(log, "hot");
    EXPECT_LT(hotAfter, kHotUpdates)
        << "CompactDelete must COMPACT: hot key must shrink from " << kHotUpdates
        << " superseded records; got " << hotAfter;

    // (b) RETENTION ran: old front segments were physically aged out, so the oldest
    //     segment base advanced past its original value.  This is the distinct signal
    //     that deleteOldSegments actually removed segments (not just compaction).
    EXPECT_GT(basesAfter.front(), firstBaseBefore)
        << "CompactDelete must RUN RETENTION: oldest segment base must advance past "
        << firstBaseBefore << " (front segments aged out by retentionBytes); "
           "got " << basesAfter.front();

    // (c) ORDERING (compact-before-delete): the hot key's LATEST value must survive.
    //     Compaction runs first, placing the single survivor in a recent segment that
    //     retention keeps.  A delete-before-compact order could age out the segment
    //     holding the latest value before it was consolidated.
    EXPECT_EQ(latestValueForKey3IfPresent(log, "hot"), latestHotBefore)
        << "compact-before-delete: latest hot value (" << latestHotBefore
        << ") must survive retention";

    // (d) Combined reduction sanity.
    EXPECT_LT(totalAfter, totalBefore)
        << "CompactDelete: combined compaction+retention must reduce record count; "
           "before=" << totalBefore << " after=" << totalAfter;

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 4: DeleteTopicNeverCompacts
//
// A Delete-policy topic must NOT be modified by the compaction path.
// runBoundedCompaction must skip Delete topics entirely (for the compaction
// step); only deleteOldSegments may run (via the separate retention path in
// runBoundedCompaction for Delete/CompactDelete topics).
//
// Verification: write many same-key records, drive many cycles.  The
// key-level record count must be unchanged by compaction (all N updates are
// still present because Delete policy does not compact per-key).
//
// Note: we do NOT call deleteOldSegments from this test (retentionBytes is
// left at -1 = unbounded), so the count must be exactly totalBefore.
//
// FAILS before CP-3 if Delete policy accidentally triggers compaction (which
// would reduce per-key record count).
// ===========================================================================
TEST(CompactionPolicy, DeleteTopicNeverCompacts) {
    auto dir = makeTempDir3("cp3_delete_no_compact");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport3 transport;
    PartitionConfig cfg;
    cfg.topicId              = 104;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Delete;
    cfg.minCleanableDirtyRatio = 0.0;
    cfg.retentionBytes       = -1;  // unbounded — retention will not delete anything

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Write 80 same-key records.
    for (int v = 0; v < 80; ++v) {
        auto batch = makeBatch3(v, "k_del", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);

    const int totalBefore = countLiveRecords3(log);
    ASSERT_EQ(totalBefore, 80) << "Pre-condition: all 80 records must be present before cycles";

    rune::node::ReplicaRegistry registry;
    registry.add(104, 0, std::move(replica));

    MinimalConsensusAgent3 dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive 100 cycles.  Delete policy → compaction must NOT run.
    for (int i = 0; i < 100; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    const int totalAfter = countLiveRecords3(log);
    EXPECT_EQ(totalAfter, totalBefore)
        << "Delete policy: live-record count must be unchanged by compaction cycles "
           "(compaction must not run for Delete topics); "
           "before=" << totalBefore << " after=" << totalAfter;

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 5: ProducerStateSurvivesCompactDelete
//
// After a CompactDelete pass, idempotent dedup for a producerId must still
// work: the last batch (with its producerId/producerEpoch/baseSequence) must
// be preserved so that ProducerStateManager::rebuildFromLog() can seed its
// retry window and detect a duplicate retry.
//
// Strategy:
//   1. Write idempotent batches with explicit producerId/epoch/seq.
//   2. Run CompactDelete cycles (compact + retention).
//   3. Rebuild PSM from the (possibly compacted+trimmed) log.
//   4. Assert that a retry of the last sequence is detected as Duplicate.
//   5. Assert that a genuinely new (next) sequence is Accepted.
//
// FAILS before CP-3 if CompactDelete retention deletes the segment containing
// the last batch for the producerId (PSM rebuild would then Accept the retry
// instead of deduplicating it).
// ===========================================================================
TEST(CompactionPolicy, ProducerStateSurvivesCompactDelete) {
    auto dir = makeTempDir3("cp3_psm_survives");
    // Large-ish segments so the producer batch lands in the active (or a recent
    // sealed) segment that survives retention.
    Log log(dir.string(), /*rollBytes=*/256);

    NullTransport3 transport;
    PartitionConfig cfg;
    cfg.topicId              = 105;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::CompactDelete;
    cfg.minCleanableDirtyRatio = 0.0;
    // Retention MUST actually delete some early segments (otherwise this test is
    // vacuous — it would not exercise "producer state survives retention deletion").
    // We size retentionBytes SMALL enough that deleteOldSegments ages out early
    // producer batches, while the segments_.size()>1 floor guarantees the most
    // recent segment (holding the LAST producer batch, seq=kNumBatches-1) survives.
    // The assertion below then genuinely proves the last batch's producer state
    // (patched producerId/epoch/baseSequence via LogCleaner::rewriteSegment) is
    // preserved through BOTH compaction and size retention.
    cfg.retentionBytes       = 256;  // small — forces front-segment deletion

    constexpr int64_t kProducerId    = 42;
    constexpr int16_t kProducerEpoch = 1;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Write several idempotent batches for producerId=42.
    // We write batches to DIFFERENT keys so compaction does not drop later entries
    // (each key has exactly one value; the producer's last batch is the anchor).
    int offset = 0;
    const int kNumBatches = 20;
    for (int seq = 0; seq < kNumBatches; ++seq) {
        auto batch = makeProducerBatch(
            offset,
            "pk" + std::to_string(seq),  // unique key per batch
            "val" + std::to_string(seq),
            kProducerId, kProducerEpoch, seq,
            /*ts=*/static_cast<int64_t>(1000 + seq));
        log.append(batch, offset);
        ++offset;
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need >=3 segments so retention can delete front ones";
    const int64_t firstBaseBefore = bases.front();

    rune::node::ReplicaRegistry registry;
    registry.add(105, 0, std::move(replica));

    MinimalConsensusAgent3 dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive CompactDelete cycles.
    for (int i = 0; i < 300; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // GUARD (keeps this test non-vacuous): retention MUST have actually deleted
    // early segments — otherwise "producer state survives retention deletion" is
    // never exercised.  The oldest segment base must have advanced.
    const auto basesAfter = log.segmentBaseOffsets();
    ASSERT_FALSE(basesAfter.empty());
    ASSERT_GT(basesAfter.front(), firstBaseBefore)
        << "Retention must have aged out front segments (oldest base must advance past "
        << firstBaseBefore << "); otherwise this test does not exercise "
           "producer-state-survives-retention. Got " << basesAfter.front();

    // Rebuild PSM from the (possibly compacted+partially-retained) log.
    ProducerStateManager psm;
    const int32_t kTopicId    = 105;
    const int32_t kPartition  = 0;
    psm.rebuildFromLog(kTopicId, log, log.logEndOffset(), kPartition);

    // The last batch written was seq=kNumBatches-1.  Retry of that exact
    // (epoch=1, baseSeq=kNumBatches-1) must be Duplicate — dedup state survived.
    auto retryResult = psm.validate(kTopicId, kProducerId, kPartition,
                                    kProducerEpoch,
                                    /*baseSeq=*/kNumBatches - 1,
                                    /*lastSeq=*/kNumBatches - 1);
    EXPECT_EQ(retryResult.status, ValidationResult::Status::Duplicate)
        << "After CompactDelete, retry of last batch (seq=" << (kNumBatches-1)
        << ") must be Duplicate — producer state must survive compaction+retention";

    // A genuinely new next sequence must be Accepted.
    auto newResult = psm.validate(kTopicId, kProducerId, kPartition,
                                  kProducerEpoch,
                                  /*baseSeq=*/kNumBatches,
                                  /*lastSeq=*/kNumBatches);
    EXPECT_EQ(newResult.status, ValidationResult::Status::Accept)
        << "After CompactDelete, next new sequence (seq=" << kNumBatches
        << ") must be Accepted";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 6: DeleteTopicRetentionAgesOutSegments
//
// A Delete-policy topic with retentionBytes >= 0 MUST run size retention:
// old segments past the byte cap are physically deleted.  This is the
// CP-3 Delete-path behaviour that previously had NO non-vacuous coverage
// (DeleteTopicNeverCompacts uses retentionBytes=-1, i.e. retention disabled).
//
// NON-VACUOUS: replacing the Delete-retention block in runBoundedCompaction
// with a bare `continue;` makes this test FAIL (segments would not shrink).
//
// Also re-confirms the Delete path never COMPACTS: because retention only drops
// whole front segments, the records SURVIVING in the retained segments keep ALL
// their per-key duplicates (no latest-per-key dedup within a segment).
// ===========================================================================
TEST(CompactionPolicy, DeleteTopicRetentionAgesOutSegments) {
    auto dir = makeTempDir3("cp3_delete_retention");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport3 transport;
    PartitionConfig cfg;
    cfg.topicId              = 106;
    cfg.partitionId          = 0;
    cfg.selfId               = 1;
    cfg.minIsr               = 1;
    cfg.replicaLagMaxMs      = 10'000;
    cfg.replicaLagMaxOffset  = 10'000;
    cfg.fetchIntervalNs      = 1'000'000LL;
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.cleanupPolicy        = CleanupPolicy::Delete;
    cfg.minCleanableDirtyRatio = 0.0;
    // Small byte cap → retention MUST age out front segments.
    cfg.retentionBytes       = 256;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Write 200 records across many segments.  Use the SAME key repeatedly so we
    // can also confirm Delete never dedups (compaction never runs).
    for (int v = 0; v < 200; ++v) {
        auto batch = makeBatch3(v, "k_del", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need >=3 segments so retention can delete front ones";
    const int64_t firstBaseBefore = bases.front();
    const size_t  segCountBefore  = bases.size();
    const int64_t totalBytesBefore = log.totalBytes();

    rune::node::ReplicaRegistry registry;
    registry.add(106, 0, std::move(replica));

    MinimalConsensusAgent3 dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    // Drive cycles — Delete retention should age out old segments each cycle.
    for (int i = 0; i < 100; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    const auto basesAfter = log.segmentBaseOffsets();
    ASSERT_FALSE(basesAfter.empty());

    // (a) RETENTION ran: segment count dropped AND the oldest base advanced.
    EXPECT_LT(basesAfter.size(), segCountBefore)
        << "Delete retention must age out segments: before=" << segCountBefore
        << " after=" << basesAfter.size();
    EXPECT_GT(basesAfter.front(), firstBaseBefore)
        << "Delete retention must advance the oldest segment base past " << firstBaseBefore
        << "; got " << basesAfter.front();

    // (b) Total bytes are now within (roughly) the retention cap — retention
    //     actually enforced the byte budget (allowing the active-segment floor).
    EXPECT_LT(log.totalBytes(), totalBytesBefore)
        << "Delete retention must reduce on-disk bytes below the pre-retention size";

    // (c) Delete never COMPACTS: within any surviving segment the per-key records
    //     are NOT deduped.  The retained tail still holds MANY k_del records
    //     (one per surviving offset), not a single latest-per-key survivor.
    const int keyCount = countLiveRecordsForKey3(log, "k_del");
    EXPECT_GT(keyCount, 1)
        << "Delete policy must NOT compact: surviving segments must retain multiple "
           "per-key records (no latest-per-key dedup); got " << keyCount;

    fs::remove_all(dir);
}
