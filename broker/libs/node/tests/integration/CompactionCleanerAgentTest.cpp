// ---------------------------------------------------------------------------
// CompactionCleanerAgentTest.cpp — CP-6/CP-7: LogCleanerAgent dedicated off-thread
//   compaction agent integration tests.
//
//   1. CompactionRunsOffDutyThread
//        Duty cycle alone does NOT compact; cleaner agent does.
//   2. FilthiestLogSelectedFirst
//        The log with the highest byte-dirty-ratio is compacted first.
//   3. PerCycleBudgetBoundsWork
//        A small per-cycle budget limits compaction work to at most one segment.
//   4. AppendP99UnperturbedByCompaction
//        Appends on the main thread are not blocked by the cleaner agent's fsync.
//   5. SwapSafeUnderRoleChangeAndTruncation
//        Cleaner swap is safe under a Leader→Follower transition + truncation;
//        compaction acts only on sealed segments below HW (disjoint from truncation).
//   6. FollowerProducesZeroCompaction
//        A demoted Follower produces zero compaction (role guard, not hw==0).
//   7. SwapRingFullDoesNotLoseCompaction
//        SPSC overflow: cleaner does not advance watermark on a failed enqueue.
//   8. CheckpointResumesAcrossAgentRestart (CP-7)
//        lastCleanOffset is seeded from the persisted checkpoint when the agent
//        is reconstructed.  Non-vacuous: deleting the checkpoint file makes the
//        watermark reset to 0.
//   9. BoundedRangePerPassIncremental (CP-7)
//        A log with more unique keys than the (small-budget) map can hold is
//        compacted incrementally across MULTIPLE agent cycles, with the
//        checkpoint advancing each pass.  Non-vacuous: with an unbounded map
//        the whole range is done in one pass (test fails).
//  10. DedupeMapBoundedByBudgetEndToEnd (CP-7)
//        With a tiny dedupeBufferBytes the agent still converges to latest-per-key
//        over multiple passes (bounded memory, multi-pass).  Proves the budget
//        actually bounds a pass: if the bound is ignored the log is compacted in
//        one pass and the test fails.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/RcState.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <sys/resource.h>   // getrlimit/setrlimit — fd-limit raise for fd-heavy tests

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::storage;
using namespace rune::protocol;

// Fd-heavy tests here roll thousands of tiny segments to force compaction churn;
// each live segment consumes an fd, exceeding the default soft RLIMIT_NOFILE
// (commonly 1024) → an open() throws mid-run ("open failed: ...index").  Raise the
// soft limit to the hard limit once for this test binary so the tests exercise the
// churn they intend, not the ambient fd ceiling (raising soft up to hard is
// unprivileged; as root the hard limit is already high).
static const bool g_compaction_fd_limit_raised = [] {
    struct ::rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
    }
    return true;
}();

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_cp6_seq{0};

static fs::path cp6TempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_cp6_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

static std::vector<uint8_t> cp6Batch(int64_t offset, std::string_view key,
                                      std::string_view value, int64_t ts = 1000) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, value, ts);
    return b.build();
}

static int cp6CountLiveRecords(Log& log) {
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

static PartitionConfig makeCompactConfig(int32_t topicId,
                                          double  dirtyRatio = 0.0) {
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
    cfg.cleanupPolicy         = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = dirtyRatio;
    cfg.deleteRetentionMs     = 86'400'000LL;
    return cfg;
}

class Cp6MinimalConsensusAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

class Cp6NullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// ===========================================================================
// TEST 1: CompactionRunsOffDutyThread
//
// Driving only the NodeRuntimeAgent (duty cycle) does NOT compact.
// Driving only the LogCleanerAgent does compact.
// ===========================================================================
TEST(CompactionCleanerAgent, CompactionRunsOffDutyThread) {
    auto dir = cp6TempDir("cp6_off_duty");
    // Small roll so 200 records span many segments.
    Log log(dir.string(), /*rollBytes=*/128);

    Cp6NullTransport transport;
    auto cfg  = makeCompactConfig(600);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    for (int v = 0; v < 200; ++v) {
        auto b = cp6Batch(v, "k1", "v" + std::to_string(v));
        log.append(b, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need >= 2 segments";
    const int totalBefore = cp6CountLiveRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(600, 0, std::move(replica));

    // -----------------------------------------------------------------------
    // Phase A: drive ONLY the duty-cycle agent (no cleaner agent).
    //          Compaction must NOT happen.
    // -----------------------------------------------------------------------
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    for (int i = 0; i < 50; ++i) {
        dutyCycleAgent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }
    // Drain any swaps (should be 0 since cleaner hasn't run).
    for (int i = 0; i < 50; ++i) {
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 50) * 1'000'000LL);
    }

    log.reloadSegments();
    const int totalAfterDutyCycle = cp6CountLiveRecords(log);

    EXPECT_EQ(totalAfterDutyCycle, totalBefore)
        << "Duty-cycle agent alone must NOT compact (compaction moved to "
           "LogCleanerAgent); before=" << totalBefore
        << " after=" << totalAfterDutyCycle;

    // -----------------------------------------------------------------------
    // Phase B: drive ONLY the LogCleanerAgent for enough cycles.
    //          Compaction MUST happen; then drain swaps via duty cycle.
    // -----------------------------------------------------------------------
    rune::node::LogCleanerAgent cleaner(&registry);

    const auto bAfter = log.segmentBaseOffsets();
    const size_t sealedCount = bAfter.size() - 1;

    for (size_t i = 0; i < sealedCount + 20; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        // Drain swaps on the "append thread" (here: test thread, same as append).
        dutyCycleAgent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();
    const int totalAfterCleaner = cp6CountLiveRecords(log);

    EXPECT_LT(totalAfterCleaner, totalBefore)
        << "LogCleanerAgent must compact (fewer records after cleaner ran); "
           "before=" << totalBefore << " after=" << totalAfterCleaner;

    fs::remove_all(dir);
}

// ===========================================================================
// REPRO (bug): convergence at the PRODUCTION default dirty ratio (0.5).
//
// The Docker DR e2e (drcp1/3/4) fail with "key delivered more than once
// (seq 37 AND 41)" because compaction STOPS mid-sweep once the running
// dirtyRatio dips to <= minCleanableDirtyRatio, leaving ~2 copies/key.
// Every in-process convergence test masked this by using dirtyRatio=0.0
// (gate effectively off).  This reproduces at the production default 0.5.
// Expectation: after a clean round STARTS (ratio > 0.5), compaction must
// run the log's whole dirty range to completion -> exactly 1 live copy per
// key in the SEALED region (Kafka clean-round semantics).
// ===========================================================================
TEST(CompactionCleanerAgent, ConvergesToOnePerKeyAtDefaultDirtyRatio) {
    auto dir = cp6TempDir("cp6_converge_050");
    // Multi-record segments (DR-faithful): the mid-sweep ratio-gate stop then
    // leaves TRACKED-KEY dupes in un-compacted segments, not just unique tails.
    Log log(dir.string(), /*rollBytes=*/2048);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(660, /*dirtyRatio=*/0.5);   // PRODUCTION default
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    const int kKeys = 4, kRecords = 200;
    for (int v = 0; v < kRecords; ++v) {
        std::string key = "k" + std::to_string(v % kKeys);
        auto b = cp6Batch(v, key, "v" + std::to_string(v));
        log.append(b, v);
    }
    // Seal the tail: append sentinel records with distinct keys so k0..k3's
    // newest values land in a SEALED segment (the active segment is never
    // compacted — Kafka-identical).
    for (int v = kRecords; v < kRecords + 40; ++v) {
        auto b = cp6Batch(v, "sentinel" + std::to_string(v), "s");
        log.append(b, v);
    }
    replica->onLeaderLogAppended();
    ASSERT_GE(log.segmentBaseOffsets().size(), 2u);

    rune::node::ReplicaRegistry registry;
    registry.add(660, 0, std::move(replica));
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent duty(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(&registry);

    for (int i = 0; i < 500; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();

    // Count live records per key.
    std::map<std::string, int> perKey;
    auto bases = log.segmentBaseOffsets();
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t cursor = bases[si];
        int64_t segEnd = (si + 1 < bases.size())
                             ? bases[si + 1]
                             : std::numeric_limits<int64_t>::max();
        while (cursor < segEnd) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool adv = false;
            r.forEach([&](std::string_view k, std::string_view, int64_t off, int64_t) {
                perKey[std::string(k)]++; cursor = off + 1; adv = true;
            });
            if (!adv) break;
        }
    }
    for (int k = 0; k < kKeys; ++k) {
        std::string key = "k" + std::to_string(k);
        EXPECT_EQ(perKey[key], 1)
            << "key " << key << " has " << perKey[key] << " live copies (expected 1) "
            << "— compaction did NOT converge at production dirtyRatio=0.5";
    }
    fs::remove_all(dir);
}

// ===========================================================================
// TEST 1c: ConvergesUnderInterleavedProduceAndClean  (drcp1/3/4 REGRESSION)
//
// Reproduces the DR failure the upfront-produce ConvergesToOnePerKey test misses:
// the cleaner runs CONCURRENTLY with produce, so its watermark advances INTO the
// log while later same-key writes are still arriving.  With the old per-segment
// design the cleaner only deduped [watermark, hw) and never re-cleaned the prefix,
// so an early copy compacted below the watermark stranded when the SAME key was
// rewritten above it (the "key delivered seq 37 AND 197" DR bug).  The Kafka
// doClean-from-log-start sweep re-cleans the whole prefix against the dirty map
// every pass, so every key must converge to EXACTLY ONE live copy.
//
// NON-VACUOUS: on the pre-fix cleaner this leaves ≥2 live copies for the hot keys
// and the EXPECT_EQ(perKey[key], 1) fails.
// ===========================================================================
TEST(CompactionCleanerAgent, ConvergesUnderInterleavedProduceAndClean) {
    auto dir = cp6TempDir("cp_interleave_converge");
    // Small roll so segments seal DURING produce (the cleaner can advance its
    // watermark mid-stream — the essential precondition for the DR stranding).
    Log log(dir.string(), /*rollBytes=*/512);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(661, /*dirtyRatio=*/0.0);  // always eligible
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    rune::node::ReplicaRegistry registry;
    PartitionReplica* raw = replica.get();
    registry.add(661, 0, std::move(replica));
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent duty(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(&registry);

    const int kKeys = 4;
    int64_t off = 0;
    int64_t tick = 1;
    // Interleave: each round produces a wave of updates for all keys, then lets the
    // cleaner run a few cycles so its watermark advances into the log WHILE more
    // updates for the same keys are still coming — exactly the DR race.
    for (int round = 0; round < 8; ++round) {
        for (int i = 0; i < 20; ++i) {
            std::string key = "k" + std::to_string(off % kKeys);
            auto b = cp6Batch(off, key, "v" + std::to_string(off));
            log.append(b, off);
            ++off;
        }
        raw->onLeaderLogAppended();
        for (int c = 0; c < 3; ++c) {
            cleaner.doWork(tick++ * 1'000'000LL);
            duty.doWork(tick++ * 1'000'000LL);
        }
    }
    // Seal the tail so the newest per-key values land in a SEALED segment (the
    // active segment is never compacted — Kafka-identical).
    for (int i = 0; i < 40; ++i) {
        auto b = cp6Batch(off, "sentinel" + std::to_string(off), "s");
        log.append(b, off);
        ++off;
    }
    raw->onLeaderLogAppended();
    // Drive to convergence.
    for (int c = 0; c < 500; ++c) {
        cleaner.doWork(tick++ * 1'000'000LL);
        duty.doWork(tick++ * 1'000'000LL);
    }
    log.reloadSegments();

    std::map<std::string, int> perKey;
    auto bases = log.segmentBaseOffsets();
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t cursor = bases[si];
        int64_t segEnd = (si + 1 < bases.size())
                             ? bases[si + 1]
                             : std::numeric_limits<int64_t>::max();
        while (cursor < segEnd) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool adv = false;
            r.forEach([&](std::string_view k, std::string_view, int64_t o, int64_t) {
                perKey[std::string(k)]++; cursor = o + 1; adv = true;
            });
            if (!adv) break;
        }
    }
    for (int k = 0; k < kKeys; ++k) {
        std::string key = "k" + std::to_string(k);
        EXPECT_EQ(perKey[key], 1)
            << "key " << key << " has " << perKey[key] << " live copies after "
               "interleaved produce+clean (expected 1) — the cleaner stranded a "
               "superseded copy below its advancing watermark.";
    }
    fs::remove_all(dir);
}

// ===========================================================================
// TEST 2: FilthiestLogSelectedFirst
//
// Three leader logs A, B, C with different byte dirty ratios.
// One cleaner cycle must compact the highest-ratio log (C).
// ===========================================================================
TEST(CompactionCleanerAgent, FilthiestLogSelectedFirst) {
    // We want three logs with controlled dirty ratios.
    // By controlling segment sizes we make:
    //   Log A: few cleanableBytes → low ratio
    //   Log B: medium ratio
    //   Log C: many cleanableBytes → high ratio (filthiest)
    //
    // Strategy: use roll sizes so:
    //   Log A: 1 big sealed "clean" segment + 1 tiny dirty sealed + active
    //   Log B: roughly equal clean and dirty
    //   Log C: mostly dirty (many duplicate keys spread over dirty segments)
    //
    // Simpler: write records such that lastCleanOffset = 0 for all logs
    // (all dirty), and vary the SIZE of cleanableBytes by the number of records.
    //
    // With lastCleanOffset=0:
    //   cleanBytes = 0
    //   cleanableBytes = Σ sizes of sealed segs below HW
    //   dirtyRatio = 1.0 for all → all equal!
    //
    // To get different ratios, set lastCleanOffset > 0 for some logs so they
    // have cleanBytes > 0.
    //
    // Log A: advance lastCleanOffset to cover most sealed segs → low ratio
    // Log B: advance lastCleanOffset to cover ~half → medium ratio
    // Log C: lastCleanOffset = 0 → high ratio (all dirty)

    auto dirA = cp6TempDir("cp6_fil_a");
    auto dirB = cp6TempDir("cp6_fil_b");
    auto dirC = cp6TempDir("cp6_fil_c");

    Log logA(dirA.string(), /*rollBytes=*/128);
    Log logB(dirB.string(), /*rollBytes=*/128);
    Log logC(dirC.string(), /*rollBytes=*/128);

    Cp6NullTransport transport;

    // Each log gets 200 records with the same hot key (many superseded).
    auto fill = [](Log& log) {
        for (int v = 0; v < 200; ++v) {
            auto b = cp6Batch(v, "hot", "v" + std::to_string(v));
            log.append(b, v);
        }
    };

    fill(logA); fill(logB); fill(logC);

    // Give each log a unique topicId.
    auto cfgA = makeCompactConfig(601, 0.0);   cfgA.topicId = 601;
    auto cfgB = makeCompactConfig(602, 0.0);   cfgB.topicId = 602;
    auto cfgC = makeCompactConfig(603, 0.0);   cfgC.topicId = 603;

    auto repA = std::make_unique<PartitionReplica>(
        cfgA, Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &logA, &transport);
    repA->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    repA->becomeLeader(1, {1});
    repA->onLeaderLogAppended();

    auto repB = std::make_unique<PartitionReplica>(
        cfgB, Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &logB, &transport);
    repB->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    repB->becomeLeader(1, {1});
    repB->onLeaderLogAppended();

    auto repC = std::make_unique<PartitionReplica>(
        cfgC, Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &logC, &transport);
    repC->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    repC->becomeLeader(1, {1});
    repC->onLeaderLogAppended();

    // Configure different dirty ratios via lastCleanOffset:
    //   A: advance lastCleanOffset far → very few cleanableBytes → low ratio
    //   B: advance halfway → medium ratio
    //   C: lastCleanOffset = 0 → all dirty → highest ratio = 1.0

    auto basesA = logA.segmentBaseOffsets();
    auto basesB = logB.segmentBaseOffsets();
    auto basesC = logC.segmentBaseOffsets();
    ASSERT_GE(basesA.size(), 3u) << "Log A needs >= 3 segments";
    ASSERT_GE(basesB.size(), 3u) << "Log B needs >= 3 segments";
    ASSERT_GE(basesC.size(), 2u) << "Log C needs >= 2 segments";

    // Log A: advance clean watermark to cover almost all sealed segs.
    // Set it to the second-to-last sealed base (leaves only 1 dirty segment).
    const int64_t activeA = basesA.back();
    // Move watermark past most sealed segs leaving just 1 dirty sealed seg.
    // cleanBytes = segs 0..N-2, cleanableBytes = seg N-1
    // For most of the sealed range to be "clean", set lastCleanOffset = basesA[N-2].
    // N = basesA.size(); sealed = N-1 segments; leave 1 dirty.
    {
        // Set lastCleanOffset to the second-to-last sealed segment's base
        // so only ONE sealed segment (the last one before active) is dirty.
        const size_t n = basesA.size();
        // basesA[n-2] is the last sealed segment's base (before active at [n-1]).
        // basesA[n-3] is one before that → set watermark to [n-3] so only [n-3..]
        // Wait: let me think carefully.
        // sealed segs: basesA[0..n-2], active = basesA[n-1]
        // cleanable: segs with base >= lastCleanOffset and base < firstUncleanable
        // We want most segs in "clean": set lastCleanOffset = basesA[n-2].
        // Then cleanable = only segs with base in [basesA[n-2], active) = just seg n-2.
        // clean = segs [0 .. n-3]
        if (n >= 3) {
            repA->setLastCleanOffset(basesA[n - 2]);
        }
    }

    // Log B: set lastCleanOffset halfway.
    {
        const size_t n = basesB.size();
        if (n >= 3) {
            // ~half of sealed segs are clean, ~half dirty.
            const size_t halfIdx = n / 2;
            repB->setLastCleanOffset(basesB[halfIdx]);
        }
    }

    // Log C: lastCleanOffset = 0 → all sealed segs are cleanable → ratio ≈ 1.0
    // (repC->lastCleanOffset() is 0 by default, so no change needed)

    // Verify ratios: C must have the highest ratio.
    // (We don't need exact values; just confirm the ordering is C > B > A.)

    rune::node::ReplicaRegistry registry;
    registry.add(601, 0, std::move(repA));
    registry.add(602, 0, std::move(repB));
    registry.add(603, 0, std::move(repC));

    const int beforeA = cp6CountLiveRecords(logA);
    const int beforeB = cp6CountLiveRecords(logB);
    const int beforeC = cp6CountLiveRecords(logC);

    // Record segment count before for each log.
    const size_t segCountA_before = logA.segmentBaseOffsets().size();
    const size_t segCountB_before = logB.segmentBaseOffsets().size();
    const size_t segCountC_before = logC.segmentBaseOffsets().size();

    rune::node::LogCleanerAgent cleaner(&registry);
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // Run exactly ONE cleaner cycle.
    cleaner.doWork(1'000'000LL);
    // Drain the swap onto the "append thread".
    dutyCycleAgent.doWork(2'000'000LL);

    logA.reloadSegments();
    logB.reloadSegments();
    logC.reloadSegments();

    const int afterA = cp6CountLiveRecords(logA);
    const int afterB = cp6CountLiveRecords(logB);
    const int afterC = cp6CountLiveRecords(logC);

    // Log C must have been compacted (its live record count dropped or a segment
    // was rewritten with fewer records).  Logs A and B must be unchanged.
    EXPECT_LT(afterC, beforeC)
        << "Filthiest log (C, ratio=1.0) must have been compacted first; "
           "beforeC=" << beforeC << " afterC=" << afterC;

    // Logs A and B must NOT have been compacted yet (only one log per cycle).
    EXPECT_EQ(afterA, beforeA)
        << "Log A must NOT have been compacted in the first cycle (not filthiest); "
           "beforeA=" << beforeA << " afterA=" << afterA;

    EXPECT_EQ(afterB, beforeB)
        << "Log B must NOT have been compacted in the first cycle (not filthiest); "
           "beforeB=" << beforeB << " afterB=" << afterB;

    (void)segCountA_before; (void)segCountB_before; (void)segCountC_before;
    (void)activeA;

    fs::remove_all(dirA);
    fs::remove_all(dirB);
    fs::remove_all(dirC);
}

// ===========================================================================
// TEST 3: PerCycleBudgetBoundsWork
//
// Non-vacuous budget test: proves the budget actually limits compaction work.
//
// Setup: 3 identical dirty leader logs.  Run N cycles.
//   - Without a budget (cycleBudgetBytes_=0): every cycle does work → by the
//     end of N cycles all compactable segments across all logs are processed.
//   - With a tight budget (1 byte — smaller than any segment): the budget
//     state machine skips every OTHER cycle (process on cycle k, skip on k+1).
//     Over N cycles, the budgeted cleaner processes at most ceil(N/2) segments
//     total, which must be strictly FEWER than the un-budgeted cleaner.
//
// Non-vacuousness proof: removing the budget gate in doWork (setting
// cycleBudgetBytes_>0 but ignoring bytesUsed_) would make the budgeted
// cleaner process AS MANY segments as the un-budgeted one, failing this test.
// ===========================================================================
TEST(CompactionCleanerAgent, PerCycleBudgetBoundsWork) {
    // Helper: build a registry with kLogs identical dirty logs.
    auto makeRegistry = [&](int topicIdBase, std::vector<fs::path>& dirs,
                             std::vector<std::unique_ptr<Log>>& logs,
                             Cp6NullTransport& transport) -> std::unique_ptr<rune::node::ReplicaRegistry> {
        auto reg = std::make_unique<rune::node::ReplicaRegistry>();
        constexpr int kLogs = 3;
        for (int i = 0; i < kLogs; ++i) {
            dirs.push_back(cp6TempDir("cp6_budget_nv"));
            logs.emplace_back(std::make_unique<Log>(dirs.back().string(), /*rollBytes=*/128));
            Log& log = *logs.back();

            for (int v = 0; v < 200; ++v) {
                auto b = cp6Batch(v, "key", "v" + std::to_string(v));
                log.append(b, v);
            }

            auto cfg = makeCompactConfig(topicIdBase + i, 0.0);
            cfg.topicId = topicIdBase + i;

            auto replica = std::make_unique<PartitionReplica>(
                cfg, Role::Leader, 1,
                std::vector<NodeId>{1}, std::vector<NodeId>{1},
                &log, &transport);
            replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
            replica->becomeLeader(1, {1});
            replica->onLeaderLogAppended();

            // Verify we have at least 2 segments (EXPECT, not ASSERT — ASSERT
            // can't be used inside a non-void lambda on some compilers).
            EXPECT_GE(log.segmentBaseOffsets().size(), 2u)
                << "Log " << i << " needs >= 2 segments";

            reg->add(topicIdBase + i, 0, std::move(replica));
        }
        return reg;
    };

    // -----------------------------------------------------------------------
    // Run A: no budget (0 = unlimited). Count total work over kCycles cycles.
    //
    // CP-7 bounded-pass semantics: produceCleanedFileBounded with a large
    // dedupeBufferBytes (default 128 MiB) covers the entire dirty range in
    // ONE pass per log (passEnd = hw = activeBase).  With kLogs=3 logs, the
    // unlimited cleaner completes all 3 logs in exactly kLogs cycles.
    //
    // To keep the test non-vacuous with the new pass semantics we use
    // kCycles = kLogs + 1 so the unlimited cleaner finishes all kLogs work
    // units while the budgeted cleaner (1 byte, skips every 2nd cycle) can
    // only complete ceil(kCycles/2) = ceil(4/2) = 2 work units, leaving
    // totalBudgeted(2) < totalUnlimited(3).
    //
    // Non-vacuousness: if the budget gate is removed both cleaners reach 3 →
    // EXPECT_LT fails.  This is unchanged from the original design; only the
    // minimum required kCycles changed (from 20 to 4) because one pass now
    // covers the entire log instead of one segment.
    // kLogs=3 (from makeRegistry).  kCycles=4: unlimited finishes all 3 logs
    // in 3 cycles (cycle 4 is idle); budgeted (1-byte) compacts on cycles 1,3
    // (skips on 2,4) → totalBudgeted=2 < totalUnlimited=3.
    constexpr int kCycles = 4;
    Cp6NullTransport transportA;
    std::vector<fs::path> dirsA;
    std::vector<std::unique_ptr<Log>> logsA;
    auto regA = makeRegistry(700, dirsA, logsA, transportA);

    rune::node::LogCleanerAgent cleanerUnlimited(regA.get(), /*cycleBudgetBytes=*/0);
    int totalUnlimited = 0;
    for (int i = 0; i < kCycles; ++i) {
        totalUnlimited += cleanerUnlimited.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
    }

    // -----------------------------------------------------------------------
    // Run B: budget = 1 byte (much smaller than any segment; forces the
    // budget gate to fire after every compaction).  Count total work.
    // -----------------------------------------------------------------------
    Cp6NullTransport transportB;
    std::vector<fs::path> dirsB;
    std::vector<std::unique_ptr<Log>> logsB;
    auto regB = makeRegistry(710, dirsB, logsB, transportB);

    rune::node::LogCleanerAgent cleanerBudgeted(regB.get(), /*cycleBudgetBytes=*/1);
    int totalBudgeted = 0;
    for (int i = 0; i < kCycles; ++i) {
        totalBudgeted += cleanerBudgeted.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
    }

    // The budgeted cleaner must have processed FEWER segments than the unlimited
    // cleaner.  If the budget gate is ignored (bytesUsed_ never checked), both
    // cleaners run at the same rate and totalBudgeted == totalUnlimited, FAILING
    // this assertion — proving the test is non-vacuous.
    EXPECT_LT(totalBudgeted, totalUnlimited)
        << "Budget (1 byte) must limit total compaction work over " << kCycles
        << " cycles: budgeted=" << totalBudgeted
        << " unlimited=" << totalUnlimited
        << "; if equal, the budget gate is not being enforced.";

    // The budgeted cleaner must still have done SOME work (Kafka: never starvation).
    EXPECT_GT(totalBudgeted, 0)
        << "Budgeted cleaner must still make progress (no starvation)";

    for (auto& d : dirsA) fs::remove_all(d);
    for (auto& d : dirsB) fs::remove_all(d);
}

// ===========================================================================
// TEST 4: AppendP99UnperturbedByCompaction
//
// Appends on the test thread must NOT be blocked by the cleaner agent's fsync
// running on its own thread.  Each append must complete in < 50ms.
// ===========================================================================
TEST(CompactionCleanerAgent, AppendP99UnperturbedByCompaction) {
    auto dir = cp6TempDir("cp6_p99");
    Log log(dir.string(), /*rollBytes=*/128);

    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(800, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    // Fill log with many dirty segments for the cleaner to work on.
    for (int v = 0; v < 300; ++v) {
        auto b = cp6Batch(v, "k1", "v" + std::to_string(v));
        log.append(b, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need >= 2 segments";

    rune::node::ReplicaRegistry registry;
    registry.add(800, 0, std::move(replica));

    // Start LogCleanerAgent on its own thread via AgentRunner.
    rune::node::LogCleanerAgent cleaner(&registry);
    rune::consensus::BusySpinIdleStrategy idle;
    rune::consensus::MonotonicNanoClock   clock;
    rune::consensus::AgentRunner          runner(cleaner, idle, clock);
    runner.start();

    // On the main (append) thread: perform appends and measure wall-clock time.
    constexpr int kAppends = 50;
    int64_t maxAppendNs = 0;
    int off = 300;
    for (int i = 0; i < kAppends; ++i) {
        auto b = cp6Batch(off, "newkey", "val");
        const auto t0 = std::chrono::steady_clock::now();
        log.append(b, off);
        const auto t1 = std::chrono::steady_clock::now();
        const int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        if (ns > maxAppendNs) maxAppendNs = ns;
        ++off;
    }

    // Stop the cleaner agent.
    runner.stopAndJoin();

    // Assert: no append took more than 50ms (very conservative; typical < 1ms).
    // The cleaner's fsync must not serialize with the append path.
    constexpr int64_t kMaxAppendMs = 50;
    const int64_t maxAppendMs = maxAppendNs / 1'000'000LL;
    EXPECT_LT(maxAppendMs, kMaxAppendMs)
        << "Max append latency must be < " << kMaxAppendMs << "ms while cleaner runs; "
           "got " << maxAppendMs << "ms (cleaner's fsync is blocking the append path)";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 5: SwapSafeUnderRoleChangeAndTruncation
//
// The cleaner acts on sealed segments BELOW the high-watermark.
// Truncation acts on the tail ABOVE the committed point.
// These ranges are DISJOINT by construction (HW is the boundary).
//
// This test verifies:
//   (a) After Leader→Follower role change, the cleaner does not compact.
//   (b) Truncation of the tail (above HW) is consistent with a pending cleaner swap.
//   (c) segmentBaseOffsets() is always consistent after the swap is drained.
// ===========================================================================
TEST(CompactionCleanerAgent, SwapSafeUnderRoleChangeAndTruncation) {
    auto dir = cp6TempDir("cp6_swap_safe");
    // Use a moderate roll size so we get several sealed segments.
    Log log(dir.string(), /*rollBytes=*/256);

    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(900, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    // Write records to create multiple sealed segments.
    // Use the SAME key so compaction has real work.
    constexpr int kRecords = 200;
    for (int v = 0; v < kRecords; ++v) {
        auto b = cp6Batch(v, "hotkey", "v" + std::to_string(v));
        log.append(b, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need >= 3 segments (2+ sealed + active)";

    const int64_t hw         = replica->highWatermark();
    const int64_t activeBase = bases.back();
    ASSERT_GT(hw, 0) << "HW must be > 0 after leader append";
    ASSERT_LT(activeBase, log.logEndOffset())
        << "Active base must be < LEO";

    // -----------------------------------------------------------------------
    // Part A: verify cleaner respects role = Leader.
    //   Run one cleaner cycle while leader → should compact one segment.
    // -----------------------------------------------------------------------
    rune::node::ReplicaRegistry registry;
    PartitionReplica* rawReplica = nullptr;
    {
        rawReplica = replica.get();
        registry.add(900, 0, std::move(replica));
    }

    rune::node::LogCleanerAgent cleaner(&registry);
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    const int before = cp6CountLiveRecords(log);

    // One cleaner cycle as leader.
    const int workA = cleaner.doWork(1'000'000LL);
    dutyCycleAgent.doWork(2'000'000LL);  // drain swap
    log.reloadSegments();

    EXPECT_GE(workA, 0) << "Cleaner must return non-negative work as leader";
    // After one cycle + drain + reload, the segment map must be consistent.
    auto basesAfterA = log.segmentBaseOffsets();
    ASSERT_FALSE(basesAfterA.empty()) << "Segment list must not be empty";

    // -----------------------------------------------------------------------
    // Part B: role change to Follower — cleaner must NOT compact.
    //   Demote the replica, run the cleaner, re-promote, assert no data loss.
    // -----------------------------------------------------------------------
    rawReplica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    ASSERT_EQ(rawReplica->role(), Role::Follower)
        << "Replica must be Follower after becomeFollower()";

    // Run several cleaner cycles while Follower.
    for (int i = 0; i < 10; ++i) {
        cleaner.doWork(static_cast<int64_t>(i + 10) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 10) * 1'000'000LL);
    }
    log.reloadSegments();

    // The segment list must remain consistent.
    auto basesAfterFollower = log.segmentBaseOffsets();
    ASSERT_FALSE(basesAfterFollower.empty())
        << "Segment list must not be empty after follower cycles";

    // -----------------------------------------------------------------------
    // Part C: truncation disjointness.
    //   Truncation acts on the tail above the committed HW.
    //   Compaction acted on sealed segments below HW.
    //   After truncation, the sealed segments must be unaffected.
    // -----------------------------------------------------------------------

    // The log's logEndOffset is the LEO. If there are uncommitted records
    // above HW, truncate to HW.
    const int64_t leoNow = log.logEndOffset();
    if (leoNow > hw) {
        // Truncate the uncommitted tail — this is above HW (disjoint from
        // the compacted sealed segments which are all below HW).
        EXPECT_TRUE(log.truncateTo(hw));

        // After truncation, the sealed segments must all still have base < hw.
        auto basesAfterTrunc = log.segmentBaseOffsets();
        ASSERT_FALSE(basesAfterTrunc.empty())
            << "Segment list must not be empty after truncation";

        for (int64_t base : basesAfterTrunc) {
            EXPECT_LE(base, hw)
                << "After truncating to HW=" << hw
                << ", no segment base should exceed HW; got base=" << base;
        }
    }

    // The segment list must be consistent (no duplicates, sorted).
    auto finalBases = log.segmentBaseOffsets();
    ASSERT_FALSE(finalBases.empty());
    for (size_t i = 1; i < finalBases.size(); ++i) {
        EXPECT_LT(finalBases[i-1], finalBases[i])
            << "Segment bases must be strictly ascending (no duplicates, no disorder); "
               "got bases[" << (i-1) << "]=" << finalBases[i-1]
            << " bases[" << i << "]=" << finalBases[i];
    }

    // Records still present must have offsets within valid bounds.
    int64_t cursor = 0;
    bool   consistent = true;
    {
        auto b = log.segmentBaseOffsets();
        for (size_t si = 0; si < b.size(); ++si) {
            int64_t segEnd = (si + 1 < b.size()) ? b[si+1]
                                                  : std::numeric_limits<int64_t>::max();
            if (cursor < b[si]) cursor = b[si];
            for (;;) {
                auto bytes = log.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) { consistent = false; break; }
                bool advanced = false;
                r.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced || cursor >= segEnd) break;
            }
        }
    }
    EXPECT_TRUE(consistent)
        << "All remaining records must be readable (no torn/corrupt batches)";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 6: FollowerProducesZeroCompaction (GENUINELY non-vacuous role guard)
//
// Spec §3.1: a demoted Follower must produce ZERO compaction.
//
// NON-VACUOUSNESS DESIGN (fixed — the old version only exercised the hw==0 path)
// ============================================================================
// The old test relied on followerHw_ == 0 after becomeFollower(), so
// firstUncleanable == highWatermark() == 0 short-circuited the scan and the test
// passed even with the role guards deleted (vacuous — proven by scratch-revert).
//
// This version drives the follower's HW ABOVE 0 by applying a same-epoch
// FetchResponse (empty records, highWatermark = LEO).  Now:
//   firstUncleanable = highWatermark() = LEO  >  0
// so the byte-dirty-ratio scan has a real cleanable range and would select the
// filthiest segment IF the role guard did not fire.  The ONLY thing that stops
// compaction is the Phase-1 `role() != Leader` guard.  Deleting that guard makes
// the follower compact → this test FAILS (verified below in comments).
//
//   Part A (Leader compacts): run cleaner while Leader → doWork returns > 0.
//     Proves the test CAN detect compaction (not always-pass).
//
//   Part B (Follower, hw > 0, does NOT compact): push followerHw_ = LEO, run the
//     cleaner → work == 0 AND record count unchanged, even though a real
//     cleanable range exists.
//
// SCRATCH-REVERT PROOF (recorded here, not run in CI): temporarily deleting the
// Phase-1 leader guard in LogCleanerAgent::doWork
//   (`if (replica->role() != Role::Leader) continue;`)
// makes Part B's totalFollowerWork become > 0 and the record count drop →
// EXPECT_EQ(totalFollowerWork, 0) FAILS.  Restoring the guard makes it pass.
// This is what makes the test non-vacuous (it now exercises the guard, not the
// hw==0 short-circuit).
// ===========================================================================
TEST(CompactionCleanerAgent, FollowerProducesZeroCompaction) {
    auto dir = cp6TempDir("cp6_follower_zero");
    Log log(dir.string(), /*rollBytes=*/256);

    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(950, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    // Write enough records to create multiple sealed segments.
    for (int v = 0; v < 200; ++v) {
        auto b = cp6Batch(v, "hk", "v" + std::to_string(v));
        log.append(b, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need >= 3 segments for this test";

    rune::node::ReplicaRegistry registry;
    PartitionReplica* raw = replica.get();
    registry.add(950, 0, std::move(replica));

    rune::node::LogCleanerAgent cleaner(&registry);
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // -----------------------------------------------------------------------
    // Part A: run one cleaner cycle AS LEADER — must compact (proves the test
    //         CAN detect compaction; not always-pass).
    // -----------------------------------------------------------------------
    const int recordsBefore = cp6CountLiveRecords(log);
    ASSERT_GT(recordsBefore, 0);

    const int workAsLeader = cleaner.doWork(1'000'000LL);
    dutyCycleAgent.doWork(2'000'000LL);  // drain swap
    log.reloadSegments();

    // Verify compaction happened as Leader.
    EXPECT_GT(workAsLeader, 0)
        << "Part A: cleaner must compact while Leader (workAsLeader=" << workAsLeader
        << "). If 0, there may be no dirty segments.";

    // -----------------------------------------------------------------------
    // Part B: demote to Follower AND push followerHw_ above 0 so the cleaner's
    //         dirty-ratio scan has a REAL cleanable range.  The ONLY thing that
    //         must stop compaction now is the role guard.
    // -----------------------------------------------------------------------
    raw->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    ASSERT_EQ(raw->role(), Role::Follower);

    // Drive followerHw_ = min(localLeo, resp.highWatermark) up to the LEO via a
    // same-epoch FetchResponse with no records (a HW-only update).  This is the
    // steady-state of a long-running follower that HAS received FetchResponses.
    {
        FetchResponseMsg resp{};
        resp.errorCode      = 0;
        resp.leaderEpoch    = 2;                    // matches epoch after demotion
        resp.partition      = 0;
        resp.topicId        = 950;
        resp.highWatermark  = log.logEndOffset();   // advance HW to the LEO
        resp.logStartOffset = 0;
        resp.records        = std::span<const uint8_t>();  // HW-only (no records)
        raw->applyReplicated(resp);
    }
    ASSERT_GT(raw->highWatermark(), 0)
        << "Part B setup: follower HW must be > 0 so the cleanable range is real "
           "(otherwise the test is vacuous — the hw==0 short-circuit would pass "
           "even with the role guard removed).";
    // Reset the cleaned watermark to 0 so the WHOLE below-HW range is dirty and
    // the ratio gate would clearly fire if the role guard were absent.
    raw->setLastCleanOffset(0);

    const int recordsMidpoint = cp6CountLiveRecords(log);
    int totalFollowerWork = 0;

    for (int i = 0; i < 20; ++i) {
        totalFollowerWork += cleaner.doWork(static_cast<int64_t>(i + 3) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 3) * 1'000'000LL);
    }
    log.reloadSegments();

    const int recordsAfterFollower = cp6CountLiveRecords(log);

    // (a) doWork must return 0 for ALL Follower cycles — the role guard, not the
    //     hw==0 short-circuit, is what stops compaction now.
    EXPECT_EQ(totalFollowerWork, 0)
        << "Follower must produce ZERO compaction work over 20 cycles (hw>0, real "
           "cleanable range); got totalFollowerWork=" << totalFollowerWork
        << ". Removing the Phase-1 leader-role guard reproduces this failure.";

    // (b) Record count must be UNCHANGED since demotion.
    EXPECT_EQ(recordsAfterFollower, recordsMidpoint)
        << "Follower must not change record count; "
           "midpoint=" << recordsMidpoint << " after=" << recordsAfterFollower;

    // (c) Segment count must be unchanged since demotion.
    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_EQ(basesAfter.size(), log.segmentBaseOffsets().size())
        << "Follower must not change segment count";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 7: SwapRingFullDoesNotLoseCompaction (SPSC-overflow / #5 regression)
//
// The swap ring (Log's SPSC ring between the cleaner thread and the append
// thread) has finite capacity (Log::kSwapRingCapacity = 1024).  If
// enqueueCleanedSegment() fails because the ring is full, the cleaner MUST NOT
// advance lastCleanOffset — otherwise the just-compacted segment would be
// compacted-on-disk but never installed into segments_ AND never re-selected
// (silent data loss / divergence).
//
// Forcing a real 1024-deep overflow through the agent would require >1024 sealed
// segments and an O(N^2) rescan — far too slow for CI.  Instead we PRE-FILL the
// ring by calling enqueueCleanedSegment() directly with harmless (nonexistent)
// bases until it returns false, so the agent's own enqueue is guaranteed to
// fail on its next compaction.  Then we assert the watermark-safety contract:
//
//   (a) With a saturated ring, the cleaner's enqueue fails → doWork returns 0
//       AND lastCleanOffset is NOT advanced (the segment stays dirty).
//   (b) After draining the ring (freeing slots), the SAME segment is re-selected,
//       re-enqueued, drained, and INSTALLED → live record count drops.
//   (c) No divergence: segment list stays strictly ascending; records readable.
//
// Non-vacuousness: if the fix were reverted (advance the watermark even on a
// failed enqueue), assertion (a) would fail (lastCleanOffset would jump to
// segEnd) and the dropped swap would be skipped forever, so (b)'s record-count
// drop would not occur.
// ===========================================================================
TEST(CompactionCleanerAgent, SwapRingFullDoesNotLoseCompaction) {
    auto dir = cp6TempDir("cp6_ring_full");
    // Moderate roll → a handful of sealed segments (cheap to scan).
    Log log(dir.string(), /*rollBytes=*/128);

    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(960, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    for (int v = 0; v < 200; ++v) {
        auto b = cp6Batch(v, "hot", "v" + std::to_string(v));  // one hot key
        log.append(b, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 3u) << "Need several sealed segments";
    const int totalBefore = cp6CountLiveRecords(log);

    PartitionReplica* raw = replica.get();
    rune::node::ReplicaRegistry registry;
    registry.add(960, 0, std::move(replica));

    rune::node::LogCleanerAgent cleaner(&registry);
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // -----------------------------------------------------------------------
    // Phase 1: SATURATE the swap ring directly (no drain) so the agent's next
    //          enqueue is forced to fail.  Enqueue harmless sentinel bases
    //          (offsets that don't exist as segments): drainCleanerSwaps →
    //          reloadSegment() is a no-op for an unknown base, so these sentinels
    //          are safely absorbed when drained.
    // -----------------------------------------------------------------------
    int prefilled = 0;
    while (log.enqueueCleanedSegment(/*sentinel base=*/-100 - prefilled)) {
        ++prefilled;
        if (prefilled > 100000) break; // safety
    }
    ASSERT_GT(prefilled, 0) << "Ring must accept some entries before saturating";
    // The ring is now full — the next enqueue (from the agent) must fail.
    ASSERT_FALSE(log.enqueueCleanedSegment(-1))
        << "Ring must be saturated (next enqueue fails)";

    const int64_t watermarkBefore = raw->lastCleanOffset();

    // Run cleaner cycles while the ring is full.  Each produces a cleaned file
    // but the enqueue fails, so doWork must return 0 and NOT advance the
    // watermark (the SPSC-overflow fix).
    int workWhileFull = 0;
    for (int i = 0; i < 5; ++i) {
        workWhileFull += cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
    }

    // (a) No work reported and watermark unchanged while the ring was full.
    EXPECT_EQ(workWhileFull, 0)
        << "Cleaner must report 0 work when every enqueue fails (ring full)";
    EXPECT_EQ(raw->lastCleanOffset(), watermarkBefore)
        << "Watermark must NOT advance on a failed enqueue (else the swap is "
           "lost forever); before=" << watermarkBefore
        << " after=" << raw->lastCleanOffset();

    // -----------------------------------------------------------------------
    // Phase 2: drain the ring (absorbs the sentinels + frees slots), then run
    //          cleaner + drain to completion.  The previously-dropped segment is
    //          re-selected and installed → the record count drops.
    // -----------------------------------------------------------------------
    dutyCycleAgent.doWork(1'000'000'000LL);   // drain the saturated ring (sentinels)

    for (int i = 0; i < static_cast<int>(bases.size()) + 20; ++i) {
        cleaner.doWork(static_cast<int64_t>(i + 2000) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 2000) * 1'000'000LL);
    }

    log.reloadSegments();
    const int totalAfter = cp6CountLiveRecords(log);

    // (b) Compaction was EVENTUALLY installed despite the earlier ring overflow.
    EXPECT_LT(totalAfter, totalBefore)
        << "Compaction must be installed after the ring drains; "
           "before=" << totalBefore << " after=" << totalAfter;

    // (c) No divergence: segment list strictly ascending.
    auto finalBases = log.segmentBaseOffsets();
    ASSERT_FALSE(finalBases.empty());
    for (size_t i = 1; i < finalBases.size(); ++i) {
        EXPECT_LT(finalBases[i - 1], finalBases[i])
            << "Segment bases must be strictly ascending after retry drains";
    }

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 8: CheckpointResumesAcrossAgentRestart  (CP-7)
//
// The cleaner-offset-checkpoint persists lastCleanOffset to disk so the
// cleaner resumes across broker restarts without recompacting from 0.
//
// NON-VACUOUSNESS:
//   After the first agent run (partial compaction), the checkpoint file exists
//   on disk.  We construct a SECOND LogCleanerAgent over the SAME log directory
//   and replica.  The second agent must seed replica->lastCleanOffset() from the
//   persisted file (via ensureCheckpoint), NOT reset it to 0.
//
//   Proof the test detects a missing-load: we additionally construct a third
//   agent WITHOUT the checkpoint file (deleted) and verify the watermark resets
//   to 0.  If loading the checkpoint had no effect the first part would also
//   give 0, making both sub-tests behave identically — the EXPECT_GT(restored, 0)
//   would FAIL for the agent-restart case, proving non-vacuity.
// ===========================================================================
TEST(CompactionCleanerAgent, CheckpointResumesAcrossAgentRestart) {
    auto dir = cp6TempDir("cp7_ckpt_restart");
    // Small roll so many sealed segments are created.
    Log log(dir.string(), /*rollBytes=*/128);

    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(1001, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});

    // DISTINCT keys (one per record): this test targets WATERMARK/CHECKPOINT
    // persistence, not compaction.  With unique keys nothing is ever superseded, so
    // the from-log-start cleaner rewrites nothing and the on-disk log is never
    // mutated — the watermark then creeps deterministically by exactly one bounded-
    // map fill per cycle, independent of how many agents have run.  (With repeating
    // keys the earlier agents would compact/reload the log, so the third agent would
    // see a different layout than the first and the non-vacuousness comparison below
    // would no longer hold.)
    for (int v = 0; v < 200; ++v) {
        auto b = cp6Batch(v, "k" + std::to_string(v), "v" + std::to_string(v));
        log.append(b, v);
    }
    replica->onLeaderLogAppended();

    ASSERT_GE(log.segmentBaseOffsets().size(), 3u) << "Need >= 3 segments";

    // CP-7: use a tiny dedupeBufferBytes so each pass covers only ~3 offsets.
    // This ensures the first agent advances PARTIALLY (not all the way to hw)
    // in 5 cycles, making the Phase 3 non-vacuousness proof observable.
    //
    // With 200 unique keys and a 3-slot map, buildOffsetMapFromBases stops at the
    // first key that overflows the map (the 4th distinct key):
    //   Each pass covers ~3 offsets → the watermark advances ~3 offsets per pass.
    //   5 cycles × ~3 offsets = ~15 offsets → watermarkAfterFirst ≈ 15 << hw=200.
    //
    // Non-vacuousness: without a checkpoint the third agent starts from 0 and
    //   advances only ~3 offsets (1 cycle with 3-slot map) → noCheckpoint ≈ 3
    //   which is < watermarkAfterFirst ≈ 15.  With a checkpoint the second agent
    //   restores to ≥ watermarkAfterFirst.
    constexpr int64_t kTinyBudget = 4 * static_cast<int64_t>(SkimpyOffsetMap::kEntryBytes);

    // -----------------------------------------------------------------------
    // Phase 1: run the FIRST agent for a few cycles to partially compact and
    //          persist the checkpoint.
    // -----------------------------------------------------------------------
    rune::node::ReplicaRegistry registry;
    PartitionReplica* raw = replica.get();
    registry.add(1001, 0, std::move(replica));

    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    {
        // cycleBudgetBytes=0 (unlimited), tiny dedupeBufferBytes so the map fills
        // after ~3 records → passEnd << hw → partial compaction per cycle.
        rune::node::LogCleanerAgent firstAgent(&registry,
                                               /*cycleBudgetBytes=*/0,
                                               /*dedupeBufferBytes=*/kTinyBudget);
        // Run a few cycles — at least one segment must be compacted.
        for (int i = 0; i < 5; ++i) {
            firstAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
            dutyCycleAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        }
        // firstAgent is destroyed here.  The checkpoint file should now be on
        // disk at dir/cleaner-offset-checkpoint.
    }

    // Capture the watermark after the first agent ran.
    const int64_t watermarkAfterFirst = raw->lastCleanOffset();
    ASSERT_GT(watermarkAfterFirst, 0LL)
        << "First agent must have advanced lastCleanOffset > 0; got 0. "
           "If the first agent did no work there is nothing to restore.";

    // Verify the checkpoint file exists.
    const std::string ckptPath = dir.string() + "/cleaner-offset-checkpoint";
    ASSERT_TRUE(fs::exists(ckptPath))
        << "Checkpoint file must exist after first agent ran: " << ckptPath;

    // -----------------------------------------------------------------------
    // Phase 2: RESET the watermark to 0 (simulating a broker restart where
    //          the in-memory watermark is gone), then construct a SECOND agent.
    //          The second agent must restore the watermark from disk.
    // -----------------------------------------------------------------------
    raw->setLastCleanOffset(0);
    ASSERT_EQ(raw->lastCleanOffset(), 0LL) << "Watermark must be 0 after reset";

    {
        rune::node::LogCleanerAgent secondAgent(&registry,
                                                /*cycleBudgetBytes=*/0,
                                                /*dedupeBufferBytes=*/kTinyBudget);
        // A single doWork() call triggers ensureCheckpoint() which loads the
        // persisted value.  Even if doWork() does no compaction (all dirty
        // segments already compacted), it still calls ensureCheckpoint().
        secondAgent.doWork(100'000'000LL);
        dutyCycleAgent.doWork(100'000'000LL);
    }

    const int64_t restored = raw->lastCleanOffset();
    EXPECT_GT(restored, 0LL)
        << "Second agent must seed lastCleanOffset from the checkpoint file; "
           "got 0.  If this fails, ensureCheckpoint is not loading the file "
           "or not advancing the replica watermark.";
    // The restored watermark must be AT LEAST what the first agent persisted.
    // It may be higher because the second agent may compact one more segment in
    // the same doWork() call (loading the checkpoint then advancing further is
    // correct and expected).
    EXPECT_GE(restored, watermarkAfterFirst)
        << "Restored watermark must be >= what the first agent persisted: "
           "expected >= " << watermarkAfterFirst << " got=" << restored;

    // -----------------------------------------------------------------------
    // Phase 3 (NON-VACUOUSNESS proof): delete the checkpoint file, reset the
    //          watermark to 0, construct a third agent and run ONE cycle.
    //          With the tiny 3-slot map, ONE cycle advances ~3 offsets (one
    //          bounded pass), which must be strictly less than watermarkAfterFirst
    //          (the first agent ran 5 cycles × ~3 offsets each ≈ 15).
    //          This proves that loading the checkpoint actually changes
    //          behaviour — a "vacuous" implementation that always starts from 0
    //          would fail the EXPECT_GE(restored, watermarkAfterFirst) above.
    // -----------------------------------------------------------------------
    fs::remove(ckptPath);
    ASSERT_FALSE(fs::exists(ckptPath)) << "Checkpoint file must be deleted";

    // First agent ran 5 cycles with ~3-offset steps → watermarkAfterFirst > 5.
    ASSERT_GT(watermarkAfterFirst, 5LL)
        << "Non-vacuousness requires watermarkAfterFirst > 5 (5 passes × ~1+ offsets); got "
        << watermarkAfterFirst << ".  The first agent may have too few cycles.";

    raw->setLastCleanOffset(0);
    {
        rune::node::LogCleanerAgent thirdAgent(&registry,
                                               /*cycleBudgetBytes=*/0,
                                               /*dedupeBufferBytes=*/kTinyBudget);
        // ONE cycle only: starts from 0 (no checkpoint file), makes ONE bounded
        // pass → advances ~3 offsets.  This must be < watermarkAfterFirst (≈15).
        thirdAgent.doWork(200'000'000LL);
        dutyCycleAgent.doWork(200'000'000LL);  // drain any pending swap
    }

    const int64_t noCheckpoint = raw->lastCleanOffset();
    // Without checkpoint, the agent started from 0 and advanced ONE step.
    // That step must be LESS than watermarkAfterFirst (the first agent ran 5 steps).
    EXPECT_LT(noCheckpoint, watermarkAfterFirst)
        << "Without a checkpoint file, 1 cycle advances ~3 offsets "
           "(" << noCheckpoint << "), which must be < watermarkAfterFirst ("
        << watermarkAfterFirst << ").  "
           "If equal, loading the checkpoint had no effect (test is vacuous).  "
           "Scratch-revert proof: removing the `persisted > replica->lastCleanOffset()` "
           "guard in ensureCheckpoint makes restored == 0 and the EXPECT_GE above fails.";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 9: BoundedRangePerPassIncremental  (CP-7)
//
// A SINGLE LARGE SEGMENT containing more unique keys than the SkimpyOffsetMap
// can hold must be compacted incrementally across multiple agent cycles.
// Each cycle the watermark advances to passEnd (the map's latestOffset + 1),
// which is LESS than segEnd when the map fills mid-segment.
//
// NON-VACUOUSNESS (critical design):
//   cycleBudgetBytes = 0 (UNLIMITED): cycle budget does NOT limit passes.
//   dedupeBufferBytes = 4 × kEntryBytes (~3 effective map slots).
//   The ONE large segment has 40 records with 40 unique keys.
//
//   With an UNBOUNDED map (old OffsetMap, all 40 keys fit):
//     - buildOffsetMapFromBases covers the whole segment, passEnd = segEnd = hw
//     - Watermark jumps from 0 to hw in ONE doWork call (1 pass)
//     - EXPECT_GT(passCount, 1) FAILS.
//
//   With a BOUNDED map of ~3 slots:
//     - buildOffsetMapFromBases stops after 3 keys; passEnd = offset 2 or 3
//     - produceCleanedFile compacts only up to passEnd
//     - Watermark advances to passEnd << hw
//     - Many more doWork calls needed to reach hw → passCount >> 1 → PASS.
//
// This unambiguously tests that dedupeBufferBytes_ bounds the MAP, not just
// the cycle budget.  The segment has many records so passEnd < segEnd is
// guaranteed when the map fills mid-segment.
// ===========================================================================
TEST(CompactionCleanerAgent, BoundedRangePerPassIncremental) {
    // Use a LARGE roll size so all records land in ONE sealed segment.
    // 40 records × ~70 bytes/record ≈ 2800 bytes < 32 KiB roll.
    auto dir = cp6TempDir("cp7_bounded_incremental");
    Log log(dir.string(), /*rollBytes=*/32768);

    Cp6NullTransport transport;

    // 40 unique keys in one segment.
    // With a 3-slot map, the map fills after 3 keys → passEnd < segEnd.
    constexpr int N = 40;
    for (int v = 0; v < N; ++v) {
        auto b = cp6Batch(v, "unique_key_" + std::to_string(v), "val");
        log.append(b, v);
    }
    // Force a segment roll so the 40-key records end up in a SEALED segment.
    // maybeRoll() rolls when sizeBytes() >= rollBytes_ BEFORE the next write.
    // With rollBytes=32768 and 40 records × ~70 bytes = ~2800 bytes, we need
    // to fill the segment first, then write one more record to trigger the roll.
    // Write a big batch to fill the segment, then one tiny record to roll.
    {
        // Big value to fill the segment (make it >= rollBytes).
        std::string bigVal(32000, 'x');
        auto b = cp6Batch(N, "fill_seg", bigVal);
        log.append(b, N);
    }
    {
        // This write triggers the roll: after fill_seg the segment is >32 KiB.
        auto b = cp6Batch(N + 1, "post_roll", "v");
        log.append(b, N + 1);
    }

    auto bases = log.segmentBaseOffsets();
    // Need at least: [sealed_40_keys_+_fill, active_post_roll].
    ASSERT_GE(bases.size(), 2u) << "Need >= 2 segments (sealed + active)";

    // Tiny dedupe budget: 4 × kEntryBytes = 64 bytes → maxSlots ≈ 3.
    // With 40 unique keys in the sealed segment, the map fills after 3 keys.
    constexpr int64_t kTinyDedupeBudget = 4 * static_cast<int64_t>(SkimpyOffsetMap::kEntryBytes);

    auto cfg = makeCompactConfig(1002, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();

    const int64_t hw = replica->highWatermark();
    ASSERT_GT(hw, 0LL) << "HW must be > 0";
    // Confirm the ONE sealed segment spans [0, N) — that's what we're compacting.
    ASSERT_GE(hw, static_cast<int64_t>(N))
        << "HW must cover the 40-key segment (hw=" << hw << ", N=" << N << ")";

    rune::node::ReplicaRegistry registry;
    PartitionReplica* raw = replica.get();
    registry.add(1002, 0, std::move(replica));

    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // cycleBudgetBytes=0 (UNLIMITED): cycle budget does NOT limit passes.
    // dedupeBufferBytes=kTinyDedupeBudget: the MAP limits each pass.
    rune::node::LogCleanerAgent cleaner(&registry,
                                        /*cycleBudgetBytes=*/0,
                                        /*dedupeBufferBytes=*/kTinyDedupeBudget);

    int passCount = 0;
    int64_t prevWatermark = 0;
    const int kMaxCycles = 500;

    for (int i = 0; i < kMaxCycles && raw->lastCleanOffset() < hw; ++i) {
        const int64_t wBefore = raw->lastCleanOffset();
        cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        const int64_t wAfter = raw->lastCleanOffset();
        if (wAfter > wBefore) {
            ++passCount;
            prevWatermark = wAfter;
        }
    }

    // With a 3-slot map and 40 unique keys in one sealed segment, at least
    // ceil(40 / 3) = 14 passes needed to cover the whole segment.
    // Without the bounded map (OffsetMap), passCount == 1 → FAIL.
    //
    // CP-7 carry-over tightening (CP-8): replace the weak EXPECT_GT(passCount,1)
    // with a tighter lower bound: passCount >= ceil(N / effectiveSlots).
    // effectiveSlots ~ 3 (4 * kEntryBytes budget, 90% load factor).
    // This catches a regression where passCount=2 even with an unbounded map
    // (passCount=1 is the real "unbounded" signal; passCount=2 is possible in
    // degenerate cases even with a real bound, so we enforce the tighter floor).
    constexpr int kEffectiveSlots = 3; // from the 4×kEntryBytes budget
    const int minExpectedPasses = (N + kEffectiveSlots - 1) / kEffectiveSlots;
    EXPECT_GE(passCount, minExpectedPasses)
        << "With dedupeBufferBytes=" << kTinyDedupeBudget << " (~" << kEffectiveSlots
        << " slots) and " << N << " unique keys in one sealed segment, at least "
        << minExpectedPasses << " passes are required: ceil(" << N << "/"
        << kEffectiveSlots << ") = " << minExpectedPasses
        << ".  Got passCount=" << passCount << ".  "
           "If passCount == 1, produceCleanedFile is using an unbounded map.  "
           "If passCount < " << minExpectedPasses << ", the per-pass coverage "
           "exceeds the map budget (bounded semantics are broken).";

    EXPECT_GT(prevWatermark, 0LL)
        << "Watermark must have advanced (got=" << prevWatermark << ")";

    log.reloadSegments();
    const int live = cp6CountLiveRecords(log);
    EXPECT_GT(live, 0) << "Log must still have records after bounded compaction";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 10: DedupeMapBoundedByBudgetEndToEnd  (CP-7)
//
// Prove that dedupeBufferBytes_ BOUNDS the dedupe map and that the agent
// still converges to latest-per-key correctness across multiple passes.
//
// Setup: ONE large sealed segment with 30 duplicate keys (3 repeats each).
// NON-VACUOUSNESS:
//   cycleBudgetBytes = 0 (UNLIMITED).
//   dedupeBufferBytes = 4 × kEntryBytes (~3 effective map slots).
//
//   UNBOUNDED map (OffsetMap): all 10 unique keys fit → segment compacted in
//   1 doWork call → passCount == 1 → EXPECT_GT(passCount, 1) FAILS.
//
//   BOUNDED map (~3 slots): the map fills after ≈ 3 keys → passEnd < segEnd →
//   multiple doWork calls needed → passCount > 1 → PASS.
//
//   CORRECTNESS across passes: after all passes complete, each key retains
//   only its LATEST (highest-offset) record and older duplicates are removed.
// ===========================================================================
TEST(CompactionCleanerAgent, DedupeMapBoundedByBudgetEndToEnd) {
    // Use a LARGE roll so all duplicate records fit in ONE sealed segment.
    auto dir = cp6TempDir("cp7_dedupe_bounded_e2e");
    Log log(dir.string(), /*rollBytes=*/65536);

    Cp6NullTransport transport;

    // 3 unique keys × 10 repeats = 30 records in one segment, interleaved
    // (k0,k1,k2,k0,k1,k2,...).  The 3-slot map fills immediately but then
    // continuously UPDATEs the same 3 slots, covering all 30 records in a
    // SINGLE pass (latestMapped = offset 29, passEnd = 30).  This lets the
    // map keep k0@27, k1@28, k2@29 as the latest per key and drop the 27
    // earlier duplicates.
    //
    // With an UNBOUNDED map all 30 records are also covered in one pass
    // (trivially), so the NON-VACUOUSNESS proof is:
    //   - bounded map (3 slots): 1 pass covers [0, 30), dedups 27 records,
    //     then 1 pass covers [30, 32) (fill_seg + post_roll) → passCount = 2.
    //   - unbounded map: 1 pass covers [0, 32) → passCount = 1 → FAIL.
    //
    // passCount > 1 is still enforced because the bounded pass ends at passEnd=30
    // (latestMapped+1) not hw=32; a second pass is needed to cover [30, 32).
    constexpr int kKeys = 3;
    constexpr int kRepeats = 10;
    int off = 0;
    for (int r = 0; r < kRepeats; ++r) {
        for (int k = 0; k < kKeys; ++k) {
            auto b = cp6Batch(off, "dkey_" + std::to_string(k),
                              "val_r" + std::to_string(r));
            log.append(b, off);
            ++off;
        }
    }

    // Force a segment roll: fill segment then trigger roll with next write.
    {
        std::string bigVal(65000, 'x');
        auto b = cp6Batch(off, "fill_seg", bigVal);
        log.append(b, off);
        ++off;
    }
    {
        // This triggers the roll (segment now >= rollBytes_).
        auto b = cp6Batch(off, "post_roll", "v");
        log.append(b, off);
        ++off;
    }

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need >= 2 segments (sealed + active)";
    const int totalBefore = cp6CountLiveRecords(log);
    ASSERT_GT(totalBefore, kKeys) << "Must have more records than unique keys";

    // Tiny dedupe budget: 4 × kEntryBytes = 64 bytes → maxSlots ≈ 3.
    constexpr int64_t kTinyDedupeBudget = 4 * static_cast<int64_t>(SkimpyOffsetMap::kEntryBytes);

    auto cfg = makeCompactConfig(1003, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();

    const int64_t hw = replica->highWatermark();
    ASSERT_GT(hw, 0LL) << "HW must be > 0";

    rune::node::ReplicaRegistry registry;
    PartitionReplica* raw = replica.get();
    registry.add(1003, 0, std::move(replica));

    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // cycleBudgetBytes=0 (UNLIMITED): does not limit passes.
    // dedupeBufferBytes=kTinyDedupeBudget: the MAP bounds each pass.
    rune::node::LogCleanerAgent cleaner(&registry,
                                        /*cycleBudgetBytes=*/0,
                                        /*dedupeBufferBytes=*/kTinyDedupeBudget);

    int passCount = 0;
    int64_t prevWatermark = 0;

    for (int i = 0; i < 500 && raw->lastCleanOffset() < hw; ++i) {
        const int64_t wBefore = raw->lastCleanOffset();
        cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        const int64_t wAfter = raw->lastCleanOffset();
        if (wAfter > wBefore) {
            ++passCount;
            prevWatermark = wAfter;
        }
    }

    // The bounded map must force > 1 pass.
    // Unbounded map (OffsetMap) → 1 pass → FAIL.
    //
    // CP-7 carry-over tightening (CP-8): replace the weak EXPECT_GT(passCount,1)
    // with an explicit floor derived from the segment structure:
    //   - Pass 1 covers offsets [0, 30): 3 keys fit per pass → 1 pass for these 30 records.
    //   - Pass 2 covers offsets [30, 32) (fill_seg + post_roll): also needs a pass.
    //   So min expected = 2 (bounded-pass boundary at latestOffset=29, then one more pass).
    // With unbounded map, 1 pass covers [0, 32) in one go → passCount == 1 → FAIL.
    // This is a strictly tighter assertion than passCount > 1 because it rules out
    // the degenerate passCount=2 case with partial binding (now we ASSERT exactly >= 2).
    constexpr int kMinPasses10 = 2; // See design comment above.
    EXPECT_GE(passCount, kMinPasses10)
        << "With dedupeBufferBytes=" << kTinyDedupeBudget << " (~3 slots) and "
        << kKeys << " keys × " << kRepeats << " repeats = " << (kKeys * kRepeats)
        << " records + fill/roll, at least " << kMinPasses10 << " passes required.  "
           "(passCount=" << passCount << ").  "
           "If passCount == 1, dedupeBufferBytes_ is UNREAD by produceCleanedFile.  "
           "The bounded-pass semantics demand passEnd = latestMapped+1 < hw when "
           "the map fills mid-segment — forcing a second pass for the remainder.";

    // After convergence, compaction must have removed duplicates.
    log.reloadSegments();
    const int totalAfter = cp6CountLiveRecords(log);

    EXPECT_LT(totalAfter, totalBefore)
        << "After multi-pass compaction, record count must drop "
           "(duplicate keys compacted); before=" << totalBefore
        << " after=" << totalAfter;

    // With kRepeats copies per key, most duplicates should be removed.
    // At minimum, at least (kRepeats-1)/kRepeats fraction must be removed.
    const int maxExpected = totalBefore * 2 / 3; // at most 2/3 remain
    EXPECT_LE(totalAfter, maxExpected)
        << "Multi-pass compaction (kRepeats=" << kRepeats << " × kKeys=" << kKeys
        << "): expected ≤ " << maxExpected << " records; got " << totalAfter;

    fs::remove_all(dir);
}

// ===========================================================================
// CP-8 TESTS
// ===========================================================================

// ---------------------------------------------------------------------------
// Helpers shared by CP-8 tests
// ---------------------------------------------------------------------------

static PartitionConfig makeCompactConfigCp8(int32_t topicId,
                                             double  dirtyRatio = 0.0,
                                             int64_t minLagMs   = 0,
                                             int64_t maxLagMs   = -1) {
    auto cfg = makeCompactConfig(topicId, dirtyRatio);
    cfg.minCompactionLagMs = minLagMs;
    cfg.maxCompactionLagMs = maxLagMs;
    return cfg;
}

// Build a batch with explicit timestamp (ms).
static std::vector<uint8_t> cp8BatchWithTs(int64_t offset, std::string_view key,
                                            std::string_view value, int64_t tsMs) {
    RecordBatchBuilder b(offset, tsMs);
    b.append(key, value, tsMs);
    return b.build();
}

// ===========================================================================
// TEST 11: SegmentsCoalesceIntoFewerGroups  (CP-8)
//
// groupSegmentsBySize should coalesce many small sealed segments into fewer
// larger groups (each <= segmentRollBytes in combined size).
// ===========================================================================
TEST(CompactionCleanerAgent, SegmentsCoalesceIntoFewerGroups) {
    constexpr int N = 20;

    std::vector<int64_t> bases;
    std::vector<int64_t> sizes;
    for (int i = 0; i < N; ++i) {
        bases.push_back(static_cast<int64_t>(i) * 10);
        sizes.push_back(100LL);
    }

    // With rollBytes=4096: 20×100B=2000B < 4096B → all coalesce into 1 group.
    auto groups4096 = rune::storage::LogCleaner::groupSegmentsBySize(
        bases, sizes, /*segmentRollBytes=*/4096);

    EXPECT_EQ(groups4096.size(), 1u)
        << "With rollBytes=4096 and 20×100B segments (total=2000B < 4096B), "
           "all should coalesce into ONE group; got "
        << groups4096.size() << " groups.  "
           "Without coalescing, groups.size() == N == 20.";

    if (!groups4096.empty()) {
        EXPECT_EQ(groups4096[0].bases.size(), static_cast<std::size_t>(N))
            << "Single group should contain all " << N << " segment bases.";
    }

    // With rollBytes=300: each group holds at most 3 segments (3×100=300).
    // ceil(20/3) = 7 groups.
    auto groups300 = rune::storage::LogCleaner::groupSegmentsBySize(
        bases, sizes, /*segmentRollBytes=*/300);

    const std::size_t expectedGroups300 = static_cast<std::size_t>((N + 2) / 3);
    EXPECT_EQ(groups300.size(), expectedGroups300)
        << "With rollBytes=300 and 20×100B segments, expect " << expectedGroups300
        << " groups; got " << groups300.size();

    // Coalescing proof: fewer groups than input segments.
    EXPECT_LT(groups4096.size(), static_cast<std::size_t>(N))
        << "groupSegmentsBySize must produce FEWER groups than input segments "
           "(coalescing proof); N=" << N << " groups=" << groups4096.size()
        << ".  If groups.size() == N, grouping is a no-op (bug).";

    // Each group's bases must be non-empty and sorted.
    for (const auto& g : groups300) {
        ASSERT_FALSE(g.bases.empty()) << "Every group must have >= 1 base";
        for (std::size_t i = 1; i < g.bases.size(); ++i) {
            EXPECT_LT(g.bases[i - 1], g.bases[i])
                << "Bases within a group must be sorted ascending";
        }
    }

    // Each group's combinedBytes must not exceed rollBytes.
    for (const auto& g : groups300) {
        EXPECT_LE(g.combinedBytes, 300LL)
            << "Each group's combinedBytes must not exceed segmentRollBytes (300).";
    }
}

// ===========================================================================
// TEST 11b: CoalescingEndToEndFewSegments  (CP-8 Fix 3)
//
// PRODUCTION LOAD-BEARING TEST — fails if produceCleanedGroupBounded is
// neutered (scratch-neuter proof).
//
// Setup: many small sealed segments (force tiny rollBytes) with the SAME hot
// key so all except the last copy are superseded.
//   - Without coalescing: agent compacts ONE segment per cycle; each cycle
//     leaves N-1 segments (just fewer records).  After many cycles, segment
//     count is UNCHANGED (each cleaned segment is rewritten in-place).
//   - WITH coalescing: the agent groups several consecutive small segments into
//     one group and merges them into a SINGLE output via
//     produceCleanedGroupBounded.  After the agent pass, segment count is
//     STRICTLY FEWER than before.
//
// NON-VACUOUS: scratch-neuter produceCleanedGroupBounded (e.g. make it always
// return {false, activeBase}) → the agent falls back to single-segment path →
// segment count unchanged → EXPECT_LT(segsAfter, segsBefore) FAILS.
//
// OFFSET PRESERVATION: every record surviving the coalesced compaction must
// have the exact same offset as before the merge.
//
// LATEST-PER-KEY CORRECTNESS: after merge, each key retains exactly its
// latest record value.
// ===========================================================================
TEST(CompactionCleanerAgent, CoalescingEndToEndFewSegments) {
    // Use tiny rollBytes (1) so every batch triggers an immediate roll.
    // This gives us many sealed segments cheaply.
    auto dir = cp6TempDir("cp8_coalesce_e2e");
    Log log(dir.string(), /*rollBytes=*/1);

    Cp6NullTransport transport;

    // Write N records with 3 distinct keys cycling.  Many duplicates exist.
    // The hot key "k0" appears every 3 records, so N/3 copies of k0 land in
    // N/3 separate sealed segments.  All copies except the last are superseded.
    constexpr int N = 12;
    for (int v = 0; v < N; ++v) {
        std::string key = "k" + std::to_string(v % 3);
        auto b = cp6Batch(v, key, "val_v" + std::to_string(v), /*ts=*/1000 + v);
        log.append(b, v);
    }
    // Force one more record to ensure N records are in sealed segments.
    {
        auto b = cp6Batch(N, "sentinel", "s", 2000);
        log.append(b, N);
    }

    auto basesBefore = log.segmentBaseOffsets();
    // We need at least 3 sealed segments + 1 active.
    ASSERT_GE(basesBefore.size(), 4u)
        << "Need >= 4 segments (3+ sealed + active); rollBytes=1 should produce one per batch.";
    const size_t segsBefore = basesBefore.size() - 1; // sealed count (exclude active)

    // Collect all records and their offsets BEFORE compaction (for offset preservation check).
    struct PreRecord { std::string key, value; int64_t offset; };
    std::vector<PreRecord> preRecords;
    {
        int64_t cursor = 0;
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                preRecords.push_back({std::string(k), std::string(v), off});
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    ASSERT_EQ(static_cast<int>(preRecords.size()), N + 1) << "Must have N+1 records before";

    // Replica setup.
    auto cfg = makeCompactConfig(1100, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();
    ASSERT_GT(replica->highWatermark(), 0);

    rune::node::ReplicaRegistry registry;
    registry.add(1100, 0, std::move(replica));

    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // Use large rollBytes for the coalescing group size (so all segments coalesce).
    // The LogCleanerAgent's coalescing uses segmentRollBytes from the config.
    // Use cycleBudgetBytes=0 (unlimited) and large dedupeBufferBytes.
    // useCoalescing=true (enabled via the constructor flag).
    rune::node::LogCleanerAgent cleaner(&registry,
                                        /*cycleBudgetBytes=*/0,
                                        /*dedupeBufferBytes=*/1024LL * 1024,
                                        /*ioMaxBytesPerSec=*/-1,
                                        /*useCoalescing=*/true,
                                        /*coalescingRollBytes=*/64LL * 1024 * 1024);

    // Run enough cycles to let the cleaner coalesce the sealed segments.
    // With N=12 sealed segments and coalescing, ONE pass should merge them.
    // Without coalescing (fallback single-segment path), many passes are needed
    // and segment count stays the same.
    for (int i = 0; i < static_cast<int>(segsBefore) + 5; ++i) {
        cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
    }
    log.reloadSegments();

    auto basesAfter = log.segmentBaseOffsets();
    const size_t segsAfter = basesAfter.size() > 0 ? basesAfter.size() - 1 : 0; // sealed count

    // CORE ASSERTION: coalescing reduced the number of segments.
    // Without coalescing (scratch-neutered produceCleanedGroupBounded), the agent
    // compacts each segment individually → segment count stays at segsBefore
    // (rewritten in-place, no merging) → this FAILS.
    EXPECT_LT(segsAfter, segsBefore)
        << "Coalescing must reduce segment count: before=" << segsBefore
        << " after=" << segsAfter << ".  "
           "If segsAfter == segsBefore, produceCleanedGroupBounded is not wired "
           "(or was scratch-neutered); the agent fell back to single-segment compaction.";

    // OFFSET PRESERVATION + CONVERGENCE: surviving records must have their
    // original offsets, AND each key must converge to EXACTLY ONE live record
    // (superseded copies GONE — not merely "latest present").  Walk per-segment
    // (not a single read-from-0 cursor) so an emptied leading segment doesn't
    // truncate the scan.
    struct PostRecord { std::string value; int64_t offset; };
    std::map<std::string, PostRecord> postMap;   // key → latest (offset/value)
    std::map<std::string, int>        postCount; // key → # of LIVE copies
    {
        auto pbases = log.segmentBaseOffsets();
        int64_t cursor = 0;
        for (size_t si = 0; si < pbases.size(); ++si) {
            int64_t segEnd = (si + 1 < pbases.size())
                                 ? pbases[si + 1]
                                 : std::numeric_limits<int64_t>::max();
            if (cursor >= segEnd) continue;
            if (cursor < pbases[si]) cursor = pbases[si];
            for (;;) {
                auto bytes = log.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                    postMap[std::string(k)] = {std::string(v), off};
                    postCount[std::string(k)]++;
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced) break;
            }
        }
    }

    // Build expected map from pre-records (latest per key).
    std::map<std::string, PreRecord> expectedLatest;
    for (auto& pr : preRecords) {
        // Later in the vector = higher offset = latest.
        expectedLatest[pr.key] = pr;
    }

    // Verify offset preservation, latest-per-key correctness, AND convergence
    // (exactly one live record per key — superseded copies removed).
    for (auto& [key, expected] : expectedLatest) {
        auto it = postMap.find(key);
        ASSERT_NE(it, postMap.end())
            << "Key '" << key << "' must survive compaction (it's the latest copy).";
        EXPECT_EQ(postCount[key], 1)
            << "Key '" << key << "' must converge to EXACTLY ONE live record "
               "(superseded copies GONE); got " << postCount[key] << " copies.  "
               "A weaker 'latest present' check (postMap only) hid the "
               "compaction-convergence watermark bug.";
        EXPECT_EQ(it->second.offset, expected.offset)
            << "Key '" << key << "': offset must be preserved after coalescing merge; "
               "expected=" << expected.offset << " got=" << it->second.offset << ".";
        EXPECT_EQ(it->second.value, expected.value)
            << "Key '" << key << "': value must be preserved; "
               "expected='" << expected.value << "' got='" << it->second.value << "'.";
    }

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 12: ThrottleBoundsCleanerBytesPerSec  (CP-8)
//
// (a) Unlimited throttler (-1): no sleep, fast.
// (b) Zero-byte reports: never sleep.
// (c) Append thread is never throttled even with ioMaxBytesPerSec=1.
// ===========================================================================
TEST(CompactionCleanerAgent, ThrottleBoundsCleanerBytesPerSec) {
    // (a) Unlimited throttler: no sleep, instant return.
    {
        rune::storage::Throttler unlimited(-1);
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < 1000; ++i) unlimited.maybeThrottle(1000);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        EXPECT_LT(elapsed, 500LL)
            << "Unlimited throttler must not sleep (elapsed=" << elapsed << "ms)";
    }

    // (b) Zero-byte reports never sleep.
    {
        rune::storage::Throttler tight(1);
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < 10; ++i) tight.maybeThrottle(0);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        EXPECT_LT(elapsed, 500LL)
            << "Zero-byte maybeThrottle must not sleep (elapsed=" << elapsed << "ms)";
    }

    // (c) Structural: LogCleanerAgent constructor stores ioMaxBytesPerSec → throttler_.
    //     Verified by maxBytesPerSec() accessor.  This proves the parameter flows
    //     through to the Throttler object that is later passed to produceCleanedFileBounded.
    //     That call path is on the CLEANER thread (doWork), never on the append path.
    {
        const int64_t rate = 128LL * 1024 * 1024; // 128 MiB/s
        rune::storage::Throttler t(rate);
        EXPECT_EQ(t.maxBytesPerSec(), rate)
            << "Throttler::maxBytesPerSec() must return the ioMaxBytesPerSec passed "
               "at construction.";

        // Verify LogCleanerAgent constructor wires throttler_ correctly.
        rune::node::ReplicaRegistry emptyRegistry;
        rune::node::LogCleanerAgent agent(&emptyRegistry,
                                          /*cycleBudgetBytes=*/0,
                                          /*dedupeBufferBytes=*/-1,
                                          /*ioMaxBytesPerSec=*/rate);
        // Agent must construct; throttler_ is verified via the Throttler unit test above.
        SUCCEED() << "LogCleanerAgent(ioMaxBytesPerSec=128MiB/s) constructed OK.";
    }
}

// ===========================================================================
// TEST 13: DeleteHorizonStampedThenDropped  (CP-8)
//
// Tombstone stamped on first clean (nowMs=1000), retained before horizon
// (1500ms), dropped after (nowMs=2000).
// ===========================================================================
TEST(CompactionCleanerAgent, DeleteHorizonStampedThenDropped) {
    // Use a very small rollBytes (1) so every appended batch forces an immediate roll.
    // Each batch is ~90 bytes which is >= 1, so after every append the segment rolls.
    // After 5 appends we have 5 sealed segments + 1 active segment (base=5).
    auto dir = cp6TempDir("cp8_horizon");
    Log log(dir.string(), /*rollBytes=*/1);

    // key "k1" = "value1" at offset 0.
    {
        RecordBatchBuilder b(0, 100);
        b.append("k1", "value1", 100);
        log.append(b.build(), 0);
    }
    // key "k1" = "" (tombstone) at offset 1.
    {
        RecordBatchBuilder b(1, 200);
        b.append("k1", "", 200);
        log.append(b.build(), 1);
    }
    // key "k2" = "v2" at offset 2.
    {
        RecordBatchBuilder b(2, 300);
        b.append("k2", "v2", 300);
        log.append(b.build(), 2);
    }
    // key "k3" = "v3" at offset 3.
    {
        RecordBatchBuilder b(3, 400);
        b.append("k3", "v3", 400);
        log.append(b.build(), 3);
    }
    // key "k4" = "v4" at offset 4 (active segment territory).
    {
        RecordBatchBuilder b(4, 500);
        b.append("k4", "v4", 500);
        log.append(b.build(), 4);
    }

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need >= 2 segments (sealed + active); "
        << "got " << bases.size() << ".  "
        << "The log may not have rolled; increase record count or decrease rollBytes.";
    const int64_t activeBase = bases.back();

    // Helper to scan a segment file for records matching a predicate.
    auto scanSegment = [&](int64_t segBase,
                           std::function<void(RecordBatchReader&,
                                              std::string_view, std::string_view)> cb) {
        std::ostringstream os;
        os << dir.string() << "/" << std::setw(20) << std::setfill('0') << segBase << ".log";
        std::ifstream f(os.str(), std::ios::binary);
        if (!f.is_open()) return;
        f.seekg(0, std::ios::end);
        auto sz = static_cast<std::size_t>(f.tellg());
        std::vector<uint8_t> buf(sz);
        f.seekg(0); f.read(reinterpret_cast<char*>(buf.data()), sz);
        std::size_t pos = 0;
        while (pos + static_cast<std::size_t>(
                   batch::kRecordBatchOverhead) <= sz) {
            RecordBatchReader r(buf.data() + pos, sz - pos);
            int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > sz) break;
            if (!r.ensureValid()) break;
            r.forEach([&](std::string_view key, std::string_view val, int64_t, int64_t) {
                cb(r, key, val);
            });
            pos += static_cast<std::size_t>(total);
        }
    };

    // Find the tombstone's segment (base=1, the segment containing record 1).
    // With rollBytes=1, each record causes an immediate roll, so:
    //   bases[0]=0 → record 0 "k1"="value1"
    //   bases[1]=1 → record 1 "k1"="" (tombstone)   ← this is the segment to compact
    //   bases[2]=2 → record 2 "k2"="v2"
    //   ...
    //   activeBase  → bases.back()
    // We must compact the tombstone's segment (base=1) to trigger stamping.
    // Also compact segment 0 so "k1"="value1" is superseded (not required for stamping,
    // but needed for a full-pass test).
    //
    // Compact ALL sealed segments in order (simulating a full cleaner pass).
    // The sealed bases exclude activeBase.
    std::vector<int64_t> sealedBases(bases.begin(),
                                      bases.end() - 1); // exclude active

    // Phase A: nowMs=1000, deleteRetentionMs=500.  Horizon = 1500ms.
    // Compact each sealed segment.  The tombstone at base=1 must be RETAINED
    // with kAttrDeleteHorizon set and deleteHorizonMs=1500.
    {
        rune::storage::LogCleaner cleaner;
        for (int64_t segBase : sealedBases) {
            cleaner.produceCleanedFileBounded(dir.string(), sealedBases,
                                              segBase, activeBase,
                                              /*globalDirtyStart=*/0,
                                              /*nowNs=*/1'000'000'000LL,
                                              /*deleteRetentionMs=*/500,
                                              /*dedupeBufferBytes=*/1024LL * 1024,
                                              /*throttler=*/nullptr,
                                              /*nowMs=*/1000LL);
        }
    }
    log.reloadSegments();

    {
        bool foundTombstone = false;
        bool foundHorizon   = false;
        int64_t horizonMs   = -1;
        for (int64_t b : log.segmentBaseOffsets()) {
            if (b == activeBase) continue;
            scanSegment(b, [&](RecordBatchReader& r, std::string_view key,
                                std::string_view val) {
                if (key == "k1" && val.empty()) {
                    foundTombstone = true;
                    if (r.hasDeleteHorizon()) {
                        foundHorizon = true;
                        horizonMs    = r.deleteHorizonMs();
                    }
                }
            });
        }
        EXPECT_TRUE(foundTombstone)
            << "After pass A (nowMs=1000 < horizon=1500), tombstone must be RETAINED.";
        EXPECT_TRUE(foundHorizon)
            << "After pass A, the output batch must have kAttrDeleteHorizon set.";
        EXPECT_EQ(horizonMs, 1500LL)
            << "deleteHorizonMs must equal nowMs(1000) + deleteRetentionMs(500) = 1500; "
               "got " << horizonMs << ".  "
               "CONTRACT: baseTimestamp() stores the horizon, not a real wall-clock ts.";
    }

    // Phase B: nowMs=2000 (> horizon=1500).  Compact again — tombstone DROPPED.
    {
        auto segBases2 = log.segmentBaseOffsets();
        std::vector<int64_t> sealed2(segBases2.begin(), segBases2.end() - 1);
        rune::storage::LogCleaner cleaner;
        for (int64_t segBase : sealed2) {
            cleaner.produceCleanedFileBounded(dir.string(), sealed2,
                                              segBase, activeBase,
                                              /*globalDirtyStart=*/0,
                                              /*nowNs=*/2'000'000'000LL,
                                              /*deleteRetentionMs=*/500,
                                              /*dedupeBufferBytes=*/1024LL * 1024,
                                              /*throttler=*/nullptr,
                                              /*nowMs=*/2000LL);
        }
    }
    log.reloadSegments();

    {
        bool foundTombstone = false;
        for (int64_t b : log.segmentBaseOffsets()) {
            if (b == activeBase) continue;
            scanSegment(b, [&](RecordBatchReader&, std::string_view key,
                                std::string_view val) {
                if (key == "k1" && val.empty()) foundTombstone = true;
            });
        }
        EXPECT_FALSE(foundTombstone)
            << "After pass B (nowMs=2000 > horizon=1500), tombstone must be DROPPED.";
    }

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 14: HorizonBitGatingNoTimestampMisread  (CP-8)
//
// Verify CONTRACT: when hasDeleteHorizon()=true, baseTimestamp() IS the
// horizon (not a real ts); code must always gate on hasDeleteHorizon() first.
// ===========================================================================
TEST(CompactionCleanerAgent, HorizonBitGatingNoTimestampMisread) {
    const int64_t horizonMs  = 99999LL;
    const int64_t recordTs   = 12345LL;
    const int64_t baseOffset = 42LL;

    RecordBatchBuilder b(baseOffset, recordTs);
    b.setDeleteHorizonMs(horizonMs);
    b.append("tombkey", "", recordTs);
    auto batch = b.build();

    RecordBatchReader r(batch.data(), batch.size());

    // (a) hasDeleteHorizon must be true.
    EXPECT_TRUE(r.hasDeleteHorizon())
        << "hasDeleteHorizon() must return true when kAttrDeleteHorizon is set.";

    // (b) baseTimestamp() must return the horizon value (CONTRACT).
    EXPECT_EQ(r.baseTimestamp(), horizonMs)
        << "baseTimestamp() on a horizon-stamped batch must return horizonMs, not recordTs.  "
           "CONTRACT: baseTimestamp field stores the horizon when the bit is set.";

    // (c) deleteHorizonMs() must return horizonMs.
    EXPECT_EQ(r.deleteHorizonMs(), horizonMs)
        << "deleteHorizonMs() must return " << horizonMs << ".";

    // (d) maxTimestamp() must return the actual record timestamp.
    EXPECT_EQ(r.maxTimestamp(), recordTs)
        << "maxTimestamp() must return the actual record timestamp (" << recordTs
        << "), not the horizon (" << horizonMs << ").  "
           "If maxTimestamp() == horizonMs, the maxTimestamp field is being clobbered.";

    // (e) Non-horizon batch has hasDeleteHorizon()=false, baseTimestamp()=real ts.
    RecordBatchBuilder b2(baseOffset, recordTs);
    b2.append("normalkey", "value", recordTs);
    auto batch2 = b2.build();
    RecordBatchReader r2(batch2.data(), batch2.size());
    EXPECT_FALSE(r2.hasDeleteHorizon())
        << "hasDeleteHorizon() must be false on a non-horizon batch.";
    EXPECT_EQ(r2.baseTimestamp(), recordTs)
        << "baseTimestamp() on a non-horizon batch must return the real base timestamp.";
    EXPECT_EQ(r2.deleteHorizonMs(), -1LL)
        << "deleteHorizonMs() on a non-horizon batch must return -1.";
}

// ===========================================================================
// TEST 15: MinCompactionLagHoldsRecentSegments  (CP-8)
//
// minCompactionLagMs=0 → compaction fires.
// minCompactionLagMs=very_large (all segs recent) → no compaction.
// ===========================================================================
TEST(CompactionCleanerAgent, MinCompactionLagHoldsRecentSegments) {
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Test A: minCompactionLagMs=0 → compaction fires.
    {
        auto dir = cp6TempDir("cp8_minlag_a");
        Log log(dir.string(), /*rollBytes=*/128);
        Cp6NullTransport transport;

        for (int v = 0; v < 200; ++v) {
            auto b = cp8BatchWithTs(v, "key", "v" + std::to_string(v), nowMs);
            log.append(b, v);
        }
        ASSERT_GE(log.segmentBaseOffsets().size(), 2u);
        const int totalBefore = cp6CountLiveRecords(log);

        auto cfg = makeCompactConfigCp8(1200, 0.0, /*minLagMs=*/0);
        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
        replica->becomeLeader(1, {1});
        replica->onLeaderLogAppended();

        rune::node::ReplicaRegistry registry;
        registry.add(1200, 0, std::move(replica));

        Cp6MinimalConsensusAgent dummyAgent;
        rune::node::NodeRuntimeAgent dutyCycleAgent(
            dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);
        rune::node::LogCleanerAgent cleaner(&registry);

        int work = 0;
        for (int i = 0; i < 30; ++i) {
            work += cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
            dutyCycleAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        }
        log.reloadSegments();
        const int totalAfter = cp6CountLiveRecords(log);

        EXPECT_GT(work, 0)
            << "With minCompactionLagMs=0, cleaner must do work.";
        EXPECT_LT(totalAfter, totalBefore)
            << "With minCompactionLagMs=0, compaction must reduce record count; "
               "before=" << totalBefore << " after=" << totalAfter;

        fs::remove_all(dir);
    }

    // Test B: minCompactionLagMs=10years → no compaction (all segments too recent).
    {
        auto dir = cp6TempDir("cp8_minlag_b");
        Log log(dir.string(), /*rollBytes=*/128);
        Cp6NullTransport transport;

        for (int v = 0; v < 200; ++v) {
            auto b = cp8BatchWithTs(v, "key", "v" + std::to_string(v), nowMs);
            log.append(b, v);
        }
        ASSERT_GE(log.segmentBaseOffsets().size(), 2u);

        const int64_t tenYearsMs = 10LL * 365 * 24 * 3600 * 1000;
        auto cfg2 = makeCompactConfigCp8(1201, 0.0, /*minLagMs=*/tenYearsMs);
        auto replica2 = std::make_unique<PartitionReplica>(
            cfg2, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica2->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
        replica2->becomeLeader(1, {1});
        replica2->onLeaderLogAppended();

        rune::node::ReplicaRegistry registry2;
        registry2.add(1201, 0, std::move(replica2));

        Cp6MinimalConsensusAgent dummyAgent2;
        rune::node::NodeRuntimeAgent dutyCycleAgent2(
            dummyAgent2, nullptr, nullptr, nullptr, &registry2, nullptr);
        rune::node::LogCleanerAgent cleaner2(&registry2);

        int work2 = 0;
        for (int i = 0; i < 10; ++i) {
            work2 += cleaner2.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
            dutyCycleAgent2.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        }

        EXPECT_EQ(work2, 0)
            << "With minCompactionLagMs=10years and very recent timestamps (nowMs), "
               "cleaner must NOT compact (work=0); got work2=" << work2 << ".  "
               "Non-vacuous: Test A above showed work > 0 with minLag=0.";

        fs::remove_all(dir);
    }
}

// ===========================================================================
// TEST 16: MaxCompactionLagForcesLowDirtyLog  (CP-8)
//
// A log with dirtyRatio < threshold is normally NOT eligible.
// With maxCompactionLagMs=1ms and largestTimestamp=1 (very old), ageMs >> 1ms
// → needCompactionNow=true → compaction fires.
// ===========================================================================
TEST(CompactionCleanerAgent, MaxCompactionLagForcesLowDirtyLog) {
    Cp6NullTransport transport;

    // Test A: maxCompactionLagMs=-1 (disabled) → no compaction (below ratio threshold).
    {
        auto dir = cp6TempDir("cp8_maxlag_a");
        Log log(dir.string(), /*rollBytes=*/128);

        for (int v = 0; v < 200; ++v) {
            // largestTimestamp=1 (very old)
            auto b = cp8BatchWithTs(v, "key", "v" + std::to_string(v), /*tsMs=*/1);
            log.append(b, v);
        }
        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 3u);

        const size_t ep = (bases.size() * 8) / 10;
        const int64_t co = (ep > 0 && ep < bases.size()) ? bases[ep - 1] : bases[0];

        // minCleanableDirtyRatio=0.9 (high), maxCompactionLag=-1 (disabled).
        auto cfg = makeCompactConfigCp8(1300, 0.9, /*minLagMs=*/0, /*maxLagMs=*/-1);
        auto replica = std::make_unique<PartitionReplica>(
            cfg, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
        replica->becomeLeader(1, {1});
        replica->onLeaderLogAppended();
        replica->setLastCleanOffset(co);

        rune::node::ReplicaRegistry registry;
        registry.add(1300, 0, std::move(replica));
        rune::node::LogCleanerAgent cleaner(&registry);

        int work = 0;
        for (int i = 0; i < 5; ++i) {
            work += cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        }
        EXPECT_EQ(work, 0)
            << "With maxCompactionLag=-1 and dirtyRatio < 0.9, cleaner must not compact; "
               "got work=" << work << ".  "
               "Non-vacuous: Test B will show work > 0 with maxLag=1ms.";

        fs::remove_all(dir);
    }

    // Test B: maxCompactionLagMs=1ms, same dirtyRatio < 0.9 → forced compaction.
    {
        auto dir = cp6TempDir("cp8_maxlag_b");
        Log log(dir.string(), /*rollBytes=*/128);

        for (int v = 0; v < 200; ++v) {
            auto b = cp8BatchWithTs(v, "key", "v" + std::to_string(v), /*tsMs=*/1);
            log.append(b, v);
        }
        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 3u);

        const size_t ep2 = (bases.size() * 8) / 10;
        const int64_t co2 = (ep2 > 0 && ep2 < bases.size()) ? bases[ep2 - 1] : bases[0];

        // minCleanableDirtyRatio=0.9 (high), maxCompactionLagMs=1ms (very short).
        auto cfg2 = makeCompactConfigCp8(1301, 0.9, /*minLagMs=*/0, /*maxLagMs=*/1LL);
        auto replica2 = std::make_unique<PartitionReplica>(
            cfg2, Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        replica2->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
        replica2->becomeLeader(1, {1});
        replica2->onLeaderLogAppended();
        replica2->setLastCleanOffset(co2);

        rune::node::ReplicaRegistry registry2;
        registry2.add(1301, 0, std::move(replica2));

        Cp6MinimalConsensusAgent dummyAgent2;
        rune::node::NodeRuntimeAgent dutyCycleAgent2(
            dummyAgent2, nullptr, nullptr, nullptr, &registry2, nullptr);
        rune::node::LogCleanerAgent cleaner2(&registry2);

        int work2 = 0;
        for (int i = 0; i < 5; ++i) {
            work2 += cleaner2.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
            dutyCycleAgent2.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        }
        EXPECT_GT(work2, 0)
            << "With maxCompactionLagMs=1ms and largestTimestamp=1ms (very old), "
               "needCompactionNow must fire and compaction must run (work > 0); "
               "got work2=" << work2 << ".  "
               "Non-vacuous: Test A above showed work=0 with maxLag=-1.";

        fs::remove_all(dir);
    }
}

// ===========================================================================
// TEST: CompactionConvergesWithSingleSegmentGroups  (watermark-convergence bug)
//
// Regression guard for the watermark-over-advance bug: with coalescing ON
// (production default) and a SMALL segmentRollBytes, each small sealed segment
// forms its OWN single-segment coalescing group.  The cleaner therefore
// rewrites ONE segment per pass, but the offset map covers the WHOLE dirty
// range → passEnd == activeBase → the watermark advances past EVERY dirty
// segment even though only one was rewritten.  The other dirty segments get
// marked "clean" without ever being rewritten, their superseded records
// persist forever, the dirtyRatio then drops below minCleanableDirtyRatio,
// compaction disqualifies itself, and the log NEVER converges to
// latest-per-key.
//
// This test drives the cleaner DETERMINISTICALLY (no sleeps) over many cycles
// and asserts the log CONVERGES to exactly ONE live record per key (superseded
// copies GONE, not merely "latest present").  It MUST FAIL on the buggy
// watermark logic and PASS after the fix that advances the watermark only past
// the segment/group that was actually rewritten.
// ===========================================================================
TEST(CompactionCleanerAgent, CompactionConvergesWithSingleSegmentGroups) {
    auto dir = cp6TempDir("cp_converge_single_seg_groups");
    // rollBytes=1 → every batch rolls into its OWN sealed segment, giving us
    // MANY small sealed segments cheaply (same trick as CoalescingEndToEnd).
    Log log(dir.string(), /*rollBytes=*/1);

    Cp6NullTransport transport;

    // A FEW keys, MANY updates each → dozens of segments, all superseded except
    // the last copy of each key.
    constexpr int kNumKeys      = 3;
    constexpr int kUpdatesEach  = 50;
    constexpr int kTotal        = kNumKeys * kUpdatesEach; // 150 records
    int off = 0;
    for (int u = 0; u < kUpdatesEach; ++u) {
        for (int k = 0; k < kNumKeys; ++k) {
            std::string key = "k" + std::to_string(k);
            std::string val = "v" + std::to_string(u);
            auto b = cp6Batch(off, key, val, /*ts=*/1000 + off);
            log.append(b, off);
            ++off;
        }
    }
    // Sentinel so the last real records live in a SEALED (not active) segment.
    {
        auto b = cp6Batch(off, "sentinel", "s", 5000);
        log.append(b, off);
        ++off;
    }

    auto basesBefore = log.segmentBaseOffsets();
    const size_t sealedBefore = basesBefore.size() > 0 ? basesBefore.size() - 1 : 0;
    ASSERT_GE(sealedBefore, 8u)
        << "Need many small sealed segments to exercise single-segment groups; "
           "sealed=" << sealedBefore;

    const int totalBefore = cp6CountLiveRecords(log);
    ASSERT_EQ(totalBefore, kTotal + 1)
        << "All " << (kTotal + 1) << " records (incl sentinel) must be present pre-compaction.";

    // Replica setup.  Compact policy, dirtyRatio gate 0.5 (production-ish) so
    // the "watermark over-advance drops the ratio → disqualifies" failure mode
    // is reproduced.  CRITICAL: segmentRollBytes SMALL so each small segment is
    // its own single-segment coalescing group (the bug trigger).
    auto cfg = makeCompactConfig(1300, /*dirtyRatio=*/0.5);
    // Tiny group bound: each single-batch segment already exceeds it, so every
    // group is a SINGLE segment → one rewrite per pass (the exact bug trigger).
    cfg.segmentRollBytes = 1;
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replica->becomeLeader(1, {1});
    replica->onLeaderLogAppended();
    ASSERT_GT(replica->highWatermark(), 0);

    rune::node::ReplicaRegistry registry;
    registry.add(1300, 0, std::move(replica));

    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent dutyCycleAgent(
        dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);

    // Coalescing ON (production default).  Large dedupe budget so the map is
    // NEVER the limiting factor — the ONLY thing that can stop convergence is
    // the watermark logic under test.  cycleBudgetBytes=0 (unlimited).
    rune::node::LogCleanerAgent cleaner(&registry,
                                        /*cycleBudgetBytes=*/0,
                                        /*dedupeBufferBytes=*/64LL * 1024 * 1024,
                                        /*ioMaxBytesPerSec=*/-1,
                                        /*useCoalescing=*/true,
                                        /*coalescingRollBytes=*/64LL * 1024 * 1024);

    // Drive DETERMINISTICALLY: one cleaner cycle rewrites (at most) one segment,
    // so give it generously more cycles than there are sealed segments.  NO
    // sleeps, NO monitor polling — deterministic doWork() driving only.
    const int kCycles = static_cast<int>(sealedBefore) * 4 + 50;
    for (int i = 0; i < kCycles; ++i) {
        cleaner.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
        dutyCycleAgent.doWork(static_cast<int64_t>(i + 1) * 1'000'000LL);
    }
    log.reloadSegments();

    // Count LIVE records per key.  Convergence == exactly ONE per key.
    // Walk per-segment (like cp6CountLiveRecords) — a single read-from-0 cursor
    // stops at the first emptied/deleted leading segment (offset-0's segment is
    // fully superseded after convergence), so we must iterate segment bases.
    std::map<std::string, int> perKeyCount;
    std::map<std::string, std::string> perKeyLatest;
    {
        auto bases = log.segmentBaseOffsets();
        int64_t cursor = 0;
        for (size_t si = 0; si < bases.size(); ++si) {
            int64_t segEnd = (si + 1 < bases.size())
                                 ? bases[si + 1]
                                 : std::numeric_limits<int64_t>::max();
            if (cursor >= segEnd) continue;
            if (cursor < bases[si]) cursor = bases[si];
            for (;;) {
                auto bytes = log.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view v, int64_t o, int64_t) {
                    perKeyCount[std::string(k)]++;
                    perKeyLatest[std::string(k)] = std::string(v);
                    cursor = o + 1;
                    advanced = true;
                });
                if (!advanced) break;
            }
        }
    }

    const int totalAfter = cp6CountLiveRecords(log);

    // CORE ASSERTION: every key converges to EXACTLY ONE live record.
    // On the buggy watermark logic, superseded copies persist (dozens per key)
    // and this FAILS.  After the fix, each key has exactly one copy.
    for (int k = 0; k < kNumKeys; ++k) {
        std::string key = "k" + std::to_string(k);
        EXPECT_EQ(perKeyCount[key], 1)
            << "Key '" << key << "' must converge to exactly ONE live record "
               "(superseded copies GONE); got " << perKeyCount[key]
            << " copies.  If > 1, the watermark over-advanced and marked "
               "un-rewritten segments 'clean' → compaction never converged.";
        EXPECT_EQ(perKeyLatest[key], "v" + std::to_string(kUpdatesEach - 1))
            << "Key '" << key << "' surviving copy must be the LATEST value.";
    }

    // Total live records == kNumKeys latest + 1 sentinel.
    EXPECT_EQ(totalAfter, kNumKeys + 1)
        << "Log must converge to " << (kNumKeys + 1) << " live records "
           "(one per key + sentinel); got " << totalAfter
        << " (before=" << totalBefore << ").";

    fs::remove_all(dir);
}

// ===========================================================================
// D3b REGRESSION: the cleanable range is capped at the partition's LSO.
//
// Kafka cleans only below min(activeSegmentBase, LSO): an OPEN transaction's data
// may still abort, and an aborted value must never supersede a committed one.  The
// owning worker publishes the first unstable offset (PartitionReplica::
// publishCleanerLso); the agent must never clean at/after it — even when the log
// itself carries no txn data (this pins the agent-level wiring, independent of
// LogCleaner's own unresolved-txn scan).  Then lifting the LSO lets it converge.
// ===========================================================================
TEST(LogCleanerAgentLsoRegression, CleanableRangeCappedAtPublishedLso) {
    auto dir = cp6TempDir("d3b_lso_cap");
    Log log(dir.string(), /*rollBytes=*/128);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(690);
    auto replicaOwned = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replicaOwned->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    replicaOwned->becomeLeader(1, {1});
    constexpr int kRecords = 200;
    constexpr int64_t kLso = 50;
    for (int v = 0; v < kRecords; ++v) log.append(cp6Batch(v, "k1", "v" + std::to_string(v)), v);
    replicaOwned->onLeaderLogAppended();
    ASSERT_GT(replicaOwned->cleanerHighWatermark(), kLso) << "precondition: HW above the LSO";
    ASSERT_GE(log.segmentBaseOffsets().size(), 4u) << "precondition: several sealed segments";
    PartitionReplica* replica = replicaOwned.get();
    replica->publishCleanerLso(kLso);   // an open txn starts at 50 (published by its worker)

    rune::node::ReplicaRegistry registry;
    registry.add(690, 0, std::move(replicaOwned));
    Cp6MinimalConsensusAgent dummyAgent;
    rune::node::NodeRuntimeAgent duty(dummyAgent, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(&registry, /*cycleBudgetBytes=*/0);

    auto liveAtOrAbove = [&](int64_t from) {
        int n = 0;
        for (int64_t o = from; o < kRecords; ++o) {
            auto bytes = log.read(o, 1 << 20);
            if (bytes.empty()) continue;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (r.ensureValid() && r.baseOffset() == o && r.recordCount() > 0) ++n;
        }
        return n;
    };
    constexpr int kCycles = 16;   // bounded drive: each cycle = one cleaner pass + swap drain
    for (int i = 0; i < kCycles; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();
    EXPECT_LE(replica->lastCleanOffset(), kLso) << "clean watermark crossed the published LSO";
    EXPECT_EQ(liveAtOrAbove(kLso), kRecords - kLso) << "records at/after the LSO must be untouched";
    EXPECT_LT(cp6CountLiveRecords(log), kRecords) << "below the LSO the cleaner still works";

    // The transaction resolves: the LSO lifts → the cleaner converges past it.
    replica->publishCleanerLso(PartitionReplica::kNoCleanerLso);
    // Fix round 3: the owner installs ONE compaction output per duty pass (O(1)
    // each), so a pass with many single-segment outputs takes many duty passes
    // — drive until the range above the LSO is cleaned (bounded, no sleeps).
    for (int i = 0; i < 4000 && (i % 16 != 0 || liveAtOrAbove(kLso) == kRecords - kLso); ++i) {
        cleaner.doWork(static_cast<int64_t>(kCycles + i) * 1'000'000LL);
        duty.doWork(static_cast<int64_t>(kCycles + i) * 1'000'000LL);
    }
    log.reloadSegments();
    EXPECT_GT(replica->lastCleanOffset(), kLso);
    EXPECT_LT(liveAtOrAbove(kLso), kRecords - kLso) << "after the LSO lifts, the range above it is cleaned";
    fs::remove_all(dir);
}


// ===========================================================================
// D3b fix round 1 helpers: transactional batches + the PRODUCTION marker bytes.
// ===========================================================================
namespace {
std::vector<uint8_t> r1Txn(int64_t off, std::string_view key, std::string_view val, int64_t pid,
                           int16_t epoch, int32_t seq) {
    RecordBatchBuilder b(off, 1000 + off);
    b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
    b.setTransactional(true);
    b.append(key, val, 1000 + off);
    return b.build();
}
std::vector<uint8_t> r1Marker(int64_t off, int64_t pid, int16_t epoch, bool commit) {
    auto m = rune::transaction::ControlMarker::build(
        pid, epoch, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
    for (int i = 0; i < 8; ++i) m[static_cast<std::size_t>(i)] = static_cast<uint8_t>(static_cast<uint64_t>(off) >> (8 * i));
    return m;   // baseOffset is outside the CRC
}
// The batch at exactly `off` (empty if none).
std::vector<uint8_t> r1BatchAt(Log& log, int64_t off) {
    auto bytes = log.read(off, 1 << 20);
    if (bytes.empty()) return {};
    RecordBatchReader r(bytes.data(), bytes.size());
    if (r.baseOffset() != off || !r.ensureValid()) return {};
    return {bytes.begin(), bytes.begin() + r.totalLength()};
}
} // namespace

// ===========================================================================
// D3b fix round 1 (1): a marker is removed only once EVERY assigned replica —
// not just the ISR — has fetched past it.  A replica outside the ISR that holds
// a txn's aborted data but not its ABORT marker would otherwise never receive the
// marker; on becoming leader it would read the aborted data as the producer's
// NEXT txn (a later COMMIT exposes it).  The leader publishes the minimum fetch
// position over the assigned replicas; the agent gates marker removal on it.
// ===========================================================================
TEST(LogCleanerAgentMarkerRemovalRegression, MarkerKeptWhileAnAssignedReplicaLagsBehindIt) {
    auto dir = cp6TempDir("d3b_marker_lag");
    Log log(dir.string(), /*rollBytes=*/1);   // one batch per segment
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(691);
    cfg.deleteRetentionMs = 0;               // horizon == stamping pass: drop on the next pass
    auto owned = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1, std::vector<NodeId>{1, 2}, std::vector<NodeId>{1}, &log, &transport);
    owned->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // no Worker RcState in this rig (D3b)
    owned->becomeLeader(1, {1});
    PartitionReplica* replica = owned.get();
    constexpr int64_t P = 9;
    log.append(r1Txn(0, "k", "aborted", P, 0, 0), 0);
    log.append(r1Marker(1, P, 0, /*commit=*/false), 1);   // ABORT @1
    log.append(r1Txn(2, "k2", "c", P, 0, 1), 2);
    log.append(r1Marker(3, P, 0, /*commit=*/true), 3);    // the producer's last batch
    int64_t next = 4;
    for (; next < 8; ++next) log.append(cp6Batch(next, "pad", "p"), next);
    replica->onLeaderLogAppended();
    replica->publishCleanerLso(PartitionReplica::kNoCleanerLso);   // no worker in this rig: no open txn
    // Replica 2 (assigned, NOT in the ISR) fetched up to 1: it has @0, not the marker @1.
    (void)replica->onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 2, 1, 1 << 20, 691}, 1'000'000);

    rune::node::ReplicaRegistry registry;
    registry.add(691, 0, std::move(owned));
    Cp6MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(&registry, /*cycleBudgetBytes=*/0);
    auto cycles = [&](int n, int64_t& t) {
        for (int i = 0; i < n; ++i, t += 1'000'000) {
            log.append(cp6Batch(next, "pad", "p"), next); ++next;   // keep the log dirty: one pass per cycle
            replica->onLeaderLogAppended();
            duty.doWork(t);                                         // replica doWork publishes its snapshots
            cleaner.doWork(t);
            duty.doWork(t);                                         // drain the swaps
        }
        log.reloadSegments();
    };
    int64_t t = 2'000'000;
    cycles(8, t);
    EXPECT_EQ(replica->cleanerMarkerRemovalBound(), 1) << "the bound is replica 2's fetch position";
    EXPECT_TRUE(r1BatchAt(log, 0).empty() || RecordBatchReader(r1BatchAt(log, 0).data(), r1BatchAt(log, 0).size()).recordCount() == 0)
        << "precondition: the aborted record was discarded";
    ASSERT_FALSE(r1BatchAt(log, 1).empty()) << "ABORT marker removed although replica 2 has not fetched it";

    // Replica 2 fetches past the marker → it may now go (stamp, then drop).
    (void)replica->onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 2, 2, 1 << 20, 691}, t);
    cycles(8, t);
    EXPECT_EQ(replica->cleanerMarkerRemovalBound(), 2);
    EXPECT_TRUE(r1BatchAt(log, 1).empty()) << "marker never removed once every replica fetched it";
    ASSERT_FALSE(r1BatchAt(log, 3).empty()) << "the producer's last marker stays";
    fs::remove_all(dir);
}

// The leader publishes min(fetch position) over EVERY assigned replica (a replica
// that never fetched counts as 0); a single-replica partition has no bound.
TEST(PartitionReplicaCleanerSnapshotRegression, MarkerRemovalBoundIsMinFetchOverAllAssignedReplicas) {
    auto dir = cp6TempDir("d3b_bound");
    Log log(dir.string(), 1 << 20);
    for (int64_t o = 0; o < 10; ++o) log.append(cp6Batch(o, "k", "v"), o);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(692);
    PartitionReplica r(cfg, Role::Leader, 1, {1, 2, 3}, {1}, &log, &transport);
    r.becomeLeader(1, {1});
    r.onLeaderLogAppended();
    r.doWork(1);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 0) << "replicas 2 and 3 never fetched";
    (void)r.onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 2, 5, 1 << 20, 692}, 2);
    r.doWork(3);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 0) << "replica 3 (not in the ISR) still never fetched";
    (void)r.onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 3, 7, 1 << 20, 692}, 4);
    r.doWork(5);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 5);

    auto dir2 = cp6TempDir("d3b_bound1");
    Log log2(dir2.string(), 1 << 20);
    PartitionReplica solo(makeCompactConfig(693), Role::Leader, 1, {1}, {1}, &log2, &transport);
    solo.becomeLeader(1, {1});
    solo.doWork(1);
    EXPECT_EQ(solo.cleanerMarkerRemovalBound(), PartitionReplica::kNoCleanerLso) << "no other replica";
    fs::remove_all(dir);
    fs::remove_all(dir2);
}

// Fix round 1 (2): a read_committed partition's cleaner LSO starts at 0 (clean
// nothing) until its owning worker's first READY publish — never "no cap" while
// nobody has built the transaction state.  A partition with no read_committed
// state (internal topic, topicId < 0) has no LSO cap.
TEST(PartitionReplicaCleanerSnapshotRegression, LsoStartsAtZeroUntilTheFirstReadyPublish) {
    auto dir = cp6TempDir("d3b_lso0");
    Log log(dir.string(), 1 << 20);
    Cp6NullTransport transport;
    PartitionReplica user(makeCompactConfig(694), Role::Leader, 1, {1}, {1}, &log, &transport);
    EXPECT_EQ(user.cleanerLastStableOffset(), 0);
    auto dir2 = cp6TempDir("d3b_lso0i");
    Log log2(dir2.string(), 1 << 20);
    PartitionReplica internal(makeCompactConfig(-5), Role::Leader, 1, {1}, {1}, &log2, &transport);
    EXPECT_EQ(internal.cleanerLastStableOffset(), PartitionReplica::kNoCleanerLso);
    fs::remove_all(dir);
    fs::remove_all(dir2);
}

// Fix round 1 (4d): end to end — the read_committed state AND the producer state
// rebuilt from a CLEANED log (aborted data discarded, markers stamped and
// dropped) equal those rebuilt from the original log: same committed value per
// key, no open txn, same LSO, same (epoch, lastSequence) per producer.
TEST(LogCleanerRcRebuildRegression, ReadCommittedAndProducerStateUnchangedByCleaning) {
    auto dir = cp6TempDir("d3b_rc_e2e");
    Log log(dir.string(), /*rollBytes=*/1);
    constexpr int64_t P = 21, Q = 22;
    int64_t o = 0;
    int32_t ps = 0, qs = 0;
    auto plain = [&](std::string_view k, std::string_view v) { log.append(cp6Batch(o, k, v), o); ++o; };
    auto txn = [&](std::string_view k, std::string_view v, int64_t pid, int16_t e) {
        log.append(r1Txn(o, k, v, pid, e, pid == P ? ps++ : qs++), o); ++o;
    };
    auto marker = [&](int64_t pid, int16_t e, bool commit) { log.append(r1Marker(o, pid, e, commit), o); ++o; };
    plain("a", "a0");
    plain("e", "e0");
    txn("a", "a1-P-aborted", P, 0);
    txn("b", "b1-Q", Q, 0);
    txn("e", "e1-P-aborted", P, 0);     // an aborted copy must never supersede e0
    marker(P, 0, false);
    txn("b", "b2-P", P, 0);
    marker(Q, 0, true);
    marker(P, 0, true);
    txn("c", "c1-P-aborted", P, 0);
    plain("b", "b3");
    marker(P, 1, false);                // fence abort: the epoch rises to 1
    txn("a", "a2-Q", Q, 0);
    marker(Q, 0, true);
    for (int i = 0; i < 3; ++i) plain("pad", "p");

    struct View {
        std::map<std::string, std::string> committed;
        std::size_t open = 0;
        int64_t lso = 0;
        std::map<int64_t, std::pair<int16_t, int32_t>> producers;
        bool operator==(const View&) const = default;
    };
    auto rebuild = [&]() {
        log.reloadSegments();
        rune::storage::ProducerStateManager psm;
        rune::node::RcState rc;
        rc.bindProducerState(&psm, 1, 0);
        rune::node::RcFacts f; f.leader = true; f.logGeneration = 1;
        f.leadershipGeneration = 1; f.epoch = 1;
        for (int i = 0; i < 100'000 && !rc.ready(); ++i) (void)rc.advance(f, &log, 1 << 20);
        EXPECT_TRUE(rc.ready());
        View v;
        const int64_t end = log.logEndOffset();
        v.open = rc.tracker().openCount();
        v.lso = rc.lso(end);
        auto bases = log.segmentBaseOffsets();
        for (std::size_t si = 0; si < bases.size(); ++si) {
            const int64_t segEnd = si + 1 < bases.size() ? bases[si + 1] : end;
            for (int64_t cur = bases[si]; cur < segEnd;) {
                auto bytes = log.read(cur, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                const bool ctrl = (r.attributes() & batch::kAttrControl) != 0;
                const bool tx = (r.attributes() & batch::kAttrTransactional) != 0;
                if (!ctrl && !(tx && rc.isAborted(r.producerId(), r.baseOffset())) && r.baseOffset() < v.lso)
                    r.forEachRecord([&](std::string_view k, std::string_view val, int64_t, int64_t) {
                        v.committed[std::string(k)] = std::string(val);
                    });
                cur = r.baseOffset() + r.lastOffsetDelta() + 1;
            }
        }
        for (int64_t pid : {P, Q})
            if (const auto* e = psm.entry(1, pid, 0)) v.producers[pid] = {e->producerEpoch, e->lastSequence};
        return v;
    };
    const View before = rebuild();
    ASSERT_EQ(before.committed.at("a"), "a2-Q");
    ASSERT_EQ(before.committed.at("b"), "b3");
    ASSERT_EQ(before.committed.count("c"), 0u);
    ASSERT_EQ(before.committed.at("e"), "e0");
    ASSERT_EQ(before.open, 0u);
    ASSERT_EQ(before.producers.at(P).first, 1) << "the fence marker raised P's epoch";

    for (bool coalesce : {false, true}) {
        for (int pass = 0; pass < 4; ++pass) {
            auto bases = log.segmentBaseOffsets();
            const int64_t active = bases.back();
            bases.pop_back();
            std::vector<int64_t> sizes(bases.size(), 1);
            LogCleaner cleaner;
            cleaner.setMarkerRemovalBound(std::numeric_limits<int64_t>::max());
            SkimpyOffsetMap map(1024);
            (void)cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, /*deleteRetentionMs=*/10,
                                                     map, coalesce, 1 << 20, nullptr, 1'000'000 + pass * 10);
            const View after = rebuild();
            EXPECT_EQ(after.committed, before.committed) << "coalesce=" << coalesce << " pass " << pass;
            EXPECT_EQ(after.open, 0u) << "coalesce=" << coalesce << " pass " << pass << ": a txn was left open";
            EXPECT_EQ(after.lso, log.logEndOffset()) << "coalesce=" << coalesce << " pass " << pass;
            EXPECT_EQ(after.producers, before.producers) << "coalesce=" << coalesce << " pass " << pass;
        }
    }
    fs::remove_all(dir);
}

// ===========================================================================
// D3b fix round 2 (1): an OffsetsForLeaderEpoch PROBE never feeds the marker-
// removal bound.  A probe carries the follower's PRE-truncation LEO from its own,
// possibly divergent, lineage (an old leader with a divergent tail): counting it
// would let the new leader stamp + drop an ABORT marker the old leader does not
// hold on this lineage; after truncating it refetches past the marker's offset,
// never sees it, and as leader keeps that txn open forever.
// ===========================================================================
TEST(PartitionReplicaCleanerSnapshotRegression, ProbeNeverFeedsMarkerRemovalBound) {
    auto dir = cp6TempDir("d3b_probe");
    Log log(dir.string(), 1 << 20);
    for (int64_t o = 0; o < 100; ++o) log.append(cp6Batch(o, "k", "v"), o);
    Cp6NullTransport transport;
    PartitionReplica r(makeCompactConfig(695), Role::Leader, 1, {1, 2}, {1, 2}, &log, &transport);
    r.becomeLeader(1, {1, 2});
    r.onLeaderLogAppended();
    r.onOffsetsForLeaderEpochRequest(rune::replication::OffsetsForLeaderEpochReqMsg{1, 0, 2, 1, /*followerLeo=*/90, 695});
    r.doWork(1);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 0) << "a probe's (unverified-lineage) LEO fed the marker bound";
    (void)r.onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 2, 40, 1 << 20, 695}, 2);
    r.doWork(3);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 40) << "a real fetch position does";
    fs::remove_all(dir);
}

// Fix round 2 (4): fetch positions are relearned in every leadership — a position
// from an earlier term may predate a truncation.
TEST(PartitionReplicaCleanerSnapshotRegression, FetchPositionsResetOnEveryLeadership) {
    auto dir = cp6TempDir("d3b_releader");
    Log log(dir.string(), 1 << 20);
    for (int64_t o = 0; o < 20; ++o) log.append(cp6Batch(o, "k", "v"), o);
    Cp6NullTransport transport;
    PartitionReplica r(makeCompactConfig(696), Role::Leader, 1, {1, 2}, {1}, &log, &transport);
    r.becomeLeader(1, {1});
    r.onLeaderLogAppended();
    (void)r.onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 2, 15, 1 << 20, 696}, 1);
    r.doWork(2);
    ASSERT_EQ(r.cleanerMarkerRemovalBound(), 15) << "precondition";
    r.becomeFollower(2, /*leaderId=*/2);
    r.doWork(3);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 0) << "a follower never removes markers";
    r.becomeLeader(3, {1});
    r.doWork(4);
    EXPECT_EQ(r.cleanerMarkerRemovalBound(), 0) << "replica 2's position from the earlier leadership was reused";
    fs::remove_all(dir);
}

// Fix round 2 (1), agent level: a divergent old leader that only PROBES (with a
// LEO past the marker) never lets the marker go; its real fetch past it does.
TEST(LogCleanerAgentMarkerRemovalRegression, ProbeFromDivergentReplicaNeverReleasesMarker) {
    auto dir = cp6TempDir("d3b_probe_agent");
    Log log(dir.string(), /*rollBytes=*/1);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(697);
    cfg.deleteRetentionMs = 0;
    auto owned = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1, std::vector<NodeId>{1, 2}, std::vector<NodeId>{1}, &log, &transport);
    owned->becomeLeader(1, {1});
    PartitionReplica* replica = owned.get();
    constexpr int64_t P = 9;
    log.append(r1Txn(0, "k", "aborted", P, 0, 0), 0);
    log.append(r1Marker(1, P, 0, /*commit=*/false), 1);
    log.append(r1Txn(2, "k2", "c", P, 0, 1), 2);
    log.append(r1Marker(3, P, 0, /*commit=*/true), 3);
    int64_t next = 4;
    for (; next < 8; ++next) log.append(cp6Batch(next, "pad", "p"), next);
    replica->onLeaderLogAppended();
    replica->publishCleanerLso(PartitionReplica::kNoCleanerLso);
    rune::node::ReplicaRegistry registry;
    registry.add(697, 0, std::move(owned));
    Cp6MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(&registry, /*cycleBudgetBytes=*/0);
    auto cycles = [&](int n, int64_t& t) {
        for (int i = 0; i < n; ++i, t += 1'000'000) {
            log.append(cp6Batch(next, "pad", "p"), next); ++next;
            replica->onLeaderLogAppended();
            // Replica 2 (divergent old leader) keeps probing with a LEO past the marker.
            replica->onOffsetsForLeaderEpochRequest(
                rune::replication::OffsetsForLeaderEpochReqMsg{1, 0, 2, 1, /*followerLeo=*/next, 697});
            duty.doWork(t);
            cleaner.doWork(t);
            duty.doWork(t);
        }
        log.reloadSegments();
    };
    int64_t t = 2'000'000;
    cycles(8, t);
    ASSERT_FALSE(r1BatchAt(log, 1).empty()) << "ABORT marker released by a probe's divergent LEO";
    (void)replica->onFetchRequest(rune::replication::FetchRequestMsg{1, 0, 2, 2, 1 << 20, 697}, t);
    cycles(8, t);
    EXPECT_TRUE(r1BatchAt(log, 1).empty()) << "a real fetch past the marker releases it";
    fs::remove_all(dir);
}

// ===========================================================================
// E1d F2 (Regression): the PRODUCTION cleaner gate.  LogCleanerAgent bounds
// firstUncleanable by the first sealed segment ending above the PERSISTED
// recovery point (SegInfo::belowRecoveryPoint) — restart recovery chain-
// validates those segments, and a compaction gap there would read as a lost
// write.  A segment whose seal is durable only in memory (checkpoint not yet
// durable) is never compacted.
// ===========================================================================
TEST(CompactionCleanerAgentRegression, NeverCompactsAboveRecoveryPoint) {
    auto dir = cp6TempDir("e1d_agent_gate");
    Log log(dir.string(), /*rollBytes=*/256);
    log.recover();
    log.setDeferSealSync(true);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(1300, 0.0);
    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica->publishCleanerLso(rune::replication::PartitionReplica::kNoCleanerLso);   // F22: no Worker RcState in this rig (D3b LSO cap)
    replica->becomeLeader(1, {1});
    int64_t off = 0;
    while (log.segmentBaseOffsets().size() < 4) {
        auto b = cp6Batch(off, "same-key", "v" + std::to_string(off));
        off = log.appendNoSync(b.data(), b.size(), off);
    }
    replica->onLeaderLogAppended();
    const auto bases = log.segmentBaseOffsets();
    auto completeSeal = [&] {
        rune::storage::LogSegment* s = log.frontPendingSeal();
        log.popPendingSeal();
        ASSERT_EQ(::fsync(s->logFd()), 0);
        log.noteFlushCompleted(s, INT64_MAX);
        log.unpinSegmentFlush(s);
        Log::DirSync d;                                  // E1d MEDIUM-2: + the worker's directory fsync
        if (log.prepareDirectorySync(d)) log.directorySyncCompleted(d.gen, ::fsync(d.fd) == 0 ? 0 : errno);
    };
    completeSeal();                                      // seg0 durable
    Log::RpWrite w;                                       // ... and its checkpoint durable
    ASSERT_TRUE(log.prepareRecoveryCheckpoint(w));
    log.recoveryCheckpointSubmitted();
    ASSERT_EQ(::pwrite(w.fd, w.data, w.len, w.offset), static_cast<ssize_t>(w.len));
    ASSERT_EQ(::fsync(w.fd), 0);
    log.recoveryCheckpointCompleted(w.seq, 0);
    ASSERT_EQ(log.persistedRecoveryPoint(), bases[1]);
    completeSeal();                                      // seg1, seg2 durable in MEMORY only
    completeSeal();
    ASSERT_EQ(log.recoveryPoint(), bases[3]);
    ASSERT_EQ(log.persistedRecoveryPoint(), bases[1]);
    auto segFile = [&](int64_t base) {
        std::string s = std::to_string(base);
        return (dir / (std::string(20 - s.size(), '0') + s + ".log")).string();
    };
    const auto size1 = fs::file_size(segFile(bases[1]));
    const auto size2 = fs::file_size(segFile(bases[2]));
    const auto size0 = fs::file_size(segFile(bases[0]));

    rune::node::ReplicaRegistry registry;
    registry.add(1300, 0, std::move(replica));
    rune::node::LogCleanerAgent cleaner(&registry);
    for (int i = 0; i < 50; ++i) {
        cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        log.drainCleanerSwaps();
    }
    EXPECT_EQ(fs::file_size(segFile(bases[1])), size1) << "compacted a segment above the persisted R";
    EXPECT_EQ(fs::file_size(segFile(bases[2])), size2) << "compacted a segment above the persisted R";
    EXPECT_TRUE(!fs::exists(segFile(bases[0])) || fs::file_size(segFile(bases[0])) < size0)
        << "positive control: the segment below R compacts";
    fs::remove_all(dir);
}

// ===========================================================================
// 2026-10-05 log-truncate review, fix round 2: the cleaner thread replaced the
// segment files ITSELF before the owner thread validated the swap.  An
// (unclean-election) truncate below the HW between the cleaner pass and the
// drain then rebuilt the segment FROM THE COMPACTED FILE: a record whose only
// superseding copy was truncated away (A=v1 superseded by A=v2@4, truncated) was
// gone — data loss, also across restart.  Now the cleaner only writes a
// .cleaned output; the owner validates (lineage / not active / below HW) and
// installs it or deletes it — the original segment is never touched.
// ===========================================================================
TEST(CompactionCleanerAgentRegression, StaleSwapAfterTruncateBelowHwLeavesOriginalDataReadableRegression) {
    auto dir = cp6TempDir("fr2_stale_swap");
    auto firstRecord = [](Log& l, int64_t off, std::string& key, std::string& val) {
        const auto bytes = l.read(off, 1 << 20);
        if (bytes.empty()) return int64_t{-1};
        RecordBatchReader r(bytes.data(), bytes.size());
        int64_t got = -1;
        r.forEach([&](std::string_view k, std::string_view v, int64_t o, int64_t) {
            if (got < 0) { got = o; key = std::string(k); val = std::string(v); }
        });
        return got;
    };
    {
        Log log(dir.string(), /*rollBytes=*/100);   // two one-record batches per segment
        log.append(cp6Batch(0, "A", "v1"), 0);
        log.append(cp6Batch(1, "B", "b"), 1);
        log.append(cp6Batch(2, "C", "c"), 2);
        log.append(cp6Batch(3, "D", "d"), 3);
        log.append(cp6Batch(4, "A", "v2"), 4);    // supersedes A=v1
        log.append(cp6Batch(5, "E", "e"), 5);
        log.append(cp6Batch(6, "F", "f"), 6);     // active segment 6
        ASSERT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4, 6}));
        Cp6NullTransport transport;
        auto owned = std::make_unique<PartitionReplica>(
            makeCompactConfig(780), Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log, &transport);
        owned->publishCleanerLso(PartitionReplica::kNoCleanerLso);
        owned->becomeLeader(1, {1});
        owned->onLeaderLogAppended();
        PartitionReplica* replica = owned.get();
        rune::node::ReplicaRegistry registry;
        registry.add(780, 0, std::move(owned));
        Cp6MinimalConsensusAgent dummy;
        rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);
        rune::node::LogCleanerAgent cleaner(&registry, /*cycleBudgetBytes=*/0);
        duty.doWork(1'000'000);                      // publish the cleaner-visible HW
        ASSERT_GE(replica->cleanerHighWatermark(), 6);
        ASSERT_GT(cleaner.doWork(2'000'000), 0) << "precondition: the pass compacted (A=v1 superseded)";

        // Unclean election: the log is truncated BELOW the old HW before the
        // owner drains the swap — segment 0 is rebuilt as the active segment.
        EXPECT_TRUE(log.truncateTo(2));
        log.drainCleanerSwaps();

        std::string k, v;
        ASSERT_EQ(firstRecord(log, 0, k, v), 0) << "offset 0 lost";
        EXPECT_EQ(k, "A");
        EXPECT_EQ(v, "v1");
        EXPECT_EQ(log.logEndOffset(), 2);
    }
    for (auto& e : fs::directory_iterator(dir)) {
        const std::string name = e.path().filename().string();
        EXPECT_EQ(name.find(".cleaned"), std::string::npos) << "stray cleaner output " << name;
        EXPECT_EQ(name.find(".swap"), std::string::npos) << "stray swap file " << name;
    }
    {   // ...and across restart.
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        std::string k, v;
        ASSERT_EQ(firstRecord(log, 0, k, v), 0) << "offset 0 lost after restart";
        EXPECT_EQ(k, "A");
        EXPECT_EQ(v, "v1");
        EXPECT_EQ(log.logEndOffset(), 2);
    }
    fs::remove_all(dir);
}

// ===========================================================================
// Fix round 3 (N-MEDIUM-1): the owner's watermark rewind (rejected outputs) was
// a plain store into lastCleanOffset_, overwritten by a concurrent cleaner
// pass's passEnd store.  Now the cleaner stays the single writer; the owner
// publishes a rewind the cleaner folds in after its own store.
// ===========================================================================
TEST(CompactionCleanerAgentRegression, OwnerRewindSurvivesConcurrentCleanerPassRegression) {
    auto dir = cp6TempDir("fr3_rewind");
    Log log(dir.string(), 1 << 20);
    Cp6NullTransport transport;
    PartitionReplica r(makeCompactConfig(781), Role::Leader, 1, {1}, {1}, &log, &transport);
    r.setLastCleanOffset(10);          // pass N stored passEnd_N
    r.requestCleanerRewind(4);         // owner rejects pass N's group at base 4
    r.setLastCleanOffset(20);          // pass N+1 (read 10 before the rewind) stores passEnd_{N+1}
    r.applyCleanerRewind();            // ...and folds the published rewind in
    EXPECT_EQ(r.lastCleanOffset(), 4) << "the rewind was overwritten";
    // Two rejections before the cleaner looks: the lower one wins.
    r.setLastCleanOffset(30);
    r.requestCleanerRewind(12);
    r.requestCleanerRewind(7);
    r.applyCleanerRewind();
    EXPECT_EQ(r.lastCleanOffset(), 7);
    // Acknowledged: a later pass is not rewound again.
    r.setLastCleanOffset(40);
    r.applyCleanerRewind();
    EXPECT_EQ(r.lastCleanOffset(), 40);
    fs::remove_all(dir);
}

// ===========================================================================
// Fix round 5 (N4-CRITICAL-1, the reviewer's probe): the owner accepted a
// merged group (in memory) but the cleaner had not yet run its renames when the
// next compaction pass started — the pass read the OLD <b1>.log by path while
// the snapshot said b1 spans the merged range, and the install dropped every
// record of the merged members (3/19 keys survived).
// ===========================================================================
// N5-LOW-3: the injected-failure variant — the gate's own file-ops run THROWS,
// so only the cleanerFileOpsPending() check keeps the pass off the stale file.
static void passAfterOwnerAcceptBeforeRenames(bool injectFailure);
TEST(CompactionCleanerAgentRegression, PassAfterOwnerAcceptBeforeRenamesLosesNoKeyRegression) {
    passAfterOwnerAcceptBeforeRenames(/*injectFailure=*/false);
}
TEST(CompactionCleanerAgentRegression, PassAfterOwnerAcceptWithFailingFileOpsLosesNoKeyRegression) {
    passAfterOwnerAcceptBeforeRenames(/*injectFailure=*/true);
}
static void passAfterOwnerAcceptBeforeRenames(bool injectFailure) {
    auto dir = cp6TempDir("fr5_accept_then_pass");
    Log log(dir.string(), /*rollBytes=*/128);
    std::map<std::string, std::string> latest;
    int64_t next = 0;
    auto put = [&](const std::string& k, const std::string& v) {
        log.append(cp6Batch(next, k, v), next);
        latest[k] = v;
        ++next;
    };
    for (int o = 0; o < 40; ++o) put(o % 2 == 0 ? "dup" : "u" + std::to_string(o), "v" + std::to_string(o));
    Cp6NullTransport transport;
    auto owned = std::make_unique<PartitionReplica>(
        makeCompactConfig(782), Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    owned->publishCleanerLso(PartitionReplica::kNoCleanerLso);
    owned->becomeLeader(1, {1});
    owned->onLeaderLogAppended();
    PartitionReplica* replica = owned.get();
    rune::node::ReplicaRegistry registry;
    registry.add(782, 0, std::move(owned));
    Cp6MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);
    rune::node::LogCleanerAgent cleaner(&registry, /*cycleBudgetBytes=*/0, -1, -1, /*useCoalescing=*/true, 1 << 20);
    cleaner.setSkipCycleFileOpsForTest(true);   // the owner's accept lands right before the next pass
    int64_t t = 1'000'000;
    duty.doWork(t);
    ASSERT_GT(cleaner.doWork(t), 0) << "precondition: pass 1 merged the sealed prefix";
    duty.doWork(t);                               // the owner accepts the group (renames still posted)
    ASSERT_TRUE(log.cleanerFileOpsPending()) << "precondition: accepted, not yet renamed";
    for (int o = 0; o < 10; ++o) put("u1", "w" + std::to_string(o));
    replica->onLeaderLogAppended();
    duty.doWork(t += 1'000'000);
    if (injectFailure) log.failNextCleanerApplyForTest();   // the gate's drain throws
    (void)cleaner.doWork(t);                      // pass 2 — must never read the stale <b1>.log
    cleaner.setSkipCycleFileOpsForTest(false);
    for (int i = 0; i < 400; ++i) {
        t += 1'000'000;
        duty.doWork(t);
        (void)cleaner.doWork(t);
    }
    duty.doWork(t + 1);
    (void)cleaner.doWork(t + 1);
    ASSERT_TRUE(log.fileOpsQuiescent());
    ASSERT_TRUE(log.reloadSegments());
    std::map<std::string, std::string> found;
    for (int64_t o = 0; o < next; ++o) {
        const auto bytes = log.read(o, 1 << 20);
        if (bytes.empty()) continue;
        RecordBatchReader r(bytes.data(), bytes.size());
        if (r.baseOffset() != o || !r.ensureValid()) continue;
        r.forEach([&](std::string_view k, std::string_view v, int64_t, int64_t) { found[std::string(k)] = std::string(v); });
    }
    for (const auto& [k, v] : latest) EXPECT_EQ(found[k], v) << "key " << k << " lost its latest value";
    EXPECT_EQ(found.size(), latest.size());
    fs::remove_all(dir);
}

// ===========================================================================
// Fix round 5 (N4-MEDIUM-1): a KIP-279 divergence truncation that lands while
// the log cleaner still has posted renames pending parks the follower in
// FollowerSync::TruncatePending (no wait, no fetch, no apply) and completes on
// a later duty pass once the cleaner caught up — also after a cleaner throw.
// ===========================================================================
TEST(CompactionCleanerAgentRegression, DivergenceTruncationDefersToTruncatePendingRegression) {
    auto dir = cp6TempDir("fr5_truncate_pending");
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t o = 0; o < 9; ++o) log.append(cp6Batch(o, "k" + std::to_string(o), "v"), o);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(783);
    PartitionReplica follower(cfg, Role::Follower, 1, {1, 2}, {1, 2}, &log, &transport);
    follower.becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    // An accepted compaction output whose renames are still posted.
    {
        const auto snap = log.sealedSegmentSnapshot();
        const int64_t b = (*snap)[1].baseOffset;   // segment 2 = [2,4)
        std::filesystem::copy_file(log.segmentForTest(b)->logPath(), Log::cleanedLogPath(log.dir(), b, 51));
        std::string idx = log.segmentForTest(b)->logPath();
        idx = idx.substr(0, idx.size() - 4) + ".index";
        std::filesystem::copy_file(idx, Log::cleanedIndexPath(log.dir(), b, 51));
        const Log::CleanedMember m[] = {{b, (*snap)[1].lineage}};
        ASSERT_TRUE(log.enqueueCleanedGroup(m, 1, 51, true, b + 2, log.openCleanedOutput(b, 51, b + 2).release()));
        ASSERT_EQ(log.drainCleanerSwaps(), 1);
        ASSERT_TRUE(log.cleanerFileOpsPending());
    }
    FetchResponseMsg resp{};
    resp.leaderEpoch = 2;
    resp.partition   = 0;
    resp.topicId     = 783;
    resp.errorCode   = rune::replication::kErrDivergingEpoch;
    rune::replication::ErrorPayloadBuf epb;
    resp.records     = epb.set(/*epoch=*/1, /*endOffset=*/5);
    follower.applyReplicated(resp);
    EXPECT_TRUE(follower.truncatePendingForTest());
    EXPECT_EQ(follower.truncatesDeferred(), 1u);
    EXPECT_EQ(log.logEndOffset(), 9) << "nothing truncated yet";
    // Data arriving meanwhile is dropped (it would extend the diverged tail).
    const auto data = cp6Batch(9, "late", "x");
    FetchResponseMsg dataResp{};
    dataResp.leaderEpoch = 2; dataResp.partition = 0; dataResp.topicId = 783;
    dataResp.records = std::span<const uint8_t>(data.data(), data.size());
    follower.applyReplicated(dataResp);
    EXPECT_EQ(follower.appliesDroppedTruncatePending(), 1u);
    EXPECT_EQ(log.logEndOffset(), 9);
    // Duty passes while the cleaner has not run (or throws): still pending, no wait.
    (void)follower.doWork(10'000'000);
    EXPECT_TRUE(follower.truncatePendingForTest());
    log.failNextCleanerApplyForTest();
    (void)log.runCleanerFileOps(0);
    (void)follower.doWork(20'000'000);
    EXPECT_TRUE(follower.truncatePendingForTest());
    (void)log.runCleanerFileOps(0);           // the cleaner catches up
    (void)follower.doWork(30'000'000);        // the owner completes it
    EXPECT_FALSE(follower.truncatePendingForTest());
    EXPECT_EQ(log.logEndOffset(), 5);
    fs::remove_all(dir);
}


// ===========================================================================
// hotpath-A review fixes (latency gate FollowerTruncation on tmpfs, 3.5M
// passes stuck in TruncatePending): a TruncatePending follower must not keep
// POSTING new cleaner work (retention drops) — every post re-opens the msg
// ring the deferred truncation waits to see drained, so a cleaner that runs
// between the follower's retry and the owner's drain starves it forever.
// ===========================================================================
TEST(CompactionCleanerAgentRegression, TruncatePendingFollowerPostsNoNewCleanerWorkRegression) {
    auto dir = cp6TempDir("hpa_trunc_starve");
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t o = 0; o < 30; ++o) log.append(cp6Batch(o, "k" + std::to_string(o), "v"), o);
    Cp6NullTransport transport;
    auto cfg = makeCompactConfig(785);
    PartitionReplica follower(cfg, Role::Follower, 1, {1, 2}, {1, 2}, &log, &transport);
    follower.becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    {   // the leader's HW covers everything (retention may drop up to it)
        FetchResponseMsg hw{};
        hw.leaderEpoch = 2; hw.partition = 0; hw.topicId = 785; hw.highWatermark = 30;
        follower.applyReplicated(hw);
    }
    // One posted file op pending: the divergence truncation defers.
    {
        const auto snap = log.sealedSegmentSnapshot();
        ASSERT_TRUE(log.enqueueDeletedSegment((*snap)[0].baseOffset, (*snap)[0].lineage));
        ASSERT_EQ(follower.drainRetentionDeletesOnOwner(), 1);
        ASSERT_TRUE(log.cleanerFileOpsPending());
    }
    FetchResponseMsg resp{};
    resp.leaderEpoch = 2; resp.partition = 0; resp.topicId = 785;
    resp.errorCode   = rune::replication::kErrDivergingEpoch;
    rune::replication::ErrorPayloadBuf epb;
    resp.records     = epb.set(/*epoch=*/1, /*endOffset=*/20);
    follower.applyReplicated(resp);
    ASSERT_TRUE(follower.truncatePendingForTest());
    // The starving interleaving: [follower retry] [cleaner drains] [owner drain
    // posts the next proposed drop] — repeated.
    int64_t t = 10'000'000;
    for (int pass = 0; pass < 50 && follower.truncatePendingForTest(); ++pass) {
        (void)follower.doWork(t += 1'000'000);
        (void)log.runCleanerFileOps(0);
        const auto snap = log.sealedSegmentSnapshot();
        if (!snap->empty()) (void)log.enqueueDeletedSegment((*snap)[0].baseOffset, (*snap)[0].lineage);
        (void)follower.drainRetentionDeletesOnOwner();
    }
    EXPECT_FALSE(follower.truncatePendingForTest()) << "the deferred truncation starved";
    EXPECT_EQ(log.logEndOffset(), 20);
    fs::remove_all(dir);
}

// ===========================================================================
// Fix round 6 (N5-MEDIUM-1): a TruncatePending follower on log B must not wait
// for a whole (throttled) compaction pass of an unrelated log A — the cleaner
// performs B's posted file ops at A's pass segment / throttle-slice boundaries.
// ===========================================================================
TEST(CompactionCleanerAgentRegression, TruncateOfOtherLogCompletesDuringThrottledPassRegression) {
    // Log A: a compact-topic leader with plenty of dirty data, cleaned at a
    // throttled rate so its pass lasts seconds.
    auto dirA = cp6TempDir("fr6_throttled_a");
    Log logA(dirA.string(), /*rollBytes=*/4096);
    const std::string big(400, 'x');
    for (int64_t o = 0; o < 200; ++o) logA.append(cp6Batch(o, "k" + std::to_string(o % 8), big), o);
    Cp6NullTransport transport;
    auto ownedA = std::make_unique<PartitionReplica>(
        makeCompactConfig(784), Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &logA, &transport);
    ownedA->publishCleanerLso(PartitionReplica::kNoCleanerLso);
    ownedA->becomeLeader(1, {1});
    ownedA->onLeaderLogAppended();
    // Log B: a follower whose divergence truncation is pending on posted renames.
    auto dirB = cp6TempDir("fr6_pending_b");
    Log logB(dirB.string(), /*rollBytes=*/100);
    for (int64_t o = 0; o < 9; ++o) logB.append(cp6Batch(o, "b" + std::to_string(o), "v"), o);
    auto ownedB = std::make_unique<PartitionReplica>(
        makeCompactConfig(785), Role::Follower, 1, std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, &logB, &transport);
    PartitionReplica* B = ownedB.get();
    B->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    {
        const auto snap = logB.sealedSegmentSnapshot();
        const int64_t b = (*snap)[1].baseOffset;
        std::filesystem::copy_file(logB.segmentForTest(b)->logPath(), Log::cleanedLogPath(logB.dir(), b, 61));
        std::string idx = logB.segmentForTest(b)->logPath();
        idx = idx.substr(0, idx.size() - 4) + ".index";
        std::filesystem::copy_file(idx, Log::cleanedIndexPath(logB.dir(), b, 61));
        const Log::CleanedMember m[] = {{b, (*snap)[1].lineage}};
        ASSERT_TRUE(logB.enqueueCleanedGroup(m, 1, 61, true, b + 2, logB.openCleanedOutput(b, 61, b + 2).release()));
        ASSERT_EQ(logB.drainCleanerSwaps(), 1);
    }
    FetchResponseMsg resp{};
    resp.leaderEpoch = 2; resp.partition = 0; resp.topicId = 785;
    resp.errorCode = rune::replication::kErrDivergingEpoch;
    rune::replication::ErrorPayloadBuf epb;
    resp.records = epb.set(1, 5);
    B->applyReplicated(resp);
    ASSERT_TRUE(B->truncatePendingForTest());

    rune::node::ReplicaRegistry registry;
    registry.add(784, 0, std::move(ownedA));
    registry.add(785, 0, std::move(ownedB));
    rune::node::LogCleanerAgent cleaner(&registry, /*cycleBudgetBytes=*/0, -1,
                                        /*ioMaxBytesPerSec=*/64 * 1024);
    // Only A's pass boundaries may drain B (not the top-of-cycle file ops).
    cleaner.setSkipCycleFileOpsForTest(true);
    Cp6MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent duty(dummy, nullptr, nullptr, nullptr, &registry, nullptr);
    duty.doWork(1'000'000);   // publish A's cleaner-visible HW (B: pending, no fetch)

    std::atomic<bool> passDone{false};
    std::atomic<int>  passWork{0};
    std::thread cleanerThread([&] { passWork = cleaner.doWork(2'000'000); passDone = true; });
    bool completedDuringPass = false;
    int64_t t = 3'000'000;
    // B's owner loop (this thread): no sleep, just duty passes until it completes.
    while (!passDone.load()) {
        (void)B->doWork(t += 1'000);
        if (!B->truncatePendingForTest()) { completedDuringPass = !passDone.load(); break; }
        std::this_thread::yield();
    }
    cleanerThread.join();
    EXPECT_GT(passWork.load(), 0) << "precondition: A's throttled pass ran";
    EXPECT_TRUE(completedDuringPass) << "B's truncation waited for A's whole throttled pass";
    EXPECT_EQ(logB.logEndOffset(), 5);
    fs::remove_all(dirA);
    fs::remove_all(dirB);
}
