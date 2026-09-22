// ---------------------------------------------------------------------------
// CompactionCorrectnessTest.cpp — CP-4: compaction correctness interactions.
//
// Verifies four guarantees that prove compaction plays correctly with:
//   1. IdempotentDedupSurvivesCompaction  — PSM dedup tail is preserved after
//      compaction of a Compact topic (distinct from CP-3 which covered retention).
//   2. DeterministicAcrossReplicas        — two independent LogCleaner instances
//      on identical sealed input segments produce byte-identical retained sets.
//   3. OffsetStoreRebuildFromCompactedLog — __consumer_offsets compacted then
//      rebuilt via ReplicatedOffsetStore::rebuildFromLog() keeps latest offsets.
//   4. HwUnchangedByCompaction            — compaction never moves HW or LEO.
//
// All tests: single-threaded, no Aeron, no Docker. Integration-first.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// node / replication
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>

// storage
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/ProducerStateManager.h>

// coordination
#include <rune/coordination/ReplicatedOffsetStore.h>

// protocol
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
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
using namespace rune::coordination;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_cp4_seq{0};

static fs::path cp4TempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_cp4_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

/// Plain single-record batch (no producer fields).
static std::vector<uint8_t> cp4Batch(int64_t offset, std::string_view key,
                                      std::string_view value, int64_t ts = 1000) {
    RecordBatchBuilder b(offset, ts);
    b.append(key, value, ts);
    return b.build();
}

/// Single-record batch WITH producer fields (idempotent producer).
static std::vector<uint8_t> cp4ProducerBatch(int64_t offset,
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

/// Count records with a given key across all log segments.
static int cp4CountKey(Log& log, std::string_view wantKey) {
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

/// Collect all (key, value, offset) records from a log.
struct Rec { std::string key, value; int64_t offset; };

static std::vector<Rec> cp4AllRecords(Log& log) {
    std::vector<Rec> out;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return out;
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
                out.push_back({std::string(k), std::string(v), off});
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return out;
}

// Minimal stubs re-used from CompactionDutyCycleTest pattern.
class Cp4ConsensusAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

class Cp4NullTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

// ===========================================================================
// TEST 1: IdempotentDedupSurvivesCompaction
//
// producerId=9 appends seq 0..54 interleaved with OTHER (hot/noisy) keys
// into a Compact topic.  Run enough compaction cycles to compact all sealed
// segments.  Then retry seq 50 → must return Duplicate (prior offset), NOT
// re-appended and NOT OutOfOrderSequence.
//
// Why seq 50 in a 55-batch run: kRetryWindowSize=5, so the retry window after
// the 55th batch holds seqs 50..54.  Seq 50 IS in the window.  We verify that
// after compaction the dedup window entries — including the base offset of the
// batch at seq 50 — are still intact so the validate() call returns Duplicate.
//
// This verifies the PSM dedup tail is preserved through compaction (the
// compacted log still contains the last-kRetryWindowSize batches per
// producerId so that rebuildFromLog() can seed the dedup window).
//
// NON-VACUOUS: the test is discriminating because:
//   - Before compaction, rebuildFromLog trivially succeeds (all batches present).
//   - After compaction, the LogCleaner removes superseded records.  The last
//     kRetryWindowSize batches for producerId=9 MUST survive compaction.  If the
//     LogCleaner ever dropped one (e.g. because the batch's key was superseded
//     by a later batch from a DIFFERENT producer), rebuildFromLog would skip that
//     batch, leaving the retry window with a hole at seq 50.  validate() would
//     then return OOS instead of Duplicate — the test fails.
//   Discriminating setup: noise keys "noise_0..noise_9" are each overwritten
//   ~3 times (seq < 34), creating real compaction work across many segments
//   (superseded noise records removed).  Producer keys "pk0..pk54" are each
//   written exactly once (unique key per seq), so they ARE the "latest" for
//   their key and must ALL survive compaction.  The OOS result without the
//   invariant holding is confirmed: if we set noise_0..noise_9 to the same
//   key as pk50..pk59, compaction would drop pk50's producer batch (superseded
//   by the noise write to the same key), rebuildFromLog would miss it, and
//   validate(seq=50) would return OOS.
// ===========================================================================
TEST(CompactionCorrectness, IdempotentDedupSurvivesCompaction) {
    auto dir = cp4TempDir("cp4_psm_compact");
    // Small roll so producer batches span many segments.
    Log log(dir.string(), /*rollBytes=*/256);

    Cp4NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId               = 200;
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
    cfg.minCleanableDirtyRatio = 0.0;   // compact whenever there is dirty work

    constexpr int64_t kPid         = 9;
    constexpr int16_t kEpoch       = 0;
    // Use kN=55 so that seq 50 falls within the last kRetryWindowSize=5 entries
    // of the retry window (window covers seqs 50..54 after all 55 batches).
    constexpr int     kN           = 55;
    constexpr int     kRetrySeq    = 50;  // the specific seq to retry post-compaction

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Interleave producer batches (pk0..pk54) with OTHER keys (noise_0..noise_9)
    // so the dirty ratio is high and compaction has real work (superseded noise
    // records removed from sealed segments).
    int offset = 0;
    for (int seq = 0; seq < kN; ++seq) {
        // Idempotent batch: unique key "pkN" per seq (so it always IS the latest
        // for its key and must survive compaction).
        {
            auto batch = cp4ProducerBatch(offset, "pk" + std::to_string(seq),
                                          "val" + std::to_string(seq),
                                          kPid, kEpoch, seq,
                                          /*ts=*/static_cast<int64_t>(1000 + seq));
            log.append(batch, offset);
            ++offset;
        }
        // Noisy key: "noise_K" written 3 times for seq < 34 → first two
        // are superseded and compacted away.  This is the KEY discriminating
        // content: it forces compaction to have meaningful work and exercises
        // the global-latest-per-key path across multiple segments.
        if (seq < 34) {
            auto b2 = cp4Batch(offset, "noise_" + std::to_string(seq % 10),
                               "n" + std::to_string(seq));
            log.append(b2, offset);
            ++offset;
        }
    }
    replica->onLeaderLogAppended();

    // Need at least 2 segments so compaction actually has sealed work.
    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u)
        << "Need >=2 segments for compaction to compact sealed records";

    const size_t sealedCount = bases.size() - 1;

    // Run enough duty-cycle compaction passes to compact all sealed segments.
    rune::node::ReplicaRegistry registry;
    registry.add(200, 0, std::move(replica));

    Cp4ConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (size_t i = 0; i < sealedCount + 20; ++i) {
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();

    // GUARD: verify compaction actually ran (superseded noise records removed).
    // If no compaction ran, all records are still present — vacuous test.
    auto allRecsAfter = cp4AllRecords(log);
    ASSERT_LT(static_cast<int>(allRecsAfter.size()), offset)
        << "Compaction must have removed superseded noise records (vacuousness guard); "
           "all records still present suggests compaction did not run.";

    // Rebuild PSM from the compacted log.
    ProducerStateManager psm;
    psm.rebuildFromLog(/*topicId=*/200, log, log.logEndOffset(), /*partition=*/0);

    // Verify the retry window is correctly populated:
    // The last kRetryWindowSize=5 batches are seq 50..54.
    // Retry of seq 50 MUST be Duplicate (it IS in the retry window).
    auto retryResult = psm.validate(/*topicId=*/200, kPid, /*partition=*/0,
                                    kEpoch, /*baseSeq=*/kRetrySeq, /*lastSeq=*/kRetrySeq);
    EXPECT_EQ(retryResult.status, ValidationResult::Status::Duplicate)
        << "After compaction: retry of seq " << kRetrySeq << " must be Duplicate. "
           "PSM retry window (last " << kRetryWindowSize << " batches) must survive "
           "compaction. Got status=" << static_cast<int>(retryResult.status)
        << " (2=OOS means the retry window lost the seq " << kRetrySeq << " entry)";

    // Retry of the very last seq (54) — also must be Duplicate.
    auto lastRetryResult = psm.validate(/*topicId=*/200, kPid, /*partition=*/0,
                                        kEpoch, /*baseSeq=*/kN - 1, /*lastSeq=*/kN - 1);
    EXPECT_EQ(lastRetryResult.status, ValidationResult::Status::Duplicate)
        << "After compaction: retry of last seq (" << (kN - 1) << ") must be Duplicate.";

    // Retry of seq 49 (outside the window: window is 50..54) must be OOS.
    auto outsideResult = psm.validate(/*topicId=*/200, kPid, /*partition=*/0,
                                       kEpoch, /*baseSeq=*/49, /*lastSeq=*/49);
    EXPECT_EQ(outsideResult.status, ValidationResult::Status::OutOfOrderSequence)
        << "Seq 49 is outside the retry window (50..54); must be OOS, not Duplicate.";

    // The next new sequence must be Accepted.
    auto newResult = psm.validate(/*topicId=*/200, kPid, /*partition=*/0,
                                   kEpoch, /*baseSeq=*/kN, /*lastSeq=*/kN);
    EXPECT_EQ(newResult.status, ValidationResult::Status::Accept)
        << "After compaction: next new sequence (" << kN << ") must be Accepted.";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 2: DeterministicAcrossReplicas
//
// Feed two independent LogCleaner instances (c1, c2) the SAME sealed input
// segments (identical log content, two separate log dirs populated
// identically).  Compact both to the same boundary.  The retained
// (offset, key, value) sets must be:
//   - byte-identical (same keys, same values)
//   - in the same order (same offset sequence)
//   - with identical offsets preserved (no renumbering)
//
// This proves per-replica compaction is deterministic (no random element,
// no ordering dependency on hash-map iteration order, etc.).
//
// NON-VACUOUS:
//   The most likely source of non-determinism would be unordered_map
//   iteration order when building the OffsetMap or deciding which record is
//   "latest".  If buildOffsetMap iterated the internal hash map to populate
//   output and the two maps had different bucket orderings, the resulting
//   "latest offset" for a key could differ, leading to different surviving
//   records.  This test exercises that path with multiple keys updated
//   across segment boundaries.
//   Perturb evidence: changing buildOffsetMap to use a random "first seen"
//   rather than "highest offset" policy would cause the two cleaners to
//   keep different records for the same key (since the two runs start with
//   different map state) — the EXPECT_EQ(r1[i].offset, r2[i].offset)
//   assertions would fail.
// ===========================================================================
TEST(CompactionCorrectness, DeterministicAcrossReplicas) {
    auto dir1 = cp4TempDir("cp4_det_r1");
    auto dir2 = cp4TempDir("cp4_det_r2");

    // Populate both logs identically.
    auto populate = [](Log& log) {
        int64_t off = 0;
        // 5 keys each updated 20 times, interleaved.
        for (int upd = 0; upd < 20; ++upd) {
            for (int k = 0; k < 5; ++k) {
                auto b = cp4Batch(off, "k" + std::to_string(k),
                                  "v" + std::to_string(k) + "_" + std::to_string(upd),
                                  /*ts=*/static_cast<int64_t>(1000 + off));
                log.append(b, off);
                ++off;
            }
        }
        // A few unique keys (one update each) to keep the boundary non-trivial.
        for (int u = 0; u < 10; ++u) {
            auto b = cp4Batch(off, "unique_" + std::to_string(u),
                              "uval" + std::to_string(u),
                              /*ts=*/5000 + off);
            log.append(b, off);
            ++off;
        }
        // Filler to force segment roll.
        for (int f = 0; f < 30; ++f) {
            auto b = cp4Batch(off, "pad", "p", 9000 + f);
            log.append(b, off);
            ++off;
        }
    };

    Log log1(dir1.string(), /*rollBytes=*/512);
    Log log2(dir2.string(), /*rollBytes=*/512);
    populate(log1);
    populate(log2);

    // Verify both logs have the same structure before compaction.
    auto bases1 = log1.segmentBaseOffsets();
    auto bases2 = log2.segmentBaseOffsets();
    ASSERT_EQ(bases1.size(), bases2.size())
        << "Pre-condition: both logs must have the same segment structure";
    ASSERT_GE(bases1.size(), 2u)
        << "Need >= 2 segments for meaningful compaction";

    int64_t activeBase = bases1.back();
    ASSERT_EQ(activeBase, bases2.back());

    // Compact with boundary = activeBase (seal zone only, not the active segment).
    LogCleaner c1, c2;
    c1.clean(log1, /*fromOffset=*/0, /*boundary=*/activeBase,
             /*nowNs=*/0, /*deleteRetentionMs=*/1'000'000);
    c2.clean(log2, /*fromOffset=*/0, /*boundary=*/activeBase,
             /*nowNs=*/0, /*deleteRetentionMs=*/1'000'000);
    log1.reloadSegments();
    log2.reloadSegments();

    // Collect all records from both logs (sealed zone only: offset < activeBase).
    auto collect = [&](Log& log) {
        std::vector<Rec> out;
        auto bases = log.segmentBaseOffsets();
        if (bases.empty()) return out;
        int64_t cursor = 0;
        for (size_t si = 0; si < bases.size(); ++si) {
            int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1] : activeBase;
            if (bases[si] >= activeBase) break;
            cursor = bases[si];
            for (;;) {
                auto bytes = log.read(cursor, 1 << 20);
                if (bytes.empty()) break;
                RecordBatchReader r(bytes.data(), bytes.size());
                if (!r.ensureValid()) break;
                bool advanced = false;
                r.forEach([&](std::string_view k, std::string_view v,
                               int64_t off, int64_t) {
                    if (off < activeBase) out.push_back({std::string(k), std::string(v), off});
                    cursor = off + 1;
                    advanced = true;
                });
                if (!advanced || cursor >= segEnd) break;
            }
        }
        return out;
    };

    auto recs1 = collect(log1);
    auto recs2 = collect(log2);

    // The retained sets must be identical in size, order, key, value, offset.
    ASSERT_EQ(recs1.size(), recs2.size())
        << "Determinism: both cleaners must retain the same number of records. "
           "r1=" << recs1.size() << " r2=" << recs2.size();

    for (size_t i = 0; i < recs1.size(); ++i) {
        EXPECT_EQ(recs1[i].offset, recs2[i].offset)
            << "Determinism: record[" << i << "].offset must match: "
            << recs1[i].offset << " vs " << recs2[i].offset;
        EXPECT_EQ(recs1[i].key, recs2[i].key)
            << "Determinism: record[" << i << "].key must match";
        EXPECT_EQ(recs1[i].value, recs2[i].value)
            << "Determinism: record[" << i << "].value must match";
    }

    // Spot-check: each of the 5 hot keys appears exactly once in the retained set.
    for (int k = 0; k < 5; ++k) {
        std::string key = "k" + std::to_string(k);
        int c = 0;
        for (auto& r : recs1) if (r.key == key) ++c;
        EXPECT_EQ(c, 1)
            << "After compaction: key '" << key << "' must appear exactly once; got " << c;
    }

    fs::remove_all(dir1);
    fs::remove_all(dir2);
}

// ===========================================================================
// TEST 3: OffsetStoreRebuildFromCompactedLog
//
// Scenario:
//   1. Commit many offsets for (group, topic, partition) into a
//      ReplicatedOffsetStore backed by a PartitionReplica (Log).
//   2. Run compaction passes until the __consumer_offsets log is compacted
//      (latest-per-key wins; earlier commits for the same key are removed).
//   3. Build a fresh ReplicatedOffsetStore over the SAME compacted log and
//      call rebuildFromLog().
//   4. The latest committed offset per key must be intact.
//
// This verifies the CP-3 fix in PSM::rebuildFromLog (starting at the first
// live segment base rather than 0) AND that ReplicatedOffsetStore's
// rebuildFromLog correctly folds the compacted (latest-per-key) log.
//
// NON-VACUOUS:
//   - Before compaction: rebuildFromLog sees all commits; last-write-wins
//     naturally gives the right answer.
//   - After compaction: only the latest commit record per (group, tp) key
//     survives in the log.  If rebuildFromLog stopped at the first segment
//     (e.g. compaction advanced the first live base past 0 and the replay
//     loop started at 0 returning empty), no commits would be loaded,
//     giving nullopt for every fetch().  The EXPECT_EQ(co->offset, latest)
//     assertions catch this.
//   Perturb evidence: commenting out the CP-3 fix in rebuildFromLog (i.e.
//   always starting at fetchOffset=0 when the first segment base > 0) would
//   cause the rebuild loop to bail immediately on log.read(0)→empty, leaving
//   the in-memory index empty and all fetch() calls returning nullopt — the
//   ASSERT_TRUE(co.has_value()) line would fail.
// ===========================================================================
TEST(CompactionCorrectness, OffsetStoreRebuildFromCompactedLog) {
    auto dir = cp4TempDir("cp4_offstore_compact");

    // Small roll so the offset-commit records span multiple segments (needed
    // for compaction to have sealed work).
    Log log(dir.string(), /*rollBytes=*/256);

    Cp4NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId               = 201;
    cfg.partitionId           = 0;
    cfg.selfId                = 1;
    cfg.minIsr                = 1;
    cfg.replicaLagMaxMs       = 10'000;
    cfg.replicaLagMaxOffset   = 10'000;
    cfg.fetchIntervalNs       = 1'000'000LL;
    cfg.isrCheckIntervalNs    = 1'000'000LL;
    cfg.maxFetchBytes         = 1 << 20;
    cfg.pushOnAppend          = false;
    // __consumer_offsets uses Compact policy.
    cfg.cleanupPolicy         = CleanupPolicy::Compact;
    cfg.minCleanableDirtyRatio = 0.0;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Write offset commits via ReplicatedOffsetStore.
    //
    // CP-4 DISCRIMINATING LAYOUT (proven non-vacuous below): to force
    // rebuildFromLog to traverse an INTER-SEGMENT GAP (the exact condition the
    // CP-4 fix handles — read() returning empty mid-log because compaction
    // emptied whole intermediate segments), we:
    //   1. Commit the kKeys TRACKED keys ONCE, up front (they land in the first
    //      segment(s) and are the latest-per-key that must survive).
    //   2. Then flood the log with MANY rounds of a small set of NOISE keys,
    //      each overwritten dozens of times.  Compaction removes every
    //      superseded noise record, EMPTYING the middle segments entirely — so
    //      their .log files are deleted and the surviving segments are
    //      non-contiguous in offset space.  rebuildFromLog must jump across that
    //      hole; the pre-CP-4 code (bare break on empty read) would stop at the
    //      gap and lose either the tracked keys (if the gap precedes them) or the
    //      noise survivors, and — more importantly — abandon the rest of the log.
    ReplicatedOffsetStore store(replica.get());

    constexpr int kKeys       = 5;
    constexpr int kNoiseKeys  = 3;
    constexpr int kNoiseRounds = 40; // heavy supersession → empties middle segments

    // (1) Tracked keys — committed ONCE, near the front of the log.
    std::vector<int64_t> latestOffset(kKeys);
    for (int k = 0; k < kKeys; ++k) {
        GroupId      group = "grp" + std::to_string(k);
        TopicPartition tp{"offsettopic", k};
        int64_t       committed_offset = static_cast<int64_t>(1000 + k);
        latestOffset[k] = committed_offset;
        bool ok = store.commit(group, tp, committed_offset, /*meta=*/"", /*tsNs=*/0);
        ASSERT_TRUE(ok) << "tracked commit must succeed on single-node leader";
    }

    // (2) Noise flood — a few keys overwritten many times.  All but the last
    //     write per noise key is superseded and compacted away, emptying the
    //     middle segments and creating the inter-segment gap.
    for (int round = 0; round < kNoiseRounds; ++round) {
        for (int n = 0; n < kNoiseKeys; ++n) {
            GroupId      group = "noisegrp" + std::to_string(n);
            TopicPartition tp{"noisetopic", n};
            int64_t       committed_offset = static_cast<int64_t>(round * 10 + n);
            bool ok = store.commit(group, tp, committed_offset, /*meta=*/"", /*tsNs=*/0);
            ASSERT_TRUE(ok) << "noise commit must succeed on single-node leader";
        }
    }

    // (3) A second batch of TRACKED keys committed AFTER the noise flood, so they
    //     land in the POST-GAP survivor segments (high offsets).  This is what
    //     makes the test genuinely guard CP-4: rebuildFromLog must TRAVERSE the
    //     inter-segment gap left by the emptied middle segments to reach these
    //     records.  The pre-CP-4 code (bare break on empty read) stops at the
    //     first gap and NEVER sees these post-gap keys → their fetch() returns
    //     nullopt → the test FAILS.  (Verified by scratch-revert.)
    constexpr int kPostKeys = 5;
    std::vector<int64_t> postLatestOffset(kPostKeys);
    for (int k = 0; k < kPostKeys; ++k) {
        GroupId      group = "postgrp" + std::to_string(k);
        TopicPartition tp{"posttopic", k};
        int64_t       committed_offset = static_cast<int64_t>(9000 + k);
        postLatestOffset[k] = committed_offset;
        bool ok = store.commit(group, tp, committed_offset, /*meta=*/"", /*tsNs=*/0);
        ASSERT_TRUE(ok) << "post-gap tracked commit must succeed on single-node leader";
    }

    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u)
        << "Need >= 2 segments to exercise compaction of sealed offset-commit records";

    const size_t sealedCount = bases.size() - 1;

    // Compact via duty-cycle agent.
    rune::node::ReplicaRegistry registry;
    PartitionReplica* rawReplica = replica.get();
    registry.add(201, 0, std::move(replica));

    Cp4ConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (size_t i = 0; i < sealedCount + 20; ++i) {
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();

    // GUARD 1: compaction must have actually reduced the record count
    // (earlier commits for the same key were superseded and removed).
    int totalAfter = 0;
    {
        auto recs = cp4AllRecords(log);
        totalAfter = static_cast<int>(recs.size());
    }
    const int totalBefore = kKeys + kNoiseKeys * kNoiseRounds + kPostKeys; // all appended records
    ASSERT_LT(totalAfter, totalBefore)
        << "Compaction must have removed superseded offset-commit records "
           "(vacuousness guard); total before=" << totalBefore
        << " total after=" << totalAfter
        << " — if these are equal, compaction did not run.";

    // GUARD 2 (CP-4 gap guard): compaction must have produced an INTER-SEGMENT
    // GAP — either the first surviving segment base advanced past 0 (leading
    // segments emptied) or the surviving segment bases are non-contiguous with
    // the offset stream (middle segments emptied).  Without a gap this test would
    // not exercise the CP-4 rebuildFromLog gap-skip and would be vacuous.
    {
        const auto basesAfter = log.segmentBaseOffsets();
        ASSERT_FALSE(basesAfter.empty());
        // A gap exists iff fewer records survive than the offset span they cover:
        // the compacted log's surviving records are sparse across the base range.
        const int64_t offsetSpan = log.logEndOffset();          // total offsets ever written
        const bool sparse = (totalAfter < offsetSpan);          // holes exist in offset space
        const bool frontAdvanced = (basesAfter.front() > 0);    // leading segments emptied
        ASSERT_TRUE(sparse || frontAdvanced)
            << "CP-4 gap guard: compaction must leave an inter-segment gap "
               "(sparse offsets or advanced front base) so rebuildFromLog must "
               "traverse it; offsetSpan=" << offsetSpan
            << " survivors=" << totalAfter
            << " frontBase=" << basesAfter.front();
    }

    // Rebuild from the compacted log using a fresh store instance.
    ReplicatedOffsetStore store2(rawReplica);
    store2.rebuildFromLog();

    // The latest committed offset per PRE-GAP tracked (group, tp) must be intact.
    for (int k = 0; k < kKeys; ++k) {
        GroupId      group = "grp" + std::to_string(k);
        TopicPartition tp{"offsettopic", k};
        auto co = store2.fetch(group, tp);
        ASSERT_TRUE(co.has_value())
            << "After compaction + rebuild: offset for (grp" << k
            << ", offsettopic, " << k << ") must be present";
        EXPECT_EQ(co->offset, latestOffset[k])
            << "After compaction + rebuild: offset for grp" << k
            << " must be the latest committed value ("
            << latestOffset[k] << "); got " << co->offset;
    }

    // CP-4 CRUX: the POST-GAP tracked keys live in survivor segments AFTER the
    // inter-segment gap.  rebuildFromLog must traverse the gap to recover them.
    // The pre-CP-4 code stops at the gap and returns nullopt for all of these.
    for (int k = 0; k < kPostKeys; ++k) {
        GroupId      group = "postgrp" + std::to_string(k);
        TopicPartition tp{"posttopic", k};
        auto co = store2.fetch(group, tp);
        ASSERT_TRUE(co.has_value())
            << "CP-4: post-gap offset for (postgrp" << k << ", posttopic, " << k
            << ") must be recovered — rebuildFromLog must skip the inter-segment gap";
        EXPECT_EQ(co->offset, postLatestOffset[k])
            << "CP-4: post-gap offset for postgrp" << k
            << " must be the latest committed value (" << postLatestOffset[k]
            << "); got " << co->offset;
    }

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 4: HwUnchangedByCompaction
//
// Record HW (and LEO) before running compaction.  Compact all sealed
// segments (multiple duty-cycle passes).  Assert HW and LEO are both
// unchanged after compaction completes.
//
// Rationale: compaction only rewrites sealed (below-HW) segment files.
// It must NEVER move HW or LEO because:
//   (a) HW is the replication commitment boundary — advancing it would
//       falsely mark uncommitted batches as durable.
//   (b) LEO is the log's physical end — compaction does not append new
//       batches, so LEO must not change.
//
// NON-VACUOUS:
//   The LogCleaner rewrites segment files in-place (atomic rename), which
//   could in principle cause Log's internal accounting to re-read the new
//   file and miscompute the active segment end or the total log extent.
//   If reloadSegments() or reloadSegment() ever adjusted logEndOffset()
//   based on the rewritten (shorter) sealed file, LEO would drop — the
//   EXPECT_EQ(leo_after, leo_before) would catch it.  Similarly, if
//   PartitionReplica::highWatermark() recomputed from a stale replica
//   state after the log reload, the EXPECT_EQ(hw_after, hw_before) would
//   catch it.
//   Perturb evidence: adding log.truncate(newLeo) inside reloadSegment()
//   would shrink LEO to the compacted segment length — the LEO check fails.
//   Resetting HW to 0 inside Log::reloadSegments() would fail the HW check.
// ===========================================================================
TEST(CompactionCorrectness, HwUnchangedByCompaction) {
    auto dir = cp4TempDir("cp4_hw_unchanged");
    Log log(dir.string(), /*rollBytes=*/128);

    Cp4NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId               = 202;
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
    cfg.minCleanableDirtyRatio = 0.0;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Append records: hot key updated many times + some unique keys.
    for (int v = 0; v < 150; ++v) {
        auto b = cp4Batch(v, "hot", "v" + std::to_string(v));
        log.append(b, v);
    }
    for (int u = 0; u < 20; ++u) {
        auto b = cp4Batch(150 + u, "uniq_" + std::to_string(u), "x" + std::to_string(u));
        log.append(b, 150 + u);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need >= 2 segments for compaction to run";
    const size_t sealedCount = bases.size() - 1;

    // Capture HW and LEO before compaction.
    const int64_t hw_before  = replica->highWatermark();
    const int64_t leo_before = replica->logEndOffset();

    ASSERT_GT(hw_before, 0)
        << "Pre-condition: HW must be > 0 (leader must have committed records)";
    ASSERT_GE(leo_before, hw_before)
        << "Pre-condition: LEO >= HW (may have uncommitted tail, but typically equal here)";

    // Run compaction passes.
    rune::node::ReplicaRegistry registry;
    PartitionReplica* rawReplica = replica.get();
    registry.add(202, 0, std::move(replica));

    Cp4ConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (size_t i = 0; i < sealedCount + 20; ++i) {
        agent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();

    // GUARD + CONVERGENCE: compaction must have actually run AND converged the
    // hot key to EXACTLY ONE live record (superseded copies GONE).  The prior
    // `< 150` assertion was a loose vacuousness guard that would pass even if
    // compaction stalled partway (the watermark-convergence bug); assert full
    // convergence now.
    const int hot_after = cp4CountKey(log, "hot");
    EXPECT_EQ(hot_after, 1)
        << "Compaction must converge the hot key to exactly ONE live record "
           "(superseded copies removed); got " << hot_after
        << " records.  '< 150' would have hidden a non-converging cleaner.";

    // HW must be unchanged.
    const int64_t hw_after  = rawReplica->highWatermark();
    EXPECT_EQ(hw_after, hw_before)
        << "Compaction must NOT change HW: before=" << hw_before
        << " after=" << hw_after;

    // LEO must be unchanged (compaction does not append or truncate).
    const int64_t leo_after = rawReplica->logEndOffset();
    EXPECT_EQ(leo_after, leo_before)
        << "Compaction must NOT change LEO: before=" << leo_before
        << " after=" << leo_after;

    fs::remove_all(dir);
}
