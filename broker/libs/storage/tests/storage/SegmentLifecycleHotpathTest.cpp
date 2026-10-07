// SegmentLifecycleHotpathTest.cpp — hotpath-A (latency-gate findings F1-F4):
// a segment roll never blocks the owner thread, and the sealed-segment
// snapshot is published in O(1) per roll / retention drop.
//
//   F1  the roll never fallocates the next segment on the owner (the
//       provisioner reserves the blocks of each READY segment).
//   F2  a provisioned log never creates a segment inline: with no ready
//       segment the roll is DEFERRED (rollDeferred(); the intake holds).
//   F3  a worker-driven (deferSealSync) log never fsyncs a seal inline: with
//       the async seal queue full the roll is deferred.
//   F4  the sealed snapshot is a chunked persistent structure: a roll appends
//       one entry, retention drops the front, truncation / reload rebuild only
//       the tail; published snapshots never change.
//   +   cold paths (truncateTo) never touch a segment by path while its
//       staging -> base rename is in flight (the dedicated provisioner renames
//       promptly — the old cleaner-driven rename rarely raced).
//
// Every test here is a regression test (name contains "Regression").
#include <gtest/gtest.h>
#include <rune/storage/testing/DemandChannelForTest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <new>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::storage;
using namespace rune::protocol;
namespace fs = std::filesystem;

// Allocation counter for THIS thread only (the owner in these single-threaded
// tests): replaces the global operator new for the whole test binary, counting
// only while a window is open.
namespace {
thread_local bool     tCount = false;
thread_local uint64_t tAllocs = 0;
}
void* operator new(std::size_t n) {
    if (tCount) ++tAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace {

fs::path freshDir(const char* stem) {
    static int seq = 0;
    auto d = fs::temp_directory_path() /
             (std::string(stem) + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" +
              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::remove_all(d);
    fs::create_directories(d);
    return d;
}

/// Append one single-record batch at `off`; returns the next offset.
int64_t put(Log& log, int64_t off, int64_t ts = 1000) {
    RecordBatchBuilder b(off, ts);
    b.append("k" + std::to_string(off % 7), "value-" + std::to_string(off), ts);
    const auto batch = b.build();
    return log.appendNoSync(batch.data(), batch.size(), off);
}

std::size_t countSuffix(const fs::path& dir, const std::string& suffix) {
    std::size_t n = 0;
    for (auto& e : fs::directory_iterator(dir)) {
        const auto s = e.path().filename().string();
        if (s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0) ++n;
    }
    return n;
}

/// Counts demands of each kind ([0] = all, [1] = Threshold, [2] = Adopted, [3] = Hold).
struct NoopHook {
    static bool fn(void* ctx, Log*, Log::ProvisionDemand k) noexcept {
        auto* c = static_cast<int*>(ctx);
        ++c[0];
        ++c[static_cast<int>(k)];
        return true;
    }
};

} // namespace

// ---------------------------------------------------------------------------
// Cold path vs in-flight staging rename.  The provisioner renamed the adopted
// segment's staging files to <base>.{log,index}; the owner has not yet drained
// the rename-done ring, so the segment's logPath_ is still the staging name.
// A follower truncation into that (active) segment used to re-open the file by
// the stale staging path → the stream failed → the next seal flush returned EIO
// and the log was permanently failed (seen in the latency gate's
// FollowerTruncation scenario once provisioning moved to its own thread).
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, TruncateAfterProvisionerRenameKeepsLogHealthyRegression) {
    const auto dir = freshDir("rune_slh_trunc_rename");
    {
        Log log(dir.string(), /*rollBytes*/ 512);
        log.recover();
        ASSERT_EQ(log.provisionReady(1), 1);   // this thread plays the provisioner
        int64_t off = 0;
        while (log.segmentBaseOffsets().size() < 2) off = put(log, off);
        const int64_t activeBase = log.segmentBaseOffsets().back();
        for (int i = 0; i < 3; ++i) off = put(log, off);
        ASSERT_EQ(log.finalizeStagingRenames(), 1);   // renamed on disk; done ring NOT drained
        const int64_t cut = activeBase + 1;
        // The cold path must either defer (caller retries) or run correctly —
        // never touch the segment through its stale staging path.
        bool done = false;
        for (int i = 0; i < 4 && !done; ++i) done = log.truncateTo(cut);
        ASSERT_TRUE(done) << "the rename already completed: the truncation must not stay deferred";
        EXPECT_FALSE(log.storageFailed()) << "errno=" << log.storageErrno();
        EXPECT_EQ(log.logEndOffset(), cut);
        // The log keeps working: more appends, a roll, a read back.
        off = cut;
        ASSERT_EQ(log.provisionReady(1), 1);
        for (int i = 0; i < 40; ++i) off = put(log, off);
        EXPECT_FALSE(log.storageFailed()) << "errno=" << log.storageErrno();
        auto bytes = log.read(cut, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), cut);
        (void)log.finalizeStagingRenames();
        (void)log.drainStagingRenamesDone();
    }
    {
        Log log(dir.string(), 512);
        EXPECT_GT(log.recover(), 0);
        EXPECT_EQ(countSuffix(dir, ".stlog"), 0u);
    }
    fs::remove_all(dir);
}

// While the staging -> base rename is still QUEUED (the provisioner has not run
// yet), a cold path defers instead of racing the rename.
TEST(SegmentLifecycleRegression, TruncateDefersWhileStagingRenameQueuedRegression) {
    const auto dir = freshDir("rune_slh_trunc_queued");
    {
        Log log(dir.string(), 512);
        log.recover();
        ASSERT_EQ(log.provisionReady(1), 1);
        int64_t off = 0;
        while (log.segmentBaseOffsets().size() < 2) off = put(log, off);
        const int64_t activeBase = log.segmentBaseOffsets().back();
        for (int i = 0; i < 3; ++i) off = put(log, off);
        EXPECT_FALSE(log.truncateTo(activeBase + 1)) << "rename queued: the cold path must defer";
        EXPECT_EQ(log.logEndOffset(), off) << "a deferred truncation changes nothing";
        ASSERT_EQ(log.finalizeStagingRenames(), 1);
        EXPECT_TRUE(log.truncateTo(activeBase + 1));
        EXPECT_FALSE(log.storageFailed()) << "errno=" << log.storageErrno();
        EXPECT_EQ(log.logEndOffset(), activeBase + 1);
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F1: a roll that adopts a ready segment performs no block reservation on the
// owner — the provisioner reserved it when it built the segment.
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, RollNeverPreallocatesOnOwnerRegression) {
    const auto dir = freshDir("rune_slh_f1");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh1{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh1);
        int64_t off = 0;
        int rolls = 0;
        const uint64_t before = log.ownerPreallocsForTest();
        while (rolls < 5) {
            ASSERT_EQ(log.provisionReady(2) >= 0, true);
            const auto n = log.segmentBaseOffsets().size();
            off = put(log, off);
            if (log.segmentBaseOffsets().size() > n) ++rolls;
            (void)log.finalizeStagingRenames();
            (void)log.drainStagingRenamesDone();
        }
        EXPECT_EQ(log.ownerPreallocsForTest(), before) << "F1: fallocate of the next segment on the owner";
        EXPECT_EQ(demands[2], rolls) << "every adopted segment posts one Adopted demand";
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F2: a provisioned log with an empty ready pool never creates a segment on
// the owner — the roll is deferred (rollDeferred()) and happens at the first
// append after the provisioner refilled the pool.
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, ProvisionedRollWithEmptyPoolDefersNeverCreatesInlineRegression) {
    const auto dir = freshDir("rune_slh_f2");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh2{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh2);
        int64_t off = 0;
        while (!log.rollDeferred()) off = put(log, off);   // fill the active segment
        const auto segsBefore = log.segmentBaseOffsets().size();
        const auto filesBefore = countSuffix(dir, ".log");
        off = put(log, off);   // an append that still arrives (follower / marker): lands, no roll
        EXPECT_EQ(log.segmentBaseOffsets().size(), segsBefore) << "F2: inline segment create";
        EXPECT_EQ(countSuffix(dir, ".log"), filesBefore);
        EXPECT_GE(log.rollDeferralsForTest(), 1u);
        EXPECT_TRUE(log.rollDeferred());
        EXPECT_EQ(log.logEndOffset(), off) << "the overshooting append was not lost";
        ASSERT_EQ(log.provisionReady(1), 1);
        EXPECT_FALSE(log.rollDeferred()) << "a ready segment clears the deferral";
        off = put(log, off);
        EXPECT_EQ(log.segmentBaseOffsets().size(), segsBefore + 1) << "the deferred roll happened";
        EXPECT_EQ(demands[2], 1);
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
    }
    fs::remove_all(dir);
}

// A roll deferred for too long is bounded: past the overshoot cap the roll is
// forced (blocking, correct) rather than letting a segment grow without limit.
TEST(SegmentLifecycleRegression, DeferredRollOvershootIsBoundedRegression) {
    const auto dir = freshDir("rune_slh_f2cap");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh3{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh3);
        int64_t off = 0;
        while (log.segmentBaseOffsets().size() < 2 && off < 1000000) off = put(log, off);
        ASSERT_EQ(log.segmentBaseOffsets().size(), 2u) << "the overshoot cap forced a roll";
        EXPECT_GE(log.forcedRollsForTest(), 1u);
        EXPECT_LE(log.segmentSizeForTest(log.segmentBaseOffsets().front()), log.maxDeferredRollBytesForTest() + 4096);
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F3: with the async seal queue full, a worker-driven log never fsyncs the
// seal inline — the roll is deferred until a pending seal completes.
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, SealQueueFullDefersNeverFsyncsInlineRegression) {
    const auto dir = freshDir("rune_slh_f3");
    {
        Log log(dir.string(), 256);
        log.recover();
        log.setDeferSealSync(true);
        int64_t off = 0;
        while (log.pendingSealCount() < Log::kMaxPendingSealsForTest()) off = put(log, off);
        const uint64_t syncs = log.syncCallsForTest();
        const auto segs = log.segmentBaseOffsets().size();
        while (!log.rollDeferred()) off = put(log, off);
        off = put(log, off);   // would have rolled: seal queue full
        EXPECT_EQ(log.syncCallsForTest(), syncs) << "F3: inline seal fsync on the owner";
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs);
        EXPECT_GE(log.rollDeferralsForTest(), 1u);
        // A completed seal frees a slot: the next append rolls.
        LogSegment* front = log.frontPendingSeal();
        ASSERT_NE(front, nullptr);
        log.popPendingSeal();
        log.unpinSegmentFlush(front);
        EXPECT_FALSE(log.rollDeferred());
        off = put(log, off);
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs + 1);
        EXPECT_EQ(log.syncCallsForTest(), syncs);
        while (LogSegment* s = log.frontPendingSeal()) { log.popPendingSeal(); log.unpinSegmentFlush(s); }
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F4: snapshot publication is O(1) per roll and per retention drop — no
// rebuild, and published snapshots are immutable (a held snapshot keeps its
// exact entries while the log rolls on across chunk / directory boundaries).
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, RollAndRetentionPublishSnapshotInConstantTimeRegression) {
    const auto dir = freshDir("rune_slh_f4");
    {
        Log log(dir.string(), 128);
        log.recover();
        const uint64_t rebuilds0 = log.snapshotRebuildsForTest();
        int64_t off = 0;
        while (log.segmentBaseOffsets().size() < 40) off = put(log, off, 5000 + off);
        const auto held = log.sealedSegmentSnapshot();
        const std::vector<SegInfo> heldCopy(held->begin(), held->end());
        // Roll across several chunk boundaries and header-pool directories.
        const std::size_t target = 3 * kSnapChunkEntries + 50;
        while (log.segmentBaseOffsets().size() < target) off = put(log, off, 5000 + off);
        EXPECT_EQ(log.snapshotRebuildsForTest(), rebuilds0) << "F4: a roll rebuilt the snapshot";
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
        ASSERT_EQ(held->size(), heldCopy.size());
        for (std::size_t i = 0; i < heldCopy.size(); ++i) EXPECT_EQ((*held)[i], heldCopy[i]) << i;
        // Retention: drop a prefix — O(1) (no rebuild).
        const auto bases = log.segmentBaseOffsets();
        const auto snap = log.sealedSegmentSnapshot();
        const std::size_t kDrop = target / 2;   // spans whole chunks
        std::size_t next = 0;
        int dropped = 0;
        for (int guard = 0; dropped < static_cast<int>(kDrop) && guard < 100000; ++guard) {
            while (next < kDrop && log.enqueueDeletedSegment(bases[next], (*snap)[next].lineage)) ++next;
            dropped += log.drainRetentionDeletes(INT64_MAX);
            (void)log.runCleanerFileOps(0, 0);
        }
        ASSERT_EQ(dropped, static_cast<int>(kDrop));
        EXPECT_EQ(log.snapshotRebuildsForTest(), rebuilds0) << "F4: a retention drop rebuilt the snapshot";
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
        // And rolls continue in O(1) after the front moved.
        const auto n = log.segmentBaseOffsets().size();
        while (log.segmentBaseOffsets().size() < n + kSnapChunkEntries) off = put(log, off, 5000 + off);
        EXPECT_EQ(log.snapshotRebuildsForTest(), rebuilds0);
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
    }
    fs::remove_all(dir);
}

// F4: a truncation rebuilds only the TAIL from the truncation point — the
// sealed prefix is shared, and the entries written are O(tail).
TEST(SegmentLifecycleRegression, TruncationRebuildsOnlyTheSnapshotTailRegression) {
    const auto dir = freshDir("rune_slh_f4trunc");
    {
        Log log(dir.string(), 128);
        log.recover();
        int64_t off = 0;
        while (log.segmentBaseOffsets().size() < 2 * kSnapChunkEntries + 30) off = put(log, off);
        const auto before = log.sealedSegmentSnapshot();
        const std::vector<SegInfo> beforeCopy(before->begin(), before->end());
        const auto bases = log.segmentBaseOffsets();
        const int64_t cut = bases[bases.size() - 6] + 1;   // into a late segment
        const uint64_t written0 = log.snapshotRebuiltEntriesForTest();
        ASSERT_TRUE(log.truncateTo(cut));
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
        EXPECT_LE(log.snapshotRebuiltEntriesForTest() - written0, 2u)
            << "F4: the truncation rewrote the untouched sealed prefix";
        for (std::size_t i = 0; i < beforeCopy.size(); ++i) EXPECT_EQ((*before)[i], beforeCopy[i]) << i;
        // Keep going: rolls after a truncation append in place again.
        const uint64_t rebuilds = log.snapshotRebuildsForTest();
        off = cut;
        while (log.segmentBaseOffsets().size() < bases.size() + 20) off = put(log, off);
        EXPECT_EQ(log.snapshotRebuildsForTest(), rebuilds);
        EXPECT_TRUE(log.snapshotMatchesSegmentsForTest());
        for (std::size_t i = 0; i < beforeCopy.size(); ++i) EXPECT_EQ((*before)[i], beforeCopy[i]) << i;
    }
    fs::remove_all(dir);
}

// F4 property test: random rolls / retention drops / truncations / reloads —
// the published snapshot always equals the sealed segments, and every snapshot
// ever handed out keeps its exact contents.
TEST(SegmentLifecycleRegression, SnapshotMatchesSegmentsUnderRandomOpsRegression) {
    const auto dir = freshDir("rune_slh_f4rand");
    {
        Log log(dir.string(), 96);
        log.recover();
        std::mt19937 rng(12345);
        int64_t off = 0;
        std::vector<std::pair<std::shared_ptr<const SealedSegmentSnapshot>, std::vector<SegInfo>>> held;
        for (int step = 0; step < 1500; ++step) {
            const int op = static_cast<int>(rng() % 100);
            if (op < 80) {
                off = put(log, off, 1000 + off);
            } else if (op < 88) {
                const auto bases = log.segmentBaseOffsets();
                const auto snap = log.sealedSegmentSnapshot();
                if (bases.size() > 3) {
                    const std::size_t k = 1 + rng() % 3;
                    for (std::size_t i = 0; i < k && i < snap->size(); ++i)
                        (void)log.enqueueDeletedSegment((*snap)[i].baseOffset, (*snap)[i].lineage);
                    (void)log.drainRetentionDeletes(INT64_MAX);
                    (void)log.runCleanerFileOps(0, 0);
                }
            } else if (op < 94) {
                const auto bases = log.segmentBaseOffsets();
                if (bases.size() > 4) {
                    const int64_t cut = bases[bases.size() - 1 - rng() % 4] + static_cast<int64_t>(rng() % 2);
                    if (cut > bases.front() && log.truncateTo(cut)) off = log.logEndOffset();
                }
            } else {
                const auto bases = log.segmentBaseOffsets();
                if (bases.size() > 2) log.reloadSegment(bases[rng() % (bases.size() - 1)]);
            }
            (void)log.runCleanerFileOps(0, 0);
            ASSERT_TRUE(log.snapshotMatchesSegmentsForTest()) << "step " << step << " op " << op;
            if (step % 50 == 0) {
                auto s = log.sealedSegmentSnapshot();
                held.emplace_back(s, std::vector<SegInfo>(s->begin(), s->end()));
            }
        }
        for (const auto& [s, copy] : held) {
            ASSERT_EQ(s->size(), copy.size());
            for (std::size_t i = 0; i < copy.size(); ++i) ASSERT_EQ((*s)[i], copy[i]);
        }
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// A provisioned log never loses a staging -> base rename.  When the rename
// request ring is full (the provisioner has not finalized the earlier renames
// yet) the roll is DEFERRED — it used to adopt anyway, the request was dropped,
// the segment kept its staging name / pathPending for good, and retention (which
// skips pathPending segments) stalled forever (latency gate FollowerTruncation:
// 10k+ segments piled up until fopen failed past fd 32767 on macOS).
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, FullRenameRingDefersRollNeverDropsRenameRegression) {
    const auto dir = freshDir("rune_slh_renamefull");
    {
        Log log(dir.string(), 256);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh4{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh4);
        int64_t off = 0;
        // Adopt segments without ever finalizing their renames.
        for (int guard = 0; guard < 100000; ++guard) {
            (void)log.provisionReady(1);
            if (log.rollDeferred()) break;   // the rename ring is full: back-pressure
            off = put(log, off);
        }
        ASSERT_TRUE(log.rollDeferred()) << "a full rename ring must defer the roll";
        const auto segs = log.segmentBaseOffsets().size();
        off = put(log, off);   // an append that still arrives: lands, no adoption
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs);
        // The provisioner catches up: every adopted segment gets its final name.
        (void)log.finalizeStagingRenames();
        (void)log.drainStagingRenamesDone();
        EXPECT_FALSE(log.rollDeferred());
        off = put(log, off);
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs + 1);
        (void)log.finalizeStagingRenames();
        (void)log.drainStagingRenamesDone();
        EXPECT_EQ(countSuffix(dir, ".stlog"), log.readyCountForTest()) << "only never-adopted ready segments";
        EXPECT_EQ(countSuffix(dir, ".log"), log.segmentBaseOffsets().size());
        // Retention can drop every sealed segment (none stuck pathPending).
        const auto snap = log.sealedSegmentSnapshot();
        std::size_t next = 0;
        int dropped = 0;
        for (int guard = 0; next < snap->size() && guard < 100000; ++guard) {
            while (next < snap->size() && log.enqueueDeletedSegment((*snap)[next].baseOffset, (*snap)[next].lineage)) ++next;
            dropped += log.drainRetentionDeletes(INT64_MAX);
            (void)log.runCleanerFileOps(0, 0);
        }
        for (int guard = 0; guard < 1000 && dropped < static_cast<int>(snap->size()); ++guard) {
            dropped += log.drainRetentionDeletes(INT64_MAX);
            (void)log.runCleanerFileOps(0, 0);
        }
        EXPECT_EQ(dropped, static_cast<int>(snap->size())) << "a sealed segment stuck pathPending";
        EXPECT_EQ(log.staleCleanerOpsDropped(), 0u);
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// The owner's roll path allocates nothing in steady state: adopting a ready
// segment, queueing its rename, posting the refill demand, the snapshot append
// (one chunk per kSnapChunkEntries rolls / one directory per kSnapHeadersPerDir
// publishes at most) and — with retention running — the map node (recycled).
// It used to allocate a map node and a whole snapshot copy at every roll.
// ---------------------------------------------------------------------------
TEST(SegmentLifecycleRegression, SteadyStateRollAllocatesNothingOnOwnerRegression) {
    const auto dir = freshDir("rune_slh_alloc");
    {
        Log log(dir.string(), 256);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh5{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh5);
        // Pre-built batches: building one allocates (test side, outside windows).
        std::vector<std::vector<uint8_t>> batches;
        int64_t off = 0;
        auto appendCounted = [&](int64_t& o) {
            RecordBatchBuilder b(o, 1000 + o);
            b.append("k", "value-" + std::to_string(o), 1000 + o);
            const auto batch = b.build();
            tCount = true;
            o = log.appendNoSync(batch.data(), batch.size(), o);
            tCount = false;
        };
        auto housekeeping = [&] {   // the provisioner's / cleaner's side (other threads in production)
            (void)log.provisionReady(2);
            (void)log.finalizeStagingRenames();
            tCount = true;
            (void)log.drainStagingRenamesDone();
            tCount = false;
            const auto snap = log.sealedSegmentSnapshot();
            if (snap->size() > 8) {
                (void)log.enqueueDeletedSegment((*snap)[0].baseOffset, (*snap)[0].lineage);
                tCount = true;
                (void)log.drainRetentionDeletes(INT64_MAX);
                tCount = false;
                (void)log.runCleanerFileOps(0, 0);
            }
        };
        // Warm up: reach steady state (retention running, nodes recycled).
        for (int i = 0; i < 2000; ++i) { housekeeping(); appendCounted(off); }
        tAllocs = 0;
        const auto segs0 = log.segmentBaseOffsets().size();
        const uint64_t installs0 = static_cast<uint64_t>(demands[2]);
        for (int i = 0; i < 2000; ++i) { housekeeping(); appendCounted(off); }
        const uint64_t rolls = static_cast<uint64_t>(demands[2]) - installs0;
        ASSERT_GT(rolls, 300u) << "the window must roll many times";
        EXPECT_LE(log.segmentBaseOffsets().size(), segs0 + 2) << "retention keeps up";
        // Bounded by the snapshot's rare chunk / directory allocations only.
        // a chunk: 1-2 allocations; a directory: 3 (control block, chunk table, header pool).
        const uint64_t allowed = 2 + 2 * (rolls / kSnapChunkEntries + 1) + 3 * (2 * rolls / kSnapHeadersPerDir + 1);
        EXPECT_LE(tAllocs, allowed) << "owner allocations: " << tAllocs << " over " << rolls << " rolls";
    }
    fs::remove_all(dir);
}

// D19 review L2: a log WITHOUT retention (compacted / infinite retention)
// releases no map node, so the owner used to allocate one map node per roll.
// The provisioner now builds the node with the ready segment.
TEST(SegmentLifecycleRegression, NoRetentionRollAllocatesNoMapNodeOnOwnerRegression) {
    const auto dir = freshDir("rune_slh_alloc_noret");
    {
        Log log(dir.string(), 256);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh);
        int64_t off = 0;
        auto appendCounted = [&](int64_t& o) {
            RecordBatchBuilder b(o, 1000 + o);
            b.append("k", "value-" + std::to_string(o), 1000 + o);
            const auto batch = b.build();
            tCount = true;
            o = log.appendNoSync(batch.data(), batch.size(), o);
            tCount = false;
        };
        auto housekeeping = [&] {   // provisioner side (another thread in production)
            (void)log.provisionReady(2);
            (void)log.finalizeStagingRenames();
            tCount = true;
            (void)log.drainStagingRenamesDone();
            tCount = false;
        };
        for (int i = 0; i < 200; ++i) { housekeeping(); appendCounted(off); }
        tAllocs = 0;
        const uint64_t installs0 = static_cast<uint64_t>(demands[2]);
        for (int i = 0; i < 1200; ++i) { housekeeping(); appendCounted(off); }
        const uint64_t rolls = static_cast<uint64_t>(demands[2]) - installs0;
        ASSERT_GT(rolls, 150u) << "the window must roll many times";
        // Only the snapshot's rare chunk / directory allocations (no map node per roll).
        const uint64_t allowed = 2 + 2 * (rolls / kSnapChunkEntries + 1) + 3 * (2 * rolls / kSnapHeadersPerDir + 1);
        EXPECT_LE(tAllocs, allowed) << "owner allocations: " << tAllocs << " over " << rolls << " rolls";
    }
    fs::remove_all(dir);
}

// ===========================================================================
// Review fixes (2026-10-06 review M1 / M3 / m4 / m6).
// ===========================================================================

// M1: a failed provisioner build is REPORTED to the owner over the ready ring;
// the held roll then happens inline (blocking, correct, counted) — a produce
// never waits forever on a provisioner that cannot deliver.
TEST(SegmentLifecycleRegression, ProvisionerFailureRollsInlineNeverHangsRegression) {
    const auto dir = freshDir("rune_slh_m1");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh6{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh6);
        int64_t off = 0;
        while (!log.rollDeferred()) off = put(log, off);
        ASSERT_GE(demands[1], 1) << "the Threshold demand was posted";
        log.setProvisionFaultForTest(EMFILE, 1);
        EXPECT_LT(log.provisionReady(1), 0) << "the build failed (provisioner side)";
        EXPECT_FALSE(log.rollDeferred()) << "a reported failure ends the hold";
        const auto segs = log.segmentBaseOffsets().size();
        off = put(log, off);
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs + 1) << "the roll went inline";
        EXPECT_EQ(log.provisionFailuresForTest(), 1u);
        EXPECT_EQ(log.failureInlineRollsForTest(), 1u);
        EXPECT_FALSE(log.storageFailed());
        // The next segment is provisioned normally again.
        while (!log.rollDeferred()) off = put(log, off);
        ASSERT_EQ(log.provisionReady(1), 1);
        off = put(log, off);
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs + 2);
        EXPECT_EQ(log.failureInlineRollsForTest(), 1u);
        (void)log.finalizeStagingRenames();
        (void)log.drainStagingRenamesDone();
    }
    fs::remove_all(dir);
}

// M1: ... and when the inline create fails too, the log FAILS (fail-stop, the
// pre-hotpath-A semantics) — loudly, never a silent hold.
TEST(SegmentLifecycleRegression, ProvisionerFailureThenInlineFailureFailsTheLogRegression) {
    const auto dir = freshDir("rune_slh_m1b");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh7{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh7);
        int64_t off = 0;
        while (!log.rollDeferred()) off = put(log, off);
        log.setProvisionFaultForTest(EACCES, 1);
        (void)log.provisionReady(1);
        // The directory becomes unwritable: the inline create fails as well.
        fs::permissions(dir, fs::perms::owner_read | fs::perms::owner_exec);
        EXPECT_THROW((void)put(log, off), std::exception);
        fs::permissions(dir, fs::perms::owner_all);
        EXPECT_TRUE(log.storageFailed()) << "a failed inline create fails the log (E1 fail-stop)";
    }
    fs::remove_all(dir);
}

// M3: demand-driven provisioning — no ready segment is demanded (reserved)
// while the active segment is below the threshold; exactly one Threshold
// demand per segment when it crosses it.
TEST(SegmentLifecycleRegression, ProvisioningIsDemandDrivenByFillThresholdRegression) {
    const auto dir = freshDir("rune_slh_m3");
    {
        Log log(dir.string(), 4096);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh8{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh8, /*thresholdPct=*/50);
        int64_t off = 0;
        // Fill to just below half: no demand.
        while (log.segmentSizeForTest(0) + 200 < 2048) off = put(log, off);
        EXPECT_EQ(demands[1], 0) << "an idle / lightly used partition demands nothing";
        EXPECT_EQ(log.readyCountForTest(), 0u);
        EXPECT_EQ(countSuffix(dir, ".stlog"), 0u) << "nothing reserved on disk";
        while (log.segmentSizeForTest(0) < 2048 + 200) off = put(log, off);
        EXPECT_EQ(demands[1], 1) << "one Threshold demand at the crossing";
        for (int i = 0; i < 5; ++i) off = put(log, off);
        EXPECT_EQ(demands[1], 1) << "never re-posted once queued";
    }
    fs::remove_all(dir);
}

// M3: a demand lost to a full provisioner ring is re-posted (never lost).
TEST(SegmentLifecycleRegression, LostDemandIsRepostedRegression) {
    const auto dir = freshDir("rune_slh_m3lost");
    {
        struct Flaky {
            int accepted = 0, refused = 0, refuseLeft = 3;
            static bool fn(void* c, Log*, Log::ProvisionDemand k) noexcept {
                auto* f = static_cast<Flaky*>(c);
                if (k != Log::ProvisionDemand::Threshold) return true;
                if (f->refuseLeft > 0) { --f->refuseLeft; ++f->refused; return false; }
                ++f->accepted;
                return true;
            }
        } flaky;
        Log log(dir.string(), 512);
        log.recover();
        rune::storage::testing::DemandChannelForTest demandCh9{&Flaky::fn, &flaky};
        log.setProvisioning(&demandCh9);
        int64_t off = 0;
        while (!log.rollDeferred()) off = put(log, off);
        EXPECT_EQ(flaky.refused, 3);
        EXPECT_EQ(flaky.accepted, 1) << "the Threshold demand got through on a retry";
    }
    fs::remove_all(dir);
}

// m6: a queued staging rename of the TAIL segment never blocks retention of
// the old segments (deleteOldSegments) — only cold paths touching it defer.
TEST(SegmentLifecycleRegression, QueuedTailRenameDoesNotBlockRetentionRegression) {
    const auto dir = freshDir("rune_slh_m6");
    {
        Log log(dir.string(), 256);
        log.recover();
        int64_t off = 0;
        while (log.segmentBaseOffsets().size() < 6) off = put(log, off);   // inline-created (legacy) segments
        ASSERT_EQ(log.provisionReady(1), 1);
        const auto before = log.segmentBaseOffsets().size();
        while (log.segmentBaseOffsets().size() == before) off = put(log, off);   // adopts: a rename queued
        for (int i = 0; i < 2; ++i) off = put(log, off);   // LEO well inside the adopted segment
        EXPECT_GT(log.deleteOldSegments(/*retainBytes=*/0, /*hw=*/INT64_MAX), 0)
            << "retention of old segments must not wait for the tail's rename";
        EXPECT_FALSE(log.truncateTo(log.segmentBaseOffsets().back() + 1))
            << "a cold path touching the renaming segment still defers";
        (void)log.finalizeStagingRenames();
        EXPECT_TRUE(log.truncateTo(log.segmentBaseOffsets().back() + 1));
        EXPECT_FALSE(log.storageFailed());
    }
    fs::remove_all(dir);
}

// m4: stale data — a CRASH with >= 2 ready (never written) segments and >= 2
// adopted-but-unrenamed segments recovers every record, the right bases, and
// leaves no staging file behind.
TEST(SegmentLifecycleRegression, CrashWithReadyAndQueuedRenamesRecoversAllDataRegression) {
    const auto dir = freshDir("rune_slh_m4crash");
    int64_t leo = 0;
    std::vector<int64_t> bases;
    {
        Log log(dir.string(), 256);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh10{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh10);
        int64_t off = 0;
        int adopted = 0;
        for (int guard = 0; adopted < 3 && guard < 100000; ++guard) {
            (void)log.provisionReady(2);
            const auto n = log.segmentBaseOffsets().size();
            off = put(log, off);
            if (log.segmentBaseOffsets().size() > n) ++adopted;
        }
        for (int i = 0; i < 3; ++i) off = put(log, off);
        (void)log.provisionReady(2);   // >= 2 ready, never written
        ASSERT_GE(countSuffix(dir, ".stlog"), 5u) << "3 unrenamed adopted + 2 ready";
        leo = log.logEndOffset();
        bases = log.segmentBaseOffsets();
        log.crashForTest();   // no rename drain, no staging cleanup
    }
    {
        Log log(dir.string(), 256);
        EXPECT_EQ(log.recover(), leo);
        EXPECT_EQ(log.segmentBaseOffsets(), bases);
        EXPECT_EQ(countSuffix(dir, ".stlog"), 0u);
        int64_t cursor = 0;
        while (cursor < leo) {
            auto bytes = log.read(cursor, 1 << 20);
            ASSERT_FALSE(bytes.empty()) << cursor;
            RecordBatchReader r(bytes.data(), bytes.size());
            ASSERT_TRUE(r.ensureValid());
            ASSERT_EQ(r.baseOffset(), cursor);
            cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
        }
    }
    fs::remove_all(dir);
}

// m4: a staging file whose first batch is torn is never parsed into a bogus
// base and never overwrites an existing segment — it is quarantined.
TEST(SegmentLifecycleRegression, TornStagingFileIsQuarantinedNeverOverwritesRegression) {
    const auto dir = freshDir("rune_slh_m4torn");
    int64_t leo = 0;
    {
        Log log(dir.string(), 1 << 20);
        log.recover();
        int64_t off = 0;
        for (int i = 0; i < 5; ++i) off = put(log, off);
        leo = log.logEndOffset();
    }
    const auto seg0 = dir / "00000000000000000000.log";
    const auto size0 = fs::file_size(seg0);
    {
        // A torn staging file whose garbage header "says" base 0.
        std::ofstream f(dir / ".staging-77.stlog", std::ios::binary);
        std::vector<char> junk(200, 0);
        junk[8] = 0x7f;   // totalLength garbage, no valid CRC
        f.write(junk.data(), static_cast<std::streamsize>(junk.size()));
    }
    {
        Log log(dir.string(), 1 << 20);
        EXPECT_EQ(log.recover(), leo);
        EXPECT_EQ(fs::file_size(seg0), size0) << "the existing segment was overwritten";
        EXPECT_TRUE(fs::exists(dir / ".staging-77.stlog.corrupt")) << "quarantined, not deleted";
    }
    fs::remove_all(dir);
}

// ===========================================================================
// Re-review N1: a rename that completes between the cold path's done-ring
// drain and its rename-ring read must never be missed — truncateTo would
// delete the segment by its STALE staging path (a silent no-op), leaving the
// real <base>.log on disk: the truncated (divergent) records resurrect after a
// restart.
// ===========================================================================
namespace {
std::string base20(int64_t b) {
    std::string s = std::to_string(b);
    return std::string(20 - s.size(), '0') + s;
}
} // namespace

TEST(SegmentLifecycleRegression, RenameCompletingInsideColdPathIsNeverMissedRegression) {
    const auto dir = freshDir("rune_slh_n1");
    int64_t bBase = -1;
    {
        Log log(dir.string(), 512);
        log.recover();
        int64_t off = 0;
        while (log.segmentSizeForTest(0) < 512) off = put(log, off);   // A [0, ..) inline-created
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh11{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh11);
        ASSERT_EQ(log.provisionReady(1), 1);
        off = put(log, off);                                          // rolls: adopts B
        bBase = log.segmentBaseOffsets().back();
        ASSERT_GT(bBase, 0);
        for (int i = 0; i < 3; ++i) off = put(log, off);
        // The provisioner finishes B's rename exactly inside the cold path.
        log.setColdPathInterleaveForTest([](void* l) { (void)static_cast<Log*>(l)->finalizeStagingRenames(); }, &log);
        bool done = false;
        for (int i = 0; i < 4 && !done; ++i) {
            done = log.truncateTo(bBase);
            log.setColdPathInterleaveForTest(nullptr, nullptr);
        }
        ASSERT_TRUE(done);
        EXPECT_FALSE(log.storageFailed());
        EXPECT_EQ(log.logEndOffset(), bBase);
        EXPECT_FALSE(fs::exists(dir / (base20(bBase) + ".log")))
            << "the truncated segment's file survived (deleted by its stale staging path)";
        log.crashForTest();
    }
    {
        Log log(dir.string(), 512);
        EXPECT_EQ(log.recover(), bBase) << "truncated records resurrected after restart";
    }
    fs::remove_all(dir);
}

// Re-review N1, the reloadSegments window: same interleaving, reload must see
// the renamed file and keep every record.
TEST(SegmentLifecycleRegression, RenameCompletingInsideReloadIsNeverMissedRegression) {
    const auto dir = freshDir("rune_slh_n1r");
    {
        Log log(dir.string(), 512);
        log.recover();
        int64_t off = 0;
        while (log.segmentSizeForTest(0) < 512) off = put(log, off);
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh12{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh12);
        ASSERT_EQ(log.provisionReady(1), 1);
        off = put(log, off);
        for (int i = 0; i < 3; ++i) off = put(log, off);
        const int64_t leo = log.logEndOffset();
        log.setColdPathInterleaveForTest([](void* l) { (void)static_cast<Log*>(l)->finalizeStagingRenames(); }, &log);
        bool done = false;
        for (int i = 0; i < 4 && !done; ++i) {
            done = log.reloadSegments();
            log.setColdPathInterleaveForTest(nullptr, nullptr);
        }
        ASSERT_TRUE(done);
        EXPECT_FALSE(log.storageFailed());
        EXPECT_EQ(log.logEndOffset(), leo);
        off = put(log, off);
        EXPECT_FALSE(log.storageFailed());
        int64_t cursor = 0;
        while (cursor < off) {
            auto bytes = log.read(cursor, 1 << 20);
            ASSERT_FALSE(bytes.empty()) << cursor;
            RecordBatchReader r(bytes.data(), bytes.size());
            ASSERT_TRUE(r.ensureValid());
            cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
        }
    }
    fs::remove_all(dir);
}

// Re-review N2: a held roll re-posts its demand (Hold) at the first blocked
// check and every kHoldRepostChecks checks — a demand the provisioner could not
// keep (parked for a log it did not know yet, then dropped) is recovered; a
// hold never waits forever on a lost demand.
TEST(SegmentLifecycleRegression, HeldRollRepostsItsDemandRegression) {
    const auto dir = freshDir("rune_slh_n2");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh13{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh13);
        int64_t off = 0;
        while (!log.rollDeferred()) off = put(log, off);
        EXPECT_EQ(demands[1], 1) << "Threshold";
        EXPECT_EQ(demands[3], 1) << "the hold episode posted its Hold demand at once";
        for (uint32_t i = 0; i < 3 * Log::kHoldRepostChecks; ++i) ASSERT_TRUE(log.rollDeferred());
        EXPECT_EQ(demands[3], 4) << "re-posted every kHoldRepostChecks blocked checks";
        ASSERT_EQ(log.provisionReady(1), 1);
        off = put(log, off);   // rolls: the episode ends
        while (!log.rollDeferred()) off = put(log, off);
        EXPECT_EQ(demands[3], 5) << "a new episode posts again";
    }
    fs::remove_all(dir);
}

// Re-review N3: over the reservation cap a ready segment is still built off the
// owner, only without its block reservation (no inline roll on the worker).
TEST(SegmentLifecycleRegression, UnpreallocatedReadySegmentIsAdoptedRegression) {
    const auto dir = freshDir("rune_slh_n3");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest demandCh14{&NoopHook::fn, demands};
        log.setProvisioning(&demandCh14);
        int64_t off = 0;
        while (!log.rollDeferred()) off = put(log, off);
        ASSERT_EQ(log.provisionReady(1, /*preallocate=*/false), 1);
        const auto segs = log.segmentBaseOffsets().size();
        off = put(log, off);
        EXPECT_EQ(log.segmentBaseOffsets().size(), segs + 1);
        EXPECT_EQ(log.failureInlineRollsForTest(), 0u);
        EXPECT_EQ(log.ownerPreallocsForTest(), 0u);
        (void)log.finalizeStagingRenames();
        (void)log.drainStagingRenamesDone();
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// D19 re-review N5a: a staging file's baseOffset is NOT under the batch CRC.
// A recovered base must fit the segment chain: above every canonical segment
// base, and (with a second batch) chained by the batch's own lastOffsetDelta.
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> batchAt(int64_t base, int records = 1) {
    RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < records; ++i) b.append("k", "v" + std::to_string(base + i), 1000);
    return b.build();
}
void writeStaging(const fs::path& dir, int64_t seq, const std::vector<std::vector<uint8_t>>& batches) {
    std::ofstream f(dir / (std::string(".staging-") + std::to_string(seq) + ".stlog"), std::ios::binary);
    for (const auto& b : batches) f.write(reinterpret_cast<const char*>(b.data()), static_cast<std::streamsize>(b.size()));
}
/// Three canonical segments; returns their bases (ascending) and the LEO.
std::pair<std::vector<int64_t>, int64_t> threeSegments(const fs::path& dir) {
    Log log(dir.string(), 512);
    log.recover();
    int64_t off = 0;
    while (log.segmentBaseOffsets().size() < 3) off = put(log, off);
    log.sync();
    auto bases = log.segmentBaseOffsets();
    return {std::vector<int64_t>(bases.begin(), bases.end()), log.logEndOffset()};
}
}  // namespace

TEST(SegmentLifecycleRegression, StagingBaseBelowTheChainIsQuarantinedRegression) {
    const auto dir = freshDir("rune_slh_n5a_low");
    const auto [bases, leo] = threeSegments(dir);
    ASSERT_GT(bases[1] + 1, bases[0]);
    // A CRC-valid batch whose (uncovered) baseOffset lands inside the chain.
    writeStaging(dir, 7, {batchAt(bases[1] + 1)});
    {
        Log log(dir.string(), 512);
        log.recover();
        const auto after = log.segmentBaseOffsets();
        EXPECT_EQ(std::count(after.begin(), after.end(), bases[1] + 1), 0) << "a bogus base entered the chain";
        EXPECT_EQ(log.logEndOffset(), leo);
        EXPECT_EQ(countSuffix(dir, ".stlog.corrupt"), 1u) << "quarantined (kept for the operator)";
    }
    fs::remove_all(dir);
}

TEST(SegmentLifecycleRegression, StagingBaseNotChainedToItsNextBatchIsQuarantinedRegression) {
    const auto dir = freshDir("rune_slh_n5a_chain");
    const auto [bases, leo] = threeSegments(dir);
    // First batch's base corrupted high; the second batch starts at the real LEO.
    writeStaging(dir, 8, {batchAt(leo + 5000, 2), batchAt(leo + 2)});
    {
        Log log(dir.string(), 512);
        log.recover();
        EXPECT_EQ(log.logEndOffset(), leo) << "a bogus base moved the LEO";
        EXPECT_EQ(countSuffix(dir, ".stlog.corrupt"), 1u);
    }
    fs::remove_all(dir);
}

TEST(SegmentLifecycleRegression, StagingBaseAboveTheChainIsRecovered) {
    const auto dir = freshDir("rune_slh_n5a_ok");
    const auto [bases, leo] = threeSegments(dir);
    writeStaging(dir, 9, {batchAt(leo, 2), batchAt(leo + 2)});   // the adopted-but-not-renamed segment
    {
        Log log(dir.string(), 512);
        log.recover();
        EXPECT_EQ(log.logEndOffset(), leo + 3) << "an adopted segment's data is never lost";
        EXPECT_EQ(countSuffix(dir, ".corrupt"), 0u);
    }
    fs::remove_all(dir);
}

// D19 re-review N6b: a ready segment queued behind a stale FAILURE record is
// adopted — the failure no longer forces one needless inline roll.
TEST(SegmentLifecycleRegression, ReadySegmentBehindAStaleFailureIsAdoptedRegression) {
    const auto dir = freshDir("rune_slh_n6b");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest ch{&NoopHook::fn, demands};
        log.setProvisioning(&ch);
        log.setProvisionFaultForTest(EIO, 1);
        ASSERT_LT(log.provisionReady(1), 0) << "the failed build posts a FAILURE record";
        ASSERT_EQ(log.provisionReady(1), 1) << "a later build succeeds behind it";
        const uint64_t prealloc0 = log.ownerPreallocsForTest();
        int64_t off = 0;
        const auto n = log.segmentBaseOffsets().size();
        while (log.segmentBaseOffsets().size() == n) off = put(log, off);
        EXPECT_EQ(log.failureInlineRollsForTest(), 0u) << "the stale failure forced an inline roll";
        EXPECT_EQ(log.ownerPreallocsForTest(), prealloc0) << "the queued ready segment was adopted";
        EXPECT_EQ(log.provisionFailuresForTest(), 1u) << "the failure is still counted";
    }
    fs::remove_all(dir);
}

// D19 re-review N6c: the rename request ring and its done ring are bounded
// TOGETHER — an adoption needs room for its completion too.  Before, the owner
// could keep adopting while the done ring was full; finalizeStagingRenames then
// stopped with requests queued and nothing re-drove them (an un-renamed
// staging file behind a non-held roll).
TEST(SegmentLifecycleRegression, RenameInFlightIsBoundedByTheDoneRingRegression) {
    const auto dir = freshDir("rune_slh_n6c");
    {
        Log log(dir.string(), 512);
        log.recover();
        int demands[4] = {0, 0, 0, 0};
        rune::storage::testing::DemandChannelForTest ch{&NoopHook::fn, demands};
        log.setProvisioning(&ch);
        int64_t off = 0;
        int rolls = 0;
        for (int i = 0; i < 80; ++i) {
            (void)log.provisionReady(2);
            const auto n = log.segmentBaseOffsets().size();
            while (log.segmentBaseOffsets().size() == n && !log.rollDeferred()) off = put(log, off);
            if (log.segmentBaseOffsets().size() == n) break;   // held
            ++rolls;
            (void)log.finalizeStagingRenames();   // the owner never drains the done ring here
        }
        EXPECT_LT(rolls, 80) << "adoptions must hold while their completions cannot be published";
        EXPECT_EQ(countSuffix(dir, ".stlog"), log.readyCountForTest())
            << "every adopted segment's rename was performed (none stranded)";
        (void)log.drainStagingRenamesDone();
        (void)log.provisionReady(2);
        const auto n = log.segmentBaseOffsets().size();
        for (int i = 0; i < 64 && log.segmentBaseOffsets().size() == n; ++i) off = put(log, off);
        EXPECT_GT(log.segmentBaseOffsets().size(), n) << "draining the done ring releases the held roll";
    }
    fs::remove_all(dir);
}

// D19 re-review N6d: a built segment whose ready-ring offer fails (ring full)
// is never dropped silently — the build reports a failure (counted).
TEST(SegmentLifecycleRegression, ReadyRingFullAfterABuildIsReportedRegression) {
    const auto dir = freshDir("rune_slh_n6d");
    {
        Log log(dir.string(), 512);
        log.recover();
        log.setProvisioning(nullptr);
        // Fill the ready ring with FAILURE records (each failed build posts one).
        log.setProvisionFaultForTest(EIO, 1 << 20);
        for (int i = 0; i < 64; ++i) (void)log.provisionReady(1);
        log.setProvisionFaultForTest(0, 0);
        EXPECT_EQ(log.readyQueuedApprox(), 0u) << "failure records are not ready segments";
        EXPECT_LT(log.provisionReady(1), 0) << "the built segment could not be queued: reported, not silent";
        EXPECT_EQ(countSuffix(dir, ".stlog"), 0u) << "its staging files are removed";
        EXPECT_EQ(log.readyDropsForTest(), 1u);
    }
    fs::remove_all(dir);
}
