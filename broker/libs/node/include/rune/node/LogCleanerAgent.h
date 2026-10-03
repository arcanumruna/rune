#pragma once
// LogCleanerAgent.h — CP-6: Dedicated compaction agent (off the duty-cycle thread).
//
// Implements rune::consensus::Agent so it can be driven by AgentRunner on its
// own thread (core-pinnable, same as NodeConsensusAgent / NodeRuntimeAgent).
// Compaction is moved OFF the duty-cycle / append hot path.
//
// Each doWork() cycle:
//   1. Scan all eligible leader logs in the ReplicaRegistry.
//   2. Compute byte-based dirty ratio for each log using the SEALED SEGMENT SNAPSHOT
//      (atomic shared_ptr published by the append thread — no direct access to
//       Log::segments_ which would be a data race).
//   3. Select the SINGLE filthiest log above minCleanableDirtyRatio (global max).
//   4. Compact one sealed segment.  The cycle budget (cycleBudgetBytes_) is
//      accumulated across segments: if the chosen segment's bytes exceed the
//      remaining budget, the cycle stops (soft cap; a single oversized segment
//      is always processed once to avoid starvation, matching Kafka semantics).
//   5. Enqueue the rewritten segment base via Log::enqueueCleanedSegment().
//      The actual in-memory reload is deferred to the append thread via
//      NodeRuntimeAgent::doWork → Log::drainCleanerSwaps().
//
// Byte dirty ratio (Kafka-exact, §3.2):
//   cleanBytes     = Σ seg.sizeBytes() for segs with base < firstDirtyOffset
//   cleanableBytes = Σ seg.sizeBytes() for segs with base ∈ [firstDirty, firstUncleanable)
//   firstUncleanable = min(activeSegmentBase, highWatermark)
//   dirtyRatio     = cleanableBytes / (cleanBytes + cleanableBytes)
//
// SINGLE-WRITER INVARIANT — the cleaner thread NEVER touches Log::segments_ (§3.1):
//   - Cleaner calls LogCleaner::produceCleanedFile() — PURE FILE WORK.  It reads
//     segment bytes ONLY from the on-disk .log files, using the base offsets
//     carried in the IMMUTABLE sealed-segment snapshot (atomic shared_ptr
//     published by the append thread) + the race-free log directory (Log::dir()).
//     It NEVER calls segmentBaseOffsets(), segmentSize() or reloadSegment().  It
//     atomically renames <base>.log.new → <base>.log on disk and returns.
//   - After the rename, the cleaner calls Log::enqueueCleanedSegment(base) — the
//     SPSC producer half (release-store publishes the base + the file rename).
//   - NodeRuntimeAgent::doWork drains these enqueues via drainCleanerSwaps()
//     on the single-writer append thread — the ONLY thread that mutates the
//     in-memory segments_ map (via reloadSegment()).  The release/acquire pair
//     on the ring establishes happens-before between the cleaner's file write and
//     the append thread's map update.  No lock on any path.
//   - Superseded LogSegment objects are freed on the append thread in
//     reloadSegment() — no reader can hold a reference after that point
//     because all reads are also on the append thread (single-writer invariant).
//   This is the CP-6 data-race fix: the previous design ran reloadSegment() AND
//     segmentBaseOffsets() on the CLEANER thread (cleanSegment), racing the
//     append thread's maybeRoll() — a TSan-proven write/write race on segments_.
//
// SPSC-overflow safety (§3.1):
//   enqueueCleanedSegment() returns false when the swap ring is momentarily full.
//   On failure the cleaner does NOT advance lastCleanOffset and does NOT charge
//   the budget — the segment stays dirty and is re-selected + re-produced +
//   re-enqueued next cycle (idempotent retry).  A dropped swap is never lost.
//
// Role-change abandon (§3.1):
//   The cleaner checks replica->cleanerRole() == Leader BEFORE calling
//   produceCleanedFile().  If the role changed to Follower BEFORE the check, the
//   segment is not compacted at all.  If the role changes AFTER the file is
//   produced (a race window we handle safely), we still enqueue the swap — the
//   append thread's drainCleanerSwaps → reloadSegment picks up the new file.
//   This is safe: the compacted data is correct (latest-per-key, below-HW), and
//   the follower will either truncate above HW (disjoint range) or transition
//   back to leader and continue.  We do NOT advance lastCleanOffset on a demoted
//   replica — the watermark only advances when the replica is confirmed Leader
//   AND the swap was successfully enqueued.
//
// Truncation disjointness (§3.1):
//   Compaction acts ONLY on sealed segments BELOW highWatermark.
//   Truncation acts on the tail ABOVE the committed HW.
//   These ranges are disjoint by construction: HW is the boundary.
//   The cleaner enforces this by computing firstUncleanable =
//   min(activeSegmentBase, highWatermark) — segments at or above HW are
//   never included in cleanableBytes and are never passed to produceCleanedFile.

#include <rune/consensus/ConsensusAgent.h>
#include <rune/node/CleanerViewChannel.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/Throttler.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::node {

class LogCleanerAgent final : public rune::consensus::Agent {
public:
    /// @param registry           Non-owning pointer to the replica registry.
    ///                           Must outlive this agent.
    /// @param cycleBudgetBytes   Maximum bytes of compaction work per doWork() call.
    ///                           Default 128 MiB matches Kafka's default.
    ///                           0 = unlimited per-cycle budget.
    /// @param dedupeBufferBytes  Memory budget for the SkimpyOffsetMap dedupe pass.
    ///                           Default -1 = use cycleBudgetBytes (or 128 MiB if
    ///                           cycleBudgetBytes is 0).  The map is sized to
    ///                           floor(dedupeBufferBytes / kEntryBytes × loadFactor).
    /// @param ioMaxBytesPerSec   Shared I/O throttle (bytes/sec). -1 = unlimited.
    /// @param useCoalescing      CP-8: enable segment coalescing — when true,
    ///                           consecutive sealed segments that together fit
    ///                           within coalescingRollBytes are merged into ONE
    ///                           output segment via produceCleanedGroupBounded.
    ///                           When false (default-compatible), each segment is
    ///                           compacted individually (prior behavior).
    /// @param coalescingRollBytes  Max combined output bytes for a coalesced group.
    ///                           Default 64 MiB (matches the standard segment roll
    ///                           threshold so groups fit within a single segment).
    explicit LogCleanerAgent(ReplicaRegistry* registry,
                             int64_t cycleBudgetBytes    = 128LL * 1024 * 1024,
                             int64_t dedupeBufferBytes   = -1,
                             int64_t ioMaxBytesPerSec    = -1,
                             bool    useCoalescing        = false,
                             int64_t coalescingRollBytes  = 64LL * 1024 * 1024) noexcept
        : registry_(registry)
        , cycleBudgetBytes_(cycleBudgetBytes)
        , dedupeBufferBytes_(dedupeBufferBytes >= 0
                             ? dedupeBufferBytes
                             : (cycleBudgetBytes > 0 ? cycleBudgetBytes
                                                     : 128LL * 1024 * 1024))
        , throttler_(ioMaxBytesPerSec)
        , useCoalescing_(useCoalescing)
        , coalescingRollBytes_(coalescingRollBytes)
        , dedupeMap_(rune::storage::SkimpyOffsetMap::maxSlotsFromBudget(dedupeBufferBytes_))
    {}

    LogCleanerAgent(const LogCleanerAgent&)            = delete;
    LogCleanerAgent& operator=(const LogCleanerAgent&) = delete;

    // -----------------------------------------------------------------------
    // Task 10 round 4: production (BrokerNode) wiring.  From now on the cleaner
    // NEVER touches the ReplicaRegistry (registry_ = nullptr): it works off its
    // own replica view, delivered generation by generation over `ch` (rings
    // only), and acks each generation it swapped to so the control thread knows
    // when a removed replica is unreachable here too.  Before the first view
    // arrives the cleaner is idle.  Call before the runner starts.
    // -----------------------------------------------------------------------
    void attachViewChannel(CleanerViewChannel* ch) noexcept {
        channel_  = ch;
        registry_ = nullptr;
    }
    /// I1: Kafka producer.id.expiration.ms for the producer-tail retention
    /// (NodeConfig::producerIdExpirationMs).  Call before the runner starts.
    void setProducerIdExpirationMs(int64_t ms) noexcept { producerIdExpirationMs_ = ms; }
    [[nodiscard]] int64_t producerIdExpirationMsForTest() const noexcept { return producerIdExpirationMs_; }
    [[nodiscard]] uint64_t viewGenerationForTest() const noexcept { return viewGen_; }
    [[nodiscard]] std::size_t viewSizeForTest() const noexcept { return view_.size(); }

    /// Test-only accessor: return the configured I/O throttle rate.
    /// -1 = unlimited.  Used by BrokerNodeTest to prove Fix-1 wiring.
    [[nodiscard]] int64_t throttleRateForTest() const noexcept {
        return throttler_.maxBytesPerSec();
    }

    // -----------------------------------------------------------------------
    // Agent::doWork — one cleaner cycle.
    //
    // Selects the filthiest eligible leader log (global max byte-dirty-ratio)
    // using the IMMUTABLE SEALED SEGMENT SNAPSHOT (atomic shared_ptr, no data
    // race with the append thread that mutates segments_).
    // Compacts one sealed segment per cycle. The cycle byte budget is checked
    // BEFORE processing: once bytesUsed_ >= cycleBudgetBytes_ (across cycles
    // within a single budget window), additional segments are skipped until
    // reset. A budget of 0 means unlimited.
    //
    // CP-9: Also runs retention on a separate cadence (retentionCheckMs).
    // Retention runs BEFORE compaction selection so the snapshot used for
    // filthiest-first is already up-to-date after deletions are enqueued.
    //
    // Returns 1 if a compaction segment was processed, 0 when idle.
    // -----------------------------------------------------------------------
    const char* name() const noexcept override { return "rune-cleaner"; }

    int doWork(int64_t nowNs) override {
        if (channel_) applyViewUpdates_();
        if (!registry_ && !channel_) return 0;

        // Append-p99 fix: keep every log's NEXT segment pre-provisioned on THIS
        // (background) thread, and finalize any deferred staging→base renames the
        // append thread enqueued at its last roll.  This is the background half of
        // the roll-cost-off-the-hot-path mechanism (Log::preprovisionPending /
        // finalizeStagingRenames — both NEVER touch segments_, preserving the
        // single-writer invariant).  Runs EVERY cycle, before any early return, and
        // for ALL replicas (followers roll too), independent of leader/compaction
        // gating.
        //
        // Its work count is deliberately NOT folded into doWork()'s return value:
        // doWork()'s return is the cleaner's COMPACTION/RETENTION signal (tests and
        // callers treat >0 as "a segment was compacted / retention ran").  Pre-
        // provisioning is background housekeeping that reaches steady state (returns
        // 0) after building one segment, so excluding it keeps that signal clean.
        for (auto* r : replicas_()) {
            // Task M1 precondition #2: skip memory-backed replicas.
            // r->log() asserts isDiskBacked() — never call it on an InMemoryLog.
            if (r && r->isDiskBacked()) {
                r->log()->preprovisionPending();
                r->log()->finalizeStagingRenames();
            }
        }

        // CP-8: sample wall-clock epoch-ms ONCE per cycle (deterministic horizon).
        const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        // ---------------------------------------------------------------
        // CP-9: Retention cadence — run retention if retentionCheckMs has
        // elapsed since the last run.  Use the MINIMUM retentionCheckMs
        // across all replicas in the registry (so fast-test replicas with
        // retentionCheckMs=1 are served promptly).
        // ---------------------------------------------------------------
        {
            int64_t minCheckMs = retentionCheckMs_; // default from ctor
            // Scan replicas for the smallest retentionCheckMs configured.
            {
                for (auto* r : replicas_()) {
                    if (!r) continue;
                    const int64_t rc = r->retentionCheckMs();
                    if (rc > 0 && rc < minCheckMs) minCheckMs = rc;
                }
            }
            if (nowMs - lastRetentionCheckMs_ >= minCheckMs) {
                runRetention(nowMs);
                lastRetentionCheckMs_ = nowMs;
            }
        }

        // ---------------------------------------------------------------
        // Phase 1: scan all eligible leader logs; compute byte-dirty ratio
        //          via the SEALED SEGMENT SNAPSHOT (cleaner must NEVER call
        //          log->segmentBaseOffsets() or log->segmentSize() directly —
        //          those iterate segments_ which is writer-thread-owned).
        // ---------------------------------------------------------------
        struct Candidate {
            rune::replication::PartitionReplica* replica  = nullptr;
            rune::storage::Log*                  log      = nullptr;
            double                               ratio    = 0.0;
            bool                                 forced   = false; // maxCompactionLag forced
            int64_t                              dirtyBytes = 0;    // cleanableBytes (budget charge)
            // CP-6 race-fix: retain the immutable snapshot + the derived dir so the
            // compaction (produceCleanedRangeBounded) reads segment bytes WITHOUT
            // ever touching Log::segments_.  The snapshot shared_ptr keeps the
            // sealed-base list alive for the whole cycle.
            std::shared_ptr<const rune::storage::SealedSegmentSnapshot> snap;
            std::string                          dir;
            int64_t                              firstUncleanable = 0;
        };
        Candidate best;

        for (auto* replica : replicas_()) {
            if (!replica) continue;
            // TSan race fix: gate on the cleaner-visible role snapshot (atomic
            // acquire), NOT role() which reads the duty-thread-mutated role_.
            if (replica->cleanerRole() != rune::replication::Role::Leader) continue;

            // Task M1 precondition #2: skip memory-backed replicas.
            // Compaction/retention only applies to on-disk logs.
            if (!replica->isDiskBacked()) continue;

            const auto policy = replica->cleanupPolicy();
            if (policy == rune::replication::CleanupPolicy::Delete) continue;

            rune::storage::Log* log = replica->log();
            if (!log) continue;

            // Load the immutable snapshot (atomic acquire — no race with append thread).
            auto snap = log->sealedSegmentSnapshot();
            if (!snap || snap->size() < 1) continue;  // need at least one sealed segment

            const int64_t firstDirtyOffset = replica->lastCleanOffset();
            // TSan race fix: use the cleaner-visible HW snapshot (atomic acquire),
            // NOT highWatermark() which reads leaderState_/isr_/leaderComplete_/
            // lastHealthyHw_ concurrently with the duty thread.  A slightly-stale
            // HW is safe: compaction is always bounded strictly below it.
            const int64_t hw               = replica->cleanerHighWatermark();

            // firstUncleanable = min(activeSegmentBase, hw).
            //
            // The active segment base is the exclusive end of the LAST sealed
            // segment (snap->back().endOffset) — carried in the immutable snapshot,
            // so this is race-free (no segments_ access).  This is CRITICAL for
            // convergence: the cleaner must NEVER advance its watermark past the
            // sealed region into the active segment.  If we used hw (which can be
            // ABOVE activeBase, inside the still-growing active segment), an empty
            // sealed dirty range would advance the watermark past active records;
            // when those records later seal they land BELOW the watermark and are
            // never compacted → a stale copy strands forever (the drcp1/3/4 bug).
            // Clamping to hw keeps compaction strictly below the committed HW.
            const int64_t activeBase = snap->back().endOffset;
            int64_t firstUncleanable = std::min(activeBase, hw);
            if (firstUncleanable < 0) firstUncleanable = 0;

            // CP-8: minCompactionLagMs — shrink firstUncleanable so segments
            // whose largestTimestamp is within the lag window are excluded from
            // the cleanable range.  This prevents compaction of very recent data.
            //
            // "A dirty non-active segment is UNCLEANABLE while:
            //   segment.largestTimestamp > nowMs - minCompactionLagMs"
            //
            // We find the FIRST segment from the front (lowest base) whose
            // largestTimestamp is within the lag; all segments from that point
            // onward are excluded (firstUncleanable shrinks to that base).
            //
            // largestTimestamp = 0 means unknown → treat as very old → cleanable.
            const int64_t minLagMs = replica->minCompactionLagMs();
            if (minLagMs > 0) {
                for (std::size_t ii = 0; ii < snap->size(); ++ii) {
                    const auto& si = (*snap)[ii];
                    if (si.baseOffset >= firstUncleanable) break;
                    const int64_t largestTs = si.largestTimestamp;
                    if (largestTs > 0 && largestTs > nowMs - minLagMs) {
                        // This segment and all after it are within the lag window
                        // — shrink firstUncleanable to exclude them.
                        if (si.baseOffset < firstUncleanable)
                            firstUncleanable = si.baseOffset;
                        break;
                    }
                }
            }

            // Compute byte-based dirty ratio (Kafka-exact formula §3.2).
            //
            // CP-7 bounded-range-per-pass amendment: after a partial pass,
            // firstDirtyOffset may land INSIDE a segment (base < firstDirty < segEnd).
            // That segment is still partially dirty and must be counted as cleanable,
            // not clean — otherwise dirtyRatio falls to 0 and the log is never
            // re-selected.  We compute each segment's logical end from the snapshot
            // and classify a segment as cleanable whenever its range overlaps
            // [firstDirtyOffset, firstUncleanable), i.e. segEnd > firstDirtyOffset.
            int64_t cleanBytes     = 0;
            int64_t cleanableBytes = 0;
            int64_t oldestDirtyTs  = std::numeric_limits<int64_t>::max(); // CP-8: for maxLag
            for (std::size_t ii = 0; ii < snap->size(); ++ii) {
                const auto& si = (*snap)[ii];
                if (si.baseOffset >= firstUncleanable) break;
                // Compute logical end of this segment.
                int64_t siEnd = (ii + 1 < snap->size())
                                ? (*snap)[ii + 1].baseOffset
                                : firstUncleanable;
                // A segment overlaps the dirty range if siEnd > firstDirtyOffset.
                // A segment is entirely clean if its range is completely before
                // firstDirtyOffset (siEnd <= firstDirtyOffset).
                if (siEnd <= firstDirtyOffset) {
                    cleanBytes += si.sizeBytes;
                } else {
                    cleanableBytes += si.sizeBytes;
                    // CP-8: track oldest dirty segment's largestTimestamp for maxLag.
                    if (si.largestTimestamp > 0 &&
                        si.largestTimestamp < oldestDirtyTs)
                        oldestDirtyTs = si.largestTimestamp;
                }
            }

            const int64_t total = cleanBytes + cleanableBytes;
            if (total <= 0) continue;

            const double dirtyRatio =
                static_cast<double>(cleanableBytes) / static_cast<double>(total);

            // CP-8: maxCompactionLagMs — force compaction (needCompactionNow) if
            // the oldest dirty segment has aged past maxCompactionLagMs.
            // "The oldest dirty segment's age = nowMs - largestTimestamp".
            // When forced, the log is eligible even if dirtyRatio <=
            // minCleanableDirtyRatio.
            bool needCompactionNow = false;
            const int64_t maxLagMs = replica->maxCompactionLagMs();
            if (maxLagMs > 0 && oldestDirtyTs < std::numeric_limits<int64_t>::max()) {
                const int64_t ageMs = nowMs - oldestDirtyTs;
                if (ageMs > maxLagMs) needCompactionNow = true;
            }

            // Ratio gate (Kafka min.cleanable.dirty.ratio): a log becomes eligible
            // when its byte-dirty ratio exceeds the threshold, OR when
            // maxCompactionLag forced it.  Unlike the old per-segment design, a
            // single selected pass re-cleans the WHOLE log prefix [logStart, passEnd)
            // against the dirty map (produceCleanedRangeBounded), so once a log is
            // picked it converges to latest-per-key in that pass — there is no
            // mid-sweep round state to keep, and no way to strand a superseded copy
            // below the watermark (the drcp1/3/4 "key delivered seq 37 AND 197" bug).
            const bool wouldSkip =
                (!needCompactionNow &&
                 dirtyRatio <= replica->minCleanableDirtyRatio());
            if (wouldSkip) continue;

            // Filthiest-first: keep the highest-ratio candidate; a forced log
            // (maxCompactionLag) always beats a non-forced one.
            bool dominates = false;
            if (!best.replica) {
                dominates = true;
            } else if (needCompactionNow && !best.forced) {
                dominates = true; // forced always beats non-forced
            } else if (!needCompactionNow && best.forced) {
                dominates = false; // already have a forced candidate
            } else {
                dominates = (dirtyRatio > best.ratio); // both same forced status
            }
            if (dominates) {
                best.ratio            = dirtyRatio;
                best.forced           = needCompactionNow;
                best.replica          = replica;
                best.log              = log;
                best.dirtyBytes       = cleanableBytes;
                // Retain the immutable snapshot + race-free dir for the off-thread
                // compaction.  dir() returns the immutable dir_ (no segments_).
                best.snap             = snap;
                best.dir              = log->dir();
                best.firstUncleanable = firstUncleanable;
            }
        }

        if (!best.replica) return 0;  // nothing to clean

        // ---------------------------------------------------------------
        // Phase 2: enforce per-cycle byte budget.
        //
        // bytesUsed_ accumulates across doWork() calls within a budget
        // window.  Reset it when a new window starts (i.e. when it was 0
        // at the top of this call — the previous cycle exhausted or idled).
        //
        // If cycleBudgetBytes_ > 0 AND bytesUsed_ is already >= budget,
        // skip this cycle.  Exception: if bytesUsed_ is 0 (first segment
        // of the window), always proceed — prevents starvation when a single
        // segment exceeds the budget (Kafka semantics).
        // ---------------------------------------------------------------
        if (cycleBudgetBytes_ > 0 && bytesUsed_ >= cycleBudgetBytes_) {
            // Budget exhausted for this window — idle this cycle, reset for next.
            bytesUsed_ = 0;
            return 0;
        }

        // ---------------------------------------------------------------
        // Phase 3: role-change guard — abort if no longer leader.
        // TSan race fix: read the cleaner-visible role snapshot (atomic acquire).
        // ---------------------------------------------------------------
        if (best.replica->cleanerRole() != rune::replication::Role::Leader) return 0;

        // ---------------------------------------------------------------
        // Phase 4: compact OFF-THREAD — PURE FILE WORK (never touches segments_).
        //
        // produceCleanedRangeBounded builds ONE bounded SkimpyOffsetMap over the
        // dirty section [globalDirtyStart, firstUncleanable) and re-copies EVERY
        // sealed segment with base < passEnd against it (Kafka doClean-from-log-
        // start).  This drops a stale prefix copy whose key reappears in the dirty
        // section, so compaction converges to latest-per-key in the SELECTED pass —
        // even when producers interleave with the cleaner (the drcp1/3/4 fix).  It
        // reads segment bytes only via the immutable snapshot bases (best.snap) +
        // race-free dir (best.dir); the in-memory reload is deferred to the append
        // thread via enqueueCleanedSegment() → drainCleanerSwaps() → reloadSegment().
        //
        // When the bounded map fills before covering the whole dirty range, passEnd
        // < firstUncleanable and the next cycle resumes there (bounded-range-per-
        // pass); records at/beyond passEnd are preserved verbatim.
        // ---------------------------------------------------------------

        // CP-7 lazy checkpoint init: open/create the per-log checkpoint, seeding
        // lastCleanOffset from the persisted value BEFORE reading globalDirtyStart.
        // Pure file I/O.  Uses best.firstUncleanable (race-free) as the clamp bound.
        ensureCheckpoint(best.log, best.dir, best.replica, best.firstUncleanable);

        // globalDirtyStart = the (possibly checkpoint-restored) clean watermark.
        const int64_t globalDirtyStart = best.replica->lastCleanOffset();

        // Snapshot-derived sealed base + size lists (no segments_ access).  Reuse
        // the agent-owned scratch vectors — cleared, not re-allocated (ULL).
        scratchBases_.clear();
        scratchSizes_.clear();
        scratchBases_.reserve(best.snap->size());
        scratchSizes_.reserve(best.snap->size());
        for (const auto& si : *best.snap) {
            scratchBases_.push_back(si.baseOffset);
            scratchSizes_.push_back(si.sizeBytes);
        }
        const std::vector<int64_t>& sealedBases = scratchBases_;
        const std::vector<int64_t>& sealedSizes = scratchSizes_;

        // CP-8 per-topic roll: coalesce up to the replica's own segmentRollBytes so a
        // 4 KiB-roll topic coalesces to 4 KiB and a 64 MiB topic to 64 MiB.
        const int64_t replicaRoll    = best.replica->segmentRollBytes();
        const int64_t groupRollBytes = (replicaRoll > 0) ? replicaRoll : coalescingRollBytes_;

        rune::storage::LogCleaner cleaner;
        cleaner.setProducerIdExpirationMs(producerIdExpirationMs_);
        rune::storage::RangeCleanResult passResult = cleaner.produceCleanedRangeBounded(
            best.dir,
            sealedBases,
            sealedSizes,
            /*activeBase=*/       best.firstUncleanable,
            /*globalDirtyStart=*/ globalDirtyStart,
            nowNs,
            best.replica->deleteRetentionMs(),
            /*map=*/              dedupeMap_,
            /*useCoalescing=*/    useCoalescing_,
            /*groupRollBytes=*/   groupRollBytes,
            /*throttler=*/        &throttler_,
            /*nowMs=*/            nowMs);

        const int64_t passEnd = passResult.passEnd;

        // ---------------------------------------------------------------
        // Phase 5: enqueue swaps for the rewritten/deleted segments, then advance
        // the clean watermark.  SPSC-overflow: if ANY enqueue fails (ring full) we
        // do NOT advance the watermark, so the segments stay dirty and are
        // re-selected + re-produced next cycle (idempotent retry — never lost).
        //   - a rewritten base → reloadSegment picks up the new .log.
        //   - a merged/emptied base → reloadSegment finds the .log gone, drops it.
        // ---------------------------------------------------------------
        bool allEnqueued = true;
        for (int64_t gb : passResult.rewrittenBases) {
            if (!best.log->enqueueCleanedSegment(gb)) { allEnqueued = false; break; }
        }
        if (!allEnqueued) {
            // Ring full — leave the segment(s) dirty; retry next cycle.  Do not
            // advance the watermark (preserves the CP-6 ring-full invariant).
            return 0;
        }

        // Charge the dirty bytes swept this pass toward the per-window budget.
        bytesUsed_ += best.dirtyBytes;

        // Advance lastCleanOffset to passEnd (MONOTONIC: forward-only, leader-only).
        // The guard also covers the ensureCheckpoint ordering hazard (a restored
        // checkpoint may already have bumped the watermark past passEnd).
        if (passEnd > globalDirtyStart &&
            best.replica->cleanerRole() == rune::replication::Role::Leader) {
            best.replica->setLastCleanOffset(passEnd);
            // CP-7: persist the checkpoint (pure file I/O; cleaner-thread-safe).
            if (auto it = checkpoints_.find(best.log); it != checkpoints_.end()) {
                it->second->store(passEnd);
            }
        }

        // Return 1 when a segment was actually rewritten/deleted this pass; 0 when
        // the pass only advanced the watermark over an already-clean dirty range.
        return passResult.compacted ? 1 : 0;
    }

private:
    // -----------------------------------------------------------------------
    // CP-9: Retention scheduler — runRetention(nowMs)
    //
    // Called from doWork() when the retentionCheckMs cadence has elapsed.
    // For each leader-owned replica with a Delete or CompactDelete policy:
    //   1. Load the immutable sealed-segment snapshot (no segments_ access).
    //   2. Compute the set of deletable segments via three predicates
    //      (time, size, log-start-offset).  Active segment EXCLUDED.
    //      Bounded by high-watermark (only compacted/committed range).
    //   3. For each deletable segment:
    //      (a) Rename <base>.log → <base>.log.deleted on disk (pure file work —
    //          safe on the cleaner thread).
    //      (b) Enqueue the base via Log::enqueueDeletedSegment() so the
    //          append thread erases it from segments_ in drainRetentionDeletes().
    //      (c) Record the .deleted path for deferred physical removal.
    //   4. Run physicallyRemoveExpiredDeletedFiles(nowMs, fileDeleteDelayMs)
    //      to physically remove .deleted files whose delay has elapsed.
    //
    // SINGLE-WRITER INVARIANT:
    //   - The cleaner thread ONLY renames files and enqueues bases.
    //   - segments_ mutation happens ONLY on the append thread (step 3b via
    //     drainRetentionDeletes).
    //   - No lock on any path.
    //
    // compact,delete coordination:
    //   - For CompactDelete replicas, compaction may have already rewritten a
    //     segment.  The segment's .log file still exists (compaction renames
    //     <base>.log.new → <base>.log atomically).  Retention then renames the
    //     final .log → .deleted.  There is no double-delete because:
    //     (a) compaction enqueues via the SWAP ring (drainCleanerSwaps →
    //         reloadSegment, which updates the in-memory segment pointer);
    //     (b) retention enqueues via the DELETE ring (drainRetentionDeletes →
    //         segments_.erase).  A segment is never in both rings simultaneously:
    //         the cleaner selects a segment for EITHER compaction OR deletion in a
    //         single cycle, never both.  The compaction's log file is the latest
    //         on disk; retention renames that latest file to .deleted.
    // -----------------------------------------------------------------------
    void runRetention(int64_t nowMs) {
        if (!registry_ && !channel_) return;

        for (auto* replica : replicas_()) {
            if (!replica) continue;
            // Retention is leader-only (failover-safe: a demoted leader stops
            // retention because cleanerRole() != Leader).
            // TSan race fix: read the cleaner-visible role snapshot (atomic acquire).
            if (replica->cleanerRole() != rune::replication::Role::Leader) continue;

            // Task M1 precondition #2: skip memory-backed replicas.
            // Retention only applies to on-disk logs.
            if (!replica->isDiskBacked()) continue;

            const auto policy = replica->cleanupPolicy();
            // Retention applies to Delete and CompactDelete topics only.
            if (policy == rune::replication::CleanupPolicy::Compact) continue;

            rune::storage::Log* log = replica->log();
            if (!log) continue;

            const int64_t retentionMs     = replica->retentionMs();
            const int64_t retentionBytes  = replica->retentionBytes();
            const int64_t logStartOffset  = replica->logStartOffset();
            const int64_t fileDelayMs     = replica->fileDeleteDelayMs();

            // If no retention policy is active, skip.
            if (retentionMs < 0 && retentionBytes < 0 && logStartOffset <= 0) continue;

            // Load the immutable sealed-segment snapshot (no segments_ access).
            auto snap = log->sealedSegmentSnapshot();
            if (!snap || snap->empty()) continue;

            // High-watermark: never delete the active segment or segments that
            // are above (or at) the high-watermark.
            // TSan race fix: use the cleaner-visible HW snapshot (atomic acquire),
            // NOT highWatermark() (races leaderState_/isr_/leaderComplete_).  A
            // stale-low HW is safe: retention only deletes strictly below it.
            const int64_t hw = replica->cleanerHighWatermark();

            // Build the list of deletable segments using the three predicates.
            // Active segment = last in the log; we must never delete it.
            // We iterate over SEALED segments only (all in the snapshot).
            // A segment is deletable if ANY predicate fires for it.
            //
            // Predicate 1 — Time: now - seg.largestTimestamp > retentionMs
            //   largestTimestamp = 0 → unknown → treat as ancient (deletable).
            //   Only fires when retentionMs >= 0.
            //
            // Predicate 2 — Size (oldest-first): running total of sealed-segment
            //   bytes from OLDEST to NEWEST; once the running total exceeds
            //   retentionBytes, older segments are deletable.
            //   Active segment is excluded (it's not in the snapshot).
            //   Only fires when retentionBytes >= 0.
            //
            // Predicate 3 — Log-start-offset: seg whose nextSegment.baseOffset
            //   (= next element's base, or hw for the last sealed) <= logStartOffset.
            //   In other words: if the entire segment's offset range falls below
            //   logStartOffset, delete it.
            //
            // For size predicate: compute total sealed bytes first.
            int64_t totalSealedBytes = 0;
            for (const auto& si : *snap) {
                if (si.baseOffset >= hw) break; // stop at hw boundary
                totalSealedBytes += si.sizeBytes;
            }

            // Running size from newest → oldest (to find which oldest segs to drop).
            // We walk from newest sealed to oldest; once keepBytes < retentionBytes,
            // anything older is deletable.
            int64_t keepBytes = totalSealedBytes;

            // Build the deletable set (base offsets to delete).
            std::vector<int64_t> toDelete;

            for (std::size_t i = 0; i < snap->size(); ++i) {
                const auto& si = (*snap)[i];
                if (si.baseOffset >= hw) break; // never touch at/above HW

                // Compute this segment's logical end (next segment's base or hw).
                const int64_t segEnd = (i + 1 < snap->size())
                                       ? (*snap)[i + 1].baseOffset
                                       : hw;

                bool deletable = false;

                // Predicate 1: Time.
                if (!deletable && retentionMs >= 0) {
                    const int64_t largestTs = si.largestTimestamp;
                    // CP-5.1 defensive guard: largestTs == 0 means UNKNOWN — treat
                    // as NOT deletable by time.  A missing timestamp must never cause
                    // accidental deletion (belt-and-suspenders alongside the CP-5.1
                    // recover() scan that populates real values; this guard prevents
                    // data loss if any path still leaves largestTs as 0).
                    // largestTs > 0 → apply the normal age predicate.
                    if (largestTs > 0 && (nowMs - largestTs) > retentionMs) {
                        deletable = true;
                    }
                }

                // Predicate 2: Size (oldest-first).
                if (!deletable && retentionBytes >= 0) {
                    // keepBytes starts at totalSealedBytes; as we walk oldest→newest
                    // we subtract each segment we consider keeping.  But we need to
                    // determine if this segment is one of the "excess" oldest ones.
                    // Simple approach: accumulate size from the end (newest to oldest
                    // is index snap->size()-1 to 0).  For index i (oldest end),
                    // keepBytes reflects all bytes EXCLUDING segments [0..i-1].
                    // At index i, keepBytes = totalSealedBytes - Σ(sizeBytes[j] for j<i)
                    // = Σ(sizeBytes[j] for j>=i).  If keepBytes > retentionBytes,
                    // the OLDEST segment (index 0) can be dropped → but we need to
                    // track which are excess.
                    //
                    // Simpler: recompute as prefix sum from oldest.
                    // deletable if Σ(bytes[0..i]) + all remaining bytes > retentionBytes
                    // is implied by keepBytes > retentionBytes before subtracting si.
                    // We want: remove segments starting from oldest until keepBytes <= retentionBytes.
                    // So: deletable at index i if keepBytes > retentionBytes.
                    if (keepBytes > retentionBytes) {
                        deletable = true;
                        keepBytes -= si.sizeBytes;
                    }
                }

                // Predicate 3: Log-start-offset.
                if (!deletable && logStartOffset > 0) {
                    // Delete if the segment's entire range is below logStartOffset.
                    // segEnd <= logStartOffset means nextSeg.base <= logStartOffset
                    // → this segment's records are all before logStartOffset.
                    if (segEnd <= logStartOffset) {
                        deletable = true;
                    }
                }

                if (!deletable) continue;

                // -------------------------------------------------------
                // Async delete: rename .log → .deleted on disk (file work
                // — safe on the cleaner thread), then enqueue for
                // segments_ removal on the append thread.
                // -------------------------------------------------------
                // Mirror LogSegment::fname: zero-pad base to 20 digits.
                std::ostringstream pathBuf;
                pathBuf << log->dir() << "/"
                        << std::setw(20) << std::setfill('0') << si.baseOffset << ".log";
                std::string logPath = pathBuf.str();
                std::string deletedPath = logPath + ".deleted";

                // Only rename if .log still exists (idempotent: a prior cycle
                // may have already renamed it but the enqueue failed).
                if (!std::filesystem::exists(logPath)) {
                    // Already renamed (or never existed); try to enqueue again.
                    // Record the .deleted path for deferred physical removal if
                    // not already recorded.
                    if (std::filesystem::exists(deletedPath)) {
                        log->recordDeletedFile(deletedPath, nowMs);
                    }
                } else {
                    // Rename .log → .deleted (atomic on most POSIX filesystems).
                    std::error_code ec;
                    std::filesystem::rename(logPath, deletedPath, ec);
                    if (!ec) {
                        log->recordDeletedFile(deletedPath, nowMs);
                    }
                    // Whether rename succeeded or not, we try to enqueue.
                    // If rename failed, the segment is still intact — no harm.
                }

                // Enqueue for segments_ removal on the append thread.
                // enqueueDeletedSegment returns false if the ring is full;
                // in that case the rename has already happened, so next cycle
                // we'll retry (find logPath absent → directly record .deleted).
                log->enqueueDeletedSegment(si.baseOffset);
                toDelete.push_back(si.baseOffset);
            }

            // ---------------------------------------------------------------
            // Deferred physical removal: sweep .deleted files whose delay
            // has elapsed.  Safe to call every retention cycle.
            // ---------------------------------------------------------------
            log->physicallyRemoveExpiredDeletedFiles(nowMs, fileDelayMs);
        }
    }

    // CP-7: Lazy-init the cleaner-offset-checkpoint for `log`.
    // If the checkpoint file already exists, seeds replica->lastCleanOffset()
    // with the persisted value (truncation-aware).  Called on the CLEANER THREAD;
    // pure file I/O — no Log::segments_ access.
    //
    // TSan fix (CP-7): takes `highWatermark` (from the Phase-1 snapshot, already
    // available and race-free) as the truncation-clamp bound instead of reading
    // log->logEndOffset() directly (logEndOffset_ is non-atomic, written by the
    // append thread → data race confirmed by TSan).  Compaction is bounded below
    // HW anyway, so HW is a safe and conservative clamp.
    void ensureCheckpoint(rune::storage::Log* log,
                          const std::string& dir,
                          rune::replication::PartitionReplica* replica,
                          int64_t highWatermark) {
        if (checkpoints_.count(log)) return; // already opened
        std::string ckptPath = dir + "/cleaner-offset-checkpoint";
        // Use highWatermark as the truncation-clamp bound (race-free; from the
        // immutable Phase-1 snapshot) instead of log->logEndOffset() which is
        // non-atomic and races with the append thread.
        auto ckpt = std::make_unique<rune::storage::CleanerOffsetCheckpoint>(
            ckptPath, highWatermark);
        // Seed the in-memory watermark from the persisted checkpoint.
        // Only advance (don't regress) the existing watermark.
        const int64_t persisted = ckpt->value();
        if (persisted > replica->lastCleanOffset()) {
            replica->setLastCleanOffset(persisted);
        }
        checkpoints_.emplace(log, std::move(ckpt));
    }

    ReplicaRegistry*        registry_;            // non-owning; nullptr once a view channel is attached

    // Task 10 round 4: cleaner-owned replica view (see CleanerViewChannel.h).
    CleanerViewChannel*                               channel_ = nullptr;
    std::vector<rune::replication::PartitionReplica*> view_;
    std::vector<rune::replication::PartitionReplica*> building_;
    uint64_t viewGen_     = 0;
    uint64_t buildingGen_ = 0;
    bool     ackPending_  = false;

    [[nodiscard]] const std::vector<rune::replication::PartitionReplica*>& replicas_() const noexcept {
        return channel_ ? view_ : registry_->allReplicas();
    }
    void applyViewUpdates_() {
        channel_->view.drain([this](int32_t type, aeron::concurrent::AtomicBuffer& buf,
                                    aeron::util::index_t off, aeron::util::index_t len) {
            if (type != CleanerViewChannel::kViewMsgType ||
                static_cast<std::size_t>(len) < CleanerViewChannel::kHeaderBytes) return;
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
            uint64_t gen; uint32_t total, index, count;
            std::memcpy(&gen, p, 8); std::memcpy(&total, p + 8, 4);
            std::memcpy(&index, p + 12, 4); std::memcpy(&count, p + 16, 4);
            if (index == 0) { building_.clear(); buildingGen_ = gen; }
            else if (gen != buildingGen_ || building_.size() != index) return;   // stale partial
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t v; std::memcpy(&v, p + CleanerViewChannel::kHeaderBytes + 8u * i, 8);
                building_.push_back(reinterpret_cast<rune::replication::PartitionReplica*>(
                    static_cast<std::uintptr_t>(v)));
            }
            if (building_.size() == total) {
                view_.swap(building_);
                building_.clear();
                viewGen_    = gen;
                ackPending_ = true;
            }
        }, 64);
        if (ackPending_ && channel_->ack.write(reinterpret_cast<const uint8_t*>(&viewGen_), 8,
                                               CleanerViewChannel::kAckMsgType))
            ackPending_ = false;   // a full ring is retried next pass (never lost)
    }

    int64_t                 cycleBudgetBytes_;    // 0 = unlimited; >0 = soft bytes-per-window cap
    int64_t                 dedupeBufferBytes_;   // dedupe map byte budget (SkimpyOffsetMap, > 0 always)
    int64_t                 bytesUsed_ = 0;       // bytes consumed so far in the current budget window
    rune::storage::Throttler throttler_;          // CP-8: shared I/O throttler (cleaner-thread only)
    int64_t producerIdExpirationMs_ = rune::storage::ProducerTails::kDefaultExpirationMs;   // I1
    // CP-8 Fix 3: segment coalescing.
    bool    useCoalescing_       = false;          // if true, group consecutive segments and merge
    int64_t coalescingRollBytes_ = 64LL * 1024 * 1024; // max combined output bytes per group

    // ULL: reusable per-pass scratch (this agent runs on ONE AgentRunner thread, so
    // no synchronization is needed).  The dedupe map is sized ONCE to the budget and
    // clear()ed each pass; the base/size vectors are cleared and refilled — so a
    // steady-state cleaner cycle performs near-zero heap allocation.
    rune::storage::SkimpyOffsetMap dedupeMap_;
    std::vector<int64_t> scratchBases_;
    std::vector<int64_t> scratchSizes_;

    // CP-7: per-log cleaner-offset-checkpoint (lazy init on first compaction).
    // Key = Log* (non-owning, lives as long as the replica registry).
    // Pure file I/O; never touches Log::segments_.
    std::unordered_map<rune::storage::Log*,
                       std::unique_ptr<rune::storage::CleanerOffsetCheckpoint>>
        checkpoints_;

    // CP-9: retention scheduler state.
    // retentionCheckMs_:  default cadence (overridden by per-replica minimum).
    //   Default 300'000 ms = 5 min (Kafka default).  Set to 1 for fast tests.
    int64_t retentionCheckMs_ = 300'000LL;
    // lastRetentionCheckMs_: last wall-clock ms when runRetention() was called.
    //   Initialized to 0 so retention runs on the very first doWork() call.
    int64_t lastRetentionCheckMs_ = 0;
};

} // namespace rune::node
