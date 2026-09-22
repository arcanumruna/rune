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
//   The cleaner checks replica->role() == Leader BEFORE calling
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
#include <limits>
#include <memory>
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
    {}

    LogCleanerAgent(const LogCleanerAgent&)            = delete;
    LogCleanerAgent& operator=(const LogCleanerAgent&) = delete;

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
    // Returns 1 if a compaction segment was processed, 0 when idle.
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs) override {
        if (!registry_) return 0;

        // CP-8: sample wall-clock epoch-ms ONCE per cycle (deterministic horizon).
        const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

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
            int64_t                              segBase  = -1;
            int64_t                              segEnd   = -1;
            int64_t                              segBytes = 0;
            // CP-6 race-fix: retain the immutable snapshot + the derived dir so
            // the compaction (produceCleanedFile) reads segment bytes WITHOUT
            // ever touching Log::segments_.  The snapshot shared_ptr keeps the
            // sealed-base list alive for the whole cycle.
            std::shared_ptr<const rune::storage::SealedSegmentSnapshot> snap;
            std::string                          dir;
            int64_t                              firstUncleanable = 0;
        };
        Candidate best;

        for (auto* replica : registry_->allReplicas()) {
            if (!replica) continue;
            if (replica->role() != rune::replication::Role::Leader) continue;

            const auto policy = replica->cleanupPolicy();
            if (policy == rune::replication::CleanupPolicy::Delete) continue;

            rune::storage::Log* log = replica->log();
            if (!log) continue;

            // Load the immutable snapshot (atomic acquire — no race with append thread).
            auto snap = log->sealedSegmentSnapshot();
            if (!snap || snap->size() < 1) continue;  // need at least one sealed segment

            const int64_t firstDirtyOffset = replica->lastCleanOffset();
            const int64_t hw               = replica->highWatermark();

            // The active segment base is just beyond the last sealed segment.
            // We approximate it from the snapshot: any segment whose base is
            // not in the snapshot is either active or not yet published.
            // For the firstUncleanable calculation we use hw (conservative):
            // if hw == 0 (no writes yet), nothing is cleanable.
            // The active segment base is also needed; since the snapshot only
            // contains SEALED segments, we infer it as the high watermark
            // (conservative — no active-base pollution in the dirty scan).
            // Note: hw == logEndOffset() when the active segment is small, but
            // that's fine: we just include slightly less in the dirty window.
            int64_t firstUncleanable = hw;  // min(active_base, hw); conservative

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

            if (!needCompactionNow &&
                dirtyRatio <= replica->minCleanableDirtyRatio()) continue;

            // Find the sealed segment that CONTAINS or starts at firstDirtyOffset.
            //
            // CP-7 bounded-range-per-pass: after a partial pass, lastCleanOffset
            // (= firstDirtyOffset) may be in the MIDDLE of a segment (e.g.
            // firstDirtyOffset=3 while the only sealed segment has base=0).  We
            // must select that segment — not skip it because base < firstDirtyOffset.
            //
            // Selection rule: pick the FIRST sealed segment s such that:
            //   s.segEnd > firstDirtyOffset  AND  s.base < firstUncleanable
            // where segEnd = next segment's base (or firstUncleanable for the last).
            // This selects the segment whose byte range overlaps [firstDirtyOffset, ∞).
            int64_t segBase  = -1;
            int64_t segEnd   = -1;
            int64_t segBytes = 0;
            for (std::size_t i = 0; i < snap->size(); ++i) {
                const auto& si = (*snap)[i];
                if (si.baseOffset >= firstUncleanable) break;
                // Compute this segment's logical end.
                int64_t sEnd = (i + 1 < snap->size())
                               ? (*snap)[i + 1].baseOffset
                               : firstUncleanable;
                // The segment is cleanable if its range overlaps [firstDirtyOffset, ∞):
                //   sEnd > firstDirtyOffset  (segment ends AFTER the dirty start)
                // AND  si.baseOffset < firstUncleanable  (segment starts before HW)
                if (sEnd <= firstDirtyOffset) continue; // entirely before dirty range
                segBase  = si.baseOffset;
                segEnd   = sEnd;
                segBytes = si.sizeBytes;
                break;
            }
            if (segBase < 0) {
                // All sealed segments in the dirty range are already clean.
                // Advance the watermark.
                replica->setLastCleanOffset(firstUncleanable);
                continue;
            }

            // Filthiest-first: keep the candidate with the highest ratio.
            // Forced logs (maxCompactionLag) win over any non-forced log.
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
                best.segBase          = segBase;
                best.segEnd           = segEnd;
                best.segBytes         = segBytes;
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
        // ---------------------------------------------------------------
        if (best.replica->role() != rune::replication::Role::Leader) return 0;

        // ---------------------------------------------------------------
        // Phase 4: compact the segment OFF-THREAD — PURE FILE WORK.
        //
        // CP-6 race-fix (the load-bearing invariant): the cleaner thread MUST
        // NOT touch Log::segments_.  We call produceCleanedFileBounded() which
        // reads segment bytes ONLY from disk using the immutable snapshot base
        // offsets (best.snap) + the race-free dir (best.dir), writes the
        // compacted .log, and returns.  It never calls segmentBaseOffsets(),
        // segmentSize() or reloadSegment().  The in-memory reload is deferred
        // to the append thread via enqueueCleanedSegment() → drainCleanerSwaps()
        // → reloadSegment().
        //
        // Uses the global dirty range for latest-per-key correctness (CP-2).
        //
        // CP-7: produceCleanedFileBounded builds a SkimpyOffsetMap bounded by
        // dedupeBufferBytes_.  When the map fills before covering the whole dirty
        // range, it returns passEnd < activeBase so the watermark advances only
        // to passEnd (bounded-range-per-pass).  The next cycle resumes from
        // passEnd.  The segment is rewritten with cleanBoundaryOffset = passEnd
        // so records beyond passEnd survive to be revisited.
        // ---------------------------------------------------------------

        // CP-7 lazy checkpoint init: open (or create) the per-log checkpoint
        // file on first use, seeding lastCleanOffset from the persisted value.
        // MUST be called BEFORE reading globalDirtyStart (= lastCleanOffset())
        // so the compaction starts from the persisted watermark, not 0.
        // Pure file I/O — never touches Log::segments_.
        // TSan fix: uses best.firstUncleanable (race-free, from Phase-1 snapshot)
        // as the truncation-clamp bound instead of log->logEndOffset().
        ensureCheckpoint(best.log, best.dir, best.replica, best.firstUncleanable);

        // Re-read globalDirtyStart AFTER ensureCheckpoint so it reflects any
        // value loaded from the persisted checkpoint file.
        const int64_t globalDirtyStart = best.replica->lastCleanOffset();

        // Snapshot-derived sealed base list (no segments_ access).
        std::vector<int64_t> sealedBases;
        sealedBases.reserve(best.snap->size());
        for (const auto& si : *best.snap) sealedBases.push_back(si.baseOffset);

        rune::storage::LogCleaner cleaner;

        // CP-8 Fix 3: segment coalescing.
        //
        // When useCoalescing_=true, group consecutive sealed segments with
        // total size <= coalescingRollBytes into one group and compact the
        // group into a SINGLE output via produceCleanedGroupBounded.
        // This eliminates the O(N) per-segment fsync overhead when many small
        // segments exist, and reduces total segment count on each pass.
        //
        // When useCoalescing_=false, fall back to the single-segment path
        // (prior behavior, fully backward-compatible).
        //
        // Coalescing is LOAD-BEARING: scratch-neutering produceCleanedGroupBounded
        // (returning {false, activeBase}) falls back to single-segment compaction
        // and the CoalescingEndToEndFewSegments test fails (segment count
        // unchanged rather than reduced).

        rune::storage::CleanPassResult passResult{false, best.firstUncleanable};
        // groupBases: the bases in the coalesced group (1 element = single-seg path).
        std::vector<int64_t> groupBases{best.segBase};

        if (useCoalescing_ && sealedBases.size() > 1) {
            // Build parallel sizes vector from the snapshot.
            std::vector<int64_t> sealedSizes;
            sealedSizes.reserve(best.snap->size());
            for (const auto& si : *best.snap) sealedSizes.push_back(si.sizeBytes);

            // CP-8 per-topic roll: use the replica's own segmentRollBytes as the
            // coalescing group bound so a topic with a 128-byte roll coalesces to
            // 128 bytes and a 64 MiB topic coalesces to 64 MiB — not a single
            // global constant.  Fall back to coalescingRollBytes_ (ctor default)
            // if the replica returns 0 or negative (safety guard).
            const int64_t replicaRoll = best.replica->segmentRollBytes();
            const int64_t groupRollBytes = (replicaRoll > 0) ? replicaRoll : coalescingRollBytes_;

            // Compute coalescing groups over ALL sealed segments.
            auto groups = rune::storage::LogCleaner::groupSegmentsBySize(
                sealedBases, sealedSizes, groupRollBytes);

            // Find the group that contains best.segBase.
            const rune::storage::SegmentGroup* chosenGroup = nullptr;
            for (const auto& g : groups) {
                for (int64_t b : g.bases) {
                    if (b == best.segBase) { chosenGroup = &g; break; }
                }
                if (chosenGroup) break;
            }

            if (chosenGroup && chosenGroup->bases.size() > 1) {
                // Multi-segment group: use coalescing path.
                groupBases = chosenGroup->bases;
                passResult = cleaner.produceCleanedGroupBounded(
                    best.dir,
                    sealedBases,
                    *chosenGroup,
                    /*activeBase=*/      best.firstUncleanable,
                    /*globalDirtyStart=*/globalDirtyStart,
                    nowNs,
                    best.replica->deleteRetentionMs(),
                    /*dedupeBufferBytes=*/dedupeBufferBytes_,
                    /*throttler=*/       &throttler_,
                    /*nowMs=*/           nowMs);
            } else {
                // Single-segment group (or best.segBase not in any multi-seg group):
                // fall back to single-segment path.
                passResult = cleaner.produceCleanedFileBounded(
                    best.dir,
                    sealedBases,
                    /*segBase=*/         best.segBase,
                    /*activeBase=*/      best.firstUncleanable,
                    /*globalDirtyStart=*/globalDirtyStart,
                    nowNs,
                    best.replica->deleteRetentionMs(),
                    /*dedupeBufferBytes=*/dedupeBufferBytes_,
                    /*throttler=*/       &throttler_,
                    /*nowMs=*/           nowMs);
            }
        } else {
            // Coalescing disabled or only one sealed segment: single-segment path.
            passResult = cleaner.produceCleanedFileBounded(
                best.dir,
                sealedBases,
                /*segBase=*/         best.segBase,
                /*activeBase=*/      best.firstUncleanable,
                /*globalDirtyStart=*/globalDirtyStart,
                nowNs,
                best.replica->deleteRetentionMs(),
                /*dedupeBufferBytes=*/dedupeBufferBytes_,
                /*throttler=*/       &throttler_,
                /*nowMs=*/           nowMs);
        }

        // CP-7: passEnd is the next-pass boundary.  When the map covered the
        // entire dirty range, passEnd == activeBase.  When the map was bounded
        // and filled before the end, passEnd < activeBase — the watermark
        // advances only to passEnd.
        const int64_t passEnd = passResult.passEnd;

        if (!passResult.compacted) {
            // No segment was rewritten.  This can happen when:
            //   (a) latestMapped < 0: map got no records in dirty range → advance
            //       to passEnd=activeBase so we don't stall (nothing to clean).
            //   (b) segBase >= passEnd: segment is beyond the bounded pass boundary.
            //       Advance to passEnd so next cycle resumes from the right place.
            // In both cases we only advance if there's no enqueue outstanding.
            // No enqueue is needed since no file was produced; just move the watermark.
            // NOTE: do NOT advance if passEnd == globalDirtyStart (no progress).
            if (passEnd > globalDirtyStart) {
                if (best.replica->role() == rune::replication::Role::Leader) {
                    best.replica->setLastCleanOffset(passEnd);
                    if (auto it = checkpoints_.find(best.log); it != checkpoints_.end()) {
                        it->second->store(passEnd);
                    }
                }
            }
            return 0;
        }

        // ---------------------------------------------------------------
        // Phase 5: enqueue the swap(s); advance the clean watermark ONLY on
        //          a successful enqueue of ALL group bases.
        //
        // For a coalesced group: enqueue ALL bases in the group.
        //   - group.bases[0] → reloadSegment picks up the merged .log file.
        //   - group.bases[1..] → reloadSegment finds the .log deleted, drops them.
        // For a single-segment path: enqueue just the one base (unchanged).
        //
        // Watermark advances to passEnd (not segEnd as before).  When the
        // SkimpyOffsetMap was bounded (map filled mid-segment), passEnd <
        // activeBase — this is the bounded-range-per-pass semantics: the
        // next cycle resumes from passEnd and recompacts the same segment
        // (now partially clean, with records up to passEnd removed).
        //
        // SPSC-OVERFLOW FIX: enqueueCleanedSegment() returns false when the
        // swap ring is full.  We DO NOT advance the watermark on failure.
        // ---------------------------------------------------------------
        // Enqueue ALL group bases atomically (SPSC: one-by-one, contiguous).
        // If ANY enqueue fails (ring full), do not advance the watermark.
        bool allEnqueued = true;
        for (int64_t gb : groupBases) {
            if (!best.log->enqueueCleanedSegment(gb)) {
                allEnqueued = false;
                break;
            }
        }
        if (!allEnqueued) {
            // Ring full — leave the segment(s) dirty so they are re-selected next cycle.
            return 0;
        }

        // Accumulate bytes consumed by this compaction toward the budget.
        bytesUsed_ += best.segBytes;

        // Advance lastCleanOffset to passEnd (the bounded-range boundary).
        // When not bounded, passEnd == best.firstUncleanable (== activeBase),
        // which naturally marks the whole dirty range as clean.
        if (best.replica->role() == rune::replication::Role::Leader) {
            best.replica->setLastCleanOffset(passEnd);
            // CP-7: persist the checkpoint to passEnd (not segEnd).
            // Pure file I/O (mmap write + msync); cleaner-thread-safe.
            if (auto it = checkpoints_.find(best.log); it != checkpoints_.end()) {
                it->second->store(passEnd);
            }
        }

        return 1;
    }

private:
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

    ReplicaRegistry*        registry_;            // non-owning
    int64_t                 cycleBudgetBytes_;    // 0 = unlimited; >0 = soft bytes-per-window cap
    int64_t                 dedupeBufferBytes_;   // dedupe map byte budget (SkimpyOffsetMap, > 0 always)
    int64_t                 bytesUsed_ = 0;       // bytes consumed so far in the current budget window
    rune::storage::Throttler throttler_;          // CP-8: shared I/O throttler (cleaner-thread only)
    // CP-8 Fix 3: segment coalescing.
    bool    useCoalescing_       = false;          // if true, group consecutive segments and merge
    int64_t coalescingRollBytes_ = 64LL * 1024 * 1024; // max combined output bytes per group

    // CP-7: per-log cleaner-offset-checkpoint (lazy init on first compaction).
    // Key = Log* (non-owning, lives as long as the replica registry).
    // Pure file I/O; never touches Log::segments_.
    std::unordered_map<rune::storage::Log*,
                       std::unique_ptr<rune::storage::CleanerOffsetCheckpoint>>
        checkpoints_;
};

} // namespace rune::node
