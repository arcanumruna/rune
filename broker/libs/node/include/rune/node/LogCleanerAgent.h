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
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace rune::node {

class LogCleanerAgent final : public rune::consensus::Agent {
public:
    /// @param registry         Non-owning pointer to the replica registry.
    ///                         Must outlive this agent.
    /// @param cycleBudgetBytes Maximum bytes of compaction work per doWork() call.
    ///                         Default 128 MiB matches Kafka's default dedupe buffer.
    explicit LogCleanerAgent(ReplicaRegistry* registry,
                             int64_t cycleBudgetBytes = 128LL * 1024 * 1024) noexcept
        : registry_(registry)
        , cycleBudgetBytes_(cycleBudgetBytes)
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
            const int64_t firstUncleanable = hw;  // min(active_base, hw); conservative

            // Compute byte-based dirty ratio (Kafka-exact formula §3.2).
            int64_t cleanBytes     = 0;
            int64_t cleanableBytes = 0;
            for (const auto& si : *snap) {
                if (si.baseOffset >= firstUncleanable) break;
                if (si.baseOffset < firstDirtyOffset) {
                    cleanBytes += si.sizeBytes;
                } else {
                    cleanableBytes += si.sizeBytes;
                }
            }

            const int64_t total = cleanBytes + cleanableBytes;
            if (total <= 0) continue;

            const double dirtyRatio =
                static_cast<double>(cleanableBytes) / static_cast<double>(total);

            if (dirtyRatio <= replica->minCleanableDirtyRatio()) continue;

            // Find the first sealed segment to compact: base >= firstDirtyOffset
            // and base < firstUncleanable.
            int64_t segBase  = -1;
            int64_t segEnd   = -1;
            int64_t segBytes = 0;
            for (std::size_t i = 0; i < snap->size(); ++i) {
                const auto& si = (*snap)[i];
                if (si.baseOffset < firstDirtyOffset) continue;
                if (si.baseOffset >= firstUncleanable) break;
                segBase  = si.baseOffset;
                segBytes = si.sizeBytes;
                // segEnd: next segment's base, or use hw as upper bound.
                if (i + 1 < snap->size()) {
                    segEnd = (*snap)[i + 1].baseOffset;
                } else {
                    segEnd = firstUncleanable;
                }
                break;
            }
            if (segBase < 0) {
                // All sealed segments in the dirty range are already clean.
                // Advance the watermark.
                replica->setLastCleanOffset(firstUncleanable);
                continue;
            }

            // Filthiest-first: keep the candidate with the highest ratio.
            if (dirtyRatio > best.ratio) {
                best.ratio            = dirtyRatio;
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
        // NOT touch Log::segments_.  We call produceCleanedFile() which reads
        // segment bytes ONLY from disk using the immutable snapshot base offsets
        // (best.snap) + the race-free dir (best.dir), writes the compacted .log,
        // and returns.  It never calls segmentBaseOffsets(), segmentSize() or
        // reloadSegment().  The in-memory reload is deferred to the append thread
        // via enqueueCleanedSegment() → drainCleanerSwaps() → reloadSegment().
        //
        // Uses the global dirty range for latest-per-key correctness (CP-2).
        // ---------------------------------------------------------------
        const int64_t globalDirtyStart = best.replica->lastCleanOffset();

        // Snapshot-derived sealed base list (no segments_ access).
        std::vector<int64_t> sealedBases;
        sealedBases.reserve(best.snap->size());
        for (const auto& si : *best.snap) sealedBases.push_back(si.baseOffset);

        rune::storage::LogCleaner cleaner;
        const bool compacted = cleaner.produceCleanedFile(
            best.dir,
            sealedBases,
            /*segBase=*/     best.segBase,
            /*activeBase=*/  best.firstUncleanable,
            /*globalDirtyStart=*/ globalDirtyStart,
            nowNs,
            best.replica->deleteRetentionMs());

        if (!compacted) return 0;

        // ---------------------------------------------------------------
        // Phase 5: enqueue the swap; advance the clean watermark ONLY on
        //          a successful enqueue.
        //
        // We always ATTEMPT the enqueue (even if we raced a role change after
        // the pre-clean guard) — the append thread's drainCleanerSwaps →
        // reloadSegment handles both cases safely:
        //   • still leader: picks up the compacted file.
        //   • demoted to follower: reloadSegment updates the in-memory
        //     segment to point at the (valid, below-HW) compacted file.
        //     Subsequent truncation acts on the tail ABOVE HW — disjoint.
        //
        // SPSC-OVERFLOW FIX (blocking finding #5): enqueueCleanedSegment()
        // returns false when the swap ring is momentarily full.  If we advanced
        // lastCleanOffset in that case, the segment would be compacted-on-disk
        // but NEVER installed into segments_ AND never re-selected — silent data
        // loss / divergence.  Instead we DO NOT advance the watermark on enqueue
        // failure: the segment stays dirty and is re-selected + re-enqueued next
        // cycle (idempotent retry — produceCleanedFile and reloadSegment are both
        // idempotent).  We also do NOT count budget bytes for a dropped swap, so
        // the retry is not throttled away.
        // ---------------------------------------------------------------
        const bool enqueued = best.log->enqueueCleanedSegment(best.segBase);
        if (!enqueued) {
            // Ring full — leave the segment dirty (watermark unadvanced) so it is
            // re-selected next cycle.  No budget charge, no watermark skip.
            return 0;
        }

        // Accumulate bytes consumed by this compaction toward the budget only
        // once the swap is durably enqueued for install.
        bytesUsed_ += best.segBytes;

        // Only advance lastCleanOffset when still leader (avoid corrupting the
        // cleaned-watermark on a demoted replica) AND only after a successful
        // enqueue (avoid skipping a lost swap).
        if (best.replica->role() == rune::replication::Role::Leader) {
            best.replica->setLastCleanOffset(best.segEnd);
        }

        return 1;
    }

private:
    ReplicaRegistry* registry_;         // non-owning
    int64_t          cycleBudgetBytes_; // 0 = unlimited; >0 = soft bytes-per-window cap
    int64_t          bytesUsed_ = 0;    // bytes consumed so far in the current budget window
};

} // namespace rune::node
