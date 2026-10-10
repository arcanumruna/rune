#pragma once
// WorkerViewSet.h — Task 15: per-partition-worker view-fold driver.
//
// A WorkerViewSet is owned by exactly ONE Worker (shared-nothing: plain
// members, no atomics, no locks, no thread_local).  It holds this worker's
// installed materialized view PLANS (`templates_`, keyed by viewTopicId), a
// reactor index from a source topic to the views interested in it
// (`subscriptionIndex_`), and — crucially — one ViewInstance PER (view,
// partition): `views_` is keyed by a (viewTopicId, partition) composite.
//
// Per-partition is a correctness requirement, not an optimization (spec
// §11.2: a view partition is pinned to its source partition).  A worker
// commonly owns MULTIPLE partitions of the same source topic; driveFolds is
// called once per owned (srcTopicId, partition).  Offsets are partition-local,
// so each (view, partition) MUST carry its own foldCursor + its own ViewTable
// — otherwise partition 1's fold would resume at partition 0's cursor and read
// an unrelated offset of the wrong log, decoding garbage.
//
// The Worker drives it from `doWork`, right AFTER `rcAdvanceAll_()`, once per
// owned source partition whose committed range may have advanced.  For each
// interested view it looks up (or lazily creates, from the stored plan) the
// (view, partition) instance, reads `[foldCursor, bound)` through a
// caller-supplied `readRange` (the worker's readNoFault_ into its push
// scratch), folds that committed range into that partition's table via
// rune::view::foldBatch, and — for a PERSISTED view only — drains the dirtied
// rows into a Task-12 differential checkpoint delta and pushes it onto this
// worker's OWN ViewSnapshotChannel.deltas ring (SPSC; the per-node
// view-snapshot IO agent drains the set of per-worker channels — Task 19).  A
// TRANSIENT view folds + serves rows but skips the checkpoint handoff.
//
// Hot path (driveFolds): zero steady-state allocation beyond the deliberate,
// reused scratch buffers (dirty-entry list, row-byte scratch, encode buffer)
// and the deliberate per-checkpoint ring push.  A delta that cannot be pushed
// because the ring is full is HELD (never dropped) and retried next pass
// before any further folding for that (view, partition).  driveFolds/driveView_
// return a real progress count (records folded + deltas pushed/held) so the
// node's idle strategy never backs off while view work is pending.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

#include <rune/node/ViewSnapshotChannel.h>

namespace rune::node {

// Task 16: restart readiness of ONE (view, partition) instance.
//
//   Recovering       — transient: loading this instance's on-disk checkpoint
//                      (base + deltas) via rune::view::replay.  Not observed at
//                      rest: installViewForRecovery performs the load inline and
//                      leaves the instance in RebuildingViews (or Ready).
//   RebuildingViews  — checkpoint loaded (foldCursor = recovered O); the
//                      committed source tail (foldCursor, committedTail] still
//                      has to be folded.  A GetViewRow here is a RETRIABLE
//                      not-ready — NEVER a half-built row (Review-focus #5).
//   Ready            — foldCursor >= committedTail; the table is fully caught up
//                      and may be served.
//
// The machine is LOCAL-ONLY: it reaches Ready purely from this node's own
// checkpoint + a replay of its own committed source log — no cluster/peer
// dependency, no cross-thread coordination.  It is bounded by the checkpoint
// cadence (each rebuild pass folds a bounded committed range), deadlock-free,
// and monotone (Recovering -> RebuildingViews -> Ready, never backwards).
enum class ViewReadiness : uint8_t { Recovering, RebuildingViews, Ready };

// Task 19b: split an on-disk view checkpoint file into its individual Task-12
// records.  The file a ViewSnapshotIoAgent writes is a BARE concatenation of
// rune::view::encodeDelta() wire records (the 8-byte routing header is stripped
// before the bytes hit the file — see ViewSnapshotIoAgent::handleDeltaFrame_),
// so recovery must split it back into the per-record spans
// rune::view::replay() expects.  Each record is self-describing:
//   [magic u32][O i64][rowLen u32][count u32] then count × {rid u32, flags u8,
//   key u64, [rowLen bytes iff flags==0 (upsert)]} then [crc u32].
// Bounds-safe (Task 8 lesson): never trusts a length/count field to over-read;
// a torn/truncated TRAILING record is DROPPED (the returned spans stop before
// it) rather than over-read — exactly what replay() then treats as the
// recoverable append-only tail.  The returned spans point INTO `bytes`, which
// must outlive them.
[[nodiscard]] std::vector<std::span<const uint8_t>>
splitViewCheckpointRecords(std::span<const uint8_t> bytes);

// One installed materialized view on this worker, for ONE partition.  MOVABLE
// (lives by value in `views_`): it declares only a converting constructor, so
// the implicit move ctor/assignment are still generated (ViewTable and
// VersionDescriptorCache are themselves movable — only vectors + an
// unordered_map, no user-declared special members).  COPY is never used.
struct ViewInstance {
    rune::view::CompiledViewPlan       plan;
    rune::view::RowLayout              layout;
    rune::codec::Descriptor            evolvingSrc;
    rune::view::VersionDescriptorCache vdc;
    rune::view::ViewTable              table;
    int32_t                            partition       = 0;
    int64_t                            foldCursor      = 0;
    // The offset O of the most recently ENCODED checkpoint delta.  This is the
    // attempted/encoded offset, NOT a durable one — durability is reported
    // separately by the view-snapshot IO agent's `done` ring (consumed in
    // Task 19).  Never mistake it for a durable/recoverable offset.
    int64_t                            lastEncodedO    = -1;
    bool                               persisted       = true;

    // Task 16 restart readiness of THIS (view, partition).  Defaults to Ready:
    // an instance created on the normal (non-recovery) driveFolds path is
    // serve-ready immediately, so the serve-guard is INERT for every existing
    // path.  installViewForRecovery is the ONLY writer that lowers it to
    // RebuildingViews; driveRebuild is the only one that raises it back to Ready.
    ViewReadiness                      readiness       = ViewReadiness::Ready;

    // A checkpoint delta encoded but not yet pushed (ring back-pressure).  Held
    // and retried next pass — the fold is NEVER re-run for already-drained
    // dirty rows, so no delta is ever lost or double-counted.
    std::vector<uint8_t>               pendingDelta;
    bool                               pendingValid    = false;

    ViewInstance(rune::view::CompiledViewPlan p, rune::view::RowLayout l,
                 rune::codec::Descriptor src, bool persist, int32_t part,
                 std::size_t indexSlots, std::size_t chunkBytes)
        : plan(std::move(p)),
          layout(std::move(l)),
          evolvingSrc(std::move(src)),
          vdc(evolvingSrc),                      // copy the already-moved member
          table(layout, indexSlots, chunkBytes), // layout member already set
          partition(part),
          persisted(persist) {}
};

class WorkerViewSet {
public:
    // `channel` is THIS worker's own ViewSnapshotChannel (per-worker SPSC — the
    // controller ruling).  `viewDataDir` is the per-view data directory used
    // only for naming / durability wiring (Task 19); it is stored, not opened.
    WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir);

    // Register a view's plan/layout/source-descriptor/persistence and index it
    // in the reactor `subscriptionIndex_` for each of the plan's source topics.
    // Per-partition ViewInstances are created LAZILY on the first driveFolds
    // for that partition (the stored template has everything lazy-create needs).
    void installView(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
                     rune::codec::Descriptor evolvingSrc, bool persisted);

    // Task 19: drain the control->worker install ring (channel_.install) and
    // install each compiled view plan ON THIS (the worker's OWN) thread — so a
    // view created at runtime folds live without any cross-thread write into the
    // worker-owned maps.  Decodes a rune::node::ViewPlanUpdateMsg per frame,
    // rebuilds the RowLayout from the output descriptor, and calls installView.
    // Returns the number of plans installed this pass (a progress count).
    //
    // Task 19b: a frame whose ViewPlanUpdateMsg.recovery is set registers the
    // plan template AND marks the view for RESTART recovery (recoveryViews_) —
    // its per-(view, partition) instances are then materialized by
    // materializeRecoveryInstancesFor (checkpoint load + RebuildingViews +
    // serve-guard), NOT lazily created Ready by the plain driveFolds path.
    int drainInstalls();

    // Task 19b: true iff at least one view here was installed for RESTART
    // recovery (recoveryViews_ non-empty).  The worker duty cycle gates the
    // extra recovery-materialize + driveRebuild work on this, so a broker with
    // NO durable views runs exactly as before (never gated, never charged).
    [[nodiscard]] bool hasRecovery() const noexcept { return !recoveryViews_.empty(); }

    // Task 19b: for an OWNED source (srcTopicId, partition), materialize — once,
    // cold — the (view, partition) instance of every recovery-marked view
    // interested in srcTopicId that is not yet hosted here, by loading THIS
    // (view, partition)'s checkpoint FILE from viewDataDir_ (split via
    // splitViewCheckpointRecords, replayed by installViewForRecovery).  A
    // missing/empty file is a valid COLD start (empty checkpoint → foldCursor 0
    // → full rebuild from the committed log).  `committedTail` is the source
    // partition's current LSO — the same bound driveFolds/driveRebuild use — so
    // a view already caught up is Ready at once.  No-op once the instance
    // exists (idempotent; the file read happens once per instance).  The worker
    // calls this BEFORE driveRebuild/driveFolds so a recovery view never gets a
    // fresh Ready instance that would bypass its checkpoint + serve-guard.
    void materializeRecoveryInstancesFor(int32_t srcTopicId, int32_t partition,
                                         int64_t committedTail);

    // Task 16: install a view FOR RECOVERY for ONE partition.  Registers the
    // plan template (like installView) AND eagerly creates the (view, partition)
    // instance, then loads its on-disk checkpoint into the partition's table via
    // rune::view::replay(base, deltasInOrder) — setting foldCursor to the
    // recovered O (the highest applied checkpoint offset; 0 for an empty/missing
    // checkpoint, since replay returns -1).  The instance is left:
    //   * Ready            if foldCursor >= committedTail (source already caught
    //                      up — e.g. a brand-new view over an empty source,
    //                      committedTail == 0), or
    //   * RebuildingViews  otherwise — the committed tail (foldCursor,
    //                      committedTail] must be folded by driveRebuild before
    //                      the instance serves.
    // `base` may be empty (no checkpoint); `deltasInOrder` are the differential
    // deltas after the base, oldest first.  Caller supplies both from disk (the
    // ViewSnapshotIoAgent / BrokerNode startup load); this core is agnostic to
    // where the bytes came from, which keeps it deterministically testable.
    void installViewForRecovery(rune::view::CompiledViewPlan plan,
                                rune::view::RowLayout layout,
                                rune::codec::Descriptor evolvingSrc, bool persisted,
                                int32_t partition, std::span<const uint8_t> base,
                                std::span<const std::span<const uint8_t>> deltasInOrder,
                                int64_t committedTail);

    // Status of a getRow probe — lets the serve path (Task 20) distinguish an
    // absent key (terminal NotFound) from a view still rebuilding (RETRIABLE
    // NotReady) from a served row (Ok).  getRow returns the row pointer; this is
    // the out-of-band reason when it returns nullptr.
    enum class RowStatus : uint8_t { Ok, NotFound, NotReady };

    // Readiness of the (view, partition) instance.  Returns Ready when no such
    // instance is hosted here (nothing to gate — consistent with allViewsReady
    // over an empty hosted set).
    [[nodiscard]] ViewReadiness readiness(int32_t viewTopicId, int32_t partition) const;

    // True iff EVERY hosted (view, partition) instance is Ready.  Empty hosted
    // set => true, so the serve/election gate is INERT when no view is
    // installed-for-recovery (never gates the no-views case closed).  noexcept:
    // a pure read over worker-owned state.
    [[nodiscard]] bool allViewsReady() const noexcept {
        for (const auto& kv : views_) {
            if (kv.second.readiness != ViewReadiness::Ready) return false;
        }
        return true;
    }

    // Drive the restart rebuild for every view interested in (srcTopicId,
    // partition) that is still RebuildingViews: fold the committed range
    // [foldCursor, committedTail) through the SAME driveFolds machinery (same
    // readRange idiom), then flip that instance RebuildingViews -> Ready once
    // foldCursor >= committedTail.  committedTail is the source partition's LSO
    // (passed in by the caller, exactly as driveFolds' `bound`).  Templated so
    // readRange inlines (no std::function).  Returns the progress count (records
    // folded + deltas handled this pass) so the idle strategy stays hot while a
    // rebuild is pending.  Bounded: each pass folds at most one committed range;
    // the loop is driven under a hard cap by the caller, never `while(!ready)`.
    template <class ReadRangeFn>
    int driveRebuild(int32_t srcTopicId, int32_t partition, ReadRangeFn&& readRange,
                     int64_t committedTail, int64_t nowNs) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return 0;
        int work = 0;
        for (int32_t viewTopicId : it->second) {
            auto vit = views_.find(instKey(viewTopicId, partition));
            if (vit == views_.end()) continue;  // not hosted-for-recovery here
            ViewInstance& inst = vit->second;
            if (inst.readiness == ViewReadiness::Ready) continue;  // already caught up
            work += driveView_(inst, readRange, committedTail);
            if (inst.foldCursor >= committedTail) inst.readiness = ViewReadiness::Ready;
        }
        return work;
    }

    // True when at least one installed view is interested in `srcTopicId`.  The
    // Worker uses this to skip the rc/bound computation for an owned partition
    // no view cares about.
    [[nodiscard]] bool interestedIn(int32_t srcTopicId) const {
        return subscriptionIndex_.find(srcTopicId) != subscriptionIndex_.end();
    }

    // Drive every view interested in (srcTopicId, partition) over the committed
    // range [foldCursor, bound) of THAT partition.  `readRange(from, to) ->
    // LogReadView` reads the committed bytes (the worker passes a readNoFault_
    // lambda over that partition's log).  No-op if no view is installed for
    // srcTopicId.  Templated so readRange inlines (no std::function on the hot
    // path).  Returns a progress count: records folded + checkpoint deltas
    // pushed/held this pass (0 == genuinely no work).
    template <class ReadRangeFn>
    int driveFolds(int32_t srcTopicId, int32_t partition, ReadRangeFn&& readRange,
                   int64_t bound, int64_t nowNs) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return 0;
        int work = 0;
        for (int32_t viewTopicId : it->second) {
            ViewInstance* inst = getOrCreate_(viewTopicId, partition);
            if (inst == nullptr) continue;  // defensive: template must exist
            work += driveView_(*inst, readRange, bound);
        }
        return work;
    }

    // Probe a view PARTITION's table for `key`.  Returns the row pointer
    // (nullptr if the (view, partition) or key is absent) and, on success, sets
    // outLayout to the view's RowLayout.  The caller (Task 20's GetViewRow
    // handler) knows the owning partition for the key.  const: a pure read of
    // worker-owned state.
    //
    // SERVE-GUARD (Task 16, Review-focus #5): when the hosted instance is NOT
    // Ready (still RebuildingViews/Recovering after a restart) getRow returns
    // nullptr with `*outStatus = NotReady` — a RETRIABLE not-ready that the
    // GetViewRow serve path surfaces so a probe never sees a half-built row.
    // `outStatus` is optional (nullptr ignores it), keeping every existing
    // 4-arg caller unchanged.
    const uint8_t* getRow(int32_t viewTopicId, int32_t partition, uint64_t key,
                          const rune::view::RowLayout*& outLayout,
                          RowStatus* outStatus = nullptr) const;

    // Task 20: the output-schema fingerprint of the view `viewTopicId` (the
    // fingerprint CreateView published for the view's typed output topic, stored
    // on the partition-independent plan template).  Returns false when no view
    // plan is installed here for that id — the GetViewRow serve path maps that to
    // UnknownView.  A pure read of worker-owned state (no allocation).
    [[nodiscard]] bool viewOutputFingerprint(int32_t viewTopicId, uint64_t& out) const {
        const auto it = templates_.find(viewTopicId);
        if (it == templates_.end()) return false;
        out = it->second.plan.outputFingerprint;
        return true;
    }

    // Number of fold passes that ran with a (view, partition) at/over its §15
    // maxRows ceiling — an OBSERVABLE capacity-exceeded metric only.  It does
    // NOT mean any fold was refused: crossing maxRows never blocks folding or
    // freezes foldCursor (the table grows on demand), it is surfaced here so a
    // capacity-bounded view's overflow is monitorable rather than a liveness
    // trap.
    [[nodiscard]] uint64_t capacityExceededCount() const noexcept { return capacityExceeded_; }

    // Count of live per-(view, partition) instances.
    [[nodiscard]] std::size_t instanceCount() const noexcept { return views_.size(); }
    // Count of installed view plans (partition-independent).
    [[nodiscard]] std::size_t viewCount() const noexcept { return templates_.size(); }

private:
    // The installed, partition-independent plan for a view — the template every
    // per-partition ViewInstance is lazily built from.
    struct ViewTemplate {
        rune::view::CompiledViewPlan plan;
        rune::view::RowLayout        layout;
        rune::codec::Descriptor      evolvingSrc;
        bool                         persisted;
    };

    // (viewTopicId, partition) -> dense composite map key.
    [[nodiscard]] static uint64_t instKey(int32_t viewTopicId, int32_t partition) noexcept {
        return (static_cast<uint64_t>(static_cast<uint32_t>(viewTopicId)) << 32) |
               static_cast<uint64_t>(static_cast<uint32_t>(partition));
    }

    // Look up the (viewTopicId, partition) instance, lazily creating it from the
    // installed template on first use.  nullptr only if no template is installed.
    ViewInstance* getOrCreate_(int32_t viewTopicId, int32_t partition);

    // Drive ONE (view, partition) instance.  Flushes any held pending delta
    // first (never drop); folds the committed range (ALWAYS advancing foldCursor
    // — maxRows is an observable event, not a fold-stop); and, for a persisted
    // view, drains + encodes + pushes a checkpoint delta.  Returns this pass's
    // progress count for the instance.
    template <class ReadRangeFn>
    int driveView_(ViewInstance& inst, ReadRangeFn&& readRange, int64_t bound) {
        int work = 0;
        // 1. A delta held from a back-pressured prior pass flushes first.  Held
        //    view work counts as progress (keep the worker hot so it retries the
        //    ring); if it still cannot be pushed, hold and do not fold further.
        if (inst.pendingValid) {
            ++work;
            if (!tryPushDelta_(inst)) return work;
        }
        // 2. Nothing new committed for this partition — caught up (checked before
        //    the capacity event so capacityExceeded_ counts folds that actually
        //    crossed the ceiling, not idle passes).
        if (inst.foldCursor >= bound) return work;
        // 3. §15 #24: maxRows is an OBSERVABLE capacity-exceeded EVENT, NOT a
        //    fold-stop.  Crossing it increments capacityExceeded_ (the metric) but
        //    MUST NOT refuse the fold or freeze foldCursor — the table grows on
        //    demand (chunk chaining).  A hard early-return here was a liveness
        //    trap: a view that recovered already at its ceiling would never
        //    advance foldCursor, so driveRebuild could never reach foldCursor >=
        //    committedTail and the instance (hence the whole node via
        //    allViewsReady) would stall in RebuildingViews forever.  So: observe
        //    and continue (0 == unbounded, never counted).
        if (inst.plan.sizing.maxRows != 0 &&
            inst.table.size() >= inst.plan.sizing.maxRows) {
            ++capacityExceeded_;
        }
        const rune::storage::LogReadView view = readRange(inst.foldCursor, bound);
        if (view.empty()) return work;

        int64_t cursor = inst.foldCursor;
        const rune::view::FoldStats st =
            rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view, bound, cursor);
        inst.foldCursor = cursor;
        work += static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedMalformed);

        // 4. Transient views stop here — no checkpoint handoff.
        if (!inst.persisted) return work;
        work += buildAndPushCheckpoint_(inst);
        return work;
    }

    // Drain the dirtied rows into a checkpoint delta (copying each row's bytes
    // into stable scratch — drainDirty's row pointer is only valid during the
    // callback), encode it, and push it; holds it on ring back-pressure.
    // Returns 1 if a delta was encoded this pass (changed rows), else 0.
    int buildAndPushCheckpoint_(ViewInstance& inst);

    // Push inst.pendingDelta onto channel_.deltas as
    // [viewTopicId i32][partition i32][delta bytes].  Returns false (and keeps
    // pendingValid) on back-pressure; true once committed.
    [[nodiscard]] bool tryPushDelta_(ViewInstance& inst);

    ViewSnapshotChannel& channel_;
    std::string          viewDataDir_;

    std::unordered_map<int32_t, ViewTemplate>         templates_;        // viewTopicId -> plan template
    std::unordered_map<uint64_t, ViewInstance>        views_;            // (viewTopicId,partition) -> instance
    std::unordered_map<int32_t, std::vector<int32_t>> subscriptionIndex_; // srcTopicId -> [viewTopicId]

    // Task 19b: viewTopicIds installed for RESTART recovery.  A view here has
    // its per-(view, partition) instances materialized from the on-disk
    // checkpoint via materializeRecoveryInstancesFor (RebuildingViews +
    // serve-guard) rather than lazily created Ready by driveFolds.  Plain
    // (Task-19 create-time) installs never touch this set — the no-recovery
    // path is unchanged.
    std::unordered_set<int32_t> recoveryViews_;

    // Reused checkpoint scratch (capacity persists; no per-batch allocation in
    // steady state).  rowScratch_ holds the copied row bytes; rowOffsets_ their
    // start offsets (SIZE_MAX for a delete); dirtyEntries_ the encodeDelta input.
    std::vector<rune::view::DirtyEntry> dirtyEntries_;
    std::vector<std::size_t>            rowOffsets_;
    std::vector<uint8_t>                rowScratch_;

    uint64_t capacityExceeded_ = 0;
};

} // namespace rune::node
