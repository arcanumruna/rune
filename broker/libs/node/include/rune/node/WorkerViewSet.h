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

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
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
    const uint8_t* getRow(int32_t viewTopicId, int32_t partition, uint64_t key,
                          const rune::view::RowLayout*& outLayout) const;

    // Number of times a fold was refused because a (view, partition) was
    // already at its §10 maxRows hard ceiling (observable capacity signal).
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
    // first (never drop); folds the committed range; enforces the maxRows
    // ceiling; and, for a persisted view, drains + encodes + pushes a
    // checkpoint delta.  Returns this pass's progress count for the instance.
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
        //    the ceiling so capacityExceeded_ counts REFUSED folds, not idle passes).
        if (inst.foldCursor >= bound) return work;
        // 3. §10 hard ceiling: refuse to grow a view past maxRows (0 = unbounded).
        if (inst.plan.sizing.maxRows != 0 &&
            inst.table.size() >= inst.plan.sizing.maxRows) {
            ++capacityExceeded_;
            return work;
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

    // Reused checkpoint scratch (capacity persists; no per-batch allocation in
    // steady state).  rowScratch_ holds the copied row bytes; rowOffsets_ their
    // start offsets (SIZE_MAX for a delete); dirtyEntries_ the encodeDelta input.
    std::vector<rune::view::DirtyEntry> dirtyEntries_;
    std::vector<std::size_t>            rowOffsets_;
    std::vector<uint8_t>                rowScratch_;

    uint64_t capacityExceeded_ = 0;
};

} // namespace rune::node
