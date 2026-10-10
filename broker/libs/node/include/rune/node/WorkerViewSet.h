#pragma once
// WorkerViewSet.h — Task 15: per-partition-worker view-fold driver.
//
// A WorkerViewSet is owned by exactly ONE Worker (shared-nothing: plain
// members, no atomics, no locks, no thread_local).  It holds this worker's
// installed materialized views (`views_`, keyed by viewTopicId), a reactor
// index from a source topic to the views interested in it
// (`subscriptionIndex_`), and the per-view in-memory ViewTable + fold cursor.
//
// The Worker drives it from `doWork`, right AFTER `rcAdvanceAll_()`, once per
// owned source partition whose committed range may have advanced.  For each
// interested view it reads `[foldCursor, bound)` through a caller-supplied
// `readRange` (the worker's existing readNoFault_ into its push scratch),
// folds that committed range into the view's table via rune::view::foldBatch,
// and — for a PERSISTED view only — drains the dirtied rows into a Task-12
// differential checkpoint delta and pushes it onto this worker's OWN
// ViewSnapshotChannel.deltas ring (SPSC; the per-node view-snapshot IO agent
// drains the set of per-worker channels — Task 19).  A TRANSIENT view folds +
// serves rows but skips the checkpoint handoff entirely.
//
// Hot path (driveFolds): zero steady-state allocation beyond the deliberate,
// reused scratch buffers (dirty-entry list, row-byte scratch, encode buffer)
// and the deliberate per-checkpoint ring push.  A delta that cannot be pushed
// because the ring is full is HELD (never dropped) and retried next pass
// before any further folding for that view.

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

// One installed materialized view on this worker.  MOVABLE (lives by value in
// `views_`): it only declares a converting constructor, so the implicit move
// ctor/assignment are still generated (ViewTable and VersionDescriptorCache
// are themselves movable — only vectors + an unordered_map, no user-declared
// special members).  COPY is never needed and never used.
struct ViewInstance {
    rune::view::CompiledViewPlan     plan;
    rune::view::RowLayout            layout;
    rune::codec::Descriptor          evolvingSrc;
    rune::view::VersionDescriptorCache vdc;
    rune::view::ViewTable            table;
    int64_t                          foldCursor      = 0;
    int64_t                          lastCheckpointO = -1;
    bool                             persisted       = true;

    // A checkpoint delta encoded but not yet pushed (ring back-pressure).  Held
    // and retried next pass — the fold is NEVER re-run for already-drained
    // dirty rows, so no delta is ever lost or double-counted.
    std::vector<uint8_t>             pendingDelta;
    bool                             pendingValid    = false;
    int32_t                          pendingPartition = 0;

    ViewInstance(rune::view::CompiledViewPlan p, rune::view::RowLayout l,
                 rune::codec::Descriptor src, bool persist,
                 std::size_t indexSlots, std::size_t chunkBytes)
        : plan(std::move(p)),
          layout(std::move(l)),
          evolvingSrc(std::move(src)),
          vdc(evolvingSrc),                      // copy the already-moved member
          table(layout, indexSlots, chunkBytes), // layout member already set
          persisted(persist) {}
};

class WorkerViewSet {
public:
    // `channel` is THIS worker's own ViewSnapshotChannel (per-worker SPSC — the
    // controller ruling).  `viewDataDir` is the per-view data directory used
    // only for naming / durability wiring (Task 19); it is stored, not opened.
    WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir);

    // Construct a ViewInstance (ViewTable sized from plan.sizing) and register
    // it in `views_` + the reactor `subscriptionIndex_` for each of the plan's
    // source topics.  `persisted` false => a transient view (no checkpoints).
    void installView(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
                     rune::codec::Descriptor evolvingSrc, bool persisted);

    // Drive every view interested in (srcTopicId, partition) over the committed
    // range [foldCursor, bound).  `readRange(from, to) -> LogReadView` reads the
    // committed bytes (the worker passes a readNoFault_ lambda).  No-op if no
    // view is installed for srcTopicId.  Templated so readRange inlines (no
    // std::function on the hot path).
    template <class ReadRangeFn>
    void driveFolds(int32_t srcTopicId, int32_t partition, ReadRangeFn&& readRange,
                    int64_t bound, int64_t nowNs) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return;
        for (int32_t viewTopicId : it->second) {
            auto vit = views_.find(viewTopicId);
            if (vit == views_.end()) continue;
            driveView_(vit->second, partition, readRange, bound);
        }
    }

    // Probe a view's table for `key`.  Returns the row pointer (nullptr if the
    // view or key is absent) and, on success, sets outLayout to the view's
    // RowLayout.  const: a pure read of worker-owned state.
    const uint8_t* getRow(int32_t viewTopicId, uint64_t key,
                          const rune::view::RowLayout*& outLayout) const;

    // Number of times a fold was refused because a view was already at its
    // §10 maxRows hard ceiling (observable capacity-exceeded signal).
    [[nodiscard]] uint64_t capacityExceededCount() const noexcept { return capacityExceeded_; }

    [[nodiscard]] std::size_t viewCount() const noexcept { return views_.size(); }

private:
    // Drive ONE view instance.  Flushes any held pending delta first (never
    // drop); enforces the maxRows ceiling; folds the committed range; and, for
    // a persisted view, drains + encodes + pushes a checkpoint delta.
    template <class ReadRangeFn>
    void driveView_(ViewInstance& inst, int32_t partition, ReadRangeFn&& readRange,
                    int64_t bound) {
        // 1. A delta held from a back-pressured prior pass flushes first; if it
        //    still cannot be pushed, hold and do not fold further this pass.
        if (inst.pendingValid) {
            if (!tryPushDelta_(inst)) return;
        }
        // 2. §10 hard ceiling: refuse to grow a view past maxRows (0 = unbounded).
        if (inst.plan.sizing.maxRows != 0 &&
            inst.table.size() >= inst.plan.sizing.maxRows) {
            ++capacityExceeded_;
            return;
        }
        // 3. Nothing new committed for this view.
        if (inst.foldCursor >= bound) return;
        const rune::storage::LogReadView view = readRange(inst.foldCursor, bound);
        if (view.empty()) return;

        int64_t cursor = inst.foldCursor;
        rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view, bound, cursor);
        inst.foldCursor = cursor;

        // 4. Transient views stop here — no checkpoint handoff.
        if (!inst.persisted) return;
        buildAndPushCheckpoint_(inst, partition);
    }

    // Drain the dirtied rows into a checkpoint delta (copying each row's bytes
    // into stable scratch — drainDirty's row pointer is only valid during the
    // callback), encode it, and push it; holds it on ring back-pressure.
    void buildAndPushCheckpoint_(ViewInstance& inst, int32_t partition);

    // Push inst.pendingDelta onto channel_.deltas as
    // [viewTopicId i32][partition i32][delta bytes].  Returns false (and keeps
    // pendingValid) on back-pressure; true once committed.
    [[nodiscard]] bool tryPushDelta_(ViewInstance& inst);

    ViewSnapshotChannel& channel_;
    std::string          viewDataDir_;

    std::unordered_map<int32_t, ViewInstance>            views_;
    std::unordered_map<int32_t, std::vector<int32_t>>    subscriptionIndex_;

    // Reused checkpoint scratch (capacity persists; no per-batch allocation in
    // steady state).  rowScratch_ holds the copied row bytes; rowOffsets_ their
    // start offsets (SIZE_MAX for a delete); dirtyEntries_ the encodeDelta input.
    std::vector<rune::view::DirtyEntry> dirtyEntries_;
    std::vector<std::size_t>            rowOffsets_;
    std::vector<uint8_t>                rowScratch_;

    uint64_t capacityExceeded_ = 0;
};

} // namespace rune::node
