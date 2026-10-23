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
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
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
//   CatchingUp       — (catalog history Task 22 fix rounds 1-2, C1/N-C1) folded under
//                      an OLDER source leadership, or below this leadership's
//                      confirmed committed prefix (HW < leadershipStartOffset(),
//                      the HW floor of a fresh leader can lag what the old leader
//                      had committed).  Entered unconditionally on a new
//                      leadership generation (Ready -> CatchingUp); left for Ready
//                      only when foldCursor >= bound AND HW >= the start offset.
//                      Not restart recovery; a GetViewRow here is the retriable
//                      NotReady, and the state does NOT gate election eligibility
//                      (allViewsReady ignores it).
//   Ready            — foldCursor >= committedTail; the table is fully caught up
//                      and may be served.
//
// The machine is LOCAL-ONLY: it reaches Ready purely from this node's own
// checkpoint + a replay of its own committed source log — no cluster/peer
// dependency, no cross-thread coordination.  It is bounded by the checkpoint
// cadence (each rebuild pass folds a bounded committed range), deadlock-free,
// and monotone (Recovering -> RebuildingViews -> (CatchingUp ->) Ready), except Ready -> CatchingUp on a new source leadership.
enum class ViewReadiness : uint8_t { Recovering, RebuildingViews, CatchingUp, Ready };

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

// Phase 2a: the auxiliary tables of an aggregate (view, partition); Phase 2b
// Task 5b: plus the per-group Min/Max value blocks when the view has Min/Max.
struct AggInstance {
    rune::view::AggLayout layout;        // declared first: the tables are built from it
    rune::view::ViewTable contributors;  // contributorKey -> pre-image (retraction)
    rune::view::ViewTable groups;        // groupKey -> accumulators
    std::optional<rune::view::ViewTable> blocks;   // engaged iff layout.numBlockCols > 0
    AggInstance(rune::view::AggLayout l, std::size_t indexSlots, std::size_t chunkBytes)
        : layout(std::move(l)),
          contributors(layout.contributorRow, indexSlots, chunkBytes),
          groups(layout.groupRow, indexSlots, chunkBytes) {
        if (layout.numBlockCols > 0) blocks.emplace(layout.blockRow, indexSlots, chunkBytes);
    }
    rune::view::ViewTable* blocksOrNull() noexcept { return blocks ? &*blocks : nullptr; }
};

// Per-(view, partition) fold-anomaly counters, accumulated from every pass's
// rune::view::FoldStats (cumulative since the instance was created; reset on
// restart).  Plain worker-owned integers — no allocation, no atomics; read on
// the owning worker thread via WorkerViewSet::foldCounters().
struct ViewFoldCounters {
    uint64_t skippedMalformed = 0;   // wire key not exactly the view key width
    uint64_t skippedAborted   = 0;   // #537: data batches of aborted transactions (never folded)
    uint64_t skippedNoGroup   = 0;   // aggregate: live record with a NULL group-by value
    uint64_t aggOverflow      = 0;   // aggregate: Sum emitted NULL (did not fit its type)
    uint64_t aggNonFinite     = 0;   // aggregate: NaN/+-Inf Float input treated as NULL
    uint64_t aggInconsistent  = 0;   // aggregate: retraction found missing/zero state
    uint64_t aggRescans       = 0;   // aggregate (Phase 2b): Min/Max value-block rescans (retracted extreme)
    uint64_t transientRecordsEvicted = 0;   // transient ring dropped records the view had not folded
    uint64_t transientSourceResets   = 0;   // E2: the source ring was recreated / cut under a live cursor (re-folded)
};

// The live [logStart, logEnd) of a TRANSIENT (InMemoryLog) source partition,
// handed to the per-instance fold so it can reconcile its cursor (Phase 2b E),
// plus (Phase 2b E2) the ring's IDENTITY: the owning replica's registry
// incarnation (a new replica + ring for the same partition on this node gets a
// new one) and its logGeneration with the offset the last content change cut at
// (PartitionReplica::lastTruncationOffset — a tail truncation or a follower ring
// reset).  Defaults: an unregistered (0) generation-0 ring.
struct TransientBounds {
    int64_t  logStart      = 0;
    int64_t  logEnd        = 0;
    uint32_t incarnation   = 0;
    uint64_t logGeneration = 0;
    int64_t  cutOffset     = 0;
};

// Where an instance's foldCursor came from (only consulted for a transient
// source; a disk source never reads it).  One explicit state, each holding only
// its variables (CLAUDE.md rule 3), one path each:
//   CursorFresh                 — created empty; the first transient reconcile
//                                 starts it at the source's logStart (records
//                                 evicted BEFORE the view existed are not
//                                 "lost" — never counted).
//   CursorRecoveredUnreconciled — cursor restored from a checkpoint taken against
//                                 an EARLIER incarnation of the source.  A
//                                 transient source restarts empty and reuses
//                                 offsets, so that cursor is meaningless: the
//                                 first reconcile warns once, resets to logStart
//                                 and keeps the rows.
//   CursorLive{incarnation, logGeneration}
//                               — cursor tracks THIS source ring: re-folded from
//                                 logStart when the ring was recreated (new
//                                 incarnation), from the cut when its content
//                                 changed below the cursor (generation + 1),
//                                 from logStart when the generation moved by
//                                 more (only the last cut is known);
//                                 EVICTED when it falls below logStart,
//                                 invariant breach when above logEnd.
struct CursorFresh {};
struct CursorRecoveredUnreconciled {};
struct CursorLive {
    uint32_t incarnation   = 0;
    uint64_t logGeneration = 0;
};
using TransientCursor = std::variant<CursorFresh, CursorRecoveredUnreconciled, CursorLive>;

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
    // Engaged iff isAggregateView(plan) (Phase 2a); empty for Replace views,
    // whose fold/checkpoint/recovery paths are unchanged.
    std::optional<AggInstance>         agg;
    int32_t                            partition       = 0;
    int64_t                            foldCursor      = 0;
    TransientCursor                    transientCursor{CursorFresh{}};
    // The offset O of the most recently ENCODED checkpoint delta.  This is the
    // attempted/encoded offset, NOT a durable one — durability is reported
    // separately by the view-snapshot IO agent's `done` ring (consumed in
    // Task 19).  Never mistake it for a durable/recoverable offset.
    int64_t                            lastEncodedO    = -1;
    bool                               persisted       = true;
    // PERSISTED instances only: the most source records one fold pass may cover
    // (bound' = min(bound, foldCursor + maxRecordsPerPass)), derived at creation
    // from the delta ring's max message length so the pass's checkpoint delta
    // ALWAYS fits one ring message (an oversized claim throws — it is not
    // back-pressure).  0 == uncapped (transient: never checkpoints).
    int64_t                            maxRecordsPerPass = 0;

    // Task 16 restart readiness of THIS (view, partition).  Defaults to Ready:
    // an instance created on the normal (non-recovery) driveFolds path is
    // serve-ready immediately, so the serve-guard is INERT for every existing
    // path.  installViewForRecovery is the ONLY writer that lowers it to
    // RebuildingViews; driveRebuild is the only one that raises it back to Ready.
    ViewReadiness                      readiness       = ViewReadiness::Ready;

    // C1 (Task 22 fix round 1): the source replica's leadershipGeneration() this
    // table is caught up under; 0 = never driven by a leader.  A read is served
    // only while it equals the replica's CURRENT generation (an ex-leader's frozen
    // table, or a new leader's not-yet-folded one, is never served).
    uint64_t                           foldLeadership  = 0;

    // Cumulative fold-anomaly counters (see ViewFoldCounters).
    ViewFoldCounters                   counters;

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
    // `underRecovery` (whole-branch review Bug A): true iff this view's
    // template was installed for RESTART recovery. It is latched on the
    // TEMPLATE (never reset by pruneRecoveryIfAllReady_'s recoveryViews_
    // erase), so getOrCreate_ can keep loading a lazily-created instance's
    // on-disk checkpoint even after SOME other partition of this view already
    // pruned the view out of recoveryViews_ in the very same driveViews_ pass.
    // Defaults false — every existing (non-recovery) call site is unaffected.
    void installView(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
                     rune::codec::Descriptor evolvingSrc, bool persisted,
                     bool underRecovery = false);

    // Catalog history Task 8: a DropView / AlterView retired the view
    // `viewTopicId` — drop its plan template, every (view, partition) instance
    // (its table, aggregate state, pending delta and dirty tracking), its
    // reactor-index entries and its recovery mark, on THIS (the worker's own)
    // thread.  From now on nothing folds into it and getRow answers NotFound
    // (no instance).  Idempotent: an unknown view is a no-op (returns false).
    bool uninstallView(int32_t viewTopicId);

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
            // CatchingUp (leader-local re-fold) never gates election eligibility.
            if (kv.second.readiness == ViewReadiness::Recovering ||
                kv.second.readiness == ViewReadiness::RebuildingViews) return false;
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
                     int64_t committedTail, const rune::view::AbortedTxnFilter& aborted, int64_t nowNs,
                     const TransientBounds* transientSrc = nullptr, uint64_t leaderGen = 0,
                     bool leaderHwConfirmed = true) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return 0;
        int work = 0;
        for (int32_t viewTopicId : it->second) {
            auto vit = views_.find(instKey(viewTopicId, partition));
            if (vit == views_.end()) continue;  // not hosted-for-recovery here
            ViewInstance& inst = vit->second;
            if (inst.readiness == ViewReadiness::Ready) continue;  // already caught up
            work += driveView_(inst, readRange, committedTail, aborted, transientSrc, leaderGen, leaderHwConfirmed);
            if (inst.foldCursor >= committedTail) {
                // N-C1: Ready only once the leader's HW reached its leadership start offset; else CatchingUp
                // (never left RebuildingViews: that state gates elections).
                inst.readiness = leaderHwConfirmed ? ViewReadiness::Ready : ViewReadiness::CatchingUp;
                // Task 19b: once EVERY hosted instance of this view is Ready, drop
                // it from recoveryViews_ so hasRecovery() latches back to false —
                // otherwise driveViews_ would pay the recovery-materialize +
                // driveRebuild lookups forever, long after only steady-state
                // folding remains.  Runs only on a flip (cold), never per pass.
                pruneRecoveryIfAllReady_(viewTopicId);
            }
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
                   int64_t bound, const rune::view::AbortedTxnFilter& aborted, int64_t nowNs,
                   const TransientBounds* transientSrc = nullptr, uint64_t leaderGen = 0,
                     bool leaderHwConfirmed = true) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return 0;
        int work = 0;
        for (int32_t viewTopicId : it->second) {
            // `bound` doubles as this pass's committedTail for a lazily-created
            // recovery instance (Bug A) — it is the SAME source-partition LSO
            // driveRebuild/materializeRecoveryInstancesFor already use.
            ViewInstance* inst = getOrCreate_(viewTopicId, partition, bound);
            if (inst == nullptr) continue;  // defensive: template must exist
            work += driveView_(*inst, readRange, bound, aborted, transientSrc, leaderGen, leaderHwConfirmed);
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
    /// C1: the view's first source topic (plan.sourceTopicId[0]), or -1 when no
    /// plan is installed here.  Pure read of worker-owned state.
    [[nodiscard]] int32_t viewSourceTopic(int32_t viewTopicId) const noexcept {
        const auto it = templates_.find(viewTopicId);
        if (it == templates_.end() || it->second.plan.sourceTopicId.empty()) return -1;
        return it->second.plan.sourceTopicId[0];
    }
    /// C1: the source leadership generation the (view, partition) table is caught
    /// up under; nullopt when no instance is hosted (never folded here).
    [[nodiscard]] std::optional<uint64_t> instanceLeadership(int32_t viewTopicId,
                                                             int32_t partition) const noexcept {
        const auto it = views_.find(instKey(viewTopicId, partition));
        if (it == views_.end()) return std::nullopt;
        return it->second.foldLeadership;
    }
    /// The serve path's view of one hosted instance (one lookup): the leadership it is caught up under and how far it
    /// folded (issue #82, the read-your-writes barrier).  nullptr leadership = not hosted.
    struct InstanceServeState { uint64_t leadership = 0; int64_t foldCursor = 0; bool hosted = false; bool ready = false; };
    [[nodiscard]] InstanceServeState instanceServeState(int32_t viewTopicId, int32_t partition) const noexcept {
        const auto it = views_.find(instKey(viewTopicId, partition));
        if (it == views_.end()) return {};
        return InstanceServeState{it->second.foldLeadership, it->second.foldCursor, true,
                                  it->second.readiness == ViewReadiness::Ready};
    }

    /// VLS B.3: is `topicId` a view this worker has the plan of -- i.e. a view SINK id?  Pure read of worker-owned
    /// state (the plan install ring carries every view id to every worker).
    [[nodiscard]] bool isViewSink(int32_t topicId) const noexcept { return templates_.find(topicId) != templates_.end(); }
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

    // Cumulative fold-anomaly counters of the (view, partition) instance, or
    // nullptr when no such instance is hosted here.  A pure read of
    // worker-owned state (no allocation).
    [[nodiscard]] const ViewFoldCounters* foldCounters(int32_t viewTopicId,
                                                       int32_t partition) const {
        auto it = views_.find(instKey(viewTopicId, partition));
        return it == views_.end() ? nullptr : &it->second.counters;
    }

    // Count of live per-(view, partition) instances.
    [[nodiscard]] std::size_t instanceCount() const noexcept { return views_.size(); }
    // Count of installed view plans (partition-independent).
    [[nodiscard]] std::size_t viewCount() const noexcept { return templates_.size(); }

    // Observability/test hook: pending dirty-tracking candidates (dirty rids +
    // pending deletes) across every table of the (view, partition) instance (0
    // if not hosted).  A transient instance must report 0 after each pass (its
    // dirty state is discarded), as must any instance right after recovery.
    [[nodiscard]] std::size_t dirtyBacklog(int32_t viewTopicId, int32_t partition) const {
        auto it = views_.find(instKey(viewTopicId, partition));
        if (it == views_.end()) return 0;
        const ViewInstance& inst = it->second;
        auto pending = [](const rune::view::ViewTable& t) {
            return t.dirtyRids().size() + t.pendingDeleteCount();
        };
        std::size_t n = pending(inst.table);
        if (inst.agg) {
            n += pending(inst.agg->contributors) + pending(inst.agg->groups);
            if (inst.agg->blocks) n += pending(*inst.agg->blocks);
        }
        return n;
    }

private:
    // The installed, partition-independent plan for a view — the template every
    // per-partition ViewInstance is lazily built from.
    struct ViewTemplate {
        rune::view::CompiledViewPlan plan;
        rune::view::RowLayout        layout;
        rune::codec::Descriptor      evolvingSrc;
        bool                         persisted;
        // Whole-branch review Bug A: true iff this view is under RESTART
        // recovery. Set true by installViewForRecovery / a recovery-flagged
        // drainInstalls() frame; read by getOrCreate_ so a lazily-created
        // instance (even one created AFTER recoveryViews_ already pruned this
        // viewTopicId) still loads its own checkpoint + starts NotReady,
        // instead of defaulting to a half-built Ready instance. Intentionally
        // NEVER cleared back to false (see getOrCreate_'s doc comment) — a
        // re-recovered Ready partition reloading its checkpoint once more is
        // benign; prematurely clearing it is not.
        bool                         underRecovery = false;
    };

    // (viewTopicId, partition) -> dense composite map key.
    [[nodiscard]] static uint64_t instKey(int32_t viewTopicId, int32_t partition) noexcept {
        return (static_cast<uint64_t>(static_cast<uint32_t>(viewTopicId)) << 32) |
               static_cast<uint64_t>(static_cast<uint32_t>(partition));
    }

    // Look up the (viewTopicId, partition) instance, lazily creating it from the
    // installed template on first use.  nullptr only if no template is installed.
    //
    // Whole-branch review Bug A fix: if the template's underRecovery is true,
    // a freshly-created instance does NOT default to Ready — it loads its own
    // on-disk checkpoint (same file materializeRecoveryInstancesFor reads) and
    // sets its readiness from ITS OWN foldCursor vs. `committedTail`, exactly
    // as a pre-materialized recovery instance would. This makes correctness
    // independent of recoveryViews_'s prune timing: within one driveViews_
    // pass over a worker's owned partitions of the SAME recovery view,
    // partition 0 reaching Ready can prune the view out of recoveryViews_
    // (hasRecovery() goes false) before partition 1's instance has even been
    // created — the underRecovery latch on the TEMPLATE (never cleared, see
    // ViewTemplate's doc comment) still gates this path for partition 1.
    // `committedTail` is unused when underRecovery is false (every existing
    // non-recovery call site is unaffected — callers just pass their `bound`).
    ViewInstance* getOrCreate_(int32_t viewTopicId, int32_t partition, int64_t committedTail);

    // Whole-branch review Bug A: load (view=viewTopicId, partition)'s on-disk
    // checkpoint file from viewDataDir_ (same path/format
    // materializeRecoveryInstancesFor uses) into `inst.table`, set foldCursor
    // to the recovered offset, and set readiness RebuildingViews/Ready from
    // foldCursor vs. committedTail. Used by getOrCreate_ for a lazily-created
    // instance of a still-underRecovery template.
    void loadRecoveryCheckpoint_(ViewInstance& inst, int32_t viewTopicId, int32_t partition,
                                 int64_t committedTail);

    // Task 19b: if every hosted (viewTopicId, *) instance is Ready, erase
    // viewTopicId from recoveryViews_ (so hasRecovery() returns to false once a
    // view's restart recovery is complete).  No-op if the view is not a recovery
    // view, or any hosted instance of it is still rebuilding.
    void pruneRecoveryIfAllReady_(int32_t viewTopicId);

    // Drive ONE (view, partition) instance.  Flushes any held pending delta
    // first (never drop); folds the committed range (ALWAYS advancing foldCursor
    // — maxRows is an observable event, not a fold-stop); and, for a persisted
    // view, drains + encodes + pushes a checkpoint delta.  Returns this pass's
    // progress count for the instance.
    template <class ReadRangeFn>
    int driveView_(ViewInstance& inst, ReadRangeFn&& readRange, int64_t bound,
                   const rune::view::AbortedTxnFilter& aborted,
                   const TransientBounds* transientSrc, uint64_t leaderGen, bool leaderHwConfirmed) {
        int work = 0;
        // 0. Phase 2b E: a TRANSIENT source — reconcile THIS instance's cursor
        //    against the source's live range (no extra hash lookups: the caller
        //    already holds the instance).  Disk sources pass nullptr.
        if (transientSrc != nullptr) work += reconcileTransient_(inst, *transientSrc);
        // 0b. C1 (Task 22 fix round 1): a NEW leadership of the source partition —
        //     this table was folded under an older one (or never).  Until it has
        //     folded the committed tail under THIS leadership it is CatchingUp
        //     (NotReady to readers); the two Ready flips below return it to Ready.
        if (inst.foldLeadership != leaderGen) {
            inst.foldLeadership = leaderGen;
            if (inst.readiness == ViewReadiness::Ready) inst.readiness = ViewReadiness::CatchingUp;   // N-C1: unconditional
        }
        // 1. A delta held from a back-pressured prior pass flushes first.  Held
        //    view work counts as progress (keep the worker hot so it retries the
        //    ring); if it still cannot be pushed, hold and do not fold further.
        if (inst.pendingValid) {
            ++work;
            if (!tryPushDelta_(inst)) return work;
        }
        // 1b. Bug A (whole-branch review) robustness: flip a not-yet-Ready
        //     instance to Ready the moment its OWN foldCursor has already
        //     caught up to `bound` — checked here, BEFORE the "nothing new"
        //     early return below, so an instance that reached `bound` on an
        //     EARLIER pass (and was never flipped because that pass's driver
        //     was driveFolds, not driveRebuild — e.g. a getOrCreate_ lazy
        //     recovery instance created after hasRecovery() had already
        //     latched false) still reaches Ready here.  This makes the
        //     Recovering/RebuildingViews -> Ready transition correct
        //     independent of which caller (driveFolds vs. driveRebuild) drives
        //     a given pass, and independent of recoveryViews_'s prune timing.
        if (inst.readiness != ViewReadiness::Ready && inst.foldCursor >= bound) {
            inst.readiness = leaderHwConfirmed ? ViewReadiness::Ready : ViewReadiness::CatchingUp;
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
        // Persisted: cap the pass so its delta fits one ring message (offsets
        // are dense; control batches only make the cap conservative).  The
        // fold resumes mid-batch correctly (records below foldCursor skipped).
        const int64_t passBound =
            inst.maxRecordsPerPass > 0
                ? std::min(bound, inst.foldCursor + inst.maxRecordsPerPass)
                : bound;
        const rune::storage::LogReadView view = readRange(inst.foldCursor, passBound);
        if (view.empty()) return work;

        int64_t cursor = inst.foldCursor;
        const rune::view::FoldStats st =
            inst.agg ? rune::view::foldAggregateBatch(inst.plan, inst.layout, inst.agg->layout,
                                                      inst.vdc, inst.table, inst.agg->contributors,
                                                      inst.agg->groups, inst.agg->blocksOrNull(), view,
                                                      passBound, cursor, aborted)
                     : rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view,
                                             passBound, cursor, aborted);
        inst.foldCursor = cursor;
        work += static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted +
                                 st.skippedMalformed + st.skippedNoGroup);
        inst.counters.skippedAborted   += st.skippedAborted;
        inst.counters.skippedMalformed += st.skippedMalformed;
        inst.counters.skippedNoGroup   += st.skippedNoGroup;
        inst.counters.aggOverflow      += st.aggOverflow;
        inst.counters.aggNonFinite     += st.aggNonFinite;
        inst.counters.aggInconsistent  += st.aggInconsistent;
        inst.counters.aggRescans       += st.aggRescans;

        // Bug A: this pass's OWN fold may be what just caught the instance up
        // to `bound` — flip it here too (see 1b's doc comment for why this
        // cannot be left to driveRebuild alone).
        if (inst.readiness != ViewReadiness::Ready && inst.foldCursor >= bound) {
            inst.readiness = leaderHwConfirmed ? ViewReadiness::Ready : ViewReadiness::CatchingUp;
        }

        // 4. Transient views stop here — no checkpoint handoff.  Their dirty
        //    tracking is never drained into a delta, so DISCARD it every pass
        //    (every table, aux tables included): otherwise each erase grows the
        //    deleted list and each erase+reinsert re-pushes a rid onto the dirty
        //    list without bound (zero-alloc steady state).  O(rows dirtied this
        //    pass), no allocation.
        if (!inst.persisted) {
            discardDirty_(inst);
            return work;
        }
        work += buildAndPushCheckpoint_(inst);
        return work;
    }

    // Phase 2b E: the one cursor-state transition for an instance over a transient
    // source (see TransientCursor).  Returns 1 if the cursor moved (counts as work),
    // else 0.  O(1), no allocation.
    static int reconcileTransient_(ViewInstance& inst, const TransientBounds& tb);

    // Drain the dirtied rows into a checkpoint delta (copying each row's bytes
    // into stable scratch — drainDirty's row pointer is only valid during the
    // callback), encode it, and push it; holds it on ring back-pressure.
    // Returns 1 if a delta was encoded this pass (changed rows), else 0.
    int buildAndPushCheckpoint_(ViewInstance& inst);

    // Clear the dirty/deleted tracking of every table of an instance: every
    // pass for a transient instance (nothing will ever encode it), and once
    // after a recovery replay (the restored rows ARE the durable checkpoint —
    // re-sending them would be a needless, possibly oversized delta).  No
    // allocation.
    static void discardDirty_(ViewInstance& inst) noexcept {
        auto drop = [](uint32_t, const uint8_t*, bool, uint64_t) noexcept {};
        inst.table.drainDirty(drop);
        if (inst.agg) {
            inst.agg->contributors.drainDirty(drop);
            inst.agg->groups.drainDirty(drop);
            if (inst.agg->blocks) inst.agg->blocks->drainDirty(drop);
        }
    }

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
