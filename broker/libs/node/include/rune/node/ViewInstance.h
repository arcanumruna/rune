#pragma once
// ViewInstance.h — the per-(view, partition) instance of a materialized view and its helper types (moved out of WorkerViewSet.h so
// VerdictSource.h and WorkerViewSet.h can both hold it).
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

#include <rune/node/NsSlotTable.h>

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

// Namespaces Phase 2 Task 12 (D26): a view's run state over its namespace's memory limit.  ORTHOGONAL to ViewReadiness
// (a paused view keeps its Ready / CatchingUp state so the resume needs no re-derivation) and a variant, never a flag:
//   ViewRunning                      - folds as before;
//   ViewPaused{OverLimit, offset}    - the namespace is over maxViewMemoryBytes: the view stopped folding at `offset`
//                                      (== its foldCursor) and reads answer ViewPaused (54) with that offset; on resume it
//                                      folds on from `offset`, so the table equals a recompute from the log.
enum class ViewPauseReason : uint8_t { OverLimit = 1 };
struct ViewRunning {};
struct ViewPaused { ViewPauseReason reason = ViewPauseReason::OverLimit; int64_t offset = 0; };
using ViewPauseState = std::variant<ViewRunning, ViewPaused>;

// The namespace a source partition (hence its views) lives in, as the owning worker's log tag.  Default = untagged
// (kNoNsSlot): the instance is not charged to any namespace (unit tests, transient sources, which carry no tag).
struct NsTag { uint16_t slot = kNoNsSlot; uint32_t nsId = 0; };

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

    // Task 12 (D26): the view-memory pause, the namespace this instance is charged to, and the bytes charged so far.
    ViewPauseState                     pause{ViewRunning{}};
    uint16_t                           nsSlot          = kNoNsSlot;
    uint32_t                           nsId            = 0;
    std::size_t                        memCharged      = 0;

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
    // Part 04 (P4-4): the checkpoint file was discarded with the tables; a reset frame goes out before any new delta.
    bool                               resetOwed       = false;

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

} // namespace rune::node
