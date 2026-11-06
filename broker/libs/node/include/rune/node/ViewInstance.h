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
#include <rune/view/KeyedFold.h>
#include <rune/view/RecordFold.h>
#include <rune/view/UniqueClaims.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

#include <rune/node/NsSlotTable.h>
#include <rune/node/ViewCheckpointScan.h>
#include <rune/node/ViewSnapshotChannel.h>   // ViewDiscardReason

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
// and monotone (Recovering -> RebuildingViews -> (CatchingUp ->) Ready), except Ready -> CatchingUp on a new source
// leadership and, from either, back to RebuildingViews when beginDiscard_ throws the rows away.
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
// (kNoNsSlot): the instance is not charged to any namespace (unit tests).
struct NsTag { uint16_t slot = kNoNsSlot; uint32_t nsId = 0; };

// Task 19b / C7: the on-disk checkpoint file is split and classified by scanViewCheckpoint (ViewCheckpointScan.h): a torn
// trailing record is reported, never silently dropped.

// Phase 2a: the auxiliary tables of an aggregate (view, partition); Phase 2b
// Task 5b: plus the per-group Min/Max value blocks when the view has Min/Max.
struct AggInstance {
    rune::view::AggLayout layout;        // declared first: the tables are built from it
    rune::view::ViewTable contributors;  // contributorKey -> pre-image (retraction)
    rune::view::ViewTable groups;        // groupKey -> accumulators
    std::optional<rune::view::ViewTable> blocks;   // engaged iff layout.numBlockCols > 0
    // Keyed views only (layout.keyed): the group key -> gid table (row [gid u64], the view's key width) and the next unused gid.
    std::optional<rune::view::ViewTable> groupIds;
    uint64_t nextGroupId = 0;
    /// `keys` are the view table's: the auxiliary tables are keyed by 8-byte contributor and group values whatever the view key is.
    AggInstance(rune::view::AggLayout l, std::size_t indexSlots, std::size_t chunkBytes, rune::view::ViewTableKeys keys = {})
        : layout(std::move(l)),
          contributors(layout.contributorRow, indexSlots, chunkBytes, auxKeys(keys)),
          groups(layout.groupRow, indexSlots, chunkBytes, auxKeys(keys)) {
        if (layout.numBlockCols > 0) blocks.emplace(layout.blockRow, indexSlots, chunkBytes, auxKeys(keys));
        if (layout.keyed) {
            rune::view::RowLayout idRow;
            idRow.rowLen = 8;
            groupIds.emplace(std::move(idRow), indexSlots, chunkBytes, rune::view::ViewTableKeys{keys.keyWidth, 0, keys.hashSeed, keys.hashMaskForTest});
        }
    }
    static rune::view::ViewTableKeys auxKeys(rune::view::ViewTableKeys keys) noexcept {
        keys.keyWidth    = 8;
        keys.prefixWidth = 0;
        return keys;
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
    uint64_t transientSourceResets   = 0;   // the source ring was recreated, cut or restarted under the cursor, or a recovered checkpoint met a new ring (re-folded)
    uint64_t checkpointDiscards      = 0;   // beginDiscard_: rows thrown away (the checkpoint file is backed up first)
    uint64_t viewBackupFailures      = 0;   // a discard whose backup failed: the view stays NotReady for the rest of the boot
    uint64_t checkpointsAheadOfLog   = 0;   // #159: a recovered checkpoint above the source log end
    uint64_t sourceRecordsTrimmed    = 0;   // source records below the log start that this view never folded (retention, compaction)
    uint64_t skippedNoKey            = 0;   // keyed projection: a live record with an absent key component
    uint64_t keyConflicts            = 0;   // keyed projection: a unique key already owned by another source key
    uint64_t ownerMoves              = 0;   // keyed projection: a row moved to another key
    uint64_t uniqueConflicts         = 0;   // unique view gauge: the current number of extra claimants
    uint64_t uniquePromotions        = 0;   // unique view: an extra claimant became the owner of a key
    uint64_t scanRows                = 0;   // rows served by scanPage
    uint64_t scanPages               = 0;   // pages served by scanPage
};

// Where the persisted checkpoint of an instance stands in a discard (one explicit state, each holding only what it needs):
//   CheckpointLive           - the file and the tables agree; the instance folds and checkpoints as usual.
//   CheckpointDiscardOwed    - the rows were thrown away; the discard frame is not on the ring yet (ring full, or a held delta first).
//   CheckpointDiscardPending - the frame is with the IO agent; the instance waits for its ack (it folds nothing).
//   CheckpointDiscardFailed  - the backup failed (errno): nothing is rebuilt over the unsaved checkpoint, NotReady for this boot.
struct CheckpointLive {};
struct CheckpointDiscardOwed    { ViewDiscardReason reason; };
struct CheckpointDiscardPending { ViewDiscardReason reason; };
struct CheckpointDiscardFailed  { ViewDiscardReason reason; int32_t err; };
using CheckpointState = std::variant<CheckpointLive, CheckpointDiscardOwed, CheckpointDiscardPending, CheckpointDiscardFailed>;

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
//                                 first reconcile logs once (INFO), counts a source
//                                 reset, resets to logStart and keeps the rows.
//   CursorLive{incarnation, logGeneration}
//                               — cursor tracks THIS source ring: re-folded from
//                                 logStart when the ring was recreated (new
//                                 incarnation), from the cut when its content
//                                 changed below the cursor (generation + 1),
//                                 from logStart when the generation moved by
//                                 more (only the last cut is known);
//                                 EVICTED when it falls below logStart,
//                                 a source restart when above logEnd.
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
    rune::view::ViewTableKeys          tableKeys;   // the hash seed (and test mask) every table of this instance is built with
    rune::view::ViewTable              table;
    // Engaged iff isAggregateView(plan) (Phase 2a); empty for Replace views,
    // whose fold/checkpoint/recovery paths are unchanged.
    std::optional<AggInstance>         agg;
    // Engaged iff the plan is Keyed (Part 07): how a record derives its view key, the owner map (source key -> current view
    // key; only when the key is not the source key) and the fixed prospect the fold prepares each record into.
    std::optional<rune::view::KeyedFoldSpec> keyed;
    std::optional<rune::view::ViewTable>     owners;
    std::optional<rune::view::UniqueClaims>  claims;   // engaged iff the view is unique and owner-mapped
    rune::view::Prospect                     prospect;
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
    // path.  The full writer list (state graph: the ViewReadiness comment above):
    //   * defaults to Ready (a plain driveFolds instance);
    //   * set to RebuildingViews (or Ready when already caught up) at restart recovery by installViewForRecovery and
    //     loadRecoveryCheckpoint_ (foldCursor vs. committedTail);
    //   * beginDiscard_ is the one writer that lowers Ready or CatchingUp to RebuildingViews (the rows are thrown away);
    //   * lowered Ready -> CatchingUp on a new source leadership (N-C1) by driveView_ step 0b and flipVerdictMembers_;
    //   * raised to CatchingUp / Ready only by driveView_ (step 1b and after the fold), flipVerdictMembers_ and
    //     driveRebuild's caught-up block, all gated on foldCursor >= the bound and on leaderHwConfirmed (Ready only
    //     when the HW reached the leadership start offset).
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
    // Part 04 (P4-4) / view backups: where this instance's checkpoint file stands in a discard (see CheckpointState).  Anything but
    // CheckpointLive means the instance folds nothing, checkpoints nothing and serves NotReady.
    CheckpointState                    checkpoint{CheckpointLive{}};

    ViewInstance(rune::view::CompiledViewPlan p, rune::view::RowLayout l,
                 rune::codec::Descriptor src, bool persist, int32_t part,
                 std::size_t indexSlots, std::size_t chunkBytes, rune::view::ViewTableKeys keys = {})
        : plan(std::move(p)),
          layout(std::move(l)),
          evolvingSrc(std::move(src)),
          vdc(evolvingSrc),                      // copy the already-moved member
          tableKeys(keys),
          table(layout, indexSlots, chunkBytes, keys), // layout member already set
          partition(part),
          persisted(persist) {}
};

/// The tables a keyed instance adds to its view table (also a discard's rebuild): the owner map of an owner-mapped projection, the
/// claims of a unique one, and the prospect its fold prepares each record into.
inline void buildKeyedTables(ViewInstance& inst, std::size_t indexSlots, std::size_t chunkBytes) {
    const rune::view::KeyedFoldSpec& k = *inst.keyed;
    if (k.ownerMapped) {
        rune::view::RowLayout ownerRow;
        ownerRow.rowLen = k.keyWidth;
        inst.owners.emplace(std::move(ownerRow), indexSlots, chunkBytes,
                            rune::view::ViewTableKeys{k.wireKeyWidth, 0, inst.tableKeys.hashSeed, inst.tableKeys.hashMaskForTest});
        if (inst.plan.unique) {
            const auto key = std::find_if(inst.evolvingSrc.fields.begin(), inst.evolvingSrc.fields.end(), [&](const auto& f) { return f.id == inst.plan.keyFieldId[0]; });
            inst.claims.emplace(k.keyWidth, k.wireKeyWidth, inst.layout.rowLen, key->type, inst.tableKeys.hashSeed, indexSlots, chunkBytes);
        }
    }
    inst.prospect.reserve(inst.layout, inst.agg ? &inst.agg->layout : nullptr);
}

/// The instance's fold inputs: a keyed instance adds its spec, owner map and claims, or its group ids.
[[nodiscard]] inline rune::view::ProjectionFold projectionFoldOf(ViewInstance& i) noexcept {
    return {i.plan, i.layout, i.vdc, i.table, rune::view::viewKeyType(i.plan), i.keyed ? &*i.keyed : nullptr,
            i.owners ? &*i.owners : nullptr, i.claims ? &*i.claims : nullptr};
}
[[nodiscard]] inline rune::view::AggregateFold aggregateFoldOf(ViewInstance& i) noexcept {
    AggInstance& a = *i.agg;
    return {i.plan, i.layout, a.layout, i.vdc, i.table, a.contributors, a.groups, a.blocksOrNull(),
            i.keyed ? &*i.keyed : nullptr, i.keyed ? &*a.groupIds : nullptr, i.keyed ? &a.nextGroupId : nullptr};
}

/// True while the instance's checkpoint file and tables agree (no discard in flight).  An index compare.
[[nodiscard]] inline bool checkpointSettled(const ViewInstance& i) noexcept { return i.checkpoint.index() == 0; }

/// True when every table of the instance (output, and the aggregate's contributors, groups and blocks) holds no row.
[[nodiscard]] inline bool viewTablesEmpty(const ViewInstance& i) noexcept {
    if (i.table.size() != 0) return false;
    if (!i.agg) return true;
    return i.agg->contributors.size() == 0 && i.agg->groups.size() == 0 && (!i.agg->blocks || i.agg->blocks->size() == 0) &&
           (!i.agg->groupIds || i.agg->groupIds->size() == 0);
}

/// The log range a source partition holds: retention may have trimmed it below a cursor (logStart); logEnd bounds a checkpoint.
/// The default (0, INT64_MAX) says nothing, so a caller without a disk log (a transient source, a unit test) changes no behaviour.
struct SourceLogBounds {
    int64_t logStart = 0;
    int64_t logEnd   = INT64_MAX;
};

/// Retention trimmed the source below this instance's cursor.  An instance holding no rows starts at the log start (true);
/// one holding rows folded from records that no longer exist cannot be trusted: false = the caller discards it (BelowLogStart).
[[nodiscard]] inline bool startAtLogStart(ViewInstance& i, int64_t logStart) noexcept {
    if (!viewTablesEmpty(i)) return false;
    i.counters.sourceRecordsTrimmed += static_cast<uint64_t>(logStart - i.foldCursor);
    i.foldCursor   = logStart;
    i.lastEncodedO = logStart - 1;
    return true;
}

} // namespace rune::node
