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

#include <memory>

#include <rune/protocol/client/DataMessages.h>
#include <rune/node/VerdictSource.h>
#include <rune/node/ViewInstance.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/NsSlotTable.h>

namespace rune::node {

/// The most row bytes (each row with its u32 length) one scanPage answers with.
inline constexpr uint32_t kMaxScanPageBytes = rune::protocol::client::ScanViewRowsResponse::kMaxPageBytes;
static_assert(rune::protocol::client::ScanViewRows::kTokenBytes == rune::view::kScanTokenBytes);

enum class CheckpointOrigin : uint8_t { None, Committed, CommittedAndLegacy, LegacyOnly };

struct RecoveredCheckpoint {
    std::vector<uint8_t> bytes;   // the committed file's bytes (empty for None and LegacyOnly)
    CheckpointOrigin     origin = CheckpointOrigin::None;
};

class WorkerViewSet {
public:
    // `channel` is THIS worker's own ViewSnapshotChannel (per-worker SPSC — the
    // controller ruling).  `viewDataDir` is the per-view data directory used
    // only for naming / durability wiring (Task 19); it is stored, not opened.
    /// `hashSeed` seeds every table of every instance (a table's key hash, so player-chosen keys cannot target a slot); the
    /// node draws a random one per worker, 0 reproduces the unseeded tables.
    WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir, uint64_t hashSeed = 0);

    [[nodiscard]] uint64_t hashSeedForTest() const noexcept { return hashSeed_; }
    /// Plans refused at install because they pin a Unicode version this broker does not have (P7-13).
    [[nodiscard]] uint64_t installRefusals() const noexcept { return installRefusals_; }
    /// Test seam: tables created afterwards AND their hashes with `mask` (0 makes every key collide).
    void setHashMaskForTest(uint64_t mask) noexcept { hashMask_ = mask; }
    [[nodiscard]] const ViewInstance* instanceForTest(int32_t viewTopicId, int32_t partition) const {
        const auto it = views_.find(instKey(viewTopicId, partition));
        return it == views_.end() ? nullptr : &it->second;
    }

    struct VerdictIdentity { rune::view::VerdictViewKind kind; uint64_t objectId; uint64_t fingerprint; };
    /// Test seam: how the installed view joins a judge's constraint set; nullopt for a view no judge drives.
    [[nodiscard]] std::optional<VerdictIdentity> verdictIdentityForTest(int32_t viewTopicId) const {
        const auto it = templates_.find(viewTopicId);
        if (it == templates_.end() || !it->second.isVerdict) return std::nullopt;
        return VerdictIdentity{it->second.verdictKind, it->second.verdictObjectId, it->second.verdictFingerprint};
    }

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

    /// Test seams: throw away one instance's rows exactly as recovery does, and read where its checkpoint stands.
    bool beginDiscardForTest(int32_t viewTopicId, int32_t partition, ViewDiscardReason reason) {
        const auto it = views_.find(instKey(viewTopicId, partition));
        if (it == views_.end()) return false;
        beginDiscard_(it->second, reason);
        return true;
    }
    [[nodiscard]] const CheckpointState* checkpointStateForTest(int32_t viewTopicId, int32_t partition) const {
        const auto it = views_.find(instKey(viewTopicId, partition));
        return it == views_.end() ? nullptr : &it->second.checkpoint;
    }

    // Drain the IO agent's `done` ring (channel_.done) on THIS thread, the ring's only consumer.  kDoneMsgType frames
    // (durable-offset acks) are ignored; a kDiscardDoneMsgType frame completes a discard (onDiscardDone_).  Without a
    // consumer the agent's pendingAcks_ would grow without bound once the 4 KiB ring filled.  Returns frames drained.
    int drainSnapshotAcks(int limit = 64);

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
                                         int64_t committedTail, const SourceLogBounds& b = {});

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
                                int64_t committedTail, std::size_t* appliedOut = nullptr);

    // Status of a getRow probe — lets the serve path (Task 20) distinguish an
    // absent key (terminal NotFound) from a view still rebuilding (RETRIABLE
    // NotReady) from a served row (Ok).  getRow returns the row pointer; this is
    // the out-of-band reason when it returns nullptr.
    enum class RowStatus : uint8_t { Ok, NotFound, NotReady, Paused, BadKey };

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
    // foldCursor >= committedTail (CatchingUp instead while the leader's HW has not reached its start offset, N-C1).
    // committedTail is the source partition's LSO (passed in by the caller, exactly as driveFolds' `bound`).  Templated so
    // readRange inlines (no std::function).  Returns the progress count (records
    // folded + deltas handled this pass) so the idle strategy stays hot while a
    // rebuild is pending.  Bounded: each pass folds at most one committed range;
    // the loop is driven under a hard cap by the caller, never `while(!ready)`.
    template <class ReadRangeFn>
    int driveRebuild(int32_t srcTopicId, int32_t partition, ReadRangeFn&& readRange,
                     int64_t committedTail, const rune::view::FoldSkip& skip, int64_t nowNs,
                     const TransientBounds* transientSrc = nullptr, uint64_t leaderGen = 0,
                     bool leaderHwConfirmed = true, NsTag ns = {}, PartitionWriteState* write = nullptr,
                     SourceLogBounds b = {}) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return 0;
        int work = 0;
        // A view of a verdict source rebuilds only up to what its judge has judged; the verdict members themselves are driven
        // by the judge (driveFolds), never here.
        int64_t rebuildTail = committedTail;
        rune::view::FoldSkip rskip = skip;
        if (write != nullptr) {
            rebuildTail = std::min(committedTail, write->verdictKnownThrough());
            rskip.rejected = &write->rejected;
        }
        for (int32_t viewTopicId : it->second) {
            if (isVerdictTemplate_(viewTopicId)) continue;
            auto vit = views_.find(instKey(viewTopicId, partition));
            if (vit == views_.end()) continue;  // not hosted-for-recovery here
            ViewInstance& inst = vit->second;
            adoptNs_(inst, ns);
            if (inst.readiness == ViewReadiness::Ready) continue;  // already caught up
            work += driveView_(inst, readRange, rebuildTail, rskip, transientSrc, leaderGen, leaderHwConfirmed, b);
            if (inst.foldCursor >= rebuildTail && checkpointSettled(inst)) {
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

    // The slowest foldCursor over the views interested in (srcTopicId, partition): a View ack may fire once this passes
    // its last offset.  INT64_MAX when no view is interested.  An instance that was never created counts 0, and with
    // `leadership != 0` so does one folded under another leadership (its table says nothing about this one).  `judged` is false on a memory-backed replica:
    // its verdict views are skipped (no judge ever folds them).
    [[nodiscard]] int64_t minFoldCursor(int32_t srcTopicId, int32_t partition, uint64_t leadership = 0, bool judged = true) const noexcept {
        const auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return INT64_MAX;
        int64_t min = INT64_MAX;
        for (const int32_t viewTopicId : it->second) {
            if (const auto tit = templates_.find(viewTopicId); tit != templates_.end() && tit->second.retiring) continue;   // a dropped verdict view is judged to its drop, never waited for
            if (!judged && isVerdictTemplate_(viewTopicId)) continue;   // #808: a memory-backed replica never hosts a judge (SD9), so its verdict views are never folded and never waited for
            const auto vit = views_.find(instKey(viewTopicId, partition));
            const int64_t cursor = vit == views_.end() || (leadership != 0 && vit->second.foldLeadership != leadership) ? 0 : vit->second.foldCursor;
            if (cursor < min) min = cursor;
        }
        return min;
    }
    /// Cold (#791 diagnostics, a View ack timeout): the view topic id that holds minFoldCursor() (same rules), or -1 when none does.
    [[nodiscard]] int32_t minFoldCursorViewForDiag(int32_t srcTopicId, int32_t partition, uint64_t leadership = 0, bool judged = true) const noexcept {
        const auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end()) return -1;
        int64_t min = INT64_MAX;
        int32_t holder = -1;
        for (const int32_t viewTopicId : it->second) {
            if (const auto tit = templates_.find(viewTopicId); tit != templates_.end() && tit->second.retiring) continue;
            if (!judged && isVerdictTemplate_(viewTopicId)) continue;
            const auto vit = views_.find(instKey(viewTopicId, partition));
            const int64_t cursor = vit == views_.end() || (leadership != 0 && vit->second.foldLeadership != leadership) ? 0 : vit->second.foldCursor;
            if (cursor < min) { min = cursor; holder = viewTopicId; }
        }
        return holder;
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
                   int64_t bound, const rune::view::FoldSkip& skip, int64_t nowNs,
                   const TransientBounds* transientSrc = nullptr, uint64_t leaderGen = 0,
                     bool leaderHwConfirmed = true, NsTag ns = {}, PartitionWriteState* write = nullptr, VerdictSource* judge = nullptr,
                   SourceLogBounds b = {}) {
        (void)nowNs;  // reserved for checkpoint cadence (Phase 1: every batch boundary)
        auto it = subscriptionIndex_.find(srcTopicId);
        if (it == subscriptionIndex_.end() && judge == nullptr) return 0;
        static const std::vector<int32_t> kNoViews;
        const std::vector<int32_t>& viewIds = it == subscriptionIndex_.end() ? kNoViews : it->second;
        int work = 0;
        // A verdict source: the judge runs first, and every other view folds only what it has judged, skipping what it rejected.
        int64_t viewBound = bound;
        rune::view::FoldSkip viewSkip = skip;
        if (write != nullptr) {
            viewSkip.rejected = &write->rejected;
            if (judge != nullptr) {
                work += syncConstraintSet_(srcTopicId, *judge, *write);
                if (judge->activationPending()) rebuildKeyIndex_(srcTopicId, partition, *judge, *write->spec, leaderGen);
                work += judge->driveJudge(readRange, bound, nowNs, *write, skip.aborted);
                viewBound = std::min(bound, judge->judgedThrough());
                work += flipVerdictMembers_(srcTopicId, partition, bound, leaderGen, leaderHwConfirmed, ns);
            } else {
                viewBound = std::min(bound, write->verdictKnownThrough());
            }
        }
        for (int32_t viewTopicId : viewIds) {
            if (isVerdictTemplate_(viewTopicId)) continue;
            // `bound` doubles as this pass's committedTail for a lazily-created
            // recovery instance (Bug A) — it is the SAME source-partition LSO
            // driveRebuild/materializeRecoveryInstancesFor already use.
            ViewInstance* inst = getOrCreate_(viewTopicId, partition, bound);
            if (inst == nullptr) continue;  // defensive: template must exist
            adoptNs_(*inst, ns);
            work += driveView_(*inst, readRange, viewBound, viewSkip, transientSrc, leaderGen, leaderHwConfirmed, b);
        }
        return work + finishRetired_();
    }

    /// The judge of a verdict source (nullptr = none).  Created by the first drive that needs one.
    [[nodiscard]] VerdictSource* verdictSource(int32_t srcTopicId, int32_t partition) const noexcept {
        const auto it = verdictSources_.find(instKey(srcTopicId, partition));
        return it == verdictSources_.end() ? nullptr : it->second.get();
    }
    /// Creates or erases the judge of (src, partition) as its verdict templates and logged timeline require, and registers its
    /// members.  nullptr when the partition is not a verdict source.  Cheap when nothing changed.
    VerdictSource* ensureVerdictSource(int32_t srcTopicId, int32_t partition, PartitionWriteState& write);
    /// The partition's write state is gone: its judge goes with it, and finishRetired_ erases the retiring views no judge holds any
    /// more (P9-6).  A judge not yet activated holds none, so a partition promoted in the same apply recovers its plan from its own
    /// type-20 record: that fallback is why the log keeps them.
    // Inline: Worker.h's rebuildRcSlots_ reaches it, and that header is built into libraries that do not link rune_node.
    void dropVerdictSource(int32_t srcTopicId, int32_t partition) {
        verdictSources_.erase(instKey(srcTopicId, partition));
        eraseKeyIndex_(instKey(srcTopicId, partition));
        if (const auto sit = subscriptionIndex_.find(srcTopicId); sit != subscriptionIndex_.end())
            for (const int32_t id : sit->second)
                if (isVerdictTemplate_(id) && templates_.at(id).retiring) retiredQueue_.push_back({id, partition});
    }
    [[nodiscard]] std::vector<uint64_t> verdictSourceKeysForTest() const {
        std::vector<uint64_t> keys;
        for (const auto& kv : verdictSources_) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end());
        return keys;
    }
    void setVerdictIntervalNs(int64_t ns) noexcept {
        verdictIntervalNs_ = ns;
        for (auto& kv : verdictSources_) kv.second->setVerdictIntervalNs(ns);
    }
    /// Cold: the source topic is gone; its judges and key indexes go with it.
    void dropSourceTopic(int32_t srcTopicId);
    /// Cold: a topic's write spec changed key.index, so syncConstraintSet_ must run again.
    void noteWriteSpecChange() noexcept { ++verdictEpoch_; }
    /// Test seam: this worker's catalog position is at least `through` (a cluster test cannot hold one node's catalog back).
    void raiseCatalogThroughForTest(int64_t through) noexcept { catalogThrough_ = std::max(catalogThrough_, through); }
    [[nodiscard]] int64_t catalogThroughForTest() const noexcept { return catalogThrough_; }
    /// A replacement plan of this object builds as a candidate in some judge.
    [[nodiscard]] bool candidateForTest(uint64_t viewObjectId) const noexcept {
        return std::any_of(verdictSources_.begin(), verdictSources_.end(), [&](const auto& kv) { return kv.second->candidateForTest(viewObjectId); });
    }
    [[nodiscard]] std::size_t retiringVerdictTemplatesForTest() const noexcept {
        return static_cast<std::size_t>(std::count_if(templates_.begin(), templates_.end(), [](const auto& kv) { return kv.second.isVerdict && kv.second.retiring; }));
    }
    /// Test seam: every judge, existing and later created, is held (nothing activates, replays or judges).
    void holdJudgesForTest(bool hold) noexcept {
        holdJudges_ = hold;
        for (auto& kv : verdictSources_) kv.second->holdJudgeForTest(hold);
    }
    /// The records one fold pass of (view, partition) may cover (0 = uncapped).  -1: not hosted.
    [[nodiscard]] int64_t maxRecordsPerPassForTest(int32_t viewTopicId, int32_t partition) const {
        const auto it = views_.find(instKey(viewTopicId, partition));
        return it == views_.end() ? -1 : it->second.maxRecordsPerPass;
    }
    [[nodiscard]] bool keyIndexContainsForTest(int32_t src, int32_t partition, std::string_view wireKey) const {
        const auto it = keyIndexes_.find(instKey(src, partition));
        return it != keyIndexes_.end() && it->second.table.get(rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wireKey.data()), wireKey.size()})) != nullptr;
    }
    [[nodiscard]] std::size_t keyIndexSizeForTest(int32_t src, int32_t partition) const {
        const auto it = keyIndexes_.find(instKey(src, partition));
        return it == keyIndexes_.end() ? 0 : it->second.table.size();
    }
    /// -1: the partition has no key index.
    [[nodiscard]] int64_t keyIndexFoldCursorForTest(int32_t src, int32_t partition) const {
        const auto it = keyIndexes_.find(instKey(src, partition));
        return it == keyIndexes_.end() ? -1 : it->second.foldCursor;
    }
    /// Test seam: a unique or check view as the install frame would register it.
    void installConstrainedViewForTest(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout, rune::codec::Descriptor src, bool persisted);
    /// Test seam: a TestReject verdict view (viewObjectId, rule) over the source of `plan`.
    void installTestVerdictViewForTest(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout, rune::codec::Descriptor src,
                                       bool persisted, uint64_t viewObjectId, TestRejectRule rule, bool underRecovery = false);

    // Probe a view PARTITION's table for `key`.  Returns the row pointer
    // (nullptr if the (view, partition) or key is absent) and, on success, sets
    // outLayout to the view's RowLayout.  The caller (Task 20's GetViewRow
    // handler) knows the owning partition for the key.  const: a pure read of
    // worker-owned state.
    //
    // SERVE-GUARD (Task 16, Review-focus #5): when the hosted instance is NOT
    // Ready (RebuildingViews/Recovering after a restart or a discard, or CatchingUp) getRow returns
    // nullptr with `*outStatus = NotReady` — a RETRIABLE not-ready that the
    // GetViewRow serve path surfaces so a probe never sees a half-built row.
    // `outStatus` is optional (nullptr ignores it), keeping every existing
    // 4-arg caller unchanged.
    // A view that is not LegacyU64 with an 8-byte key (P7-10) answers BadKey: its keys cannot be named by a u64.
    const uint8_t* getRow(int32_t viewTopicId, int32_t partition, uint64_t key,
                          const rune::view::RowLayout*& outLayout,
                          RowStatus* outStatus = nullptr) const;

    // getRow by the view-key encoding (big-endian, ViewKeyEncoding.h).  A Keyed view's key.size() is its keyWidth; a LegacyU64
    // view's is its one column's width, decoded to the table's u64.  Any other size answers BadKey.
    const uint8_t* getRowK(int32_t viewTopicId, int32_t partition, std::span<const uint8_t> key,
                           const rune::view::RowLayout*& outLayout, RowStatus* outStatus = nullptr) const;

    struct ScanPage {
        uint16_t status = 0;    // ViewRowResponse::Status
        uint16_t rows = 0;
        uint32_t bytes = 0;     // bytes written to `out`
        uint8_t  nextTokenLen = 0;   // 0 = the scan is complete
        std::array<uint8_t, rune::view::kScanTokenBytes> nextToken{};
    };
    // Writes the rows under `prefix` as [len u32][payload] into `out`, at most min(limit or 256, 256) rows and
    // kMaxScanPageBytes bytes.  `resumeToken` is empty or the previous page's nextToken.
    ScanPage scanPage(int32_t viewTopicId, int32_t partition, std::span<const uint8_t> prefix,
                      std::span<const uint8_t> resumeToken, uint16_t limit, std::span<uint8_t> out);

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
    struct InstanceServeState { uint64_t leadership = 0; int64_t foldCursor = 0; bool hosted = false; bool ready = false;
                                bool paused = false; int64_t pausedOffset = 0; };
    [[nodiscard]] InstanceServeState instanceServeState(int32_t viewTopicId, int32_t partition) const noexcept {
        const auto it = views_.find(instKey(viewTopicId, partition));
        if (it == views_.end()) return {};
        const ViewPaused* pz = std::get_if<ViewPaused>(&it->second.pause);
        return InstanceServeState{it->second.foldLeadership, it->second.foldCursor, true,
                                  it->second.readiness == ViewReadiness::Ready, pz != nullptr, pz ? pz->offset : 0};
    }

    /// The (view, partition) instance's fold cursor, -1 when it is not hosted.  Tests only.
    [[nodiscard]] int64_t foldCursorForTest(int32_t viewTopicId, int32_t partition) const noexcept {
        const auto it = views_.find(instKey(viewTopicId, partition));
        return it == views_.end() ? -1 : it->second.foldCursor;
    }

    /// The (view, partition) instance's key index slot count, 0 when it is not hosted.  Tests only.
    [[nodiscard]] std::size_t indexSlotsForTest(int32_t viewTopicId, int32_t partition) const noexcept {
        const auto it = views_.find(instKey(viewTopicId, partition));
        return it == views_.end() ? 0 : it->second.table.indexSlotsForTest();
    }

    /// VLS B.3: is `topicId` a view this worker has the plan of -- i.e. a view SINK id?  Pure read of worker-owned
    /// state (the plan install ring carries every view id to every worker).
    [[nodiscard]] bool isViewSink(int32_t topicId) const noexcept { return templates_.find(topicId) != templates_.end(); }

    // Namespaces Phase 2 Task 8 (D22): this view set's OWN slot table, fed by its worker from the NsSlotAssign /
    // NsSlotRelease ops (the worker thread is the only caller).  Task 12 (view memory) is its first consumer: a view
    // charges its chunks to its namespace's slot and checks the (slot, nsId) tag with nsTagMatches().
    void onNsSlotAssign(uint16_t slot, uint32_t nsId, const rune::protocol::NamespaceLimits& l) {
        if (nsSlots_.nsIdAt(slot) != nsId) {
            if (!nsSlots_.place(slot, nsId)) { ++nsSlotMismatches_; return; }
            nsViewMem_[slot] = 0; nsViewMemOver_[slot] = 0;   // a reused slot starts from zero usage
        }
        nsSlots_.setLimits(slot, l);   // the over decision is the aggregator's (ViewMemOver): a raised limit clears it there
    }
    void onNsSlotFreed(uint16_t slot, uint32_t nsId) {
        if (!nsSlots_.release(slot, nsId)) { ++nsSlotMismatches_; return; }
        nsViewMem_[slot] = 0; nsViewMemOver_[slot] = 0;
    }
    /// Task 14 (D26): the aggregator's decision (node total over maxViewMemoryBytes).  Tag-checked: a message for a
    /// namespace that no longer owns the slot is counted and ignored.  The fold gate reads the byte on the next pass.
    void onViewMemOver(uint16_t slot, uint32_t nsId, bool on) noexcept {
        if (!nsTagMatches(slot, nsId)) return;
        nsViewMemOver_[slot] = on ? 1 : 0;
    }
    /// Task 12: the namespace's view memory on this worker (sum of its instances' chunk bytes) and whether it is over.
    [[nodiscard]] uint64_t nsViewMemBytes(uint16_t slot) const noexcept { return slot < nsViewMem_.size() ? nsViewMem_[slot] : 0; }
    [[nodiscard]] bool     nsViewMemOver(uint16_t slot) const noexcept { return slot < nsViewMemOver_.size() && nsViewMemOver_[slot] != 0; }
    [[nodiscard]] uint64_t viewResumesForTest() const noexcept { return viewResumes_; }
    [[nodiscard]] uint64_t viewPausesForTest() const noexcept { return viewPauses_; }
    [[nodiscard]] bool nsTagMatches(uint16_t slot, uint32_t nsId) noexcept {
        if (nsSlots_.matches(slot, nsId)) return true;
        ++nsSlotMismatches_;
        return false;
    }
    void configureNsSlots(uint32_t capacity) {
        nsSlots_.reset(capacity);
        nsViewMem_.assign(nsSlots_.capacity(), 0);
        nsViewMemOver_.assign(nsSlots_.capacity(), 0);
    }
    [[nodiscard]] uint32_t nsSlotCapacity() const noexcept { return nsSlots_.capacity(); }
    [[nodiscard]] const NsSlotTable& nsSlotsForTest() const noexcept { return nsSlots_; }
    [[nodiscard]] uint64_t nsSlotMismatchesForTest() const noexcept { return nsSlotMismatches_; }
private:
    NsSlotTable nsSlots_;
    uint64_t    nsSlotMismatches_ = 0;
    // Task 12 (D26): per-slot view memory (chunk bytes) and the over-limit byte the fold gate reads.  Worker-owned arrays
    // sized to the slot table; the over byte is set only by the aggregator's ViewMemOver message (Task 14).
    std::vector<uint64_t> nsViewMem_     = std::vector<uint64_t>(NsSlotTable::kDefaultCapacity, 0);
    std::vector<uint8_t>  nsViewMemOver_ = std::vector<uint8_t>(NsSlotTable::kDefaultCapacity, 0);
    uint64_t viewPauses_ = 0, viewResumes_ = 0;

    [[nodiscard]] static std::size_t instanceMemory_(const ViewInstance& inst) noexcept {
        std::size_t m = inst.table.memoryBytes();
        if (inst.owners) m += inst.owners->memoryBytes();
        if (inst.claims) m += inst.claims->indexTable().memoryBytes() + inst.claims->extrasTable().memoryBytes();
        if (inst.agg) {
            m += inst.agg->contributors.memoryBytes() + inst.agg->groups.memoryBytes();
            if (inst.agg->blocks) m += inst.agg->blocks->memoryBytes();
            if (inst.agg->groupIds) m += inst.agg->groupIds->memoryBytes();
        }
        return m;
    }
    /// Charge the instance's current memory to its slot (one compare when nothing grew).
    void chargeMem_(ViewInstance& inst) noexcept {
        if (inst.nsSlot == kNoNsSlot) return;
        const std::size_t m = instanceMemory_(inst);
        if (m == inst.memCharged) return;
        if (!nsSlots_.matches(inst.nsSlot, inst.nsId)) { ++nsSlotMismatches_; return; }   // stale tag: never charge another namespace
        nsViewMem_[inst.nsSlot] = nsViewMem_[inst.nsSlot] + m - inst.memCharged;
        inst.memCharged = m;
    }
    void releaseMem_(ViewInstance& inst) noexcept {
        if (inst.nsSlot == kNoNsSlot || inst.memCharged == 0) return;
        if (nsSlots_.matches(inst.nsSlot, inst.nsId)) {
            nsViewMem_[inst.nsSlot] -= std::min<uint64_t>(nsViewMem_[inst.nsSlot], inst.memCharged);
        }
        inst.memCharged = 0;
    }
    /// The first drive that carries the source's namespace tag adopts it (cold, once per instance).
    void adoptNs_(ViewInstance& inst, NsTag ns) noexcept {
        if (ns.slot == kNoNsSlot || inst.nsSlot != kNoNsSlot) return;
        if (!nsSlots_.matches(ns.slot, ns.nsId)) { ++nsSlotMismatches_; return; }
        inst.nsSlot = ns.slot;
        inst.nsId   = ns.nsId;
        chargeMem_(inst);
    }
    /// The fold gate: false = the instance is (still) paused.  An array index, an nsId compare and one byte.
    [[nodiscard]] bool nsViewGate_(ViewInstance& inst) noexcept {
        const bool over = nsViewMemOver_[inst.nsSlot] != 0 && nsSlots_.matches(inst.nsSlot, inst.nsId);
        if (std::holds_alternative<ViewPaused>(inst.pause)) {
            if (over) return false;
            inst.pause = ViewRunning{};   // resume: the fold continues from foldCursor (== the paused offset)
            ++viewResumes_;
            return true;
        }
        if (!over) return true;
        inst.pause = ViewPaused{ViewPauseReason::OverLimit, inst.foldCursor};
        ++viewPauses_;
        return false;
    }
public:
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

    // True when the (view, partition) instance is a unique view with a claims index (stat 32 is served).
    [[nodiscard]] bool hasUniqueClaims(int32_t viewTopicId, int32_t partition) const {
        auto it = views_.find(instKey(viewTopicId, partition));
        return it != views_.end() && it->second.claims.has_value();
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
        if (inst.owners) n += pending(*inst.owners);
        if (inst.claims) n += pending(inst.claims->indexTable()) + pending(inst.claims->extrasTable());
        if (inst.agg) {
            n += pending(inst.agg->contributors) + pending(inst.agg->groups);
            if (inst.agg->blocks) n += pending(*inst.agg->blocks);
            if (inst.agg->groupIds) n += pending(*inst.agg->groupIds);
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
        // Part 04: a verdict view (its source's judge drives it) and, once uninstalled, Retiring until a committed marker passes its drop.
        bool                         isVerdict = false;
        rune::view::VerdictViewKind  verdictKind{};
        uint64_t                     verdictObjectId = 0;
        uint64_t                     verdictFingerprint = 0;
        TestRejectRule               testRule{};
        bool                         retiring = false;
        std::vector<uint8_t>         planRecordPayload;   // P9-8: the type-20 value a judge appends when this unique or check view joins its set
    };

    [[nodiscard]] bool isVerdictTemplate_(int32_t viewTopicId) const noexcept {
        const auto it = templates_.find(viewTopicId);
        return it != templates_.end() && it->second.isVerdict;
    }
    /// The instance a read is served from: nullptr (with the reason in `outStatus` and the layout set when the view is installed)
    /// while it is absent, paused or not Ready.
    [[nodiscard]] const ViewInstance* serveInstance_(int32_t viewTopicId, int32_t partition, const rune::view::RowLayout*& outLayout,
                                                     RowStatus* outStatus) const;
    void onDiscardDone_(int32_t viewTopicId, int32_t partition, int32_t status, ViewDiscardReason reason);
    /// The catalog gate every constraint-set change request passes (#788): true while this worker's catalog has not applied the
    /// position the logged set was computed at, or while a drained plan install or uninstall has no mark yet.  The request must then be left unasked; one that goes ahead stamps `catalogThrough_`.
    [[nodiscard]] bool catalogBehind_(int32_t srcTopicId, VerdictSource& judge, const PartitionWriteState& write);
    int syncConstraintSet_(int32_t srcTopicId, VerdictSource& judge, PartitionWriteState& write);
    int flipVerdictMembers_(int32_t srcTopicId, int32_t partition, int64_t bound, uint64_t leaderGen, bool leaderHwConfirmed, NsTag ns);
    void rebuildKeyIndex_(int32_t srcTopicId, int32_t partition, VerdictSource& judge, const TopicWriteSpec& spec, uint64_t leaderGen);
    // Inline: Worker.h's rebuildRcSlots_ reaches it, and that header is built into libraries that do not link rune_node.
    void eraseKeyIndex_(uint64_t key) {
        if (const auto it = keyIndexes_.find(key); it != keyIndexes_.end()) {
            releaseMem_(it->second);
            keyIndexes_.erase(it);
        }
    }
    int finishRetired_();
    /// P9-8: the plan a judge waits for, installed from the type-20 record of its log when the catalog does not have it: retiring and in
    /// memory, it judges up to its drop.  Cold.
    void installRetainedPlan_(int32_t srcTopicId, VerdictSource& judge, const PartitionWriteState& write);
    int pushVerdictCheckpoint_(ViewInstance& inst);
    void beginDiscard_(ViewInstance& inst, ViewDiscardReason reason);
    [[nodiscard]] bool tryPushDiscard_(ViewInstance& inst);
    bool eraseView_(int32_t viewTopicId);
    void eraseInstance_(int32_t viewTopicId, int32_t partition);
    static int  checkpointHook_(void* ctx, ViewInstance& inst) { return static_cast<WorkerViewSet*>(ctx)->pushVerdictCheckpoint_(inst); }
    static void discardHook_(void* ctx, ViewInstance& inst, ViewDiscardReason reason) { static_cast<WorkerViewSet*>(ctx)->beginDiscard_(inst, reason); }
    static void retiredHook_(void* ctx, int32_t viewTopicId, int32_t partition) {
        static_cast<WorkerViewSet*>(ctx)->retiredQueue_.push_back({viewTopicId, partition});
    }

    std::unordered_map<uint64_t, std::unique_ptr<VerdictSource>> verdictSources_;   // (srcTopicId, partition) -> judge
    // The key index of a key.index partition: a verdict member of its judge that is in neither views_ nor templates_.  Rebuilt from the
    // local log at every activation; never persisted.  unordered_map keeps the node's address across the move-assign that rebuilds it.
    std::unordered_map<uint64_t, ViewInstance> keyIndexes_;
    bool holdJudges_ = false;
    struct Retired { int32_t viewTopicId, partition; };
    bool installsSinceMark_ = false;   // a verdict plan install or uninstall was drained whose catalog mark has not been: the gate stays closed
    int64_t catalogThrough_ = -1;   // the latest catalog position this node's control thread marked here; never moves back (#788)
    struct RefusedPlan { uint64_t viewObjectId, fingerprint; };
    std::vector<RefusedPlan> refusedPlans_;   // the catalog installs this worker refused: the log never installs them instead, and the sync keeps their object's logged member
    std::vector<uint64_t> unreadablePlans_;   // retained plans whose decode failure was logged
    void noteRefused_(uint64_t viewObjectId, uint64_t fingerprint) {
        if (std::none_of(refusedPlans_.begin(), refusedPlans_.end(), [&](const RefusedPlan& r) { return r.fingerprint == fingerprint; })) refusedPlans_.push_back({viewObjectId, fingerprint});
    }
    uint64_t verdictEpoch_ = 0;   // bumped by every verdict template install, retire and erase; syncConstraintSet_ reruns on a change
    std::vector<Retired> retiredQueue_;   // the members the judges dropped, per partition
    std::vector<ConstraintSetMember> constraintWant_;   // syncConstraintSet_ scratch: grows once, never per pass
    int64_t verdictIntervalNs_ = 100'000'000;

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

    /// Cold: what to do with the files a recovered instance found (#590).  Legacy only -> back it up and rebuild; a legacy file
    /// next to a committed one -> back the legacy one up, keep the committed rows; a committed file that did not replay cleanly
    /// (a corrupt scan, or `applied` < the verified records) -> back it up and rebuild; a torn tail is only logged (the IO agent trims it);
    /// a clean checkpoint whose offset is above the source log end (`b.logEnd`) -> back it up and rebuild (#159).
    void applyRecoveredCheckpoint_(ViewInstance& inst, const RecoveredCheckpoint& rc, const ViewCheckpointScan& scan, std::size_t applied,
                                   const SourceLogBounds& b);
    /// One kDiscardMsgType{LegacySuperseded} frame (fire and forget: its ack, reason LegacySuperseded, never touches CheckpointState; a failure is only logged and counted).  False = ring full.
    [[nodiscard]] bool tryPushLegacySuperseded_(int32_t viewTopicId, int32_t partition);
    std::vector<uint64_t> legacyOwed_;   ///< instKeys whose superseded-legacy frame awaits ring room (cold path, nearly always empty)

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
                   const rune::view::FoldSkip& skip,
                   const TransientBounds* transientSrc, uint64_t leaderGen, bool leaderHwConfirmed, const SourceLogBounds& b) {
        int work = 0;
        // 0a. A discard in flight (the old checkpoint is being backed up): fold nothing, checkpoint nothing, serve NotReady until
        //     the IO agent acks.  Owed = the frame is not on the ring yet (retry it); Failed = never rebuilt over the unsaved file.
        if (!checkpointSettled(inst)) [[unlikely]] {
            if (std::holds_alternative<CheckpointDiscardOwed>(inst.checkpoint)) return tryPushDiscard_(inst) ? 1 : 0;
            return 0;
        }
        // 0c. Retention trimmed the disk source below this cursor (#726; a paused view, a stale checkpoint, a fresh view over a trimmed
        //     log): an instance without rows starts at the log start; one with rows is backed up and rebuilt.  Pure arithmetic, so it
        //     runs before the namespace pause return: a resumed view sees it on its first pass.
        if (transientSrc == nullptr && inst.foldCursor < b.logStart) [[unlikely]] {
            if (!startAtLogStart_(inst, b.logStart)) return 1;
        }
        // 0c'. #159: a persisted instance whose cursor is above the source log end holds rows from records the log no longer has
        //      (a checkpoint outran a lost or truncated log; a lazily recovered instance, created without bounds, lands here on its
        //      first pass): back it up and rebuild from the log.
        if (transientSrc == nullptr && inst.persisted && inst.foldCursor > b.logEnd) [[unlikely]] {
            ++inst.counters.checkpointsAheadOfLog;
            beginDiscard_(inst, ViewDiscardReason::AheadOfLog);
            return 1;
        }
        // 00. Task 12 (D26): the namespace is over its view-memory limit -> pause at foldCursor (reads answer
        //     ViewPaused); back under it -> resume from that same foldCursor.  Untagged instances skip this.
        if (inst.nsSlot != kNoNsSlot && !nsViewGate_(inst)) return work;
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
        rune::storage::LogReadView view = readRange(inst.foldCursor, passBound);
        // A disk read stops at the next segment base at or above its end: a compacted gap at a segment end reaching past
        // passBound reads empty below it.  Nothing is there, so read on to `bound`; the fold still stops at passBound.
        // (A transient source's read ignores the end, so the second read just repeats the first.)
        if (view.empty() && passBound < bound) [[unlikely]] view = readRange(inst.foldCursor, bound);
        if (view.empty()) return work;

        int64_t cursor = inst.foldCursor;
        const rune::view::FoldStats st =
            inst.keyed ? foldKeyed_(inst, view, passBound, cursor, skip)
          : inst.agg ? rune::view::foldAggregateBatch(inst.plan, inst.layout, inst.agg->layout,
                                                      inst.vdc, inst.table, inst.agg->contributors,
                                                      inst.agg->groups, inst.agg->blocksOrNull(), view,
                                                      passBound, cursor, skip)
                     : rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view,
                                             passBound, cursor, skip);
        work += cursor != inst.foldCursor;   // a pass across a compacted gap folds nothing but moves the cursor
        inst.foldCursor = cursor;
        chargeMem_(inst);   // Task 12: the pass may have appended chunks
        work += static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted +
                                 st.skippedMalformed + st.skippedNoGroup + st.skippedNoKey + st.keyConflicts);
        inst.counters.skippedAborted   += st.skippedAborted;
        inst.counters.skippedMalformed += st.skippedMalformed;
        inst.counters.skippedNoGroup   += st.skippedNoGroup;
        inst.counters.aggOverflow      += st.aggOverflow;
        inst.counters.aggNonFinite     += st.aggNonFinite;
        inst.counters.aggInconsistent  += st.aggInconsistent;
        inst.counters.aggRescans       += st.aggRescans;
        inst.counters.skippedNoKey     += st.skippedNoKey;
        inst.counters.keyConflicts     += st.keyConflicts;
        inst.counters.ownerMoves       += st.ownerMoves;
        inst.counters.uniquePromotions += st.uniquePromotions;
        if (inst.claims) inst.counters.uniqueConflicts = inst.claims->extras();

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
    /// The keyed variant: one v3 record, the view table then the owner map (a projection) or the aggregate's tables.
    int buildAndPushKeyedCheckpoint_(ViewInstance& inst);

    /// Folds [cursor, bound) of a keyed instance.
    static rune::view::FoldStats foldKeyed_(ViewInstance& inst, const rune::storage::LogReadView& view, int64_t bound, int64_t& cursor,
                                            const rune::view::FoldSkip& skip) {
        if (inst.agg) {
            rune::view::AggregateFold f = aggregateFoldOf(inst);
            return rune::view::foldKeyedAggregateBatch(f, inst.prospect, view, bound, cursor, skip);
        }
        rune::view::ProjectionFold f = projectionFoldOf(inst);
        return rune::view::foldProjectionBatch(f, inst.prospect, view, bound, cursor, skip);
    }

    // Clear the dirty/deleted tracking of every table of an instance: every
    // pass for a transient instance (nothing will ever encode it), and once
    // after a recovery replay (the restored rows ARE the durable checkpoint —
    // re-sending them would be a needless, possibly oversized delta).  No
    // allocation.
    static void discardDirty_(ViewInstance& inst) noexcept {
        auto dropKeyed = [](uint32_t, const uint8_t*, bool, std::span<const uint8_t>) noexcept {};
        inst.table.drainDirtyKeyed(dropKeyed);   // also a key index: its table is as wide as the wire key
        if (inst.keyed) {
            if (inst.owners) inst.owners->drainDirtyKeyed(dropKeyed);
            if (inst.claims) {
                inst.claims->indexTable().drainDirtyKeyed(dropKeyed);
                inst.claims->extrasTable().drainDirtyKeyed(dropKeyed);
            }
            if (inst.agg) {
                inst.agg->contributors.drainDirtyKeyed(dropKeyed);
                inst.agg->groups.drainDirtyKeyed(dropKeyed);
                if (inst.agg->blocks) inst.agg->blocks->drainDirtyKeyed(dropKeyed);
                inst.agg->groupIds->drainDirtyKeyed(dropKeyed);
            }
            return;
        }
        auto drop = [](uint32_t, const uint8_t*, bool, uint64_t) noexcept {};
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

    /// Cold: the source log starts above this instance's cursor.  True = it now starts at the log start; false = it held rows
    /// from trimmed records and a BelowLogStart discard began.
    [[nodiscard]] bool startAtLogStart_(ViewInstance& inst, int64_t logStart);

    ViewSnapshotChannel& channel_;
    std::string          viewDataDir_;
    uint64_t             hashSeed_ = 0;
    uint64_t             hashMask_ = ~uint64_t{0};
    uint64_t             installRefusals_ = 0;

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
    std::vector<rune::view::KeyedDirtyEntry> keyedEntries_;   // the v3 encoder input
    std::vector<uint8_t>                keyScratch_;          // the entries' key bytes, copied like the rows

    uint64_t capacityExceeded_ = 0;

};

} // namespace rune::node
