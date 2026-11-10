#pragma once
// VerdictSource.h — checked writes Part 04: the judge of one source partition.
//
// A verdict source owns the verdict views of its partition.  judge() prepares every active member for a record,
// evaluates them in set order, and applies all or none; a rejection goes into PartitionWriteState::rejected.  driveJudge
// walks the committed log, follows the logged constraint-set changes and emits durable verdict markers.
#include <rune/codec/PrimType.h>
#include <rune/node/VerdictState.h>
#include <rune/node/ViewInstance.h>
#include <rune/node/WriteState.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/ControlMarkerLayout.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/ProducerBatchWindow.h>
#include <rune/view/FoldWalk.h>
#include <rune/view/TxnOverlay.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <array>
#include <deque>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace rune::node {

// Checked writes Part 05: the key index, a verdict member over a one-byte-row table keyed by the packed wire key.
inline constexpr uint64_t    kKeyIndexPlanFingerprint      = 0x4B45'5949'4E44'4558ull;   // "KEYINDEX"
inline constexpr std::size_t kKeyIndexChunkBytes           = 64u << 10;
inline constexpr int64_t     kKeyIndexReplayRecordsPerPass = 65536;
/// Part 11: what the judge reads per pass looking ahead for an undecided transaction's TxnJudge or marker (P11-2).
inline constexpr std::size_t kTxnProbeBytesPerPass = 1u << 20;
/// Part 11: what a judgement reads per pass, for each of its two walks (the chained read of T and the P11-3 scan).
inline constexpr std::size_t kVoteScanBytesPerPass = 1u << 20;

struct KeyIndexFold {
    rune::view::ViewTable&          table;
    uint8_t                         keyWidth = 0;
    uint8_t                         member = 0;         // this view's id within the judge's set, for TxnOverlay keys
    const rune::view::TxnLockSet*   locks  = nullptr;   // rows held by other transactions: a prepare that touches one is Locked
};

using rune::storage::keyIndexWord;
/// With `chain` the key state is read chain-first and the record's insert or erase is written into the chain; a Locked or CapacityExhausted
/// result leaves the chain unspecified.
[[nodiscard]] rune::view::PrepareStatus prepareKeyIndex(const KeyIndexFold&, const rune::view::SourceRecord&, rune::view::Prospect&,
                                                        rune::view::TxnOverlay* chain = nullptr) noexcept;
/// The intent table (P5-3) over the key state the prospect holds; a Malformed prospect is accepted.
[[nodiscard]] rune::view::Verdict evaluateIntent(const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
void applyKeyIndex(KeyIndexFold&, const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
/// foldBatch's contract (batch, bound, cursor, corrupt tail) over the accepted records: nothing is judged.
rune::view::FoldStats foldKeyIndexBatch(KeyIndexFold&, const rune::storage::LogReadView&, int64_t bound, int64_t& cursor,
                                        const rune::view::FoldSkip&, rune::view::TxnFold* txn = nullptr);
/// What a unique or check member is evaluated against: its plan, row layout and current table.
struct ConstraintFold {
    const rune::view::CompiledViewPlan& plan;
    const rune::view::RowLayout&        layout;
    const rune::view::ViewTable&        table;
};
/// P9-2..P9-4 over the member's prepared prospect: a tombstone passes; a key owned by another source key is ordinal 0; then each
/// check, in ordinal order, that is False on a row its record changed.
[[nodiscard]] rune::view::Verdict evaluateConstraints(const ConstraintFold&, const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
[[nodiscard]] rune::view::CompiledViewPlan keyIndexPlan(int32_t srcTopicId, uint16_t keyFieldId);   // cold
[[nodiscard]] rune::view::RowLayout keyIndexRowLayout();                                           // rowLen 1

/// What the owning WorkerViewSet does for a judge's members (fn-pointer + ctx, no std::function).
struct VerdictViewHooks {
    void* ctx = nullptr;
    /// Push the member's dirty rows as one checkpoint delta (or drop them for a transient member).
    int (*checkpoint)(void* ctx, ViewInstance&) = nullptr;
    /// A checkpoint that outran the markers (P4-4): the owner empties the member's tables, backs its checkpoint file up and
    /// rebuilds it (WorkerViewSet::beginDiscard_).  The judge never touches the member's cursor on a discard.
    void (*discard)(void* ctx, ViewInstance&, ViewDiscardReason) = nullptr;
    /// A retiring member left this partition's set for good (W passed its drop): the owner may erase the partition's instance.
    void (*retired)(void* ctx, int32_t viewTopicId, int32_t partition) = nullptr;
};

/// Part 11: the source keys of one transaction with their last offsets (P11-3, P11-3a), and the other producers' markers and TxnJudge
/// records met below its end, in offset order, with the markers past the end that decide those TxnJudges (resolve()).
/// Open-addressed and reserved once.  A key past the reserved count overflows the set, and so does a marker or TxnJudge past the
/// reserved count (they share the slots): the list then holds every entry below the first that did not fit.  A key longer than
/// any checked topic's is not tracked (note()).
class TxnKeySet {
public:
    struct Key {
        std::array<uint8_t, rune::codec::kMaxTopicKeyBytes> bytes{};
        uint8_t len = 0;
        bool    used = false, superseded = false;
        int64_t last = -1;
    };
    struct Marker { int64_t producerId; int64_t offset; bool commit, judge; int16_t epoch = -1; int64_t first = -1; };   // epoch, first: a TxnJudge's
    enum class Applied : uint8_t { No, Yes, Unknown };

    void reserve(std::size_t keys, std::size_t markers);   // cold
    [[nodiscard]] std::size_t keyCapacity() const noexcept { return keyCap_; }
    void clear() noexcept;
    void clearMarkers() noexcept;
    /// The key's last offset is `offset` (records come in offset order).  A key wider than kMaxTopicKeyBytes is not tracked: every
    /// member skips a record whose key is not its key field's width (at most that), so the record changes no row and can neither
    /// supersede nor conflict.  False: a new key past the reserved count, not noted.
    bool note(std::string_view key, int64_t offset) noexcept;
    [[nodiscard]] Key* find(std::string_view key) noexcept;
    void noteMarker(int64_t producerId, int64_t offset, bool commit) noexcept;
    void noteJudge(int64_t producerId, int16_t epoch, int64_t first, int64_t offset) noexcept;
    /// Whether the producer's transactional batch at `base` (of `epoch`) is applied below the end, from the log alone: the producer's
    /// first noted marker above `base` commits, or a TxnJudge of the batch's own transaction (its epoch, first <= base) comes first
    /// (deferred, judged below the end) and the marker after it commits.  Another TxnJudge decides nothing.  Unknown: that marker lies
    /// past the end and is not resolved yet, or past where the marker list overflowed.
    [[nodiscard]] Applied appliedAt(int64_t producerId, int16_t epoch, int64_t base) const noexcept;
    /// A marker past the end: noted when it decides a noted TxnJudge that has none yet.  True: it was noted.
    bool resolve(int64_t producerId, int64_t offset, bool commit) noexcept;
    [[nodiscard]] bool pending() const noexcept { return !pending_.empty(); }   // a noted TxnJudge whose marker is not noted
    [[nodiscard]] bool awaits(int64_t producerId) const noexcept;          // the producer's last noted entry is such a TxnJudge
    [[nodiscard]] std::span<const Marker> markers() const noexcept { return markers_; }
    [[nodiscard]] bool markersOverflowed() const noexcept { return markersOverflowed_; }

private:
    [[nodiscard]] std::size_t slotOf_(std::string_view key) const noexcept;

    std::vector<Key>      slots_;   // a power of two, at least twice keyCap_
    std::vector<uint32_t> used_;
    std::vector<Marker>   markers_;
    std::vector<int64_t>  pending_;   // the producers whose last noted entry is a TxnJudge
    std::size_t           keyCap_ = 0;
    bool                  markersOverflowed_ = false;
};

/// The worker's transaction overlays.  `at(i)` is stable; an overlay is bound to one source key while it is acquired.  Each overlay
/// has its key set (a judgement keeps both across passes); `oneStep()` is the key set of work done within one step (P11-3a).
/// Only another transaction's marker frees an overlay a yes vote holds, and that marker may wait for a vote that needs one: when
/// none is free the worker grows the pool by one (grow()) instead of making the judge wait, and trims the last one once it is free.
class TxnOverlayPool {
public:
    /// D1: one marker application's noted keys at a time, for every fold of the worker (path A and the members' replays).
    rune::view::SupersedeKeys supersedeKeys;

    /// Cold.  The caller's precondition: rows >= txnJudgedRecordsMax x the sum, over the verdict members of the source, of
    /// (key index 1; aggregate 4 x kSlotsPerTable + 3; projection = its applyTo write set + 1), and maxRowLen >= the largest
    /// member table rowLen, ClaimsExtras included (VerdictSource::overlayNeed).  An undersized overlay stalls the judge on a legal
    /// transaction.  Only grows; an overlay in use keeps its rows until release() finds it free (pending()).
    void reserveFor(std::size_t rows, uint32_t maxRowLen) {
        rowsWanted_ = std::max(rowsWanted_, rows);
        rowLenWanted_ = std::max(rowLenWanted_, maxRowLen);
        overlaysPending_ = false;
        for (std::size_t i = 0; i < overlays_.size(); ++i) {
            if (reservedRows_[i] >= rowsWanted_ && reservedRowLen_[i] >= rowLenWanted_) continue;
            if (inUse_[i]) {
                overlaysPending_ = true;
                continue;
            }
            overlays_[i].reserve(rowsWanted_, rowLenWanted_);
            reservedRows_[i] = rowsWanted_;
            reservedRowLen_[i] = rowLenWanted_;
        }
    }
    /// A reserve call left an overlay or a key set in use short: release() retries it once that one is free.
    [[nodiscard]] bool pending() const noexcept { return overlaysPending_ || keysPending_; }
    /// A cleared overlay bound to `sourceKey`, or -1 when all are in use: the pool is starved until an acquire succeeds.
    [[nodiscard]] int32_t acquire(uint64_t sourceKey) noexcept {
        for (std::size_t i = 0; i < overlays_.size(); ++i) {
            if (inUse_[i]) continue;
            inUse_[i] = true;
            starved_ = false;
            overlays_[i].bind(sourceKey);
            return static_cast<int32_t>(i);
        }
        starved_ = true;
        return -1;
    }
    void release(int32_t i) noexcept {
        overlays_[static_cast<std::size_t>(i)].clear();
        inUse_[static_cast<std::size_t>(i)] = false;
        if (pending()) {   // a reserve found this one in use and short: nothing else retries it
            reserveKeys(0);
            reserveFor(0, 0);
        }
    }
    /// A judge is waiting for an overlay and none is free: the worker grows the pool.
    [[nodiscard]] bool starved() const noexcept { return starved_ && freeCount() == 0; }
    [[nodiscard]] std::size_t size() const noexcept { return overlays_.size(); }
    /// Cold: one more overlay, reserved like the others.  Every judge binds again afterwards (its lock vector holds `size()`).
    void grow(int64_t nowNs) {
        grownAtNs_ = nowNs;
        overlays_.emplace_back();
        keys_.emplace_back();
        inUse_.push_back(false);
        reservedRows_.push_back(0);
        reservedRowLen_.push_back(0);
        reserveFor(0, 0);
        reserveKeys(0);
    }
    /// Pops the last overlay once it is free, the pool is above its base size, no judge is waiting and the pool has not grown for
    /// kTrimAfterNs (contention at the edge would otherwise reserve an overlay anew every pass).  A free overlay is bound to no
    /// source (releaseOverlay_ refreshes the lock set).
    void trim(int64_t nowNs) noexcept {
        if (starved_ || overlays_.size() <= rune::view::kTxnOverlaysPerWorker || inUse_.back() || nowNs - grownAtNs_ < kTrimAfterNs) return;
        overlays_.pop_back();
        keys_.pop_back();
        inUse_.pop_back();
        reservedRows_.pop_back();
        reservedRowLen_.pop_back();
    }
    [[nodiscard]] std::size_t freeCount() const noexcept { return static_cast<std::size_t>(std::count(inUse_.begin(), inUse_.end(), false)); }
    [[nodiscard]] rune::view::TxnOverlay& at(int32_t i) noexcept { return overlays_[static_cast<std::size_t>(i)]; }
    /// Cold: every key set holds at least `keys` keys.  A set whose overlay is in use keeps its keys (a judgement is reading them):
    /// release() grows it once it is free (keysPending()).  The one-step set has a fixed size: an ordinary transaction is
    /// refused past it on every leader alike.
    void reserveKeys(std::size_t keys);
    [[nodiscard]] bool keysPending() const noexcept { return keysPending_; }
    [[nodiscard]] TxnKeySet& keysAt(int32_t i) noexcept { return keys_[static_cast<std::size_t>(i)]; }
    [[nodiscard]] TxnKeySet& oneStep() noexcept { return oneStep_; }
    /// The one-step set is shared by the worker's sources: a source that keeps its content across passes holds a claim until it
    /// releases it.  While it is held another source waits: two sources taking it from each other would never finish (T7 N3).
    /// A holder parked on another transaction's marker is the exception: that marker may wait for a vote the waiter would judge
    /// (T7 review 4, I1), so the waiter takes the set and the holder collects anew when it resumes.  A holder paused on a row a
    /// judged transaction holds is parked too: only that transaction's marker clears the lock (review 5, N1).
    [[nodiscard]] bool oneStepHeld() const noexcept { return oneStepHeld_; }
    [[nodiscard]] bool oneStepClaimable() const noexcept { return !oneStepHeld_ || oneStepParked_; }
    [[nodiscard]] uint64_t claimOneStep() noexcept {
        oneStepHeld_ = true;
        oneStepParked_ = false;
        return ++oneStepClaim_;
    }
    [[nodiscard]] uint64_t oneStepClaimsForTest() const noexcept { return oneStepClaim_; }
    [[nodiscard]] bool holdsOneStep(uint64_t claim) const noexcept { return oneStepHeld_ && claim == oneStepClaim_; }
    void parkOneStep(uint64_t claim, bool parked) noexcept { oneStepParked_ = holdsOneStep(claim) ? parked : oneStepParked_; }
    void releaseOneStep(uint64_t claim) noexcept { oneStepHeld_ = oneStepHeld_ && claim != oneStepClaim_; }
    /// `out` is the acquired overlays bound to `sourceKey`; its capacity is the caller's (at least size(): no allocation).
    void lockSetFor(uint64_t sourceKey, std::vector<const rune::view::TxnOverlay*>& out) const noexcept {
        out.clear();
        for (std::size_t i = 0; i < overlays_.size(); ++i)
            if (inUse_[i] && overlays_[i].sourceKey() == sourceKey) out.push_back(&overlays_[i]);
    }

private:
    std::deque<rune::view::TxnOverlay>                                     overlays_{rune::view::kTxnOverlaysPerWorker};   // stable across grow()
    std::vector<bool>                                                      inUse_ = std::vector<bool>(rune::view::kTxnOverlaysPerWorker);
    std::deque<TxnKeySet>                                                  keys_{rune::view::kTxnOverlaysPerWorker};
    TxnKeySet                                                              oneStep_;
    std::vector<std::size_t>                                               reservedRows_ = std::vector<std::size_t>(rune::view::kTxnOverlaysPerWorker);
    std::vector<uint32_t>                                                  reservedRowLen_ = std::vector<uint32_t>(rune::view::kTxnOverlaysPerWorker);
    std::size_t                                                            rowsWanted_ = 0;
    uint32_t                                                               rowLenWanted_ = 0;
    bool                                                                   overlaysPending_ = false;
    std::size_t                                                            keysWanted_ = 0;
    uint64_t                                                               oneStepClaim_ = 0;
    bool                                                                   oneStepHeld_ = false;
    bool                                                                   oneStepParked_ = false;   // the holder waits for another transaction's marker
    bool                                                                   keysPending_ = false;
    bool                                                                   starved_ = false;   // an acquire found none free
    int64_t                                                                grownAtNs_ = 0;
    static constexpr int64_t                                               kTrimAfterNs = 10'000'000'000;
};

class VerdictSource {
public:
    static constexpr int64_t kChangeDeferred = -1, kChangeUnchanged = -2;

    /// What WorkerViewSet::syncConstraintSet_ last reconciled this judge against: the template epoch and the latest logged change.
    uint64_t constraintSyncEpoch = UINT64_MAX;
    int64_t  constraintSyncChange = -2;
    int64_t  constraintSyncCatalog = -2;   ///< WorkerViewSet::catalogThrough_ at that sync
    uint64_t retainedTriedFingerprint = 0, retainedTriedEpoch = UINT64_MAX;   ///< the retained plan WorkerViewSet last tried to install for this judge, and at which template epoch

    VerdictSource(int32_t srcTopicId, int32_t partition);

    /// The partition state judge() records rejections in and driveJudge() reads the timeline from.
    void bindState(PartitionWriteState* state) noexcept { state_ = state; }
    [[nodiscard]] PartitionWriteState* boundState() const noexcept { return state_; }

    /// Prepares every member of the active set for `rec`, evaluates them in set order, applies all or none.  A rejection
    /// is added to the index and returned.  A full index or an exhausted prospect sets stalled() and applies nothing.
    [[nodiscard]] rune::view::Verdict judge(const rune::view::SourceRecord& rec) noexcept;

    /// One duty-pass of the judge over the committed range below `bound` (the HW, P11-2).  `readRange(from, to)` returns whole
    /// batches as a LogReadView.  `lso`: an empty constraint set is passed only below it (nothing judges an open transaction
    /// there, and a member joining later must not replay one).  Returns a progress count.
    template <class ReadRangeFn>
    int driveJudge(ReadRangeFn&& readRange, int64_t bound, int64_t nowNs, PartitionWriteState& state,
                   const rune::view::AbortedTxnFilter& aborted = {}, int64_t lso = INT64_MAX);

    /// Cold: the worker's overlay pool, the batch's own overlays and the knob the pool was sized for.  Reserves the pool's key sets.
    void bindTxn(TxnOverlayPool* pool, std::span<rune::view::TxnOverlay> batchOverlay, int32_t txnJudgedRecordsMax);
    /// The overlay rows one record of a transaction may take over this judge's members, and their longest table row (the reserve
    /// bound of TxnOverlayPool::reserveFor).
    struct OverlayNeed { std::size_t rowsPerRecord = 0; uint32_t maxRowLen = 0; };
    [[nodiscard]] OverlayNeed overlayNeed() const noexcept;
    void setVoteScanBytesForTest(std::size_t bytes) noexcept { voteScanBytes_ = bytes; }

    /// P11-2: never past an undecided transaction, nor past an ordinary one the judge has not applied at its marker yet.  The judge's
    /// contribution to K.
    [[nodiscard]] int64_t judgedThrough() const noexcept {
        int64_t through = judgeCursor_;
        if (state_ != nullptr) through = std::min(through, state_->txn.earliestUndecidedFirst());
        for (const OrdinaryTxn& o : ordinary_) through = std::min({through, o.first, o.holdFrom});
        return through;
    }
    /// P11-8 (c): the lowest checkpoint a persisted member restarts from; a replay from it backfills from the TxnJudges at or above it.
    [[nodiscard]] int64_t checkpointFloor() const noexcept {
        int64_t floor = INT64_MAX;
        for (const Member& m : members_)
            if (m.inst->persisted && m.inst->lastDurableO >= 0) floor = std::min(floor, m.inst->lastDurableO);
        return floor;
    }
    /// Where the judge stands; it runs ahead of deferred transactions.
    [[nodiscard]] int64_t judgeCursor() const noexcept { return judgeCursor_; }
    /// Gives the pool every overlay `txn` holds, and drops the judgement in progress.  The leadership scan's begin and activate_ call
    /// it before the table is reset.
    void releaseTxnOverlays(TxnJudgeState& txn) noexcept;
    /// Gives back the one-step set this source holds mid-segment; its walk collects anew if it claims again.  The worker calls it
    /// for a partition it does not drive while its leadership scan rebuilds, so that source never keeps the other sources waiting.
    void releaseOneStep() noexcept {
        if (oneStepClaim_ != 0) txnPool_->releaseOneStep(oneStepClaim_);
        oneStepClaim_ = 0;
        stepParked_ = false;
    }
    /// The worker no longer drives this source as leader: the one-step set and every overlay its transactions hold go back, so a
    /// demoted source neither keeps the other sources waiting nor holds the pool.  A new leadership re-activates it and judges them anew.
    void releaseDemoted(TxnJudgeState& txn) noexcept {
        if (txnLocks_.locked.empty()) releaseOneStep();
        else releaseTxnOverlays(txn);
    }
    /// The leadership scan's begin: the overlays go back, and the judge re-activates from K once the scan is done (the table's
    /// entries are judged anew, so a reset Locked entry gets its apply).
    void rescan(TxnJudgeState& txn) noexcept {
        releaseTxnOverlays(txn);
        if (generation_ != kUnbound) needsActivation_ = true;
    }
    [[nodiscard]] int32_t partition() const noexcept { return partition_; }
    [[nodiscard]] bool stalled() const noexcept { return stalled_; }
    /// Cold, the owner after growing the index: resumes a judge that stalled on a full index.  The other stalls stay.
    void unstall() noexcept {
        if (stallReason_ == StallReason::Full && state_ != nullptr && state_->rejected.size() + stallRoom_ <= state_->rejected.capacity()) stalled_ = false;
    }
    [[nodiscard]] bool waitingForInstall() const noexcept { return waitingForInstall_; }
    /// While waitingForInstall(): the member whose view is not installed.
    [[nodiscard]] const ConstraintSetMember& waitingFor() const noexcept { return waitingFor_; }
    /// Cold (#791 diagnostics, read on a View ack timeout): the judge's state in one snapshot.
    struct DiagSnapshot {
        int64_t judgedThrough; bool stalled; int stallReason; bool waitingForInstall; bool needsActivation;
        uint32_t activeSetId; std::size_t members; std::size_t active; uint64_t leaderGeneration;
    };
    [[nodiscard]] DiagSnapshot diagSnapshot() const noexcept {
        return {judgedThrough(), stalled_, static_cast<int>(stallReason_), waitingForInstall_, needsActivation_, activeSet_.id, members_.size(), active_.size(), generation_};
    }
    [[nodiscard]] bool activationPending() const noexcept { return needsActivation_; }
    /// A leadership has activated this judge and no new one is pending: what it folds now belongs to that leadership.
    [[nodiscard]] bool activated() const noexcept { return generation_ != kUnbound && !needsActivation_; }
    [[nodiscard]] int64_t logStart() const noexcept { return logStart_; }

    /// `planRecordPayload` (P9-8, a Unique or Check member): the type-20 value requestChange appends when the member joins.
    void addVerdictView(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint = 0,
                        std::span<const uint8_t> planRecordPayload = {});
    /// P9-5: a replacement plan of a logged member.  It replays the log below judgedThrough() without judging, and joins the set
    /// when a change record names it (the owner asks for one once candidateCaughtUp()).
    void addCandidate(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint,
                      std::span<const uint8_t> planRecordPayload = {});
    void removeCandidate(uint64_t viewObjectId, uint64_t planFingerprint);
    [[nodiscard]] bool hasCandidate(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept;
    [[nodiscard]] bool hasCandidates() const noexcept;
    [[nodiscard]] bool candidateCaughtUp(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept;
    void removeVerdictView(int32_t viewTopicId);
    /// The view was dropped: a member with the same plan installed since judges in its place (#786).
    void markLeaving(int32_t viewTopicId) noexcept;
    /// Appends a constraint-set change at the LEO.  Returns the activation offset, kChangeDeferred or kChangeUnchanged.
    /// `refresh`: append even when the members are those of the latest change (retention is about to delete its segment).
    int64_t requestChange(const ConstraintSet& next, bool refresh = false);
    void installTestVerdictViewForTest(ViewInstance*, uint64_t viewObjectId, TestRejectRule);

    /// Cold: the replica to append through and the leadership this judge serves.  A new generation re-activates.
    void bindLeader(rune::replication::PartitionReplica* replica, uint64_t leadershipGeneration, int64_t logStart) noexcept;
    void setVerdictIntervalNs(int64_t ns) noexcept { intervalNs_ = ns; }
    void setViewHooks(const VerdictViewHooks& h) noexcept { hooks_ = h; }
    [[nodiscard]] bool hasMember(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept;
    [[nodiscard]] bool hasView(int32_t viewTopicId) const noexcept;
    [[nodiscard]] bool hasMembers() const noexcept { return !members_.empty(); }
    [[nodiscard]] VerdictSourceStats& statsMut() noexcept { return stats_; }
    [[nodiscard]] const VerdictSourceStats& stats() const noexcept { return stats_; }

    /// With bindLeader(nullptr, ...) appends are captured here instead of reaching a replica.
    [[nodiscard]] std::span<const std::vector<uint8_t>> appendedForTest() const noexcept { return appended_; }
    /// Without a replica the appended batches are kept for the tests; a bench turns that (an allocation per batch) off.
    void captureAppendedForTest(bool on) noexcept { captureAppended_ = on; }
    [[nodiscard]] uint64_t judgeCallsForTest() const noexcept { return judgeCalls_; }
    void holdJudgeForTest(bool hold) noexcept { held_ = hold; }
    void failCapacityAtForTest(int64_t offset) noexcept { failCapacityAt_ = offset; }   // the prepare of that record stalls once (Capacity)
    [[nodiscard]] uint8_t lastIntentForTest() const noexcept { return lastIntent_; }
    [[nodiscard]] bool retiringForTest(uint64_t viewObjectId) const noexcept;
    [[nodiscard]] bool candidateForTest(uint64_t viewObjectId) const noexcept {
        return std::any_of(members_.begin(), members_.end(), [&](const Member& m) { return m.candidate && m.objectId == viewObjectId; });
    }
    [[nodiscard]] std::size_t memberCountForTest() const noexcept { return members_.size(); }

private:
    struct Member {
        ViewInstance*               inst = nullptr;
        rune::view::VerdictViewKind kind{};
        uint64_t                    objectId = 0, fingerprint = 0;
        TestRejectRule              rule;
        rune::view::Prospect        prospect;
        uint8_t                     keyWidth = 0;   // KeyIndex: the wire key width of the source's key field
        uint8_t                     slot = 0;       // the member's id in TxnOverlay keys, stable while it is a member
        int32_t                     chainLive = 0;  // CapLiveKeys inside a chained judgement: the rows the chain added less those it erased
        bool                        retiring = false, leaving = false, candidate = false;
        int64_t                     leaveOffset = -1;
        // P11-8 (a): the cursor its replay began at (a TxnJudge whose firstOffset is below it is backfilled); the cursor of its last push
        int64_t                     replayFrom = -1, pushedAt = -1;
        std::vector<uint8_t>        planRecordPayload;   // the type-20 value requestChange appends when the member joins
    };

    void   addMember_(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint, bool candidate,
                      std::span<const uint8_t> planRecordPayload = {});
    template <class ReadRangeFn>
    int    replayCandidates_(ReadRangeFn& readRange, int64_t bound, const rune::view::AbortedTxnFilter& aborted);
    void   activate_(PartitionWriteState& state);
    int    replay_(Member& m, const rune::storage::LogReadView& view, int64_t upTo, const rune::view::AbortedTxnFilter& aborted,
                   rune::view::SpanReader reader);
    [[nodiscard]] rune::view::JudgeDecision judgeStep_(Member& m, const uint8_t* batch, std::size_t len, int64_t at) noexcept;
    void   closeSpan_(int64_t producerId) noexcept;
    template <class ReadRangeFn>
    int    replayMember_(ReadRangeFn& readRange, Member& m, int64_t bound, const rune::view::AbortedTxnFilter& aborted);
    [[nodiscard]] bool runsAheadAt_(int64_t cursor) const noexcept;
    [[nodiscard]] int64_t runAheadFrom_() const noexcept;
    int    judgeBatches_(const rune::storage::LogReadView& view, int64_t limit, int64_t hardLimit, const rune::view::AbortedTxnFilter& aborted);
    void   syncMemberCursors_() noexcept;
    // Part 11 (P11-2): a transaction without a committed TxnJudge or marker stops the judge at its first record (TxnStop::Probe) until a
    // look-ahead finds one; an ordinary transaction stops it at its marker (TxnStop::Marker) to be applied there.
    // `vote`: P11-7, -1 until the judge classifies it at the marker.
    struct OrdinaryTxn { int64_t producerId; int64_t first; int64_t marker; bool commit; int8_t vote = -1; int64_t intentFrom = -1; int64_t holdFrom = INT64_MAX; };
    struct TxnProbe { int64_t producerId = -1, first = -1, from = -1, waitAt = -1; };
    struct OwedMarker { int64_t producerId; int64_t judgeOffset; int64_t from; };   // a re-offered TxnJudge's marker, searched from `from`
    // Judge: the judge stands at a TxnJudge to judge (judgePid_).  Locked: a record touches a row a transaction holds (P11-4).
    // Fold: the judge passed a decided commit's TxnJudge that an active member's replay still owes.  The members replay through it
    // before the judge goes on (no cursor sync).
    enum class TxnStop : uint8_t { None, Probe, Marker, Judge, Locked, Fold };
    enum class ProbeStep : uint8_t { Next, Classified, Wait };
    // The transaction being judged at `end`, its TxnJudge (or its marker, P11-7): the chained read of [first, end), then the P11-3
    // scan of (first, end), each from `from` and bounded per pass.  The overlay and its key set are kept across passes.
    struct TxnJudgement {
        int64_t       producerId = -1, first = -1, end = -1, from = -1;
        int32_t       overlay = -1, records = 0;
        bool          atMarker = false, scanning = false, done = false;
        int64_t       endedAt = -1;           // the producer's own marker in (first, end): T ended there, before its TxnJudge
        bool          endedCommit = false;
        uint64_t      members = 0;            // membersEpoch_ at the start: a member change restarts the judgement
        int32_t       cap = 0;                // the record cap in effect at the start (the key set holds that many)
        TxnVoteAnswer answer{};
        int64_t       resolvePid = -1, resolveFrom = -1;   // the scan waits for this producer's marker past the end, searched from here
    };
    enum class ChainStep : uint8_t { Ok, Failed, Locked, Stalled };
    [[nodiscard]] bool txnBatchDeferred_(int64_t producerId, int64_t base, int32_t records) noexcept;
    [[nodiscard]] bool endsBeforeJudge_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len) noexcept;
    template <class ReadRangeFn, class Fn>
    bool   walkBatches_(ReadRangeFn& readRange, int64_t from, int64_t to, Fn&& fn) noexcept;
    template <class ReadRangeFn>
    bool   probeTxn_(ReadRangeFn& readRange, int64_t bound) noexcept;
    ProbeStep probeBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len) noexcept;
    template <class ReadRangeFn>
    void   feedOwedMarkers_(ReadRangeFn& readRange, int64_t bound) noexcept;
    [[nodiscard]] bool owedMarkerBatch_(const OwedMarker& m, const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len) noexcept;
    void   pruneOwedMarkers_() noexcept;
    template <class ReadRangeFn>
    bool   applyOrdinary_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept;
    template <class ReadRangeFn>
    bool   checkOrdinary_(ReadRangeFn& readRange, const OrdinaryTxn& o, const rune::view::AbortedTxnFilter& aborted, TxnKeySet& keys) noexcept;
    template <class ReadRangeFn>
    bool   lookupTxn_(ReadRangeFn& readRange, int64_t end) noexcept;
    static void noteControl_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, int64_t self, TxnKeySet& keys) noexcept;
    void   collectOrdinaryBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, int64_t producerId, TxnKeySet& keys) noexcept;
    [[nodiscard]] bool checkOrdinaryBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, int64_t producerId,
                                           const rune::view::AbortedTxnFilter& aborted, TxnKeySet& keys) noexcept;
    template <class ReadRangeFn>
    bool   resolveMarkers_(ReadRangeFn& readRange, TxnKeySet& keys, int64_t& from, std::size_t budget, int64_t producerId) noexcept;
    [[nodiscard]] bool applyTxnBatch_(const uint8_t* batch, std::size_t len, TxnKeySet& keys, int64_t end) noexcept;
    [[nodiscard]] TxnKeySet& oneStepKeys_() noexcept { return txnPool_->oneStep(); }
    template <class ReadRangeFn>
    bool   judgeTxn_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept;
    template <class ReadRangeFn>
    bool   judgeAtMarker_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept;
    template <class ReadRangeFn>
    int8_t shouldHaveVoted_(ReadRangeFn& readRange, OrdinaryTxn& o) noexcept;
    template <class ReadRangeFn>
    bool   runJudgement_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept;
    template <class ReadRangeFn>
    bool   finalizeLocked_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept;
    template <class ReadRangeFn>
    bool   applyTxnRecords_(ReadRangeFn& readRange, int64_t producerId, int64_t first, int64_t end, const rune::view::AbortedTxnFilter& aborted,
                            int64_t& applied) noexcept;
    template <class ReadRangeFn>
    bool   rejectTxnRecords_(ReadRangeFn& readRange, int64_t producerId, int64_t first, int64_t end, const rune::view::AbortedTxnFilter& aborted,
                             rune::view::Verdict v) noexcept;
    void   endOrdinary_(const OrdinaryTxn& o) noexcept;
    /// `onLock`: paused on a row one of this source's judged transactions holds, else waiting for a lookup's marker past the HW.
    void   parkOneStep_(bool parked, bool onLock = false) noexcept {
        txnPool_->parkOneStep(oneStepClaim_, parked);
        stepParked_   = parked;
        parkedOnLock_ = onLock;
        parkedFreed_  = overlaysFreed_;
    }
    /// This source's own park still holds: a lock pause until it gives an overlay back (the locking transaction's marker applied, or
    /// its judgement ended), a lookup park until the lookup is decided.  Only then may it take the set from a parked holder.
    [[nodiscard]] bool parkHolds_() const noexcept { return stepParked_ && (parkedOnLock_ ? overlaysFreed_ == parkedFreed_ : !ordinaryWalk_.lookup.decided); }
    void   releaseOverlay_(int32_t overlay) noexcept {
        txnPool_->release(overlay);
        ++overlaysFreed_;
        refreshLocks_();   // the pool may pop this overlay
    }
    void   restartJudgement_() noexcept;
    [[nodiscard]] bool beginJudgement_(int64_t producerId, int64_t first, int64_t end, bool atMarker) noexcept;
    void   endJudgement_() noexcept;
    [[nodiscard]] bool readTxnBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, const rune::view::AbortedTxnFilter& aborted) noexcept;
    [[nodiscard]] bool scanTxnBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, const rune::view::AbortedTxnFilter& aborted) noexcept;
    [[nodiscard]] ChainStep chainRecord_(const rune::view::SourceRecord& rec, rune::view::TxnOverlay& chain, rune::view::Verdict& failed) noexcept;
    [[nodiscard]] bool judgeAtomic_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, bool keyZero) noexcept;
    [[nodiscard]] bool applyRecord_(const rune::view::SourceRecord& rec) noexcept;
    void   resetChainLive_() noexcept {
        for (Member& m : members_) m.chainLive = 0;
    }
    void   refreshLocks_() noexcept {
        if (txnPool_ != nullptr) txnPool_->lockSetFor(sourceKey_, txnLockPtrs_);
        txnLocks_.locked = txnLockPtrs_;
    }
    [[nodiscard]] const rune::view::TxnLockSet* lockSet_() const noexcept { return txnLocks_.locked.empty() ? nullptr : &txnLocks_; }
    void   notePause_(int64_t offset) noexcept;
    void   noteMarkerWithoutVote_(const OrdinaryTxn& o, bool applied) noexcept;          // cold
    void   countVote_(const TxnVoteAnswer& a) noexcept;
    [[nodiscard]] bool recordRejection_(int64_t offset, const rune::view::Verdict& v) noexcept;
    void   countRejection_(const rune::view::Verdict& v) noexcept;
    [[nodiscard]] bool indexHasRoom_(std::size_t n) noexcept;
    [[nodiscard]] rune::view::SourceRecord recordOf_(const rune::protocol::RecordSpan& span) noexcept;
    int    emitMarkers_(PartitionWriteState& state, int64_t nowNs);
    int    pushCheckpoints_(PartitionWriteState& state);
    int    dropRetired_(PartitionWriteState& state);
    bool   resolveSet_(const ConstraintSet& set, std::vector<std::size_t>& out) const;
    bool   reindex_();
    void   recomputeCap_() noexcept;
    void   discardAhead_(Member& m);
    enum class StallReason : uint8_t { None, Full, OutOfOrder, Capacity };
    void   stall_(StallReason reason) noexcept;
    void   waitForInstall_(const ConstraintSet& set) noexcept;
    int64_t extendOverEmptySets_(int64_t offset) const noexcept;
    rune::view::PrepareStatus prepare_(Member& m, const rune::view::SourceRecord& rec, rune::view::TxnOverlay* chain,
                                       const rune::view::TxnLockSet* locks) noexcept;
    /// P6-13: a rejected record of a key-0 batch burns its id; a KeyExists on one is a reused id (counted and logged).
    void   noteKeyZeroRejection_(const rune::view::Verdict& v, const rune::view::SourceRecord& rec) noexcept;
    void   apply_(Member& m, const rune::view::SourceRecord& rec) noexcept;
    /// Retention may have trimmed the log below a member's cursor: an empty member starts at the log start, one with rows is
    /// discarded (BelowLogStart), as is one with an open span that lost its first batches.  False when it was discarded.
    bool   clampToLogStart_(Member& m) {
        if (m.inst->foldCursor >= logStart_ && !m.inst->spanFence.straddles(logStart_)) return true;
        if (startAtLogStart(*m.inst, logStart_)) return true;
        if (hooks_.discard != nullptr) hooks_.discard(hooks_.ctx, *m.inst, ViewDiscardReason::BelowLogStart);
        return false;
    }
    static void discardKeyIndexDirty_(ViewInstance& inst) noexcept;
    [[nodiscard]] rune::view::Verdict verdictOf_(const Member& m, const rune::view::SourceRecord& rec) const noexcept;
    template <class ReadRangeFn>
    bool   switchSet_(int64_t changeOffset, ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted, int& work);

    int32_t                     srcTopicId_;
    int32_t                     partition_;
    PartitionWriteState*        state_ = nullptr;
    std::vector<Member>         members_;
    ConstraintSet               activeSet_;
    std::vector<std::size_t>    active_;
    std::vector<std::size_t>    scratchIdx_;
    rune::replication::PartitionReplica* replica_ = nullptr;
    static constexpr uint64_t   kUnbound = ~0ull;
    uint64_t                    generation_ = kUnbound;   // the leadership generation bound; a replica built as leader serves generation 0
    int64_t                     logStart_ = 0;
    bool                        needsActivation_ = false;
    bool                        stalled_ = false, waitingForInstall_ = false, held_ = false;
    StallReason                 stallReason_ = StallReason::None;
    std::size_t                 stallRoom_ = 1;   ///< the index entries the stalled rejection waits for
    ConstraintSetMember         waitingFor_;
    int64_t                     judgeCursor_ = 0, emitFrom_ = 0, setFrom_ = 0;
    int64_t                     checkpointCap_ = INT64_MAX;
    int64_t                     intervalNs_ = 100'000'000, lastMarkerNs_ = 0;
    uint64_t                    rejectedThisPass_ = 0, judgeCalls_ = 0;
    bool                        sawEmptyChange_ = false;
    bool                        judgedRecord_ = false;   // a data record is judged and not yet covered by a marker
    uint8_t                     lastIntent_ = 0;
    int64_t                     testNextOffset_ = 0;
    bool                        captureAppended_ = true;
    TxnOverlayPool*             txnPool_ = nullptr;
    std::span<rune::view::TxnOverlay> batchOverlay_;
    int32_t                     txnJudgedRecordsMax_ = 0;
    int64_t                     passHw_ = 0, lso_ = INT64_MAX;
    uint64_t                    membersEpoch_ = 0;        // bumped by every change of the members or of the active set
    TxnStop                     txnStop_ = TxnStop::None;
    std::size_t                 markerAt_ = 0;            // TxnStop::Marker: the entry of ordinary_ whose marker the judge stands at
    int64_t                     ordinaryApplied_ = -1;    // that entry's apply walk resumes here: its records below it are applied
    // P11-3a: the decision of a producer's batches of `epoch` in [base, to) (decided), searched from `from`; judged: the TxnJudge of
    // their transaction is at `to`, its marker still searched
    struct TxnLookup {
        int64_t producerId = -1, base = -1, from = -1, to = -1;
        int16_t epoch = -1;
        bool    judged = false, decided = false, applied = false;
    };
    // P11-3a: the walk of that entry while the judge stands at its marker.  T's records apply in segments [from, end) of at most
    // the one-step set's keys, each key with its last offset in all of T, so every segment takes the same supersede decision for a
    // key.  The set's markers hold every entry from markersFrom up to where they overflowed (refill: noted anew from checkFrom); the
    // supersede walk resumes at checkFrom (-1: the collect is not done) until checked; `lookup` decides one batch the markers cannot.
    // The collect walk resumes at collectFrom (-1: not begun), a refill at refillFrom.
    struct OrdinaryWalk {
        int64_t   marker = -1, from = -1, end = -1, collectFrom = -1, markersFrom = -1, checkFrom = -1, refillFrom = -1;
        bool      checked = false, refill = false;
        TxnLookup lookup;
    };
    OrdinaryWalk                ordinaryWalk_;
    uint64_t                    oneStepClaim_ = 0;        // the pool's one-step set holds this source's collect while the claim stands
    bool                        stepParked_ = false;      // this source last parked on the set, waiting for a marker: while that holds (parkHolds_) it takes it from no other parked holder
    bool                        parkedOnLock_ = false;
    uint64_t                    overlaysFreed_ = 0, parkedFreed_ = 0;   // overlays this source gave back, in all and when it parked
    int64_t                     failCapacityAt_ = -1;
    TxnProbe                    probe_;
    std::vector<OrdinaryTxn>    ordinary_;
    std::vector<OwedMarker>     owedMarkers_;
    std::size_t                 owedStart_ = 0;           // rotates each pass: one owed marker far behind never starves the rest
    uint64_t                    sourceKey_ = 0;           // the overlays' source key: (topic, partition)
    rune::view::TxnLockSet      txnLocks_;
    std::vector<const rune::view::TxnOverlay*> txnLockPtrs_;   // reserved to the pool's size in bindTxn: refreshLocks_ never allocates
    TxnJudgement                jx_;
    int64_t                     judgePid_ = -1;           // TxnStop::Judge: the producer whose TxnJudge the judge stands at
    int64_t                     overlayWaitAt_ = -1, lockPausedAt_ = -1;   // each wait counted once
    int64_t                     nowNs_ = 0, lockPausedNs_ = 0;
    int64_t                     finalizePid_ = -1, finalizeApplied_ = -1;   // a commit finalized part-way (a read failed)
    std::size_t                 voteScanBytes_ = kVoteScanBytesPerPass;
    std::size_t                 spanBytes_ = 0;           // what an ordinary transaction's intent, collect, supersede and apply walks may still read this pass
    VerdictViewHooks            hooks_;
    VerdictSourceStats          stats_;
    std::vector<uint8_t>        scratch_;
    std::vector<std::vector<uint8_t>> appended_;
    std::vector<rune::storage::control_record::MarkerEntry> entryScratch_;
};

/// The leadership scan feed (RcVerdictHooks): `state` is a PartitionWriteState*.
namespace cr = rune::storage::control_record;

inline constexpr std::size_t kIndexReserve = 65536;

inline void verdictFeedBegin(void* state, int64_t, int64_t target) noexcept {
    auto& s = *static_cast<PartitionWriteState*>(state);
    if (s.verdicts != nullptr) s.verdicts->rescan(s.txn);   // the overlays go back before truncateFrom resets the entries' overlay indexes
    s.constraintSets.truncateFrom(target);
    s.retainedPlans.truncateFrom(target);
    s.markers.truncateFrom(target);
    s.rejected.truncateFrom(target);
    s.txn.truncateFrom(target);
}

inline void verdictFeedBatch(void* state, const uint8_t* b, std::size_t len, int64_t base) noexcept {
    auto& s = *static_cast<PartitionWriteState*>(state);
    const auto c = cr::parseControl(b, len);   // one CRC per batch (#804)
    if (!c) return;
    if (c->type == cr::kVerdictMarker) {
        const auto m = cr::parseMarker(*c);
        if (!m) return;
        if (s.rejected.capacity() == 0) s.rejected.reserve(kIndexReserve);
        s.rejected.onMarker(m->header.fromOffset, m->header.finalThrough, m->header.count, [&](std::size_t i) {
            return rune::storage::RejectedOffsetIndex::Entry{m->offsetAt(static_cast<uint32_t>(i)), m->codeAt(static_cast<uint32_t>(i)), m->constraintIdAt(static_cast<uint32_t>(i))};
        });
        if (!s.markers.onMarker(base, m->header.finalThrough, cr::closesCoverage(m->header.count))) {   // full: the slots below the HW are committed (the scan read them in order)
            s.markers.onHw(std::min(base, s.scanHw));
            (void)s.markers.onMarker(base, m->header.finalThrough, cr::closesCoverage(m->header.count));   // still full: 1024 uncommitted markers above the HW, counted in appendedThrough
        }
    } else if (c->type == cr::kConstraintSetChange) {
        std::array<cr::ChangeMember, kMaxVerdictViewsPerSource> wire{};
        uint32_t id = 0;
        int64_t catalogThrough = 0;
        uint16_t count = 0;
        if (!cr::parseConstraintSetChange(*c, id, catalogThrough, wire, count)) return;
        ConstraintSet set;
        set.id = id;
        set.catalogThrough = catalogThrough;
        set.count = static_cast<uint8_t>(count);
        for (uint16_t i = 0; i < count; ++i)
            set.members[i] = ConstraintSetMember{wire[i].viewObjectId, static_cast<rune::view::VerdictViewKind>(wire[i].kind), wire[i].planFingerprint};
        s.constraintSets.onChange(base, set);
    } else if (c->type == cr::kTxnJudge) {
        if (const auto j = cr::parseTxnJudge(*c)) {
            if (!s.txn.reserved()) s.txn.reserve();
            (void)s.txn.onJudgeRecord(base, *j);
        }
    } else if (c->type == cr::kReservation) {
        if (const auto r = cr::parseReservation(*c)) s.autoInc.onReservation(*r);
    } else if (c->type == cr::kVerdictPlan) {
        cr::VerdictPlanHeader h{};
        std::span<const uint8_t> plan, out, src;
        if (cr::parseVerdictPlan(*c, h, plan, out, src)) s.retainedPlans.onRecord(base, h.viewObjectId, h.planFingerprint, static_cast<rune::view::VerdictViewKind>(h.kind), plan, out, src);
    }
}

/// A COMMIT / ABORT marker (RcVerdictHooks::marker).
inline void verdictFeedMarker(void* state, int64_t producerId, int64_t offset, bool commit) noexcept {
    static_cast<PartitionWriteState*>(state)->txn.onMarker(producerId, offset, commit);
}

/// verdictFeedBegin for an autoincrement source: the counter and the side window are rebuilt by the same scan.
inline void autoIncFeedBegin(void* state, int64_t logStart, int64_t target) noexcept {
    verdictFeedBegin(state, logStart, target);
    auto& s = *static_cast<PartitionWriteState*>(state);
    s.autoInc.beginScan();
    s.assignedBases.clear();
}

/// Autoincrement (P6-3): every insert in an idempotent data batch raises the highest n assigned; a batch whose first record is
/// an insert also refills its producer's window entry with the first key as the base (a key-0 batch and an import look the
/// same in the log).
inline void autoIncFeedDataBatch(void* state, const uint8_t* b, std::size_t len, int64_t base) noexcept {
    auto& s = *static_cast<PartitionWriteState*>(state);
    const rune::protocol::RecordBatchReader r(b, len);
    if (r.baseSequence() < 0) return;
    const auto partitions = static_cast<uint64_t>(s.autoInc.partitions());
    bool insertFirst = false;
    uint64_t firstKey = 0, maxN = 0;
    (void)rune::protocol::forEachRecordSpan(b, len, [&](const rune::protocol::RecordSpan& rec) {
        if (rune::protocol::record::intentOf(rec.attributes) != static_cast<uint8_t>(rune::protocol::record::Intent::Insert) ||
            rec.key.size() > sizeof(uint64_t)) return;
        const uint64_t key = keyIndexWord(rec.key);
        if (rec.index == 0) { insertFirst = true; firstKey = key; }
        maxN = std::max(maxN, key / partitions);
    });
    s.autoInc.onAssignedThrough(maxN);
    if (insertFirst) s.assignedBases.recordScanned(r.producerId(), base, firstKey);
}
/// The scan ended: the counter resumes above the latest reservation, and the reserved ids nothing took are burned.
inline void autoIncFeedEnd(void* state) noexcept {
    auto& s = *static_cast<PartitionWriteState*>(state);
    s.autoIncStats.idsBurned += s.autoInc.finishScan();
}
/// The HW moved: markers below it are committed, and the committed coverage W follows.
void verdictOnHw(PartitionWriteState& state, int64_t hw) noexcept;

template <class ReadRangeFn>
bool VerdictSource::switchSet_(int64_t A, ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted, int& work) {
    const ConstraintSet& next = *state_->constraintSets.setAt(A + 1);
    if (!resolveSet_(next, scratchIdx_)) {
        waitForInstall_(next);
        return false;
    }
    waitingForInstall_ = false;
    for (const std::size_t i : scratchIdx_) {
        if (std::find(active_.begin(), active_.end(), i) != active_.end()) continue;   // staying
        Member& m = members_[i];
        if (!checkpointSettled(*m.inst)) return false;   // its old checkpoint is being backed up: it cannot join a set yet
        if (!clampToLogStart_(m)) return false;   // retention deleted what this member folded from: it is being backed up and rebuilt
        if (m.inst->foldCursor < A + 1) {
            const rune::storage::LogReadView v = readRange(m.inst->foldCursor, A + 1);
            if (!v.empty()) work += replay_(m, v, A + 1, aborted, rune::view::spanReaderOf(readRange));
        }
        if (m.inst->foldCursor != A + 1) return false;
    }
    for (const std::size_t i : active_) {
        if (std::find(scratchIdx_.begin(), scratchIdx_.end(), i) != scratchIdx_.end()) continue;
        members_[i].retiring = true;
        members_[i].leaveOffset = A;
    }
    active_.swap(scratchIdx_);
    ++membersEpoch_;
    for (const std::size_t i : active_) {
        if (!members_[i].candidate) continue;
        members_[i].candidate = false;
        ++stats_.candidateSwaps;
    }
    activeSet_ = next;
    setFrom_ = A + 1;
    if (next.empty()) sawEmptyChange_ = true;
    return true;
}

// The candidates fold what the judge has judged, after its pass, so a candidate that was caught up at the start of a pass still is at
// the next one while writes keep flowing.
template <class ReadRangeFn>
int VerdictSource::replayCandidates_(ReadRangeFn& readRange, int64_t bound, const rune::view::AbortedTxnFilter& aborted) {
    int work = 0;
    for (Member& m : members_) {
        if (!m.candidate || !checkpointSettled(*m.inst) || !clampToLogStart_(m) || m.inst->foldCursor >= judgeCursor_) continue;
        work += replayMember_(readRange, m, bound, aborted);
    }
    return work;
}

// A member's replay to the judge's cursor, through the fold every view shares (P11-0, R11b): a transaction applies at its TxnJudge
// when it has one (D2), else at its marker, and an undecided one is never applied (judgeStep_).  P11-8 (a): a checkpoint pushed while
// the judge ran ahead of a deferred transaction holds none of its records, so a TxnJudge whose firstOffset lies below where the replay
// began applies the whole transaction from there.
template <class ReadRangeFn>
int VerdictSource::replayMember_(ReadRangeFn& readRange, Member& m, int64_t bound, const rune::view::AbortedTxnFilter& aborted) {
    ViewInstance& inst = *m.inst;
    if (m.replayFrom < 0 || inst.foldCursor < m.replayFrom) m.replayFrom = inst.foldCursor;   // a discard refolds from below
    // A read stops at the next segment base at or above its end: a compacted gap whose next segment starts at judgeCursor_ reads
    // empty to judgeCursor_, and still to the bound when judgeCursor_ == bound.  So read to the bound and at least one past it.
    const rune::storage::LogReadView v = readRange(inst.foldCursor, std::max(bound, judgeCursor_ + 1));
    if (v.empty()) return 0;
    const int work = replay_(m, v, judgeCursor_, aborted, rune::view::spanReaderOf(readRange));
    if (inst.foldCursor >= judgeCursor_) m.replayFrom = -1;
    return work;
}

// fn(reader, batch, len) for every batch with an offset in [from, to); fn returns false to stop.  False: a read came back short.
template <class ReadRangeFn, class Fn>
bool VerdictSource::walkBatches_(ReadRangeFn& readRange, int64_t from, int64_t to, Fn&& fn) noexcept {
    for (int64_t at = from; at < to;) {
        const rune::storage::LogReadView v = readRange(at, to);
        if (v.empty()) return false;
        const int64_t start = at;
        for (std::size_t pos = 0; pos < v.len;) {
            const rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
            if (!r.ensureValid()) break;
            const int64_t last = r.baseOffset() + r.lastOffsetDelta();
            if (r.baseOffset() >= to) return true;
            const auto len = static_cast<std::size_t>(r.totalLength());
            if (last >= at) {
                if (!fn(r, v.data + pos, len)) return true;
                at = last + 1;
            }
            pos += len;
        }
        if (at == start) return false;
    }
    return true;
}

// P11-2: the judge stands at the first record of probe_'s transaction, which has neither a committed TxnJudge it knows nor a marker.
// Looks ahead below the HW for either, at most kTxnProbeBytesPerPass per pass, resuming where it stopped.  True: classified.
template <class ReadRangeFn>
bool VerdictSource::probeTxn_(ReadRangeFn& readRange, int64_t bound) noexcept {
    std::size_t budget = kTxnProbeBytesPerPass;
    ProbeStep step = ProbeStep::Next;
    const bool read = walkBatches_(readRange, probe_.from, bound, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
        step = probeBatch_(r, b, len);
        if (step != ProbeStep::Next) return false;
        probe_.from = r.baseOffset() + r.lastOffsetDelta() + 1;
        budget -= std::min(budget, len);
        return budget > 0;
    });
    if (step == ProbeStep::Next && read && budget > 0) probe_.from = std::max(probe_.from, bound);
    if (step != ProbeStep::Classified) return false;
    probe_ = {};
    return true;
}

// P11-2 / P11-3a: an ordinary transaction is applied at its marker, its records in offset order, each key that a record of another
// writer applied before the marker supersedes applying nothing.  An aborted one is dropped.  More keys than the one-step set holds
// apply in segments, one per pass; more markers than it holds are noted in windows.  Each pass reads the span a bounded number of
// times, and the decision of another transaction's batch the window cannot give is searched voteScanBytes_ per pass.
template <class ReadRangeFn>
bool VerdictSource::applyOrdinary_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept {
    const OrdinaryTxn o = ordinary_[markerAt_];
    if (o.commit) {
        TxnKeySet& keys = oneStepKeys_();
        OrdinaryWalk& w = ordinaryWalk_;
        if (w.marker != o.marker) w = {o.marker, o.first};
        if (!txnPool_->holdsOneStep(oneStepClaim_)) {
            if (!txnPool_->oneStepClaimable()) return false;   // another source is mid-segment: wait for it
            if (txnPool_->oneStepHeld() && stepParked_) {   // parked as this one is: take the set only once this one's own wait is over
                if (!parkedOnLock_ && !w.lookup.decided) (void)lookupTxn_(readRange, o.marker);   // reads the log and the lookup, not the set
                if (parkHolds_()) return false;
            }
            oneStepClaim_ = txnPool_->claimOneStep();
            stepParked_ = false;
            w.collectFrom = -1;   // another source may have taken the set mid-segment: note the keys again
        }
        if (w.collectFrom < 0) {
            keys.clear();
            w.end = w.checkFrom = -1;
            w.collectFrom = w.from;
        }
        if (w.checkFrom < 0) {   // the collect is not done
            bool more = false;
            if (!walkBatches_(readRange, w.collectFrom, o.marker, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
                    collectOrdinaryBatch_(r, b, len, o.producerId, keys);
                    w.collectFrom = r.baseOffset() + r.lastOffsetDelta() + 1;
                    spanBytes_ -= std::min(spanBytes_, len);
                    more = spanBytes_ == 0;
                    return !more;
                }) || more) return false;
            if (w.end < 0) w.end = o.marker;
            w.markersFrom = w.checkFrom = w.from;
            w.refillFrom = -1;
            w.checked = w.refill = false;
        }
        if (!w.checked && !checkOrdinary_(readRange, o, aborted, keys)) return false;
        parkOneStep_(false);   // applyTxnBatch_ parks it again on a row a judged transaction holds
        // The apply walk shares the pass's budget too: it resumes past every batch it walked, plain ones included, as a Locked stop
        // resumes past the last record it applied.
        bool applied = true, more = false;
        if (!walkBatches_(readRange, std::max(w.from, ordinaryApplied_), w.end, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
                if (r.producerId() == o.producerId && (r.attributes() & (rune::protocol::batch::kAttrTransactional | rune::protocol::batch::kAttrControl)) == rune::protocol::batch::kAttrTransactional) {
                    applied = applyTxnBatch_(b, len, keys, w.end);
                    if (!applied) return false;
                }
                ordinaryApplied_ = std::max(ordinaryApplied_, std::min(r.baseOffset() + r.lastOffsetDelta() + 1, w.end));
                spanBytes_ -= std::min(spanBytes_, len);
                more = spanBytes_ == 0;
                return !more;
            }) || !applied || more) return false;
        if (w.end < o.marker) {
            if (w.from == o.first) ++state_->txn.stats.supersedeKeysOverflow;
            ordinaryApplied_ = std::max(ordinaryApplied_, w.end);
            w = {o.marker, w.end};
            return false;
        }
    }
    endOrdinary_(o);
    return true;
}

// The supersede walk of the segment from checkFrom, after the step that its last stop asked for: a fresh marker list from there, or
// the lookup of one batch's decision.  True: the segment is checked.
template <class ReadRangeFn>
bool VerdictSource::checkOrdinary_(ReadRangeFn& readRange, const OrdinaryTxn& o, const rune::view::AbortedTxnFilter& aborted, TxnKeySet& keys) noexcept {
    OrdinaryWalk& w = ordinaryWalk_;
    if (w.refill) {
        if (w.refillFrom < 0) {
            keys.clearMarkers();
            w.refillFrom = w.checkFrom;
        }
        bool more = false;
        if (!walkBatches_(readRange, w.refillFrom, o.marker, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
                if ((r.attributes() & rune::protocol::batch::kAttrControl) != 0) noteControl_(r, b, len, o.producerId, keys);
                if (keys.markersOverflowed()) return false;
                w.refillFrom = r.baseOffset() + r.lastOffsetDelta() + 1;
                spanBytes_ -= std::min(spanBytes_, len);
                more = spanBytes_ == 0;
                return !more;
            }) || more) return false;
        w.markersFrom = w.checkFrom;
        w.refillFrom = -1;
        w.refill = false;
    } else if (w.lookup.producerId >= 0 && !w.lookup.decided && !lookupTxn_(readRange, o.marker)) {
        return false;
    }
    bool stopped = false;
    if (!walkBatches_(readRange, w.checkFrom, o.marker, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
            if (!checkOrdinaryBatch_(r, b, len, o.producerId, aborted, keys)) {
                stopped = true;
                return false;
            }
            w.checkFrom = r.baseOffset() + r.lastOffsetDelta() + 1;
            spanBytes_ -= std::min(spanBytes_, len);
            stopped = spanBytes_ == 0;
            return !stopped;
        }) || stopped) return false;
    w.checked = true;
    return true;
}

// The first marker or TxnJudge of lookup's producer above its batch below `end` that decides it, then a TxnJudge's marker below the
// HW (TxnKeySet::appliedAt's rule), at most voteScanBytes_ per pass.  True: decided.
template <class ReadRangeFn>
bool VerdictSource::lookupTxn_(ReadRangeFn& readRange, int64_t end) noexcept {
    namespace cm = rune::storage::control_marker;
    constexpr int16_t kTxnControl = rune::protocol::batch::kAttrTransactional | rune::protocol::batch::kAttrControl;
    TxnLookup& l = ordinaryWalk_.lookup;
    std::size_t budget = voteScanBytes_;
    (void)walkBatches_(readRange, l.from, passHw_, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
        const int16_t attrs = r.attributes();
        if (!l.judged && r.baseOffset() >= end) {   // not decided below the end: not applied there
            l.to = end;
            l.decided = true;
            return false;
        }
        if ((attrs & kTxnControl) == kTxnControl) {
            if (const int type = cm::controlType(b, len); r.producerId() == l.producerId && type != cm::kUnparseable) {
                if (!l.judged) l.to = r.baseOffset();
                l.decided = true;
                l.applied = type == cm::kCommit;
            }
        } else if ((attrs & rune::protocol::batch::kAttrControl) != 0 && !l.judged && len == cr::kTxnJudgeBatchBytes) {
            if (const auto j = cr::parseTxnJudge(b, len); j && j->producerId == l.producerId && j->producerEpoch == l.epoch && j->firstOffset <= l.base) {
                l.judged = true;
                l.to = r.baseOffset();
            }
        }
        l.from = r.baseOffset() + r.lastOffsetDelta() + 1;
        budget -= std::min(budget, len);
        return !l.decided && budget > 0;
    });
    if (txnPool_->holdsOneStep(oneStepClaim_)) parkOneStep_(!l.decided && l.from >= passHw_);   // scanned to the HW: only T's marker decides it
    return l.decided;
}

// F10: a re-offered TxnJudge's marker the feed dropped is found below the HW, kTxnProbeBytesPerPass per pass in all, resuming where it
// stopped.  Until then T is undecided and holds judgedThrough().
template <class ReadRangeFn>
void VerdictSource::feedOwedMarkers_(ReadRangeFn& readRange, int64_t bound) noexcept {
    pruneOwedMarkers_();
    std::size_t budget = kTxnProbeBytesPerPass;
    const std::size_t n = owedMarkers_.size();
    const std::size_t start = n == 0 ? 0 : owedStart_++ % n;
    for (std::size_t k = 0; k < n && budget > 0; ++k) {
        OwedMarker& m = owedMarkers_[(start + k) % n];
        (void)walkBatches_(readRange, m.from, bound, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
            if (owedMarkerBatch_(m, r, b, len)) return false;
            m.from = r.baseOffset() + r.lastOffsetDelta() + 1;
            budget -= std::min(budget, len);
            return budget > 0;
        });
    }
    pruneOwedMarkers_();
}

// P11-4 / P11-7: the chained read of [first, end), then the P11-3 scan of (first, end), each at most voteScanBytes_ per pass and
// resuming where it stopped.  True: jx_ is answered.
template <class ReadRangeFn>
bool VerdictSource::runJudgement_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept {
    if (jx_.members != membersEpoch_) restartJudgement_();   // the set it was judged with changed: judged anew from the first record
    if (jx_.done) return true;
    std::size_t budget = voteScanBytes_;
    bool stop = false;   // the answer, a pause or a stall
    const auto walk = [&](auto&& batchFn) {
        budget = voteScanBytes_;
        return walkBatches_(readRange, jx_.from, jx_.end, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
            if (!batchFn(r, b, len)) { stop = true; return false; }
            jx_.from = r.baseOffset() + r.lastOffsetDelta() + 1;
            budget -= std::min(budget, len);
            return budget > 0;
        });
    };
    if (!jx_.scanning) {
        const bool read = walk([&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) { return readTxnBatch_(r, b, len, aborted); });
        if (stop || !read || budget == 0) return jx_.done;
        jx_.scanning = true;
        jx_.from = jx_.first + 1;
        jx_.resolveFrom = jx_.end;
    }
    if (jx_.resolvePid >= 0) {
        TxnKeySet& keys = txnPool_->keysAt(jx_.overlay);
        if (!resolveMarkers_(readRange, keys, jx_.resolveFrom, voteScanBytes_, jx_.resolvePid)) return false;
        jx_.resolvePid = -1;
    }
    const bool read = walk([&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) { return scanTxnBatch_(r, b, len, aborted); });
    if (!stop && read && budget > 0) jx_.done = true;   // no conflict: yes
    return jx_.done;
}

// The markers past the end of a key set's range that decide its pending TxnJudges, from `from` to the HW, at most `budget` bytes.
// True: `producerId`'s is noted (any producer's with -1: none is pending), or the marker list overflowed.
template <class ReadRangeFn>
bool VerdictSource::resolveMarkers_(ReadRangeFn& readRange, TxnKeySet& keys, int64_t& from, std::size_t budget, int64_t producerId) noexcept {
    namespace cm = rune::storage::control_marker;
    constexpr int16_t kTxnControl = rune::protocol::batch::kAttrTransactional | rune::protocol::batch::kAttrControl;
    const auto done = [&] { return keys.markersOverflowed() || (producerId < 0 ? !keys.pending() : !keys.awaits(producerId)); };
    if (done()) return true;
    bool found = false;
    (void)walkBatches_(readRange, from, passHw_, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
        if ((r.attributes() & kTxnControl) == kTxnControl)
            if (const int type = cm::controlType(b, len); type != cm::kUnparseable && keys.resolve(r.producerId(), r.baseOffset(), type == cm::kCommit)) found = done();
        from = r.baseOffset() + r.lastOffsetDelta() + 1;
        budget -= std::min(budget, len);
        return !found && budget > 0;
    });
    return found;
}

// P11-4: the judge stands at T's TxnJudge.  A yes locks T's rows in the overlay; a no releases it.  True: judged, the judge passed J.
template <class ReadRangeFn>
bool VerdictSource::judgeTxn_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept {
    DeferredTxn& t = *state_->txn.find(judgePid_);
    if (jx_.atMarker || jx_.producerId != t.producerId || jx_.end != t.judgeOffset) {
        if (!beginJudgement_(t.producerId, t.firstOffset, t.judgeOffset, false)) return false;
        t.overlay = jx_.overlay;
        t.phase = TxnPhase::Judging;
    }
    if (!runJudgement_(readRange, aborted)) return false;
    if (jx_.endedAt >= 0) {   // T ended before its J (abort before J): decided from the log, nothing to judge; a parked vote is answered NotLeader
        releaseOverlay_(t.overlay);
        t.overlay = -1;
        t.markerOffset = jx_.endedAt;   // decided, as endsBeforeJudge_ decides it: the producer's next TxnJudge is accepted
        t.commit = jx_.endedCommit;
        t.phase = TxnPhase::Finalized;
        endJudgement_();
        judgeCursor_ = t.judgeOffset + 1;
        txnStop_ = TxnStop::None;
        syncMemberCursors_();
        return true;
    }
    t.answer = jx_.answer;
    t.judged = true;
    countVote_(t.answer);
    if (t.answer.code == 0) {
        t.phase = TxnPhase::Locked;
    } else {
        releaseOverlay_(t.overlay);
        t.overlay = -1;
        t.phase = TxnPhase::Finalized;
    }
    endJudgement_();
    judgeCursor_ = t.judgeOffset + 1;
    txnStop_ = TxnStop::None;
    syncMemberCursors_();
    return true;
}

// P11-1 (a), (b) or (c) on the log: a committed transaction without a TxnJudge that should have voted (1), or not (0).  -1: the
// intent search's read failed or used up spanBytes_, the transaction stays unclassified and the search resumes at intentFrom.
template <class ReadRangeFn>
int8_t VerdictSource::shouldHaveVoted_(ReadRangeFn& readRange, OrdinaryTxn& o) noexcept {
    const auto constrained = [](const ConstraintSet& s) {
        return std::any_of(s.members.begin(), s.members.begin() + s.count, [](const ConstraintSetMember& m) {
            return m.kind == rune::view::VerdictViewKind::Unique || m.kind == rune::view::VerdictViewKind::Check || m.kind == rune::view::VerdictViewKind::TestReject;
        });
    };
    if (constrained(*state_->constraintSets.setAt(o.first)) || constrained(*state_->constraintSets.setAt(o.marker))) return 1;   // the log's sets up to M, never one registered past it
    bool intent = false, more = false;
    if (!walkBatches_(readRange, std::max(o.first, o.intentFrom), o.marker, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
        if (r.producerId() == o.producerId && (r.attributes() & (rune::protocol::batch::kAttrTransactional | rune::protocol::batch::kAttrControl)) == rune::protocol::batch::kAttrTransactional)
            (void)rune::protocol::forEachRecordSpan(b, len, [&](const rune::protocol::RecordSpan& span) { intent = intent || rune::protocol::record::intentOf(span.attributes) != 0; });
        o.intentFrom = r.baseOffset() + r.lastOffsetDelta() + 1;
        spanBytes_ -= std::min(spanBytes_, len);
        more = !intent && spanBytes_ == 0;
        return !intent && !more;
    }) || more) return -1;
    return intent ? 1 : 0;
}

// P11-7: T committed without a TxnJudge and should have voted.  It is judged at its marker, all or nothing: a pass applies it, a
// failure rejects every record of it.  Either way counted and logged.
template <class ReadRangeFn>
bool VerdictSource::judgeAtMarker_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept {
    const OrdinaryTxn o = ordinary_[markerAt_];
    if (!jx_.atMarker || jx_.producerId != o.producerId || jx_.end != o.marker) {
        if (!beginJudgement_(o.producerId, o.first, o.marker, true)) return false;
    }
    if (!runJudgement_(readRange, aborted)) return false;
    if (jx_.answer.code == 0 ? !applyTxnRecords_(readRange, o.producerId, o.first, o.marker, aborted, ordinaryApplied_)
                             : !rejectTxnRecords_(readRange, o.producerId, o.first, o.marker, aborted, {jx_.answer.code, jx_.answer.constraintId}))
        return false;
    noteMarkerWithoutVote_(o, jx_.answer.code == 0);
    releaseOverlay_(jx_.overlay);
    endJudgement_();
    endOrdinary_(o);
    return true;
}

// T's records in [max(first, applied), end), through every member's normal path.  `applied` keeps the progress when a read fails.
template <class ReadRangeFn>
bool VerdictSource::applyTxnRecords_(ReadRangeFn& readRange, int64_t producerId, int64_t first, int64_t end, const rune::view::AbortedTxnFilter& aborted,
                                     int64_t& applied) noexcept {
    bool ok = true;
    return walkBatches_(readRange, std::max(first, applied), end, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
        const int16_t attrs = r.attributes();
        if (r.producerId() != producerId || (attrs & (rune::protocol::batch::kAttrTransactional | rune::protocol::batch::kAttrControl)) != rune::protocol::batch::kAttrTransactional ||
            aborted.skips(r, attrs)) return true;
        (void)rune::protocol::forEachRecordSpan(b, len, [&](const rune::protocol::RecordSpan& span) {
            if (!ok || span.offset < applied) return;
            ok = applyRecord_(recordOf_(span));
            if (ok) applied = span.offset + 1;
        });
        return ok;
    }) && ok;
}

// P11-7's failure, or a refused ordinary transaction: every record of T in [first, end) goes into the index (cold, out of order) with `v`.
template <class ReadRangeFn>
bool VerdictSource::rejectTxnRecords_(ReadRangeFn& readRange, int64_t producerId, int64_t first, int64_t end, const rune::view::AbortedTxnFilter& aborted,
                                      const rune::view::Verdict v) noexcept {
    const auto each = [&](auto&& fn) {
        return walkBatches_(readRange, first, end, [&](const rune::protocol::RecordBatchReader& r, const uint8_t* b, std::size_t len) {
            const int16_t attrs = r.attributes();
            if (r.producerId() != producerId || (attrs & (rune::protocol::batch::kAttrTransactional | rune::protocol::batch::kAttrControl)) != rune::protocol::batch::kAttrTransactional ||
                aborted.skips(r, attrs)) return true;
            (void)rune::protocol::forEachRecordSpan(b, len, fn);
            return true;
        });
    };
    std::size_t n = 0;
    if (!each([&](const rune::protocol::RecordSpan&) { ++n; })) return false;
    if (!indexHasRoom_(n)) return false;
    std::array<int64_t, 256> run{};
    std::size_t used = 0;
    const auto flush = [&] {
        (void)state_->rejected.insertRun(std::span<const int64_t>(run.data(), used), v.code, v.constraintId);   // fits: checked above
        used = 0;
    };
    if (!each([&](const rune::protocol::RecordSpan& span) {
            run[used++] = span.offset;
            countRejection_(v);
            if (used == run.size()) flush();
        })) return false;
    flush();
    judgedRecord_ = true;
    return true;
}

// P11-4: every Locked transaction whose marker is below the HW, before any further judging: a commit re-applies its records through
// each member's normal path in this one step, an abort only gives the overlay back.  False: a read failed, the judge stalled, or a
// commit at least the checkpoint cap long was applied (nothing more is judged in this pass, so its delta holds that one).
template <class ReadRangeFn>
bool VerdictSource::finalizeLocked_(ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted) noexcept {
    bool any = false;
    while (DeferredTxn* t = state_->txn.lockedDecided(passHw_)) {
        if (t->commit) {
            if (finalizePid_ != t->producerId) {
                finalizePid_ = t->producerId;
                finalizeApplied_ = -1;
            }
            if (!applyTxnRecords_(readRange, t->producerId, t->firstOffset, t->judgeOffset, aborted, finalizeApplied_)) return false;
        }
        releaseOverlay_(t->overlay);
        t->overlay = -1;
        t->phase = TxnPhase::Finalized;
        closeSpan_(t->producerId);   // the step's postcondition: T was applied to every active member (no span is open here today)
        finalizePid_ = finalizeApplied_ = -1;
        any = true;
        if (checkpointCap_ != INT64_MAX && t->records >= checkpointCap_) {   // a delta holds at most one transaction past the cap
            refreshLocks_();
            syncMemberCursors_();
            return false;
        }
    }
    if (any) {
        refreshLocks_();
        syncMemberCursors_();
    }
    return true;
}

template <class ReadRangeFn>
int VerdictSource::driveJudge(ReadRangeFn&& readRange, int64_t bound, int64_t nowNs, PartitionWriteState& state,
                              const rune::view::AbortedTxnFilter& aborted, int64_t lso) {
    if (stalled_ || held_) return 0;
    state_ = &state;
    int work = 0;
    lso_ = lso;
    if (needsActivation_) { activate_(state); ++work; }
    passHw_ = bound;
    nowNs_ = nowNs;
    spanBytes_ = voteScanBytes_;
    if (!owedMarkers_.empty()) feedOwedMarkers_(readRange, bound);
    state.txn.finalizeDecided(state.rejected.coveredThrough());
    rejectedThisPass_ = 0;
    for (const std::size_t i : active_) {
        Member& m = members_[i];
        if (!checkpointSettled(*m.inst)) continue;   // behind (below): the judge neither folds nor passes it
        if (!clampToLogStart_(m)) continue;   // below the log start with rows: discarded, unsettled until the backup is acked
        if (m.inst->foldCursor >= judgeCursor_) continue;
        work += replayMember_(readRange, m, bound, aborted);
    }
    bool behind = false;
    for (const std::size_t i : active_) behind = behind || !checkpointSettled(*members_[i].inst) || members_[i].inst->foldCursor < judgeCursor_;
    const bool ready = activeSet_.count == active_.size() || reindex_();
    if (!ready) {
        waitForInstall_(activeSet_);
    } else if (!behind && finalizeLocked_(readRange, aborted)) {
        // The checkpoint cap holds the judge at the markers' coverage plus the cap.  While a deferred transaction holds K the markers
        // wait for it and the judge runs ahead, the members pushing there (P11-8 (a)): the cap counts from their last push.  An
        // ordinary one is applied at its marker, which the judge must reach.  While nothing judged waits for a marker (only control
        // records or an aborted transaction past the coverage) no marker is due and the members' tables did not change: the cap
        // counts from the cursor.
        const bool ordinaryHolds = std::any_of(ordinary_.begin(), ordinary_.end(), [&](const OrdinaryTxn& o) { return o.first < judgeCursor_; });
        const int64_t capFrom = judgedThrough() < judgeCursor_ ? runAheadFrom_() : judgedRecord_ || sawEmptyChange_ ? emitFrom_ : judgeCursor_;
        const int64_t passBound = std::min(bound, checkpointCap_ == INT64_MAX || ordinaryHolds || capFrom == INT64_MAX ? INT64_MAX : capFrom + checkpointCap_);
        while (!stalled_) {
            const int64_t A = state.constraintSets.nextChangeAtOrAfter(setFrom_);
            if (A != INT64_MAX && judgeCursor_ > A) {
                if (!switchSet_(A, readRange, aborted, work)) break;
                ++work;
                continue;
            }
            if (judgeCursor_ >= passBound) break;
            const int64_t limit = A == INT64_MAX ? passBound : std::min(passBound, A + 1);
            if (active_.empty()) {   // nothing judges an empty set, but an open transaction is passed only once it is decided
                const int64_t to = std::min(limit, std::max(judgeCursor_, lso_));
                if (to <= judgeCursor_) break;
                judgeCursor_ = to;
                ++work;
                continue;
            }
            // A unit (an atomic batch, a transaction's batch) that starts below the checkpoint cap is passed whole: one larger than
            // the cap would wait at it forever.  A delta holds at most the cap plus one unit.
            const int64_t hardLimit = A == INT64_MAX ? bound : std::min(bound, A + 1);
            const rune::storage::LogReadView v = readRange(judgeCursor_, limit);
            if (v.empty()) break;
            const int64_t before = judgeCursor_;
            work += judgeBatches_(v, limit, hardLimit, aborted);
            if (txnStop_ == TxnStop::Probe) {
                if (!probeTxn_(readRange, bound)) break;
                ++work;
                continue;
            }
            if (txnStop_ == TxnStop::Judge) {
                if (!judgeTxn_(readRange, aborted)) break;
                ++work;
                continue;
            }
            if (txnStop_ == TxnStop::Marker) {
                OrdinaryTxn& o = ordinary_[markerAt_];
                if (o.vote < 0) o.vote = !o.commit ? 0 : shouldHaveVoted_(readRange, o);
                if (o.vote < 0) break;
                const int64_t span = o.marker - o.first;
                if (!(o.vote == 1 ? judgeAtMarker_(readRange, aborted) : applyOrdinary_(readRange, aborted))) break;
                ++work;
                if (checkpointCap_ != INT64_MAX && span >= checkpointCap_) break;   // one step past the cap: its delta holds it alone
                continue;
            }
            if (txnStop_ == TxnStop::Fold || txnStop_ == TxnStop::Locked || judgeCursor_ == before) break;
        }
        state.rejected.noteCovered(judgedThrough());
    }
    work += replayCandidates_(readRange, bound, aborted);
    state.judgedThrough = judgedThrough();
    work += emitMarkers_(state, nowNs);
    work += pushCheckpoints_(state);
    work += dropRetired_(state);
    return work;
}

}  // namespace rune::node
