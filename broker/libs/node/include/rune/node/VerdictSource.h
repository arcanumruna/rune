#pragma once
// VerdictSource.h — checked writes Part 04: the judge of one source partition.
//
// A verdict source owns the verdict views of its partition.  judge() prepares every active member for a record,
// evaluates them in set order, and applies all or none; a rejection goes into PartitionWriteState::rejected.  driveJudge
// walks the committed log, follows the logged constraint-set changes and emits durable verdict markers.
#include <rune/node/VerdictState.h>
#include <rune/node/ViewInstance.h>
#include <rune/node/WriteState.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/ProducerBatchWindow.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace rune::node {

// Checked writes Part 05: the key index, a verdict member over a one-byte-row table keyed by the packed wire key.
inline constexpr uint64_t    kKeyIndexPlanFingerprint      = 0x4B45'5949'4E44'4558ull;   // "KEYINDEX"
inline constexpr std::size_t kKeyIndexChunkBytes           = 64u << 10;
inline constexpr int64_t     kKeyIndexReplayRecordsPerPass = 65536;

struct KeyIndexFold {
    rune::view::ViewTable& table;
    uint8_t                keyWidth = 0;
};

using rune::storage::keyIndexWord;
[[nodiscard]] rune::view::PrepareStatus prepareKeyIndex(const KeyIndexFold&, const rune::view::SourceRecord&, rune::view::Prospect&) noexcept;
/// The intent table (P5-3) over the key state the prospect holds; a Malformed prospect is accepted.
[[nodiscard]] rune::view::Verdict evaluateIntent(const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
void applyKeyIndex(KeyIndexFold&, const rune::view::SourceRecord&, const rune::view::Prospect&) noexcept;
/// foldBatch's contract (batch, bound, cursor, corrupt tail) over the accepted records: nothing is judged.
rune::view::FoldStats foldKeyIndexBatch(KeyIndexFold&, const rune::storage::LogReadView&, int64_t bound, int64_t& cursor,
                                        const rune::view::FoldSkip&) noexcept;
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

class VerdictSource {
public:
    static constexpr int64_t kChangeDeferred = -1, kChangeUnchanged = -2;

    /// What WorkerViewSet::syncConstraintSet_ last reconciled this judge against: the template epoch and the latest logged change.
    uint64_t constraintSyncEpoch = UINT64_MAX;
    int64_t  constraintSyncChange = -2;
    int64_t  constraintSyncCatalog = -2;   ///< WorkerViewSet::catalogThrough_ at that sync

    VerdictSource(int32_t srcTopicId, int32_t partition);

    /// The partition state judge() records rejections in and driveJudge() reads the timeline from.
    void bindState(PartitionWriteState* state) noexcept { state_ = state; }
    [[nodiscard]] PartitionWriteState* boundState() const noexcept { return state_; }

    /// Prepares every member of the active set for `rec`, evaluates them in set order, applies all or none.  A rejection
    /// is added to the index and returned.  A full index or an exhausted prospect sets stalled() and applies nothing.
    [[nodiscard]] rune::view::Verdict judge(const rune::view::SourceRecord& rec) noexcept;

    /// One duty-pass of the judge over the committed range below `bound`.  `readRange(from, to)` returns whole batches
    /// as a LogReadView.  Returns a progress count.
    template <class ReadRangeFn>
    int driveJudge(ReadRangeFn&& readRange, int64_t bound, int64_t nowNs, PartitionWriteState& state,
                   const rune::view::AbortedTxnFilter& aborted = {});

    [[nodiscard]] int64_t judgedThrough() const noexcept { return judgeCursor_; }
    [[nodiscard]] int32_t partition() const noexcept { return partition_; }
    [[nodiscard]] bool stalled() const noexcept { return stalled_; }
    /// Cold, the owner after growing the index: resumes a judge that stalled on a full index.  The other stalls stay.
    void unstall() noexcept {
        if (stallReason_ == StallReason::Full && state_ != nullptr && state_->rejected.size() < state_->rejected.capacity()) stalled_ = false;
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
        return {judgeCursor_, stalled_, static_cast<int>(stallReason_), waitingForInstall_, needsActivation_, activeSet_.id, members_.size(), active_.size(), generation_};
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
        bool                        retiring = false, leaving = false, candidate = false;
        int64_t                     leaveOffset = -1;
        std::vector<uint8_t>        planRecordPayload;   // the type-20 value requestChange appends when the member joins
    };

    void   addMember_(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint, bool candidate,
                      std::span<const uint8_t> planRecordPayload = {});
    template <class ReadRangeFn>
    int    replayCandidates_(ReadRangeFn& readRange, int64_t bound, const rune::view::AbortedTxnFilter& aborted);
    void   activate_(PartitionWriteState& state);
    int    replay_(Member& m, const rune::storage::LogReadView& view, int64_t upTo, const rune::view::AbortedTxnFilter& aborted);
    int    judgeBatches_(const rune::storage::LogReadView& view, int64_t limit, const rune::view::AbortedTxnFilter& aborted);
    int    emitMarkers_(PartitionWriteState& state, int64_t nowNs);
    int    pushCheckpoints_(PartitionWriteState& state);
    int    dropRetired_(PartitionWriteState& state);
    bool   resolveSet_(const ConstraintSet& set, std::vector<std::size_t>& out) const;
    bool   reindex_();
    void   recomputeCap_() noexcept;
    void   discardCheckpointAhead_(Member& m);
    enum class StallReason : uint8_t { None, Full, OutOfOrder, Capacity };
    void   stall_(StallReason reason) noexcept;
    void   waitForInstall_(const ConstraintSet& set) noexcept;
    int64_t extendOverEmptySets_(int64_t offset) const noexcept;
    rune::view::PrepareStatus prepare_(Member& m, const rune::view::SourceRecord& rec) noexcept;
    /// P6-13: a rejected record of a key-0 batch burns its id; a KeyExists on one is a reused id (counted and logged).
    void   noteKeyZeroRejection_(const rune::view::Verdict& v, const rune::view::SourceRecord& rec) noexcept;
    void   apply_(Member& m, const rune::view::SourceRecord& rec) noexcept;
    /// Retention may have trimmed the log below a member's cursor: an empty member starts at the log start, one with rows is
    /// discarded (BelowLogStart).  False when it was discarded.
    bool   clampToLogStart_(Member& m) {
        if (m.inst->foldCursor >= logStart_) return true;
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
    s.constraintSets.truncateFrom(target);
    s.retainedPlans.truncateFrom(target);
    s.markers.truncateFrom(target);
    s.rejected.truncateFrom(target);
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
        if (!s.markers.onMarker(base, m->header.finalThrough)) {   // full: the slots below the HW are committed (the scan read them in order)
            s.markers.onHw(std::min(base, s.scanHw));
            (void)s.markers.onMarker(base, m->header.finalThrough);   // still full: 1024 uncommitted markers above the HW, counted in appendedThrough
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
    } else if (c->type == cr::kReservation) {
        if (const auto r = cr::parseReservation(*c)) s.autoInc.onReservation(*r);
    } else if (c->type == cr::kVerdictPlan) {
        cr::VerdictPlanHeader h{};
        std::span<const uint8_t> plan, out, src;
        if (cr::parseVerdictPlan(*c, h, plan, out, src)) s.retainedPlans.onRecord(base, h.viewObjectId, h.planFingerprint, static_cast<rune::view::VerdictViewKind>(h.kind), plan, out, src);
    }
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
            if (!v.empty()) work += replay_(m, v, A + 1, aborted);
        }
        if (m.inst->foldCursor != A + 1) return false;
    }
    for (const std::size_t i : active_) {
        if (std::find(scratchIdx_.begin(), scratchIdx_.end(), i) != scratchIdx_.end()) continue;
        members_[i].retiring = true;
        members_[i].leaveOffset = A;
    }
    active_.swap(scratchIdx_);
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
        const rune::storage::LogReadView v = readRange(m.inst->foldCursor, std::max(bound, judgeCursor_ + 1));
        if (!v.empty()) work += replay_(m, v, judgeCursor_, aborted);
    }
    return work;
}

template <class ReadRangeFn>
int VerdictSource::driveJudge(ReadRangeFn&& readRange, int64_t bound, int64_t nowNs, PartitionWriteState& state,
                              const rune::view::AbortedTxnFilter& aborted) {
    if (stalled_ || held_) return 0;
    state_ = &state;
    int work = 0;
    if (needsActivation_) { activate_(state); ++work; }
    rejectedThisPass_ = 0;
    for (const std::size_t i : active_) {
        Member& m = members_[i];
        if (!checkpointSettled(*m.inst)) continue;   // behind (below): the judge neither folds nor passes it
        if (!clampToLogStart_(m)) continue;   // below the log start with rows: discarded, unsettled until the backup is acked
        if (m.inst->foldCursor >= judgeCursor_) continue;
        // A read stops at the next segment base at or above its end: a compacted gap whose next segment starts at
        // judgeCursor_ reads empty to judgeCursor_, and still to the bound when judgeCursor_ == bound.  So read to the
        // bound and at least one past judgeCursor_.
        const rune::storage::LogReadView v = readRange(m.inst->foldCursor, std::max(bound, judgeCursor_ + 1));
        if (!v.empty()) work += replay_(m, v, judgeCursor_, aborted);
    }
    bool behind = false;
    for (const std::size_t i : active_) behind = behind || !checkpointSettled(*members_[i].inst) || members_[i].inst->foldCursor < judgeCursor_;
    const bool ready = activeSet_.count == active_.size() || reindex_();
    if (!ready) {
        waitForInstall_(activeSet_);
    } else if (!behind) {
        const int64_t passBound = std::min(bound, checkpointCap_ == INT64_MAX ? INT64_MAX : emitFrom_ + checkpointCap_);
        while (!stalled_) {
            const int64_t A = state.constraintSets.nextChangeAtOrAfter(setFrom_);
            if (A != INT64_MAX && judgeCursor_ > A) {
                if (!switchSet_(A, readRange, aborted, work)) break;
                ++work;
                continue;
            }
            if (judgeCursor_ >= passBound) break;
            const int64_t limit = A == INT64_MAX ? passBound : std::min(passBound, A + 1);
            if (active_.empty()) {
                judgeCursor_ = limit;
                ++work;
                continue;
            }
            const rune::storage::LogReadView v = readRange(judgeCursor_, limit);
            if (v.empty()) break;
            const int64_t before = judgeCursor_;
            work += judgeBatches_(v, limit, aborted);
            if (judgeCursor_ == before) break;
        }
        state.rejected.noteCovered(judgeCursor_);
    }
    work += replayCandidates_(readRange, bound, aborted);
    state.judgedThrough = judgeCursor_;
    work += emitMarkers_(state, nowNs);
    work += pushCheckpoints_(state);
    work += dropRetired_(state);
    return work;
}

}  // namespace rune::node
