#pragma once
// VerdictSource.h — checked writes Part 04: the judge of one source partition.
//
// A verdict source owns the verdict views of its partition.  judge() prepares every active member for a record,
// evaluates them in set order, and applies all or none; a rejection goes into PartitionWriteState::rejected.  driveJudge
// walks the committed log, follows the logged constraint-set changes and emits durable verdict markers.
#include <rune/node/VerdictState.h>
#include <rune/node/ViewInstance.h>
#include <rune/node/WriteState.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/view/Verdict.h>

#include <array>
#include <cstdint>
#include <span>
#include <vector>

namespace rune::node {

/// What the owning WorkerViewSet does for a judge's members (fn-pointer + ctx, no std::function).
struct VerdictViewHooks {
    void* ctx = nullptr;
    /// Push the member's dirty rows as one checkpoint delta (or drop them for a transient member).
    int (*checkpoint)(void* ctx, ViewInstance&) = nullptr;
    /// A checkpoint that outran the markers (P4-4): empty the member's tables and owe its file a reset.
    void (*discard)(void* ctx, ViewInstance&) = nullptr;
    /// A retiring member left this partition's set for good (W passed its drop): the owner may erase the partition's instance.
    void (*retired)(void* ctx, uint64_t viewObjectId, uint64_t planFingerprint, int32_t partition) = nullptr;
};

class VerdictSource {
public:
    static constexpr int kChangeDeferred = -1, kChangeUnchanged = -2;

    /// What WorkerViewSet::syncConstraintSet_ last reconciled this judge against: the template epoch and the latest logged change.
    uint64_t constraintSyncEpoch = UINT64_MAX;
    int64_t  constraintSyncChange = -2;

    VerdictSource(int32_t srcTopicId, int32_t partition);

    /// The partition state judge() records rejections in and driveJudge() reads the timeline from.
    void bindState(PartitionWriteState* state) noexcept { state_ = state; }

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

    void addVerdictView(ViewInstance*, rune::view::VerdictViewKind, uint64_t viewObjectId, uint64_t planFingerprint = 0);
    void removeVerdictView(uint64_t viewObjectId, uint64_t planFingerprint);
    /// Appends a constraint-set change at the LEO.  Returns the activation offset, kChangeDeferred or kChangeUnchanged.
    /// `refresh`: append even when the members are those of the latest change (retention is about to delete its segment).
    int requestChange(const ConstraintSet& next, bool refresh = false);
    void installTestVerdictViewForTest(ViewInstance*, uint64_t viewObjectId, TestRejectRule);

    /// Cold: the replica to append through and the leadership this judge serves.  A new generation re-activates.
    void bindLeader(rune::replication::PartitionReplica* replica, uint64_t leadershipGeneration, int64_t logStart) noexcept;
    void setVerdictIntervalNs(int64_t ns) noexcept { intervalNs_ = ns; }
    void setViewHooks(const VerdictViewHooks& h) noexcept { hooks_ = h; }
    [[nodiscard]] bool hasMember(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept;
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
    [[nodiscard]] std::size_t memberCountForTest() const noexcept { return members_.size(); }

private:
    struct Member {
        ViewInstance*               inst = nullptr;
        rune::view::VerdictViewKind kind{};
        uint64_t                    objectId = 0, fingerprint = 0;
        TestRejectRule              rule;
        rune::view::Prospect        prospect;
        bool                        retiring = false;
        int64_t                     leaveOffset = -1;
    };

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
    void   waitForInstall_() noexcept;
    int64_t extendOverEmptySets_(int64_t offset) const noexcept;
    rune::view::PrepareStatus prepare_(Member& m, const rune::view::SourceRecord& rec) noexcept;
    void   apply_(Member& m, const rune::view::SourceRecord& rec) noexcept;
    bool   rejects_(const Member& m, const rune::view::SourceRecord& rec) const noexcept;
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
    s.markers.truncateFrom(target);
    s.rejected.truncateFrom(target);
}

inline void verdictFeedBatch(void* state, const uint8_t* b, std::size_t len, int64_t base) noexcept {
    auto& s = *static_cast<PartitionWriteState*>(state);
    const int type = cr::recordType(b, len);
    if (type == cr::kVerdictMarker) {
        const auto m = cr::parseMarker(b, len);
        if (!m) return;
        if (s.rejected.capacity() == 0) s.rejected.reserve(kIndexReserve);
        s.rejected.onMarker(m->header.fromOffset, m->header.finalThrough, m->header.count, [&](std::size_t i) {
            return rune::storage::RejectedOffsetIndex::Entry{m->offsetAt(static_cast<uint32_t>(i)), m->codeAt(static_cast<uint32_t>(i)), m->constraintIdAt(static_cast<uint32_t>(i))};
        });
        (void)s.markers.onMarker(base, m->header.finalThrough);
    } else if (type == cr::kConstraintSetChange) {
        std::array<cr::ChangeMember, kMaxVerdictViewsPerSource> wire{};
        uint32_t id = 0;
        uint16_t count = 0;
        if (!cr::parseConstraintSetChange(b, len, id, wire, count)) return;
        ConstraintSet set;
        set.id = id;
        set.count = static_cast<uint8_t>(count);
        for (uint16_t i = 0; i < count; ++i)
            set.members[i] = ConstraintSetMember{wire[i].viewObjectId, static_cast<rune::view::VerdictViewKind>(wire[i].kind), wire[i].planFingerprint};
        s.constraintSets.onChange(base, set);
    }
}
/// The HW moved: markers below it are committed, and the committed coverage W follows.
void verdictOnHw(PartitionWriteState& state, int64_t hw) noexcept;

template <class ReadRangeFn>
bool VerdictSource::switchSet_(int64_t A, ReadRangeFn& readRange, const rune::view::AbortedTxnFilter& aborted, int& work) {
    const ConstraintSet& next = *state_->constraintSets.setAt(A + 1);
    if (!resolveSet_(next, scratchIdx_)) {
        waitForInstall_();
        return false;
    }
    waitingForInstall_ = false;
    for (const std::size_t i : scratchIdx_) {
        if (std::find(active_.begin(), active_.end(), i) != active_.end()) continue;   // staying
        Member& m = members_[i];
        m.inst->foldCursor = std::max(m.inst->foldCursor, logStart_);   // retention deleted what a member that was never folded would read
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
    activeSet_ = next;
    setFrom_ = A + 1;
    if (next.empty()) sawEmptyChange_ = true;
    return true;
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
        m.inst->foldCursor = std::max(m.inst->foldCursor, logStart_);
        if (m.inst->foldCursor >= judgeCursor_) continue;
        const rune::storage::LogReadView v = readRange(m.inst->foldCursor, judgeCursor_);
        if (!v.empty()) work += replay_(m, v, judgeCursor_, aborted);
    }
    bool behind = false;
    for (const std::size_t i : active_) behind = behind || members_[i].inst->foldCursor < judgeCursor_;
    const bool ready = activeSet_.count == active_.size() || reindex_();
    if (!ready) {
        waitForInstall_();
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
    state.judgedThrough = judgeCursor_;
    work += emitMarkers_(state, nowNs);
    work += pushCheckpoints_(state);
    work += dropRetired_(state);
    return work;
}

}  // namespace rune::node
