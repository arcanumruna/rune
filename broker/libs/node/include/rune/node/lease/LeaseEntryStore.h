#pragma once
// LeaseEntryStore.h - lease topics Task 9 (spec 9.1, 9.5, 9.6, 10): the per-key lease entry, its chunked store and the pure
// four-state machine.  Header-only, standard library only, owned by the partition leader's worker thread (shared-nothing).
//
// One 48-byte entry per live lease row, indexed by the lease view's dense row id.  `lastAppended` is NOT in the entry: it stays
// in the lease table's aux (kLeaseAuxBytes, LeaseIndex.h), written by the produce hook; with it the entry would be 56 bytes.
//
// DEADLINE ARITHMETIC: callers compute EVERY deadline with satAdd (saturating at INT64_MAX).  Neither the store nor the wheel
// does any other deadline arithmetic; a saturated deadline lands in the top wheel level and is never due.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace rune::node::lease {

inline constexpr uint32_t kLeaseNil        = 0xFFFF'FFFFu;
inline constexpr uint32_t kLeaseEntryChunk = 4096;
inline constexpr int64_t  kLeaseNoCheckpoint = INT64_MAX;   // checkpointNs when the key has no checkpoint schedule
inline constexpr uint8_t  kLeaseUnlinked   = 0xFF;          // levelD / levelC of an entry that is not in that wheel

/// a + b for a, b >= 0, saturating at INT64_MAX.  The only deadline arithmetic the store and the wheel use.
[[nodiscard]] inline constexpr int64_t satAdd(int64_t a, int64_t b) noexcept {
    return b > INT64_MAX - a ? INT64_MAX : a + b;
}

enum class LeaseEntryState : uint8_t { Free = 0, Armed, Checkpointing, Clearing, Emitting };

struct LeaseEntry {
    int64_t  deadlineNs     = INT64_MAX;
    int64_t  checkpointNs   = kLeaseNoCheckpoint;
    int64_t  inflightOffset = -1;   // emitOffset / ckptOffset / clearOffset (one at a time); -1 in Free / Armed
    uint32_t dPrev = kLeaseNil, dNext = kLeaseNil;   // deadline wheel links
    uint32_t cPrev = kLeaseNil, cNext = kLeaseNil;   // checkpoint wheel links
    uint32_t parkHead = kLeaseNil;                   // Task 11's park table
    uint8_t  levelD = kLeaseUnlinked, levelC = kLeaseUnlinked;   // 0..4 wheel level, 5 = ready list, 0xFF = unlinked
    LeaseEntryState state = LeaseEntryState::Free;
    uint8_t  pad = 0;
};
static_assert(sizeof(LeaseEntry) == 48, "the lease entry is exactly 48 bytes");
static_assert(alignof(LeaseEntry) == 8);

/// Chunked entry store: chunks of 4096 entries, the pointer vector reserved up front so it never reallocates, chunks added on
/// the cold path, never past `maxKeys` (rounded up to a chunk), never freed while the partition lives (clear() resets them).
class LeaseEntryStore {
public:
    /// maxKeys must be < 2^32 - 4096 (row ids are u32 and kLeaseNil is reserved).
    explicit LeaseEntryStore(uint32_t maxKeys)
        : cap_(roundUp(maxKeys)) {
        chunks_.reserve(cap_ / kLeaseEntryChunk);
        addChunk();
    }
    LeaseEntryStore(const LeaseEntryStore&)            = delete;
    LeaseEntryStore& operator=(const LeaseEntryStore&) = delete;

    [[nodiscard]] LeaseEntry& at(uint32_t rid) noexcept { return chunks_[rid >> 12][rid & (kLeaseEntryChunk - 1)]; }
    [[nodiscard]] const LeaseEntry& at(uint32_t rid) const noexcept {
        return chunks_[rid >> 12][rid & (kLeaseEntryChunk - 1)];
    }
    [[nodiscard]] bool covers(uint32_t rid) const noexcept {
        return static_cast<uint64_t>(rid) < static_cast<uint64_t>(chunks_.size()) * kLeaseEntryChunk;
    }

    /// Cold: adds chunks until covers(rid).  False (nothing added) if rid is past the cap.
    [[nodiscard]] bool reserveFor(uint32_t rid) {
        if (rid >= cap_) return false;
        while (!covers(rid)) addChunk();
        return true;
    }

    /// Hot-path note that `rid` is in use; sets growOwed_ at 75 % of the current capacity while below the cap.
    void noteUse(uint32_t rid) noexcept {
        if (rid + 1 > highWater_) highWater_ = rid + 1;
        evaluate();
    }
    [[nodiscard]] bool growOwed() const noexcept { return growOwed_; }
    /// Cold: one chunk, clears the flag, re-evaluates.
    void growIfOwed() {
        if (!growOwed_) return;
        growOwed_ = false;
        if (capacity() < cap_) addChunk();
        evaluate();
    }
    [[nodiscard]] uint32_t capacity() const noexcept {
        return static_cast<uint32_t>(chunks_.size() * kLeaseEntryChunk);
    }
    [[nodiscard]] uint32_t cap() const noexcept { return cap_; }
    [[nodiscard]] std::size_t bytes() const noexcept {
        return chunks_.size() * kLeaseEntryChunk * sizeof(LeaseEntry) + chunks_.capacity() * sizeof(chunks_[0]);
    }

    /// Demotion: keeps the chunks (never freed while the partition lives), resets every entry.  Destruction frees everything.
    void clear() noexcept {
        for (auto& c : chunks_) std::fill_n(c.get(), kLeaseEntryChunk, LeaseEntry{});
        highWater_ = 0;
        growOwed_  = false;
    }

private:
    static uint32_t roundUp(uint32_t maxKeys) noexcept {
        const uint64_t chunks = (static_cast<uint64_t>(maxKeys) + kLeaseEntryChunk - 1) / kLeaseEntryChunk;
        return static_cast<uint32_t>((chunks == 0 ? 1 : chunks) * kLeaseEntryChunk);
    }
    void addChunk() { chunks_.push_back(std::make_unique<LeaseEntry[]>(kLeaseEntryChunk)); }   // value-initialized
    void evaluate() noexcept {
        if (static_cast<uint64_t>(highWater_) * 4 >= static_cast<uint64_t>(capacity()) * 3 && capacity() < cap_) growOwed_ = true;
    }

    uint32_t cap_;
    std::vector<std::unique_ptr<LeaseEntry[]>> chunks_;
    uint32_t highWater_ = 0;
    bool     growOwed_  = false;
};

// ---------------------------------------------------------------------------------------------------------------------
// The pure state machine (spec 9.6).  No I/O, no allocation: the caller performs the action.
// ---------------------------------------------------------------------------------------------------------------------

/// Arm = a grant fold or the promotion pass.  OwnRecordFolds = the entry's in-flight record (offset == inflightOffset) folded.
/// OtherUpdateFolds = an accepted Put+update of the key.  EndFolds = an end of the key that is NOT the entry's own Expire (a
/// revoke, or an Expire appended by an earlier leader).  `due` (on the step input) = the deadline passed.
enum class LeaseEntryEvent : uint8_t { Arm, FallDue, CheckpointDue, Keepalive, OwnRecordFolds, OtherUpdateFolds, EndFolds, Demotion };

enum class LeaseAction : uint8_t {
    None, Link, AppendExpire, AppendCheckpoint, AppendClear, Park, RearmAnswerOk,
    ReturnArmedReplayParked, ReturnArmedAnswerParkedOk, FreeAnswerParkedNotFound, DiscardAnswerParkedNotLeader
};

struct LeaseStepIn {
    LeaseEntryState state;
    LeaseEntryEvent event;
    bool gateOpen;
    bool due;
    bool remainingPositive;
};
struct LeaseStepOut {
    LeaseEntryState next;
    LeaseAction     action;
};

/// A switch over state then event, no default branch (-Wswitch flags a missing case).  AppendExpire / AppendCheckpoint appear
/// only in the Armed row (R4).  Free x Keepalive is None: the caller answers an absent row LeaseNotFound before the entry.
/// AppendClear means the keepalive is parked on that Checkpoint(0); ReturnArmedReplayParked means the parked keepalives run
/// again as Keepalive events, now seeing remaining > 0.
[[nodiscard]] constexpr LeaseStepOut leaseStep(LeaseStepIn in) noexcept {
    using S = LeaseEntryState;
    using E = LeaseEntryEvent;
    using A = LeaseAction;
    switch (in.state) {
        case S::Free:
            switch (in.event) {
                case E::Arm: return {S::Armed, A::Link};
                case E::FallDue:
                case E::CheckpointDue:
                case E::Keepalive:
                case E::OwnRecordFolds:
                case E::OtherUpdateFolds:
                case E::EndFolds:
                case E::Demotion: return {S::Free, A::None};
            }
            break;
        case S::Armed:
            switch (in.event) {
                case E::Arm: return {S::Armed, A::None};
                case E::FallDue: return in.gateOpen ? LeaseStepOut{S::Emitting, A::AppendExpire} : LeaseStepOut{S::Armed, A::None};
                case E::CheckpointDue:
                    return in.gateOpen ? LeaseStepOut{S::Checkpointing, A::AppendCheckpoint} : LeaseStepOut{S::Armed, A::None};
                case E::Keepalive:
                    if (in.due) return {S::Armed, A::Park};
                    if (!in.remainingPositive) return {S::Armed, A::RearmAnswerOk};
                    return in.gateOpen ? LeaseStepOut{S::Clearing, A::AppendClear} : LeaseStepOut{S::Armed, A::Park};
                case E::OwnRecordFolds:
                case E::OtherUpdateFolds: return {S::Armed, A::None};
                case E::EndFolds: return {S::Free, A::FreeAnswerParkedNotFound};
                case E::Demotion: return {S::Free, A::DiscardAnswerParkedNotLeader};
            }
            break;
        case S::Checkpointing:
            switch (in.event) {
                case E::Arm:
                case E::FallDue:
                case E::CheckpointDue:
                case E::OtherUpdateFolds: return {S::Checkpointing, A::None};
                case E::Keepalive: return {S::Checkpointing, A::Park};
                case E::OwnRecordFolds: return {S::Armed, A::ReturnArmedReplayParked};
                case E::EndFolds: return {S::Free, A::FreeAnswerParkedNotFound};
                case E::Demotion: return {S::Free, A::DiscardAnswerParkedNotLeader};
            }
            break;
        case S::Clearing:
            switch (in.event) {
                case E::Arm:
                case E::FallDue:
                case E::CheckpointDue:
                case E::OtherUpdateFolds: return {S::Clearing, A::None};
                case E::Keepalive: return {S::Clearing, A::Park};
                case E::OwnRecordFolds: return {S::Armed, A::ReturnArmedAnswerParkedOk};
                case E::EndFolds: return {S::Free, A::FreeAnswerParkedNotFound};
                case E::Demotion: return {S::Free, A::DiscardAnswerParkedNotLeader};
            }
            break;
        case S::Emitting:
            switch (in.event) {
                case E::Arm:
                case E::FallDue:
                case E::CheckpointDue:
                case E::OtherUpdateFolds: return {S::Emitting, A::None};
                case E::Keepalive: return {S::Emitting, A::Park};
                case E::OwnRecordFolds:
                case E::EndFolds: return {S::Free, A::FreeAnswerParkedNotFound};
                case E::Demotion: return {S::Free, A::DiscardAnswerParkedNotLeader};
            }
            break;
    }
    return {in.state, A::None};   // unreachable: every (state, event) is a case above
}

}  // namespace rune::node::lease
