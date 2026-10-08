#pragma once
// ---------------------------------------------------------------------------
// CandidacyState.h — the newest durable __cluster_metadata ISR state a node
// holds, exchanged between election participants (re-review 2026-10-04,
// round 3 CRITICAL-R3).
//
// The consensus candidacy rule (Election::setVoteGate) gates EVERY vote — the
// candidate's own included — on the voter's durable knowledge of the newest
// metadata ISR.  For liveness the participants exchange that knowledge while
// they canvass / vote (CandidacyState, consensus template 58): a node that
// missed a committed ISR state learns it from a peer that persisted it (newest
// (leaderEpoch, version) wins; LEARNING bypasses the ack term rule — it acks
// nothing).  Mirrors InternalIsrState (replication) without depending on it.
// ---------------------------------------------------------------------------
#include <array>
#include <cstdint>

namespace rune::consensus {

struct CandidacyState {
    static constexpr int kMaxMembers = 16;
    int64_t leaderEpoch = -1;
    int64_t version     = 0;
    int32_t count       = 0;
    std::array<int32_t, kMaxMembers> members{};
};

} // namespace rune::consensus
