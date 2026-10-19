#pragma once
// ---------------------------------------------------------------------------
// AckTerm.h — the consensus term that may REFUSE an internal-ISR ack
// (re-review 2026-10-04 MEDIUM-A; round 3 MAJOR-R3).
//
// A node refuses to persist or ack an internal ISR state whose leaderEpoch is
// older than this term — Raft's term rule applied to the ack: a zombie leader
// can never commit through a node that moved on.
//
// "Moved on" = the node GRANTED a vote in a newer term, or follows / leads a
// leader of a newer term.  NOT the raw candidateTermId: Aeron's RequestVote
// deny branch raises (and persists) candidateTermId on any node outside Init —
// also a Closed leader and its followers, which never step down for it — so
// one denied candidacy would make the live leader refuse its own metadata ISR
// states forever (a dead member pinning the HW).  An own candidacy does not
// raise it either: an abandoned candidacy helped no leader, so a node whose
// only newer term is its own failed candidacy may follow T's leader again
// (D24), and it must not freeze the live leader.  A node that GRANTED, led,
// followed or adopted T+1 never follows T (Election's durable follow floor).
// ---------------------------------------------------------------------------
#include <algorithm>
#include <cstdint>

namespace rune::consensus {

[[nodiscard]] constexpr int64_t ackTerm(int64_t /*candidateTermId*/, int64_t votedTermId,
                                        int64_t leadershipTermId) noexcept {
    return std::max(votedTermId, leadershipTermId);
}

} // namespace rune::consensus
