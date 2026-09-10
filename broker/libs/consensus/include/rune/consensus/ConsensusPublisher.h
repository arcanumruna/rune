#pragma once
// ConsensusPublisher.h — Abstract interface for emitting consensus messages.
// Faithful port of io.aeron.cluster.ConsensusPublisher (abstract side only).
// Real Aeron impl comes in P2P-6; tests inject a recording fake.
//
// Methods added in P2P-5b: newLeadershipTerm, appendPosition, commitPosition.

#include <rune/consensus/Types.h>
#include <cstdint>

namespace rune::consensus {

// ConsensusPublisher — one pure-virtual method per outbound consensus message.
// The Election class depends on this interface; the publisher routes each call
// to the correct peer's ExclusivePublication (in the real impl).
//
// toMember: NodeId of the destination member (publisher resolves to publication).
// All other params mirror the SBE wire fields defined in ConsensusMessages.h.
struct ConsensusPublisher {
    virtual ~ConsensusPublisher() = default;

    // ConsensusPublisher.java:53-89  canvassPosition(Publication, ...)
    // Emitted by: CANVASS and NOMINATE states (periodic broadcast to peers).
    virtual void canvassPosition(
        NodeId  toMember,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    leadershipTermId,
        NodeId  followerMemberId) = 0;

    // ConsensusPublisher.java:91-129  requestVote(Publication, ...)
    // Emitted by: CANDIDATE_BALLOT (once per peer that hasn't received a ballot).
    virtual void requestVote(
        NodeId  toMember,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    candidateTermId,
        NodeId  candidateMemberId) = 0;

    // ConsensusPublisher.java:131-170  placeVote(Publication, ...)
    // Emitted by: onRequestVote handler (grant or deny).
    virtual void placeVote(
        NodeId  toMember,
        Term    candidateTermId,
        Term    logLeadershipTermId,
        int64_t logPosition,
        NodeId  candidateMemberId,
        NodeId  followerMemberId,
        bool    vote) = 0;

    // NOTE: newLeadershipTerm / appendPosition / commitPosition added in P2P-5b.
};

} // namespace rune::consensus
