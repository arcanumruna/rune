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

    // ConsensusPublisher.java:172-232  newLeadershipTerm(Publication, ...)
    // Emitted by: LEADER_LOG_REPLICATION, LEADER_REPLAY, LEADER_READY (on heartbeat interval).
    // Template ID 53 per §3.1. Fields match SBE schema.
    // Election.java:809-825 (LEADER_LOG_REPLICATION), 877-894 (LEADER_READY).
    virtual void newLeadershipTerm(
        NodeId  toMember,
        Term    logLeadershipTermId,
        Term    nextLeadershipTermId,
        int64_t nextTermBaseLogPosition,
        int64_t nextLogPosition,
        Term    leadershipTermId,
        int64_t termBaseLogPosition,
        int64_t logPosition,
        int64_t leaderRecordingId,
        int64_t timestamp,
        NodeId  leaderMemberId,
        int32_t logSessionId,
        bool    isStartup,
        int64_t commitPosition) = 0;

    // ConsensusPublisher.java:234-271  appendPosition(Publication, ...)
    // Emitted by: FOLLOWER_READY (once, on election complete path).
    // Template ID 54 per §3.1.
    // Election.java:1166-1179 (FOLLOWER_READY).
    virtual void appendPosition(
        NodeId  toMember,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  followerMemberId,
        uint8_t flags) = 0;

    // ConsensusPublisher.java:273-306  commitPosition(Publication, ...)
    // Emitted by: LEADER_LOG_REPLICATION, LEADER_REPLAY (on heartbeat interval).
    // Template ID 55 per §3.1.
    // Election.java:809-825 (LEADER_LOG_REPLICATION).
    virtual void commitPosition(
        NodeId  toMember,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  leaderMemberId) = 0;
};

} // namespace rune::consensus
