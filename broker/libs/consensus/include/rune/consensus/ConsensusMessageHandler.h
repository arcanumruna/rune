#pragma once
// ConsensusMessageHandler.h — Abstract handler that ConsensusAdapter dispatches to.
//
// P2P-7 ConsensusAgent implements this and forwards to Election. Election's
// on-handler method signatures already match closely.
//
// Field lists match the decoded SBE messages from ConsensusMessages.h (§3.1).

#include <rune/consensus/Types.h>
#include <cstdint>

namespace rune::consensus {

struct ConsensusMessageHandler {
    virtual ~ConsensusMessageHandler() = default;

    // Dispatched from templateId=50 (CanvassPosition)
    virtual void onCanvassPosition(
        int64_t nowNs,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    leadershipTermId,
        NodeId  followerMemberId,
        int32_t protocolVersion) = 0;

    // Dispatched from templateId=51 (RequestVote)
    virtual void onRequestVote(
        int64_t nowNs,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    candidateTermId,
        NodeId  candidateMemberId,
        int32_t protocolVersion) = 0;

    // Dispatched from templateId=52 (Vote)
    virtual void onVote(
        int64_t nowNs,
        Term    candidateTermId,
        Term    logLeadershipTermId,
        int64_t logPosition,
        NodeId  candidateMemberId,
        NodeId  followerMemberId,
        bool    vote) = 0;

    // Dispatched from templateId=53 (NewLeadershipTerm)
    virtual void onNewLeadershipTerm(
        int64_t nowNs,
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
        int32_t appVersion,
        bool    isStartup,
        int64_t commitPosition) = 0;

    // Dispatched from templateId=54 (AppendPosition)
    virtual void onAppendPosition(
        int64_t nowNs,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  followerMemberId,
        uint8_t flags) = 0;

    // Dispatched from templateId=55 (CommitPosition)
    virtual void onCommitPosition(
        int64_t nowNs,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  leaderMemberId) = 0;
};

} // namespace rune::consensus
