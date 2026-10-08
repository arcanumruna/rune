#include <rune/consensus/SbeConsensusPublisher.h>

namespace rune::consensus {

void SbeConsensusPublisher::candidacyState(NodeId toMember, NodeId fromMember,
                                           const CandidacyState& state)
{
    const int32_t count = state.count < 0 ? 0
        : (state.count > CandidacyState::kMaxMembers ? CandidacyState::kMaxMembers : state.count);
    const auto len = CandidacyStateMsg::encode(buf_.data(), 0, state.leaderEpoch, state.version,
                                               fromMember, count, state.members.data());
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::preVoteRequest(NodeId toMember, Term term, Term logLeadershipTermId,
                                           int64_t logPosition, NodeId candidateId)
{
    const auto len = PreVoteRequestMsg::encode(buf_.data(), 0, term, logLeadershipTermId, logPosition, candidateId);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::preVoteResponse(NodeId toMember, Term term, NodeId voterId, bool granted,
                                            Term voterTermId)
{
    const auto len = PreVoteResponseMsg::encode(buf_.data(), 0, term, voterTermId, voterId, granted);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::termFloorNotice(NodeId toMember, NodeId fromMember, Term floorTermId,
                                            Term refusedTermId)
{
    const auto len = TermFloorNoticeMsg::encode(buf_.data(), 0, floorTermId, refusedTermId, fromMember);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::canvassPosition(
    NodeId  toMember,
    Term    logLeadershipTermId,
    int64_t logPosition,
    Term    leadershipTermId,
    NodeId  followerMemberId)
{
    // protocolVersion is not threaded through ConsensusPublisher; send 0 (Java uses
    // SemanticVersion.compose(0,17,0) but the Election interface omits it).
    // The SBE field is present in the wire format; we encode it as 0 here.
    // P2P-7 can wire in the real value once ConsensusModuleAgent is ported.
    const auto len = CanvassPositionMsg::encode(
        buf_.data(), 0,
        logLeadershipTermId,
        logPosition,
        leadershipTermId,
        followerMemberId,
        /*protocolVersion=*/0);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::requestVote(
    NodeId  toMember,
    Term    logLeadershipTermId,
    int64_t logPosition,
    Term    candidateTermId,
    NodeId  candidateMemberId)
{
    const auto len = RequestVoteMsg::encode(
        buf_.data(), 0,
        logLeadershipTermId,
        logPosition,
        candidateTermId,
        candidateMemberId,
        /*protocolVersion=*/0);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::placeVote(
    NodeId  toMember,
    Term    candidateTermId,
    Term    logLeadershipTermId,
    int64_t logPosition,
    NodeId  candidateMemberId,
    NodeId  followerMemberId,
    bool    vote)
{
    const auto len = VoteMsg::encode(
        buf_.data(), 0,
        candidateTermId,
        logLeadershipTermId,
        logPosition,
        candidateMemberId,
        followerMemberId,
        vote ? BooleanType::TRUE_VALUE : BooleanType::FALSE_VALUE);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::newLeadershipTerm(
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
    int64_t commitPosition)
{
    const auto len = NewLeadershipTermMsg::encode(
        buf_.data(), 0,
        logLeadershipTermId,
        nextLeadershipTermId,
        nextTermBaseLogPosition,
        nextLogPosition,
        leadershipTermId,
        termBaseLogPosition,
        logPosition,
        leaderRecordingId,
        timestamp,
        leaderMemberId,
        logSessionId,
        /*appVersion=*/0,
        isStartup ? BooleanType::TRUE_VALUE : BooleanType::FALSE_VALUE,
        commitPosition);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::appendPosition(
    NodeId  toMember,
    Term    leadershipTermId,
    int64_t logPosition,
    NodeId  followerMemberId,
    uint8_t flags)
{
    const auto len = AppendPositionMsg::encode(
        buf_.data(), 0,
        leadershipTermId,
        logPosition,
        followerMemberId,
        flags);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::commitPosition(
    NodeId  toMember,
    Term    leadershipTermId,
    int64_t logPosition,
    NodeId  leaderMemberId)
{
    const auto len = CommitPositionMsg::encode(
        buf_.data(), 0,
        leadershipTermId,
        logPosition,
        leaderMemberId);
    sink_.send(toMember, buf_.data(), len);
}

void SbeConsensusPublisher::standDown(NodeId toMember, Term leadershipTermId, NodeId leaderMemberId)
{
    const auto len = StandDownMsg::encode(buf_.data(), 0, leadershipTermId, leaderMemberId);
    sink_.send(toMember, buf_.data(), len);
}

} // namespace rune::consensus
