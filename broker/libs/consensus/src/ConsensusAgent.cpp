// ConsensusAgent.cpp — Implementation of ConsensusAgent.
//
// Single-writer duty-cycle agent:
//   doWork(nowNs):
//     a) drain FRAGMENT_LIMIT ingress frames from ingressRing_ →
//        adapter_.onFragment(nowNs, buf, len) →
//        this->onXxx(nowNs, ...) → election_.onXxx(...)
//     b) workCount += election_.doWork(nowNs)
//     c) return workCount
//
// All onXxx handlers simply forward to election_.onXxx with the appropriate
// parameter mapping (matching Election's API which takes nowNs as the first
// arg in most handlers).

#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/ConsensusAdapter.h>

namespace rune::consensus {

ConsensusAgent::ConsensusAgent(Election&    election,
                               IngressRing& ingressRing) noexcept
    : election_{election}
    , ingressRing_{ingressRing}
    , adapter_{*this}   // ConsensusAdapter dispatches to *this (ConsensusMessageHandler)
{}

// ---------------------------------------------------------------------------
// doWork — §4.2
// ---------------------------------------------------------------------------
int ConsensusAgent::doWork(int64_t nowNs) {
    // Store nowNs so that the synchronous onXxx callbacks (called from
    // drain's handler lambda below) have access to it without an extra
    // parameter.
    nowNsForHandlers_ = nowNs;

    int workCount = 0;

    // a) Drain up to FRAGMENT_LIMIT ingress frames.
    //    ManyToOneRingBuffer::read delivers (msgTypeId, AtomicBuffer&, offset, len).
    const int drained = ingressRing_.drain(
        [this](int32_t /*msgTypeId*/,
               aeron::concurrent::AtomicBuffer& buf,
               aeron::util::index_t             offset,
               aeron::util::index_t             length) {
            // Decode the SBE frame and dispatch to this->onXxx(nowNs, ...).
            // adapter_.onFragment takes (nowNs, const uint8_t*, size_t).
            adapter_.onFragment(
                nowNsForHandlers_,
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(length));
            // Count here; onXxx will also count via messagesProcessed_.
            // We count the raw ring messages drained so that the integration
            // test's "exactly N processed" assertion works even for unknown
            // templateIds (which onFragment silently ignores — the ring read
            // still happened).
            ++messagesProcessed_;
        },
        ConsensusAdapter::FRAGMENT_LIMIT);

    workCount += drained;

    // b) Drive the election state machine.
    workCount += election_.doWork(nowNs);

    return workCount;
}

// ---------------------------------------------------------------------------
// ConsensusMessageHandler — forward to election_
// (All called synchronously from doWork → drain → adapter_.onFragment)
// ---------------------------------------------------------------------------

void ConsensusAgent::onCandidacyState(int64_t /*nowNs*/, NodeId fromMember,
                                      const CandidacyState& state)
{
    election_.onCandidacyState(fromMember, state);
}

void ConsensusAgent::onPreVoteRequest(int64_t nowNs, Term term, Term logLeadershipTermId,
                                      int64_t logPosition, NodeId candidateId)
{
    election_.onPreVoteRequest(nowNs, term, logLeadershipTermId, logPosition, candidateId);
}

void ConsensusAgent::onPreVoteResponse(int64_t nowNs, Term term, NodeId voterId, bool granted,
                                       Term voterTermId)
{
    election_.onPreVoteResponse(term, voterId, granted, voterTermId, nowNs);
}

void ConsensusAgent::onTermFloorNotice(int64_t nowNs, NodeId fromMember, Term floorTermId, Term refusedTermId)
{
    election_.onTermFloorNotice(nowNs, fromMember, floorTermId, refusedTermId);
}

void ConsensusAgent::onCanvassPosition(
    int64_t nowNs,
    Term    logLeadershipTermId,
    int64_t logPosition,
    Term    leadershipTermId,
    NodeId  followerMemberId,
    int32_t /*protocolVersion*/)
{
    election_.onCanvassPosition(logLeadershipTermId, logPosition,
                                leadershipTermId, followerMemberId);
    (void)nowNs; // Election::onCanvassPosition doesn't take nowNs
}

void ConsensusAgent::onRequestVote(
    int64_t nowNs,
    Term    logLeadershipTermId,
    int64_t logPosition,
    Term    candidateTermId,
    NodeId  candidateMemberId,
    int32_t /*protocolVersion*/)
{
    election_.onRequestVote(logLeadershipTermId, logPosition,
                            candidateTermId, candidateMemberId, nowNs);
}

void ConsensusAgent::onVote(
    int64_t /*nowNs*/,
    Term    candidateTermId,
    Term    logLeadershipTermId,
    int64_t logPosition,
    NodeId  candidateMemberId,
    NodeId  followerMemberId,
    bool    vote)
{
    election_.onVote(logLeadershipTermId, logPosition,
                     candidateTermId, candidateMemberId, followerMemberId, vote);
}

void ConsensusAgent::onNewLeadershipTerm(
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
    int32_t /*appVersion*/,
    bool    isStartup,
    int64_t commitPosition)
{
    election_.onNewLeadershipTerm(
        nowNs,
        logLeadershipTermId, nextLeadershipTermId,
        nextTermBaseLogPosition, nextLogPosition,
        leadershipTermId, termBaseLogPosition,
        logPosition, leaderRecordingId, timestamp,
        leaderMemberId, logSessionId, isStartup, commitPosition);
}

void ConsensusAgent::onAppendPosition(
    int64_t nowNs,
    Term    leadershipTermId,
    int64_t logPosition,
    NodeId  followerMemberId,
    uint8_t flags)
{
    election_.onAppendPosition(nowNs, leadershipTermId, logPosition,
                               followerMemberId, flags);
}

void ConsensusAgent::onCommitPosition(
    int64_t nowNs,
    Term    leadershipTermId,
    int64_t logPosition,
    NodeId  leaderMemberId)
{
    election_.onCommitPosition(nowNs, leadershipTermId, logPosition, leaderMemberId);
}

void ConsensusAgent::onStandDown(int64_t nowNs, Term leadershipTermId, NodeId leaderMemberId)
{
    election_.onStandDown(nowNs, leadershipTermId, leaderMemberId);
}

} // namespace rune::consensus
