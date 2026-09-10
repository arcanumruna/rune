#include <rune/consensus/ConsensusAdapter.h>

namespace rune::consensus {

void ConsensusAdapter::onFragment(int64_t nowNs, const uint8_t* buf, std::size_t /*len*/)
{
    // Peek the templateId from the 8-byte SBE header (bytes 2-3).
    const uint16_t tid = peekTemplateId(buf, 0);

    switch (tid) {
    case CanvassPositionMsg::TEMPLATE_ID: {
        const auto m = CanvassPositionMsg::decode(buf, 0);
        handler_.onCanvassPosition(
            nowNs,
            m.logLeadershipTermId,
            m.logPosition,
            m.leadershipTermId,
            m.followerMemberId,
            m.protocolVersion);
        break;
    }
    case RequestVoteMsg::TEMPLATE_ID: {
        const auto m = RequestVoteMsg::decode(buf, 0);
        handler_.onRequestVote(
            nowNs,
            m.logLeadershipTermId,
            m.logPosition,
            m.candidateTermId,
            m.candidateMemberId,
            m.protocolVersion);
        break;
    }
    case VoteMsg::TEMPLATE_ID: {
        const auto m = VoteMsg::decode(buf, 0);
        handler_.onVote(
            nowNs,
            m.candidateTermId,
            m.logLeadershipTermId,
            m.logPosition,
            m.candidateMemberId,
            m.followerMemberId,
            m.vote == BooleanType::TRUE_VALUE);
        break;
    }
    case NewLeadershipTermMsg::TEMPLATE_ID: {
        const auto m = NewLeadershipTermMsg::decode(buf, 0);
        handler_.onNewLeadershipTerm(
            nowNs,
            m.logLeadershipTermId,
            m.nextLeadershipTermId,
            m.nextTermBaseLogPosition,
            m.nextLogPosition,
            m.leadershipTermId,
            m.termBaseLogPosition,
            m.logPosition,
            m.leaderRecordingId,
            m.timestamp,
            m.leaderMemberId,
            m.logSessionId,
            m.appVersion,
            m.isStartup == BooleanType::TRUE_VALUE,
            m.commitPosition);
        break;
    }
    case AppendPositionMsg::TEMPLATE_ID: {
        const auto m = AppendPositionMsg::decode(buf, 0);
        handler_.onAppendPosition(
            nowNs,
            m.leadershipTermId,
            m.logPosition,
            m.followerMemberId,
            m.flags);
        break;
    }
    case CommitPositionMsg::TEMPLATE_ID: {
        const auto m = CommitPositionMsg::decode(buf, 0);
        handler_.onCommitPosition(
            nowNs,
            m.leadershipTermId,
            m.logPosition,
            m.leaderMemberId);
        break;
    }
    default:
        // Unknown or unhandled templateId — silently ignore for forward-compat.
        // (CatchupPosition, StopCatchup, TerminationPosition, TerminationAck
        //  are handled by other adapters in the full Aeron port.)
        break;
    }
}

} // namespace rune::consensus
