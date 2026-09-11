#include <rune/consensus/ConsensusAdapter.h>

namespace rune::consensus {

void ConsensusAdapter::onFragment(int64_t nowNs, const uint8_t* buf, std::size_t len)
{
    // A3 fix (audit 2026-09-11): Aeron ConsensusAdapter always receives complete,
    // framed SBE messages from a ring buffer, so short frames should not occur in
    // production — but a schema mismatch, corruption, or test error can produce a
    // truncated buffer.  Reading past 'len' is undefined behaviour.  Guard every
    // decode path by checking the required minimum size before reading any field.
    // Mirrors the defensive posture in Aeron's own message decoders.

    // Step 1: need at least a full SBE header to peek the templateId.
    if (len < ConsensusMessageHeader::ENCODED_SIZE) {
        ignoredFrameCount_++;
        return;
    }

    const uint16_t tid = peekTemplateId(buf, 0);

    switch (tid) {
    case CanvassPositionMsg::TEMPLATE_ID: {
        if (len < CanvassPositionMsg::TOTAL_SIZE) { ignoredFrameCount_++; return; }
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
        if (len < RequestVoteMsg::TOTAL_SIZE) { ignoredFrameCount_++; return; }
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
        if (len < VoteMsg::TOTAL_SIZE) { ignoredFrameCount_++; return; }
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
        if (len < NewLeadershipTermMsg::TOTAL_SIZE) { ignoredFrameCount_++; return; }
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
        if (len < AppendPositionMsg::TOTAL_SIZE) { ignoredFrameCount_++; return; }
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
        if (len < CommitPositionMsg::TOTAL_SIZE) { ignoredFrameCount_++; return; }
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
        ignoredFrameCount_++;
        break;
    }
}

} // namespace rune::consensus
