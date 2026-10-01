#pragma once
// ConsensusAdapter.h — Decodes incoming SBE consensus frames and dispatches
// to a ConsensusMessageHandler.
//
// onFragment() is the per-frame entry point:
//   - bounds-check: len >= ConsensusMessageHeader::ENCODED_SIZE (header peek)
//   - peekTemplateId(buf) → switch on templateId
//   - bounds-check: len >= Msg::TOTAL_SIZE before each decode
//   - decode with the matching P2P-4 decoder (no alloc; views into buf)
//   - call handler_.onXxx(nowNs, ...decoded fields...)
//
// A3 fix (audit 2026-09-11): 'len' was previously ignored (/*len*/ comment),
// allowing an OOB read on truncated/corrupt frames.  Every decode path now
// validates len >= required size and increments ignoredFrameCount_ on rejection.
//
// FRAGMENT_LIMIT = 10 mirrors the Java ConsensusAdapter constant. The real
// Aeron poll loop (P2P-8) wraps this in a FragmentAssembler and calls
// subscription.poll(fragmentAssembler, FRAGMENT_LIMIT) per duty cycle.
//
// Unknown templateIds are silently ignored (forward-compat); they also increment
// ignoredFrameCount_ so callers can surface dropped/unknown frames.

#include <rune/consensus/ConsensusMessageHandler.h>
#include <rune/consensus/ConsensusMessages.h>
#include <cstdint>
#include <cstddef>

namespace rune::consensus {

/// Task 11 fix 2 (B2): templated on the CONCRETE handler so every decoded
/// message is a direct call (ConsensusAgent is final) — never a virtual
/// ConsensusMessageHandler dispatch on the runtime duty cycle (worker 0's pass
/// under fused placement).  `ConsensusAdapter` (= the interface instantiation)
/// stays for test doubles only.
template <class Handler>
class BasicConsensusAdapter {
public:
    // Mirrors Java ConsensusAdapter.FRAGMENT_LIMIT.
    // P2P-8 wraps this in an aeron FragmentAssembler.
    static constexpr int FRAGMENT_LIMIT = 10;

    explicit BasicConsensusAdapter(Handler& handler) noexcept
        : handler_{handler}
    {}

    /// Decode one SBE frame and dispatch to the handler.
    /// buf[0..len) is a complete SBE frame starting with an 8-byte header.
    /// Frames shorter than the required message size are rejected (no OOB read);
    /// ignoredFrameCount() increments on each rejected or unknown frame.
    void onFragment(int64_t nowNs, const uint8_t* buf, std::size_t len)
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

    /// Count of frames rejected due to truncation, unknown templateId, or OOB guard.
    /// Useful for monitoring/alerting on corrupt ingress.
    [[nodiscard]] int64_t ignoredFrameCount() const noexcept { return ignoredFrameCount_; }

private:
    Handler& handler_;
    int64_t  ignoredFrameCount_{0};
};

/// Interface instantiation — virtual dispatch; test doubles only.
using ConsensusAdapter = BasicConsensusAdapter<ConsensusMessageHandler>;

} // namespace rune::consensus
