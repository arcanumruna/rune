#pragma once
// SbeConsensusPublisher.h — Concrete ConsensusPublisher that encodes via the
// P2P-4 SBE codecs (ConsensusMessages.h) into a single reusable buffer and
// delegates transport to a MessageSink.
//
// tryClaim-ready shape: encode-into-buffer-then-send. P2P-8 replaces the buffer
// with a claimed Aeron term-buffer slot, keeping the encode path unchanged.
//
// No per-message heap allocation: uses a fixed std::array sized for the largest
// message (NewLeadershipTerm = 104 bytes).

#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/ConsensusMessages.h>
#include <array>

namespace rune::consensus {

// Buffer capacity: NewLeadershipTerm is the largest fixed message at 104 bytes.
// We add a small margin for alignment / future fields.
inline constexpr std::size_t kEncodeBufCapacity = 128;

class SbeConsensusPublisher final : public ConsensusPublisher {
public:
    explicit SbeConsensusPublisher(MessageSink& sink) noexcept
        : sink_{sink}
    {}

    ~SbeConsensusPublisher() override = default;

    // -------------------------------------------------------------------------
    // ConsensusPublisher interface
    // -------------------------------------------------------------------------

    void canvassPosition(
        NodeId  toMember,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    leadershipTermId,
        NodeId  followerMemberId) override;

    void requestVote(
        NodeId  toMember,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    candidateTermId,
        NodeId  candidateMemberId) override;

    void placeVote(
        NodeId  toMember,
        Term    candidateTermId,
        Term    logLeadershipTermId,
        int64_t logPosition,
        NodeId  candidateMemberId,
        NodeId  followerMemberId,
        bool    vote) override;

    void newLeadershipTerm(
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
        int64_t commitPosition) override;

    void appendPosition(
        NodeId  toMember,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  followerMemberId,
        uint8_t flags) override;

    void commitPosition(
        NodeId  toMember,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  leaderMemberId) override;

private:
    MessageSink& sink_;
    // Single reusable encode buffer — no per-message allocation.
    std::array<uint8_t, kEncodeBufCapacity> buf_{};
};

} // namespace rune::consensus
