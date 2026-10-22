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
#include <rune/consensus/NullSink.h>
#include <rune/consensus/UdpMessageSink.h>
#include <rune/consensus/ConsensusMessages.h>
#include <array>
#include <type_traits>

namespace rune::consensus {

// Buffer capacity: NewLeadershipTerm is the largest fixed message at 104 bytes.
// We add a small margin for alignment / future fields.
inline constexpr std::size_t kEncodeBufCapacity = 128;

class SbeConsensusPublisher final : public ConsensusPublisher {
public:
    explicit SbeConsensusPublisher(MessageSink& sink) noexcept
        : sink_{SinkRef::of(sink)}
    {}

    /// Task 11 fix 2 (B2): true when the sink is a production (final) sink —
    /// every send is a direct call.  False only for test doubles (virtual).
    [[nodiscard]] bool sinkIsConcrete() const noexcept { return sink_.kind != SinkKind::Generic; }

    ~SbeConsensusPublisher() override = default;

    // -------------------------------------------------------------------------
    // ConsensusPublisher interface
    // -------------------------------------------------------------------------

    void candidacyState(NodeId toMember, NodeId fromMember,
                        const CandidacyState& state) override;
    void preVoteRequest(NodeId toMember, Term term, Term logLeadershipTermId, int64_t logPosition,
                        NodeId candidateId) override;
    void preVoteResponse(NodeId toMember, Term term, NodeId voterId, bool granted, Term voterTermId) override;
    void termFloorNotice(NodeId toMember, NodeId fromMember, Term floorTermId, Term refusedTermId) override;
    void readLeaseRequest(NodeId toMember, Term term, NodeId memberId, int64_t reqSeq, int64_t heardGrantSeq) override;
    void readLeaseGrant(NodeId toMember, Term term, NodeId leaderMemberId, NodeId memberId, int64_t reqSeq,
                        int64_t grantSeq, int64_t commitPos, int64_t validForNs) override;

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

    void standDown(NodeId toMember, Term leadershipTermId, NodeId leaderMemberId) override;

    /// ISR v3 R12: the founding-evidence flags every CanvassPosition carries in its protocolVersion field
    /// (bit 1 = valid, bit 0 = this process has history).  Set by the owning (control) thread every cycle.
    void setCanvassFlags(int32_t flags) noexcept { canvassFlags_ = flags; }
    [[nodiscard]] int32_t canvassFlags() const noexcept { return canvassFlags_; }

private:
    int32_t canvassFlags_ = 0;   ///< 0 = "flags invalid" (an unset publisher reads as history at the receiver)
    // Concrete sink + construction-time tag (Task 11 fix 2, B2): the runtime duty
    // cycle (worker 0's pass under fused placement) never dispatches through the
    // MessageSink vtable.  Generic = test doubles only.
    enum class SinkKind : uint8_t { Udp, Null, Generic };
    struct SinkRef {
        SinkKind        kind    = SinkKind::Generic;
        UdpMessageSink* udp     = nullptr;
        MessageSink*    generic = nullptr;
        static SinkRef of(MessageSink& s) noexcept {
            SinkRef r;
            if (auto* u = dynamic_cast<UdpMessageSink*>(&s)) { r.kind = SinkKind::Udp; r.udp = u; }
            else if (dynamic_cast<NullSink*>(&s) != nullptr)  { r.kind = SinkKind::Null; }
            else                                              { r.generic = &s; }
            return r;
        }
        void send(NodeId to, const uint8_t* buf, std::size_t len) const {
            if (kind == SinkKind::Udp) [[likely]] { udp->send(to, buf, len); return; }   // final → direct
            if (kind == SinkKind::Null) return;                                          // no-op sink
            generic->send(to, buf, len);                                                 // test doubles
        }
    };
    static_assert(std::is_final_v<UdpMessageSink> && std::is_final_v<NullSink>);
    SinkRef sink_;
    // Single reusable encode buffer — no per-message allocation.
    std::array<uint8_t, kEncodeBufCapacity> buf_{};
};

} // namespace rune::consensus
