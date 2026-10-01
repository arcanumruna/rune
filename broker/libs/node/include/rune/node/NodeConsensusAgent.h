#pragma once
// NodeConsensusAgent.h — Composite duty-cycle agent (BR-3).
//
// For multi-node UDP operation, the ConsensusAgent::doWork drains the
// IngressRing — but nothing fills the ring unless we poll AeronIngress first.
//
// This composite agent runs on ONE thread (the AgentRunner):
//   int doWork(nowNs):
//     1. aeronIngress.poll(limit)  — fills IngressRing from Aeron subscription
//     2. consensusAgent.doWork(nowNs)  — drains ring + drives Election
//     3. return total workCount
//
// Single-writer invariant: poll + doWork on the same thread, so both
// ExclusivePublication (in UdpMessageSink) and Subscription (in AeronIngress)
// are accessed from the same thread. Correct.
//
// Also mirrors the observed leader id: when onNewLeadershipTerm is accepted
// (leaderMemberId known + Closed/FollowerReady reached), the agent updates
// observedLeaderId_.  F9a: that field is a plain duty-cycle-owned value —
// leaderId() is for the duty-cycle thread only.  Observer threads learn the
// leader via LeaderObserver's ring (published once per cycle by the runtime).

#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/Types.h>
#include <rune_fr/FlightRecorder.h>

#include <cstdint>
#include <memory>

namespace rune::node {

// ---------------------------------------------------------------------------
// NodeConsensusAgent
// ---------------------------------------------------------------------------
class NodeConsensusAgent final : public rune::consensus::Agent {
public:
    // @param ingress         AeronIngress (owned by BrokerNode, must outlive this)
    // @param consensusAgent  ConsensusAgent (owned by BrokerNode, must outlive this)
    // @param election        Election (owned by BrokerNode, must outlive this)
    // @param nodeId          This node's own id (used to seed initial observedLeaderId)
    NodeConsensusAgent(rune::consensus::AeronIngress&  ingress,
                       rune::consensus::ConsensusAgent& consensusAgent,
                       rune::consensus::Election&       election,
                       rune::consensus::NodeId          nodeId) noexcept
        : ingress_(ingress)
        , consensusAgent_(consensusAgent)
        , election_(election)
        , nodeId_(nodeId)
    {}

    NodeConsensusAgent(const NodeConsensusAgent&)            = delete;
    NodeConsensusAgent& operator=(const NodeConsensusAgent&) = delete;

    // -----------------------------------------------------------------------
    // Agent::doWork — single-writer duty cycle
    // -----------------------------------------------------------------------
    const char* name() const noexcept override { return "rune-consensus"; }

    int doWork(int64_t nowNs) override {
        RUNE_FR_SPAN_CONSENSUS(rune_fr::Site::ConsensusDoWork, 0);
        // 1. Poll Aeron subscription → fills IngressRing
        int workCount = ingress_.poll(kFragmentLimit);

        // 2. Drive consensus (drains IngressRing + runs Election)
        workCount += consensusAgent_.doWork(nowNs);

        // 3. Mirror the observed leader id (duty-cycle-owned; observers get it
        //    via LeaderObserver's ring).  Only a decided leader is mirrored:
        //    election closed (leader confirmed) or leaderMember known (follower
        //    accepted NLT).
        updateObservedLeader();

        return workCount;
    }

    // -----------------------------------------------------------------------
    // Observed leader id — DUTY-CYCLE (control) thread only (F9a).
    // -----------------------------------------------------------------------
    rune::consensus::NodeId leaderId() const noexcept { return observedLeaderId_; }

private:
    static constexpr int kFragmentLimit = 10;

    void updateObservedLeader() {
        // Called on the duty-cycle thread — safe to read election_ directly.
        const int32_t sc = election_.stateCode();
        const auto state = static_cast<rune::consensus::ElectionState>(sc);

        // Update when the election has a decided leader:
        //   Leader* states (LeaderLogReplication/LeaderReplay/LeaderInit/LeaderReady):
        //     this node IS the leader → newLeaderId = nodeId_ (explicit, not leaderMemberId()).
        //   Closed: reached from leader OR follower path → use leaderMemberId().
        //   Follower states (FollowerBallot/FollowerReady/etc.) → leaderMemberId() when set.
        //   Init/Canvass/Nominate/CandidateBallot → no leader yet → -1.
        rune::consensus::NodeId newLeaderId = -1;

        switch (state) {
            case rune::consensus::ElectionState::LeaderLogReplication:
            case rune::consensus::ElectionState::LeaderReplay:
            case rune::consensus::ElectionState::LeaderInit:
            case rune::consensus::ElectionState::LeaderReady:
                // In Leader* states this node IS the leader by definition.
                // Using nodeId_ directly (rather than election_.leaderMemberId())
                // prevents a latent wrong-isLeader() if a future Election refactor
                // leaves leaderMember_ stale during a Leader* pass-through.
                newLeaderId = nodeId_;
                break;
            case rune::consensus::ElectionState::Closed: {
                // Closed can be reached from either the leader OR the follower path.
                // leaderMemberId() holds the accepted leader id in both cases.
                rune::consensus::NodeId lid = election_.leaderMemberId();
                newLeaderId = (lid >= 0) ? lid : -1;
                break;
            }
            case rune::consensus::ElectionState::FollowerBallot:
            case rune::consensus::ElectionState::FollowerLogReplication:
            case rune::consensus::ElectionState::FollowerReplay:
            case rune::consensus::ElectionState::FollowerCatchupInit:
            case rune::consensus::ElectionState::FollowerCatchupAwait:
            case rune::consensus::ElectionState::FollowerCatchup:
            case rune::consensus::ElectionState::FollowerLogInit:
            case rune::consensus::ElectionState::FollowerLogAwait:
            case rune::consensus::ElectionState::FollowerReady: {
                // leaderMember set when NLT accepted.
                rune::consensus::NodeId lid = election_.leaderMemberId();
                newLeaderId = (lid >= 0) ? lid : -1;
                break;
            }
            default:
                // Init / Canvass / Nominate / CandidateBallot — no leader yet.
                newLeaderId = -1;
                break;
        }

        observedLeaderId_ = newLeaderId;
    }

    rune::consensus::AeronIngress&   ingress_;
    rune::consensus::ConsensusAgent& consensusAgent_;
    rune::consensus::Election&       election_;
    rune::consensus::NodeId          nodeId_;

    // Duty-cycle-thread-owned (F9a: no atomics).  -1 = no leader known.
    rune::consensus::NodeId observedLeaderId_{-1};
};

} // namespace rune::node
