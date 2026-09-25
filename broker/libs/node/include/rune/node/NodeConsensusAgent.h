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
// Also mirrors the atomic leader-id update: when onNewLeadershipTerm is
// accepted (leaderMemberId known + Closed/FollowerReady reached), the agent
// updates observedLeaderId_ so observer threads can read it via leaderId().

#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/Types.h>

#include <atomic>
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
    {
        // kNullLeaderId = -1 (no leader known yet)
        observedLeaderId_.store(-1, std::memory_order_relaxed);
    }

    NodeConsensusAgent(const NodeConsensusAgent&)            = delete;
    NodeConsensusAgent& operator=(const NodeConsensusAgent&) = delete;

    // -----------------------------------------------------------------------
    // Agent::doWork — single-writer duty cycle
    // -----------------------------------------------------------------------
    const char* name() const noexcept override { return "rune-consensus"; }

    int doWork(int64_t nowNs) override {
        // 1. Poll Aeron subscription → fills IngressRing
        int workCount = ingress_.poll(kFragmentLimit);

        // 2. Drive consensus (drains IngressRing + runs Election)
        workCount += consensusAgent_.doWork(nowNs);

        // 3. Mirror the observed leader id for observer-thread-safe reads.
        //    The election.leaderMemberId() is only safe on the duty-cycle thread;
        //    we store it into the atomic so external threads can read it safely.
        //    Only update when the election is closed (leader confirmed) or when
        //    leaderMember is known (follower accepted NLT).
        updateObservedLeader();

        return workCount;
    }

    // -----------------------------------------------------------------------
    // Observer-thread-safe leader id (atomic read)
    // -----------------------------------------------------------------------
    rune::consensus::NodeId leaderId() const noexcept {
        return observedLeaderId_.load(std::memory_order_acquire);
    }

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

        // Only write if changed to avoid unnecessary cache-line bouncing.
        if (observedLeaderId_.load(std::memory_order_relaxed) != newLeaderId) {
            observedLeaderId_.store(newLeaderId, std::memory_order_release);
        }
    }

    rune::consensus::AeronIngress&   ingress_;
    rune::consensus::ConsensusAgent& consensusAgent_;
    rune::consensus::Election&       election_;
    rune::consensus::NodeId          nodeId_;

    // Release-written by duty-cycle thread; acquire-read by observer threads.
    std::atomic<rune::consensus::NodeId> observedLeaderId_{-1};
};

} // namespace rune::node
