#pragma once
// ---------------------------------------------------------------------------
// FollowerLiveness.h — steady-state (CLOSED) follower liveness STATE MACHINE.
//
// Namespace: rune::consensus
// C++23.  Pure logic: no clock, no I/O, no Aeron, no allocation.  The caller
// supplies nowNs and acts on the returned Action.  Purity makes it exhaustively
// unit-testable in isolation (see test/FollowerLivenessTest.cpp) and lets each
// STATE own exactly the fields it runs on — instead of scattering
// timeOfLastLeaderUpdateNs_ / timeOfLastAppendPositionSentNs_ across Election.
//
// ── The two states (every state handled; transitions are event-driven) ───────
//   Unarmed   — this node is NOT a steady-state follower (it is the leader, or is
//               still canvassing / has accepted no leader).  No duties: decide()
//               returns {false,false}.  Entered at construction and via reset().
//   Following — this node has accepted a leader (electionComplete as follower).
//               Two periodic duties every tick:
//                 (1) SEND our AppendPosition to the leader on `sendIntervalNs_`
//                     so the leader's per-member liveness (eligibleNodeIds →
//                     partition failover) never goes stale.  WITHOUT this the
//                     leader marks every follower dead within one heartbeat window
//                     → eligible set collapses to the leader alone → failover can
//                     never reach quorum AND the balancer churns leadership.  This
//                     single omission broke DR failover *and* multinode throughput.
//                 (2) DETECT a dead leader: if no heartbeat within leaderTimeoutNs_
//                     signal re-election (the caller then reset()s us → Unarmed).
//
// Transitions (all driven by explicit events):
//   Unarmed  --onBecomeFollower(now)--> Following   (accepted a leader)
//   Following --reset()--------------->  Unarmed     (became leader / re-canvass)
//   Following --onLeaderHeartbeat(now)-> Following   (self-loop: refresh liveness)
//   Following --decide()==reElect------> (caller reset()s → Unarmed)
// Events in the wrong state are ignored (onLeaderHeartbeat while Unarmed is a no-op).
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cstdint>

namespace rune::consensus {

class FollowerLiveness {
public:
    enum class State : uint8_t { Unarmed, Following };

    static constexpr const char* toString(State s) noexcept {
        return s == State::Following ? "Following" : "Unarmed";
    }

    /// What a follower should do this duty tick.  Both false in Unarmed.
    struct Action {
        bool sendAppendPosition;   ///< republish our AppendPosition to the leader
        bool triggerReElection;    ///< leader silent past timeout → re-canvass
    };

    /// Set the cadence constants.  Called once from Election's ctor.
    void configure(int64_t sendIntervalNs, int64_t leaderTimeoutNs) noexcept {
        sendIntervalNs_  = sendIntervalNs;
        leaderTimeoutNs_ = leaderTimeoutNs;
    }

    /// EVENT: accepted a leader (electionComplete as a follower).  → Following.
    /// Arms the liveness timeout and forces an immediate first AppendPosition send.
    void onBecomeFollower(int64_t nowNs) noexcept {
        state_              = State::Following;
        lastLeaderUpdateNs_ = nowNs;       // liveness armed
        lastAppendSentNs_   = INT64_MIN;   // send on the next tick
    }

    /// EVENT: heartbeat from the CURRENT leader (NLT / matching-term CommitPosition).
    /// Self-loop in Following (refresh liveness); ignored in Unarmed (no leader).
    void onLeaderHeartbeat(int64_t nowNs) noexcept {
        if (state_ == State::Following) lastLeaderUpdateNs_ = nowNs;
    }

    /// EVENT: no longer a steady-state follower (became leader / re-entered canvass).
    /// → Unarmed.
    void reset() noexcept {
        state_              = State::Unarmed;
        lastLeaderUpdateNs_ = INT64_MIN;
        lastAppendSentNs_   = INT64_MIN;
    }

    /// Decide this tick's duties for the CURRENT state.  MUTATES: advances the send
    /// clock when a send is due (single source of truth for the cadence).
    [[nodiscard]] Action decide(int64_t nowNs) noexcept {
        if (state_ != State::Following) return {false, false};   // Unarmed: no duties

        Action a{false, false};
        // (1) Periodic AppendPosition send (immediately if never sent).
        if (lastAppendSentNs_ == INT64_MIN ||
            nowNs - lastAppendSentNs_ >= sendIntervalNs_) {
            lastAppendSentNs_    = nowNs;
            a.sendAppendPosition = true;
        }
        // (2) Dead-leader detection (Following is always armed on entry).
        if (nowNs >= lastLeaderUpdateNs_ + leaderTimeoutNs_) {
            a.triggerReElection = true;
        }
        return a;
    }

    /// Next wake-up deadline for the idle strategy.  INT64_MIN = "act now".
    [[nodiscard]] int64_t nextDeadlineNs() const noexcept {
        if (state_ != State::Following) return INT64_MIN;
        if (lastAppendSentNs_ == INT64_MIN) return INT64_MIN;   // send now
        const int64_t send = lastAppendSentNs_ + sendIntervalNs_;
        const int64_t live = lastLeaderUpdateNs_ + leaderTimeoutNs_;
        return std::min(send, live);
    }

    // ── Observability / test accessors ──────────────────────────────────────
    [[nodiscard]] State   state()              const noexcept { return state_; }
    [[nodiscard]] bool    armed()              const noexcept { return state_ == State::Following; }
    [[nodiscard]] int64_t lastLeaderUpdateNs() const noexcept { return lastLeaderUpdateNs_; }
    [[nodiscard]] int64_t lastAppendSentNs()   const noexcept { return lastAppendSentNs_; }

private:
    State   state_              = State::Unarmed;
    int64_t sendIntervalNs_     = 0;
    int64_t leaderTimeoutNs_    = 0;
    int64_t lastAppendSentNs_   = INT64_MIN;  ///< last AppendPosition send (INT64_MIN = never)
    int64_t lastLeaderUpdateNs_ = INT64_MIN;  ///< last leader heartbeat
};

} // namespace rune::consensus
