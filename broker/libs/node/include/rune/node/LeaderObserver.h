#pragma once
// LeaderObserver.h — F9a: the control thread's leadership view, published to an
// observer thread over a ring (no atomics).
//
// OWNERSHIP
//   The leadership view (leader id, leadership term, "this node leads") is
//   derived from Election / NodeConsensusAgent state, which is owned by the
//   CONTROL thread (the AgentRunner thread that drives NodeRuntimeAgent).
//
//   - Control thread: computeOnControl() reads that state directly (plain
//     reads, owner thread).  publishOnControl() runs once per duty cycle (from
//     NodeRuntimeAgent) and, only when the view CHANGED, offers a snapshot
//     message on ring_.  A full ring (observer not draining) is not an error:
//     lastPublished_ is left unchanged, so the next cycle retries with the then
//     current view — the observer always converges on the latest view.
//   - Observer thread (EXACTLY ONE, enforced): bindObserver() — called by
//     BrokerNode::start() — makes the CALLING thread the only observer.
//     observe() drains ring_ and returns the latest snapshot; observed_ is owned
//     by that thread.  observe() from any OTHER thread is refused: logged
//     critical then std::abort() in ALL builds — a second consumer is a
//     programming error and a silent "no leader" in release would hide it.
//     Before bindObserver() nothing is published (the control thread is not
//     running) and observe() returns the initial view without draining.
//     There is no rebinding: bindObserver() runs once, from start().
//
//   Everything the CONTROL thread itself needs (gateway leaderView, group / txn
//   coordinators, admin role hook) MUST use computeOnControl(), never observe():
//   observe() on the control thread would make it a second ring consumer.
//
// Before the control thread runs (or after it was joined) observe() returns
// the last published view (initially "no leader", term 0).

#include <rune/consensus/ControlRing.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/Types.h>
#include <rune/node/NodeConsensusAgent.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstdlib>
#include <thread>

namespace rune::node {

struct LeaderSnapshot {
    rune::consensus::NodeId leaderId = -1;   ///< -1 = no leader known
    rune::consensus::Term   term     = 0;
    bool                    isLeader = false;

    friend bool operator==(const LeaderSnapshot&, const LeaderSnapshot&) = default;
};

class LeaderObserver {
public:
    explicit LeaderObserver(rune::consensus::NodeId self) noexcept : self_(self) {}

    LeaderObserver(const LeaderObserver&)            = delete;
    LeaderObserver& operator=(const LeaderObserver&) = delete;

    /// Startup thread, BEFORE the control thread starts (ordered by its start).
    /// nodeAgent == nullptr → single-node (leadership from the election state).
    void bind(const rune::consensus::Election* election,
              const NodeConsensusAgent*        nodeAgent) noexcept {
        election_  = election;
        nodeAgent_ = nodeAgent;
    }

    // ---------------------------------------------------------------- control
    /// CONTROL thread only: the current view, read from owner state.
    [[nodiscard]] LeaderSnapshot computeOnControl() const noexcept {
        LeaderSnapshot s;
        if (!election_) return s;
        s.term = election_->leadershipTermId();
        if (nodeAgent_) {
            s.leaderId = nodeAgent_->leaderId();
            s.isLeader = (s.leaderId == self_);
            return s;
        }
        // Single-node: every leader state, including the pass-through ones.
        using ES = rune::consensus::ElectionState;
        const auto st = static_cast<ES>(election_->stateCode());
        s.isLeader = st == ES::LeaderLogReplication || st == ES::LeaderReplay ||
                     st == ES::LeaderInit || st == ES::LeaderReady || st == ES::Closed;
        s.leaderId = s.isLeader ? self_ : -1;
        return s;
    }

    /// CONTROL thread only, once per duty cycle: publish the view if it changed.
    /// Never blocks; returns 0 (publishing is bookkeeping, not duty-cycle work).
    int publishOnControl() noexcept { return publish(computeOnControl()); }

    /// CONTROL thread only: publish `now` if it differs from the last published
    /// view (publishOnControl's core; also the unit-test seam).
    int publish(const LeaderSnapshot& now) noexcept {
        if (now == lastPublished_) return 0;
        if (ring_.offer(now.isLeader ? kLeaderMsg : kNotLeaderMsg,
                        static_cast<std::int64_t>(now.leaderId),
                        static_cast<std::int64_t>(now.term))) {
            lastPublished_ = now;
        }
        // else: ring full — retried next cycle with the then-current view.
        return 0;
    }

    // --------------------------------------------------------------- observer
    /// Make the CALLING thread the one observer (BrokerNode::start()).
    void bindObserver() noexcept { observer_ = std::this_thread::get_id(); }

    /// OBSERVER thread only (enforced): drain published snapshots; return the
    /// latest.  Another thread is refused (critical log + abort, all builds).
    LeaderSnapshot observe() noexcept {
        if (observer_ == std::thread::id{}) return observed_;   // unbound: nothing published
        if (std::this_thread::get_id() != observer_) [[unlikely]] {
            refuseForeignObserver_();
            return LeaderSnapshot{};
        }
        ring_.poll([this](std::int32_t type, std::int64_t a, std::int64_t b) noexcept {
            observed_.isLeader = (type == kLeaderMsg);
            observed_.leaderId = static_cast<rune::consensus::NodeId>(a);
            observed_.term     = static_cast<rune::consensus::Term>(b);
        });
        return observed_;
    }

private:
    [[gnu::cold]] [[gnu::noinline]] static void refuseForeignObserver_() noexcept {
        spdlog::critical("LeaderObserver: observe() from a thread that is not the bound "
                         "observer — the leadership ring has exactly ONE consumer (F9a); "
                         "refused");
        std::abort();
    }

    static constexpr std::int32_t kNotLeaderMsg = 1;
    static constexpr std::int32_t kLeaderMsg    = 2;

    const rune::consensus::NodeId     self_;
    const rune::consensus::Election*  election_  = nullptr;   // set before control start
    const NodeConsensusAgent*         nodeAgent_ = nullptr;   // set before control start

    LeaderSnapshot                    lastPublished_{};       // control-thread-owned
    rune::consensus::ControlRing<4096> ring_;                 // control → observer
    LeaderSnapshot                    observed_{};            // observer-thread-owned
    std::thread::id                   observer_{};            // set by bindObserver()
};

} // namespace rune::node
