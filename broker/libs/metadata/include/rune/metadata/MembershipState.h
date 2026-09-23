#pragma once
// MembershipState.h — Controller-side soft membership / liveness table.
//
// This state is OFF the consensus log: it is rebuilt by replaying heartbeats on
// controller failover.  Only DECISIONS (fence/unfence) are durable (Task 4).
//
// Guards implemented:
//   - Incarnation-id zombie guard: different incarnationId while session is
//     still valid → RejectedZombie (same nodeId, different process).
//   - Broker-epoch: registration log offset, monotonic, set by MSM apply.
//   - Unfence-only-after-catch-up: markCatchUpComplete() moves state to Unfenced.
//   - Overload safety valve: setState() is a no-op while overloaded_ == true.
//   - Clock-rollover guard in isExpired(): now < lastHeartbeat → not expired.
//
// Namespace: rune::metadata

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

namespace rune::metadata {

// ---------------------------------------------------------------------------
// NodeControlState — mirrors Kafka KIP-631 §8 broker state machine.
// ---------------------------------------------------------------------------
enum class NodeControlState {
    Fenced,            // controller-directed fence (e.g. overload/reassignment)
    Unfenced,          // fence lifted; node may serve once caughtUp==true
    ControlledShutdown,// graceful drain in progress
    ShutdownNow,       // immediate shutdown (no drain)
};

// ---------------------------------------------------------------------------
// MembershipState
// ---------------------------------------------------------------------------
class MembershipState {
public:
    // Default session guard for the zombie check in onJoin: 30 seconds in nanoseconds.
    // Can be overridden at construction for testing with small timestamp values.
    static constexpr int64_t kDefaultSessionTimeoutNs = 30'000'000'000LL;

    explicit MembershipState(int64_t sessionTimeoutNs = kDefaultSessionTimeoutNs) noexcept
        : sessionTimeoutNs_(sessionTimeoutNs) {}

    // -----------------------------------------------------------------------
    // Public types
    // -----------------------------------------------------------------------
    struct MemberInfo {
        int32_t          nodeId          = 0;
        std::string      endpoint;
        int64_t          incarnationId   = 0;
        int64_t          brokerEpoch     = 0;
        int64_t          lastHeartbeatNs = 0;
        NodeControlState state           = NodeControlState::Unfenced;
        bool             caughtUp        = false; // §8 gate: true only after markCatchUpComplete()
    };

    enum class JoinResult {
        Accepted,         // new node; inserted as Unfenced
        IdempotentAmend,  // same nodeId + same incarnationId → endpoint/epoch updated
        RejectedZombie,   // different incarnationId while incumbent session is still valid
    };

    // -----------------------------------------------------------------------
    // Join / heartbeat / leave
    // -----------------------------------------------------------------------

    /// Process a broker registration attempt.
    ///   - New nodeId              → insert as Unfenced, caughtUp=false, return Accepted.
    ///   - Same incarnationId      → amend endpoint/epoch only (caughtUp preserved), return IdempotentAmend.
    ///   - Different incarnationId + session still valid  → RejectedZombie.
    ///   - Different incarnationId + session expired      → replace, caughtUp=false, return Accepted.
    ///
    /// NOTE: a freshly joined node is Unfenced but NOT yet eligible (caughtUp==false).
    /// Call markCatchUpComplete() once the node has replayed the metadata log.
    /// Use eligibleNodes() — not liveNodes() — for assignment/election consumers.
    JoinResult onJoin(int32_t nodeId, std::string endpoint,
                      int64_t incarnationId, int64_t brokerEpoch,
                      int64_t nowNs) {
        auto it = members_.find(nodeId);
        if (it == members_.end()) {
            // Brand-new node — starts Unfenced but not yet eligible (caughtUp=false).
            MemberInfo info;
            info.nodeId          = nodeId;
            info.endpoint        = std::move(endpoint);
            info.incarnationId   = incarnationId;
            info.brokerEpoch     = brokerEpoch;
            info.lastHeartbeatNs = nowNs;
            info.state           = NodeControlState::Unfenced;
            info.caughtUp        = false;
            members_.emplace(nodeId, std::move(info));
            return JoinResult::Accepted;
        }

        MemberInfo& existing = it->second;

        if (existing.incarnationId == incarnationId) {
            // Same incarnation — idempotent re-registration; amend mutable fields.
            // caughtUp is NOT reset: a live, already-caught-up node stays eligible.
            existing.endpoint        = std::move(endpoint);
            existing.brokerEpoch     = brokerEpoch;
            existing.lastHeartbeatNs = nowNs;
            return JoinResult::IdempotentAmend;
        }

        // Different incarnation: check whether the incumbent session is still alive.
        // Per brief: "if NOT expired at nowNs (session valid) → RejectedZombie;
        //             if session expired → replace incarnation, Accepted."
        // Use the class-level session timeout for this guard.
        const bool sessionValid = !isExpiredWith(existing.lastHeartbeatNs,
                                                  nowNs,
                                                  sessionTimeoutNs_);
        if (sessionValid) {
            return JoinResult::RejectedZombie;
        }

        // Session expired: replace with new incarnation.
        // Fresh incarnation must re-catch-up before becoming eligible (caughtUp=false).
        existing.endpoint        = std::move(endpoint);
        existing.incarnationId   = incarnationId;
        existing.brokerEpoch     = brokerEpoch;
        existing.lastHeartbeatNs = nowNs;
        existing.state           = NodeControlState::Unfenced;
        existing.caughtUp        = false;
        return JoinResult::Accepted;
    }

    /// Record a heartbeat.  No-op for unknown nodeId.
    void onHeartbeat(int32_t nodeId, int64_t nowNs) noexcept {
        auto it = members_.find(nodeId);
        if (it != members_.end()) {
            it->second.lastHeartbeatNs = nowNs;
        }
    }

    /// Remove a node entirely (clean leave).
    void onLeave(int32_t nodeId) {
        members_.erase(nodeId);
    }

    // -----------------------------------------------------------------------
    // Liveness queries
    // -----------------------------------------------------------------------

    /// Returns true when nodeId's session has expired.
    /// Clock-rollover guard: if nowNs < lastHeartbeatNs, treat as NOT expired.
    [[nodiscard]] bool isExpired(int32_t nodeId,
                                  int64_t nowNs,
                                  int64_t sessionTimeoutNs) const noexcept {
        auto it = members_.find(nodeId);
        if (it == members_.end()) return true; // absent = expired
        return isExpiredWith(it->second.lastHeartbeatNs, nowNs, sessionTimeoutNs);
    }

    /// All members whose session is not expired AND whose state is not Fenced or ShutdownNow.
    /// NOTE: liveness != eligibility.  A just-joined node appears here but is NOT yet
    /// eligible for assignment/election (caughtUp==false).  Use eligibleNodes() for that.
    [[nodiscard]] std::vector<int32_t> liveNodes(int64_t nowNs,
                                                   int64_t sessionTimeoutNs) const {
        std::vector<int32_t> result;
        result.reserve(members_.size());
        for (const auto& [id, info] : members_) {
            if (info.state == NodeControlState::Fenced ||
                info.state == NodeControlState::ShutdownNow) {
                continue;
            }
            if (isExpiredWith(info.lastHeartbeatNs, nowNs, sessionTimeoutNs)) {
                continue;
            }
            result.push_back(id);
        }
        return result;
    }

    /// KIP-631 §8 eligibility: Unfenced AND caughtUp AND session not expired.
    /// Assignment and election consumers MUST use this instead of liveNodes().
    [[nodiscard]] std::vector<int32_t> eligibleNodes(int64_t nowNs,
                                                      int64_t sessionTimeoutNs) const {
        std::vector<int32_t> result;
        result.reserve(members_.size());
        for (const auto& [id, info] : members_) {
            if (info.state != NodeControlState::Unfenced) continue;
            if (!info.caughtUp) continue;
            if (isExpiredWith(info.lastHeartbeatNs, nowNs, sessionTimeoutNs)) continue;
            result.push_back(id);
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------------

    /// Signal that nodeId has replayed the metadata log and is eligible to serve.
    /// Sets caughtUp=true unconditionally (regardless of fence state).
    /// Also promotes Fenced → Unfenced if the node was fenced (backwards-compat).
    /// Returns true if nodeId exists (false = unknown node, no-op).
    bool markCatchUpComplete(int32_t nodeId) {
        auto it = members_.find(nodeId);
        if (it == members_.end()) return false;
        it->second.caughtUp = true;
        if (it->second.state == NodeControlState::Fenced) {
            it->second.state = NodeControlState::Unfenced;
        }
        return true;
    }

    /// Transition a node's control state.  No-op while overloaded_ is true
    /// (overload safety valve — suppress state transitions under controller backlog).
    void setState(int32_t nodeId, NodeControlState newState) {
        if (overloaded_) return; // overload valve: suppress state transitions
        auto it = members_.find(nodeId);
        if (it != members_.end()) {
            it->second.state = newState;
        }
    }

    // -----------------------------------------------------------------------
    // Overload valve
    // -----------------------------------------------------------------------

    void setOverloaded(bool v) noexcept { overloaded_ = v; }
    [[nodiscard]] bool overloaded() const noexcept { return overloaded_; }

    // -----------------------------------------------------------------------
    // Read-only access
    // -----------------------------------------------------------------------

    /// Returns nullptr if nodeId is not present.
    [[nodiscard]] const MemberInfo* find(int32_t nodeId) const noexcept {
        auto it = members_.find(nodeId);
        return (it != members_.end()) ? &it->second : nullptr;
    }

    [[nodiscard]] std::size_t size() const noexcept { return members_.size(); }

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Core expiry predicate — clock-rollover guarded.
    [[nodiscard]] static bool isExpiredWith(int64_t lastHeartbeatNs,
                                             int64_t nowNs,
                                             int64_t sessionTimeoutNs) noexcept {
        // Clock-rollover guard: if clock went backwards treat session as valid.
        if (nowNs < lastHeartbeatNs) return false;
        return (nowNs - lastHeartbeatNs) > sessionTimeoutNs;
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, MemberInfo> members_;
    bool    overloaded_       = false;
    int64_t sessionTimeoutNs_ = kDefaultSessionTimeoutNs;
};

} // namespace rune::metadata
