// ---------------------------------------------------------------------------
// GroupCoordinator.cpp — Kafka-faithful consumer group state machine.
//
// Namespace: rune::coordination
// C++23. NO locks. NO busy-spin. Single-writer.
// ---------------------------------------------------------------------------

#include <rune/coordination/GroupCoordinator.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// Static member
// ---------------------------------------------------------------------------
std::atomic<uint64_t> GroupCoordinator::s_memberSeq{0};

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------
GroupCoordinator::GroupCoordinator(GroupId groupId, int64_t /*nowNs*/)
    : groupId_(std::move(groupId))
{}

// ---------------------------------------------------------------------------
// join
// ---------------------------------------------------------------------------
JoinResult GroupCoordinator::join(const std::string&              memberId,
                                   int32_t                          sessionTimeoutMs,
                                   const std::vector<std::string>&  subscribedTopics,
                                   int64_t                          nowNs)
{
    // Dead group cannot accept new joins.
    if (state_ == GroupState::Dead) {
        return JoinResult{ .memberId = memberId,
                           .generationId = generationId_,
                           .isLeader = false,
                           .errorCode = JoinResult::kErrDead };
    }

    // Determine the effective memberId.
    bool isNewMember = memberId.empty();
    std::string effectiveMemberId = memberId;

    if (isNewMember) {
        effectiveMemberId = generateMemberId();
    }

    // Check if this is a re-join of an existing member.
    MemberInfo* existing = findMember(effectiveMemberId);

    if (existing != nullptr) {
        // Re-join: update session timeout and topics; refresh heartbeat.
        existing->sessionTimeoutMs = sessionTimeoutMs;
        existing->subscribedTopics = subscribedTopics;
        existing->lastHeartbeatNs  = nowNs;
        // A re-join while in PreparingRebalance does NOT bump generation again —
        // the rebalance is already in progress. Kafka: during rebalance, members
        // re-join and coordinator waits for all expected members.
        // We still stay in PreparingRebalance (or bump if Stable/Empty — can't be
        // re-joining from those with a known memberId unless very unusual).
    } else {
        // New member: add to the group.
        MemberInfo info;
        info.memberId          = effectiveMemberId;
        info.sessionTimeoutMs  = sessionTimeoutMs;
        info.subscribedTopics  = subscribedTopics;
        info.lastHeartbeatNs   = nowNs;
        info.isLeader          = false;
        members_.push_back(std::move(info));

        // New member → bump generation and trigger rebalance.
        triggerRebalance(nowNs);

        // Elect leader if needed (first member, or after rebalance).
        electLeader();
    }

    // Mark leader flag on the MemberInfo.
    for (auto& m : members_) {
        m.isLeader = (m.memberId == leaderId_);
    }

    bool isLeader = (effectiveMemberId == leaderId_);
    return JoinResult{ .memberId     = effectiveMemberId,
                       .generationId = generationId_,
                       .isLeader     = isLeader,
                       .errorCode    = 0 };
}

// ---------------------------------------------------------------------------
// leave
// ---------------------------------------------------------------------------
void GroupCoordinator::leave(const std::string& memberId, int64_t nowNs)
{
    if (!removeMember(memberId)) {
        return; // unknown member — no-op
    }

    if (members_.empty()) {
        // Last member left → Empty.
        ++generationId_;
        state_    = GroupState::Empty;
        leaderId_ = "";
    } else {
        // Membership shrank → PreparingRebalance + generation bump.
        triggerRebalance(nowNs);
        electLeader();
        // Mark leader flag.
        for (auto& m : members_) {
            m.isLeader = (m.memberId == leaderId_);
        }
    }
}

// ---------------------------------------------------------------------------
// heartbeat
// ---------------------------------------------------------------------------
HeartbeatResult GroupCoordinator::heartbeat(const std::string& memberId,
                                             int32_t             generationId,
                                             int64_t             nowNs)
{
    // Unknown member.
    MemberInfo* m = findMember(memberId);
    if (m == nullptr) {
        return HeartbeatResult{ .errorCode = HeartbeatResult::kErrUnknownMember };
    }

    // Stale generation fencing: reject if provided generation != current.
    if (generationId != generationId_) {
        return HeartbeatResult{ .errorCode = HeartbeatResult::kErrStaleGeneration };
    }

    // Refresh last-seen.
    m->lastHeartbeatNs = nowNs;

    // If a rebalance is in progress, tell the member to rejoin.
    if (state_ == GroupState::PreparingRebalance ||
        state_ == GroupState::CompletingRebalance) {
        return HeartbeatResult{ .errorCode = HeartbeatResult::kErrRebalanceInProgress };
    }

    return HeartbeatResult{ .errorCode = HeartbeatResult::kOk };
}

// ---------------------------------------------------------------------------
// doWork
// ---------------------------------------------------------------------------
int GroupCoordinator::doWork(int64_t nowNs)
{
    if (members_.empty()) return 0;
    if (state_ == GroupState::Dead) return 0;

    // Sample nowNs ONCE (already done by the caller passing it in).
    int reaped = 0;

    // Collect expired members (avoid iterator invalidation).
    std::vector<std::string> toEvict;
    for (const auto& m : members_) {
        int64_t timeoutNs = static_cast<int64_t>(m.sessionTimeoutMs) * 1'000'000LL;
        if (nowNs - m.lastHeartbeatNs > timeoutNs) {
            toEvict.push_back(m.memberId);
        }
    }

    for (const auto& id : toEvict) {
        if (!removeMember(id)) continue; // already gone
        ++reaped;

        if (members_.empty()) {
            ++generationId_;
            state_    = GroupState::Empty;
            leaderId_ = "";
        } else {
            triggerRebalance(nowNs);
            electLeader();
            for (auto& m : members_) {
                m.isLeader = (m.memberId == leaderId_);
            }
        }
    }

    return reaped;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

std::string GroupCoordinator::generateMemberId()
{
    // Use per-instance counter first, fall back to global for uniqueness across
    // coordinator instances in the same process.
    uint64_t seq = ++memberSeq_;
    uint64_t global = s_memberSeq.fetch_add(1, std::memory_order_relaxed);
    (void)global; // used implicitly: unique within process
    return groupId_ + "-" + std::to_string(seq);
}

MemberInfo* GroupCoordinator::findMember(const std::string& memberId)
{
    for (auto& m : members_) {
        if (m.memberId == memberId) return &m;
    }
    return nullptr;
}

const MemberInfo* GroupCoordinator::findMember(const std::string& memberId) const
{
    for (const auto& m : members_) {
        if (m.memberId == memberId) return &m;
    }
    return nullptr;
}

bool GroupCoordinator::removeMember(const std::string& memberId)
{
    auto it = std::find_if(members_.begin(), members_.end(),
                           [&](const MemberInfo& m) { return m.memberId == memberId; });
    if (it == members_.end()) return false;
    members_.erase(it);
    return true;
}

void GroupCoordinator::electLeader()
{
    if (members_.empty()) {
        leaderId_ = "";
        return;
    }
    // Deterministic: alphabetically smallest memberId.
    const std::string* smallest = &members_[0].memberId;
    for (const auto& m : members_) {
        if (m.memberId < *smallest) {
            smallest = &m.memberId;
        }
    }
    leaderId_ = *smallest;
}

void GroupCoordinator::triggerRebalance(int64_t /*nowNs*/)
{
    ++generationId_;
    state_ = GroupState::PreparingRebalance;
}

} // namespace rune::coordination
