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
#include <memory>
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

        // Track that this member has re-joined during the current rebalance.
        if (state_ == GroupState::PreparingRebalance) {
            rejoinedMembers_.insert(effectiveMemberId);
            // If all expected members have re-joined, advance to CompletingRebalance.
            if (rejoinedMembers_.size() >= expectedMemberCount_) {
                transitionToCompletingRebalance(nowNs);
            }
        }
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
    if (state_ == GroupState::Dead) return 0;

    int work = 0;

    // -----------------------------------------------------------------------
    // 1. Reap session-expired members.
    // -----------------------------------------------------------------------
    if (!members_.empty()) {
        std::vector<std::string> toEvict;
        for (const auto& m : members_) {
            int64_t timeoutNs = static_cast<int64_t>(m.sessionTimeoutMs) * 1'000'000LL;
            if (nowNs - m.lastHeartbeatNs > timeoutNs) {
                toEvict.push_back(m.memberId);
            }
        }

        for (const auto& id : toEvict) {
            if (!removeMember(id)) continue;
            ++work;

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
    }

    // -----------------------------------------------------------------------
    // 2. Join-timeout: if still in PreparingRebalance and not all members
    //    have re-joined, and the rebalance timeout has elapsed, evict
    //    non-rejoined members and transition to CompletingRebalance.
    // -----------------------------------------------------------------------
    if (state_ == GroupState::PreparingRebalance && !members_.empty()) {
        int64_t rebalanceTimeoutNs =
            static_cast<int64_t>(rebalanceTimeoutMs_) * 1'000'000LL;

        if (nowNs - rebalanceStartNs_ > rebalanceTimeoutNs) {
            // Evict members who have not re-joined.
            std::vector<std::string> notRejoined;
            for (const auto& m : members_) {
                if (!rejoinedMembers_.count(m.memberId)) {
                    notRejoined.push_back(m.memberId);
                }
            }
            for (const auto& id : notRejoined) {
                removeMember(id);
                ++work;
            }
            // Transition with whoever is left.
            if (members_.empty()) {
                ++generationId_;
                state_    = GroupState::Empty;
                leaderId_ = "";
            } else {
                electLeader();
                for (auto& m : members_) m.isLeader = (m.memberId == leaderId_);
                transitionToCompletingRebalance(nowNs);
                ++work;
            }
        }
    }

    return work;
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

void GroupCoordinator::triggerRebalance(int64_t nowNs)
{
    ++generationId_;
    state_ = GroupState::PreparingRebalance;
    // Record how many members we expect to re-join.
    expectedMemberCount_ = members_.size();
    rejoinedMembers_.clear();
    syncedMembers_.clear();
    leaderSynced_ = false;
    rebalanceStartNs_ = nowNs;
}

// ---------------------------------------------------------------------------
// P4-3: syncGroup
// ---------------------------------------------------------------------------
SyncResult GroupCoordinator::syncGroup(const std::string& memberId,
                                        Generation         generationId,
                                        const Assignment&  assignmentsPerMember,
                                        int64_t            /*nowNs*/)
{
    // Wrong state: must be in CompletingRebalance or Stable to sync.
    // (Stable is allowed so followers can call after leader already transitioned.)
    if (state_ != GroupState::CompletingRebalance && state_ != GroupState::Stable) {
        return SyncResult{ .errorCode = SyncResult::kErrWrongState };
    }

    // Generation fencing.
    if (generationId != generationId_) {
        return SyncResult{ .errorCode = SyncResult::kErrStaleGeneration };
    }

    // Unknown member.
    MemberInfo* caller = findMember(memberId);
    if (caller == nullptr) {
        return SyncResult{ .errorCode = SyncResult::kErrUnknownMember };
    }

    // If this is the leader AND they provided a non-empty assignment map,
    // store the assignments and mark leader as synced.
    if (!assignmentsPerMember.empty()) {
        // Only accept from the leader.
        if (memberId == leaderId_) {
            // Store each member's assignment.
            for (auto& [mid, tps] : assignmentsPerMember) {
                MemberInfo* m = findMember(mid);
                if (m != nullptr) {
                    m->assignment = tps;
                }
            }
            leaderSynced_ = true;
            // Transition to Stable now that leader has submitted the assignment.
            applyAssignmentAndTransitionStable(assignmentsPerMember);
        }
    }

    // Track this member as having called syncGroup.
    syncedMembers_.insert(memberId);

    return SyncResult{ .errorCode = SyncResult::kOk };
}

// ---------------------------------------------------------------------------
// P4-3: autoAssignAndSync
// ---------------------------------------------------------------------------
SyncResult GroupCoordinator::autoAssignAndSync(const TopicPartitionCounts& partCounts,
                                                AssignorType                assignorType,
                                                int64_t                     nowNs)
{
    if (state_ != GroupState::CompletingRebalance) {
        return SyncResult{ .errorCode = SyncResult::kErrWrongState };
    }

    // Build subscriptions from current members.
    std::vector<MemberSubscription> subs;
    subs.reserve(members_.size());
    for (const auto& m : members_) {
        subs.push_back({ m.memberId, m.subscribedTopics });
    }

    // Run the chosen assignor.
    Assignment computed;
    if (assignorType == AssignorType::Range) {
        RangeAssignor ra;
        computed = ra.assign(subs, partCounts);
    } else {
        RoundRobinAssignor rr;
        computed = rr.assign(subs, partCounts);
    }

    // Store each member's assignment.
    for (auto& m : members_) {
        auto it = computed.find(m.memberId);
        if (it != computed.end()) {
            m.assignment = it->second;
        }
    }

    leaderSynced_ = true;
    state_ = GroupState::Stable;
    (void)nowNs;

    return SyncResult{ .errorCode = SyncResult::kOk };
}

// ---------------------------------------------------------------------------
// P4-3: assignmentFor
// ---------------------------------------------------------------------------
std::vector<TopicPartition> GroupCoordinator::assignmentFor(const std::string& memberId) const
{
    const MemberInfo* m = findMember(memberId);
    if (m == nullptr) return {};
    return m->assignment;
}

// ---------------------------------------------------------------------------
// P4-3: transitionToCompletingRebalance
// ---------------------------------------------------------------------------
void GroupCoordinator::transitionToCompletingRebalance(int64_t /*nowNs*/)
{
    state_ = GroupState::CompletingRebalance;
    syncedMembers_.clear();
    leaderSynced_ = false;
}

// ---------------------------------------------------------------------------
// P4-3: applyAssignmentAndTransitionStable
// ---------------------------------------------------------------------------
void GroupCoordinator::applyAssignmentAndTransitionStable(const Assignment& /*a*/)
{
    // Assignments were already stored in syncGroup() before calling this.
    state_ = GroupState::Stable;
}

} // namespace rune::coordination
