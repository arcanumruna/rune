// ---------------------------------------------------------------------------
// GroupCoordinator.cpp — Kafka-faithful consumer group state machine.
//
// Namespace: rune::coordination
// C++23. NO locks. NO busy-spin. Single-writer.
// ---------------------------------------------------------------------------

#include <rune/coordination/GroupCoordinator.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdint>
#include <memory>
#include <optional>
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
void GroupCoordinator::leave(const std::string& memberId,
                              Generation         generationId,
                              int64_t            nowNs)
{
    // Kafka LeaveGroup fencing: a stale-generation leave is silently ignored.
    // An unknown member is also a no-op (handled by removeMember below).
    if (generationId != generationId_) {
        return; // stale or future generation — no-op, do NOT rebalance
    }

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
    // Incorporate the GLOBAL atomic seq so that memberIds are unique across
    // multiple GroupCoordinator instances in the same process.  Without this,
    // two coordinators with the same groupId produce identical ids
    // ("grp-1", "grp-2", ...).
    //
    // Format: "<groupId>-g<globalSeq>-<localSeq>"
    //   globalSeq: monotonically increasing across ALL coordinators in the process.
    //   localSeq:  per-instance counter (extra disambiguation + readability).
    uint64_t local  = ++memberSeq_;
    uint64_t global = s_memberSeq.fetch_add(1, std::memory_order_relaxed);
    return groupId_ + "-g" + std::to_string(global) + "-" + std::to_string(local);
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
    // Keep rejoinedMembers_ consistent with live membership (S1 fix).
    rejoinedMembers_.erase(memberId);
    return true;
}

/// Compare two memberIds for deterministic leader election.
///
/// Generated ids have the form "<group>-g<globalSeq>-<localSeq>", so an id
/// carries MULTIPLE numeric fields (the global seq and the local seq).  Pure
/// lexicographic comparison is wrong whenever ANY numeric field crosses a
/// power-of-ten boundary ("grp-g10-2" < "grp-g2-3" lexicographically because
/// "g1" < "g2", even though global seq 2 was minted before 10).  Comparing
/// only the trailing (<local>) field numerically — the prior fix — leaves the
/// same surprise on the <global> field one level up (audit N1-a).
///
/// Strategy: NATURAL ordering.  Tokenize each id into an alternating sequence
/// of non-digit and digit runs and compare run-by-run: digit runs compared as
/// integers (by value, so 2 < 10 regardless of digit count), non-digit runs
/// compared lexicographically.  This orders EVERY numeric field of the
/// generated id — both <global> and <local> — as a numeric tuple, while still
/// giving a well-defined total order for arbitrary hand-supplied ids (e.g.
/// "member-a").  It is a valid strict-weak ordering (irreflexive,
/// antisymmetric, transitive: the first differing run decides).
static bool memberIdLess(const std::string& a, const std::string& b)
{
    auto isDigit = [](char c) { return std::isdigit(static_cast<unsigned char>(c)) != 0; };

    std::size_t ia = 0, ib = 0;
    const std::size_t na = a.size(), nb = b.size();

    while (ia < na && ib < nb) {
        const bool da = isDigit(a[ia]);
        const bool db = isDigit(b[ib]);

        if (da && db) {
            // Both at a digit run: compare the runs numerically.  Skip leading
            // zeros so run length reflects magnitude, then compare by length,
            // then lexicographically (both digits → char order == value order).
            std::size_t sa = ia, sb = ib;
            while (sa < na && a[sa] == '0') ++sa;
            while (sb < nb && b[sb] == '0') ++sb;
            std::size_t ea = sa; while (ea < na && isDigit(a[ea])) ++ea;
            std::size_t eb = sb; while (eb < nb && isDigit(b[eb])) ++eb;

            const std::size_t la = ea - sa; // significant-digit count
            const std::size_t lb = eb - sb;
            if (la != lb) return la < lb;    // fewer significant digits → smaller
            for (std::size_t k = 0; k < la; ++k) {
                if (a[sa + k] != b[sb + k]) return a[sa + k] < b[sb + k];
            }
            // Equal magnitude: shorter raw run (fewer leading zeros) sorts first
            // for a stable, deterministic total order.
            const std::size_t rawA = ea - ia, rawB = eb - ib;
            if (rawA != rawB) return rawA < rawB;
            // Fully equal digit run: advance both past it.
            ia = ea; ib = eb;
        } else if (!da && !db) {
            // Both at a non-digit char: compare lexicographically, advance one.
            if (a[ia] != b[ib]) return a[ia] < b[ib];
            ++ia; ++ib;
        } else {
            // One is a digit, the other a non-digit at this position.  Define a
            // stable order: digit runs sort before non-digit runs.
            return da;
        }
    }

    // Shared prefix consumed: the shorter id sorts first.
    return (na - ia) < (nb - ib);
}

void GroupCoordinator::electLeader()
{
    if (members_.empty()) {
        leaderId_ = "";
        return;
    }
    // Deterministic: numerically-correct smallest memberId.
    const std::string* smallest = &members_[0].memberId;
    for (const auto& m : members_) {
        if (memberIdLess(m.memberId, *smallest)) {
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
    // (Stable is allowed so followers can retrieve their assignment after
    // the leader already transitioned the group.)
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

    // Leader branch: only honored during CompletingRebalance (not Stable).
    // A leader re-sync while Stable is silently ack'd (no clobber).
    if (!assignmentsPerMember.empty() && memberId == leaderId_) {
        if (state_ == GroupState::CompletingRebalance && !leaderSynced_) {
            storeAssignments(assignmentsPerMember);   // shared helper (no CL3 duplication)
            leaderSynced_ = true;
            transitionToStable();
        }
        // In Stable (or leaderSynced_ already): ack without re-writing assignments.
        return SyncResult{ .errorCode = SyncResult::kOk };
    }

    // Follower branch.
    if (state_ == GroupState::CompletingRebalance && !leaderSynced_) {
        // Leader hasn't submitted the assignment yet.  Tell the follower to
        // retry (Kafka holds the SyncGroup response; we return a retry signal).
        return SyncResult{ .errorCode = SyncResult::kErrRebalanceInProgress };
    }

    // Stable or leader already synced: follower can read its assignment.
    return SyncResult{ .errorCode = SyncResult::kOk };
}

// ---------------------------------------------------------------------------
// P4-3: storeAssignments (shared helper — eliminates CL3 duplication)
// ---------------------------------------------------------------------------
void GroupCoordinator::storeAssignments(const Assignment& a)
{
    for (auto& [mid, tps] : a) {
        MemberInfo* m = findMember(mid);
        if (m != nullptr) {
            m->assignment = tps;
        }
    }
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

    storeAssignments(computed);   // shared with syncGroup — no duplication
    leaderSynced_ = true;
    transitionToStable();
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
    leaderSynced_ = false;
}

// ---------------------------------------------------------------------------
// P4-3: transitionToStable
// ---------------------------------------------------------------------------
void GroupCoordinator::transitionToStable()
{
    // Assignments were already stored by the caller (syncGroup / autoAssignAndSync).
    state_ = GroupState::Stable;
}

} // namespace rune::coordination
