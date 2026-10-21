#pragma once
// ---------------------------------------------------------------------------
// GroupCoordinator.h — Kafka-faithful consumer group state machine.
//
// Namespace: rune::coordination
// C++23. NO locks. NO busy-spin. Single-writer doWork(nowNs) driven.
//
// ## Overview (spec P4-2)
//
// GroupCoordinator manages ONE consumer group's membership and generation
// (epoch), mirroring Kafka's GroupCoordinator/GroupMetadata design.
//
// ## State machine (Kafka-faithful)
//
//   Empty             — no members (initial state, or after all leave/expire).
//   PreparingRebalance — membership is changing; members must re-join.
//   CompletingRebalance — all expected members have joined; awaiting SyncGroup
//                         (partition assignment computed in P4-3).
//   Stable            — all members heartbeating; partitions assigned.
//   Dead              — group has been deleted (terminal).
//
// Transitions:
//   Empty + join            → PreparingRebalance (generation bumped)
//   Stable + join/leave     → PreparingRebalance (generation bumped)
//   PreparingRebalance + all re-joined → CompletingRebalance (P4-3 hook)
//   CompletingRebalance + sync → Stable
//   Any state + last member leaves/expires → Empty (generation bumped)
//   Any state → Dead (on coordinator shutdown/delete)
//
// ## single-writer doWork(nowNs)
//
//   Called by one owning thread. Samples nowNs ONCE. Reaps members whose
//   `nowNs - lastHeartbeatNs > sessionTimeoutMs * 1e6`. Returns work count.
//   0 when idle. NEVER spins. NEVER allocates on the hot path.
//
// ## Generation
//
//   generationId is monotonic, bumped on EVERY membership change:
//     - member joins (new member)
//     - member leaves
//     - member expires (session timeout)
//   Exposed via generationId(). Heartbeat fences stale generations.
//
// ## Leader selection
//
//   The FIRST member to join becomes the leader. When the leader leaves or
//   expires, the alphabetically smallest remaining memberId becomes the new
//   leader (deterministic, stable).
//
// ## No hot-path allocation
//
//   Member records are stored in a pre-allocated vector. Generated memberIds
//   use a monotonic counter. No per-operation heap allocation after construction.
// ---------------------------------------------------------------------------

#include <rune/coordination/Types.h>
#include <rune/coordination/Assignor.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <atomic>
#include <map>
#include <unordered_set>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// GroupState — Kafka-faithful group lifecycle states.
// ---------------------------------------------------------------------------
enum class GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
};

// ---------------------------------------------------------------------------
// AssignorType — which assignor to use for autoAssignAndSync().
// ---------------------------------------------------------------------------
enum class AssignorType {
    Range,
    RoundRobin,
};

// ---------------------------------------------------------------------------
// SyncResult — returned by syncGroup().
// ---------------------------------------------------------------------------
struct SyncResult {
    int errorCode{0};

    static constexpr int kOk                    = 0;
    static constexpr int kErrStaleGeneration     = 1;
    static constexpr int kErrUnknownMember       = 2;
    static constexpr int kErrWrongState          = 3;
    /// Returned to a follower that calls syncGroup before the leader has
    /// submitted the assignment.  The caller must retry (Kafka: hold the
    /// SyncGroup response until leader submits).
    static constexpr int kErrRebalanceInProgress = 4;

    [[nodiscard]] bool ok() const noexcept { return errorCode == kOk; }
};

// ---------------------------------------------------------------------------
// JoinResult — returned by join().
// ---------------------------------------------------------------------------
struct JoinResult {
    std::string memberId;
    Generation  generationId{0};
    bool        isLeader{false};

    /// Error code: 0 = OK, non-zero = error.
    /// kErrDead (1): group is Dead, cannot join.
    int errorCode{0};

    static constexpr int kErrDead = 1;
};

// ---------------------------------------------------------------------------
// HeartbeatResult — returned by heartbeat().
// ---------------------------------------------------------------------------
struct HeartbeatResult {
    /// 0 = OK, non-zero = error.
    int errorCode{0};

    /// Error codes.
    static constexpr int kOk              = 0;
    static constexpr int kErrUnknownMember = 1;  ///< memberId not in group
    static constexpr int kErrStaleGeneration = 2; ///< generationId is stale
    static constexpr int kErrRebalanceInProgress = 3; ///< member must rejoin

    [[nodiscard]] bool ok()                const noexcept { return errorCode == kOk; }
    [[nodiscard]] bool unknownMember()     const noexcept { return errorCode == kErrUnknownMember; }
    [[nodiscard]] bool staleGeneration()   const noexcept { return errorCode == kErrStaleGeneration; }
    [[nodiscard]] bool rebalanceInProgress() const noexcept { return errorCode == kErrRebalanceInProgress; }
};

// ---------------------------------------------------------------------------
// MemberInfo — per-member state.
// ---------------------------------------------------------------------------
struct MemberInfo {
    std::string              memberId;
    int32_t                  sessionTimeoutMs{30'000};
    std::vector<std::string> subscribedTopics;   ///< for P4-3 assignment seam
    int64_t                  lastHeartbeatNs{0}; ///< injected clock time of last heartbeat
    bool                     isLeader{false};
    std::vector<TopicPartition> assignment;      ///< P4-3 will populate this
};

// ---------------------------------------------------------------------------
// GroupCoordinator
// ---------------------------------------------------------------------------
class GroupCoordinator {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param groupId  The group identifier.
    /// \param nowNs    Construction time (used to seed member heartbeat timestamps).
    explicit GroupCoordinator(GroupId groupId, int64_t nowNs = 0);

    // Non-copyable (holds monotonic state).
    GroupCoordinator(const GroupCoordinator&)            = delete;
    GroupCoordinator& operator=(const GroupCoordinator&) = delete;

    // -----------------------------------------------------------------------
    // Membership API
    // -----------------------------------------------------------------------

    /// Join or re-join the group.
    ///
    /// \param memberId          Empty string → assign a new generated id.
    ///                          Non-empty → re-join as existing member (no dup).
    /// \param sessionTimeoutMs  Session timeout in milliseconds.
    /// \param subscribedTopics  Topics this member subscribes to (for P4-3).
    /// \param nowNs             Injected clock (nanoseconds).
    /// \returns JoinResult with assigned memberId, current generationId, isLeader.
    ///          errorCode = kErrDead if the group is Dead.
    JoinResult join(const std::string&              memberId,
                    int32_t                          sessionTimeoutMs,
                    const std::vector<std::string>&  subscribedTopics,
                    int64_t                          nowNs);

    /// Remove a member from the group.
    ///
    /// Kafka LeaveGroup semantics: fenced by generationId.  A leave carrying a
    /// stale (or future) generationId is silently ignored — it does NOT shrink
    /// membership and does NOT trigger a rebalance.  An unknown memberId is also
    /// a no-op.
    ///
    /// \param memberId    The leaving member.
    /// \param generationId  The member's last-known generation. Must match
    ///                      current generationId_; otherwise the request is
    ///                      rejected as stale.
    /// \param nowNs       Injected clock (nanoseconds).
    void leave(const std::string& memberId, Generation generationId, int64_t nowNs);

    /// Refresh a member's last-seen timestamp.
    ///
    /// Fences stale generationId (errorCode = kErrStaleGeneration).
    /// Rejects unknown members (errorCode = kErrUnknownMember).
    /// If a rebalance is in progress (PreparingRebalance or CompletingRebalance),
    /// signals the member to rejoin (errorCode = kErrRebalanceInProgress).
    HeartbeatResult heartbeat(const std::string& memberId,
                              int32_t             generationId,
                              int64_t             nowNs);

    // -----------------------------------------------------------------------
    // P4-3: SyncGroup API
    // -----------------------------------------------------------------------

    /// Sync the group assignment (JoinGroup/SyncGroup Kafka protocol step).
    ///
    /// The LEADER calls this with the full assignment map (memberId → partitions).
    /// Followers call this with an empty map to receive their assignment.
    ///
    /// Generation-fenced: rejects if generationId != current.
    /// Wrong-state: rejects if not in CompletingRebalance.
    /// Unknown-member: rejects if memberId not in group.
    ///
    /// Stores assignments into each member's MemberInfo::assignment.
    /// Transitions CompletingRebalance→Stable once the leader has submitted.
    /// (Follower sync calls succeed but do not advance state by themselves.)
    ///
    /// \param memberId             Caller's memberId.
    /// \param generationId         Must match current generationId.
    /// \param assignmentsPerMember Leader: full map. Followers: empty map.
    /// \param nowNs                Injected clock.
    SyncResult syncGroup(const std::string& memberId,
                         Generation         generationId,
                         const Assignment&  assignmentsPerMember,
                         int64_t            nowNs);

    /// Convenience: run the chosen assignor internally, store result,
    /// and transition CompletingRebalance→Stable.
    ///
    /// Equivalent to the leader computing and calling syncGroup + all followers
    /// calling syncGroup with empty maps.
    SyncResult autoAssignAndSync(const TopicPartitionCounts& partCounts,
                                 AssignorType                assignorType,
                                 int64_t                     nowNs);

    /// Return the assigned partitions for a member (post-Stable).
    /// Returns empty vector if member is unknown or group is not Stable.
    [[nodiscard]] std::vector<TopicPartition> assignmentFor(const std::string& memberId) const;

    // -----------------------------------------------------------------------
    // P4-3: Join timeout (for doWork-driven rebalance expiry)
    // -----------------------------------------------------------------------

    /// Set the maximum time (ms) to wait for all members to re-join during
    /// PreparingRebalance. After this elapses, doWork transitions to
    /// CompletingRebalance with whoever has re-joined (others evicted).
    /// Default: 30 000 ms (matching session timeout default).
    void setRebalanceTimeoutMs(int32_t ms) noexcept { rebalanceTimeoutMs_ = ms; }

    // -----------------------------------------------------------------------
    // doWork — single-writer periodic step (no spin, no sleep)
    // -----------------------------------------------------------------------

    /// Reap members whose session timeout has elapsed.
    /// Also handles join-timeout: if in PreparingRebalance and the join timeout
    /// has elapsed (rebalanceTimeoutMs_), members who haven't re-joined are
    /// evicted and the state transitions to CompletingRebalance.
    ///
    /// \param nowNs  Injected clock (nanoseconds). Sampled ONCE per call.
    /// \returns Number of members reaped / state transitions (0 when no work done).
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] const GroupId&        groupId()      const noexcept { return groupId_; }
    [[nodiscard]] GroupState            state()        const noexcept { return state_; }
    [[nodiscard]] Generation            generationId() const noexcept { return generationId_; }
    [[nodiscard]] const std::string&    leaderId()     const noexcept { return leaderId_; }

    /// True when the group leader has submitted sync assignments in this rebalance
    /// epoch.  Used by GroupCoordinationService::doWork to detect
    /// CompletingRebalance groups that still need autoAssignAndSync (regardless
    /// of whether the state was reached via join-timeout or via all-members-rejoined).
    [[nodiscard]] bool leaderSynced() const noexcept { return leaderSynced_; }

    /// All current members (snapshot — no alloc guarantee for the vector itself,
    /// but elements are stable while no join/leave/expire occurs).
    [[nodiscard]] const std::vector<MemberInfo>& members() const noexcept { return members_; }

    /// Number of current members.
    [[nodiscard]] std::size_t memberCount() const noexcept { return members_.size(); }

    /// True if the group has no members.
    [[nodiscard]] bool isEmpty() const noexcept { return members_.empty(); }

    /// Phase 2c (spec 13.3): an EMPTY group adopts `filter` (any, including none); a non-empty one admits
    /// only byte-identical bytes.  The "unfixed" state is exactly isEmpty() - no extra flag.  Cold (JoinGroup).
    [[nodiscard]] bool admitFilter(std::span<const uint8_t> filter);
    [[nodiscard]] std::span<const uint8_t> filterBytes() const noexcept { return filter_; }

    /// E2 residual round 5 (R1): start a rebalance NOW (generation bump,
    /// PreparingRebalance) — every member must re-join and re-fetch its
    /// positions; a request at the old generation is fenced.  No-op (false) for
    /// an empty or Dead group.
    bool forceRebalance(int64_t nowNs) {
        if (members_.empty() || state_ == GroupState::Dead) return false;
        triggerRebalance(nowNs);
        return true;
    }

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Generate a new unique memberId: "<groupId>-<seq>".
    std::string generateMemberId();

    /// Find a member by id. Returns nullptr if not found.
    [[nodiscard]] MemberInfo* findMember(const std::string& memberId);
    [[nodiscard]] const MemberInfo* findMember(const std::string& memberId) const;

    /// Remove a member by id. Returns true if found and removed.
    bool removeMember(const std::string& memberId);

    /// Elect a leader: alphabetically smallest memberId (deterministic).
    /// Sets leaderId_. No-op if no members.
    void electLeader();

    /// Bump generationId and set state to PreparingRebalance.
    void triggerRebalance(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    GroupId           groupId_;
    GroupState        state_{GroupState::Empty};
    Generation        generationId_{0};
    std::string       leaderId_;               ///< current leader memberId (empty if none)

    std::vector<MemberInfo> members_;          ///< current membership
    std::vector<uint8_t>    filter_;           ///< the group's filter while it has members (stale bytes are overwritten on the next empty-group join)

    // [[rune-shared-nothing-law]] step 4: the process-global atomic counter was
    // removed — it was shared mutable state across GroupCoordinator instances that
    // may live on different threads (multiple in-process nodes).  Cross-instance
    // memberId uniqueness now derives from this coordinator's own address (distinct
    // for every live instance in the process); within an instance, memberSeq_ (a
    // plain single-owner counter) disambiguates successive joins.  No shared state.
    uint64_t memberSeq_{0};                    ///< per-instance counter (single-owner)

    // -----------------------------------------------------------------------
    // P4-3 rebalance state
    // -----------------------------------------------------------------------

    /// Members that have re-joined during the current PreparingRebalance epoch.
    /// Reset on every new rebalance (generationId bump). Used to track when
    /// ALL expected members have re-joined → transition to CompletingRebalance.
    std::unordered_set<std::string> rejoinedMembers_;

    /// Expected member count at the START of the current rebalance epoch.
    /// Set when triggerRebalance() is called. When rejoinedMembers_.size() ==
    /// expectedMemberCount_, we can transition to CompletingRebalance.
    std::size_t expectedMemberCount_{0};

    /// Whether the leader has submitted the full assignment map.  This alone
    /// drives CompletingRebalance→Stable completion (audit N1-b: the former
    /// `syncedMembers_` set was written but never read and has been removed).
    bool leaderSynced_{false};

    /// Timestamp (nowNs) when the current rebalance (PreparingRebalance) started.
    /// Used by doWork to detect join-timeout expiry.
    int64_t rebalanceStartNs_{0};

    /// Maximum ms to wait for all members to re-join. Driven by doWork.
    int32_t rebalanceTimeoutMs_{INT32_MAX}; // effectively infinite until set

    // -----------------------------------------------------------------------
    // P4-3 private helpers
    // -----------------------------------------------------------------------

    /// Called when all expected members have re-joined OR the join timeout fired.
    /// Transitions PreparingRebalance → CompletingRebalance.
    /// Members who did NOT re-join (not in rejoinedMembers_) are evicted.
    void transitionToCompletingRebalance(int64_t nowNs);

    /// Transition the group from CompletingRebalance → Stable.
    /// (Assignments are already stored by the caller before this is invoked.)
    void transitionToStable();

    /// Store an Assignment map into each member's MemberInfo::assignment.
    /// Shared between syncGroup and autoAssignAndSync to avoid duplication (CL3).
    void storeAssignments(const Assignment& a);
};

} // namespace rune::coordination
