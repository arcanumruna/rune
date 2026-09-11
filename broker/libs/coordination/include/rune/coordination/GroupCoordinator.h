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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <atomic>

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
    /// If the member is unknown, this is a no-op.
    /// If the member is the last → group transitions to Empty (generation bumped).
    /// Otherwise → PreparingRebalance (generation bumped); new leader elected if needed.
    void leave(const std::string& memberId, int64_t nowNs);

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
    // doWork — single-writer periodic step (no spin, no sleep)
    // -----------------------------------------------------------------------

    /// Reap members whose session timeout has elapsed.
    ///
    /// \param nowNs  Injected clock (nanoseconds). Sampled ONCE per call.
    /// \returns Number of members reaped (0 when no work done).
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] const GroupId&        groupId()      const noexcept { return groupId_; }
    [[nodiscard]] GroupState            state()        const noexcept { return state_; }
    [[nodiscard]] Generation            generationId() const noexcept { return generationId_; }
    [[nodiscard]] const std::string&    leaderId()     const noexcept { return leaderId_; }

    /// All current members (snapshot — no alloc guarantee for the vector itself,
    /// but elements are stable while no join/leave/expire occurs).
    [[nodiscard]] const std::vector<MemberInfo>& members() const noexcept { return members_; }

    /// Number of current members.
    [[nodiscard]] std::size_t memberCount() const noexcept { return members_.size(); }

    /// True if the group has no members.
    [[nodiscard]] bool isEmpty() const noexcept { return members_.empty(); }

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

    static std::atomic<uint64_t> s_memberSeq; ///< global monotonic counter for memberId generation
    uint64_t memberSeq_{0};                    ///< per-instance counter (primary)
};

} // namespace rune::coordination
