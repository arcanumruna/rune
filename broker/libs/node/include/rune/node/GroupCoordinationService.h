#pragma once
// ---------------------------------------------------------------------------
// GroupCoordinationService.h — CG-3: wires GroupCoordinator + ReplicatedOffsetStore
// into the broker duty cycle.
//
// Namespace: rune::node
// C++23. Single-writer (doWork driven). No locks. No hot-path allocation.
//
// ## Responsibilities
//
//   - Per-group GroupCoordinator lifecycle: lazy get-or-create on first Join.
//   - doWork(nowNs): drives every GroupCoordinator::doWork; on
//     CompletingRebalance-transition → autoAssignAndSync; returns work count.
//   - Six request handlers (JoinGroup / SyncGroup / Heartbeat / LeaveGroup /
//     OffsetCommit / OffsetFetch): decode, act, encode response into a reused
//     caller-supplied buffer; return bytes written.
//   - NotCoordinator fence: when isLocalCoordinator()==false every handler
//     encodes a response with errorCode=NotCoordinator.
//
// ## Constructor deps (chosen for testability without a real bridge pointer)
//
//   - ReplicatedOffsetStore*  offsets  — non-owning; must outlive.
//   - TopicRegistry*          topics   — non-owning; for autoAssignAndSync
//                                        TopicPartitionCounts.  May be null
//                                        (autoAssignAndSync is skipped).
//   - std::function<bool()>   isLeader — leader query callback; called from
//                                        isLocalCoordinator() and every handler.
//                                        Default: always true (for tests).
//   - std::function<NodeId()> leaderId — current leader nodeId callback.
//                                        Default: returns 0.
//
// ## Zero-alloc behaviour
//
//   Handlers allocate nothing on the response-encode path; response buffers
//   are caller-supplied.  Per-handler scratch vectors (assignment results,
//   offset results) are members of GroupCoordinationService and pre-reserved
//   so steady-state is allocation-free.  New-group creation (coordinator map
//   insert + GroupCoordinator ctor) allocates once — cold path only.
//
// ## CG-4 seam
//
//   After doWork calls autoAssignAndSync, CG-4 will additionally push Assign
//   frames to all member sessions and create/revoke push subscriptions.
//   The service returns the work count; CG-4 can extend doWork or hook a
//   callback.  The interface here is exactly what the brief specifies so CG-4
//   can consume coordinator(groupId) without API changes.
// ---------------------------------------------------------------------------

#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/gateway/TopicRegistry.h>

// Protocol codecs for response encoding scratch types.
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace rune::node {

using NodeId = int32_t;   // mirrors rune::replication::NodeId
using GroupId = rune::coordination::GroupId;

class GroupCoordinationService {
public:
    // -----------------------------------------------------------------------
    // Construction
    //
    // @param offsets   ReplicatedOffsetStore — non-owning; must outlive.
    // @param topics    TopicRegistry — non-owning, may be null.
    // @param isLeader  Callable → true when this node is the cluster leader.
    // @param leaderId  Callable → current cluster leader NodeId.
    // -----------------------------------------------------------------------
    GroupCoordinationService(
        rune::coordination::ReplicatedOffsetStore* offsets,
        rune::gateway::TopicRegistry*              topics,
        std::function<bool()>                      isLeader = [] { return true; },
        std::function<NodeId()>                    leaderId = [] { return NodeId{0}; });

    // Non-copyable, non-movable (holds per-group map + pre-reserved scratch).
    GroupCoordinationService(const GroupCoordinationService&)            = delete;
    GroupCoordinationService& operator=(const GroupCoordinationService&) = delete;

    // -----------------------------------------------------------------------
    // doWork — drive every GroupCoordinator::doWork; on CompletingRebalance
    // transition → autoAssignAndSync with AssignorType::Range.
    //
    // Returns sum of work counts (0 when idle in steady state).
    // Must be called from ONE thread only (duty-cycle thread).
    // No per-cycle allocation in steady state.
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // isLocalCoordinator — true when this node is the cluster leader.
    // v1 seam: all groups are coordinated by the cluster leader.
    // Milestone-D will add per-group routing.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool isLocalCoordinator() const;

    // -----------------------------------------------------------------------
    // coordinatorFor — returns the cluster-leader nodeId for any group.
    // v1 seam (Milestone-D: will map groupId → owning node).
    // -----------------------------------------------------------------------
    [[nodiscard]] NodeId coordinatorFor(const GroupId& groupId) const;

    // -----------------------------------------------------------------------
    // coordinator — get-or-create the GroupCoordinator for a group.
    // Cold path: first call per groupId inserts into the map (one allocation).
    // Subsequent calls are O(1) map lookup, no allocation.
    // -----------------------------------------------------------------------
    rune::coordination::GroupCoordinator& coordinator(const GroupId& groupId);

    // -----------------------------------------------------------------------
    // Handlers — decode the request from buf[0..len-1], act, encode the
    // response into out[] (reused by caller), return bytes written.
    //
    // When isLocalCoordinator()==false: encode the protocol response with
    // errorCode = NotCoordinator and return that size (no coordinator action).
    //
    // buf and out MUST not overlap.
    // outCap is the capacity of out[] in bytes; handlers bound-check BEFORE
    // writing each variable-length element so out[] is never overrun.
    // Fixed-size responses (JoinGroup, Heartbeat, LeaveGroup) fit in any
    // reasonable scratch; variable-size ones (OffsetCommit, OffsetFetch)
    // cap the entry count to what fits.
    // Never throws.  Never allocates from heap in steady state.
    // -----------------------------------------------------------------------
    size_t handleJoinGroup   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleSyncGroup   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleHeartbeat   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleLeaveGroup  (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleOffsetCommit(const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);
    size_t handleOffsetFetch (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap);

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build a TopicPartitionCounts map from the TopicRegistry for autoAssignAndSync.
    /// Cold path — called only on join-timeout transition.
    rune::coordination::TopicPartitionCounts buildTopicPartitionCounts() const;

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    rune::coordination::ReplicatedOffsetStore* offsets_;    // non-owning
    rune::gateway::TopicRegistry*              topics_;     // non-owning; may be null
    std::function<bool()>                      isLeaderFn_;
    std::function<NodeId()>                    leaderIdFn_;

    /// Per-group coordinators.  Inserted lazily on first Join.
    /// std::map for deterministic ordering (not hot path).
    std::map<GroupId, rune::coordination::GroupCoordinator> coordinators_;

    // -----------------------------------------------------------------------
    // Pre-reserved scratch vectors — avoid per-handler allocation on hot path.
    // -----------------------------------------------------------------------

    /// assignment encode scratch: TopicPartition32 for SyncGroupResponse.
    /// Pre-reserved; stays at steady-state capacity.
    std::vector<rune::protocol::client::TopicPartition32> assignScratch_;

    /// OffsetCommit response results scratch.
    std::vector<rune::protocol::client::OffsetCommitResult> commitResultScratch_;

    /// OffsetFetch response results scratch.
    std::vector<rune::protocol::client::OffsetFetchResult> fetchResultScratch_;

    /// topicName ↔ topicId mapping scratch (for OffsetCommit/Fetch topic lookup).
    /// The protocol sends topicId (int32), the coordination layer uses topic name.
    /// We build the reverse map from TopicRegistry on the first lookup per handler.
    /// Re-used scratch; cleared per-call (no allocation if capacity >= nTopics).
    std::vector<std::pair<int32_t, std::string>> topicIdToNameScratch_;

    /// JoinGroup subscribedTopics scratch — reused across calls to avoid per-call
    /// vector allocation on the hot path.  Cleared + refilled on each JoinGroup.
    /// Short topic names benefit from SSO; the vector itself is amortized.
    std::vector<std::string> joinTopicsScratch_;
};

} // namespace rune::node
