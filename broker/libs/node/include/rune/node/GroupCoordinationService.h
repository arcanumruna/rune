#pragma once
// ---------------------------------------------------------------------------
// GroupCoordinationService.h — CG-3/CG-4: wires GroupCoordinator +
// ReplicatedOffsetStore into the broker duty cycle; translates group
// assignments into HW-bounded push subscriptions (CG-4).
//
// Namespace: rune::node
// C++23. Single-writer (doWork driven). No locks. No hot-path allocation.
//
// ## Responsibilities
//
//   - Per-group GroupCoordinator lifecycle: lazy get-or-create on first Join.
//   - doWork(nowNs): drives every GroupCoordinator::doWork; on
//     CompletingRebalance-transition → autoAssignAndSync; returns work count.
//   - CG-4: on Stable: for each member, seed push subscriptions (one per
//     assigned partition, resumed from committed offset or auto.offset.reset)
//     and send Assign control frames.  On PreparingRebalance: eagerly revoke
//     all group push subscriptions and send empty Assign (revoke) frames.
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
//   CG-4: Assign encoding uses assignFrameScratch_ (pre-allocated 68 KiB
//   member buffer); no per-rebalance heap allocation.
// ---------------------------------------------------------------------------

#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/gateway/IGroupRequestHandler.h>

// Protocol codecs for response encoding scratch types.
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declaration — ClientGateway lives in rune_gateway; node→gateway direction is fine.
namespace rune::gateway { class ClientGateway; }

namespace rune::node {

using NodeId = int32_t;   // mirrors rune::replication::NodeId
using GroupId = rune::coordination::GroupId;

// CG-4 fix: GroupCoordinationService implements IGroupRequestHandler so that
// ClientGateway can dispatch group messages through the interface without
// introducing a rune_gateway→rune_node symbol dependency.
class GroupCoordinationService : public rune::gateway::IGroupRequestHandler {
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
    // setGateway — wire in the ClientGateway for CG-4 push-subscription
    // translation.  Must be called before doWork() for group push delivery.
    // May be null (doWork still drives rebalance but sends no Assign frames
    // and creates no push subscriptions — useful for tests that exercise
    // coordinator state without a real gateway).
    // Non-owning; lifetime must exceed the service.
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setGateway(rune::gateway::ClientGateway* gateway) noexcept {
        gateway_ = gateway;
    }

    // -----------------------------------------------------------------------
    // isLocalCoordinator — true when this node is the cluster leader.
    // v1 seam: all groups are coordinated by the cluster leader.
    // Milestone-D will add per-group routing.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool isLocalCoordinator() const;

    // Task 11 fix 2 (B1): this node's leadership for the current runtime duty
    // cycle, pushed as a VALUE by NodeRuntimeAgent (same thread) right after
    // consensus runs.  Once pushed, isLocalCoordinator() never calls the
    // isLeader std::function (per-cycle, worker 0's pass under fused placement).
    // Unpushed (-1): the callable is used (service-only tests).
    void setLocalLeadership(bool leader) noexcept { localLeader_ = leader ? 1 : 0; }

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
    size_t handleJoinGroup   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleSyncGroup   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleHeartbeat   (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleLeaveGroup  (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleOffsetCommit(const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleOffsetFetch (const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap) override;
    /// No-hang fix round 4 (item 8): every member tracked on `sessionId` leaves
    /// its group (subscriptions revoked, session mapping dropped).  Cold.
    void evictSessionMembers(int64_t sessionId, int64_t nowNs) override;
    /// No-hang fix round 5 (N23): only the request's (group, member) on `sessionId`
    /// (memberId empty: this session's members of that group).  Cold.
    void evictMember(std::string_view groupId, std::string_view memberId, int64_t sessionId,
                     int64_t nowNs) override;

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build a TopicPartitionCounts map from the TopicRegistry for autoAssignAndSync.
    /// Cold path — called only on join-timeout transition.
    rune::coordination::TopicPartitionCounts buildTopicPartitionCounts() const;

    // CG-4: called from doWork on the PreparingRebalance entry for a group.
    // Revokes all group push subscriptions and sends empty Assign (revoke) to
    // every member session.
    void onEnterPreparingRebalance(const GroupId& groupId,
                                   rune::coordination::GroupCoordinator& coord);

    // CG-4: called from doWork after autoAssignAndSync succeeds (Stable).
    // Creates push subscriptions (resumed from committed offset) and sends
    // Assign frames with each member's new partition set.
    void onReachedStable(const GroupId& groupId,
                         rune::coordination::GroupCoordinator& coord,
                         int64_t nowNs);

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    rune::coordination::ReplicatedOffsetStore* offsets_;    // non-owning
    rune::gateway::TopicRegistry*              topics_;     // non-owning; may be null
    std::function<bool()>                      isLeaderFn_;
    int8_t                                     localLeader_ = -1;   // see setLocalLeadership()
    std::function<NodeId()>                    leaderIdFn_;

    // CG-6 failover: detect Follower→Leader transition so we can rebuild the
    // in-memory offset index from the replicated Log once we become coordinator.
    // Initialised to false; set to true after the first successful rebuild.
    // Cleared back to false when we detect we are no longer the leader (so a
    // subsequent re-election triggers another rebuild).
    bool wasLeader_ = false;

    // CP-5.2 cold-restart fix: the offset-index rebuild is a NO-OP while the
    // __consumer_offsets partition's high-watermark is still 0 (rebuildFromLog
    // bails on hw<=0).  On a COLD cluster restart a node becomes coordinator with
    // HW temporarily gated at 0 (ISR not yet re-expanded to minIsr / followers not
    // yet caught up), so a one-shot rebuild on the leader transition recovers
    // NOTHING and consumer groups resume from offset 0.  We therefore RETRY the
    // rebuild every coordinator doWork until it actually runs with hw>0
    // (offsetsRebuilt_ latches true only then).  This retry window is BEFORE any
    // group resumes committing, so re-running rebuild (clear+repopulate from the
    // log) cannot clobber a live in-memory commit.
    bool offsetsRebuilt_ = false;
    /// (leadershipGeneration << 32) ^ logGeneration of the offsets replica the
    /// index was rebuilt against (see doWork).
    uint64_t offsetsGen_ = 0;

    // CG-4: ClientGateway — non-owning; may be null (tests without push subs).
    rune::gateway::ClientGateway*              gateway_ = nullptr;

    /// Per-group coordinators.  Inserted lazily on first Join.
    /// std::map with std::less<> (transparent comparator) so hot-path find()
    /// can be called with std::string_view without constructing a std::string.
    std::map<GroupId, rune::coordination::GroupCoordinator, std::less<>> coordinators_;
    /// LOW-3 (CG round 4): JoinGroup refusals per old-client session (log rate-limit; cold).
    std::map<int64_t, uint64_t> oldClientRefusals_;

    // CG-4: member → sessionId tracking.
    // Keyed by (groupId + ":" + memberId) for global uniqueness.
    // Populated on JoinGroup; cleared on Leave/expiry revoke.
    // Used by doWork to know which egress session to send Assign frames to and
    // which session to create push subscriptions for.
    // Single-writer (duty-cycle thread); no locks needed.
    // std::map with std::less<> enables transparent find() with string_view.
    std::map<std::string, int64_t, std::less<>> memberSessionMap_;

    // CG-7 zero-alloc: per-handler scratch strings.
    // Assigned via assign(data, size) each call — no heap alloc when
    // capacity >= size (capacity grows on first long-string call, then stable).
    // Never holds state across call boundaries — purely per-handler scratch.
    std::string groupIdScratch_;    ///< groupId span → reusable std::string
    std::string memberIdScratch_;   ///< memberId span → reusable std::string
    std::string memberKeyScratch_;  ///< "groupId:memberId" composite lookup key
    std::vector<std::string> evictScratch_;   ///< fix round 4: members to evict (cold path)

    // CG-4: scratch buffer for Assign frame encoding.
    // 68 KiB bound: header 24 + generationId 4 + count 2 = 30 B fixed; each
    // TopicPartition32 entry is 8 B (topicId 4 + partition 4).
    // Max entries = (68*1024 - 30) / 8 = 8704 partitions per member in one frame.
    // This matches the encodedSizeFor() guard in onReachedStable; real clusters
    // have far fewer partitions per member.  Frame is capped, not overrun.
    // Pre-allocated; no per-rebalance heap allocation.
    static constexpr std::size_t kAssignFrameScratchSize = 68 * 1024;
    std::array<uint8_t, kAssignFrameScratchSize> assignFrameScratch_{};

    // CG-4: assignment TopicPartition32 encode scratch (for Assign frame encoding).
    // Pre-reserved; stays at steady-state capacity after first rebalance.
    std::vector<rune::protocol::client::TopicPartition32> assignFrameTpScratch_;

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
