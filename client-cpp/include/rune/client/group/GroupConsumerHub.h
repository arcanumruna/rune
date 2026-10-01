// include/rune/client/group/GroupConsumerHub.h
//
// CG-5 — GroupConsumerHub: manages all registered GroupConsumerImpl instances.
//
// F8a — shared-nothing: the hub (its consumer list and every duty-side
// GroupConsumerImpl field) is OWNED by the duty thread.  No COW snapshot, no
// mutex, no atomics: app threads reach it only through HubCommands drained from
// the agent command ring (applyCommand), and learn "finished" only through each
// impl's status ring (closed when the consumer is closed or the Client closes).
//
// Duty-cycle entry points (duty thread):
//   applyCommand(cmd)               — GroupAdd / GroupCommit / GroupCommitSync / GroupClose
//   doWork(nowNs, sessionId, sendFn) — heartbeat + autocommit + rejoin timers
//   onRecords(rv)                   — route Records frame to assigned consumer
//   onJoinGroupResponse(...)        — state machine: Joining → Syncing
//   onSyncGroupResponse(...)        — state machine: Syncing → Stable
//   onHeartbeatResponse(...)        — state machine: Stable → Rejoining (on error)
//   onOffsetCommitResponse(...)     — resolve commitSync promise
//   onAssign(...)                   — inline assignment update (Assign frame 44)
//   closeAll()                      — deactivate all consumers on Client::close()

#pragma once

#include <rune/client/group/GroupConsumer.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>

#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace rune::client::group {

// The duty-thread-owned consumer list.
using ImplList = std::vector<std::shared_ptr<GroupConsumerImpl>>;

// SendFn: (ingressStreamId, buf, len) -> bool.
// ingressStreamId=0 means "use the default ingress stream".
// Matches ClientAgent::SendFn so Task 1.3 can route group frames to the
// coordinator session without an adapter at every call site.
using SendFn = std::function<bool(int32_t, const uint8_t*, std::size_t)>;

class GroupConsumerHub {
public:
    GroupConsumerHub() = default;

    // Non-copyable / non-movable.
    GroupConsumerHub(const GroupConsumerHub&) = delete;
    GroupConsumerHub& operator=(const GroupConsumerHub&) = delete;
    GroupConsumerHub(GroupConsumerHub&&) = delete;
    GroupConsumerHub& operator=(GroupConsumerHub&&) = delete;

    // ---- Registration + commands (duty thread) --------------------------------

    /// Register a GroupConsumerImpl (duty thread; Client reaches it via
    /// HubCommand{GroupAdd}).  Cold path.
    void add(std::shared_ptr<GroupConsumerImpl> impl);

    /// Apply one group HubCommand drained from the agent ring.  Non-group ops
    /// are ignored.  A command naming an impl this hub does not hold is
    /// ignored (a commitSync promise it carries is failed, never dropped).
    void applyCommand(const detail::HubCommand& cmd);

    // ---- Agent-thread duty cycle --------------------------------------------

    /// Drive heartbeat, autocommit, and rejoin timers for all consumers.
    /// Also drains any pending commit() requests.
    /// Returns work count (frames sent).
    int doWork(int64_t nowNs,
               int64_t sessionId,
               const SendFn& sendFn);

    // ---- Egress frame routing (agent thread) --------------------------------

    /// Route a Records(20) frame: if (topicId, partition) is in any consumer's
    /// current assignment, deliver to that consumer and advance its position.
    /// Also tries the existing subscriberHub path first (via the caller).
    void onRecords(const codec::RecordsView& rv);

    /// Called when JoinGroupResponse (tid 33) arrives.
    void onJoinGroupResponse(const uint8_t* buf, std::size_t len,
                             int64_t sessionId, const SendFn& sendFn);

    /// Called when SyncGroupResponse (tid 35) arrives.
    void onSyncGroupResponse(const uint8_t* buf, std::size_t len,
                             int64_t sessionId, const SendFn& sendFn);

    /// Called when HeartbeatResponse (tid 37) arrives.
    void onHeartbeatResponse(const uint8_t* buf, std::size_t len,
                             int64_t sessionId, const SendFn& sendFn);

    /// Called when OffsetCommitResponse (tid 41) arrives.
    void onOffsetCommitResponse(const uint8_t* buf, std::size_t len);

    /// Called when OffsetFetchResponse (tid 43) arrives.
    void onOffsetFetchResponse(const uint8_t* buf, std::size_t len);

    /// Called when Assign (tid 44) arrives for a specific groupId member.
    void onAssign(const uint8_t* buf, std::size_t len,
                  int64_t sessionId, const SendFn& sendFn);

    // ---- Called on connect (runner thread) ----------------------------------

    /// Trigger all Unjoined / Rejoining consumers to send JoinGroup now
    /// (called when a new ConnectResponse{OK} arrives).
    void onConnected(int64_t sessionId, const SendFn& sendFn);

    // ---- Shutdown -----------------------------------------------------------

    /// Deactivate all consumers (send LeaveGroup where possible + fire onRevoked).
    void closeAll(int64_t sessionId, const SendFn& sendFn);

    /// True if there are no registered consumers (duty thread).
    [[nodiscard]] bool empty() const noexcept { return impls_.empty(); }

    /// fix1 C1: onAssigned/onRevoked/onRecords run inside the noexcept
    /// ClientAgent::doWork chain (or, for closeAll's final onRevoked, inside
    /// Client::closeImpl_); a throwing callback is caught, counted here, and
    /// logged instead of terminating the process. PLAIN counter (fix2
    /// minor-5 — shared-nothing rule, no atomics): written only on the
    /// runner/duty-cycle thread, or from closeAll()'s thread strictly after
    /// the runner is joined. See Client::testCallbackExceptionCount()'s doc
    /// for the exact read-side contract this implies.
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept {
        return callbackExceptions_;
    }

private:
    // ---- Registered consumers (duty-thread owned; never shrinks) -------------
    ImplList impls_;

    // fix1 C1 / fix2 minor-5: plain counter (see testCallbackExceptionCount()).
    uint64_t callbackExceptions_ = 0;

    // ---- Internal helpers (runner thread only) ------------------------------

    /// Send JoinGroup for impl; transitions to Joining.
    static void sendJoinGroup(GroupConsumerImpl& impl,
                              int64_t sessionId,
                              const SendFn& sendFn);

    /// Send SyncGroup for impl; transitions to Syncing.
    static void sendSyncGroup(GroupConsumerImpl& impl,
                              int64_t sessionId,
                              const SendFn& sendFn);

    /// Send Heartbeat for impl.
    static void sendHeartbeat(GroupConsumerImpl& impl,
                              int64_t sessionId,
                              const SendFn& sendFn);

    /// Send OffsetCommit for impl's current positions (skips unseeded sentinel entries).
    static void sendOffsetCommit(GroupConsumerImpl& impl,
                                 int64_t sessionId,
                                 const SendFn& sendFn);

    /// Send OffsetFetch for all currently-assigned partitions to seed positions
    /// from the broker's committed offsets.  Called after entering Stable.
    static void sendOffsetFetch(GroupConsumerImpl& impl,
                                int64_t sessionId,
                                const SendFn& sendFn);

    /// TX-5 (fix-round 1, CRITICAL 2): Send a Subscribe frame for each assigned
    /// partition, carrying cfg_.isolation so the broker applies the correct
    /// read isolation level when delivering Records to this group consumer.
    /// Called after entering Stable (SyncGroupResponse) and after Assign.
    static void sendSubscribeForPartitions(GroupConsumerImpl& impl,
                                           int64_t sessionId,
                                           const SendFn& sendFn);

    /// The registered impl whose address is `handle`, or nullptr.
    GroupConsumerImpl* find_(uint64_t handle) const noexcept;

    /// Deactivate impl: inactive, fail a pending commitSync with `why`, close
    /// its status ring (the handle then never touches the Client again).
    static void finish_(GroupConsumerImpl& impl, const char* why);

    /// Apply a new assignment to impl: fire revoked+assigned callbacks. Not
    /// static (fix1 C1): catches/counts a throwing callback via callbackExceptions_.
    void applyAssignment(GroupConsumerImpl& impl,
                         std::vector<TopicPartition> newAssignment);
};

} // namespace rune::client::group
