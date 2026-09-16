// include/rune/client/group/GroupConsumerHub.h
//
// CG-5 — GroupConsumerHub: manages all registered GroupConsumerImpl instances.
//
// Mirrors ProducerHub's atomic COW snapshot pattern (DR-BUG-1 discipline):
//   - add() (cold path, app thread): allocates a new ImplList, publishes it
//     with memory_order_release into snapshot_, retains old pointer in owned_.
//   - Hot-path methods (runner thread): one snapshot_.load(acquire) → iterate.
//     No locks, no refcount churn, no per-cycle heap allocation.
//
// Duty-cycle entry points (runner thread):
//   doWork(nowNs, sessionId, sendFn) — heartbeat + autocommit + rejoin timers
//   onRecords(rv)                   — route Records frame to assigned consumer
//   onJoinGroupResponse(...)        — state machine: Joining → Syncing
//   onSyncGroupResponse(...)        — state machine: Syncing → Stable
//   onHeartbeatResponse(...)        — state machine: Stable → Rejoining (on error)
//   onOffsetCommitResponse(...)     — resolve commitSync promise
//   onAssign(...)                   — inline assignment update (Assign frame 44)
//   closeAll()                      — deactivate all consumers on Client::close()
//
// Threading:
//   All hot-path methods are called exclusively on the runner/agent thread.
//   add() acquires addMtx_ (cold path) and is the only app-thread writer.

#pragma once

#include <rune/client/group/GroupConsumer.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace rune::client::group {

// Alias for the immutable consumer list (same COW pattern as ProducerHub).
using ImplList = std::vector<std::shared_ptr<GroupConsumerImpl>>;

// SendFn: (buf, len) -> bool, same as agent_.submitSend.
using SendFn = std::function<bool(const uint8_t*, std::size_t)>;

class GroupConsumerHub {
public:
    GroupConsumerHub() {
        auto* empty = new ImplList{};
        snapshot_.store(empty, std::memory_order_relaxed);
        owned_.push_back(empty);
    }

    ~GroupConsumerHub() {
        for (auto* p : owned_) delete p;
        owned_.clear();
    }

    // Non-copyable / non-movable.
    GroupConsumerHub(const GroupConsumerHub&) = delete;
    GroupConsumerHub& operator=(const GroupConsumerHub&) = delete;
    GroupConsumerHub(GroupConsumerHub&&) = delete;
    GroupConsumerHub& operator=(GroupConsumerHub&&) = delete;

    // ---- Registration (app thread, cold path) --------------------------------

    /// Register a new GroupConsumerImpl.  Thread-safe (under addMtx_).
    void add(std::shared_ptr<GroupConsumerImpl> impl);

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

    /// True if there are any registered consumers.
    [[nodiscard]] bool empty() const noexcept {
        return snapshot_.load(std::memory_order_acquire)->empty();
    }

private:
    // ---- Wait-free snapshot pointer -----------------------------------------
    std::atomic<const ImplList*> snapshot_{nullptr};

    // ---- Retire list (cold path, under addMtx_) ----------------------------
    std::vector<const ImplList*> owned_;
    std::mutex addMtx_;

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

    /// Send OffsetCommit for impl's current positions.
    static void sendOffsetCommit(GroupConsumerImpl& impl,
                                 int64_t sessionId,
                                 const SendFn& sendFn);

    /// Apply a new assignment to impl: fire revoked+assigned callbacks.
    static void applyAssignment(GroupConsumerImpl& impl,
                                std::vector<TopicPartition> newAssignment);
};

} // namespace rune::client::group
