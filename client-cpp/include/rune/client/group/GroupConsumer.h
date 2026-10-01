// include/rune/client/group/GroupConsumer.h
//
// CG-5 — Consumer-group API: GroupConsumer + GroupConfig.
// Namespace: rune::client
//
// Threading model (F8a — shared-nothing, no mutex, no atomics):
//   - All state-machine transitions (join/sync/stable/rejoin), Records routing,
//     heartbeat, and auto-commit duty-cycle run on the RUNNER (duty) thread,
//     which OWNS the GroupConsumerHub and every duty-side GroupConsumerImpl field.
//   - App thread only calls: onAssigned/onRevoked/onRecords (setup-phase
//     callback registration), commit(), commitSync(), close() and active().
//   - commit() / commitSync() / close() hand a HubCommand to the duty thread
//     through the agent command ring; commitSync()'s promise is owned and
//     resolved by the duty thread.  subscribeGroup() registers the impl the
//     same way (HubCommand{GroupAdd}) — the hub's consumer list is single-owner.
//   - The duty thread signals "this consumer is finished" (close(), or the
//     Client closed) by closing the impl's status ring; the handle checks it
//     and then never touches the Client again.
//
// Public API:
//   GroupConsumer& Client::subscribeGroup(groupId, topics, GroupConfig)
//   gc.onAssigned(fn)  — called on runner thread when partitions are assigned
//   gc.onRevoked(fn)   — called on runner thread when partitions are revoked
//   gc.onRecords(fn)   — called on runner thread when records arrive (assigned partitions)
//   gc.commit()        — async: a commit command for the duty thread
//   gc.commitSync()    — returns a future resolved when OffsetCommitResponse arrives
//   gc.close()         — deactivate (idempotent; the handle's owner thread)

#pragma once

#include <rune/client/subscriber/Subscription.h>  // TopicPartition, Record types
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/StatusRing.h>

#include <rune/protocol/client/OffsetMessages.h>  // OffsetCommitEntry (for commitEntriesScratch)

#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// TopicPartition — (topicId, partition) pair used in assignments.
// ---------------------------------------------------------------------------
struct TopicPartition {
    int32_t topicId   = 0;
    int32_t partition = 0;

    bool operator==(const TopicPartition& o) const noexcept {
        return topicId == o.topicId && partition == o.partition;
    }
    bool operator<(const TopicPartition& o) const noexcept {
        return topicId < o.topicId || (topicId == o.topicId && partition < o.partition);
    }
};

// ---------------------------------------------------------------------------
// RecordSet — a single pushed Records frame routed to this consumer.
// This is the view from codec::RecordsView — zero-copy spans into Aeron buffer.
// Valid ONLY during the onRecords callback.
// ---------------------------------------------------------------------------
using RecordSet = codec::RecordsView;

// ---------------------------------------------------------------------------
// OffsetReset — what to do when no committed offset exists for a partition.
// ---------------------------------------------------------------------------
enum class OffsetReset {
    Earliest,  // start from offset 0
    Latest,    // start from LEO (only new records)
};

// ---------------------------------------------------------------------------
// IsolationLevel — read-isolation level for group consumer.
//   ReadUncommitted (0): consume all records (including open transactions).
//   ReadCommitted   (1): consume only records from committed transactions
//                        (broker filters out aborted/open txn records).
// ---------------------------------------------------------------------------
enum class IsolationLevel : uint8_t {
    ReadUncommitted = 0,  // default: consume all records
    ReadCommitted   = 1,  // consume only committed transaction records
};

// ---------------------------------------------------------------------------
// GroupConfig — consumer-group options.
// ---------------------------------------------------------------------------
struct GroupConfig {
    bool         autoCommit           = true;
    int32_t      autoCommitIntervalMs = 5000;
    int32_t      sessionTimeoutMs     = 30000;
    OffsetReset  autoOffsetReset      = OffsetReset::Latest;
    IsolationLevel isolation          = IsolationLevel::ReadUncommitted;  // TX-5: read_committed support
};

// ---------------------------------------------------------------------------
// GroupConsumerState — state machine (runner thread only).
// ---------------------------------------------------------------------------
enum class GroupConsumerState : uint8_t {
    Unjoined,   // initial / after close
    Joining,    // JoinGroup sent; waiting JoinGroupResponse
    Syncing,    // SyncGroup sent; waiting SyncGroupResponse
    Stable,     // SyncGroupResponse received; consuming; heartbeat + autocommit running
    Rejoining,  // about to re-send JoinGroup (triggered by HB error / Assign / NotCoord)
};

// ---------------------------------------------------------------------------
// GroupConsumerStatus — duty thread -> app handle snapshot (fix1 I2: the
// assignment generation reaches app threads only through the status ring).
// ---------------------------------------------------------------------------
struct GroupConsumerStatus {
    int32_t generationId = 0;
    int32_t reserved     = 0;
};

// ---------------------------------------------------------------------------
// GroupConsumerImpl — internal state (heap-allocated, shared_ptr between the
// GroupConsumer handle and the GroupConsumerHub's consumer list).
//
// F8a ownership: config is written by the app thread BEFORE the GroupAdd
// command is put on the agent ring (publishing it); EVERYTHING else — callbacks
// included (fix1 I1: the setters travel as GroupSetCallback commands) — is
// owned by the duty thread.  The app thread reads only `status` (the ring's own
// memory ordering is the only synchronisation).
// ---------------------------------------------------------------------------
struct GroupConsumerImpl {
    // ---- Config (immutable after construction) --------------------------------
    std::string              groupId;
    std::vector<int32_t>     topicIds;     // resolved from topic names on creation
    GroupConfig              cfg;

    // ---- Callbacks (duty thread; installed by GroupSetCallback commands) ------
    std::function<void(std::vector<TopicPartition>)> cbAssigned;
    std::function<void(std::vector<TopicPartition>)> cbRevoked;
    std::function<void(const RecordSet&)>             cbRecords;

    // ---- State machine (duty thread only) -------------------------------------
    GroupConsumerState                  state        {GroupConsumerState::Unjoined};
    std::string                         memberId;      // assigned by broker; empty = new
    int32_t                             generationId {0};

    // Current assignment: partition → nextOffset to commit.
    // Sentinel value -1 means "position not yet seeded from OffsetFetch".
    // sendOffsetCommit MUST skip any entry whose value is kUnseededSentinel.
    static constexpr int64_t kUnseededSentinel = -1;
    std::map<TopicPartition, int64_t>   assignment;   // topicPartition → nextOffset (or sentinel)

    // ---- Timers (duty thread) -------------------------------------------------
    std::chrono::steady_clock::time_point nextHeartbeat;
    std::chrono::steady_clock::time_point nextAutoCommit;
    std::chrono::steady_clock::time_point joinDeadline;  // for rejoin backoff guard

    // ---- Send scratch buffers (duty thread, zero-alloc hot path) --------------
    std::vector<uint8_t> sendBuf;
    std::vector<rune::protocol::client::OffsetCommitEntry> commitEntriesScratch;

    // ---- Commit requests (duty thread; set by GroupCommit/GroupCommitSync) ----
    bool commitRequested {false};
    // The in-flight commitSync() promise (null = none).  Owned by the duty
    // thread — no lock.  Resolved by the OffsetCommitResponse whose
    // correlationId == commitSyncCorrId (error code checked); failed when
    // superseded, on close, on rejoin, or on session loss once sent.
    std::unique_ptr<std::promise<void>> commitSyncPromise;
    int64_t commitSyncCorrId {0};   // 0 = its OffsetCommit not sent yet

    // A generation snapshot the near-full status ring refused (fix1 I4).
    bool statusDirty {false};

    // fix3 M4: a LeaveGroup the transport refused, retried per duty cycle.
    bool    leavePending {false};
    int64_t leaveSessionId {0};

    // ---- Liveness (duty thread) -----------------------------------------------
    bool active {true};

    // ---- Duty thread -> app handle ------------------------------------------------
    detail::StatusRing<GroupConsumerStatus, 256> status;

    // ---- Construction --------------------------------------------------------
    GroupConsumerImpl() {
        sendBuf.resize(4096);  // pre-allocate scratch (avoids per-send allocation)
    }

    // Non-copyable / non-movable (shared_ptr ownership; the ring is pinned).
    GroupConsumerImpl(const GroupConsumerImpl&) = delete;
    GroupConsumerImpl& operator=(const GroupConsumerImpl&) = delete;
    GroupConsumerImpl(GroupConsumerImpl&&) = delete;
    GroupConsumerImpl& operator=(GroupConsumerImpl&&) = delete;
};

// ---------------------------------------------------------------------------
// GroupConsumer — user-facing handle returned by Client::subscribeGroup().
//
// Threading:
//   - Callback setters (onAssigned/onRevoked/onRecords) are SETUP PHASE: call
//     them before connect().  Each travels to the duty thread as a command (the
//     callback is installed there, in ring order — before the connect), so the
//     duty-owned std::functions are never written from an app thread.
//   - The handle is OWNED by one app thread at a time (move-only, like
//     Subscription): its app-side methods must not be called concurrently.
//   - Calls from INSIDE a callback (duty thread, Thread mode) are allowed:
//     commit() / close() / active() / generation() act directly on duty-owned
//     state.  commitSync() there throws std::logic_error (its future could
//     never resolve while the callback blocks the duty thread) — use commit().
//   - finished(): the duty side is done with this consumer — no further
//     callback will run.  A finished handle never touches the Client again, so
//     a handle may outlive its Client (not be used CONCURRENTLY with its
//     destruction); commitSync() then returns an already-failed future.
// ---------------------------------------------------------------------------
class GroupConsumer {
public:
    using AssignmentCallback = std::function<void(std::vector<TopicPartition>)>;
    using RecordsCallback    = std::function<void(const RecordSet&)>;

    /// Constructed by Client::subscribeGroup().  `sink`/`sinkCtx` deliver the
    /// handle's commands to the duty thread.  `dutyThread`: the Thread-mode
    /// runner (calls from it take the duty path); default id disables that.
    GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl,
                  detail::HubCommandSink             sink,
                  void*                              sinkCtx,
                  std::thread::id                    dutyThread = {}) noexcept;

    ~GroupConsumer();

    // Non-copyable, movable.
    GroupConsumer(const GroupConsumer&) = delete;
    GroupConsumer& operator=(const GroupConsumer&) = delete;
    GroupConsumer(GroupConsumer&&) noexcept;
    GroupConsumer& operator=(GroupConsumer&&) noexcept;

    // ---- Callback registration (SETUP PHASE: call before connect()) ----------

    /// Called on the runner thread when the broker assigns partitions.
    void onAssigned(AssignmentCallback cb);

    /// Called on the runner thread when the broker revokes partitions.
    void onRevoked(AssignmentCallback cb);

    /// Called on the runner thread when records arrive for assigned partitions.
    void onRecords(RecordsCallback cb);

    // ---- Commit API ----------------------------------------------------------

    /// Ask the duty thread to commit the current per-partition positions (sent
    /// on a following duty cycle once Stable).  At-least-once: only positions
    /// of records already delivered via onRecords are committed.  No-op once
    /// finished.  App thread: throws only if the command ring stays full past
    /// the bounded wait.
    void commit();

    /// Commit and return a future that resolves when the broker acknowledges
    /// THIS commit (OffsetCommitResponse matched by correlationId, error codes
    /// checked — a failed commit throws from get()), or resolves at once when
    /// there is nothing to commit.  It fails when superseded by a newer
    /// commitSync(), on close(), on a group rejoin, as soon as the session to
    /// the broker goes DOWN (the session machine's Connected -> not-Connected
    /// transition — no timeout of its own) or a new session replaces the one
    /// its commit was sent on, or when the Client closes.  An OffsetCommit the
    /// transport refuses is retried every duty cycle (never dropped).
    /// Waiting: Thread mode — get() from an app thread.  Invoker mode — keep
    /// calling Client::doWork() until it is ready.  NEVER from inside a duty-
    /// cycle callback: commitSync() throws std::logic_error there.
    std::future<void> commitSync();

    /// Deactivate this consumer: no more dispatch occurs, a pending commitSync
    /// fails.  Idempotent.  Delivery is guaranteed (a full command ring is
    /// waited out on duty-thread progress, never a timeout); never throws.
    void close();

    /// True until close(), or until the duty side finished the consumer.
    [[nodiscard]] bool active() const noexcept;

    /// True once the duty side finished this consumer: no further callback
    /// (onAssigned / onRevoked / onRecords) will run.
    [[nodiscard]] bool finished() const noexcept;

    /// Current assignment generation (JoinGroupResponse / Assign).  Inside a
    /// callback: the duty thread's live value.  On the owning app thread: the
    /// latest generation the duty thread published (eventually consistent).
    [[nodiscard]] int32_t generation() const noexcept;

private:
    [[nodiscard]] bool onDutyThread_() const noexcept {
        return dutyThread_ != std::thread::id{} && std::this_thread::get_id() == dutyThread_;
    }
    /// Submit `cmd` unless the consumer is already finished.  Returns the
    /// sink's verdict (Closed when finished / no sink).
    detail::HubSubmit submit_(const detail::HubCommand& cmd);
    template <class Fn>
    void setCallback_(int which, Fn cb);

    std::shared_ptr<GroupConsumerImpl> impl_;
    detail::HubCommandSink             sink_    = nullptr;
    void*                              sinkCtx_ = nullptr;
    std::thread::id                    dutyThread_{};
    bool moved_  = false;
    bool closed_ = false;   // handle-local (app-owned): close() was called
    mutable GroupConsumerStatus cache_{};  // app-owned: last drained status snapshot
};

} // namespace rune::client
