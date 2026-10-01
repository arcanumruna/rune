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
//     through the agent command ring (or apply it directly on the duty
//     thread); commitSync(target, token) completes exactly once (F8c C4: a
//     completion token, no future).  subscribeGroup() registers the impl the
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
//   gc.commitSync(target, token) — one completion when OffsetCommitResponse arrives
//   gc.commitSyncAndWait(timeout) — cold-path blocking form (throws on failure)
//   gc.close()         — deactivate (idempotent; the handle's owner thread)

#pragma once

#include <rune/client/subscriber/Subscription.h>  // TopicPartition, Record types
#include <rune/client/Completion.h>
#include <rune/client/Completions.h>  // CompletionTarget
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/detail/StatusRing.h>

#include <rune/protocol/client/OffsetMessages.h>  // OffsetCommitEntry (for commitEntriesScratch)

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// SessionLostError — F8a fix4 M6: commitSyncAndWait() failed because the
// session to the broker was lost (OpStatus::SessionLost: it went down, or a
// new session / connect episode replaced the one the commit was sent on) or
// was down when the commitSync was applied.  The commit's fate is UNKNOWN (it
// may or may not have been applied).  RETRIABLE: wait for the next onAssigned
// (the consumer rejoins after the reconnect and re-seeds its positions), then
// commit again.  Every other failure (closed, superseded, rejoin, broker error
// code, timeout) is a plain std::runtime_error.
// ---------------------------------------------------------------------------
class SessionLostError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

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
    /// F8c C4 (spec Q6): a commitSync that has not completed this long after
    /// it was applied (duty clock) completes OpStatus::Timeout.
    int32_t      commitSyncTimeoutMs  = 30000;
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
    // F8c C4: the pending commitSync (at most one; duty thread only — no
    // lock, no promise).  Completed exactly once: Ok by the OffsetCommitResponse
    // whose correlationId == commitSyncCorrId (BrokerError on an error code),
    // Superseded / Rejoined / SessionLost / Closed / Timeout otherwise.
    struct PendingCommitSync {
        detail::OpTarget target{};
        Token            token = 0;
        int64_t          deadlineNs = 0;  // duty clock (GroupConfig::commitSyncTimeoutMs)
    };
    bool              hasCommitSync {false};
    PendingCommitSync commitSync{};
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
//   - Calls from INSIDE a callback (duty thread) are allowed: commit() /
//     close() / active() / generation() / commitSync(target, token) act
//     directly on duty-owned state (F8c C4: the token form is just a command
//     applied there).  Only commitSyncAndWait() throws std::logic_error there
//     (it would wait for the duty thread it is blocking).
//   - finished(): the duty side is done with this consumer — no further
//     callback will run.  A finished handle never touches the Client again, so
//     a handle may outlive its Client (not be used CONCURRENTLY with its
//     destruction); commitSync() then returns OpResult::Closed.
//   - HANDLE LIFETIME (fix4 M6): the handle must NOT be destroyed, moved from
//     or moved into while ANY thread may still be calling it — its owner app
//     thread, or a runner callback (onAssigned / onRevoked / onRecords calling
//     commit() / close() on it).  The direct (duty-thread) path acts on the
//     impl through the caller's handle for the whole call; a concurrent
//     destroy/move is a data race on the handle itself.
// ---------------------------------------------------------------------------
class GroupConsumer {
public:
    using AssignmentCallback = std::function<void(std::vector<TopicPartition>)>;
    using RecordsCallback    = std::function<void(const RecordSet&)>;

    /// Constructed by Client::subscribeGroup().  `sink`/`sinkCtx` deliver the
    /// handle's commands to the duty thread.  `dutyThread`: the Thread-mode
    /// runner (calls from it take the duty path); default id disables that.
    /// `opSink` issues token ops (commitSync), `waitSink` backs
    /// commitSyncAndWait() (both nullptr: commitSync is rejected Closed).
    GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl,
                  detail::HubCommandSink             sink,
                  void*                              sinkCtx,
                  std::thread::id                    dutyThread = {},
                  detail::TargetedOpSink             opSink     = nullptr,
                  detail::AndWaitSink                waitSink   = nullptr) noexcept;

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

    /// F8c C4: commit the current positions; exactly ONE completion
    /// (op = OpKind::CommitSync) follows an Accepted return:
    ///   Ok           — the broker acknowledged THIS commit (OffsetCommitResponse
    ///                  matched by correlationId), or there was nothing to commit;
    ///   BrokerError  — the response carried an error code (errorCode);
    ///   Superseded   — a newer commitSync replaced it;
    ///   Rejoined     — the group rebalanced (rejoin) before the answer;
    ///   SessionLost  — the session went down / was replaced after it was
    ///                  applied, or was down when it was applied (fate unknown,
    ///                  RETRIABLE after the next onAssigned — fix4 M6);
    ///   NotConnected — the transport can never carry the OffsetCommit (Fatal);
    ///   Timeout      — GroupConfig::commitSyncTimeoutMs of duty time passed;
    ///   Closed       — close() of this consumer, or Client close.
    /// Synchronous rejections (no completion): Closed (finished / Client
    /// closed), InvalidArgument (token > kMaxToken, no target, a Completions
    /// of another Client), WrongThread (Invoker off the owner, a Completions
    /// off its owner), QueueFull, BackPressured (command ring full).
    /// Callable from inside a duty-cycle callback (applied directly).
    OpResult commitSync(CompletionTarget target, Token token);

    /// Cold-path blocking form (spec §2.3): issue + wait (Invoker owner pumps
    /// doWork(); a Thread-mode app thread idle-polls).  Throws
    /// SessionLostError on SessionLost, std::runtime_error on any other
    /// non-Ok outcome or wait timeout, std::logic_error from a duty-cycle
    /// callback.
    void commitSyncAndWait(std::chrono::nanoseconds timeout = std::chrono::seconds(60));

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

    static OpResult issueCommitSync_(void* self, const CompletionTarget& target, Token token);

    std::shared_ptr<GroupConsumerImpl> impl_;
    detail::HubCommandSink             sink_    = nullptr;
    void*                              sinkCtx_ = nullptr;
    detail::TargetedOpSink             opSink_   = nullptr;
    detail::AndWaitSink                waitSink_ = nullptr;
    std::thread::id                    dutyThread_{};
    bool moved_  = false;
    bool closed_ = false;   // handle-local (app-owned): close() was called
    mutable GroupConsumerStatus cache_{};  // app-owned: last drained status snapshot
};

} // namespace rune::client
