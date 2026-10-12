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
/// Why the duty side finished a GroupConsumer on its own (GroupConsumer::failReason()).
///
/// Compatibility rule (client / broker version 3, see rune-protocol
/// Protocol.h kClientVersionExplicitGroupSubscriptions / kBrokerVersion...):
/// a group member subscribes to its partitions' leaders itself and needs a
/// broker that does not auto-subscribe members and stamps Records with the
/// creating Subscribe's correlationId.  This client runs a GroupConsumer only
/// against a seed / coordinator broker advertising ConnectResponse.brokerVersion
/// >= 3; a partition whose LEADER node is below 3 (a rolling upgrade) is skipped
/// (logged once per that node's connect episode) until that node is upgraded
/// and reconnects; a broker coordinator refuses
/// JoinGroup from a client below 3.  The whole cluster must be >= 3 for every
/// partition to be consumed.
enum class GroupFailReason : int32_t {
    None = 0,
    /// The broker does not advertise explicit group subscriptions (brokerVersion < 3).
    /// Rolling upgrades must upgrade the BOOTSTRAP (seed) nodes the clients
    /// connect to first (they are the group coordinator's session): a v3 client
    /// whose seed is still pre-v3 finishes its GroupConsumer with this reason;
    /// partitions whose leader is a not-yet-upgraded node are only skipped
    /// (logged once) until that node is upgraded and reconnects.
    BrokerTooOld = 1,
    /// The broker coordinator refused this consumer's request non-retriably
    /// (ProtocolVersion, or InvalidRequest: the broker could not decode it).
    /// Recreating the consumer will be refused again.
    RefusedByBroker = 2,
    /// No-hang fix round 3: a retriable failure persisted past its retry budget
    /// (Internal / unclassified coordinator answers, OffsetFetch per-entry
    /// errors, or — fix round 4 — an OffsetFetch answered NotCoordinator on
    /// every re-join).  A transient cause: the application may recreate the consumer.
    RetryBudgetExhausted = 3,
};

struct GroupConsumerStatus {
    int32_t generationId = 0;
    int32_t reserved     = 0;  // final snapshot: a GroupFailReason
};

// ---------------------------------------------------------------------------
// GroupConsumerImpl — internal state (heap-allocated).  F8c C7 ownership (spec
// §3.3, no shared_ptr): owned jointly by the GroupConsumer handle and the duty
// side (the GroupConsumerHub, from the GroupAdd's adoption until the handle's
// HandleRelease is applied and the hub no longer references it — or until
// Client close hands it back).  Each side detaches its share of `status`'s
// ownership word once; the side that detaches last frees the impl.
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

    // Fetch routing (distributed partition leadership; duty thread only).  One
    // entry per assigned partition while Stable: the ONE subscription instance
    // whose records this member accepts for it (C1).  The member subscribes
    // explicitly to the partition's LEADER (the coordinator creates none),
    // after its OffsetFetch answer, from the committed offset (or the reset
    // policy).  Cold path: changed only by group / routing events.
    struct FetchSub {
        uint32_t session        = 0;      // agent session index it was sent on (0 = the seed)
        uint64_t epoch          = 0;      // that session's connect epoch when sent; 0 = no subscription
        int64_t  brokerSid      = 0;      // that session's broker id when sent (Records' sessionId)
        int64_t  instanceCorr   = 0;      // its Subscribe correlationId — the instance identity the
                                          // broker stamps on every push (kept while it lives)
        int64_t  corrId         = 0;      // == instanceCorr while unanswered (no SubscribeResponse / Records yet)
        int64_t  subscriptionId = 0;      // bound by the SubscribeResponse (0 = unanswered)
        bool     offsetKnown    = false;  // OffsetFetch answered (OK) for it this generation
        bool     parked         = false;  // refused: retried on fresh metadata
        uint8_t  redirects      = 0;      // consecutive Redirects without a bind
    };
    std::map<TopicPartition, FetchSub> fetch;
    // MEDIUM-2: OffsetFetch re-send pacing (duty-clock ns; 0 = due).
    int64_t offsetFetchDueNs     = 0;
    int64_t offsetFetchBackoffNs = 0;
    // No-hang fix round 2 (I2): the correlationId of the latest OffsetFetch
    // (only ITS answer seeds positions) and consecutive answers carrying a
    // retriable per-entry error (budget: GroupConsumerHub::kMaxOffsetFetchAttempts).
    int64_t  offsetFetchCorrId   = 0;
    uint32_t offsetFetchAttempts = 0;
    int64_t  offsetFetchCountedCorr = 0;   // fix round 3: the fetch whose failure was already counted
    // Fix round 4: consecutive OffsetFetch answers that re-joined (NotCoordinator /
    // rebalance set) with no fetch served in between (cap: kMaxOffsetFetchRejoins).
    uint32_t offsetFetchRejoins  = 0;
    // C1: consecutive backoff rejoins (Internal / unclassified errors) and the
    // earliest time the next JoinGroup may go out.
    uint32_t backoffRejoins = 0;
    std::chrono::steady_clock::time_point rejoinNotBefore{};

    // ---- Timers (duty thread) -------------------------------------------------
    std::chrono::steady_clock::time_point nextHeartbeat;
    std::chrono::steady_clock::time_point nextAutoCommit;
    std::chrono::steady_clock::time_point joinDeadline;  // for rejoin backoff guard
    // D4: the session id the Join / Sync in flight went out on (0 = refused /
    // not sent: held while the coordinator session connects).  A connect of THAT
    // session (a resume keeping its id) keeps the in-flight join — re-sending it
    // with an empty memberId would register a ghost member (review MEDIUM-3).
    int64_t joinSessionId = 0;

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
    GroupFailReason failReason {GroupFailReason::None};  // set before a failing finish

    // ---- F8c C7 fix round 2 (F1): orphaned release -------------------------------
    int64_t orphanFence  = 0;      // written by the handle BEFORE its orphan RMW (published by it)
    bool    releaseNoted = false;  // duty thread: its release was applied / queued

    // ---- Duty thread -> app handle ------------------------------------------------
    detail::StatusRing<GroupConsumerStatus, 256> status;

    // ---- Construction --------------------------------------------------------
    GroupConsumerImpl() {
        sendBuf.resize(4096);  // pre-allocate scratch (avoids per-send allocation)
    }
    // F8c C7: freed by whichever of {the handle, the duty side} detaches its
    // share of `status`'s ownership word last (see GroupConsumer).
    ~GroupConsumerImpl() {
        if (detail::testOnImplFree != nullptr) detail::testOnImplFree(detail::HandleKind::GroupConsumer, this);
    }

    // Non-copyable / non-movable (raw single-owner hand-off; the ring is pinned).
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
//   - Destruction (F8c C7) = close() + the handle's share of the impl handed
//     to the duty side (one HandleRelease: applied directly on the duty
//     thread, through the command ring elsewhere — orphaned and applied by
//     the duty side in FIFO order when the ring is full); the impl is freed by
//     whichever side lets go last — never while one of its callbacks runs.
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
    GroupConsumer(GroupConsumerImpl*     impl,
                  detail::HubCommandSink sink,
                  void*                  sinkCtx,
                  std::thread::id        dutyThread = {},
                  detail::TargetedOpSink opSink     = nullptr,
                  detail::AndWaitSink    waitSink   = nullptr) noexcept;

    /// F8c C7 release (§3.3): closes the consumer (the duty side applies it)
    /// and gives the handle's share of the impl to the duty side.  Never
    /// throws, never waits; routed like ~Producer (duty thread: direct;
    /// elsewhere: the command ring, orphaned when full).  After Client
    /// close the handle frees the impl, never touching the Client.
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

    /// Why the duty side finished this consumer on its own (None otherwise) —
    /// see GroupFailReason for the client / broker compatibility rule.
    [[nodiscard]] GroupFailReason failReason() const noexcept;

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
    /// F8c C7: the §3.3 release of the handle's share (see the destructor).
    void release_() noexcept;

    GroupConsumerImpl*                 impl_    = nullptr;  // the handle's share (§3.3)
    detail::HubCommandSink             sink_    = nullptr;
    void*                              sinkCtx_ = nullptr;
    detail::TargetedOpSink             opSink_   = nullptr;
    detail::AndWaitSink                waitSink_ = nullptr;
    std::thread::id                    dutyThread_{};
    bool closed_ = false;   // handle-local (app-owned): close() was called
    mutable GroupConsumerStatus cache_{};  // app-owned: last drained status snapshot
};

} // namespace rune::client
