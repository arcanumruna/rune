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
// GroupConsumerStatus — duty thread -> app handle snapshot.  Only the status
// ring's closed word carries information today (liveness); the record type
// exists so the channel can carry more without a layout change.
// ---------------------------------------------------------------------------
struct GroupConsumerStatus {
    int32_t generationId = 0;
    int32_t reserved     = 0;
};

// ---------------------------------------------------------------------------
// GroupConsumerImpl — internal state (heap-allocated, shared_ptr between the
// GroupConsumer handle and the GroupConsumerHub's consumer list).
//
// F8a ownership: config + callbacks are written by the app thread BEFORE the
// GroupAdd command is put on the agent ring (publishing them); EVERYTHING else
// is owned by the duty thread.  The app thread reads only `status` (the ring's
// own memory ordering is the only synchronisation).
// ---------------------------------------------------------------------------
struct GroupConsumerImpl {
    // ---- Config (immutable after construction) --------------------------------
    std::string              groupId;
    std::vector<int32_t>     topicIds;     // resolved from topic names on creation
    GroupConfig              cfg;

    // ---- Callbacks (setup phase; read on the duty thread) ---------------------
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
    // The in-flight commitSync() promise (null = none).  Resolved on the next
    // OffsetCommitResponse, superseded by a newer commitSync(), or failed on
    // close.  Owned by the duty thread — no lock.
    std::unique_ptr<std::promise<void>> commitSyncPromise;

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
//   - Callback setters (onAssigned/onRevoked/onRecords) are SETUP PHASE ONLY
//     (must be called before the first connect() — same contract as Client
//     lifecycle callbacks).
//   - The handle is OWNED by one thread at a time (move-only, like
//     Subscription): commit()/commitSync()/close()/active() must not be called
//     concurrently on the same handle.  Each submits a command to the duty
//     thread (commit() may also be called from inside onRecords/onAssigned —
//     the command is applied on a later duty cycle).
//   - A handle may outlive its Client: once the consumer is finished on the
//     duty side its status ring is closed and the handle never touches the
//     Client again (commitSync() then returns an already-failed future).
// ---------------------------------------------------------------------------
class GroupConsumer {
public:
    /// Constructed by Client::subscribeGroup().  `sink`/`sinkCtx` deliver the
    /// handle's commands to the duty thread (the Client's command ring).
    GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl,
                  detail::HubCommandSink             sink,
                  void*                              sinkCtx) noexcept;

    ~GroupConsumer();

    // Non-copyable, movable.
    GroupConsumer(const GroupConsumer&) = delete;
    GroupConsumer& operator=(const GroupConsumer&) = delete;
    GroupConsumer(GroupConsumer&&) noexcept;
    GroupConsumer& operator=(GroupConsumer&&) noexcept;

    // ---- Callback registration (SETUP PHASE: call before connect()) ----------

    /// Called on the runner thread when the broker assigns partitions.
    void onAssigned(std::function<void(std::vector<TopicPartition>)> cb);

    /// Called on the runner thread when the broker revokes partitions.
    void onRevoked(std::function<void(std::vector<TopicPartition>)> cb);

    /// Called on the runner thread when records arrive for assigned partitions.
    void onRecords(std::function<void(const RecordSet&)> cb);

    // ---- Async commit API (safe from any thread) ----------------------------

    /// Ask the duty thread to commit the current per-partition positions (sent
    /// on a following duty cycle).  At-least-once: only positions of records
    /// already delivered via onRecords are committed.  No-op once finished.
    /// Throws only if the command ring stays full past the bounded wait.
    void commit();

    /// Submit a commit and return a future resolved when the broker acknowledges
    /// the OffsetCommitResponse — or failed when superseded by a newer
    /// commitSync(), on close(), or when the Client closes.  Never hangs.
    /// Invoker mode: the owner thread must keep calling Client::doWork() while
    /// it waits on the future.
    std::future<void> commitSync();

    /// Deactivate this consumer: no more dispatch occurs, a pending commitSync
    /// fails.  Idempotent.  Never throws (a command that cannot be delivered
    /// is logged; the Client's close() deactivates the consumer anyway).
    void close();

    /// True until close(), or until the duty side finished the consumer (the
    /// Client closed).
    [[nodiscard]] bool active() const noexcept;

    /// Current assignment generation as last set by the broker
    /// (JoinGroupResponse / Assign).  Increments on every rebalance.
    ///
    /// THREADING: generationId is written on the runner thread (during
    /// JoinGroupResponse / Assign handling) and MUST only be read from the
    /// runner thread — i.e. from inside an onAssigned / onRevoked / onRecords
    /// callback, which is exactly where the generation of the current
    /// assignment/record is meaningful.  Reading it from the app thread is a
    /// data race and is not supported.
    [[nodiscard]] int32_t generation() const noexcept;

    /// Access the raw impl (for hub wiring — not part of user API).
    [[nodiscard]] std::shared_ptr<GroupConsumerImpl> impl() const noexcept {
        return impl_;
    }

private:
    /// Submit `cmd` unless the consumer is already finished.  Returns the
    /// sink's verdict (Closed when finished / no sink).
    detail::HubSubmit submit_(const detail::HubCommand& cmd);

    std::shared_ptr<GroupConsumerImpl> impl_;
    detail::HubCommandSink             sink_    = nullptr;
    void*                              sinkCtx_ = nullptr;
    bool moved_  = false;
    bool closed_ = false;   // handle-local (app-owned): close() was called
};

} // namespace rune::client
