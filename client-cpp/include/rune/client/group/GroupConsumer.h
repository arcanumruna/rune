// include/rune/client/group/GroupConsumer.h
//
// CG-5 — Consumer-group API: GroupConsumer + GroupConfig.
// Namespace: rune::client
//
// Threading model:
//   - All state-machine transitions (join/sync/stable/rejoin), Records routing,
//     heartbeat, and auto-commit duty-cycle run on the RUNNER (agent) thread.
//   - App thread only calls: onAssigned/onRevoked/onRecords (setup-phase
//     callback registration), commit(), commitSync(), and close().
//   - commit() / commitSync() are wait-free from the app thread: they enqueue a
//     commit request via an atomic flag + position snapshot (no lock on hot path).
//   - GroupConsumer registration in Client uses the same ProducerHub-style
//     atomic COW snapshot (GroupConsumerHub) — no locks on the runner hot path.
//
// Public API:
//   GroupConsumer& Client::subscribeGroup(groupId, topics, GroupConfig)
//   gc.onAssigned(fn)  — called on runner thread when partitions are assigned
//   gc.onRevoked(fn)   — called on runner thread when partitions are revoked
//   gc.onRecords(fn)   — called on runner thread when records arrive (assigned partitions)
//   gc.commit()        — async, enqueues commit of current positions (wait-free)
//   gc.commitSync()    — returns a future resolved when OffsetCommitResponse arrives
//   gc.close()         — deactivate (idempotent, any thread)

#pragma once

#include <rune/client/subscriber/Subscription.h>  // TopicPartition, Record types
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/OffsetMessages.h>  // OffsetCommitEntry (for commitEntriesScratch)

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
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
// GroupConsumerImpl — internal state owned by Client (heap-allocated,
// shared_ptr between GroupConsumer handle and GroupConsumerHub routing table).
//
// RUNNER-THREAD WRITES; APP-THREAD reads only via wait-free atomic accessors.
// ---------------------------------------------------------------------------
struct GroupConsumerImpl {
    // ---- Config (immutable after construction) --------------------------------
    std::string              groupId;
    std::vector<int32_t>     topicIds;     // resolved from topic names on creation
    GroupConfig              cfg;

    // ---- Callbacks (set at setup time; read on runner thread) ----------------
    std::function<void(std::vector<TopicPartition>)> cbAssigned;
    std::function<void(std::vector<TopicPartition>)> cbRevoked;
    std::function<void(const RecordSet&)>             cbRecords;

    // ---- State machine (runner thread only) ----------------------------------
    GroupConsumerState                  state        {GroupConsumerState::Unjoined};
    std::string                         memberId;      // assigned by broker; empty = new
    int32_t                             generationId {0};

    // Current assignment: partition → nextOffset to commit.
    // Written only on the runner thread.
    // Sentinel value -1 means "position not yet seeded from OffsetFetch".
    // sendOffsetCommit MUST skip any entry whose value is kUnseededSentinel.
    static constexpr int64_t kUnseededSentinel = -1;
    std::map<TopicPartition, int64_t>   assignment;   // topicPartition → nextOffset (or sentinel)

    // ---- Timers (runner thread) ----------------------------------------------
    std::chrono::steady_clock::time_point nextHeartbeat;
    std::chrono::steady_clock::time_point nextAutoCommit;
    std::chrono::steady_clock::time_point joinDeadline;  // for rejoin backoff guard

    // ---- Send scratch buffers (runner thread, zero-alloc hot path) -----------
    // Reused on every heartbeat / join / sync / commit send.
    std::vector<uint8_t> sendBuf;
    // Reusable OffsetCommit entries scratch — avoids per-commit heap allocation.
    // Only accessed on the runner thread; no locking needed.
    std::vector<rune::protocol::client::OffsetCommitEntry> commitEntriesScratch;

    // ---- Wait-free commit request from app thread ---------------------------
    // commitRequested_ is set (release) by the app thread's commit().
    // Runner thread clears it (acquire) and sends OffsetCommit.
    std::atomic<bool> commitRequested {false};

    // commitSyncPending_: if true, runner must resolve commitSyncPromise_ after
    // the next OffsetCommitResponse.
    // Access: commitSyncMtx_ guards promise + flag (rare cold path).
    std::mutex              commitSyncMtx;
    bool                    commitSyncPending {false};
    std::promise<void>      commitSyncPromise;

    // ---- Liveness ------------------------------------------------------------
    // active: cleared by close() (any thread); runner checks before dispatching.
    std::atomic<bool> active {true};

    // ---- Construction --------------------------------------------------------
    GroupConsumerImpl() {
        sendBuf.resize(4096);  // pre-allocate scratch (avoids per-send allocation)
    }

    // Non-copyable / non-movable (shared_ptr ownership + atomics).
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
//   - commit() / commitSync() are safe from any thread (wait-free flag).
//   - close() is safe from any thread (atomic store).
//   - The handle is movable (unique ownership, like Subscription).
// ---------------------------------------------------------------------------
class GroupConsumer {
public:
    // Constructed by Client::subscribeGroup().
    explicit GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl) noexcept;

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

    /// Enqueue a commit of current per-partition positions (wait-free from app
    /// thread).  The commit is sent on the runner thread on the next duty cycle.
    /// At-least-once: only positions of records already delivered via onRecords
    /// are committed.
    void commit();

    /// Submit a commit and return a future resolved when the broker acknowledges
    /// the OffsetCommitResponse (or when close() is called).
    std::future<void> commitSync();

    /// Deactivate this consumer: no more dispatch occurs.  Idempotent, any thread.
    void close();

    /// True if the consumer is still active.
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
    std::shared_ptr<GroupConsumerImpl> impl_;
    bool moved_ = false;
};

} // namespace rune::client
