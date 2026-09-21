// include/rune/client/producer/TransactionalProducerImpl.h
//
// TX-5 (fix-round 1) — TransactionalProducerImpl: agent-side state for Tier-3
// transactional produce + idempotent-by-default sequencing.
//
// Threading model (DR-BUG-1 discipline):
//   App threads:  submit commands via cmdQueue_ / pendingSends_ (mutex-guarded cold path).
//   Agent thread: drain queues, emit protocol frames, resolve futures/callbacks.
//   No lock on the hot frame encode/send path (single-writer: agent).
//
// Idempotent sequence (CRITICAL 1 fix):
//   Once producerId >= 0, every TxnPartitionState carries a monotonic nextBaseSequence
//   stamped into the RecordBatch.  Produce frames are sent with a real correlationId
//   and acks=cfg_.acks so the broker returns a ProduceResponse.  On success the
//   sequence advances.  On retriable error the SAME in-flight batch (same wire
//   frame, same baseSequence) is re-sent unchanged — the broker deduplicates via
//   (producerId, partition, baseSequence).  On OutOfOrderSequence or
//   InvalidProducerEpoch/ProducerFenced the producer is fenced.
//
// Fencing:
//   On any response with errorCode ∈ {InvalidProducerEpoch, ProducerFenced},
//   producerId_ is reset to -1 and all pending futures are failed with a
//   ProducerFenced error.  The caller MUST call initTransactions() again.
//
// Timeout sweep:
//   doWork() sweeps pendingOps_ for expired deadlines and fails those futures
//   with a timeout error.  In-flight produce batches whose corrId is no longer
//   in pendingProduceBatches_ are already resolved.

#pragma once

#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/producer/ProducerImpl.h>    // ProducerConfig, PartitionState helpers

#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// TxnPartitionState — per-(topicId,partition) accumulator for txn producer.
//   Shares the idempotent sequence numbering with Tier-1.
// ---------------------------------------------------------------------------
struct TxnPartitionState {
    int32_t topicId          = 0;
    int32_t partition        = 0;
    int32_t nextBaseSequence = 0;  // monotonic idempotent seq
    bool    addedToTxn       = false; // true once AddPartitionsToTxn was emitted

    TxnPartitionState() = default;
    TxnPartitionState(int32_t tid, int32_t p) : topicId(tid), partition(p) {}
};

// ---------------------------------------------------------------------------
// PendingTxnOp — a pending async operation (future-backed).
// ---------------------------------------------------------------------------
struct PendingTxnOp {
    enum class Kind { InitProducerId, EndTxn, SendOffsets };
    Kind                 kind;
    std::promise<void>   promise;
    std::chrono::steady_clock::time_point deadline;
};

// ---------------------------------------------------------------------------
// TransactionalProducerImpl — internal state owned behind TransactionalProducer.
//
// Agent thread drives all sends; app thread enqueues via submitXxx() methods.
// ---------------------------------------------------------------------------
class TransactionalProducerImpl {
public:
    static constexpr int64_t kNoPid = -1;

    explicit TransactionalProducerImpl(
        std::string transactionalId,
        ProducerConfig cfg,
        std::atomic<int64_t>& nextCorrId,
        int32_t transactionTimeoutMs = 60'000);

    ~TransactionalProducerImpl() = default;

    // Non-copyable / non-movable.
    TransactionalProducerImpl(const TransactionalProducerImpl&) = delete;
    TransactionalProducerImpl& operator=(const TransactionalProducerImpl&) = delete;
    TransactionalProducerImpl(TransactionalProducerImpl&&) = delete;
    TransactionalProducerImpl& operator=(TransactionalProducerImpl&&) = delete;

    // ---- App-thread: submit commands (wait-free ring writes) ----------------

    /// Submit InitProducerId request. Returns a future resolved on response.
    std::future<void> submitInitTransactions();

    /// Mark beginTransaction (local state change — no wire message).
    void beginTransaction();

    /// Submit a transactional send (enqueues SEND_RECORD to ring).
    /// Auto AddPartitionsToTxn is emitted by the agent on first send to a partition.
    void submitSend(int32_t topicId,
                    int32_t partition,
                    const Key& key,
                    std::span<const uint8_t> value);

    /// Submit sendOffsetsToTransaction: emits AddOffsetsToTxn + TxnOffsetCommit.
    std::future<void> submitSendOffsets(
        const std::map<rune::client::TopicPartitionKey, int64_t>& offsets,
        const std::string& groupId);

    /// Submit EndTxn(commit).  Returns future resolved on EndTxnResponse.
    std::future<void> submitCommit();

    /// Submit EndTxn(abort).  Returns future resolved on EndTxnResponse.
    std::future<void> submitAbort();

    // ---- Accessors (may be called from any thread — atomic reads) -----------

    [[nodiscard]] int64_t producerId() const noexcept {
        return producerId_.load(std::memory_order_acquire);
    }
    [[nodiscard]] int16_t producerEpoch() const noexcept {
        return producerEpoch_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool transactionActive() const noexcept {
        return txnActive_.load(std::memory_order_acquire);
    }

    // ---- Agent-thread: duty cycle ------------------------------------------

    /// Drain the command ring and process pending frames.
    /// sendFn: transport send function.
    /// Returns work count.
    int doWork(int64_t sessionId,
               std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// Called when InitProducerIdResponse (templateId 46) arrives.
    void onInitProducerIdResponse(const uint8_t* buf, std::size_t len);

    /// Called when AddPartitionsToTxnResponse (templateId 48) arrives.
    void onAddPartitionsToTxnResponse(const uint8_t* buf, std::size_t len);

    /// Called when AddOffsetsToTxnResponse (templateId 50) arrives.
    void onAddOffsetsToTxnResponse(const uint8_t* buf, std::size_t len,
                                    int64_t sessionId,
                                    std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// Called when TxnOffsetCommitResponse (templateId 52) arrives.
    void onTxnOffsetCommitResponse(const uint8_t* buf, std::size_t len);

    /// Called when EndTxnResponse (templateId 54) arrives.
    void onEndTxnResponse(const uint8_t* buf, std::size_t len);

    /// Called when ProduceResponse (templateId 17) arrives for a txn produce.
    /// On success: removes in-flight batch, future resolved if any.
    /// On retriable error: re-sends the same wire frame (same baseSequence).
    /// On fencing error: calls handleFencing().
    void onProduceResponse(const uint8_t* buf, std::size_t len,
                           std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// Fail all pending ops (called on close).
    void failAll(const std::string& reason);

private:
    // ---- Internal command types in the ring ---------------------------------
    // We reuse the same ring as the Tier-1 ProducerImpl for SEND_RECORD.
    // Additional commands are queued in a separate atomic command queue (cold path).

    // ---- Internal helpers (agent thread) ------------------------------------

    TxnPartitionState& getOrCreatePartition(int32_t topicId, int32_t partition);

    bool sendInitProducerId(int64_t sessionId, int64_t corrId,
                            const std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    bool sendAddPartitionsToTxn(int64_t sessionId, int64_t corrId,
                                 const std::vector<rune::protocol::client::TxnTopicPartition>& parts,
                                 const std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    bool sendEndTxn(int64_t sessionId, int64_t corrId, bool commit,
                    const std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    bool sendAddOffsetsToTxn(int64_t sessionId, int64_t corrId,
                              std::span<const uint8_t> groupIdSpan,
                              const std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    bool sendTxnOffsetCommit(int64_t sessionId, int64_t corrId,
                              std::span<const uint8_t> groupIdSpan,
                              const std::vector<rune::protocol::client::TxnOffsetCommitEntry>& entries,
                              const std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    void handleFencing(int16_t errorCode);

    // Sweep pendingOps_ for expired deadlines; fail those futures with timeout.
    void sweepTimedOut();

    // ---- State (agent-thread writes; some atomics for app-thread reads) -----

    std::string    transactionalId_;
    ProducerConfig cfg_;
    int32_t        transactionTimeoutMs_;

    // Shared correlationId counter with other client ops.
    std::atomic<int64_t>& nextCorrId_;

    // Idempotent producer identity (acquired via InitProducerId).
    // Stored atomically so accessors can read from any thread.
    std::atomic<int64_t> producerId_     {kNoPid};
    std::atomic<int16_t> producerEpoch_  {0};

    // Transaction state (agent-thread only; atomic for app-thread read).
    std::atomic<bool>    txnActive_      {false};

    // Partitions added to the current transaction (agent-thread only).
    std::set<rune::client::TopicPartitionKey> addedPartitions_;

    // Per-(topicId,partition) sequence state (agent-thread only).
    std::map<rune::client::TopicPartitionKey, TxnPartitionState> partitions_;

    // Pending send records: app thread appends, agent thread drains.
    struct SendRecord {
        int32_t              topicId;
        int32_t              partition;
        Key                  key;
        std::vector<uint8_t> value;
    };

    // SendOffsets state: waiting for AddOffsetsToTxn response before TxnOffsetCommit.
    struct PendingOffsetOp {
        std::string                                       groupId;
        std::map<rune::client::TopicPartitionKey, int64_t> offsets;
        int64_t                                           corrId = 0;
        std::promise<void>                                promise;
    };
    std::unique_ptr<PendingOffsetOp> pendingOffsetOp_;

    // Pending async ops keyed by correlationId (agent-thread only).
    std::map<int64_t, PendingTxnOp> pendingOps_;

    // In-flight produce batches keyed by correlationId (agent-thread only).
    // Retained so that on retry the SAME wire frame (same baseSequence) can be
    // re-sent without rebuilding the batch.
    struct InFlightBatch {
        int32_t              topicId;
        int32_t              partition;
        int32_t              baseSequence; // the sequence stamped in the batch header
        std::vector<uint8_t> wireFrame;    // encoded Produce frame bytes for re-send
        int32_t              retryCount = 0;
        static constexpr int kMaxRetries = 5;
    };
    std::map<int64_t, InFlightBatch> pendingProduceBatches_; // corrId → in-flight batch

    // Encode scratch buffers (agent-thread; no alloc on hot path).
    std::vector<uint8_t> encodeScratch_;
    std::vector<uint8_t> batchScratch_;

    // Command queue from app thread → agent thread (mutex-guarded cold path).
    // Commands: InitTransactions, EndTxn(commit), EndTxn(abort), SendOffsets.
    enum class CmdKind { InitTransactions, EndTxnCommit, EndTxnAbort, SendOffsets, SendRecord };
    struct Command {
        CmdKind kind = CmdKind::InitTransactions;
        // for SendOffsets:
        std::string                                       groupId;
        std::map<rune::client::TopicPartitionKey, int64_t> offsets;
        std::promise<void>                                promise;
    };
    std::mutex           cmdMtx_;
    std::vector<Command> cmdQueue_;    // app threads push, agent drains

    // Pending sends queue (separate from cmdQueue_ to avoid extra lock on send path).
    std::mutex               pendingSendsMtx_;
    std::vector<SendRecord>  pendingSends_;
};

} // namespace rune::client::producer
