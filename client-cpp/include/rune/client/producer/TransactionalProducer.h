// include/rune/client/producer/TransactionalProducer.h
//
// TX-5 — Tier-3 Transactional Producer API.
//
// Threading model (mirrors ProducerHub DR-BUG-1 discipline):
//   All transactional state (producerId, epoch, transaction-in-progress flag,
//   addedPartitions set) is owned by TransactionalProducerImpl and accessed
//   ONLY on the agent/runner thread via the duty cycle.  App threads enqueue
//   commands via a lock-free ring (same ManyToOneRingBuffer discipline as
//   ProducerImpl).
//
//   Hot-path rule: send/commit hot path is wait-free: the app thread writes a
//   compact token into the ring and returns.  The agent thread processes the
//   ring, emits InitProducerId/AddPartitionsToTxn/EndTxn/TxnOffsetCommit wire
//   frames, and resolves futures/callbacks.  NO lock, NO per-op heap alloc on
//   the hot path.
//
// Idempotent-by-default (implemented):
//   Once InitProducerIdResponse arrives the producer has a real (producerId,
//   epoch).  Thereafter every send stamps a monotonically increasing baseSequence
//   per (topic, partition) into the RecordBatch header and sends with a real
//   correlationId so a ProduceResponse is received.  The encoded wire frame is
//   retained in-flight.  On a retriable error (NotLeader etc.) the SAME stored
//   wire frame is re-sent byte-for-byte — same baseSequence, same corrId — so
//   the broker can deduplicate if the original was already accepted.  On
//   OutOfOrderSequence / InvalidProducerEpoch / ProducerFenced the producer
//   is fenced (producerId reset to -1; all pending futures fail).
//
// Tier-3 transactional lifecycle:
//   initTransactions()              → send InitProducerId(transactionalId)
//                                      → on response: store pid/epoch
//   beginTransaction()              → set txnActive = true
//   send(topic, key, value)         → first send to a partition emits
//                                      AddPartitionsToTxn; stamps pid/epoch/seq
//   sendOffsetsToTransaction(...)   → AddOffsetsToTxn + TxnOffsetCommit
//   commitTransaction()             → EndTxn(commit)
//   abortTransaction()              → EndTxn(abort)
//
// Error handling:
//   ProducerFenced / InvalidProducerEpoch in any response → the error is
//   surfaced to the caller via a std::runtime_error exception on the future
//   or a callback with a negative offset; the caller MUST re-initTransactions.
//
// Usage:
//   auto tp = client.transactionalProducer("my-txn-id", {.acks=Acks::All});
//   tp.initTransactions().get();   // blocks until pid/epoch acquired
//   tp.beginTransaction();
//   tp.send(topic, key, value);
//   tp.commitTransaction().get();  // blocks until EndTxn acked

#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>       // Acks, ProducerConfig, want_future_t

#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <span>
#include <string>

namespace rune::client {

// Forward-declare implementation.
namespace producer { class TransactionalProducerImpl; }

// ---------------------------------------------------------------------------
// TopicPartitionKey — (topicId, partition) pair for the addedPartitions set.
// ---------------------------------------------------------------------------
struct TopicPartitionKey {
    int32_t topicId   = 0;
    int32_t partition = 0;
    bool operator==(const TopicPartitionKey& o) const noexcept {
        return topicId == o.topicId && partition == o.partition;
    }
    bool operator<(const TopicPartitionKey& o) const noexcept {
        return topicId < o.topicId ||
               (topicId == o.topicId && partition < o.partition);
    }
};

// ---------------------------------------------------------------------------
// TransactionalProducer — user-facing handle for the Tier-3 transactional API.
//
// Shared_ptr semantics (same as Producer): cheap to copy.
// ---------------------------------------------------------------------------
class TransactionalProducer {
public:
    /// Constructed by Client::transactionalProducer() — not for direct use.
    explicit TransactionalProducer(
        std::shared_ptr<producer::TransactionalProducerImpl> impl) noexcept;

    // Copyable / movable (shared_ptr semantics).
    TransactionalProducer(const TransactionalProducer&) = default;
    TransactionalProducer& operator=(const TransactionalProducer&) = default;
    TransactionalProducer(TransactionalProducer&&) = default;
    TransactionalProducer& operator=(TransactionalProducer&&) = default;

    ~TransactionalProducer() = default;

    // ---- Tier-3 transactional lifecycle (return futures for sync usage) ------

    /// Send InitProducerId(transactionalId) to the broker.
    /// Resolved when InitProducerIdResponse arrives with a valid (pid, epoch).
    /// Throws std::runtime_error on broker error (including ProducerFenced).
    std::future<void> initTransactions();

    /// Mark the start of a transaction.  Must be called after initTransactions()
    /// and before any transactional send.  Idempotent if already in a txn.
    void beginTransaction();

    /// Transactional send.
    ///   - Automatically emits AddPartitionsToTxn on the first send to a new
    ///     (topic, partition) within this transaction.
    ///   - Stamps producerId / producerEpoch / baseSequence from the stored
    ///     idempotent producer identity.
    void send(TopicHandle            topic,
              const Key&             key,
              std::span<const uint8_t> value);

    /// Send offsets to the transaction coordinator (consume-transform-produce
    /// pattern).  Emits AddOffsetsToTxn then TxnOffsetCommit to the broker.
    /// Resolved when TxnOffsetCommitResponse arrives with all-OK.
    std::future<void> sendOffsetsToTransaction(
        const std::map<TopicPartitionKey, int64_t>& offsets,
        const std::string& groupId);

    /// Commit the current transaction (EndTxn with commit=1).
    /// Resolved when EndTxnResponse arrives with errorCode==0.
    /// Throws std::runtime_error on failure (including ProducerFenced).
    std::future<void> commitTransaction();

    /// Abort the current transaction (EndTxn with commit=0).
    /// Resolved when EndTxnResponse arrives with errorCode==0.
    std::future<void> abortTransaction();

    // ---- Accessors -----------------------------------------------------------

    /// Current producerId (from InitProducerIdResponse).  -1 if not yet acquired.
    [[nodiscard]] int64_t producerId() const noexcept;

    /// Current producerEpoch (from InitProducerIdResponse).  0 if not yet acquired.
    [[nodiscard]] int16_t producerEpoch() const noexcept;

    /// True if a transaction is in progress (between beginTransaction and commit/abort).
    [[nodiscard]] bool transactionActive() const noexcept;

    /// Access raw impl (for hub wiring — not user API).
    [[nodiscard]] std::shared_ptr<producer::TransactionalProducerImpl> impl() const noexcept {
        return impl_;
    }

private:
    std::shared_ptr<producer::TransactionalProducerImpl> impl_;
};

} // namespace rune::client
