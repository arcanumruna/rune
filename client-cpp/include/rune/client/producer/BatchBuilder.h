// include/rune/client/producer/BatchBuilder.h
//
// CB-3 — Tier-2 Producer: explicit all-or-nothing batch to ONE partition.
//
// A single RecordBatch to one partition is already atomic (one log append) —
// Tier-2 is the explicit form; NO transaction/markers.
//
// Threading model:
//   BatchBuilder is NOT thread-safe.  A single app thread constructs it, calls
//   add() any number of times, then calls send() exactly once.  After send()
//   the builder is "consumed" — add() will throw.
//
// Idempotence:
//   Shares the same producerId/epoch/baseSequence state as the Tier-1 producer
//   for the (topicId, partition).  The agent thread stamps the baseSequence
//   when it processes the batch token from the ring — so seq stays monotonic
//   and gap-free across Tier-1 and Tier-2 batches.
//
// Ordering vs Tier-1:
//   send() writes a kMsgTier2Batch token into the same ring that Tier-1 uses.
//   The agent processes both token types in the order they were written —
//   so submission order = processing order for a given partition.
//   A Tier-2 batch is NEVER merged into the Tier-1 accumulator; it is always
//   flushed as its own RecordBatch.
//
// RAII / lifecycle:
//   - add() after send() → throws std::logic_error ("BatchBuilder: add after send").
//   - send() with 0 records → throws std::invalid_argument ("BatchBuilder: empty batch").
//   - BatchBuilder destroyed without send() → no-op (nothing produced).
//     (Tier-3 RAII auto-abort of uncommitted txns is Milestone C; Tier-2 has
//      no transaction, so an unsent builder simply does nothing.)
//
// Usage:
//   auto bb = producer.batch(topic, Key(orderId));
//   bb.add(valueSpan);
//   bb.add(value2Span);
//   auto f = bb.send();          // returns future<int64_t> resolving with baseOffset
//   int64_t base = f.get();      // records land at base..base+K-1 (contiguous)

#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>

#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <future>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace rune::client::producer {
class ProducerImpl;
} // namespace rune::client::producer

namespace rune::client {

// ---------------------------------------------------------------------------
// BatchBuilder — returned by Producer::batch(topic, key).
//
// Not copyable; movable (ownership transfer only).
// ---------------------------------------------------------------------------
class BatchBuilder {
public:
    // Constructed by Producer::batch() — not for direct construction.
    explicit BatchBuilder(std::shared_ptr<producer::ProducerImpl> impl,
                          int32_t topicId,
                          int32_t partition) noexcept;

    // Movable, not copyable.
    BatchBuilder(BatchBuilder&&) noexcept = default;
    BatchBuilder& operator=(BatchBuilder&&) noexcept = default;
    BatchBuilder(const BatchBuilder&) = delete;
    BatchBuilder& operator=(const BatchBuilder&) = delete;

    // Destructor: if send() was never called, this is a no-op (nothing produced).
    ~BatchBuilder() = default;

    // ---- Record accumulation -------------------------------------------------

    /// Append a value to the batch (record key = empty).
    ///
    /// Throws std::logic_error if send() has already been called.
    void add(std::span<const uint8_t> value);

    /// Append a value with an optional record-level key.
    ///
    /// Note: the record-level key is distinct from the partition-routing Key;
    /// it is carried inside the RecordBatch body (for application use).
    ///
    /// Throws std::logic_error if send() has already been called.
    void add(std::span<const uint8_t> value,
             std::string_view         record_key);

    // ---- Flush ---------------------------------------------------------------

    /// Flush the accumulated records as a single Produce to the resolved partition.
    ///
    /// Semantics:
    ///   - All records accumulated via add() are encoded into ONE RecordBatch.
    ///   - The batch is submitted as a single atomic log append (one Produce
    ///     message, one partition, one RecordBatch).
    ///   - The returned future resolves with the broker's baseOffset.
    ///     The K records land at contiguous offsets base..base+K-1.
    ///   - Ordering with Tier-1: the batch token is enqueued into the same
    ///     ring used by Tier-1, so submission order is preserved.
    ///
    /// Throws std::invalid_argument if the batch is empty (no add() calls).
    /// Throws std::logic_error if send() has already been called.
    ///
    /// After send() returns, add() will throw on subsequent calls.
    std::future<int64_t> send();

private:
    std::shared_ptr<producer::ProducerImpl> impl_;
    int32_t topicId_   = -1;
    int32_t partition_ = -1;

    // Per-batch RecordBatchBuilder (reused; reset if needed, but a BatchBuilder
    // is single-use so the builder is built once).
    std::unique_ptr<rune::protocol::RecordBatchBuilder> builder_;

    // Lifecycle guard.
    bool sent_ = false;
};

} // namespace rune::client
