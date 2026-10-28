// include/rune/client/producer/BatchBuilder.h
//
// CB-3 — Tier-2 Producer: explicit all-or-nothing batch to ONE partition.
//
// A single RecordBatch to one partition is already atomic (one log append) —
// Tier-2 is the explicit form; NO transaction/markers.
//
// Threading model:
//   Tier-2 is single-thread per producer: batch() / add() / send() / destruction run on the
//   producer's batch thread (the thread that created it; Invoker mode: the owner thread; or the
//   thread that called Producer::bindBatchThread()).  batch() off that thread returns a refused
//   builder whose send() is WrongThread.
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
//   - send() on a transactional producer outside a transaction (its gate closed) → throws
//     std::logic_error ("produce outside a transaction"); no credit is kept and the batch stays intact, so the
//     same builder can be sent once a transaction is open.  (OpResult has no such value, so it throws like every
//     other Tier-2 misuse.)
//   - send() on an Invoker-mode producer while one of its claims (tryClaim / claimBatch) is open → returns
//     BackPressured and keeps the batch: retry after the claim is committed or aborted.
//   - BatchBuilder destroyed without send() → nothing produced; its pool slot goes back to the
//     producer (so it must be destroyed on the batch thread, before the Producer).
//     Thread mode: a builder destroyed elsewhere hands its slot back through the producer ring;
//     Direct (Invoker) mode loses the slot and logs an error.
//     (Tier-3 RAII auto-abort of uncommitted txns is Milestone C; Tier-2 has
//      no transaction, so an unsent builder simply does nothing.)
//
// Usage:
//   auto bb = producer.batch(topic, Key(orderId));
//   bb.add(valueSpan);
//   bb.add(value2Span);
//   bb.send(cq, token);          // F8c C6: Completion{op=Tier2Batch, value=baseOffset,
//                                //   aux=recordCount} into cq; records at base..base+K-1

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>

#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace rune::client::producer {
class ProducerImpl;
} // namespace rune::client::producer

namespace rune::client {

class Completions;
struct AckToken;

// ---------------------------------------------------------------------------
// BatchBuilder — returned by Producer::batch(topic, key).
//
// Not copyable; movable.  F8c C7: BORROWS its Producer's impl — it must not
// outlive the Producer it came from (spec §3.1 rule 1).
// ---------------------------------------------------------------------------
class BatchBuilder {
public:
    // Constructed by Producer::batch() — not for direct construction.  F8c
    // C7: BORROWS the producer (spec §3.1 rule 1): it must not outlive the
    // Producer it came from (a send() after that Producer was destroyed
    // throws std::logic_error while the duty side still holds the impl).
    explicit BatchBuilder(producer::ProducerImpl* impl,
                          int32_t topicId,
                          int32_t partition) noexcept;

    // Movable, not copyable.
    BatchBuilder(BatchBuilder&& o) noexcept;
    BatchBuilder& operator=(BatchBuilder&& o) noexcept;
    BatchBuilder(const BatchBuilder&) = delete;
    BatchBuilder& operator=(const BatchBuilder&) = delete;

    // Destructor: if send() was never accepted, nothing is produced and the pool slot
    // (see below) goes back to the producer.
    ~BatchBuilder();

    // ---- Record accumulation -------------------------------------------------

    /// Append a value to the batch (record key = empty).
    ///
    /// Throws std::logic_error if send() has already been called.
    BatchBuilder& add(std::span<const uint8_t> value);

    /// Append a value with an optional record-level key.
    ///
    /// Note: the record-level key is distinct from the partition-routing Key;
    /// it is carried inside the RecordBatch body (for application use).
    ///
    /// Throws std::logic_error if send() has already been called, and
    /// std::length_error (MessageTooLarge, non-retryable) if the record would take the
    /// batch past ProducerConfig::tier2SlotBytes (or the record alone past kMaxRecordBytes).
    ///
    /// The batch is built in a preallocated pool slot (ProducerConfig::tier2Slots), taken when
    /// Producer::batch() creates the builder.  A builder that holds a slot never refuses add() for
    /// the pool - only for size (std::length_error).  A REFUSED builder (every slot in use, or
    /// batch() called off the batch thread) stores nothing; its send() reports BackPressured /
    /// WrongThread, so the caller retries the WHOLE batch with a new builder.
    /// Throws std::logic_error when called off the batch thread on a builder that holds a slot.
    /// Returns *this (chainable).
    BatchBuilder& add(std::span<const uint8_t> value,
                      std::string_view         record_key);

    // ---- Flush ---------------------------------------------------------------

    /// Flush the accumulated records as a single Produce to the resolved partition.
    ///
    /// Semantics:
    ///   - All records accumulated via add() are encoded into ONE RecordBatch.
    ///   - The batch is submitted as a single atomic log append (one Produce
    ///     message, one partition, one RecordBatch); the K records land at
    ///     contiguous offsets base..base+K-1.
    ///   - Ordering with Tier-1: the batch token is enqueued into the same
    ///     ring used by Tier-1, so submission order is preserved.
    ///
    /// F8c C6 — three forms, all returning the issue result synchronously:
    ///   send()                 no completion (fire-and-forget);
    ///   send(cq, token)        one Completion{op = Tier2Batch, value =
    ///                          baseOffset (or -1), aux = recordCount} into cq
    ///                          (credit reserved on cq first: QueueFull);
    ///   send(AckToken{token})  the producer's onAck handler (baseOffset).
    /// Accepted: the batch is consumed (add() throws afterwards) and exactly
    /// one completion follows for the token forms.  Any other result leaves
    /// the batch INTACT (retry send() later): BackPressured (ring full /
    /// deferred arena full / its partition still has deferred records),
    /// Closed (the producer's intake is closed), WrongThread (also: the builder was
    /// made off the batch thread, or send() runs off it), QueueFull,
    /// InvalidArgument (unaddressable partition, bad token / queue).  A builder
    /// refused at batch() because every pool slot was in use reports BackPressured
    /// (nothing was stored: rebuild the batch).
    ///
    /// Throws std::invalid_argument if the batch is empty (no add() calls).
    /// Throws std::logic_error if send() has already been accepted.
    [[nodiscard]] OpResult send();
    [[nodiscard]] OpResult send(Completions& cq, Token token);
    [[nodiscard]] OpResult send(AckToken token);

private:
    void checkProducer_() const;

    /// Where the batch is.  The slot index is meaningful only in Open / Built (each state
    /// holds exactly what it needs):
    ///   Refused  batch() ran off the batch thread: nothing touched, send() = WrongThread;
    ///   Starved  every pool slot was in use at batch(): add() stores nothing, send() = BackPressured;
    ///   Open     records accumulate in pool slot slot_ (taken at batch());
    ///   Built    the RecordBatch is sealed in slot slot_ (kept across a rejected send());
    ///   Sent     accepted (or moved-from): the slot belongs to the duty side / nobody.
    enum class State : uint8_t { Refused, Starved, Open, Built, Sent };

    producer::ProducerImpl* impl_ = nullptr;  // borrowed (owned by the Producer + duty side)
    int32_t  topicId_   = -1;
    int32_t  partition_ = -1;
    uint32_t slot_      = 0xFFFF'FFFFu;      // kNoTier2Slot
    State    state_     = State::Refused;

    /// Return the held slot (Open / Built) to the producer's pool, exactly once.
    void releaseSlot_() noexcept;

    /// Seal (once) and submit with this ack handle (0 = none).
    OpResult submit_(uint64_t ackHandle);
};

} // namespace rune::client
