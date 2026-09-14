// include/rune/client/producer/Producer.h
//
// CB-2 — Tier-1 Producer: auto-batched, idempotent, ZERO per-send heap on the
// void fast path.
//
// Threading model:
//   App threads call send()/flush() → write a compact SEND_RECORD into the lock-
//   free ManyToOneRingBuffer (the zero-alloc handoff).  No lock is taken on the
//   hot path.
//   The agent thread (doWork, single-writer) drains the ring, accumulates records
//   into per-(topicId,partition) RecordBatchBuilders, and flushes batches when
//   linger elapsed OR batchBytes reached OR flush() requested.
//
// Acks:
//   void send(...)              — fire-and-forget; no ack tracking.
//   send(..., callback)         — opt-in callback; allocates a correlationId entry.
//   send(..., want_future_tag)  — opt-in future; allocates a promise.
//   (Per-send heap allowed ONLY on callback/future paths, NEVER on void path.)
//
// Idempotence:
//   Each partition carries a monotonic baseSequence stamped into the batch header.
//   producerId is a local stub for M-B (TODO(milestone-c): real InitProducerId).
//
// acks=None: the batch is sent without requesting a ProduceResponse.  The void
//   send fast path with acks=None is fully alloc-free end-to-end.
//
// Usage:
//   auto topic = client.topic("orders");          // returns TopicHandle
//   auto p     = client.producer({.acks=Acks::Leader, .lingerUs=1000});
//   p.send(topic, Key(orderId), valueSpan);       // fast path: zero alloc
//   auto f = p.send(topic, Key(orderId), val, want_future); // opt-in ack

#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <span>

namespace rune::client {

// Forward-declare the implementation class (PIMPL — impl lives in Producer.cpp).
namespace producer { class ProducerImpl; }

// ---------------------------------------------------------------------------
// Acks — acknowledgement policy for produced batches.
// ---------------------------------------------------------------------------
enum class Acks : uint8_t {
    None   = 0,  // send without waiting for a response (fully fire-and-forget)
    Leader = 1,  // wait for ProduceResponse from the leader (default)
    All    = 2,  // wait for all replicas (treated as Leader for M-B)
};

// ---------------------------------------------------------------------------
// ProducerConfig — tuning knobs; sane defaults.
// ---------------------------------------------------------------------------
struct ProducerConfig {
    Acks    acks        = Acks::Leader;
    int64_t lingerUs    = 5'000;      // max time to accumulate a batch (µs)
    int32_t batchBytes  = 64 * 1024;  // flush when a partition's builder reaches this
    int32_t maxInFlight = 4;          // max outstanding un-acked batches per partition
};

// ---------------------------------------------------------------------------
// want_future_t — tag type for the future-returning send() overload.
// ---------------------------------------------------------------------------
struct want_future_t {};
inline constexpr want_future_t want_future{};

// ---------------------------------------------------------------------------
// Producer
//
// Returned by Client::producer(config).  Owns a shared_ptr to ProducerImpl so
// it is cheap to copy, and copyable into lambdas etc.
// ---------------------------------------------------------------------------
class Producer {
public:
    // Constructed by Client::producer() — not intended for direct construction.
    explicit Producer(std::shared_ptr<producer::ProducerImpl> impl) noexcept;

    // Copyable, movable (shared_ptr semantics).
    Producer(const Producer&) = default;
    Producer& operator=(const Producer&) = default;
    Producer(Producer&&) = default;
    Producer& operator=(Producer&&) = default;

    ~Producer() = default;

    // ---- Hot path — void send (ZERO per-send heap allocation) ---------------

    /// Fire-and-forget send.
    /// Computes the partition from Key via partitionForKey (shared FNV-1a),
    /// then writes a compact SEND_RECORD into the agent ring.
    /// NO heap allocation, NO lock, NO std::function/promise.
    void send(TopicHandle            topic,
              const Key&             key,
              std::span<const uint8_t> value) noexcept;

    // ---- Opt-in ack paths (may allocate per send) ----------------------------

    /// Send with callback.  The callback fires on the agent thread with the
    /// broker's baseOffset when the ProduceResponse arrives.
    /// Allocates a correlationId entry in the promise map (control-plane).
    void send(TopicHandle              topic,
              const Key&               key,
              std::span<const uint8_t> value,
              std::function<void(int64_t /*baseOffset*/)> callback);

    /// Send with future.  Returns a future that resolves with the broker's
    /// baseOffset (or throws on error / timeout).
    /// Allocates a std::promise entry in the promise map (control-plane).
    std::future<int64_t> send(TopicHandle              topic,
                               const Key&               key,
                               std::span<const uint8_t> value,
                               want_future_t);

    // ---- Control -------------------------------------------------------------

    /// Force-flush all pending batches immediately (sets the flush flag; the
    /// agent thread does the actual encoding on its next doWork cycle).
    void flush() noexcept;

private:
    std::shared_ptr<producer::ProducerImpl> impl_;
};

} // namespace rune::client
