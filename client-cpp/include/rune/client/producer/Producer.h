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
//   producerId is a per-instance client-drawn random id for M-B (F5 fix — see
//   ProducerImpl.h/.cpp's generateProducerId()); TODO(milestone-c): real
//   broker-allocated InitProducerId + zombie-fencing.
//
// acks=None: the batch is sent without requesting a ProduceResponse.  The void
//   send fast path with acks=None is fully alloc-free end-to-end.  Nothing
//   tracks or retransmits an acks=None batch, so it is ALWAYS encoded as
//   NON-idempotent: producerId=-1 (the wire sentinel that tells the broker to
//   skip all sequence validation for this batch) with producerEpoch and
//   baseSequence left at -1 too (RecordBatchBuilder's own "unset" defaults —
//   see RecordBatch.h).  This is the documented convention for "no
//   idempotence" throughout this client: -1/-1/-1.  A partition's real
//   (producerId_, producerEpoch_, nextBaseSequence) idempotent state is never
//   read, stamped, or advanced for an acks=None batch, so switching acks back
//   to Leader/All later resumes cleanly from wherever the idempotent sequence
//   last left off.
//
// Usage:
//   auto topic = client.topic("orders");          // returns TopicHandle
//   auto p     = client.producer({.acks=Acks::Leader, .lingerUs=1000});
//   p.send(topic, Key(orderId), valueSpan);       // fast path: zero alloc
//   auto f = p.send(topic, Key(orderId), val, want_future); // opt-in ack

#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/BatchBuilder.h>

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
    Acks    acks           = Acks::Leader;
    int64_t lingerUs       = 5'000;      // max time to accumulate a batch (µs)
    int32_t batchBytes     = 64 * 1024;  // flush when a partition's builder reaches this
    int32_t batchRecords   = 0;          // flush when a partition's builder reaches this many records (0=disabled)
    int32_t maxInFlight    = 32;         // max outstanding un-acked batches per partition
                                          // (deep pipeline: throughput = outstanding / RTT;
                                          //  Aeron's µs RTT means a shallow window throttles
                                          //  hard — 32 batches keeps the pipe full while the
                                          //  idempotent seq window / ackRing bound memory)
    int64_t produceTimeoutMs = 5'000;    // timeout for a pending ProduceResponse ack
    int32_t maxFrameBytes    = 65536;   // hard per-FRAME wire cap (≤ this many bytes/frame),
                                         // independent of batchBytes — bounds fragmentation.
    int64_t deliveryTimeoutMs = 30000;  // total time to keep retransmitting before failing.
    int64_t agentLivenessTimeoutMs = 1000; // void send(): back-pressured with no ring-consumer
                                           // heartbeat for this long => throw (runner gone)
    int32_t directDeferredBytes = 256 * 1024; // Invoker (Direct ingress) only: room for sends made
                                              // from callbacks the producer fires mid-duty-work
                                              // (deferred, applied after the walk). Clamped up
                                              // to fit two max-size records. See send()'s docs.
    int32_t directDeferredPartitionBytes = 0; // Invoker only: one partition's max share of that
                                              // arena (0 = min(arena/2, 4 x batchBytes)), so a
                                              // stalled partition cannot exhaust it.

    /// Build a ProducerConfig from environment variables, falling back to
    /// sane defaults for any variable that is not set.
    ///
    ///   RUNE_LINGER_US            → lingerUs
    ///   RUNE_BATCH_BYTES          → batchBytes
    ///   RUNE_BATCH_RECORDS        → batchRecords
    ///   RUNE_MAX_FRAME_BYTES      → maxFrameBytes
    ///   RUNE_DELIVERY_TIMEOUT_MS  → deliveryTimeoutMs
    static ProducerConfig fromEnv() noexcept;
};

// ---------------------------------------------------------------------------
// want_future_t — tag type for the future-returning send() overload.
// ---------------------------------------------------------------------------
struct want_future_t {};
inline constexpr want_future_t want_future{};

// ---------------------------------------------------------------------------
// BatchAck — one acknowledged (or failed) produce batch (spec §4.3).  Records of
// the batch are baseSequence .. baseSequence+recordCount-1 of this producer's
// per-partition idempotent sequence and landed at baseOffset .. +recordCount-1.
//
// acks=None: no response is awaited; a BatchAck{errorCode=0, baseOffset=-1,
// baseSequence=-1} is reported when the batch is handed to the transport
// ("sent, offset unknown") — baseSequence=-1 reflects that the batch was
// encoded non-idempotent (see the "acks=None" note above) and never occupied
// a slot in the partition's real idempotent sequence.
//
// Failure of a sequenced batch (delivery timeout, redirect exhausted, close,
// broker rejection) bumps the producer epoch and restarts every partition's
// sequence at 0, so the failed batch never leaves a permanent sequence gap.
// ---------------------------------------------------------------------------
struct BatchAck {
    static constexpr int16_t kClientFailure = -1;  ///< delivery timeout / redirect exhausted / closed
    int32_t topicId      = 0;
    int32_t partition    = 0;
    int64_t baseOffset   = -1;  ///< -1 unless errorCode == 0
    int32_t baseSequence = 0;
    int32_t recordCount  = 0;
    int16_t errorCode    = 0;   ///< 0 OK; >0 broker ErrorCode; kClientFailure
};

// ---------------------------------------------------------------------------
// SendResult — outcome of a non-blocking send (spec §4.3: back-pressure is
// propagated, never a silent drop).
// ---------------------------------------------------------------------------
enum class SendResult : uint8_t {
    Ok = 0,         ///< accepted: will be batched and sent
    BackPressured,  ///< ring full right now: retry after the duty cycle drains
    ValueTooLarge,  ///< value > kMaxSendValueBytes, OR too big to EVER fit this producer's ring
                    ///< (> maxMsgLength) — never retryable
    InvalidTopic,   ///< TopicHandle not valid
    WrongThread,    ///< ClientShard only (later tasks): producer used off its owner thread
    WrongShard,     ///< ClientShard only (later tasks): partition owned by another shard
    Closed,         ///< this producer's Client has been closed (producer outlived it, or
                    ///< send() raced a concurrent close())
};

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

    // ---- Max value size (named constant) ------------------------------------

    /// Maximum value size for a single send().
    ///
    /// The SEND_RECORD ring message is limited to kMaxSendValueBytes bytes of
    /// value data.  Values larger than this are REJECTED, never silently
    /// dropped: trySend() returns SendResult::ValueTooLarge and the void
    /// send() throws std::length_error.
    ///
    /// Rationale: the ring write uses an 8 KiB stack scratch buffer
    /// (kMaxStackBuf = 8192) and the record header is 38 bytes, leaving
    /// 8192 - 38 = 8154 bytes for the value.  Chunking for larger payloads
    /// is left to the caller.
    static constexpr std::size_t kMaxSendValueBytes = 8192 - 38; // 8154 bytes

    // ---- Hot path — void send (ZERO per-send heap allocation) ---------------

    /// Non-blocking send (ZERO per-send heap allocation, lock-free).  Never drops
    /// silently: anything but Ok means the record was NOT accepted.
    ///
    /// fix2 minor: Ok is a point-in-time answer, not a durability guarantee — a
    /// send() racing a concurrent close() can still observe Ok (the record was
    /// written into the ring before Closed became visible) and then have that
    /// record FAILED (an ack-expecting send) or simply DISCARDED (the
    /// fire-and-forget void path: close() does not flush records still queued
    /// in the ring, it only stops draining them) once close() actually runs.
    /// Acks-expecting callers must treat a late close() the same as any other
    /// failure path; the void path has no acknowledgement to lose.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key,
                                     std::span<const uint8_t> value) noexcept;

    /// Fire-and-forget send that NEVER drops.  On BackPressured it retries:
    /// Invoker mode, called on the owner thread outside a callback, pumps the
    /// Client's duty cycle itself (no idle); any OTHER genuinely separate thread
    /// (Thread mode's app thread; a foreign thread) backs off and waits, bounded
    /// by the ring's consumer-liveness heartbeat — a BACKSTOP for a wedged
    /// runner ONLY (fix1 I5): a cleanly closed Client is reported immediately
    /// (see Closed below), never decided by this timer.
    ///
    /// **Callbacks must call trySend(), not this.**  Called from inside a
    /// duty-cycle callback (onConnected/onError, a subscription handler, a
    /// group-consumer callback, an onBatchAck) while back-pressured, this
    /// throws immediately instead of pumping (pumping would recursively
    /// re-enter the agent's own poll on the same thread) — see
    /// std::runtime_error below. Time spent in ANY callback counts against the
    /// next agentLivenessTimeoutMs window for every producer on that Client.
    ///
    /// Throws:
    ///   std::length_error    — value too large (> kMaxSendValueBytes, or too
    ///                          big to ever fit this producer's ring).
    ///   std::invalid_argument — invalid topic handle.
    ///   std::logic_error      — this producer's Client has been closed (the
    ///                          Producer outlived it, or send() raced a
    ///                          concurrent close()) — consistent with
    ///                          Client::producer() after close().
    ///   std::runtime_error    — EITHER back-pressured with pumping unsafe
    ///                          right now (already inside a duty-cycle
    ///                          callback — the common case — or, in Thread
    ///                          mode, the runner thread itself outside its own
    ///                          cycle; use trySend there), OR back-pressured
    ///                          and the runner stopped draining for longer
    ///                          than agentLivenessTimeoutMs (a wedged runner,
    ///                          NOT a closed Client).
    /// Zero allocation when accepted.
    ///
    /// **Invoker mode (Direct ingress, Task 14) — burst room.**  There is no
    /// producer ring: send()/trySend() append straight into the partition's
    /// builder on the owner thread, and the records of one duty-cycle pass leave
    /// at the end of the pass.  The room for a burst between two doWork() calls
    /// is therefore ONE builder per partition (batchBytes, and batchRecords if
    /// set) — a full builder reports BackPressured even when the in-flight
    /// window has room (void send() then pumps one duty cycle and retries).
    /// Sends made from a callback the producer fires inside its OWN duty work
    /// (an ack callback, onBatchAck, a delivery-timeout failure) are DEFERRED
    /// into a preallocated owner-thread arena (ProducerConfig::
    /// directDeferredBytes) and applied right after that work, in submission
    /// order PER PARTITION.  A partition that cannot take them yet (window full,
    /// leader down) keeps its own deferred records — later sends to THAT
    /// partition report BackPressured until they are applied — while every
    /// other partition proceeds normally.  One partition may hold at most
    /// directDeferredPartitionBytes of the arena.  When its share (or the
    /// arena) is full: trySend() returns BackPressured, a void send() inside
    /// the duty cycle throws std::runtime_error, and the callback/future
    /// overloads fail with a distinct "deferred buffer full" reason.  A deferred
    /// record has the normal delivery deadline (deliveryTimeoutMs from
    /// acceptance; a void one is counted + logged), and flush() covers it.  Only
    /// the owner thread may send (others get WrongThread).
    ///
    /// **Ordering vs a Tier-2 batch (both modes).**  A record is never placed
    /// ahead of a Tier-2 batch (Producer::batch) queued earlier on the same
    /// partition: it waits until that batch is sent.  In Thread mode (Ring
    /// ingress) a waiting record is the head of the producer's ring, so the
    /// whole ring drain pauses until the batch is sent (same as a record behind
    /// a full builder at maxInFlight); sends keep accumulating in the ring and
    /// back-pressure once it is full.  In Invoker mode only that partition's
    /// sends report BackPressured.
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);

    // ---- Opt-in ack paths (may allocate per send) ----------------------------

    /// Send with callback.  The callback fires on the agent thread with the
    /// broker's baseOffset when the ProduceResponse arrives.
    /// acks=None: fires with -1 once the record's batch is handed to the transport.
    /// Under acks=None that -1 carries NO success/failure information (nothing is
    /// acknowledged); use onBatchAck() and its errorCode for per-batch outcome.
    /// Allocates one PendingAck whose ownership travels to the duty-cycle thread
    /// inside the SEND_RECORD (no map, no lock).
    void send(TopicHandle              topic,
              const Key&               key,
              std::span<const uint8_t> value,
              std::function<void(int64_t /*baseOffset*/)> callback);

    /// Send with future.  Returns a future that resolves with the broker's
    /// baseOffset (or throws on error / timeout).
    /// acks=None: resolves with -1 once the record's batch is handed to the
    /// transport — no success/failure information (see onBatchAck()'s errorCode).  Allocates one PendingAck (ownership travels through the ring).
    std::future<int64_t> send(TopicHandle              topic,
                               const Key&               key,
                               std::span<const uint8_t> value,
                               want_future_t);

    /// Per-batch acknowledgement callback (one call per batch, not per record).
    /// Setup-phase: call before the first send(); fires on the duty-cycle thread
    /// (the runner in thread mode, the app thread inside doWork() in invoker mode).
    void onBatchAck(std::function<void(const BatchAck&)> cb);

    // ---- Tier-2: explicit atomic batch ---------------------------------------

    /// Create a BatchBuilder bound to the partition resolved from Key.
    ///
    /// The returned BatchBuilder accumulates records via add() and flushes them
    /// all as ONE RecordBatch (one partition, one atomic log append) via send().
    ///
    /// The batch does NOT merge into the Tier-1 auto-accumulator; it is always
    /// flushed as its own independent RecordBatch.  Ordering with Tier-1 batches
    /// for the same partition follows submission order (both share the same ring).
    ///
    /// Idempotent-seq (baseSequence) is shared with Tier-1 and advances
    /// monotonically across both tiers for the same partition.
    BatchBuilder batch(TopicHandle topic, const Key& key);

    // ---- Control -------------------------------------------------------------

    /// Force-flush all pending batches immediately (sets the flush flag; the
    /// agent thread does the actual encoding on its next doWork cycle).
    void flush() noexcept;

    // ---- Instrumentation counters (agent-thread written, caller-read after sync)
    //
    // These counters are updated on the single-writer agent thread.  They are
    // plain uint64_t (no atomics) because the producer is single-writer.
    // The caller must ensure the agent has finished its work (e.g. via flush()
    // + pump) before reading these from a different thread.

    /// Total Aeron frames offered to transport since this producer was created.
    uint64_t framesSent() const noexcept;

    /// Total records accepted by send() / batch-add since this producer was
    /// created.
    uint64_t recordsSent() const noexcept;

    /// Test hook: user ack-callback / onBatchAck exceptions caught on the
    /// duty-cycle thread (see ProducerImpl::testCallbackExceptionCount() for
    /// the read-safety contract: the owner thread in Invoker mode; after
    /// close() in Thread mode).
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept;

private:
    std::shared_ptr<producer::ProducerImpl> impl_;
};

} // namespace rune::client
