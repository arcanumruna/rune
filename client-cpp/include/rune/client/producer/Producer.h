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
// Acks (F8c C6 — no futures, no per-send heap on ANY path):
//   void send(...) / trySend(...)          — fire-and-forget; no ack tracking.
//   trySend(..., Completions& cq, Token t) — one Completion{op=Send} into `cq`
//                                            (credit reserved at issue).
//   trySend(..., AckToken{t})              — the producer's onAck(fn, ctx) handler,
//                                            called on the duty thread.
//   The token travels in the record's 64-bit ack slot (no PendingAck, no
//   std::function, no allocation): exactly one completion per accepted send.
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
//   Completions cq(client);
//   p.trySend(topic, Key(orderId), val, cq, token); // opt-in ack: zero alloc
//   cq.poll([](const Completion& c, auto) { /* c.token, c.value = offset */ });
//   auto c = p.tryClaim(topic, Key(id), PositionEncoder::kEncodedLength);  // zero-copy (2b)
//   if (c) { PositionEncoder(c.buffer()).zone(4).x(1.5); c.commit(); }

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/BatchBuilder.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <variant>

namespace rune::client {

// Forward-declare the implementation class (PIMPL — impl lives in Producer.cpp).
namespace producer { class ProducerImpl; }
class Completions;
struct CompletionTarget;

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
                                          //  idempotent seq window / ackRing bound memory).
                                          // EOS-OOS: acks=Leader/All (idempotent) clamps it to
                                          // rune::protocol::client::kIdempotentMaxInFlightBatches
                                          // (the broker's dedup window; Kafka caps at 5) — logged
                                          // once at producer creation when clamped.
    // Timeout for a pending ProduceResponse ack.  It also bounds the retriable
    // answers: MinIsrViolation and (Phase 2b E2) TransientLogFull (42 — a
    // transient topic's ring is full of not-yet-replicated records) are re-sent
    // after a backoff with the same identity, never failed early.
    int64_t produceTimeoutMs = 5'000;
    // F8c fix round 1 (I-4): TransactionalProducer only — the deadline of
    // initTransactions / sendOffsetsToTransaction / commit / abort (Kafka
    // max.block.ms; >= the default transaction timeout).  EndTxn gets a fresh
    // one when it is created (the wait for in-flight data does not consume
    // its retry budget).  Data batches and AddPartitions use produceTimeoutMs.
    int64_t transactionOpTimeoutMs = 60'000;
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
// F8c C6 — per-record / Tier-2 ack targets (no std::function, no allocation).
// ---------------------------------------------------------------------------

/// The producer's ack handler (Producer::onAck): a plain function pointer +
/// context, called on the DUTY thread (the Thread-mode runner, or the Invoker
/// owner inside doWork(); the closing thread for the Closed completions
/// Client::close() delivers) once per AckToken send / Tier-2 batch:
///   Ok             baseOffset = the record's offset (acks=None: -1, "handed to
///                  the transport, offset unknown"; also -1 when the broker
///                  answered a retransmit as a known-old duplicate — the record
///                  IS in the log, its offset is no longer known: Kafka
///                  DUPLICATE_SEQUENCE parity)
///   BrokerError    errorCode = the broker's ErrorCode, baseOffset = -1
///   DeliveryFailed delivery timeout / redirect budget exhausted / a deferred
///                  record that could not be applied, baseOffset = -1
///   SessionLost    the redirect reconnect failed (no answer possible)
///   Closed         Producer::closeImmediately() / Client::close()
/// It must not block (it runs inside the duty cycle).  `ctx` must outlive
/// every op that targets it.
using AckFn = void (*)(void* ctx, Token token, int64_t baseOffset, OpStatus status,
                       uint16_t errorCode) noexcept;

/// Tag for the onAck-handler target: trySend(topic, key, value, AckToken{t}).
struct AckToken {
    Token token = 0;
};

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
// producerEpoch is the epoch the batch was stamped with (-1: not sequenced), so
// (partition, producerEpoch, baseSequence) names a batch across epoch restarts.
//
// EOS-OOS: errorCode 0 with baseOffset -1 on a sequenced batch = a known-old
// duplicate (the broker had already written it; offset unknown) — a success.
// OutOfOrderSequence on a batch behind an older live batch is not reported: the
// batch is re-sent once the older ones resolve (Kafka parity).
// ---------------------------------------------------------------------------
//
// F8c C6: EVERY accepted record is reported in exactly one BatchAck — including
// records that never reached the wire (failed while still in the producer ring,
// a builder, the Tier-2 queue or the deferred arena: baseSequence = -1,
// errorCode = kClientFailure).  A Tier-2 batch is one BatchAck.
struct BatchAck {
    static constexpr int16_t kClientFailure = -1;  ///< delivery timeout / redirect exhausted / closed
    int32_t topicId      = 0;
    int32_t partition    = 0;
    int64_t baseOffset   = -1;  ///< -1 unless errorCode == 0
    int32_t baseSequence = 0;
    int32_t recordCount  = 0;
    int16_t errorCode    = 0;   ///< 0 OK; >0 broker ErrorCode; kClientFailure
    int16_t producerEpoch = -1; ///< epoch the batch was stamped with; -1 = not sequenced
    /// producerId the batch was stamped with (with producerEpoch and
    /// baseSequence it names the batch, also across an epoch-overflow id
    /// rotation).  Handler form (onBatchAck) only; -1 in the Completions form
    /// (batchAcksTo / batchAckOf: no room in a Completion) and when not sequenced.
    int64_t producerId = -1;
};

/// Per-batch handler (Producer::onBatchAck): called on the duty thread, one
/// call per batch.  Must not block.
using BatchAckFn = void (*)(void* ctx, const BatchAck& ack) noexcept;

/// A BatchAck delivered into a Completions (Producer::batchAcksTo) is a
/// Completion with op == OpKind::BatchAck: token = topicId << 32 | partition,
/// value = baseOffset, aux = baseSequence << 32 | recordCount, errorCode = the
/// BatchAck errorCode (kClientFailure as 0xFFFF), reserved0 = producerEpoch
/// (int16 in the low 16 bits; the credit units are reservedBytes /
/// kBatchAckUnitBytes), status = Ok / BrokerError / DeliveryFailed /
/// SessionLost / Closed.  This decodes it.
[[nodiscard]] inline BatchAck batchAckOf(const Completion& c) noexcept {
    BatchAck a;
    a.topicId      = static_cast<int32_t>(static_cast<uint32_t>(c.token >> 32));
    a.partition    = static_cast<int32_t>(static_cast<uint32_t>(c.token));
    a.baseOffset   = c.value;
    a.baseSequence = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(c.aux) >> 32));
    a.recordCount  = static_cast<int32_t>(static_cast<uint32_t>(c.aux));
    a.errorCode    = static_cast<int16_t>(c.errorCode);
    a.producerEpoch = static_cast<int16_t>(static_cast<uint16_t>(c.reserved0));
    return a;
}

// ---------------------------------------------------------------------------
// SendResult — outcome of a non-blocking send (spec §4.3: back-pressure is
// propagated, never a silent drop).
// ---------------------------------------------------------------------------
enum class SendResult : uint8_t {
    Ok = 0,         ///< accepted: will be batched and sent
    BackPressured,  ///< ring full right now: retry after the duty cycle drains
    ValueTooLarge,  ///< too big to EVER fit this producer's ring (> maxMsgLength; only tiny
                    ///< rings) — never retryable
    InvalidTopic,   ///< TopicHandle not valid
    WrongThread,    ///< ClientShard only (later tasks): producer used off its owner thread
    WrongShard,     ///< ClientShard only (later tasks): partition owned by another shard
    Closed,         ///< this producer's intake is closed: Producer::close() /
                    ///< closeImmediately(), or its Client was closed (producer outlived
                    ///< it, or send() raced a concurrent close())
    QueueFull,      ///< F8c C6: the target Completions has no credit — poll() it, then retry
    InvalidArgument,///< F8c C6: token > kMaxToken, or the Completions is not registered
                    ///< with this producer's Client
    MessageTooLarge,///< backlog B6: key + value > Producer::kMaxRecordBytes (16 KiB) —
                    ///< rejected at append, never retryable (Kafka MESSAGE_TOO_LARGE)
    ClaimHeld,      ///< Phase 2b B, Invoker mode: a claim on this producer is still open —
                    ///< commit() or abort() it first.  Never retryable by waiting (the
                    ///< blocking send() throws std::logic_error).
};

// ---------------------------------------------------------------------------
// Phase 2b Part B — zero-copy produce: Claim (one record) / BatchClaim (n
// records appended as ONE RecordBatch).
//
// The caller writes the record straight into the producer's memory:
//   auto c = p.tryClaim(topic, Key(id), PositionEncoder::kEncodedLength);
//   if (c) { PositionEncoder(c.buffer()).zone(4).x(118.5); c.commit(); }
// Copies left (spec §B): Thread mode 2 (producer ring -> Produce frame ->
// Aeron); Invoker mode 1 (the claim IS the frame's bytes; frame -> Aeron).
//
// RAII + move-only: commit() publishes, abort() discards, the destructor aborts
// an open claim.  A refused claim is false; result() says why (BackPressured:
// retry later; MessageTooLarge / ValueTooLarge / InvalidTopic / InvalidArgument
// / Closed / WrongThread / QueueFull / ClaimHeld exactly as for trySend).  The
// claimed bytes are NOT zeroed (a generated XEncoder zeroes its block).
//
// A held claim blocks its producer: Thread mode — the duty side cannot read
// past it, so never hold one across blocking work and never call a blocking
// send() on the same producer from the thread holding it; Invoker mode — until
// it is settled every other send / claim on the producer returns ClaimHeld and
// its partition is not flushed, and it must be settled on the owner thread.
// A claim carrying Completions credit is settled on that queue's owner thread.
// A claim must not outlive its Producer.  Zero allocation.
// ---------------------------------------------------------------------------
namespace detail {
/// The state of an OPEN claim — exactly what settling it needs.
struct ClaimOpen {
    producer::ProducerImpl* impl  = nullptr;
    int64_t                 token = 0;        ///< Ring: the M2O claim index; Direct: the claim generation
    Completions*            cq    = nullptr;  ///< per-record ack credit (tryClaim(.., cq, token)); nullptr: none
    Completions*            bq    = nullptr;  ///< batch-ack credit (batchAcksTo); nullptr: none
    uint32_t                units = 0;        ///< batch-ack units reserved on bq
};
/// Settles a claim and its Completions credit (a friend of Completions).
struct ClaimCredit {
    /// Publish; true = accepted (its completions follow), false = abandoned
    /// (nothing follows: every reservation is returned).
    static bool commit(const ClaimOpen& o) noexcept;
    /// True when this thread may settle the claim's Completions credit: no
    /// credit is carried, or the caller is that queue's owner thread (the same
    /// check trySend makes, in every build).
    static bool mayCommit(const ClaimOpen& o) noexcept;
    /// Discard; every reservation is returned.
    static void abort(const ClaimOpen& o) noexcept;
};
}  // namespace detail

class Claim {
public:
    // Task 10 (perf): the per-record accessors, the move and the destructor's
    // "is it still open" test are inline — a claim pays ONE out-of-line call to
    // settle (commitOpen_ / abortOpen_), not one per accessor, move and
    // destruction of a moved-from or settled Claim.
    Claim() noexcept = default;  ///< refused: InvalidTopic
    Claim(Claim&& o) noexcept : state_(std::exchange(o.state_, Settled{})) {}
    Claim& operator=(Claim&& o) noexcept {  ///< aborts this claim first if it is open
        if (this != &o) {
            abort();
            state_ = std::exchange(o.state_, Settled{});
        }
        return *this;
    }
    Claim(const Claim&) = delete;
    Claim& operator=(const Claim&) = delete;
    ~Claim() { abort(); }  ///< aborts an open claim

    /// True while open (granted, neither committed nor aborted).
    [[nodiscard]] explicit operator bool() const noexcept { return std::holds_alternative<Open>(state_); }
    /// Ok if the claim was granted; otherwise why it was refused.
    [[nodiscard]] SendResult result() const noexcept {
        if (const auto* r = std::get_if<Refused>(&state_)) return r->why;
        return SendResult::Ok;
    }
    /// The record's value bytes (exactly the claimed length); empty unless open.
    [[nodiscard]] std::span<uint8_t> buffer() const noexcept {
        if (const auto* o = std::get_if<Open>(&state_)) [[likely]] return {o->value, o->len};
        return {};
    }
    /// Publish the record.  True: accepted, exactly like an Ok send (its ack /
    /// BatchAck follows).  False: nothing was open, or the claim was abandoned
    /// under it (Invoker: its partition was failed / the producer closed;
    /// Thread: Client close released it) — nothing is sent, nothing completes.
    /// A claim carrying Completions credit committed off that queue's owner
    /// thread returns false and STAYS OPEN (settle it on the owner thread).
    bool commit() noexcept {
        if (!std::holds_alternative<Open>(state_)) [[unlikely]] return false;
        return commitOpen_();
    }
    /// Discard the record: nothing is sent, nothing completes.  No-op unless open.
    /// Off the owner thread of a carried queue the ring slot is still released
    /// but the credit cannot be returned (it is the owner's plain state): the
    /// queue loses that credit — settle credit-carrying claims on their owner.
    void abort() noexcept {
        if (std::holds_alternative<Open>(state_)) [[unlikely]] abortOpen_();
    }

private:
    friend class Producer;
    bool commitOpen_() noexcept;  ///< commit() of an Open claim
    void abortOpen_() noexcept;   ///< abort() of an Open claim
    struct Refused {
        SendResult why = SendResult::InvalidTopic;
    };
    struct Open {
        detail::ClaimOpen core;
        uint8_t*          value = nullptr;
        uint32_t          len   = 0;
    };
    struct Settled {};
    explicit Claim(SendResult why) noexcept : state_(Refused{why}) {}
    Claim(const detail::ClaimOpen& core, uint8_t* value, uint32_t len) noexcept
        : state_(Open{core, value, len}) {}
    std::variant<Refused, Open, Settled> state_{Refused{}};
};

/// n records of recLen bytes claimed together and appended as ONE RecordBatch
/// (one partition, contiguous offsets, all or nothing) — Producer::claimBatch.
/// An unfenced claim may JOIN an open RecordBatch with neighbouring sends: its
/// n records stay contiguous (nothing foreign between them) but the batch is
/// larger than n.  Call flush() (+ doWork() in Invoker mode) before the claim
/// to fence it into a RecordBatch of its own.  Same RAII / refusal / holding rules as Claim.
class BatchClaim {
public:
    BatchClaim() noexcept = default;  ///< refused: InvalidTopic
    BatchClaim(BatchClaim&& o) noexcept;
    BatchClaim& operator=(BatchClaim&& o) noexcept;  ///< aborts this claim first if it is open
    BatchClaim(const BatchClaim&) = delete;
    BatchClaim& operator=(const BatchClaim&) = delete;
    ~BatchClaim();  ///< aborts an open claim

    [[nodiscard]] explicit operator bool() const noexcept { return std::holds_alternative<Open>(state_); }
    [[nodiscard]] SendResult result() const noexcept;
    /// Record i's value bytes (recordLength() bytes); empty for i >= count() or unless open.
    [[nodiscard]] std::span<uint8_t> record(uint32_t i) const noexcept;
    [[nodiscard]] uint32_t count() const noexcept;         ///< n (0 unless open)
    [[nodiscard]] uint32_t recordLength() const noexcept;  ///< recLen (0 unless open)
    /// Publish all n records contiguously and atomically into one RecordBatch,
    /// acked by the BatchAck(s) covering that RecordBatch (shared with any
    /// neighbouring sends unless fenced by flush()).  False: nothing was open, or the claim was abandoned under it (see Claim).
    bool commit() noexcept;
    void abort() noexcept;

private:
    friend class Producer;
    struct Refused {
        SendResult why = SendResult::InvalidTopic;
    };
    struct Open {
        detail::ClaimOpen core;
        uint8_t*          base    = nullptr;  ///< record 0's value bytes (Ring) / the frame (Direct)
        const uint32_t*   offsets = nullptr;  ///< Direct: record i at base + offsets[i]; nullptr: stride recLen
        uint32_t          n       = 0;
        uint32_t          recLen  = 0;
    };
    struct Settled {};
    explicit BatchClaim(SendResult why) noexcept : state_(Refused{why}) {}
    BatchClaim(const detail::ClaimOpen& core, uint8_t* base, const uint32_t* offsets, uint32_t n,
               uint32_t recLen) noexcept
        : state_(Open{core, base, offsets, n, recLen}) {}
    std::variant<Refused, Open, Settled> state_{Refused{}};
};

// ---------------------------------------------------------------------------
// Producer
//
// Returned by Client::producer(config).  F8c C7: MOVE-ONLY.  The handle holds
// its share of the ProducerImpl (raw — no shared_ptr, no refcount); the duty
// side (the Client's ProducerHub) holds the other (spec §3.3).
//
// Sharing across threads (Thread mode): keep ONE Producer and lend
// `Producer&` to the threads that send — trySend/send are safe concurrently
// (the producer ring is many-to-one).  The Producer must not be destroyed,
// moved from or moved into while any thread may still be using it (the same
// contract as any C++ object).  A BatchBuilder borrows the producer and must
// not outlive it.
//
// Destruction = closeImmediately() (user decision 2: a destructor never
// blocks): every accepted record not yet acknowledged completes Closed,
// exactly once, unless a close is already in progress (close(target, token)
// / close(): that drain continues on the duty side and its token still
// completes).  Call close() first when delivery matters.  The impl itself is
// freed by whichever of the handle / the duty side lets go last (one ring
// word; nobody waits) — never while one of its handlers is running.
// ---------------------------------------------------------------------------
class Producer {
public:
    /// Constructed by Client::producer(): takes the handle's share of `impl`.
    /// A bare impl (no Client hooks — unit rigs) is owned by the handle alone.
    explicit Producer(producer::ProducerImpl* impl) noexcept;

    // Move-only (F8c C7).
    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;
    Producer(Producer&& o) noexcept;
    Producer& operator=(Producer&& o) noexcept;

    /// F8c C7: closeImmediately() (unless a close already started) + the §3.3
    /// release.  Never throws.  The release: the duty thread (the Invoker
    /// owner, a Thread-mode runner callback) applies it directly; any other
    /// thread hands it over on the command ring, FIFO behind its own ops; if
    /// the ring is full the release is ORPHANED instead — no wait, no time
    /// bound: the duty side discovers it and applies it in the same FIFO
    /// order (or Client close frees the impl).  Like every handle, it must not
    /// run concurrently with its Client's destruction (the release touches
    /// the Client's command ring); after the Client is gone it never does.
    ~Producer();

    /// The handle owns no producer (moved-from).
    [[nodiscard]] bool valid() const noexcept { return impl_ != nullptr; }

    // ---- Max value size (named constant) ------------------------------------

    /// Maximum record size: key bytes + value bytes (backlog B6, user decision
    /// 2026-10-03).  A larger record is REJECTED at append, never silently
    /// dropped: trySend() returns SendResult::MessageTooLarge (non-retriable),
    /// the void send() throws std::length_error, a Tier-2 BatchBuilder::add()
    /// and TransactionalProducer::send() throw std::length_error.  One O(1)
    /// compare at append — no per-record scan anywhere else.  The broker's
    /// hard limit stays its batch limit (term/8).  Chunking larger payloads is
    /// left to the caller.
    static constexpr std::size_t kMaxRecordBytes = 16 * 1024;

    /// The largest value a send() accepts with ANY key (keys are at most
    /// Key::kMaxInline = 16 bytes): kMaxRecordBytes - 16 = 16368 bytes.  With a
    /// shorter key the value may grow to kMaxRecordBytes - key.size().
    static constexpr std::size_t kMaxSendValueBytes = kMaxRecordBytes - Key::kMaxInline;

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
    ///   std::length_error    — record too large (key + value > kMaxRecordBytes:
    ///                          MessageTooLarge), or too big to ever fit this
    ///                          producer's ring.
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
    /// arena) is full: every trySend() form returns BackPressured and every
    /// blocking send() form (void, AckToken, Completions) inside the duty cycle
    /// throws std::runtime_error — the record is not accepted, so no completion
    /// follows.  A deferred
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

    /// Router forms (T15b M3: a ShardGroup / ClientShard hashes the key ONCE to
    /// route and passes the result down): `partition` is the caller's
    /// key.partition(topic.partitionCount) — it is used as is, never re-hashed.
    /// trySendTo: InvalidArgument for a partition outside [0, partitionCount),
    /// otherwise exactly trySend().  sendTo: exactly send() (std::invalid_argument
    /// for a bad partition).
    [[nodiscard]] SendResult trySendTo(TopicHandle topic, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value) noexcept;
    void sendTo(TopicHandle topic, int32_t partition, const Key& key, std::span<const uint8_t> value);

    // ---- Opt-in ack paths (F8c C6: ZERO per-send allocation) ----------------

    /// Send with a polled completion: exactly one Completion{op = Send,
    /// value = the record's offset (Ok) or -1, status, errorCode} arrives in
    /// `cq` for an accepted (Ok) send; nothing for any other result.  The
    /// record's ack credit is reserved on `cq` first (QueueFull: poll(), then
    /// retry).  `cq` must be registered with this producer's Client
    /// (InvalidArgument otherwise, or for token > kMaxToken) and used on its
    /// owner thread (WrongThread otherwise — checked in every build).  Statuses
    /// as for AckFn.  acks=None: Ok with value -1 when handed to the transport.
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value,
                                     Completions& cq, Token token) noexcept;

    /// Send whose completion goes to the onAck() handler (duty thread).
    /// InvalidArgument for token > kMaxToken.  Without a handler installed the
    /// completion is dropped (the record is still sent and tracked).
    [[nodiscard]] SendResult trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value,
                                     AckToken token) noexcept;

    /// Blocking-on-back-pressure forms (the void send() rules: Invoker owner
    /// pumps, other threads back off; QueueFull is NOT retried here — it
    /// throws std::runtime_error, since only the caller can poll its queue).
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value, Completions& cq,
              Token token);
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value, AckToken token);

    // ---- Phase 2b Part B: zero-copy produce (ZERO allocation) -----------------

    /// Claim one record of `valueLen` value bytes (key + valueLen <=
    /// kMaxRecordBytes), routed by `key` exactly like trySend(topic, key, ..):
    /// write it through claim.buffer() (e.g. a generated XEncoder), then
    /// commit().  Non-blocking: a refused claim is false and claim.result()
    /// says why.  Thread rules as trySend (Invoker: owner thread only).  See
    /// Claim for the rules while it is held.
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen) noexcept;
    /// tryClaim with a polled completion (trySend(.., cq, token)'s rules): the
    /// credit is reserved at claim time and returned if the claim is aborted.
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, Completions& cq,
                                 Token token) noexcept;
    /// tryClaim whose completion goes to the onAck() handler.
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, AckToken token) noexcept;

    /// Claim n records of recLen value bytes each for ONE atomic append (one
    /// RecordBatch on key's partition, contiguous offsets, all or nothing):
    /// write record(i) for every i, then commit().  Limits — never retryable:
    /// n >= 1 (InvalidArgument); key + recLen <= kMaxRecordBytes, the whole
    /// batch within ProducerConfig::maxFrameBytes (a conservative bound) and,
    /// Thread mode, 42 + n x recLen <= the producer ring's maxMsgLength
    /// (MessageTooLarge).  Never ahead of a Tier-2 batch queued earlier on the
    /// partition.  With batchAcksTo bound it reserves n credit units (the
    /// BatchAck(s) covering the claim's RecordBatch return them).  ZERO allocation in steady state.
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) noexcept;

    /// Setup phase (before the first AckToken send): the per-record ack
    /// handler.  See AckFn.
    void onAck(AckFn fn, void* ctx) noexcept;

    /// Setup phase (before the first send): per-batch acknowledgements to a
    /// handler on the duty thread (one call per batch, not per record).
    /// Exclusive with batchAcksTo().  Throws std::logic_error after a send or
    /// when batchAcksTo() was called.
    void onBatchAck(BatchAckFn fn, void* ctx);

    /// Setup phase (before the first send): per-batch acknowledgements into
    /// `cq` as op == OpKind::BatchAck completions (decode with batchAckOf()).
    ///
    /// Credit model (F8c C6, ruled as designed):
    ///   * Once bound, EVERY send on this producer — void and acked, Tier-1 and
    ///     Tier-2 — reserves one 48-byte credit unit on `cq` before it is
    ///     accepted.  So ALL of the producer's sends must be made on `cq`'s
    ///     owner thread (WrongThread otherwise), and a void send can return
    ///     SendResult::QueueFull: poll `cq`, then retry.
    ///   * One BatchAck record returns the units of every record of its batch
    ///     (a Tier-2 batch is one unit).  EVERY accepted record is reported in
    ///     exactly one BatchAck, including records failed before they were sent
    ///     (baseSequence -1, errorCode kClientFailure).
    ///   * Sizing: records accepted but not yet batch-acked AND polled are capped
    ///     at  (capacityBytes - maxRecordBytes) / 48,  where maxRecordBytes =
    ///     8-aligned(48 + max(maxResultBytes, maxErrorText)).  Defaults (1 MiB,
    ///     16 KiB results): (1'048'576 - 16'432) / 48 ≈ 21'500 records.  Size
    ///     capacityBytes >= 48 x (max in-flight records + max unpolled per-record
    ///     acks on the same queue) + maxRecordBytes.  A dedicated batch-ack queue
    ///     is recommended (a BatchAck's token field packs topicId << 32 |
    ///     partition; Completions::waitFor never matches BatchAcks).
    ///   * `cq` must outlive the producer's sends and must not be moved while
    ///     bound (a moved-from queue: InvalidArgument).
    /// Throws std::logic_error after a send, on a second call, when
    /// onBatchAck() was set, or for a queue not registered with this producer's
    /// Client.
    void batchAcksTo(Completions& cq);

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

    /// Force-flush all pending batches: a flush token goes through this
    /// producer's ring; the duty thread flushes every record accepted before it
    /// on its next cycle.  Ring full: an app thread backs off until the token
    /// fits (the duty cycle drains the ring) or the producer closes — bounded by
    /// the ring consumer's heartbeat: stale for > agentLivenessTimeoutMs (a
    /// wedged runner) logs and drops the flush request (records still flush on
    /// linger / batch size).  Called from a duty-cycle callback it never waits
    /// (the next flush pass flushes all).
    void flush() noexcept;

    // ---- Close (F8c C6a) --------------------------------------------------------

    /// Begin-close (non-blocking).  Closes this producer's intake SYNCHRONOUSLY
    /// (the producer ring's tail-bit close, linearizable with every send; Invoker
    /// / Direct mode: owner-local) — once this returns, every later send gets
    /// SendResult::Closed.  The duty side then drains: it consumes the producer
    /// ring up to the closed tail (a record claimed before the close but
    /// committed after it is still read and sent), flushes every batch at once
    /// (no linger), and waits for every accepted record to complete.  The token
    /// then completes (op = ProducerClose):
    ///   Ok      every accepted record completed (with its own status);
    ///   Timeout deliveryTimeoutMs passed first (the remaining records still
    ///           complete, each with its own status);
    ///   Closed  closeImmediately() / Client::close() intervened.
    /// OpResult: Accepted (exactly one completion follows); Closed (the intake
    /// was already closed — a second begin-close, closeImmediately() or Client
    /// close — no completion); QueueFull / InvalidArgument / WrongThread as for
    /// any token op.
    OpResult close(const CompletionTarget& target, Token token);

    /// Invoker mode: a blocking close() / closeAndWait() abandons a claim this
    /// (owner) thread holds - its commit() then returns false.  Thread mode: a
    /// thread holding a claim must settle it BEFORE calling close() (the drain
    /// cannot read past an open ring claim and waits up to deliveryTimeoutMs).
    ///
    /// Blocking close: begin-close + wait for its token (Invoker owner pumps
    /// doWork(); a Thread-mode app thread polls under an idle strategy; from a
    /// duty-cycle callback: std::logic_error).  Returns the token's status (Ok /
    /// Timeout / Closed), or Closed at once when the intake was already closed.
    OpStatus close();
    /// close() with a bound on the wait: throws std::runtime_error when the wait
    /// times out (the drain continues; its records still complete).
    OpStatus closeAndWait(std::chrono::nanoseconds timeout);

    /// Close now: stops the intake and completes everything not yet
    /// acknowledged — in the ring (including records claimed before the close
    /// and committed after it), builders, the Tier-2 queue, the deferred arena,
    /// in flight — with Closed, exactly once; a pending begin-close token
    /// completes Closed.  No flush, no wait (Thread mode: applied by the runner
    /// shortly after).  Idempotent.  Accepted, or Closed (the Client is closed:
    /// its close completes everything), or WrongThread (Invoker, off the owner).
    /// The handle destructor does this (F8c C7) unless a close already started.
    OpResult closeImmediately();

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

    /// Test seams (T15b R3, Ring ingress only): claim a void record in the
    /// producer ring WITHOUT committing it (returns the claim index; < 0: not
    /// claimed), then commit it — the "claimed before the close, committed
    /// after it" window a begin-close must wait for.
    [[nodiscard]] long testClaimRecord(TopicHandle topic, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value) noexcept;
    void testCommitClaim(long claimIndex) noexcept;
    /// Test seam (Invoker owner thread = the duty thread; re-review 1 NEW-2):
    /// set a partition's next baseSequence (ProducerImpl::debugSetNextSequenceForTest)
    /// so an end-to-end test reaches the INT32_MAX epoch move without 2^31 records.
    void testSetNextSequence(TopicHandle topic, int32_t partition, int32_t seq) noexcept;

private:
    /// T15b M3: the one send body; `part` computed once by the caller.
    [[nodiscard]] SendResult trySendPart_(TopicHandle topic, int32_t part, const Key& key,
                                          std::span<const uint8_t> value) noexcept;
    /// 2b-B: the one claim body; `part` computed once by the caller, the
    /// per-record ack credit (cq) already reserved by the caller.
    [[nodiscard]] Claim claimPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t valueLen,
                                   uint64_t ackHandle, Completions* cq) noexcept;
    /// F8c C7: the §3.3 release of the handle's share (see the destructor).
    void release_() noexcept;

    producer::ProducerImpl* impl_ = nullptr;  // the handle's share (§3.3): ONE load per send
};

} // namespace rune::client
