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
//   producerId is assigned by the broker (InitProducerId with an empty transactional id, Phase 2b F FD4):
//   an acked producer HOLDS its records until the id arrives, then stamps (id, epoch) per partition.
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
#include <rune/client/group/GroupMetadata.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace rune::client {

// Forward-declare the implementation class (PIMPL — impl lives in Producer.cpp).
namespace producer { class ProducerImpl; }
class Completions;
class ProducerRef;
class Client;
struct CompletionTarget;

// ---------------------------------------------------------------------------
// Acks — acknowledgement policy for produced batches.
// Leader (like Kafka acks=1): a leader restart or loss before the followers replicate can lose ACKED records.
// All: with the broker's minIsr >= 2 (the multi-node default), acked records survive any single broker failure;
// use it for durability (#41).
// ---------------------------------------------------------------------------
enum class Acks : uint8_t {
    None   = 0,  // send without waiting for a response (fully fire-and-forget)
    Leader = 1,  // wait for ProduceResponse from the leader (default)
    All    = 2,  // wait until the batch is committed by the in-sync replica majority
};

// ---------------------------------------------------------------------------
// TopicPartitionKey - (topicId, partition) pair.
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
// TxnState - Kafka's TransactionManager states (F8c fix round 1), owned by
// the duty thread; app threads see it through Producer::state().
//   Uninitialized -> Initializing -> Ready -> InTransaction
//     -> CommittingTransaction | AbortingTransaction -> Ready
//   AbortableError: the transaction failed (a refused AddPartitions, a lost
//     batch, failed offsets): commit completes with that error; the app MUST
//     abortTransaction().
//   A commit / abort that times out stays Committing / Aborting (in doubt):
//     retry the SAME call (abort is refused while committing).  A transaction
//     with nothing added ends locally (no EndTxn).  Commit's total wait can
//     reach ~2x transactionOpTimeoutMs (data wait + EndTxn's own budget).
//   FatalError: fenced (17 / 18) or InvalidTxnState on EndTxn - terminal:
//     every op completes Fenced, no network op is ever sent again.
// ---------------------------------------------------------------------------
enum class TxnState : uint8_t {
    Uninitialized = 0,
    Initializing,
    Ready,
    InTransaction,
    CommittingTransaction,
    AbortingTransaction,
    AbortableError,
    FatalError,
};

/// One offset of sendOffsetsToTransaction (a span, not a std::map: no allocation at the call).
struct OffsetEntry {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
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
    // F8c fix round 1 (I-4): transactional producers only — the deadline of
    // initTransactions / sendOffsetsToTransaction / commit / abort (Kafka
    // max.block.ms; >= the default transaction timeout).  EndTxn gets a fresh
    // one when it is created (the wait for in-flight data does not consume
    // its retry budget).  Data batches and AddPartitions use produceTimeoutMs.
    int64_t transactionOpTimeoutMs = 60'000;
    int32_t maxFrameBytes    = 65536;   // hard per-FRAME wire cap (≤ this many bytes/frame),
                                         // independent of batchBytes — bounds fragmentation.
    /// Phase 2b F: non-empty = a transactional producer.  A transactional producer has exactly ONE
    /// issuing thread (every send / claim / batch / lifecycle call comes from it; Thread mode) - a
    /// plain producer keeps "any thread" for send / trySend / tryClaim / claimBatch; its Tier-2 batch()
    /// is single-thread (the creating thread, or bindBatchThread()).  acks must be Leader or All (Client::producer throws
    /// std::invalid_argument for acks = None and for an id longer than 255 bytes; ClientShard refuses it).
    std::string transactionalId;
    /// InitProducerId's transactionTimeoutMs for a transactional producer.
    int32_t     transactionTimeoutMs = 60'000;
    /// Tier-2 (BatchBuilder) slot pool, allocated ONCE by the producer's first batch() (or
    /// bindBatchThread()) on its batch thread - a producer that never uses Tier-2 reserves nothing: this many
    /// batches may be built or queued-unsent at a time (a slot returns to the pool when its batch
    /// is encoded and offered, failed or discarded - not at ack).  Exhausted pool =
    /// BackPressured, never an allocation after that first one.  >= 2 x maxInFlight recommended.  Memory (once
    /// Tier-2 is used): tier2Slots x 2 x tier2SlotBytes of address space (touched only as used) - ~8 MiB by
    /// default.  The reserve is per Tier-2-using producer: N per-shard producers that all batch reserve N times
    /// that, so lower tier2Slots (or tier2SlotBytes) when running many of them.
    int32_t tier2Slots     = 64;
    /// Largest Tier-2 batch (RecordBatch header + records) one slot holds; 0 = max(batchBytes,
    /// maxFrameBytes).
    /// BatchBuilder::add() throws std::length_error (MessageTooLarge, non-retryable) for a
    /// record that would take the batch past it.
    int32_t tier2SlotBytes = 0;
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
    NotInTransaction,///< Phase 2b F: a transactional producer outside a transaction (before
                    ///< beginTransaction() / after commit or abort was issued).  Nothing was
                    ///< written, no credit is kept; the blocking send() throws std::logic_error.
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
// An Invoker claim abandoned under its holder (a failed partition during a
// pump, a close) keeps buffer() writable but harmless: the bytes no longer
// belong to any frame; sends resume, and another claim is ClaimHeld until
// this one is committed (false), aborted or destroyed.
// A claim carrying Completions credit is settled on that queue's owner thread.
// Aborting (or destroying) a credit-carrying claim off its queue's owner thread loses that credit: it is counted
// and logged.  An Invoker claim settled off the owner thread is refused (commit() false, the claim stays open) or,
// on abort, logged and left held until the producer closes.
// A claim must not outlive its Producer.  Zero allocation.
// ---------------------------------------------------------------------------
namespace detail {
/// Debug check of a ProducerRef (ProducerRef.h): the handle of `impl` was destroyed while the duty side still holds
/// it.  false for nullptr (a moved-from Producer answers like any such Producer).
[[nodiscard]] bool producerImplDetached(const producer::ProducerImpl* impl) noexcept;

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
    /// #326: the claim was abandoned under its holder (Invoker: a failed partition / a close); cheap, owner thread.
    [[nodiscard]] static bool abandoned(const ClaimOpen& o) noexcept;
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
    /// #326: true when this claim was ABANDONED under its holder (Invoker mode: its partition failed during a pump,
    /// or the producer closed) while it is still unsettled.  buffer() stays writable but harmless, and commit()
    /// will return false; a holder encoding a large record can poll this to stop early.  Cheap (one variant
    /// test plus one load of the producer's claim state).  Owner thread only: off the owner thread, and in Thread
    /// mode (a ring claim released by the Client close is learned through commit() == false), it reports false.
    [[nodiscard]] bool abandoned() const noexcept;
    /// Discard the record: nothing is sent, nothing completes.  No-op unless open.
    /// Off the owner thread of a carried queue the ring slot is still released
    /// but the credit cannot be returned (it is the owner's plain state): the
    /// queue loses that credit (counted and logged) - settle credit-carrying claims on their owner.
    /// An Invoker claim aborted off the owner thread is logged and stays held until the producer closes.
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
    // The same inline move / destructor bodies as Claim (#329); commit / abort share Claim's settle code in Claim.cpp.
    BatchClaim(BatchClaim&& o) noexcept : state_(std::exchange(o.state_, Settled{})) {}
    BatchClaim& operator=(BatchClaim&& o) noexcept {  ///< aborts this claim first if it is open
        if (this != &o) {
            abort();
            state_ = std::exchange(o.state_, Settled{});
        }
        return *this;
    }
    BatchClaim(const BatchClaim&) = delete;
    BatchClaim& operator=(const BatchClaim&) = delete;
    ~BatchClaim() { abort(); }  ///< aborts an open claim

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
    /// #326: see Claim::abandoned().
    [[nodiscard]] bool abandoned() const noexcept;

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
// (the producer ring is many-to-one).  EXCEPT Tier-2: batch() is single-thread per
// producer (the creating thread, or the thread that called bindBatchThread()); a
// batch() from another thread returns a refused builder whose send() is WrongThread.  The Producer must not be destroyed,
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
    /// This producer was created by `client` (Client::producer()).  False for a moved-from handle and for a producer
    /// of another Client.
    [[nodiscard]] bool ownedBy(const Client& client) const noexcept;

    // ---- Max value size (named constant) ------------------------------------

    /// Maximum record size: key bytes + value bytes (backlog B6, user decision
    /// 2026-10-03).  A larger record is REJECTED at append, never silently
    /// dropped: trySend() returns SendResult::MessageTooLarge (non-retriable),
    /// the void send() throws std::length_error, a Tier-2 BatchBuilder::add()
    /// and a transactional Producer::send() throw std::length_error.  One O(1)
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
    /// #324: claimBatch with ONE polled completion for the whole batch (tryClaim(.., cq, token)'s rules: the credit
    /// is reserved at claim time and returned if the claim is aborted, a refused or an abandoned claim never
    /// completes).  The completion {op = Send, value = the batch's BASE offset (its FIRST record's offset), status}
    /// arrives when the RecordBatch holding the n records is acknowledged - a claimBatch lands whole in ONE
    /// RecordBatch (neighbouring sends of an unfenced claim join the same RecordBatch), so that is one ack and
    /// the completion fires exactly once, after the batch's LAST record is covered.  An acked claimBatch also
    /// reserves its n batchAcksTo() units, like the unacked form.  Both modes.
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen,
                                        Completions& cq, Token token) noexcept;
    /// claimBatch whose single completion goes to the onAck() handler (same value rule as above).
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen,
                                        AckToken token) noexcept;
    /// #326: how many Invoker claims were abandoned under their holder (a failed partition, a close) since the
    /// producer was created.  A plain counter owned by the Invoker owner thread: read it there (off the owner
    /// thread, and in Thread mode, where claims are never abandoned, it reads 0).
    [[nodiscard]] uint64_t claimsAbandoned() const noexcept;

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
    ///
    /// Single-thread per producer: the thread that created it (Invoker mode: the owner thread), or
    /// the thread that last called bindBatchThread().  batch() from another thread returns a refused
    /// builder (send() = WrongThread; the batch pool is untouched).  With every pool slot in use
    /// (ProducerConfig::tier2Slots) it returns a refused builder whose send() is BackPressured -
    /// rebuild the whole batch.  The first batch() (or bindBatchThread()) of a producer allocates its slot pool
    /// (ProducerConfig::tier2Slots); after that the Tier-2 path never allocates.  Invoker mode: while a claim
    /// (tryClaim / claimBatch) is open on the producer, send() is refused (BackPressured, the batch stays intact:
    /// retry once the claim is committed or aborted) - like every other append, it waits on the claim.
    BatchBuilder batch(TopicHandle topic, const Key& key);

    /// Rebind the Tier-2 batch thread to the CALLING thread.  Call it before the first batch() from
    /// the new thread; the hand-off itself (e.g. thread start / join) must order the old thread's
    /// last use before this call.  A BatchBuilder still live on the old thread is refused there
    /// (send() = WrongThread); destroying it hands its slot back (Thread mode; an Invoker producer
    /// loses the slot and logs it).
    void bindBatchThread();

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

    // ---- Transactions (Phase 2b F) ------------------------------------------------
    // Only a producer created with ProducerConfig::transactionalId has a
    // lifecycle.  On a plain producer every token form returns
    // OpResult::InvalidArgument (no completion) and every *AndWait /
    // beginTransaction() throws std::logic_error("<op>: producer has no transactionalId").
    // UnknownProducerId (28) inside a transaction (the broker forgot the producer id: retention removed every
    // record of ours, then a failover): while this transaction has not yet written to that partition (no batch of it
    // acked there), the producer restarts that partition's sequences at 0 under the SAME producer id and epoch (a
    // transactional epoch belongs to the coordinator) and the transaction continues.  When this transaction's own
    // acked data on that partition is gone, the transaction ABORTS: the commit completes BrokerError(28), the app
    // aborts and redoes it, and the abort bumps the epoch (InitProducerId) so the NEXT transaction runs under
    // epoch+1 from sequence 0.
    // A transactional producer outside a transaction refuses every produce path
    // with SendResult::NotInTransaction (send / sendTo / blocking claim helpers
    // throw std::logic_error("produce outside a transaction")).

    //
    // Control ops (FD1): Thread mode writes each one to the producer's own ring, FIFO with the records sent
    // before it - a commit contains exactly the records issued ahead of it, a record after it is refused
    // (NotInTransaction).  A full ring is OpResult::BackPressured (the *AndWait forms and beginTransaction()
    // wait for room).  Invoker mode applies the op at once on the owner thread (WrongThread elsewhere): its
    // refusals are synchronous where Thread mode accepts and then completes with the failure.
    // commitTransaction / abortTransaction close the produce gate when they are ISSUED; beginTransaction opens
    // it once its message is written.  One issuing thread per transactional producer (FD3).
    OpResult initTransactions(const CompletionTarget& target, Token token);
    /// Void: a begin the machine refuses (not Ready: still committing / aborting, fenced, closed, init not done) is
    /// only counted; it is surfaced by the NEXT op (that op completes 19 / the fenced status) and by the records
    /// refused behind it.  Call it after the previous commit / abort / init completed.
    /// Call only after the previous commit/abort has completed: in Thread mode `commit(token); begin(); send()` does
    /// NOT pipeline - the ring is FIFO, so the duty side meets the begin while the commit is still running, refuses
    /// it, refuses the record behind it, and the EARLIER commit completes BrokerError(19 InvalidTxnState) with the
    /// machine in AbortableError (pinned by BeginBeforeThePreviousEndCompletesIsRefusedRegression).
    void     beginTransaction();
    /// One sendOffsetsToTransaction runs at a time: a call made while another is still running completes
    /// InvalidArgument at once with the text "a sendOffsetsToTransaction is already in flight on this producer; wait
    /// for its completion" (no queueing; the first call completes normally and the transaction is NOT poisoned) - wait
    /// for the first completion before issuing the next.  An EMPTY offsets list completes Ok locally with no wire
    /// traffic and does not open the transaction on the coordinator (a commit with nothing else in the transaction
    /// then sends no EndTxn).  At most 1024 offsets and a 1..255-byte group id: otherwise the call is
    /// accepted and COMPLETES InvalidArgument.  Only a ring too small to ever carry the message (a tiny producer
    /// ring) is a synchronous OpResult::InvalidArgument with no completion.
    OpResult sendOffsetsToTransaction(std::span<const OffsetEntry> offsets, std::string_view groupId,
                                      const CompletionTarget& target, Token token);
    /// Issue #125 (KIP-447, needs a broker that understands client version 10 to be enforced; an older broker
    /// ignores the member and behaves like the groupId form): the offsets carry the consumer-group member they belong
    /// to (GroupConsumer::groupMetadata()), and the coordinator fences them like an OffsetCommit.  A member of an
    /// older generation or one it does not know - including one joined on ANOTHER Client (the member is bound to its
    /// session: run the producer on the GroupConsumer's Client) - completes BrokerError IllegalGeneration (15) /
    /// UnknownMember (12) and the transaction is AbortableError: abort it, let the consumer rejoin, re-process from
    /// the committed offsets.  generationId < 0 (not joined) sends no member (the groupId form).  A member id longer
    /// than kMaxGroupMemberIdBytes completes InvalidArgument.  Otherwise exactly the groupId form.
    OpResult sendOffsetsToTransaction(std::span<const OffsetEntry> offsets, const GroupMetadata& group,
                                      const CompletionTarget& target, Token token);
    /// Calls that arrive while a commit (abort) is already pending JOIN it: one EndTxn, every call completes with its
    /// result.  At most 4 joiners (kMaxTxnJoiners) per pending op: a further call completes InvalidArgument with the
    /// text "too many concurrent commitTransaction waiters (max 4)" ("... abortTransaction waiters ...").  It is a
    /// capacity refusal, not a state error (BrokerError 19 stays "no transaction to commit / abort").
    OpResult commitTransaction(const CompletionTarget& target, Token token);
    OpResult abortTransaction(const CompletionTarget& target, Token token);
    /// Token form of flush(); the existing void flush() stays.  At most 8 flush calls pending at once
    /// (kMaxTxnFlushCalls): a further one completes InvalidArgument with the text "too many concurrent flush calls
    /// (max 8)".
    OpResult flush(const CompletionTarget& target, Token token);

    static constexpr std::chrono::nanoseconds kDefaultTxnWait = std::chrono::seconds(60);
    void initTransactionsAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
    void sendOffsetsToTransactionAndWait(std::span<const OffsetEntry> offsets, std::string_view groupId,
                                         std::chrono::nanoseconds timeout = kDefaultTxnWait);
    void sendOffsetsToTransactionAndWait(std::span<const OffsetEntry> offsets, const GroupMetadata& group,
                                         std::chrono::nanoseconds timeout = kDefaultTxnWait);
    void commitTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
    void abortTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);
    void flushAndWait(std::chrono::nanoseconds timeout = kDefaultTxnWait);

    /// state() / producerId() / producerEpoch() / transactionActive() drain a single-consumer status
    /// ring (FD9): they are SINGLE-THREAD per producer, on the same bound thread as batch() (the creating thread,
    /// or the one that called bindBatchThread()).  From any other thread they return "not available"
    /// (TxnState::Uninitialized / -1 / -1 / false) and touch nothing.  They report the latest status the duty
    /// side published, so after issuing an op a Thread-mode caller sees the change once the op completed.
    [[nodiscard]] TxnState state() const noexcept;
    [[nodiscard]] int64_t  producerId() const noexcept;  ///< -1 until assigned, after FatalError (a deliberate change: a fenced producer has no id), or off the bound thread
    [[nodiscard]] int16_t  producerEpoch() const noexcept;
    [[nodiscard]] bool     transactionActive() const noexcept;
    [[nodiscard]] bool     transactional() const noexcept;

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
    /// (owner) thread holds once the close is issued - its commit() then returns
    /// false (a refused close leaves it open).  Thread mode: a
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
    /// Stored frames offered again (retransmits after a timeout / reconnect / redirect); same threading as framesSent().
    uint64_t framesResent() const noexcept;

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
    friend class ProducerRef;   // reads implForRef_() (FD7 debug check)
    [[nodiscard]] const producer::ProducerImpl* implForRef_() const noexcept { return impl_; }
    /// Phase 2b F Task 8: issue one transaction control op (resolves and reserves the completion target).
    OpResult issueTxn_(std::uint8_t kind, const CompletionTarget& target, Token token,
                       std::span<const OffsetEntry> offsets, std::string_view groupId,
                       int32_t generationId = -1, std::string_view memberId = {});
    /// The *AndWait body: issue + wait + map the completion to an exception.
    void txnAndWait_(const char* what, std::chrono::nanoseconds timeout, std::uint8_t kind,
                     std::span<const OffsetEntry> offsets, std::string_view groupId,
                     int32_t generationId = -1, std::string_view memberId = {});
    /// T15b M3: the one send body; `part` computed once by the caller.
    [[nodiscard]] SendResult trySendPart_(TopicHandle topic, int32_t part, const Key& key,
                                          std::span<const uint8_t> value) noexcept;
    /// 2b-B: the one claim body; `part` computed once by the caller, the
    /// per-record ack credit (cq) already reserved by the caller.
    [[nodiscard]] Claim claimPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t valueLen,
                                   uint64_t ackHandle, Completions* cq) noexcept;
    /// #324: the claimBatch body; `part` computed once by the caller, `cq` (the per-batch completion credit) already
    /// reserved by the caller, `ackHandle` 0 = no completion.
    [[nodiscard]] BatchClaim claimBatchPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t n,
                                             uint32_t recLen, uint64_t ackHandle, Completions* cq) noexcept;
    /// F8c C7: the §3.3 release of the handle's share (see the destructor).
    void release_() noexcept;

    producer::ProducerImpl* impl_ = nullptr;  // the handle's share (§3.3): ONE load per send
};

} // namespace rune::client
