// include/rune/client/producer/TransactionalProducer.h
//
// TX-5 — Tier-3 Transactional Producer API.
//
// Threading model (F8c C5 — shared-nothing; no atomics, no mutex, no future):
//   All transactional state (producerId, epoch, transaction-in-progress flag,
//   per-partition sequences, pending requests) is owned by the DUTY thread
//   (TransactionalProducerImpl).  This handle hands every operation to it as a
//   command on the agent command ring (or applies it directly when called on
//   the duty thread: the Invoker owner, a Thread-mode runner callback).
//   Results come back as completion tokens (rune/client/Completion.h): into a
//   Completions the calling thread owns and polls, or to a CompletionHandler
//   called on the duty thread.  Exactly-once: an op either returns a
//   non-Accepted OpResult (no completion ever) or completes exactly once.
//
// Idempotent-by-default (implemented):
//   Once InitProducerIdResponse arrives the producer has a real (producerId,
//   epoch).  Thereafter every send stamps a monotonically increasing
//   baseSequence per (topic, partition) and is retained in flight; a retriable
//   produce error re-sends the SAME frame (same baseSequence) so the broker
//   deduplicates.  OutOfOrderSequence / InvalidProducerEpoch / ProducerFenced
//   fence the producer: pending ops complete OpStatus::Fenced and the app MUST
//   initTransactions() again.
//
// Tier-3 transactional lifecycle:
//   initTransactions(target, t)     -> InitProducerId(transactionalId)
//   beginTransaction()              -> the transaction is active
//   send(topic, key, value)         -> the first send to a partition emits
//                                      AddPartitionsToTxn; its records are
//                                      produced only once that is OK (F14)
//   sendOffsetsToTransaction(...)   -> AddOffsetsToTxn, then TxnOffsetCommit
//   commitTransaction(target, t)    -> EndTxn(commit) once every record is acked
//   abortTransaction(target, t)     -> EndTxn(abort)
//
// Coordinator errors (F14): ConcurrentTransactions (27) is retried after
// min(2^attempt, cap) duty cycles; NotCoordinator (14) re-discovers the
// coordinator and then retries; EndTxn is retried until 0 / 19 / 17 / 18.  The
// only time bound is the op's deadline (ProducerConfig::produceTimeoutMs of
// duty time -> OpStatus::Timeout).
//
// Usage:
//   auto tp = client.transactionalProducer("my-txn-id", {.acks=Acks::All});
//   tp.initTransactionsAndWait();
//   tp.beginTransaction();
//   tp.send(topic, key, value);
//   tp.commitTransactionAndWait();

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/Completions.h>             // CompletionTarget
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/producer/Producer.h>       // Acks, ProducerConfig

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <thread>

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
// TxnState — Kafka's TransactionManager states (F8c fix round 1), owned by
// the duty thread; app threads see it through TransactionalProducer::state().
//   Uninitialized -> Initializing -> Ready -> InTransaction
//     -> CommittingTransaction | AbortingTransaction -> Ready
//   AbortableError: the transaction failed (a refused AddPartitions, a lost
//     batch, failed offsets): commit completes with that error; the app MUST
//     abortTransaction().
//   A commit / abort that times out stays Committing / Aborting (in doubt):
//     retry the SAME call (abort is refused while committing).  A transaction
//     with nothing added ends locally (no EndTxn).  Commit's total wait can
//     reach ~2x transactionOpTimeoutMs (data wait + EndTxn's own budget).
//   FatalError: fenced (17 / 18) or InvalidTxnState on EndTxn — terminal:
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

// ---------------------------------------------------------------------------
// TransactionalProducer — user-facing handle for the Tier-3 transactional API.
//
// F8c C7: MOVE-ONLY.  The handle holds its share of the impl (raw — no
// shared_ptr); the duty side (the Client) holds the other (spec §3.3).  The
// status accessors (producerId / producerEpoch / transactionActive) drain ONE
// status channel: call them from one app thread (or a duty-cycle callback,
// which reads the live state).  A handle may outlive its Client: after close
// every op returns Closed.
//
// Shutdown (user decision 2): close() aborts an open broker transaction
// (EndTxn(abort)) before it reports closed; closeImmediately() discards
// locally (pending ops complete Closed; an open broker transaction is left to
// the coordinator's transaction timeout); the destructor = closeImmediately()
// unless a close already started (that close then completes on the duty side).
// ---------------------------------------------------------------------------
class TransactionalProducer {
public:
    static constexpr std::chrono::nanoseconds kDefaultWaitTimeout = std::chrono::seconds(60);

    /// Constructed by Client::transactionalProducer() (tests: their own
    /// sinks).  `sink` carries the no-completion commands (begin / send),
    /// `opSink` the token ops, `waitSink` the *AndWait sugar; `dutyThread` is
    /// the Thread-mode runner (calls from it read the duty state directly).
    TransactionalProducer(producer::TransactionalProducerImpl* impl,
                          detail::HubCommandSink sink, void* sinkCtx,
                          std::thread::id dutyThread = {},
                          detail::TargetedOpSink opSink = nullptr,
                          detail::AndWaitSink waitSink = nullptr) noexcept;

    // Move-only (F8c C7).
    TransactionalProducer(const TransactionalProducer&) = delete;
    TransactionalProducer& operator=(const TransactionalProducer&) = delete;
    TransactionalProducer(TransactionalProducer&& o) noexcept;
    TransactionalProducer& operator=(TransactionalProducer&& o) noexcept;

    /// F8c C7: closeImmediately() (unless a close already started) + the §3.3
    /// release.  Never throws, never waits; routed like ~Producer (the duty
    /// thread applies it, any other thread uses the command ring — orphaned
    /// when the ring is full, applied by the duty side in FIFO order).
    ~TransactionalProducer();

    // ---- Token ops (exactly one completion after Accepted) --------------------
    // Synchronous rejections (no completion): Closed (the Client / producer is
    // closed), InvalidArgument (token > kMaxToken, no target, a Completions of
    // another Client), WrongThread (Invoker off the owner, a Completions off
    // its owner), QueueFull, BackPressured (command ring full).
    // Completion statuses: Ok, BrokerError (errorCode, payload = text),
    // Fenced, Timeout (transactionOpTimeoutMs of duty time), Closed; a commit
    // whose records were not all durably acked: DeliveryFailed.
    // State gating (Kafka TransactionManager; see TxnState): init only when
    // Uninitialized; sendOffsets / commit only InTransaction; abort in
    // InTransaction or AbortableError.  A commit in AbortableError completes
    // with the transaction's error; any other misuse completes
    // BrokerError(19 InvalidTxnState) — a client-side verdict reported with
    // the broker's code for it.  In FatalError every op completes Fenced.
    // One issuing thread per producer (Thread mode: an app thread's ops travel
    // the command ring, a runner callback's apply at once — mixing them can
    // reorder a commit ahead of a queued send).

    /// InitProducerId.  Ok: value = producerId, aux = producerEpoch.
    OpResult initTransactions(CompletionTarget target, Token token);
    /// sendOffsetsToTransaction: AddOffsetsToTxn then TxnOffsetCommit.
    OpResult sendOffsetsToTransaction(const std::map<TopicPartitionKey, int64_t>& offsets,
                                      const std::string& groupId, CompletionTarget target, Token token);
    /// EndTxn(commit) once every record of the transaction is acked.
    OpResult commitTransaction(CompletionTarget target, Token token);
    /// EndTxn(abort).
    OpResult abortTransaction(CompletionTarget target, Token token);
    /// Kafka flush(): completes Ok once every record sent so far is produced and acked (the transaction stays
    /// open); with the transaction's failure status if it failed meanwhile.  Before an abort it makes the
    /// aborted records reach the log (an abort drops records not yet produced).
    OpResult flush(CompletionTarget target, Token token);

    // ---- *AndWait: cold-path blocking forms (throw std::runtime_error on any
    //      non-Ok outcome or wait timeout; std::logic_error from a callback) ---
    void initTransactionsAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void sendOffsetsToTransactionAndWait(const std::map<TopicPartitionKey, int64_t>& offsets,
                                         const std::string& groupId,
                                         std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void commitTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void abortTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void flushAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);

    // ---- Close (F8c C7) ---------------------------------------------------------

    /// Begin-close (token op, op = OpKind::TxnClose).  The duty side first
    /// aborts an open broker transaction (EndTxn(abort); one that never
    /// started ends locally), waits for a commit / abort / init already
    /// running, then completes every remaining op Closed and the token:
    ///   Ok        closed (nothing was open, or the abort succeeded);
    ///   Timeout / Fenced / BrokerError  the abort failed (or a commit was
    ///             left in doubt: Timeout) — the producer is closed anyway;
    ///   Closed    closeImmediately() / Client close intervened, or a second
    ///             close.
    /// Ops issued after this return Closed (handle-side), and any that were
    /// still queued complete Closed.
    OpResult close(CompletionTarget target, Token token);
    /// Blocking close: begin-close + wait (Invoker owner pumps doWork(); a
    /// Thread-mode app thread idle-polls; from a duty-cycle callback:
    /// std::logic_error).  Returns the token's status; Closed at once when
    /// the producer is already closed.
    OpStatus close();
    /// close() with a bound on the wait (std::runtime_error on wait timeout).
    OpStatus closeAndWait(std::chrono::nanoseconds timeout);
    /// Discard locally now: pending ops complete Closed, a pending close token
    /// completes Closed, records not yet produced are dropped (no EndTxn).
    /// Idempotent.  Accepted, or Closed (already closed / Client closed).
    OpResult closeImmediately();

    // ---- No-completion commands ------------------------------------------------

    /// Start a transaction (a command for the duty thread).  Applied only in
    /// Ready; otherwise ignored (the following sends are dropped and the
    /// commit completes BrokerError(19)).  A no-op once closed.
    void beginTransaction();

    /// Transactional send (a command for the duty thread).  The first send to
    /// a partition within a transaction emits AddPartitionsToTxn; the record
    /// is produced once that is OK.  Outside InTransaction the record is
    /// dropped (never produced: no frame, no transactional bit).  Throws std::invalid_argument (invalid
    /// topic), std::logic_error("client closed") once closed, and — like a
    /// registration — std::runtime_error / std::logic_error when the command
    /// ring stays full (or is full inside a callback).
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value);

    // ---- Accessors (eventually consistent; see the class comment) -------------

    /// Current producerId (from InitProducerIdResponse).  -1 if not acquired.
    [[nodiscard]] int64_t producerId() const noexcept;
    /// Current producerEpoch.  0 if not acquired.
    [[nodiscard]] int16_t producerEpoch() const noexcept;
    /// True while a transaction is open on the broker side: InTransaction,
    /// Committing, Aborting or AbortableError (until EndTxn's final answer).
    [[nodiscard]] bool transactionActive() const noexcept;
    /// The transaction state machine's state (eventually consistent).
    [[nodiscard]] TxnState state() const noexcept;

    /// Access raw impl (hub wiring / tests — not user API; borrowed).
    [[nodiscard]] producer::TransactionalProducerImpl* impl() const noexcept { return impl_; }

private:
    [[nodiscard]] bool onDutyThread_() const noexcept {
        return dutyThread_ != std::thread::id{} && std::this_thread::get_id() == dutyThread_;
    }
    [[nodiscard]] bool finished_() const noexcept;
    OpResult issue_(detail::HubCommand& cmd, const CompletionTarget& target, Token token, OpKind op);
    void andWait_(const char* what, std::chrono::nanoseconds timeout, detail::AndWaitIssue issue, void* ctx);
    /// F8c C7: the §3.3 release of the handle's share (see the destructor).
    void release_() noexcept;

    producer::TransactionalProducerImpl* impl_ = nullptr;  // the handle's share (§3.3)
    detail::HubCommandSink sink_     = nullptr;
    void*                  sinkCtx_  = nullptr;
    std::thread::id        dutyThread_{};
    detail::TargetedOpSink opSink_   = nullptr;
    detail::AndWaitSink    waitSink_ = nullptr;
    bool                   closeIssued_ = false;  // handle-local: close() / closeImmediately() was called
};

} // namespace rune::client
