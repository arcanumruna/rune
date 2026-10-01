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
//     batch, failed offsets, a timed-out commit / abort): commit completes
//     with that error; the app MUST abortTransaction().
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
// Copies share one impl (shared_ptr).  The status accessors (producerId /
// producerEpoch / transactionActive) drain ONE status channel: call them
// from one app thread (or a duty-cycle callback, which reads the live state).
// A handle may outlive its Client: after close every op returns Closed.
// ---------------------------------------------------------------------------
class TransactionalProducer {
public:
    static constexpr std::chrono::nanoseconds kDefaultWaitTimeout = std::chrono::seconds(60);

    /// Constructed by Client::transactionalProducer() (tests: their own
    /// sinks).  `sink` carries the no-completion commands (begin / send),
    /// `opSink` the token ops, `waitSink` the *AndWait sugar; `dutyThread` is
    /// the Thread-mode runner (calls from it read the duty state directly).
    TransactionalProducer(std::shared_ptr<producer::TransactionalProducerImpl> impl,
                          detail::HubCommandSink sink, void* sinkCtx,
                          std::thread::id dutyThread = {},
                          detail::TargetedOpSink opSink = nullptr,
                          detail::AndWaitSink waitSink = nullptr) noexcept;

    TransactionalProducer(const TransactionalProducer&) = default;
    TransactionalProducer& operator=(const TransactionalProducer&) = default;
    TransactionalProducer(TransactionalProducer&&) = default;
    TransactionalProducer& operator=(TransactionalProducer&&) = default;
    ~TransactionalProducer() = default;

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

    // ---- *AndWait: cold-path blocking forms (throw std::runtime_error on any
    //      non-Ok outcome or wait timeout; std::logic_error from a callback) ---
    void initTransactionsAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void sendOffsetsToTransactionAndWait(const std::map<TopicPartitionKey, int64_t>& offsets,
                                         const std::string& groupId,
                                         std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void commitTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    void abortTransactionAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);

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

    /// Access raw impl (hub wiring / tests — not user API).
    [[nodiscard]] std::shared_ptr<producer::TransactionalProducerImpl> impl() const noexcept {
        return impl_;
    }

private:
    [[nodiscard]] bool onDutyThread_() const noexcept {
        return dutyThread_ != std::thread::id{} && std::this_thread::get_id() == dutyThread_;
    }
    [[nodiscard]] bool finished_() const noexcept;
    OpResult issue_(detail::HubCommand& cmd, const CompletionTarget& target, Token token, OpKind op);
    void andWait_(const char* what, std::chrono::nanoseconds timeout, detail::AndWaitIssue issue, void* ctx);

    std::shared_ptr<producer::TransactionalProducerImpl> impl_;
    detail::HubCommandSink sink_     = nullptr;
    void*                  sinkCtx_  = nullptr;
    std::thread::id        dutyThread_{};
    detail::TargetedOpSink opSink_   = nullptr;
    detail::AndWaitSink    waitSink_ = nullptr;
};

} // namespace rune::client
