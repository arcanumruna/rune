// include/rune/client/producer/TransactionalProducerImpl.h
//
// TX-5 / F8c C5 / F14 — TransactionalProducerImpl: the DUTY-side state of a
// Tier-3 transactional producer (idempotent-by-default sequencing).
//
// Threading (F8c C5 — shared-nothing; no atomics, no mutex, no future):
//   * EVERY field below is owned by the duty thread (the Thread-mode runner,
//     or the Invoker owner inside doWork()).  App threads never touch it:
//     the TransactionalProducer handle hands each operation to the duty thread
//     as a detail::HubCommand (Txn*) through the agent command ring — or
//     applies it directly when called ON the duty thread — and the duty thread
//     calls applyCommand().  Payloads (a send record, a sendOffsets map) travel
//     as heap holders whose ownership passes with the command.
//   * Results reach the app as completions (rune/client/Completion.h) through
//     the CompletionSink (the Client's completion-queue registry); producer
//     identity / txn state reach app threads only through `status` (a
//     StatusRing the duty thread publishes into; the handle drains it).
//
// F14 (broker F15 design §11) — retriable coordinator errors:
//   * every pending txn request is a retained descriptor TxnRequest
//     {kind, encoded frame, attempt, state}, owned by the duty thread;
//   * ConcurrentTransactions (27) on InitProducerId / AddPartitionsToTxn /
//     AddOffsetsToTxn / TxnOffsetCommit / EndTxn: re-sent after
//     min(2^attempt, kRetryBackoffCapCycles) duty cycles (no wall-clock sleep;
//     the only time bound is the op's own deadline);
//   * NotCoordinator (14): re-discover the coordinator (the Client sends a
//     MetadataRequest), then — once a MetadataResponse arrived — back off and
//     re-send (also while a new coordinator is still loading);
//   * EndTxn is retried until it gets 0 (Ok), 19 (InvalidTxnState ->
//     BrokerError) or 17 / 18 (fenced);
//   * AddPartitionsToTxn errors are no longer ignored: a record for partition p
//     is produced only after p's AddPartitions result is OK; a non-retriable
//     error poisons the transaction (its records are dropped, a commit fails).
//   * A request whose session changed while it was in flight is re-sent on the
//     new session (re-stamped; same correlationId — every txn request is
//     idempotent at the coordinator, produce batches dedup by sequence).
//
// Transaction state machine (F8c fix round 1 — Kafka's TransactionManager,
// rich-state: each state holds its own variables; see TxnState):
//   * every command is gated by the state (begin only in Ready, send /
//     sendOffsets / commit only InTransaction, abort in InTransaction /
//     AbortableError, init only Uninitialized; FatalError -> Fenced);
//   * a poisoned transaction (refused AddPartitions, lost / out-of-order
//     batch, failed offsets) goes AbortableError: commit completes with the
//     error, the app must abort — EndTxn(abort) is sent — and the local
//     transaction state resets ONLY on EndTxn's final answer, never when it
//     is created;
//   * fix round 2: a transaction that never started (no AddPartitions /
//     AddOffsets acknowledged OK) ends locally with no EndTxn (B-1); a commit
//     / abort that times out is IN DOUBT and stays Committing / Aborting — the
//     app retries the same direction, which re-sends EndTxn (B-2, Kafka); a
//     sequence gap (lost batch, OutOfOrderSequence) makes the abort bump the
//     epoch with an InitProducerId after EndTxn(abort) and restart every
//     sequence (B-3, KIP-360 — the client InitProducerId frame carries no
//     pid / epoch, so the bump relies on the coordinator bumping the epoch of
//     an existing transactionalId, which F15 does);
//   * fencing (17 / 18), 19 on EndTxn, and a non-retriable InitProducerId
//     error are FatalError: terminal.
//   Abortable here but fatal in Kafka: 19 / unknown codes on AddPartitions,
//   AddOffsets, TxnOffsetCommit (converges via abort).  A client-synthesized
//   19 (misuse) and the broker's 19 share the code: check state().

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>    // ProducerConfig, Acks
#include <rune/client/producer/TransactionalProducer.h>  // TopicPartitionKey
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/detail/StatusRing.h>

#include <rune/protocol/client/TxnMessages.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace rune::client::producer {

/// A transactional send handed to the duty thread (HubCommand::TxnSend, d).
struct TxnSendRecord {
    int32_t              topicId   = 0;
    int32_t              partition = 0;
    Key                  key;
    std::vector<uint8_t> value;
};

/// sendOffsetsToTransaction()'s payload (HubCommand::TxnSendOffsets, d).
struct TxnOffsetsPayload {
    std::string                                        groupId;
    std::map<rune::client::TopicPartitionKey, int64_t> offsets;
};

/// Duty thread -> app handle snapshot.
struct TxnStatus {
    int64_t producerId    = -1;
    int16_t producerEpoch = 0;
    uint8_t txnActive     = 0;
    uint8_t state         = 0;  // TxnState
    uint8_t pad[4]{};
};

// ---------------------------------------------------------------------------
// TxnPartitionState — per-(topicId, partition) sequencing + the F14 gate.
// ---------------------------------------------------------------------------
struct TxnPartitionState {
    int32_t topicId          = 0;
    int32_t partition        = 0;
    int32_t nextBaseSequence = 0;  // monotonic idempotent seq
    /// F14: in the current transaction.  Records wait until Added.
    enum class Add : uint8_t { NotAdded, Adding, Added, Failed };
    Add                       add = Add::NotAdded;
    std::deque<TxnSendRecord> waiting;  // not yet produced (FIFO)
    // EOS follow-ups (I-1 / M-2): in-flight batches of this partition (clamped
    // to kIdempotentMaxInFlightBatches), those parked on OutOfOrderSequence
    // behind an older one, the highest sequence acked OK, and the producer's
    // send tick when the latest OK was processed.
    uint32_t inFlight     = 0;
    uint32_t parked       = 0;
    int32_t  lastAckedSeq = -1;
    uint64_t lastAckTick  = 0;

    TxnPartitionState() = default;
    TxnPartitionState(int32_t tid, int32_t p) : topicId(tid), partition(p) {}
};

class TransactionalProducerImpl {
public:
    static constexpr int64_t  kNoPid = -1;
    /// F14: the retry backoff cap, in duty cycles.
    static constexpr uint32_t kRetryBackoffCapCycles = 1024;

    using SendFn = std::function<bool(int32_t, const uint8_t*, std::size_t)>;
    /// Asks the Client to re-discover the transaction coordinator (send a
    /// MetadataRequest); onCoordinatorRediscovered() reports the answer.
    using RediscoverFn = void (*)(void* ctx) noexcept;

    explicit TransactionalProducerImpl(
        std::string transactionalId,
        ProducerConfig cfg,
        int64_t& nextCorrId,
        int32_t transactionTimeoutMs = 60'000);

    // F8c C7: freed by whichever of {the handle, the duty side} detaches its
    // share of `status`'s ownership word last (see TransactionalProducer).
    ~TransactionalProducerImpl();

    TransactionalProducerImpl(const TransactionalProducerImpl&) = delete;
    TransactionalProducerImpl& operator=(const TransactionalProducerImpl&) = delete;
    TransactionalProducerImpl(TransactionalProducerImpl&&) = delete;
    TransactionalProducerImpl& operator=(TransactionalProducerImpl&&) = delete;

    // ---- Wiring (before the impl is published to the duty thread) -----------
    void setCompletionSink(detail::CompletionSink sink) noexcept { completions_ = sink; }
    void setRediscoverHook(RediscoverFn fn, void* ctx) noexcept { rediscover_ = fn; rediscoverCtx_ = ctx; }

    // ---- Duty thread ----------------------------------------------------------

    /// Apply one Txn* HubCommand (takes ownership of its payload).  `nowNs`:
    /// the duty clock (an op's deadline starts here).  After failAll() every
    /// command is discarded (its op completes Closed).
    void applyCommand(const detail::HubCommand& cmd, int64_t nowNs);

    /// A Txn* command for a producer the duty side does not hold (or after
    /// close): free its payload, complete its op Closed.
    static void discardCommand(const detail::HubCommand& cmd, const detail::CompletionSink& sink);

    /// True for the Txn* HubCommand ops.
    [[nodiscard]] static bool isTxnOp(detail::HubCommand::Op op) noexcept;

    /// One duty cycle: deadlines, re-sends, AddPartitions, produce, EndTxn.
    /// `sessionId` 0 = not connected (nothing is sent; deadlines still run).
    int doWork(int64_t nowNs, int64_t sessionId, const SendFn& sendFn);

    void onInitProducerIdResponse(const uint8_t* buf, std::size_t len);
    void onAddPartitionsToTxnResponse(const uint8_t* buf, std::size_t len);
    void onAddOffsetsToTxnResponse(const uint8_t* buf, std::size_t len);
    void onTxnOffsetCommitResponse(const uint8_t* buf, std::size_t len);
    void onEndTxnResponse(const uint8_t* buf, std::size_t len);
    /// ProduceResponse for a txn produce: Ok removes the batch; a retriable
    /// error re-sends the SAME frame (same baseSequence) on the next cycle;
    /// OutOfOrderSequence / fencing errors fence.
    void onProduceResponse(const uint8_t* buf, std::size_t len);

    /// F14: a MetadataResponse arrived — requests waiting for coordinator
    /// re-discovery back off and re-send.
    void onCoordinatorRediscovered() noexcept { ++discoveryGen_; }

    /// Client close / closeImmediately: every pending op completes Closed,
    /// every request / batch / waiting record is dropped, a pending close
    /// token completes Closed, the status channel is finished.  Later commands
    /// are discarded.  Idempotent.
    void failAll();

    // ---- F8c C7: close (rich state: Open -> Closing{...} -> closed_) ---------
    /// Begin closing: an open broker transaction (InTransaction /
    /// AbortableError, or an in-doubt abort) is aborted with EndTxn(abort)
    /// first; a commit / abort / init already running is waited for (its own
    /// completion is delivered as usual).  Then every remaining op completes
    /// Closed and the token completes: Ok, or the abort's failure status
    /// (Timeout / Fenced / BrokerError), or Timeout when
    /// transactionOpTimeoutMs passes first (a commit left in doubt, ...).
    /// The producer is closed either way.  A second close completes Closed.
    void beginClose(const detail::OpTarget& target, Token token, int64_t nowNs);
    /// The handle's destructor semantics (the duty side of HandleRelease):
    /// closeImmediately unless a close already started.
    void applyHandleRelease() {
        if (!closed_ && !std::holds_alternative<LifeClosing>(life_)) failAll();
    }
    /// A close (TxnClose / Client::close()'s drain) is in progress.
    [[nodiscard]] bool closingDuty() const noexcept { return std::holds_alternative<LifeClosing>(life_); }

    // ---- Duty-thread reads (a duty-cycle callback; tests) ---------------------
    [[nodiscard]] int64_t  producerIdDuty() const noexcept { return producerId_; }
    [[nodiscard]] int16_t  producerEpochDuty() const noexcept { return producerEpoch_; }
    [[nodiscard]] TxnState stateDuty() const noexcept { return static_cast<TxnState>(state_.index()); }
    [[nodiscard]] bool     transactionActiveDuty() const noexcept { return txnOpen_(stateDuty()); }
    [[nodiscard]] bool     closedDuty() const noexcept { return closed_; }
    [[nodiscard]] static bool txnOpen_(TxnState s) noexcept {
        return s == TxnState::InTransaction || s == TxnState::CommittingTransaction ||
               s == TxnState::AbortingTransaction || s == TxnState::AbortableError;
    }

    // ---- Test hooks (duty thread) ---------------------------------------------
    [[nodiscard]] std::size_t testPendingRequests() const noexcept { return reqs_.size(); }
    [[nodiscard]] std::size_t testPendingOps() const noexcept { return ops_.size(); }
    [[nodiscard]] std::size_t testInFlightBatches() const noexcept { return batches_.size(); }
    [[nodiscard]] std::size_t testWaitingRecords() const noexcept;
    /// Sends / begins refused by the state machine (dropped, never produced).
    [[nodiscard]] uint64_t testRefusedSends() const noexcept { return refusedSends_; }
    [[nodiscard]] uint64_t testRefusedBegins() const noexcept { return refusedBegins_; }
    /// The correlationId of the (single) pending request of `kind` (0: none).
    [[nodiscard]] int64_t testRequestCorrId(uint8_t kind) const noexcept;

    // ---- App side ---------------------------------------------------------------
    /// Duty -> app snapshots (producer id / epoch / txn active).  Closed by
    /// failAll(); abandoned by the Client when the registration was refused.
    detail::StatusRing<TxnStatus, 4096> status;  // room for many unread transitions
    /// App-owned: the latest snapshot drained by the handle (one app thread).
    TxnStatus appStatusCache{};
    /// F8c C7 fix round 2 (F1): orphaned release.
    int64_t orphanFence  = 0;      // written by the handle BEFORE its orphan RMW (published by it)
    bool    releaseNoted = false;  // duty thread: its release was applied / queued

    /// The F14 request kinds (testRequestCorrId's `kind`).
    enum class ReqKind : uint8_t { InitProducerId, AddPartitions, AddOffsets, TxnOffsetCommit, EndTxn };

private:
    // ---- F14 retained request descriptor (duty thread) -----------------------
    struct TxnRequest {
        struct Unsent {};                                    // offered on the next cycle
        struct InFlight { int64_t sessionId = 0; };          // offered; awaiting its response
        struct Backoff { uint32_t cyclesLeft = 0; };         // 27 / transient: wait N cycles
        struct AwaitCoordinator { uint64_t discoveryGen = 0; };  // 14: wait for re-discovery
        using State = std::variant<Unsent, InFlight, Backoff, AwaitCoordinator>;

        ReqKind              kind = ReqKind::InitProducerId;
        int64_t              corrId = 0;
        std::vector<uint8_t> frame;    // encoded; sessionId re-stamped at each (re)send
        uint32_t             attempt = 0;
        bool                 sent = false;  // offered at least once (m15: a timeout may have landed)
        uint64_t             opId = 0;  // the user op it serves (0: AddPartitions)
        rune::client::TopicPartitionKey tp{};  // AddPartitions
        int64_t              deadlineNs = 0;   // AddPartitions (ops carry their own)
        State                state = Unsent{};
    };

    // ---- User ops (one completion each; duty thread) ------------------------
    struct UserOp {
        struct Init { int64_t corrId = 0; };
        struct AddOffsets { int64_t corrId = 0; };
        struct CommitOffsets { int64_t corrId = 0; };
        struct AwaitData {};                     // EndTxn: waiting for every record to be acked
        struct Ending { int64_t corrId = 0; };   // EndTxn sent
        struct Bumping { int64_t corrId = 0; };  // KIP-360: InitProducerId after EndTxn(abort)
        using Phase = std::variant<Init, AddOffsets, CommitOffsets, AwaitData, Ending, Bumping>;

        uint64_t          id = 0;
        OpKind            kind = OpKind::InitTransactions;
        bool              internal = false;      // C7: the close's own abort (recorded, not delivered)
        detail::OpTarget  target{};
        Token             token = 0;
        int64_t           deadlineNs = 0;
        bool              commit = false;        // EndTxn
        TxnOffsetsPayload offsets;               // SendOffsets
        Phase             phase = Init{};
        /// m5: abort calls that joined this pending abort (same completion).
        std::vector<std::pair<detail::OpTarget, Token>> joiners;
    };

    // ---- In-flight produce batches (retained for idempotent re-send) ---------
    struct InFlightBatch {
        int32_t              topicId = 0;
        int32_t              partition = 0;
        int32_t              baseSequence = 0;
        std::vector<uint8_t> wireFrame;          // sessionId re-stamped on (re)send
        int32_t              retryCount = 0;
        static constexpr int kMaxRetries = 5;
        int64_t              deadlineNs = 0;     // duty clock
        int64_t              sentSessionId = 0;  // 0: not on the wire (re-sent next cycle)
        // EOS follow-ups: every offer after the first is a NEW attempt with a
        // fresh correlationId (I-2: an earlier attempt's error answer never
        // decides the batch; its OK still does — stored is stored).
        bool                 offered  = false;
        bool                 parked   = false;  // I-1: OOS behind an older live batch
        uint64_t             sendTick = 0;      // TransactionalProducerImpl::sendTick_ at the latest offer
        std::vector<int64_t> priorCorrIds;      // the superseded attempts' ids
    };

    enum class Verdict : uint8_t { Ok, Retry, Rediscover, Fenced, Fail };
    [[nodiscard]] static Verdict classify_(ReqKind kind, int16_t errorCode) noexcept;

    // ---- Transaction state machine (Kafka TransactionManager; the variant's
    //      alternative index IS the TxnState value) ------------------------------
    struct SUninitialized {};
    struct SInitializing { uint64_t opId = 0; };
    struct SReady {};
    // `started` (B-1, Kafka transactionStarted): an AddPartitions / AddOffsets
    // was ACKNOWLEDGED OK — a transaction that never started ends locally,
    // no EndTxn.  `opId` 0 in Committing / Aborting (B-2, Kafka m1): the end
    // op timed out in doubt — the producer stays there and the app retries
    // the same direction.  `endSent`: EndTxn went out (the data settled).
    // `bumpEpoch` (B-3, KIP-360): a sequence gap (lost batch / OOS) — after
    // EndTxn(abort), InitProducerId bumps the epoch and sequences restart.
    // `maybeStarted` (fix round 3, m15): an AddPartitions / AddOffsets was
    // SENT but timed out unanswered — the coordinator may hold an Ongoing
    // transaction: abort sends EndTxn(abort), and only then a 19 (nothing open)
    // completes it Ok.  `ended` (B-4): EndTxn(abort) was answered Ok and the
    // KIP-360 bump is pending — a retried abort re-sends ONLY the bump
    // (never EndTxn under the stale epoch).
    struct SInTransaction { bool started = false; bool maybeStarted = false; };
    struct SCommitting {
        uint64_t opId = 0;
        bool     started = false;
        bool     endSent = false;
        bool     maybeStarted = false;
    };
    struct SAborting {
        uint64_t opId = 0;
        bool     started = false;
        bool     endSent = false;
        bool     bumpEpoch = false;
        bool     maybeStarted = false;
        bool     ended = false;
    };
    struct SAbortableError {
        OpStatus status = OpStatus::BrokerError;
        uint16_t errorCode = 0;
        bool     started = false;
        bool     bumpEpoch = false;
        bool     maybeStarted = false;
    };
    struct SFatalError { uint16_t errorCode = 0; };
    using TxnSm = std::variant<SUninitialized, SInitializing, SReady, SInTransaction, SCommitting,
                               SAborting, SAbortableError, SFatalError>;
    static_assert(std::variant_size_v<TxnSm> == static_cast<std::size_t>(TxnState::FatalError) + 1);

    // ---- F8c C7: close life (duty thread; rich states) -----------------------
    struct LifeOpen {};
    struct LifeClosing {
        detail::OpTarget target{};
        Token            token      = 0;
        int64_t          deadlineNs = 0;
        uint64_t         abortOpId  = 0;      // the close's own EndTxn(abort) op (0: none issued)
        bool             abortDone  = false;  // that op completed (status below)
        OpStatus         abortStatus = OpStatus::Ok;
        uint16_t         abortCode   = 0;
    };
    std::variant<LifeOpen, LifeClosing> life_{};
    /// One close step (beginClose, then every duty cycle while closing).
    void closeStep_(int64_t nowNs);
    /// The close's own abort (an internal op: its completion is recorded in
    /// LifeClosing, never delivered).  Returns its op id (0: nothing to abort).
    uint64_t startCloseAbort_(int64_t nowNs);
    /// Close: fail what is left, finish the status channel, complete the token.
    void finishClose_(OpStatus st, uint16_t errorCode);

    // ---- helpers ----------------------------------------------------------------
    void complete_(UserOp& op, OpStatus st, uint16_t errorCode = 0, std::string_view text = {});
    void completeAndErase_(uint64_t opId, OpStatus st, uint16_t errorCode = 0, std::string_view text = {});
    UserOp*     findOp_(uint64_t id) noexcept;
    TxnRequest* findReq_(int64_t corrId) noexcept;
    void eraseReq_(int64_t corrId) noexcept;
    void eraseReqsOf_(uint64_t opId) noexcept;
    int64_t addRequest_(ReqKind kind, uint64_t opId, int64_t deadlineNs);  // encodes; Unsent
    bool encode_(TxnRequest& r, const UserOp* op);
    void retry_(TxnRequest& r, Verdict v);
    void enterFatal_(uint16_t errorCode, uint64_t exceptOpId = 0);
    void poison_(OpStatus st, uint16_t errorCode, bool bumpEpoch = false);
    void finishTxn_();
    void markStarted_() noexcept;
    void markMaybeStarted_() noexcept;
    /// The end op `opId` is done on the broker (EndTxn Ok, or a txn that never
    /// started): bump the epoch first when required (abort), else Ready + Ok.
    void endDone_(uint64_t opId, int64_t nowNs);
    void dropWaiting_() noexcept;
    void refuse_(const detail::HubCommand& cmd, OpKind kind, OpStatus st, uint16_t code, std::string_view why);
    void publishStatus_();
    TxnPartitionState& partition_(int32_t topicId, int32_t partition);
    void produce_(TxnPartitionState& ps, TxnSendRecord& rec, int64_t nowNs, int64_t sessionId,
                  const SendFn& sendFn, int& work);
    bool sendFrame_(std::vector<uint8_t>& frame, int64_t sessionId, const SendFn& sendFn);
    void sweep_(int64_t nowNs);
    bool dataSettled_() const noexcept;
    void gateEndTxn_(int64_t nowNs);
    [[nodiscard]] int64_t opDeadline_(int64_t nowNs) const noexcept {
        return nowNs + cfg_.transactionOpTimeoutMs * 1'000'000LL;
    }

    // ---- configuration --------------------------------------------------------
    std::string    transactionalId_;
    ProducerConfig cfg_;
    int32_t        transactionTimeoutMs_;
    int64_t&       nextCorrId_;  // the Client's duty-owned correlation counter

    detail::CompletionSink completions_{};
    RediscoverFn           rediscover_    = nullptr;
    void*                  rediscoverCtx_ = nullptr;

    // ---- duty-thread state ------------------------------------------------------
    int64_t  producerId_    = kNoPid;
    int16_t  producerEpoch_ = 0;
    TxnSm    state_{SUninitialized{}};
    bool     closed_        = false;
    uint64_t refusedSends_  = 0;
    uint64_t refusedBegins_ = 0;

    uint64_t nextOpId_      = 1;
    uint64_t discoveryGen_  = 0;
    uint64_t rediscoverRequestedGen_ = ~uint64_t{0};
    int64_t  lastSessionId_ = 0;
    int64_t  lastNowNs_     = 0;

    std::vector<UserOp>     ops_;   // apply order
    std::vector<TxnRequest> reqs_;  // creation order
    std::map<rune::client::TopicPartitionKey, TxnPartitionState> partitions_;
    std::map<int64_t, InFlightBatch> batches_;  // current attempt's corrId -> in-flight batch
    std::map<int64_t, int64_t>       superseded_;  // an earlier attempt's corrId -> the current one
    uint64_t                         sendTick_ = 0;
    std::vector<int64_t>             dueScratch_;  // doWork's re-offer list (capacity kept)
    /// Drops a batch (any outcome): its partition's counters and superseded ids.
    void eraseBatch_(std::map<int64_t, InFlightBatch>::iterator it);
    /// Drops every batch (fatal / close).
    void clearBatches_() noexcept;
    /// I-1: once nothing un-acked lies before a partition's first parked batch,
    /// its parked batches are re-sent (next cycle, in sequence order).
    void armParked_(TxnPartitionState& ps);

    TxnStatus statusLast_{};
    bool      statusDirty_ = false;

    std::vector<uint8_t> batchScratch_;
};

} // namespace rune::client::producer
