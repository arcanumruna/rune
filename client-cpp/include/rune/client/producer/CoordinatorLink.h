// include/rune/client/producer/CoordinatorLink.h - Phase 2b F (FD5 / FD6): a producer's coordinator request
// slots.
//
// Owned by the producer's duty thread (shared-nothing: no atomics, no locks).  A fixed pool of
// kMaxCoordRequests slots with preallocated kCoordFrameCap-byte frames; each slot is a per-state variant
// (Free / Unsent / InFlight / Backoff / AwaitCoordinator) holding only what that state needs.  Correlation
// ids are coordcorr::make(ordinal, slot, seq): the Client routes an answer to its producer by the ordinal.
// Retries run on the duty clock (wall time), never in duty cycles.  ZERO allocation after construction.
#pragma once

#include <rune/client/Completion.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/producer/TxnTypes.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <type_traits>
#include <variant>

namespace rune::client::producer {

class CoordinatorLink {
public:
    static constexpr std::size_t kMaxCoordRequests = 8;
    static constexpr std::size_t kCoordFrameCap    = 4096;
    static constexpr int64_t     kBackoffBaseNs    = 1'000'000;     // 1 ms
    static constexpr int64_t     kBackoffCapNs     = 100'000'000;   // 100 ms
    static constexpr std::size_t kHdrSessionOff    = 8;             // MessageHeader.sessionId
    static constexpr std::size_t kHdrCorrOff       = 16;            // MessageHeader.correlationId

    /// What every live request carries (the facts that were stale in `Free` when they sat on the slot itself).
    struct Req {
        ReqKind  kind       = ReqKind::InitProducerId;
        uint16_t frameLen   = 0;
        int64_t  deadlineNs = 0;   ///< absolute duty-clock expiry, > 0
    };
    /// A request that has been submitted: it can be retried, so it also counts its retries (`attempt` drives the
    /// backoff and survives a session change's InFlight -> Unsent).
    struct Sent : Req { uint32_t attempt = 0; };

    struct Free {};
    struct Building : Req {};   ///< acquired, the caller is encoding; submit() makes it Unsent
    struct Unsent : Sent {};
    struct InFlight : Sent {};
    struct Backoff : Sent { int64_t untilNs = 0; };
    /// NotCoordinator: re-sent once the coordinator moved (gen) AND its backoff elapsed - a coordinator that keeps
    /// answering NotCoordinator (not loaded yet, a follower) is retried paced, never once per metadata round trip.
    struct AwaitCoordinator : Sent { uint64_t gen = 0; int64_t notBeforeNs = 0; };
    using SlotState = std::variant<Free, Building, Unsent, InFlight, Backoff, AwaitCoordinator>;
    struct Slot {
        SlotState state = Free{};
        int64_t   corrId = 0;
        uint32_t  offers  = 0;      ///< frames offered for this request (first send + every re-send of the link)
        std::array<uint8_t, kCoordFrameCap> frame{};

        /// The live request's facts; nullptr in Free.
        [[nodiscard]] Req* req() noexcept {
            return std::visit([](auto& st) -> Req* {
                if constexpr (std::is_base_of_v<Req, std::remove_reference_t<decltype(st)>>) return &st;
                else return nullptr;
            }, state);
        }
        [[nodiscard]] const Req* req() const noexcept { return const_cast<Slot*>(this)->req(); }
        [[nodiscard]] Sent* sent() noexcept {
            return std::visit([](auto& st) -> Sent* {
                if constexpr (std::is_base_of_v<Sent, std::remove_reference_t<decltype(st)>>) return &st;
                else return nullptr;
            }, state);
        }
        [[nodiscard]] ReqKind  kind() const noexcept { const Req* r = req(); return r != nullptr ? r->kind : ReqKind::InitProducerId; }
        [[nodiscard]] uint16_t frameLen() const noexcept { const Req* r = req(); return r != nullptr ? r->frameLen : uint16_t{0}; }
        [[nodiscard]] int64_t  deadlineNs() const noexcept { const Req* r = req(); return r != nullptr ? r->deadlineNs : 0; }
        /// Retries so far (a test hook; 0 before the first retry and in Building / Free).
        [[nodiscard]] uint32_t attempt() const noexcept { const Sent* x = const_cast<Slot*>(this)->sent(); return x != nullptr ? x->attempt : 0u; }
        /// The caller encoded `n` bytes into `frame` (Building).
        void setFrameLen(uint16_t n) noexcept { if (Req* r = req()) r->frameLen = n; }
    };

    /// `seqBase`: the ordinal's carried-forward request sequence (ProducerHub keeps it per ordinal, like the
    /// batch sequences): ids never repeat across incarnations of an ordinal, so a late answer to a closed
    /// producer's request can never match the new one's.  The 32-bit sequence wraps only after 2^32 requests
    /// on one ordinal (~50 days at one per millisecond), far past any answer's lifetime.
    /// `capacity` (1..kMaxCoordRequests): the slots this producer can ever have in flight, allocated here (control
    /// plane) and never grown - a transactional producer needs kMaxCoordRequests, a plain one only its InitProducerId.
    explicit CoordinatorLink(uint32_t ordinal, CoordinatorSink sink, uint32_t seqBase = 0,
                             std::size_t capacity = kMaxCoordRequests)
        : slots_(std::make_unique<Slot[]>(std::clamp<std::size_t>(capacity, 1, kMaxCoordRequests))),
          capacity_(std::clamp<std::size_t>(capacity, 1, kMaxCoordRequests)),
          ordinal_(ordinal), seq_(seqBase), sink_(sink) {}
    /// Slots allocated for this link (a test hook; fixed for the link's life).
    [[nodiscard]] std::size_t slotCapacity() const noexcept { return capacity_; }

    /// A free slot, or nullptr (the caller retries next pass).  The slot carries its correlation id
    /// (`corrId`); the caller encodes the request into slot->frame with that id and sets frameLen, then
    /// submit() makes it Unsent.  ZERO allocation.  deadlineNs MUST be > 0 (absolute duty clock): every slot, in every state, expires by it.
    /// A non-positive deadline is refused (nullptr; asserts in debug).
    [[nodiscard]] Slot* acquire(ReqKind kind, int64_t deadlineNs) noexcept;
    /// Unsent: pump() offers it.  Stamps the frame's header correlation id.
    void submit(Slot& s) noexcept;

    /// One duty pass: offer Unsent / due Backoff / AwaitCoordinator-after-move slots; expire deadlines.
    /// `onExpired(slot)` reports a slot whose deadline passed (the caller completes its op Timeout); the
    /// link releases it afterwards, so a deadline fires exactly once and a late answer finds no slot.
    /// Returns the work done (offers + expiries).
    template <class OnExpired>
    int pump(int64_t nowNs, OnExpired&& onExpired) {
        if (live_ == 0) return 0;   // idle: touches no slot memory
        ++scans_;
        int work = 0;
        for (Slot& s : slots()) {
            if (std::holds_alternative<Free>(s.state) || std::holds_alternative<Building>(s.state)) continue;
            if (nowNs >= s.deadlineNs()) {
                onExpired(s);
                release(s);
                ++work;
                continue;
            }
            work += offerIfDue_(s, nowNs);
        }
        return work;
    }

    /// An answer for one of our ids: classify (FD5 / FD6), retry in place or hand it to the caller.
    /// Returns the slot to the caller (then release()) unless the answer was a retriable one or matches
    /// no live slot (late / duplicate).
    [[nodiscard]] Slot* onAnswer(const CoordAnswer& a, int64_t nowNs) noexcept;
    /// Idempotent.
    void release(Slot& s) noexcept;
    /// Every slot back to Free (a terminal failure: fenced / revoked / closeImmediately - nothing is ever
    /// offered or re-sent again for them).  Idempotent.
    void releaseAll() noexcept {
        for (Slot& s : slots()) release(s);
    }
    void onCoordinatorMoved() noexcept { ++gen_; }   ///< AwaitCoordinator slots become due
    void onSessionChanged() noexcept;                ///< InFlight -> Unsent (same corrId)
    [[nodiscard]] std::size_t inUse() const noexcept { return live_; }
    /// Next sequence to be handed out minus one: ProducerHub carries it to the ordinal's next incarnation.
    [[nodiscard]] uint32_t seq() const noexcept { return seq_; }
    /// Test hook: pump passes that scanned the slots (an idle link never does).
    [[nodiscard]] uint64_t scans() const noexcept { return scans_; }
    [[nodiscard]] uint32_t ordinal() const noexcept { return ordinal_; }
    /// The coordinator session exists (the sink reports a non-zero id): a request offered now is sent.
    [[nodiscard]] bool connected() const noexcept {
        return sink_.sessionId != nullptr && sink_.sessionId(sink_.ctx) != 0;
    }

    /// The verdict table (FD5 / FD6; Kafka parity, plus auth).
    [[nodiscard]] static Verdict classify(ReqKind kind, int16_t errorCode) noexcept;
    /// min(1 ms << (attempt - 1), 100 ms); attempt 0 -> 0.
    [[nodiscard]] static int64_t backoffNs(uint32_t attempt) noexcept;

private:
    int offerIfDue_(Slot& s, int64_t nowNs) noexcept;
    void askRediscover_() noexcept;   // #351: once per generation; a refused ask stays due
    bool offer_(Slot& s) noexcept;

    [[nodiscard]] std::span<Slot> slots() noexcept { return {slots_.get(), capacity_}; }

    std::unique_ptr<Slot[]> slots_;   // capacity_ slots, preallocated at construction
    std::size_t     capacity_;
    uint32_t        ordinal_;
    uint32_t        seq_;
    uint32_t        live_ = 0;   // slots not Free (Building included)
    uint64_t        scans_ = 0;
    uint64_t        gen_ = 0;
    uint64_t        askedGen_ = ~uint64_t{0};   // the generation a re-discovery was last SUBMITTED for (#351)
    CoordinatorSink sink_;
};

// ---- Phase 2b F (FD4): the producer identity machine, one state per phase, each holding only its data ----
// (Declared here, not in TxnTypes.h: IdRequested names CoordinatorLink::Slot.)

/// FD4: a bare ProducerImpl with no link wired (unit rigs, benches) starts IdAssigned with this id.
inline constexpr int64_t kRigProducerId = 0x7E57'0000'0000'0001LL;

struct IdUnassigned {};
struct IdRequested { CoordinatorLink::Slot* slot = nullptr; };
struct IdAssigned  { int64_t producerId = -1; int16_t epoch = 0; };
struct IdFailed    { OpStatus status = OpStatus::BrokerError; uint16_t errorCode = 0; };
using ProducerIdentity = std::variant<IdUnassigned, IdRequested, IdAssigned, IdFailed>;
inline constexpr std::size_t kIdAssignedIndex = 2;   // index of IdAssigned in ProducerIdentity (static_asserted below)
static_assert(std::is_same_v<std::variant_alternative_t<kIdAssignedIndex, ProducerIdentity>, IdAssigned>);

// ---- Phase 2b F Task 5: the transaction machine, one state per phase, each holding only its data ----
// (Declared here, not in TxnTypes.h, for the same reason: the states name CoordinatorLink::Slot.)  Owned by
// ProducerImpl's duty side; the alternative index IS the TxnState value (static_asserted in ProducerImpl.h).
// Today's TxnSm booleans became states / enums, and every fact survives an in-doubt timeout:
//   started / maybeStarted -> TxnStarted (only ever upgraded, and frozen once EndTxn(abort) was answered);
//   endSent                -> EndSent, then InDoubt{AfterEnd} (a commit that may have landed is never aborted);
//   bumpEpoch              -> AbortKind (sticky);
//   ended                  -> Bumping, then InDoubt{Bump} (a retried abort / close re-sends ONLY the bump);
//   opId == 0 (in doubt)   -> InDoubt{BeforeEnd | AfterEnd | Bump}.

/// Who issued a lifecycle op.  An Internal op is the close's own abort (startCloseAbort_): its outcome is the
/// close's (closeTxnStep_ reads it), never delivered on its own.  Every other op is an App op, whether or not its
/// target is empty (a fire-and-forget call completes into nothing but never overwrites the close's outcome).
enum class TxnOpOrigin : uint8_t { App, Internal };

/// One pending lifecycle op: where its single completion goes and when it times out.
struct TxnOp {
    detail::OpTarget target{};
    Token            token      = 0;
    int64_t          deadlineNs = 0;   ///< absolute duty clock (transactionOpTimeoutMs)
    TxnOpOrigin      origin     = TxnOpOrigin::App;
};
/// m5 / m13: calls that joined a pending commit / abort complete with it (one EndTxn).  Fixed capacity (zero
/// allocation): a call beyond it completes InvalidArgument (Producer.h documents the cap).
inline constexpr std::size_t kMaxTxnJoiners = 4;
/// Pending flush() calls: a call beyond the cap completes InvalidArgument.
inline constexpr std::size_t kMaxTxnFlushCalls = 8;
struct TxnJoiners {
    std::array<TxnOp, kMaxTxnJoiners> ops{};
    uint8_t                           n = 0;
};
struct AwaitData {};                                          ///< EndTxn waits for every record of the txn to be acked
/// Which send of EndTxn is on the link: the first, or a re-send - the app's retry of an in-doubt one (ResendEnd), or
/// the link's own re-offer of the same request (session change, coordinator move, backoff).  After any re-send of
/// EndTxn(commit) an earlier one may have landed, so only a definite answer ends it: a non-retriable one is fatal.
enum class EndTry : uint8_t { First, Resend };
struct EndSent  { CoordinatorLink::Slot* slot = nullptr; EndTry attempt = EndTry::First; };   ///< EndTxn on the link
/// Where an op that timed out left its transaction (the app retries the SAME direction, B-2):
///   BeforeEnd - EndTxn was never sent (the data wait timed out): the retry waits for the data, then sends it;
///   AfterEnd  - EndTxn was sent, its answer is unknown: it may have landed - never poisoned, never aborted over;
///   Bump      - EndTxn(abort) was answered OK, the KIP-360 epoch bump was not: the retry re-sends only the bump.
enum class InDoubtAt : uint8_t { BeforeEnd, AfterEnd, Bump };
struct InDoubt  { InDoubtAt at = InDoubtAt::BeforeEnd; };
struct Bumping  { CoordinatorLink::Slot* slot = nullptr; };   ///< InitProducerId after EndTxn(abort) (B-3); nullptr = not yet sent
/// The retry of an InDoubt{AfterEnd} op: EndTxn was sent before (the data had settled) - re-send it at once.  Like
/// EndSent it may have landed: never poisoned, never aborted over.
struct ResendEnd {};
using EndPhase = std::variant<AwaitData, EndSent, InDoubt, Bumping, ResendEnd>;

struct Uninitialized {};
struct Initializing   { TxnOp op; CoordinatorLink::Slot* slot = nullptr; };   ///< slot nullptr = not yet sent
struct Ready          {};
struct InTransaction  { TxnStarted started = TxnStarted::No; };
struct Committing     { TxnOp op; EndPhase phase = AwaitData{}; TxnStarted started = TxnStarted::No; TxnJoiners joined{}; };
/// What the records an abort discards complete with (Task 6): the poison of an AbortableError, else 19.
struct TxnOutcome {
    OpStatus status    = OpStatus::BrokerError;
    uint16_t errorCode = 19;   // InvalidTxnState
};
struct Aborting       {
    TxnOp      op;
    EndPhase   phase   = AwaitData{};
    TxnStarted started = TxnStarted::No;
    AbortKind  kind    = AbortKind::Plain;
    TxnJoiners joined{};
    TxnOutcome discard{};   ///< unsent records of the transaction complete with it (never produced)
};
/// #368: has the poison failed the records the transaction still held unsent?  Pending = the next duty pass does it
/// (never inside the call that poisoned: that may be failing a batch); Done = nothing is left to fail.
enum class PoisonDiscard : uint8_t { Pending, Done };
struct AbortableError {
    OpStatus      status    = OpStatus::BrokerError;
    uint16_t      errorCode = 0;
    TxnStarted    started   = TxnStarted::No;
    AbortKind     kind      = AbortKind::Plain;
    PoisonDiscard discard   = PoisonDiscard::Pending;
};
struct FatalError     { OpStatus status = OpStatus::Fenced; uint16_t errorCode = 0; };
using TxnMachine = std::variant<Uninitialized, Initializing, Ready, InTransaction, Committing, Aborting,
                                AbortableError, FatalError>;

// ---- close() of a transactional producer with a transaction open (ProducerImpl's life_ state LifeEndingTxn) ----
// Declared here, not inside ProducerImpl, so the rule-3 static_assert (ProducerTxn.MachineStatesCarryNoBooleans) can
// name them.
struct CloseAbortNone {};
struct CloseAbortRunning {};
struct CloseAbortDone { OpStatus status = OpStatus::Ok; uint16_t errorCode = 0; };
struct LifeEndingTxn {
    detail::OpTarget target{};
    Token            token      = 0;
    int64_t          deadlineNs = 0;   ///< transactionOpTimeoutMs
    std::variant<CloseAbortNone, CloseAbortRunning, CloseAbortDone> own{};
};

// ---- Phase 2b F Task 6: a partition's membership in the current transaction (PartitionState::txn) ----
//   NotInTxn -first record while InTransaction (seen by the flush)-> Adding{nullptr} (queued) -AddPartitionsToTxn
//   offered-> Adding{slot} -OK-> Added | -non-retriable-> AddFailed (the machine AbortableError) | -timeout-> queued
//   again (TxnStarted::Maybe when the frame was offered).  Only an Added partition's batches are ever sent; on
//   EndTxn's final answer every partition of the transaction goes back to NotInTxn.  A plain producer's partitions
//   are Added for ever: the flush pays one index compare.
struct NotInTxn  {};
struct Adding    { CoordinatorLink::Slot* slot = nullptr; };   ///< nullptr: queued for the next AddPartitionsToTxn
/// ackedBatches: how many of THIS transaction's batches the partition leader acked on this partition (#694).  0 means
/// the broker may hold no producer state for the transaction here, so an UnknownProducerId restarts the partition's
/// sequences in place; > 0 means the transaction's own acked data is gone and it can only abort.  Resets with the
/// membership at every transaction end.  A counter, not a flag (no booleans in a state); saturating.
struct Added     { uint32_t ackedBatches = 0; };
struct AddFailed { uint16_t errorCode = 0; };
using TxnMembership = std::variant<NotInTxn, Adding, Added, AddFailed>;
inline constexpr std::size_t kAddedIndex = 2;   // index of Added in TxnMembership
static_assert(std::is_same_v<std::variant_alternative_t<kAddedIndex, TxnMembership>, Added>);

// ---- Phase 2b F Task 7: the sendOffsetsToTransaction op (one at a time, only while InTransaction) ----
//   OffsetsIdle -sendOffsets-> OffsetsAdding{op, slot} -AddOffsetsToTxn OK (TxnStarted::Yes)-> OffsetsCommitting
//   {op, sent, outstanding, slots} -every TxnOffsetCommit request OK-> OffsetsIdle (the op completes Ok)
//   A non-retriable answer / the op deadline / an abort / a fatal error ends it from either state (slots released).
// The offsets and the group id live in the duty side's preallocated scratch (ProducerImpl::OffsetsScratch); the
// states hold only what they need.  FD5: the TxnOffsetCommit entries are split across requests (slots) that fit one
// kCoordFrameCap frame; `sent` entries are encoded so far, `slots` the requests still unanswered.
struct OffsetsIdle {};
struct OffsetsAdding { TxnOp op; CoordinatorLink::Slot* slot = nullptr; };   ///< slot nullptr: not yet sent
struct OffsetsCommitting {
    TxnOp    op;
    uint16_t sent        = 0;   ///< entries already encoded into a request
    uint8_t  outstanding = 0;   ///< requests sent and unanswered (== the non-null prefix of `slots`)
    std::array<CoordinatorLink::Slot*, CoordinatorLink::kMaxCoordRequests> slots{};
};
using OffsetsOp = std::variant<OffsetsIdle, OffsetsAdding, OffsetsCommitting>;

}  // namespace rune::client::producer
